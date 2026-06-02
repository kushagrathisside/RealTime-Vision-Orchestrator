# Current Implementation

This document describes what the current Rust code does. It is kept separate
from the product vision so implementation status stays unambiguous.

## Repository Shape

Rust workspace with eleven crates under `crates/`. All are listed as workspace
members so `cargo test --workspace` covers every crate.

| Crate | Responsibility |
|---|---|
| `rvo-bin` | Entrypoint, runtime wiring, SIGHUP reload |
| `rvo-config` | YAML config loading and validation |
| `rvo-camera` | OpenCV capture, mock camera, RTSP/URI source |
| `rvo-buffer` | Bounded circular frame buffer |
| `rvo-detector` | Detector trait, synthetic detectors |
| `rvo-signals` | Typed signal store |
| `rvo-events` | Condition DSL, temporal event engine, publishers |
| `rvo-scheduler` | Orchestration loop, load shedding |
| `rvo-clips` | Clip job pipeline, JPEG encoder, JSON metadata |
| `rvo-metrics` | Prometheus-style counters, HTTP endpoints |
| `rvo-core` | Shared time helper, reserved frame module |

## Entrypoint

`crates/rvo-bin/src/main.rs`. Startup order:

1. Read config path from `RVO_CONFIG` env var (default: `config/rvo.yaml`).
2. Start metrics server on `127.0.0.1:9090`.
3. Load and validate config.
4. Create the shared `Arc<Mutex<FrameBuffer>>`.
5. Build detectors and event engine from config.
6. Start camera thread.
7. Start clip encoder worker thread.
8. Optionally start event file sink if `event_log` is set in config.
9. Build scheduler (receives the shared frame buffer).
10. Spawn SIGHUP reload thread (Unix only; no-op on Windows).
11. Enter the 1 ms tick loop.

## Configuration

Active config: `config/rvo.yaml`.

Top-level fields:

- `camera`: source definition (`device_index` or `source_uri`).
- `detectors`: list of detector definitions.
- `events`: list of event definitions.
- `clips_dir`: output directory for evidence clips (default: `clips/`).
- `event_log`: optional path for JSON-lines event output (e.g. `events.jsonl`).

Detector kinds: `dummy`, `load`, `jitter`.

Event `condition` block supports `all` and `any` predicates. The shorthand
`signal_type` + `signal_threshold` fields expand to a single `gte` predicate.

## Camera Path

`rvo-camera` opens a `VideoCapture` from either a device index (integer) or a
URI string (RTSP, file path, HTTP stream).

For every successful read:
1. Create `Frame { ts: Instant, id: u64, image: Mat }`.
2. `try_send` through a bounded channel (capacity 5).
3. On full channel: increment `METRICS.frame_drops` and discard.

Camera failures are logged with a consecutive-failure counter to avoid log
spam. The thread does not panic on open or read failure.

## Frame Buffer

`rvo-buffer` holds a fixed-capacity circular buffer of `Frame`.

- `push(frame)` overwrites the oldest slot (O(1), no allocation).
- `slice(start, end)` returns timestamp-sorted frames in the window.
- `newest_frame()` / `newest_instant()` return `Option` (safe on empty buffer).

The buffer is wrapped in `Arc<Mutex<FrameBuffer>>` and shared between the
Scheduler (writes, tick-driven) and ClipManager (post-roll reads from spawned
threads). The lock is held only for the brief push/read operations, so
contention between the scheduler tick and post-roll threads is minimal.

## Detector Model

```rust
pub trait DetectorNode: Send {
    fn meta(&self) -> DetectorMeta;
    fn execute(&mut self, ctx: &DetectorContext<'_>) -> DetectorResult;
    // id(), max_fps(), dependencies(), output_signals(), cost_hint(),
    // requires_frame() all delegate to meta()
}
```

`DetectorContext` carries `now_ns: u64` and `frame: Option<&Frame>`.

`DetectorMeta` declares: `id`, `max_fps`, `dependencies: &'static [SignalType]`,
`output_signals: &'static [SignalType]`, `cost_hint`, `requires_frame`.

Synthetic detectors:
- `DummyDetector`: 30 FPS, no frame, emits `SignalType::Dummy = 1` with 1s TTL.
- `LoadDetector`: 10 FPS, no frame, busy-spins for `busy_ns`, emits nothing.
- `JitterDetector`: 30 FPS, no frame, busy-spins for random ≤ 2 ms.

## Scheduler

`rvo-scheduler` is the central orchestration loop.

Each `tick()`:

1. Drain all pending frames from the camera channel into the frame buffer.
2. Increment `rvo_scheduler_ticks`.
3. Snapshot `latest_frame` (holds the newest frame timestamp for dependency checks).
4. For each detector, evaluate in order:
   - Disabled? (set by `DetectorHealth::Failed`)
   - FPS cap elapsed?
   - Load-shedding backoff active?
   - Frame required but buffer empty?
   - All declared signal dependencies fresh?
5. Execute, measure elapsed nanoseconds, store produced signals.
6. On `Failed` health: disable detector permanently until reload.
7. For each event emitted by the event engine: publish, trigger clip job.

### Load Shedding

When a detector's last execution time exceeds 2× its FPS-budget interval,
a backoff period is applied based on cost hint:

- `Low`: no backoff — always allowed to recover.
- `Medium`: 100 ms backoff.
- `High`: 500 ms backoff.

During backoff the detector is skipped. The scheduler does not accumulate a
queue of missed executions. This keeps the live path from falling behind when
a model runs slow.

## Signal Store

`rvo-signals` stores one slot per `SignalType` in a fixed `Vec<SignalSlot>`.

Types: `Dummy`, `MotionLevel`, `FacePresent`, `PersonDetected`.

Each slot uses a seqlock-style version counter. `upsert` takes `&mut self`
so writes are already serialised by the borrow checker; the version check is
defensive for a future move to concurrent writes.

Freshness rule: `signal.ts_ns + signal.ttl_ns < now_ns` → signal is absent.

## Condition DSL

`rvo-events` exposes a composable condition type:

```rust
pub enum CompareOp { Gte, Gt, Eq, Lt, Lte }

pub struct SignalPredicate {
    pub signal_type: SignalType,
    pub op: CompareOp,
    pub value: u64,
}

pub enum Condition {
    All(Vec<SignalPredicate>),   // all predicates must hold (AND)
    Any(Vec<SignalPredicate>),   // any predicate must hold (OR)
}
```

`Condition::single_gte(signal_type, value)` is the shorthand for the common
single-signal greater-than-or-equal case.

A missing or stale signal evaluates to `false` for any predicate that reads it.

## Event Engine

One `EventMachine` per `EventDefinition`. State machine:

```
Idle → (condition true) → Potential { start_ns }
     → (duration elapsed) → emit Event + Cooldown { until_ns }
     → (cooldown elapsed) → Idle
```

`EventDefinition`: `event_type`, `condition: Condition`, `duration_ns`, `cooldown_ns`.

`EventEngine::update()` returns `Vec<Event>`. The scheduler iterates and for
each event: increments `rvo_events_emitted_total`, calls `EventPublisher`,
calls `ClipManager`.

## Evidence Pipeline

`ClipManager::on_event`:

1. Lock buffer, read `newest_instant()` → None: drop, count `rvo_clip_drops_total`.
2. Compute clip window `[event_ts − before, event_ts + after]`.
3. Spawn a thread: sleep `after`, then lock buffer, slice frames, `try_send` to encoder.
4. On full encoder queue: count `rvo_clip_drops_total`.

Encoder worker:

1. Create `{clips_dir}/{EventType}_{ts_ns}/` directory.
2. Write each frame as `frame_NNNN.jpg` via `opencv::imgcodecs::imwrite`.
3. Write `meta.json`: event type, timestamp ns, frame count, written count,
   per-frame timestamp array, clip window, encoding latency ms.

## Event Publishers

**Channel logger** (`start_event_logger`): always active; logs to stdout.

**File sink** (`start_event_file_sink`): active when `event_log` is set.
Appends one JSON line per event to the 
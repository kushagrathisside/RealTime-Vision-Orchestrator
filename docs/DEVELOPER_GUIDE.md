# RVO Developer Guide

This document explains why RVO is built the way it is: the language choice, the system invariant, the crate decomposition, the layered architecture, and the hard design calls with their tradeoffs. It is written for engineers who need a deep understanding of the system, not a tour of the directory tree.

---

## 1. Why Rust

### Predictable latency is the first requirement

A garbage-collected runtime periodically stops the world to reclaim memory. For a realtime video pipeline where the live path runs on a 1 ms tick, a GC pause is not a performance regression — it is a correctness failure. Rust eliminates this by having no runtime allocator overhead on the hot path and no GC.

This is not a theoretical concern. Any language with a managed heap (Go, Java, Python) introduces latency variance that is bounded by the GC's pause time, not by the application's logic. Rust's ownership model forces memory to be reclaimed at a known point — the end of its owner's scope — which is deterministic.

### Ownership makes concurrency correct by construction

RVO shares exactly one piece of mutable state between the live path and the evidence path: the frame buffer, wrapped in `Arc<Mutex<FrameBuffer>>`. Rust's type system enforces that:

- `Arc` is the only way to share ownership across threads.
- `Mutex` is the only way to get a mutable reference to the inner value.
- The borrow checker prevents holding a `MutexGuard` across an `await` or across a blocking call — enforcing the lock discipline that makes the design correct.

In Go or C++, this discipline is a comment. In Rust, it is a compile error.

The `Send` and `Sync` marker traits mean the compiler will reject any type that is not safe to move across thread boundaries. `DetectorNode: Send` is not a convention — it is enforced on every detector implementation.

### Zero-cost abstractions over detectors

`DetectorNode` is a trait object (`Box<dyn DetectorNode>`). Dynamic dispatch has a pointer-indirect call overhead, but no heap allocation per call and no boxing on every invocation. The trait object itself is allocated once at startup. On the hot scheduler path, the cost is one vtable lookup per detector per tick, which is acceptable.

Iterator chains, `map`, `filter_map`, and `try_send` all compile down to the same machine code as a hand-written loop. There is no "cost" to expressing the scheduler logic at the level of the problem.

### Cargo workspace maps to the component model

Each crate in the workspace is a unit of compilation, linkage, and dependency. Adding `rvo-testkit` as a `dev-dependency` of `rvo-scheduler` means test infrastructure never enters the production binary. This is not achievable with a flat module layout in a single crate.

### crossbeam channels over std channels

`crossbeam_channel::bounded` is the backbone of every bounded queue in the system. Compared to `std::sync::mpsc`:

- Both `Sender` and `Receiver` are `Clone` — multiple producers are trivial.
- `try_send` is lock-free on the fast path.
- The bounded channel semantics match what the system needs: reject and count, never block.

---

## 2. What RVO Is

### The problem

Live video AI pipelines have a structural tension. The live path — camera read → frame decode → detector inference → signal publish — must run at a fixed FPS. The evidence path — clip capture → frame encode → file write — is slow and variable. Naive implementations couple these, causing the live path to stall behind a slow encoder or a disk flush.

A second problem: model inference is non-deterministic in latency. A detector that usually runs in 5 ms may occasionally take 50 ms. Without explicit scheduling policy, a slow run cascades into accumulated backlog.

A third problem: single-frame detections are noisy. A human face appearing for one frame is not an event worth capturing. A face present continuously for 3 seconds is.

### The system invariant

> The live path must never wait for slow work.

Everything that could be slow — encoding, file I/O, downstream delivery, post-roll capture — is off the live path. The live path is: camera ingestion → frame buffer push → detector execution → signal publication → event evaluation.

### Core contract

1. Capture frames without blocking; drop when the channel is full.
2. Maintain a bounded rolling window of frames for evidence extraction.
3. Schedule detector nodes under FPS and latency constraints; shed load rather than accumulate backlog.
4. Publish typed signals with TTL freshness semantics.
5. Evaluate temporal conditions over signals; confirm events over time, not on instantaneous matches.
6. Extract evidence asynchronously; the live path never waits for the clip encoder.
7. Expose metrics for every drop, skip, and failure.

### What RVO is not

- A computer vision model or training framework. It runs models; it does not define them.
- A video codec or muxer. It writes JPEG frames; video muxing is future work.
- A general-purpose stream processor like Kafka or Flink. It is a single-process, bounded runtime.
- A durable event log. Events are written to a JSON-lines file; there is no replay or retention guarantee.

---

## 3. Crate Layout

### `rvo-bin`
Entrypoint and wiring. Reads config, starts the metrics server, instantiates the frame buffer, builds detectors and the event engine, starts the camera thread and encoder worker, optionally starts the event file sink, installs the SIGHUP reload handler, then enters the 1 ms tick loop. Nothing in `rvo-bin` is a library — it is all startup sequencing and integration glue.

### `rvo-config`
YAML config loading and validation. The only crate that touches `serde` deserialization. All other crates accept already-validated config structs.

### `rvo-core`
Shared time primitives and the `Frame` type. Kept minimal so every other crate can depend on it without pulling in heavier dependencies.

### `rvo-camera`
Opens a `VideoCapture` from a device index or URI. Produces `Frame` values and `try_send`s them into a bounded channel. On a full channel, it increments the frame drop counter and discards. The camera thread does not panic on read failure; it logs a consecutive-failure count and retries. This crate has no knowledge of the downstream scheduler.

### `rvo-buffer`
`FrameBuffer`: a fixed-capacity circular buffer of `Frame`. `push` is O(1) and overwrites the oldest slot. `slice(start, end)` returns timestamp-ordered frames in the window. Wrapped in `Arc<Mutex<FrameBuffer>>` and shared between the scheduler (writer) and the clip manager (reader). The lock is never held across a blocking operation.

### `rvo-detector`
Defines the `DetectorNode` trait, `DetectorMeta`, `DetectorContext`, and `DetectorResult`. Also contains the synthetic detectors used in production config (`DummyDetector`, `LoadDetector`, `JitterDetector`). This crate is the abstraction boundary between the scheduler and any model implementation.

### `rvo-signals`
Defines `SignalType`, `Signal`, and `SignalStore`. The store is a typed blackboard: one slot per `SignalType`, O(1) read and write by type index, TTL freshness enforced at read time. Signals are the shared language between detectors and the event engine.

### `rvo-events`
Implements the condition DSL (`All` / `Any` over `SignalPredicate`), `EventDefinition`, and the temporal state machine per event (`EventMachine`). Also contains `EventPublisher` and the JSON-lines file sink. The event engine consumes the signal store, not raw frames.

### `rvo-scheduler`
The orchestration loop. Each `tick()` drains camera frames into the buffer, snapshots the newest frame, evaluates each detector (FPS gate, backoff gate, dependency gate, frame-required gate), runs eligible detectors, stores produced signals, runs the event engine, and dispatches events to publishers and the clip manager. This is the core of the runtime.

### `rvo-clips`
`ClipManager`: receives clip jobs triggered by the event engine. Spawns a post-roll thread that sleeps for the configured duration, then locks the frame buffer, slices frames, and sends the job to the encoder via `try_send`. The encoder worker writes JPEG frames and a `meta.json` sidecar. All of this is off the live path.

### `rvo-metrics`
Global atomic counters and an HTTP server. `/metrics` serves Prometheus text format. `/health` returns `200 ok` as a liveness check. Counters cover frame drops, scheduler ticks, detector executions, skips, failures, aggregate latency, event emissions, clip drops, and event drops.

### `rvo-testkit`
Synthetic test infrastructure: `SyntheticCamera`, `ScriptedDetector`, `ProbabilisticDetector`, `LatencyDetector`, `FailingDetector`, `ChainedDetector`, `EventCapture`, `MetricsSnapshot`, `PipelineBuilder`. This crate is a `dev-dependency` only and never enters a production binary.

### `rvo-scenarios`
End-to-end integration tests using `rvo-testkit`. Each scenario builds a complete pipeline and asserts temporal behavior, drop counts, or detector health transitions.

---

## 4. End-to-End Data Flow

```
Camera / RTSP Stream
    |
    v  [bounded channel, cap 5 — drop on full → rvo_frame_drops_total]
Scheduler.tick()  [1 ms loop]
    |
    +--→ FrameBuffer  [Arc<Mutex<>>, circular, cap 300 — overwrite oldest]
    |
    +--→ DetectorNode(s)  [sequential, FPS-gated, backoff-gated, dep-gated]
    |        |
    |        v
    |    SignalStore  [typed slots, TTL freshness, O(1) read/write]
    |
    +--→ EventEngine  [Condition DSL: All/Any over SignalPredicates]
    |        |
    |        v  [EventMachine: Idle → Potential → emit → Cooldown → Idle]
    |    Vec<Event>
    |
    +--→ EventPublisher  [try_send, cap 64 — drop on full → rvo_event_drops_total]
    |        |
    |        +--→ stdout logger
    |        +--→ JSON-lines file sink (if configured)
    |
    +--→ ClipManager  [try_send, cap 8 — drop on full → rvo_clip_drops_total]
              |
              v  [spawned post-roll thread: sleep after_ns, then lock buffer]
         Encoder Worker
              |
              v
         clips/{type}_{ts}/frame_NNNN.jpg + meta.json
```

### Bounded queue table

| Handoff | Capacity | Overflow behavior |
|---|---|---|
| Camera → Scheduler | 5 frames | Drop, count `rvo_frame_drops_total` |
| Scheduler → EventPublisher | 64 events | Drop, count `rvo_event_drops_total` |
| Scheduler → ClipManager | 8 jobs | Drop, count `rvo_clip_drops_total` |
| FrameBuffer | 300 frames (~10 s at 30 fps) | Overwrite oldest |

No unbounded queue exists anywhere.

---

## 5. Key Design Decisions and Tradeoffs

### 5.1 Bounded channels and explicit frame drops

Every channel is bounded and uses `try_send`. When a channel is full, the sender discards and increments a metric counter.

The alternative — blocking the producer until the consumer catches up — turns downstream slowness into upstream stalls, which propagates back to the camera thread and breaks the realtime contract. Dropping is the correct choice when freshness matters more than completeness. A 1-second-old frame is worth less than a fresh one; it should be discarded, not queued.

Frame drops are a signal of overload, not a bug. The metrics surface them explicitly so operators can observe and respond.

The capacity choices are deliberate:
- Camera channel at 5: enough to absorb a single slow tick without dropping, not enough to buffer stale frames.
- Event publisher at 64: events are small, and bursts are short. A sink stall should not drop events immediately.
- Clip queue at 8: encoding is slow. A small queue prevents runaway memory growth from queued raw frames.

### 5.2 Frame buffer lock discipline

The `Arc<Mutex<FrameBuffer>>` is the only shared mutable state between the live path and the evidence path. The invariant is: the lock is held only for the duration of a buffer operation, never across a blocking call.

The scheduler holds the lock briefly to push a frame and to read the newest timestamp. The post-roll thread holds the lock briefly to call `slice()`. The post-roll thread sleeps for the post-roll duration *before* acquiring the lock, not *while* holding it. This means the live path is never contending with a sleeping thread.

If the lock were held across the sleep, a post-roll wait of 2 seconds would stall the scheduler tick for 2 seconds. The current design makes post-roll contention a brief critical section at the start and end, not across the wait.

### 5.3 Sequential detector execution

All detectors run sequentially within a single `tick()` call. This is a deliberate simplification: no thread pool, no work stealing, no concurrent signal writes.

The tradeoff: a single slow detector delays all subsequent detectors in the tick. The mitigation is load shedding — slow detectors are backed off, so they do not run every tick. For a pipeline where each detector is expected to run in milliseconds, sequential execution is simpler and has lower overhead than the coordination cost of a thread pool.

Parallel execution would require concurrent signal writes, which would require the signal store to use atomics or fine-grained locking rather than the current `&mut self` borrow model. That complexity is reserved for a future multi-stream or distributed phase.

### 5.4 Signal store as typed blackboard with TTL freshness

The signal store is not a queue. It answers one question: *What is the latest valid value of signal X right now?* There is one slot per `SignalType`, and a new write overwrites the previous value. Reads are O(1) by type index.

Freshness is enforced via TTL at read time: `signal.ts_ns + signal.ttl_ns < now_ns` means absent. This prevents stale signals from triggering events long after their detector stopped running. A signal that is not refreshed within its TTL becomes invisible to the event engine.

The store uses a seqlock-style version counter on each slot. Writes increment the version before and after the value update. Reads check that the version is even (no write in progress) and consistent across the read. Currently, writes are serialized by the `&mut self` borrow on `SignalStore`, so the seqlock is defensive against a future move to concurrent writes. If that changes, the memory ordering on the version counter must be reviewed carefully — seqlocks require at minimum `Release` on write and `Acquire` on read.

### 5.5 Temporal event confirmation

Events are confirmed over time, not on instantaneous signal matches. The state machine per event definition:

```
Idle
  → (condition first evaluates true) → Potential { start_ns }
  → (condition still true, elapsed >= duration_ns) → emit Event + Cooldown { until_ns }
  → (cooldown elapsed) → Idle

  (condition becomes false while Potential) → Idle
```

This eliminates the noise problem: a one-frame detection never fires an event. The event fires only if the condition holds continuously for `duration_ns`. The cooldown prevents re-triggering on the same sustained condition.

The tradeoff: added complexity in the event engine, and tests that validate event timing must run with real elapsed time (no clock injection). The correctness benefit — eliminating noisy one-shot events — is the core value proposition of the temporal confirmation layer.

### 5.6 Load shedding via cost hints and overrun detection

When a detector's last execution time exceeds 2× its FPS-budget interval, the scheduler applies a backoff:

- `CostHint::Low`: no backoff — always allowed to recover immediately.
- `CostHint::Medium`: 100 ms backoff.
- `CostHint::High`: 500 ms backoff.

During backoff, the detector is skipped entirely. The scheduler does not queue missed executions. This keeps the live path from falling behind when a model runs slow: RVO degrades by doing less work per unit time, not by accumulating a backlog of stale work.

The cost hint is self-declared by the detector, not measured. This is intentional: the scheduler needs the hint before execution to make admission decisions. The overrun check corrects for detectors that underestimate their cost.

A known limitation: the current load shedding policy only uses execution latency as input. It does not account for downstream queue depth (clip queue depth, event queue depth). A detector that produces many events per tick could saturate the event publisher even under acceptable execution latency.

### 5.7 Evidence pipeline as best-effort

The clip pipeline is explicitly best-effort. `ClipManager::on_event` uses `try_send` and drops on failure. The encoder worker thread runs at its own pace and does not signal back to the scheduler. A failed or slow encoding does not affect the scheduler tick rate.

The philosophical point: an event is meaningful even if the evidence capture fails. The event itself is the ground truth; the clip is supporting material. Coupling event reliability to encoding reliability would be the wrong abstraction boundary.

### 5.8 Hot reload and event machine state

SIGHUP (Unix only) reloads config, rebuilds detectors and the event engine, and swaps them into the running scheduler. Invalid configs preserve the live runtime.

A consequence: the event state machines are rebuilt from scratch on reload. Any in-progress `Potential` state is lost. Events that were building toward confirmation are reset to `Idle`. This is acceptable for config changes but should be documented as a known behavior.

---

## 6. Known Caveats and Open Problems

### Clock injection is absent

The scheduler uses `Instant::now()` directly, not an injected clock. This means tests that validate temporal behavior — event confirmation, TTL expiry, backoff duration — must run with real elapsed time. A test validating a 500 ms backoff must actually wait 500 ms. This makes temporal tests inherently slower and introduces flakiness on a loaded CI machine.

The fix is a `Clock` trait that the scheduler accepts, with a `MockClock` in testkit. This is a non-trivial refactor because `Instant` is used in many places across the scheduler and event engine.

### Sequential detector execution creates a bottleneck

All detectors share the same scheduler tick thread. A detector that takes 30 ms on a given tick delays every subsequent detector in that tick by 30 ms, even if their individual FPS budgets would have allowed them to run. Load shedding mitigates this by backing off the slow detector on the next tick, but it does not address the current tick's delay.

In a pipeline where high-cost detectors run at low FPS alongside low-cost detectors at high FPS, this can cause the low-cost detectors to miss their timing windows on ticks where a high-cost detector runs.

### Mutex contention under concurrent post-roll threads

Each event triggers a post-roll thread that eventually locks the frame buffer to call `slice()`. Multiple events in a short window can spawn multiple concurrent post-roll threads. At the end of each post-roll wait, all threads compete for the frame buffer lock simultaneously.

The lock hold time for `slice()` is proportional to the number of frames in the clip window. At 30 fps with a 10-second post-roll, that is 300 frames. On a slow machine, this can introduce scheduler tick jitter at the moment multiple clips mature.

The mitigation would be to add a semaphore limiting concurrent post-roll readers, or to move frame slicing into the encoder worker (accepting a brief lock hold at enqueue time, not at sleep completion).

### Global metric atomics in parallel tests

`rvo-metrics` uses global atomic counters shared across all test threads. In a parallel test run, counter deltas from different tests intermingle. A test that asserts `rvo_frame_drops_total == 2` may see a higher value because another test dropped frames concurrently.

The workaround is `cargo test -- --test-threads=1`. The fix requires per-instance metrics objects rather than global atomics, which is a larger refactor that affects the metric API across all crates.

### Post-roll clip timing requires test accommodation

`ClipManager` spawns a thread that sleeps for `clip_after` before slicing the buffer. Any test that reads `clip_rx` must wait longer than `clip_after` before asserting. This is a real-time dependency in an otherwise deterministic test.

Until clock injection is implemented, tests must use a short but non-zero `clip_after` and sleep at least that long before asserting on clip output.

### Load shedding ignores queue depth

The backoff policy uses only execution latency as input. A detector that runs fast but produces a burst of events can saturate the event publisher channel (capacity 64) without triggering any backoff. Similarly, a ClipManager that spawns many post-roll threads can exhaust the encoder queue (capacity 8) without affecting detector scheduling.

Incorporating downstream queue depth into the shedding policy would make the system more holistically self-limiting under load.

### Static lifetime on `DetectorMeta` output lists

`DetectorMeta.output_signals` and `dependencies` are typed as `&'static [SignalType]`. This is correct for detectors whose signal lists are compile-time constants. For test detectors with dynamic output lists, the only options are:

1. Declare the slice as a module-level `static`.
2. Leak a `Box<[SignalType]>` to obtain a `&'static` reference.

Option 2 is acceptable in test-only code but is a memory leak that grows with the number of test detector instances. If the API is ever extended to support dynamically configured detectors, the lifetime constraint must be relaxed, likely by switching to a `Vec<SignalType>` owned by the `DetectorMeta` struct.

---

## 7. Practical Dev Workflow

### Build and run

```sh
cargo build -p rvo-bin
RVO_CONFIG=config/rvo.yaml cargo run -p rvo-bin
```

### Observe the running system

```sh
curl http://127.0.0.1:9090/metrics
curl http://127.0.0.1:9090/health
tail -f events.jsonl          # if event_log is set in config
ls clips/                     # evidence output directories
```

### Run tests

```sh
cargo test --workspace
cargo test -p rvo-scenarios   # integration scenarios only
cargo test -- --test-threads=1  # if metric assertions are sensitive to parallel runs
```

### Add a detector

1. Implement `DetectorNode` in `rvo-detector` (production) or `rvo-testkit` (test-only).
2. Declare `meta()` with `id`, `max_fps`, `output_signals`, `dependencies`, `cost_hint`, and `requires_frame`.
3. Wire it into `rvo-bin` or config.
4. Write a scenario test in `rvo-scenarios` if it affects event behavior.

### Add a signal type

1. Extend `SignalType` in `rvo-signals`.
2. Update condition builders or event definitions that reference the new type.
3. Verify freshness and TTL semantics in a scenario test.

### Add an event rule

1. Define `EventDefinition` with `condition`, `duration_ns`, and `cooldown_ns`.
2. Register it in `EventEngine::new_many(...)`.
3. Write a scenario that validates both the confirmation duration and the cooldown.

---

## 8. Repo Structure

```
Cargo.toml                      workspace members
config/rvo.yaml                 runtime config
crates/
  rvo-bin/                      entrypoint
  rvo-config/                   YAML loading
  rvo-core/                     Frame, time primitives
  rvo-camera/                   capture
  rvo-buffer/                   circular frame buffer
  rvo-detector/                 trait and synthetic detectors
  rvo-signals/                  typed signal store
  rvo-events/                   condition DSL, event engine, publishers
  rvo-scheduler/                orchestration loop
  rvo-clips/                    evidence pipeline
  rvo-metrics/                  Prometheus counters, HTTP
  rvo-testkit/                  test-only infrastructure
  rvo-scenarios/                integration tests
docs/
  ARCHITECTURE.md               design decisions and data flow
  CURRENT_IMPLEMENTATION.md     implementation status
  ROADMAP.md                    phased work plan
  ISSUES_AND_LEFTOVERS.md       open issues and triage
  DEVELOPER_GUIDE.md            this file
```

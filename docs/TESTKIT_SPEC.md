# RVO Testkit — Implementation Spec

This document is a self-contained implementation brief for building the RVO
test infrastructure. It is written for an AI coding agent (Codex, Claude Code,
etc.) that has access to the repository but has not read the full session
history. Follow the sections in order — earlier sections define types that
later sections depend on.

> Note: all changes described in this spec have now been implemented in the repository.

---

## 0. Context: What Exists Today

### Workspace layout

```
RVO/
├── Cargo.toml                  (workspace root, resolver = "2")
├── config/rvo.yaml
├── docs/
├── crates/
│   ├── rvo-bin/                entrypoint, runtime wiring
│   ├── rvo-buffer/             bounded circular frame buffer
│   ├── rvo-camera/             OpenCV capture + existing mock
│   ├── rvo-clips/              clip job pipeline, JPEG encoder
│   ├── rvo-config/             YAML config
│   ├── rvo-core/               shared time + reserved frame module
│   ├── rvo-detector/           DetectorNode trait + synthetic detectors
│   ├── rvo-events/             condition DSL, event engine, publishers
│   ├── rvo-metrics/            atomic counters, HTTP endpoints
│   ├── rvo-scheduler/          orchestration loop, load shedding
│   └── rvo-signals/            typed signal store
```

### Key types — read these carefully before writing any code

**`rvo_buffer::Frame`** (`crates/rvo-buffer/src/buffer.rs`):
```rust
#[derive(Clone)]
pub struct Frame {
    pub ts: Instant,
    pub id: u64,
    pub image: opencv::core::Mat,
}
```

**`rvo_buffer::FrameBuffer`** — circular buffer, capacity set at construction.
- `push(frame)` — O(1), overwrites oldest.
- `slice(start: Instant, end: Instant) -> Vec<Frame>` — timestamp-filtered,
  sorted ascending by `ts`.
- `newest_frame() -> Option<Frame>`
- `newest_instant() -> Option<Instant>`

**`rvo_signals::store::SignalType`** (enum, 4 variants):
```rust
pub enum SignalType { Dummy, MotionLevel, FacePresent, PersonDetected }
```

**`rvo_signals::store::Signal`**:
```rust
pub struct Signal {
    pub signal_type: SignalType,
    pub value: u64,
    pub ts_ns: u64,   // monotonic ns since scheduler started_at
    pub ttl_ns: u64,  // duration; expires when ts_ns + ttl_ns < now_ns
}
```

**`rvo_detector::detector::DetectorNode`** (trait):
```rust
pub trait DetectorNode: Send {
    fn meta(&self) -> DetectorMeta;
    fn execute(&mut self, ctx: &DetectorContext<'_>) -> DetectorResult;
    // default methods: id(), max_fps(), dependencies(), output_signals(),
    //                  cost_hint(), requires_frame() — all delegate to meta()
}
```

**`rvo_detector::detector::DetectorMeta`**:
```rust
pub struct DetectorMeta {
    pub id: &'static str,
    pub max_fps: f64,
    pub dependencies: &'static [SignalType],
    pub output_signals: &'static [SignalType],
    pub cost_hint: DetectorCostHint,  // Low | Medium | High
    pub requires_frame: bool,
}
```

**`rvo_detector::detector::DetectorContext<'a>`**:
```rust
pub struct DetectorContext<'a> {
    pub now_ns: u64,
    pub frame: Option<&'a Frame>,
}
```

**`rvo_detector::detector::DetectorResult`**:
```rust
pub struct DetectorResult {
    pub signals: Vec<Signal>,
    pub health: DetectorHealth,  // Ok | Failed
}
```

**`rvo_events::EventDefinition`**:
```rust
pub struct EventDefinition {
    pub event_type: EventType,    // DummyEvent (only variant today)
    pub condition: Condition,     // All(Vec<SignalPredicate>) | Any(...)
    pub duration_ns: u64,
    pub cooldown_ns: u64,
}
```

**`rvo_events::Condition`**:
```rust
pub enum Condition {
    All(Vec<SignalPredicate>),
    Any(Vec<SignalPredicate>),
}
impl Condition {
    pub fn single_gte(signal_type: SignalType, value: u64) -> Self { ... }
    pub fn evaluate(&self, signals: &SignalStore, now_ns: u64) -> bool { ... }
}
```

**`rvo_events::Event`** (`#[derive(Serialize)]`):
```rust
pub struct Event {
    pub event_type: EventType,
    pub ts_ns: u64,
    pub confidence: f64,  // [0.0, 1.0]
}
```

**`rvo_metrics::METRICS`** — global `Lazy<Metrics>`, all fields are `AtomicU64`:
```
scheduler_ticks, detector_execs, detector_skips, detector_failures,
detector_exec_ns_total, events_emitted, frame_drops, clip_drops, event_drops
```

**`rvo_scheduler::scheduler::Scheduler::new`** signature:
```rust
pub fn new(
    detectors: Vec<Box<dyn DetectorNode>>,
    event_engine: EventEngine,
    frame_rx: Receiver<Frame>,
    clip_manager: ClipManager,
    event_publisher: EventPublisher,
    frame_buffer: Arc<Mutex<FrameBuffer>>,
) -> Self
```

**`rvo_clips::ClipManager::new`** signature:
```rust
pub fn new(
    tx: Sender<(ClipJob, Vec<Frame>)>,
    before: Duration,
    after: Duration,
    buffer: Arc<Mutex<FrameBuffer>>,
) -> Self
```

### What already exists for testing

- `crates/rvo-camera/src/mock.rs` — `start_mock_camera(tx: Sender<Frame>)`
  sends `Frame { image: Mat::default() }` at 30fps in a loop. **Empty frames
  only.** Kept for backward compat but superseded by `SyntheticCamera` below.
- `crates/rvo-scheduler/src/scheduler.rs` — one `#[test]` `scheduler_runs_without_blocking`
  that runs 100 ticks and asserts no panic.
- Unit tests in `rvo-signals`, `rvo-buffer`, `rvo-events` (condition + engine).

### What is missing

1. **Synthetic camera with real pixel data** — `Mat::default()` breaks any
   detector that reads image data.
2. **Configurable mock detectors** — `DummyDetector` always returns value=1;
   cannot simulate probabilistic output, failure, latency, or dependency chains.
3. **Event capture and metrics assertion helpers** — no way to collect emitted
   events or snapshot metric deltas in tests.
4. **Full-pipeline scenario tests** — no test verifies that a specific input
   sequence produces a specific event at a specific time.
5. **Demo binary** — no runnable demo for stakeholder use without real hardware.

---

## 1. New Crate: `rvo-testkit`

### Location
```
crates/rvo-testkit/
├── Cargo.toml
└── src/
    ├── lib.rs
    ├── camera.rs        SyntheticCamera, FileCamera
    ├── detectors.rs     ScriptedDetector, ProbabilisticDetector,
    │                    LatencyDetector, FailingDetector, ChainedDetector
    ├── capture.rs       EventCapture, MetricsSnapshot
    └── pipeline.rs      PipelineBuilder
```

### `Cargo.toml`

```toml
[package]
name = "rvo-testkit"
version = "0.1.0"
edition = "2021"

# This crate is test infrastructure. Never pull it into a production binary.
# Other crates should list it under [dev-dependencies] only.

[dependencies]
crossbeam-channel = "0.5"
opencv = { version = "0.88", default-features = false }
rand = "0.8"
rvo-buffer   = { path = "../rvo-buffer" }
rvo-clips    = { path = "../rvo-clips" }
rvo-detector = { path = "../rvo-detector" }
rvo-events   = { path = "../rvo-events" }
rvo-metrics  = { path = "../rvo-metrics" }
rvo-scheduler = { path = "../rvo-scheduler" }
rvo-signals  = { path = "../rvo-signals" }
```

Add `"crates/rvo-testkit"` to the workspace `members` list in the root
`Cargo.toml`.

---

### 1.1 `src/camera.rs` — Synthetic video sources

#### `SyntheticCamera`

Generates real `Mat` frames at a configurable FPS. Frames have actual pixel
data so detectors that call OpenCV functions do not crash.

```rust
pub enum SyntheticPattern {
    /// Solid fill: all pixels set to (r, g, b).
    SolidColor { r: u8, g: u8, b: u8 },

    /// Alternates between two solid colors every `period_frames` frames.
    /// Simulates a simple motion signal (pixel diff > 0).
    Alternating {
        color_a: (u8, u8, u8),
        color_b: (u8, u8, u8),
        period_frames: u64,
    },

    /// Draws a filled white rectangle on a black background at a fixed
    /// position. Simulates a "face region" or "object bounding box."
    RectOnBlack {
        x: i32, y: i32, w: i32, h: i32,
    },
}

pub struct SyntheticCamera {
    width: i32,
    height: i32,
    fps: f64,
    pattern: SyntheticPattern,
}

impl SyntheticCamera {
    pub fn new(width: i32, height: i32, fps: f64, pattern: SyntheticPattern) -> Self;

    /// Spawn a background thread that generates frames and sends them through
    /// `tx`. Mirrors the `start_camera` / `start_mock_camera` API.
    pub fn start(self, tx: crossbeam_channel::Sender<rvo_buffer::Frame>);
}
```

**Implementation notes:**
- Use `opencv::core::Mat::new_rows_cols_with_default` to allocate frames.
- For `SolidColor`, set all pixels to the given BGR scalar.
- For `Alternating`, toggle color every `period_frames` and increment the
  frame id. The pixel difference between frames is non-zero, which matters for
  any motion-detection detector stub.
- For `RectOnBlack`, start with a black Mat and call `opencv::imgproc::rectangle`
  to draw the white rect.
- Sleep `Duration::from_secs_f64(1.0 / fps)` between frames.
- Use `try_send`; drop on full (same contract as real camera).

#### `FileCamera`

Reads frames from a video file using `VideoCapture::from_file`. Useful for
regression testing with known footage.

```rust
pub struct FileCamera {
    path: String,
    /// If true, loop back to the start when the file ends.
    looping: bool,
}

impl FileCamera {
    pub fn new(path: impl Into<String>, looping: bool) -> Self;
    pub fn start(self, tx: crossbeam_channel::Sender<rvo_buffer::Frame>);
}
```

**Implementation notes:**
- Open `VideoCapture::from_file` inside the spawned thread, not in `new`.
- On end-of-file: if `looping`, re-open; otherwise let the thread exit cleanly
  (the channel will then be the only indicator to the scheduler that no more
  frames are coming).
- On read failure: sleep 10ms and retry up to 5 times, then exit.

---

### 1.2 `src/detectors.rs` — Configurable mock detectors

All detectors here implement `rvo_detector::detector::DetectorNode`.

#### `ScriptedDetector`

Emits specific signal values at specific tick counts. Fully deterministic —
useful for verifying exact event timing without real-time dependencies.

```rust
pub struct ScriptEntry {
    /// Which tick (0-indexed calls to execute()) this entry activates.
    pub tick: u64,
    pub signal_type: SignalType,
    pub value: u64,
    /// TTL to write into the signal. 1_000_000_000 (1s) is a safe default.
    pub ttl_ns: u64,
}

pub struct ScriptedDetector {
    id: &'static str,
    max_fps: f64,
    script: Vec<ScriptEntry>,
    tick_count: u64,
    output_signals: Vec<SignalType>,  // derived from script at construction
}

impl ScriptedDetector {
    /// `id` must be a `'static str` (string literal).
    /// Panics if `script` is empty.
    pub fn new(id: &'static str, max_fps: f64, script: Vec<ScriptEntry>) -> Self;
}
```

**`execute` behaviour:**
1. Increment internal `tick_count`.
2. Collect all `ScriptEntry` where `entry.tick == tick_count - 1` (0-indexed).
3. Emit one `Signal` per matching entry, using `ctx.now_ns` as `ts_ns`.
4. Return `DetectorHealth::Ok`.

**`meta()` behaviour:**
- `requires_frame: false`
- `dependencies: &[]`
- `cost_hint: DetectorCostHint::Low`
- `output_signals`: derive at construction from the unique `SignalType`s in the
  script. Store as a `Vec<SignalType>` and expose via a wrapper that leaks or
  uses a `Box::leak` — see note below.

> **Note on `&'static [SignalType]`:** `DetectorMeta::output_signals` requires
> `&'static [SignalType]`. For test detectors where the signal list is dynamic,
> use `Box::leak(signals.into_boxed_slice())` in `new()` and store the leaked
> reference. This is acceptable in test code.

---

#### `ProbabilisticDetector`

Emits a signal with a configurable probability per execute call. Uses a seeded
RNG for reproducibility.

```rust
pub struct ProbabilisticDetector {
    id: &'static str,
    max_fps: f64,
    signal_type: SignalType,
    /// Probability in [0.0, 1.0] that a signal is emitted per tick.
    emit_probability: f64,
    /// Value to write when emitting.
    value: u64,
    ttl_ns: u64,
    rng: rand::rngs::SmallRng,
}

impl ProbabilisticDetector {
    pub fn new(
        id: &'static str,
        max_fps: f64,
        signal_type: SignalType,
        emit_probability: f64,
        value: u64,
        ttl_ns: u64,
        seed: u64,
    ) -> Self;
}
```

**`execute` behaviour:**
- Draw a `f64` from `rng` in `[0.0, 1.0)`.
- If < `emit_probability`, emit the signal.
- Always return `DetectorHealth::Ok`.

---

#### `LatencyDetector`

Wraps another `Box<dyn DetectorNode>` and adds a configurable sleep before
delegating. Used to precisely trigger the load shedding backoff path.

```rust
pub struct LatencyDetector {
    inner: Box<dyn DetectorNode>,
    latency: Duration,
    /// Optional random jitter added on top of fixed latency.
    jitter: Option<Duration>,
    rng: rand::rngs::SmallRng,
}

impl LatencyDetector {
    pub fn new(
        inner: Box<dyn DetectorNode>,
        latency: Duration,
        jitter: Option<Duration>,
        seed: u64,
    ) -> Self;
}
```

**`execute` behaviour:**
1. Compute total sleep = `latency` + random in `[0, jitter)` if jitter is set.
2. `thread::sleep(total_sleep)`.
3. Delegate to `self.inner.execute(ctx)` and return its result.

**`meta()` behaviour:** delegate entirely to `self.inner.meta()`.

---

#### `FailingDetector`

Returns `DetectorHealth::Ok` for the first `ok_count` executions, then returns
`DetectorHealth::Failed` on execution `ok_count + 1` and never executes again
(the scheduler will disable it).

```rust
pub struct FailingDetector {
    inner: Box<dyn DetectorNode>,
    ok_count: u64,
    exec_count: u64,
}

impl FailingDetector {
    pub fn new(inner: Box<dyn DetectorNode>, ok_count: u64) -> Self;
}
```

**`execute` behaviour:**
- If `exec_count < ok_count`: delegate to inner, return inner's result.
- If `exec_count == ok_count`: return `DetectorResult { signals: vec![], health: DetectorHealth::Failed }`.
- After `ok_count`: unreachable (scheduler disables detector after `Failed`).
- Increment `exec_count` before branching.

---

#### `ChainedDetector`

Declares a signal dependency and only emits its output when that dependency is
fresh. Tests the scheduler's dependency gating without requiring two real
models.

```rust
pub struct ChainedDetector {
    id: &'static str,
    max_fps: f64,
    depends_on: SignalType,
    emits: SignalType,
    emit_value: u64,
    ttl_ns: u64,
}

impl ChainedDetector {
    pub fn new(
        id: &'static str,
        max_fps: f64,
        depends_on: SignalType,
        emits: SignalType,
        emit_value: u64,
        ttl_ns: u64,
    ) -> Self;
}
```

**`meta()` behaviour:**
- `dependencies: Box::leak(vec![self.depends_on].into_boxed_slice())`
- `output_signals: Box::leak(vec![self.emits].into_boxed_slice())`
- `requires_frame: false`
- `cost_hint: DetectorCostHint::Low`

**`execute` behaviour:**
- This is called by the scheduler only when `depends_on` is fresh (the
  scheduler gates on dependencies before calling execute).
- Emit `self.emits` with `self.emit_value`. Return `Ok`.

---

### 1.3 `src/capture.rs` — Assertion helpers

#### `EventCapture`

Collects events from a `Receiver<Event>` into a `Vec` for later assertion.

```rust
pub struct EventCapture {
    rx: crossbeam_channel::Receiver<Event>,
    collected: Vec<Event>,
}

impl EventCapture {
    pub fn new(rx: crossbeam_channel::Receiver<Event>) -> Self;

    /// Drain all currently available events without blocking.
    pub fn drain(&mut self);

    /// Drain, then return the number of collected events.
    pub fn count(&mut self) -> usize;

    /// Drain, then assert exactly `n` events have been collected.
    /// Panics with a descriptive message on mismatch.
    pub fn assert_count(&mut self, n: usize);

    /// Drain, then assert at least one event of `event_type` is present.
    pub fn assert_has_event(&mut self, event_type: EventType);

    /// Drain, then assert NO events have been collected.
    pub fn assert_empty(&mut self);

    /// Return all collected events (call drain first).
    pub fn events(&self) -> &[Event];

    /// Clear collected events (reset for the next assertion window).
    pub fn clear(&mut self);
}
```

---

#### `MetricsSnapshot`

Captures a point-in-time snapshot of `METRICS` for delta assertions.

```rust
#[derive(Clone, Debug)]
pub struct MetricsSnapshot {
    pub scheduler_ticks: u64,
    pub detector_execs: u64,
    pub detector_skips: u64,
    pub detector_failures: u64,
    pub detector_exec_ns_total: u64,
    pub events_emitted: u64,
    pub frame_drops: u64,
    pub clip_drops: u64,
    pub event_drops: u64,
}

impl MetricsSnapshot {
    /// Read current METRICS atomics with Relaxed ordering.
    pub fn capture() -> Self;

    /// Return field-wise delta: `self - earlier`.
    /// All fields use saturating subtraction.
    pub fn delta_since(&self, earlier: &MetricsSnapshot) -> MetricsSnapshot;
}
```

Usage pattern in tests:
```rust
let before = MetricsSnapshot::capture();
// ... run N ticks ...
let after = MetricsSnapshot::capture();
let delta = after.delta_since(&before);
assert_eq!(delta.events_emitted, 1);
assert!(delta.detector_skips > 0);
```

---

### 1.4 `src/pipeline.rs` — PipelineBuilder

Reduces full-pipeline test setup from ~35 lines of boilerplate to ~5.

```rust
pub struct BuiltPipeline {
    pub scheduler: rvo_scheduler::scheduler::Scheduler,
    pub frame_buffer: Arc<Mutex<FrameBuffer>>,
    pub event_capture: EventCapture,
    /// Sender side of the clip channel — drop to shut down encoder, or
    /// ignore it; the encoder channel is bounded(8) by default.
    pub clip_rx: crossbeam_channel::Receiver<(rvo_clips::clip::ClipJob, Vec<Frame>)>,
}

impl BuiltPipeline {
    /// Run `n` ticks with no inter-tick sleep.
    pub fn run_ticks(&mut self, n: u64);

    /// Run ticks until `duration` of real time has elapsed.
    pub fn run_for(&mut self, duration: Duration);

    /// Inject a frame directly into the frame buffer (bypasses camera channel).
    pub fn inject_frame(&self, frame: Frame);
}

pub struct PipelineBuilder {
    detectors: Vec<Box<dyn DetectorNode>>,
    event_defs: Vec<EventDefinition>,
    frame_buffer_capacity: usize,
    frame_channel_capacity: usize,
    clip_before: Duration,
    clip_after: Duration,
}

impl PipelineBuilder {
    pub fn new() -> Self;

    pub fn detector(mut self, d: impl DetectorNode + 'static) -> Self;
    pub fn detectors(mut self, ds: Vec<Box<dyn DetectorNode>>) -> Self;

    pub fn event(mut self, def: EventDefinition) -> Self;

    pub fn frame_buffer_capacity(mut self, n: usize) -> Self;
    pub fn frame_channel_capacity(mut self, n: usize) -> Self;

    pub fn clip_window(mut self, before: Duration, after: Duration) -> Self;

    /// Assemble and return a `BuiltPipeline`.
    /// The event publisher sends to an in-process channel; `EventCapture`
    /// wraps the receiver side for assertion.
    pub fn build(self) -> BuiltPipeline;
}
```

**`PipelineBuilder::build` wiring (implement exactly this):**
```
frame_buffer = Arc::new(Mutex::new(FrameBuffer::new(capacity)))
(frame_tx, frame_rx) = bounded(frame_channel_capacity)   // camera channel
(clip_tx, clip_rx)   = bounded(8)
(event_tx, event_rx) = bounded(256)

clip_manager     = ClipManager::new(clip_tx, before, after, Arc::clone(&frame_buffer))
event_publisher  = EventPublisher::new(event_tx)
event_engine     = EventEngine::new_many(event_defs)
event_capture    = EventCapture::new(event_rx)

scheduler = Scheduler::new(
    detectors, event_engine, frame_rx,
    clip_manager, event_publisher, Arc::clone(&frame_buffer)
)

// Do NOT start encoder worker — clip_rx is returned raw so tests can
// inspect jobs without disk I/O.
```

**Default values:**
- `frame_buffer_capacity`: 300
- `frame_channel_capacity`: 16 (larger than production 5 to avoid drops in fast tests)
- `clip_before / clip_after`: `Duration::from_secs(2)` / `Duration::from_secs(1)`

---

### 1.5 `src/lib.rs`

```rust
pub mod camera;
pub mod capture;
pub mod detectors;
pub mod pipeline;

pub use camera::{FileCamera, SyntheticCamera, SyntheticPattern};
pub use capture::{EventCapture, MetricsSnapshot};
pub use detectors::{
    ChainedDetector, FailingDetector, LatencyDetector,
    ProbabilisticDetector, ScriptedDetector, ScriptEntry,
};
pub use pipeline::{BuiltPipeline, PipelineBuilder};
```

---

## 2. New Crate: `rvo-scenarios`

Integration tests that run the full pipeline and make scenario-level assertions.

### Location
```
crates/rvo-scenarios/
├── Cargo.toml
└── src/
    └── main.rs        (or lib.rs with #[cfg(test)] modules)
```

Use `[[test]]` targets or a `src/lib.rs` with `#[cfg(test)]` mod blocks. Either
works; prefer `src/lib.rs` with test modules so `cargo test -p rvo-scenarios`
runs them all.

### `Cargo.toml`

```toml
[package]
name = "rvo-scenarios"
version = "0.1.0"
edition = "2021"

[lib]
path = "src/lib.rs"

[dev-dependencies]
rvo-testkit  = { path = "../rvo-testkit" }
rvo-events   = { path = "../rvo-events" }
rvo-signals  = { path = "../rvo-signals" }
rvo-detector = { path = "../rvo-detector" }
rvo-metrics  = { path = "../rvo-metrics" }
rvo-buffer   = { path = "../rvo-buffer" }
```

Add `"crates/rvo-scenarios"` to workspace members.

---

### 2.1 Test scenarios — implement all of these

Each scenario is a `#[test]` function. The namespace is:
```rust
#[cfg(test)]
mod scenarios {
    use super::*;
    use rvo_testkit::*;
    use rvo_events::*;
    use rvo_signals::store::SignalType;
    use rvo_detector::detector::*;
    use std::time::Duration;
```

---

#### Scenario 1 — Happy path: event fires after duration

```
Setup:
  - ScriptedDetector: tick 0 → Dummy=1, ttl=5_000_000_000 (5s)
  - EventDefinition: condition=single_gte(Dummy, 1), duration_ns=1_000_000_000, cooldown_ns=5_000_000_000

Run:
  - 30 ticks (at 30fps each tick ≈ 33ms, so ~1s of simulated time)
  - Between ticks, advance now_ns: inject ScriptedDetector output at tick 0
    then let scheduler run naturally

Assert:
  - event_capture.count() == 1
  - event.confidence ≈ 1.0 (within 0.05)
  - delta.events_emitted == 1
```

**Implementation guidance:** The scheduler uses `Instant::now()` internally, so
you cannot inject synthetic time. Instead, run real ticks with a real detector
that emits signals, and assert that after enough real ticks (using
`run_for(Duration::from_millis(1100))`) the event fires. Use a `duration_ns`
of 50_000_000 (50ms) to keep the test fast while still exercising the temporal
logic.

---

#### Scenario 2 — Duration not met: no event

```
Setup:
  - ScriptedDetector: tick 0 → Dummy=1, ttl=200_000_000 (200ms)
  - EventDefinition: duration_ns=500_000_000 (500ms), cooldown_ns=5_000_000_000

Run:
  - run_for(Duration::from_millis(150))  # less than duration

Assert:
  - event_capture.assert_empty()
```

---

#### Scenario 3 — Signal breaks: state machine resets

```
Setup:
  - ScriptedDetector script:
      tick 0  → Dummy=1, ttl=100_000_000 (100ms)
      tick 10 → Dummy=0, ttl=100_000_000   # signal goes below threshold
      tick 20 → Dummy=1, ttl=10_000_000_000
  - EventDefinition: duration_ns=200_000_000 (200ms), cooldown_ns=2_000_000_000

Run:
  - run_for(Duration::from_millis(500))

Assert:
  - At most 1 event (if the second sustained window hits duration)
  - Event ts_ns > 200ms from scheduler start (first window was interrupted)
```

---

#### Scenario 4 — Cooldown enforced

```
Setup:
  - ScriptedDetector: tick 0 → Dummy=1, ttl=10_000_000_000
  - EventDefinition: duration_ns=0, cooldown_ns=300_000_000 (300ms)

Run:
  - Phase 1: run_for(50ms) → one event should fire
  - capture.assert_count(1); capture.clear()
  - Phase 2: run_for(100ms) → still in cooldown
  - capture.assert_empty()
  - Phase 3: run_for(300ms) → cooldown expired
  - capture.assert_count(1)
```

---

#### Scenario 5 — Multi-event independence

```
Setup:
  - ScriptedDetector A: tick 0 → Dummy=1, ttl=10_000_000_000
  - ScriptedDetector B: tick 0 → MotionLevel=100, ttl=10_000_000_000
  - EventDefinition 1: condition=single_gte(Dummy, 1),       duration=0, cooldown=10s
  - EventDefinition 2: condition=single_gte(MotionLevel, 50), duration=0, cooldown=10s

Run:
  - run_for(50ms)

Assert:
  - capture.count() == 2
  - Both EventType::DummyEvent (only one EventType exists today — both events
    are DummyEvent but driven by different signals)
```

---

#### Scenario 6 — All condition: requires both signals

```
Setup:
  - ScriptedDetector: tick 0 → Dummy=1, ttl=10s
  - NO detector for FacePresent
  - EventDefinition: condition=All([Dummy>=1, FacePresent==1]), duration=0, cooldown=10s

Run phase 1:
  - run_for(100ms)
Assert:
  - capture.assert_empty()  # FacePresent never set

Add FacePresent detector to pipeline (not possible after build — instead):
  - Build a second pipeline WITH a ScriptedDetector that also sets FacePresent=1
  - Run phase 2 of that pipeline for 100ms

Assert phase 2:
  - capture.count() == 1
```

---

#### Scenario 7 — Any condition: fires on first matching signal

```
Setup:
  - ScriptedDetector: tick 0 → MotionLevel=200, ttl=10s (Dummy NOT set)
  - EventDefinition: condition=Any([Dummy>=1, MotionLevel>=50]), duration=0, cooldown=10s

Run:
  - run_for(50ms)

Assert:
  - capture.count() == 1
```

---

#### Scenario 8 — Dependency gating

```
Setup:
  - ScriptedDetector: emits Dummy=1 at tick 0, ttl=2s
  - ChainedDetector: depends_on=Dummy, emits=FacePresent, value=1, ttl=2s
  - EventDefinition: condition=single_gte(FacePresent, 1), duration=0, cooldown=10s

Run:
  - run_for(200ms)

Assert:
  - capture.count() == 1
  - delta.detector_execs >= 2  # both ScriptedDetector and ChainedDetector ran
```

---

#### Scenario 9 — Load shedding: High-cost detector backs off

```
Setup:
  - ScriptedDetector: tick 0 → Dummy=1, ttl=10s (cost=Low)
  - LatencyDetector wrapping a LoadDetector or ScriptedDetector:
      latency = Duration::from_millis(200)   # far exceeds 10fps budget of 100ms
      inner detector: max_fps=10, cost=High, emits nothing
  - EventDefinition watching Dummy

Run:
  - before = MetricsSnapshot::capture()
  - run_for(Duration::from_millis(600))
  - after = MetricsSnapshot::capture()
  - delta = after.delta_since(&before)

Assert:
  - delta.detector_skips > 0   # LatencyDetector skipped during backoff
  - delta.events_emitted >= 1  # Dummy event still fires (Low cost, not backed off)
```

---

#### Scenario 10 — Failed health: detector disabled

```
Setup:
  - FailingDetector wrapping ScriptedDetector, ok_count=2
  - ScriptedDetector: always emits Dummy=1
  - EventDefinition: duration=0, cooldown=1s

Run:
  - before = MetricsSnapshot::capture()
  - run_for(Duration::from_millis(500))
  - after  = MetricsSnapshot::capture()
  - delta  = after.delta_since(&before)

Assert:
  - delta.detector_failures == 1
  - delta.detector_execs == 3    # ran ok_count=2 times + 1 failure = 3
  # After failure, detector is disabled — no further execs in the remaining time
```

---

#### Scenario 11 — Frame drops under camera pressure

```
Setup:
  - SyntheticCamera: SolidColor, fps=300 (10× scheduler tick rate)
  - PipelineBuilder: frame_channel_capacity=5 (tight)
  - No real detectors needed

Run:
  - before = MetricsSnapshot::capture()
  - SyntheticCamera.start(frame_tx)
  - run_for(Duration::from_millis(200))
  - after = MetricsSnapshot::capture()
  - delta = after.delta_since(&before)

Assert:
  - delta.frame_drops > 0
  - delta.scheduler_ticks > 0
```

---

#### Scenario 12 — Post-roll frames captured

```
Setup:
  - SyntheticCamera: Alternating colors, fps=30
  - ScriptedDetector: tick 0 → Dummy=1, ttl=10s
  - EventDefinition: duration=0, cooldown=10s
  - PipelineBuilder: clip_before=200ms, clip_after=200ms

Run:
  - start SyntheticCamera into frame_buffer via inject_frame loop
    OR: start camera normally and wait for buffer to fill
  - run_for(50ms) to trigger event
  - capture.assert_count(1)
  - Wait 250ms (> clip_after=200ms) for post-roll thread to complete
  - Drain clip_rx

Assert:
  - clip_rx has at least one (ClipJob, Vec<Frame>) received
  - frames.len() > 0
  - At least one frame timestamp >= event_ts (post-roll frame)
```

> Note: `ClipJob.event_ts_ns` is a monotonic u64. Compare frame timestamps
> relative to `Instant::now()` and the scheduler's `started_at`. The simplest
> assertion is `frames.len() > 0` and the clip job's `start_ts < end_ts`.

---

## 3. Move `mock.rs` out of `rvo-camera`

`crates/rvo-camera/src/mock.rs` currently ships in the production library.
It should become test-only.

**Steps:**
1. Delete `crates/rvo-camera/src/mock.rs`.
2. Remove `pub mod mock;` from `crates/rvo-camera/src/lib.rs`.
3. Move the `start_mock_camera` function into `rvo-testkit/src/camera.rs`
   alongside `SyntheticCamera`. Keep the same signature:
   ```rust
   pub fn start_mock_camera(tx: crossbeam_channel::Sender<rvo_buffer::Frame>)
   ```
4. Update the import in `crates/rvo-scheduler/src/scheduler.rs` test:
   ```rust
   // Old:
   use rvo_camera::mock::start_mock_camera;
   // New:
   use rvo_testkit::start_mock_camera;
   ```
5. Add `rvo-testkit` to `crates/rvo-scheduler/Cargo.toml` `[dev-dependencies]`.

---

## 4. New Examples

### Location
```
examples/
├── synthetic_demo.rs
└── rtsp_demo.rs
```

Add to root `Cargo.toml`:
```toml
[[example]]
name = "synthetic_demo"

[[example]]
name = "rtsp_demo"
```

Both examples use the actual `rvo-bin` wiring (not `PipelineBuilder`) so they
demonstrate the production code path.

---

### 4.1 `examples/synthetic_demo.rs`

A full-pipeline demo that runs without any real camera hardware.

**Behaviour:**
1. Start metrics server on `127.0.0.1:9091` (different port to avoid conflict
   with a running production instance).
2. Create `Arc<Mutex<FrameBuffer>>`.
3. Start `SyntheticCamera` with `Alternating { color_a: (0,0,0), color_b: (255,255,255), period_frames: 15 }` at 30fps, 640×480.
4. Build a `ScriptedDetector` with an infinite repeating script: emit `Dummy=1`
   every tick with `ttl=2_000_000_000` (2s). The simplest approach is to
   override `execute` to always emit rather than using a finite script.
   Alternatively, use `ProbabilisticDetector` with `emit_probability=1.0`.
5. Build `EventDefinition`: `single_gte(Dummy, 1)`, `duration_ns=3_000_000_000`
   (3s), `cooldown_ns=5_000_000_000` (5s).
6. Wire clips to `clips/synthetic/` directory.
7. Wire event log to `events_synthetic.jsonl`.
8. Build scheduler, enter tick loop with 1ms sleep.
9. Print `[DEMO] Running synthetic pipeline — Ctrl-C to stop` on startup.

**Clip directory:** create `clips/synthetic/` if absent.

---

### 4.2 `examples/rtsp_demo.rs`

Accepts a URI as a CLI argument and runs the production pipeline on it.

**Behaviour:**
1. Read URI from `std::env::args().nth(1)`. If absent, print usage and exit.
2. Start metrics server on `127.0.0.1:9092`.
3. Start `CameraSource::Uri(uri)` via `start_camera`.
4. Use the same detector + event config as `synthetic_demo`.
5. Wire clips to `clips/rtsp/` and events to `events_rtsp.jsonl`.
6. Tick loop with 1ms sleep.

**Usage:**
```sh
cargo run --example rtsp_demo -- rtsp://user:pass@192.168.1.100:554/stream
cargo run --example rtsp_demo -- /path/to/test.mp4
```

---

## 5. CI update

Update `.github/workflows/ci.yml` to include the new crates:

```yaml
- name: cargo test (all including scenarios)
  run: cargo test --workspace

- name: cargo check examples
  run: cargo check --examples
```

The existing `cargo test --workspace` already covers `rvo-testkit` and
`rvo-scenarios` once they are in the workspace members list. No other CI
changes needed.

---

## 6. Implementation order

Follow this order to avoid broken intermediate states:

1. **`rvo-testkit`** — implement `capture.rs` first (no dependencies on other
   testkit modules), then `detectors.rs`, then `camera.rs`, then `pipeline.rs`.
   `pipeline.rs` depends on all of the above.

2. **Move `mock.rs`** — do this immediately after `rvo-testkit/src/camera.rs`
   is done so the import in `rvo-scheduler` tests can be updated atomically.

3. **`rvo-scenarios`** — implement after `rvo-testkit` is complete. Start with
   scenarios 1–5 (no frame data needed), then 6–8 (condition DSL), then 9–10
   (load shedding / health), then 11–12 (camera pressure / post-roll).

4. **Examples** — implement last, after scenarios pass. They use production
   wiring, so they implicitly test that everything compiles together.

5. **`cargo test --workspace`** — run this after each step and fix any
   compilation errors before proceeding.

---

## 7. Key constraints and gotchas

**`&'static [SignalType]` in `DetectorMeta`** — mock detectors with runtime-
determined signal lists must use `Box::leak`. This is intentional test-only
code. Do not use `Box::leak` in production detectors.

**`Instant`-based timing** — the scheduler uses `Instant::now()` internally;
there is no clock injection. Tests that verify temporal behavior (duration,
cooldown) must use real elapsed time. Keep `duration_ns` values small
(50–200ms) so tests finish quickly. Do not use `thread::sleep` inside test
functions except where testing post-roll capture.

**Metrics are global atomics** — `METRICS` is a process-global `Lazy<Metrics>`.
In tests that check metric deltas, always take a `before` snapshot at the
start of the scenario and compute `delta_since`. Do not assert on absolute
values. Run tests with `cargo test -- --test-threads=1` if metric
cross-contamination between parallel tests is observed.

**OpenCV Mat in tests** — `Mat::default()` is a valid empty Mat. For tests that
do not exercise frame content (most unit tests), use `Mat::default()`. For
tests that verify frame-reading detectors, use `SyntheticCamera` which
produces real pixel data.

**Post-roll thread timing** — `ClipManager::on_event` spawns a thread that
sleeps `clip_after` before slicing the buffer. In scenario 12, the test must
`thread::sleep` for longer than `clip_after` before checking `clip_rx`.
`Duration::from_millis(clip_after_ms + 100)` is sufficient.

**`rvo-scenarios` produces no binary** — use `[lib]` not `[[bin]]` so
`cargo test` runs the tests without needing a `main` function.

**Encoder worker not started in `PipelineBuilder`** — `clip_rx` is returned
raw. Tests can inspect `ClipJob` metadata without writing JPEG files to disk.
This keeps scenario tests hermetic and fast.

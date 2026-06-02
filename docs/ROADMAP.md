# Roadmap

This roadmap turns the current codebase into the broader RVO platform.
For concrete open issues in the current code, see
[Issues And Leftovers](ISSUES_AND_LEFTOVERS.md).

## Phase 1: Correctness And Core Runtime — COMPLETE

All items in this phase are resolved:

- Scheduler monotonic time (stable `started_at` origin).
- Signal TTL semantics normalised (duration, not absolute expiry).
- Event duration and cooldown tests correct.
- Zero-duration events handled (confidence = 1.0, no divide-by-zero).
- Config reload non-fatal (preserves live runtime on error).
- Clip manager safe on empty frame buffer.
- Events metric incremented on emit.
- Frame buffer slice chronologically ordered.
- All crates in workspace.
- SIGHUP reload Unix-only with platform guard.

## Phase 2: Real Detector Runtime Contract — COMPLETE

- `DetectorNode::meta()` exposes id, max_fps, dependencies, output signals,
  cost hint, frame requirement.
- `DetectorContext` carries `frame: Option<&Frame>`.
- Scheduler gates on frame availability and signal dependency freshness.
- `DetectorHealth::Failed` disables detectors.
- Aggregate execution latency tracked.

## Phase 3: Multi-Signal Store — COMPLETE

- `SignalType` has typed slots: `Dummy`, `MotionLevel`, `FacePresent`,
  `PersonDetected`.
- O(1) read and write by type index.
- TTL freshness check per slot.
- Seqlock-style version counter guards concurrent read safety.

## Phase 4: Condition DSL And Event Definitions — IN PROGRESS

Completed:
- `Condition` type: `All(Vec<SignalPredicate>)` and `Any(Vec<SignalPredicate>)`.
- `CompareOp`: Gte, Gt, Eq, Lt, Lte.
- Multiple `EventDefinition`s update independently.
- `EventEngine::update()` returns `Vec<Event>`.
- `Condition::single_gte()` shorthand for backward compat.

Remaining:
- Config YAML `condition:` block parsing (currently only `signal_type` +
  `signal_threshold` shorthand is parsed from YAML; the DSL types exist
  programmatically and can be used by embedders).
- Additional `EventType` variants beyond `DummyEvent`.
- Event confidence model beyond simple elapsed/duration ratio.
- Event IDs and session correlation.

## Phase 5: Real Evidence Pipeline — IN PROGRESS

Completed:
- Post-roll capture via `Arc<Mutex<FrameBuffer>>` shared with `ClipManager`.
- Spawned post-roll thread waits `after` duration before slicing buffer.
- JPEG frame output via `opencv::imgcodecs::imwrite`.
- `meta.json` sidecar per clip.
- Clip drop metric.

Remaining:
- Video muxing (MP4/MKV via GStreamer or ffmpeg-sys).
- Frame drop accounting within a clip (detect gaps in the slice).
- Encoder latency metric.
- Post-roll accuracy beyond 10 s (buffer size vs. post-roll window).
- Thumbnail extraction.

## Phase 6: External Interfaces — IN PROGRESS

Completed:
- JSON-lines file sink (`start_event_file_sink`).
- `/health` endpoint.
- `RVO_CONFIG` env var for config path.
- RTSP/URI camera source.
- Drop metrics at every bounded queue.

Remaining:
- Local IPC or Unix socket event push.
- HTTP pull endpoint for latest events.
- Control API: enable/disable detectors, adjust FPS, trigger reload.
- Graceful shutdown (drain clips, flush event log, join threads).
- `/ready` endpoint (camera alive, scheduler alive, not just process alive).

## Phase 7: Load Shedding And Scheduling Policy — IN PROGRESS

Completed:
- Per-detector cost-aware backoff on overrun (Medium: 100 ms, High: 500 ms).
- `rvo_detector_skip_total` counts backoff skips.

Remaining:
- Per-detector execution latency histograms (not just aggregate).
- Queue pressure as a shedding input (clip queue depth, event queue depth).
- Explicit FPS reduction for degraded-but-not-failed detectors.
- Overload state metric visible in `/metrics`.

## Phase 8: Multi-Stream And Distributed Runtime — NOT STARTED

- Multiple camera or RTSP sources with per-stream scheduler instances.
- Detector worker pools with process isolation for heavy models.
- GPU/model runtime abstraction (CUDA, CoreML, TensorRT).
- Remote detector workers where latency permits.
- Shared event schema and session IDs across streams.
- Central metrics collection.
- Distributed detector health and reload propagation.

The distributed architecture preserves the core RVO contracts: bounded message
paths, freshness-aware scheduling, typed signals, temporal event confirmation,
best-effort evidence extraction.

## Platform Hardening (Cross-Phase)

These apply across all phases:

- **Structured logging** — replace `println!` / `eprintln!` with `tracing`
  or `log` + a JSON formatter.
- **Per-detector config schemas** — currently only `busy_ns` is a detector-
  level config field.
- **Durable metadata storage** — event records are currently ephemeral
  (log + file). A durable store (SQLite, append-only file, obj
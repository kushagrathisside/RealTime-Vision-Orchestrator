# RVO: Realtime Video Orchestration

RVO is low-latency data infrastructure for realtime video AI systems.

It is designed for applications where live video streams need to be processed by
multiple AI or deep-learning models without allowing slow inference, storage,
encoding, or downstream consumers to stall the realtime path.

In short:

> RVO is a realtime orchestration runtime for video streams. It ingests live
> streams, schedules models under latency budgets, moves results through bounded
> message paths, fuses model outputs into temporal events, and emits clips or
> metadata without blocking the live pipeline.

## Why RVO Exists

Modern video AI systems are not limited only by model accuracy. In production,
they are often limited by orchestration:

- Frame backlogs create stale inference.
- Slow models block fast models.
- Unbounded queues hide latency until the system falls behind.
- Ad hoc glue code makes detector dependencies hard to reason about.
- One-frame detections create noisy events.
- Clip extraction and storage compete with realtime inference.
- Production teams lack clear metrics for latency, drops, and overload.

RVO treats realtime video AI as a data systems problem. Instead of asking every
application team to reinvent capture loops, model schedulers, signal freshness,
event debouncing, and evidence capture, RVO provides a common runtime for those
concerns.

## Product Framing

```
Realtime Frame Bus
+ Model Orchestrator
+ Signal Store
+ Temporal Event Engine
+ Evidence Pipeline
+ Observability Layer
```

Or more simply:

> Message Queue + Stream Processor + Model Scheduler + Temporal Event Engine
> for realtime video AI.

RVO does not aim to replace Kafka, Ray, GStreamer, or model-serving systems.
It sits in the missing middle layer for live video inference.

## Core Principles

**Bounded Everything** — Frame buffers, channels, job queues, and signal state
have fixed bounds. RVO does not hide overload behind unbounded memory growth.

**No Stale Work** — Old frames are less valuable than fresh frames. If the
system falls behind, RVO drops stale work instead of processing it late.

**Time-Aware Scheduling** — Models run according to time budgets, FPS caps,
dependency freshness, and load policy. The goal is not to run every model on
every frame; it is to run the right model at the right time with bounded latency.

**Signal-Oriented Modularity** — Models emit typed signals that other modules
consume if the signals are still fresh. Models do not call each other directly.

**Temporal Correctness** — Events are confirmed over time, not triggered by
every raw detection. A sustained condition is more useful than a noisy spike.

**Failure Isolation** — Slow encoding, disk writes, or downstream consumers
never block capture or scheduling. Evidence is best-effort.

**Production Observability** — A realtime runtime proves its behavior with
metrics: frame drops, model latency, scheduler ticks, event counts, clip
pipeline health, and queue pressure.

## Target Use Cases

- Retail analytics
- Smart cameras and edge AI
- Industrial safety monitoring
- Warehouse and logistics
- Traffic analytics
- Sports highlight generation
- Robotics perception
- Healthcare safety
- Proctoring and interview monitoring

The common pattern: multiple models run over live video, their outputs are fused
over time, and the application cannot afford unbounded latency.

## The Runtime Contract

1. Ingest live frames without blocking capture.
2. Keep a bounded rolling memory of recent frames.
3. Schedule model nodes using time-aware policies with load shedding.
4. Publish model outputs as fresh typed signals.
5. Evaluate configurable conditions over signals; confirm events temporally.
6. Extract evidence asynchronously without stalling the live path.
7. Expose metrics that make realtime behavior measurable.

## Current Status

This repository contains a working Rust implementation of the RVO runtime:

- OpenCV camera capture with RTSP/URI support.
- Bounded frame channel and circular frame buffer.
- Typed signal store (Dummy, MotionLevel, FacePresent, PersonDetected).
- Time-gated detector execution with dependency gating.
- Composable condition DSL (`All`/`Any` over typed signal predicates).
- Temporal event engine (Idle → Potential → Confirmed → Cooldown).
- Cost-aware load shedding with per-detector backoff.
- Post-roll clip capture via shared frame buffer.
- JPEG frame output + JSON metadata sidecar per clip.
- Prometheus-style metrics and `/health` endpoint.
- JSON-lines event file sink.
- YAML config with hot-reload on SIGHUP (Unix).
- CI workflow (check, test, clippy, fmt).

See [Current Implementation](CURRENT_IMPLEMENTATION.md) for the precise
behavior of the running code.

See [Roadmap](ROADMAP.md) for what is complete and what remains.

See [Issues And Leftovers](ISSUES_AND_LEFTOVERS.md) for the concrete open item
triage list.

## Quick Start

```sh
# Build (requires Rust stable and OpenCV system libraries)
cargo build -p rvo-bin

# Run with default config
cargo run -p rvo-bin

# Run with a custom config
RVO_CONFIG=/path/to/config.yaml cargo run -p rvo-bin

# Observe
curl http://127.0.0.1:9090/metrics
curl http://127.0.0.1:9090/health
tail -f events.jsonl      # if event_log is set in config
ls clips/                 # evidence output
```

## Architecture

See [Architecture](ARCHITECTURE.md) for the full syste
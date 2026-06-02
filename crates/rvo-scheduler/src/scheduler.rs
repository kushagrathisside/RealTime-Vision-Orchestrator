use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rvo_detector::detector::{
    DetectorContext,
    DetectorCostHint,
    DetectorHealth,
    DetectorNode,
};
use rvo_signals::store::SignalStore;
use rvo_events::{
    EventEngine,
    EventPublisher,
    EventDefinition,
    EventType,
    Condition,
};
use rvo_metrics::METRICS;
use std::sync::atomic::Ordering;
use rvo_buffer::{Frame, FrameBuffer};
use crossbeam_channel::Receiver;
use rvo_clips::ClipManager;

// When a detector overruns its FPS budget by this factor, it is placed in
// backoff. A factor of 2 means: if a 30fps detector takes > 66ms it backs off.
const OVERRUN_FACTOR: f64 = 2.0;

struct DetectorRuntime {
    last_run: Instant,
    disabled: bool,
    /// When set, the detector is skipped until this instant passes.
    backoff_until: Option<Instant>,
}

impl DetectorRuntime {
    fn new(now: Instant) -> Self {
        Self { last_run: now, disabled: false, backoff_until: None }
    }

    fn is_in_backoff(&self, now: Instant) -> bool {
        self.backoff_until.map_or(false, |until| now < until)
    }

    fn apply_backoff(&mut self, cost: DetectorCostHint, now: Instant) {
        let duration = match cost {
            DetectorCostHint::Low    => return, // never back off low-cost detectors
            DetectorCostHint::Medium => Duration::from_millis(100),
            DetectorCostHint::High   => Duration::from_millis(500),
        };
        self.backoff_until = Some(now + duration);
    }
}

pub struct Scheduler {
    detectors: Vec<Box<dyn DetectorNode>>,
    runtime: Vec<DetectorRuntime>,
    started_at: Instant,
    signal_store: SignalStore,
    event_engine: EventEngine,
    /// Shared with ClipManager so post-roll threads can read frames after the
    /// scheduler has moved on.
    frame_buffer: Arc<Mutex<FrameBuffer>>,
    frame_rx: Receiver<Frame>,
    clip_manager: ClipManager,
    event_publisher: EventPublisher,
}

impl Scheduler {
    pub fn frame_slice(&self, start: Instant, end: Instant) -> Vec<Frame> {
        self.frame_buffer.lock().unwrap().slice(start, end)
    }

    pub fn new(
        detectors: Vec<Box<dyn DetectorNode>>,
        event_engine: EventEngine,
        frame_rx: Receiver<Frame>,
        clip_manager: ClipManager,
        event_publisher: EventPublisher,
        frame_buffer: Arc<Mutex<FrameBuffer>>,
    ) -> Self {
        let now = Instant::now();
        let runtime = detectors.iter().map(|_| DetectorRuntime::new(now)).collect();

        Self {
            detectors,
            runtime,
            started_at: now,
            signal_store: SignalStore::new(),
            event_engine,
            frame_buffer,
            frame_rx,
            clip_manager,
            event_publisher,
        }
    }

    pub fn tick(&mut self) {
        // Drain new frames without holding the lock across the rest of tick.
        {
            let mut buf = self.frame_buffer.lock().unwrap();
            while let Ok(frame) = self.frame_rx.try_recv() {
                buf.push(frame);
            }
        }

        METRICS.scheduler_ticks.fetch_add(1, Ordering::Relaxed);

        let now    = Instant::now();
        let now_ns = now.duration_since(self.started_at).as_nanos() as u64;
        let latest_frame = self.frame_buffer.lock().unwrap().newest_frame();

        for (i, detector) in self.detectors.iter_mut().enumerate() {
            // --- Gate 1: permanently disabled by Failed health ---
            if self.runtime[i].disabled {
                METRICS.detector_skips.fetch_add(1, Ordering::Relaxed);
                continue;
            }

            // --- Gate 2: FPS cap ---
            let min_interval = Duration::from_secs_f64(1.0 / detector.max_fps());
            if now.duration_since(self.runtime[i].last_run) < min_interval {
                METRICS.detector_skips.fetch_add(1, Ordering::Relaxed);
                continue;
            }

            // --- Gate 3: load-shedding backoff ---
            if self.runtime[i].is_in_backoff(now) {
                METRICS.detector_skips.fetch_add(1, Ordering::Relaxed);
                continue;
            }

            // --- Gate 4: frame requirement ---
            if detector.requires_frame() && latest_frame.is_none() {
                METRICS.detector_skips.fetch_add(1, Ordering::Relaxed);
                continue;
            }

            // --- Gate 5: signal dependency freshness ---
            let dependencies_fresh = detector
                .dependencies()
                .iter()
                .all(|dep| self.signal_store.get(*dep, now_ns).is_some());

            if !dependencies_fresh {
                METRICS.detector_skips.fetch_add(1, Ordering::Relaxed);
                continue;
            }

            // --- Execute ---
            let ctx = DetectorContext { now_ns, frame: latest_frame.as_ref() };
            let exec_start = Instant::now();
            let result     = detector.execute(&ctx);
            let elapsed_ns = exec_start.elapsed().as_nanos().min(u64::MAX as u128) as u64;

            METRICS.detector_execs.fetch_add(1, Ordering::Relaxed);
            METRICS.detector_exec_ns_total.fetch_add(elapsed_ns, Ordering::Relaxed);

            self.runtime[i].last_run = now;

            // --- Health: disable on Failed ---
            if result.health == DetectorHealth::Failed {
                self.runtime[i].disabled = true;
                METRICS.detector_failures.fetch_add(1, Ordering::Relaxed);
                continue;
            }

            // --- Load shedding: backoff on cost overrun ---
            let budget_ns = (min_interval.as_nanos() as f64 * OVERRUN_FACTOR) as u64;
            if elapsed_ns > budget_ns {
                self.runtime[i].apply_backoff(detector.cost_hint(), now);
            }

            // --- Store produced signals ---
            for sig in result.signals {
                self.signal_store.upsert(sig);
            }
        }

        // --- Event evaluation ---
        for event in self.event_engine.update(now_ns, &self.signal_store) {
            METRICS.events_emitted.fetch_add(1, Ordering::Relaxed);
            self.event_publisher.publish(&event);
            self.clip_manager.on_event(&event);
        }
    }

    pub fn swap_runtime(
        &mut self,
   
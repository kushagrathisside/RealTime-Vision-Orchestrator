use once_cell::sync::Lazy;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct Metrics {
    // Scheduler
    pub scheduler_ticks: AtomicU64,

    // Detectors
    pub detector_execs: AtomicU64,
    pub detector_skips: AtomicU64,
    pub detector_failures: AtomicU64,
    pub detector_exec_ns_total: AtomicU64,

    // Events
    pub events_emitted: AtomicU64,

    // Drop counters — incremented by ClipManager, EventPublisher, and Camera
    // when a bounded queue is full and work is shed.
    pub frame_drops: AtomicU64,
    pub clip_drops: AtomicU64,
    pub event_drops: AtomicU64,
}

pub static METRICS: Lazy<Metrics> = Lazy::new(|| Metrics {
    scheduler_ticks: AtomicU64::new(0),
    detector_execs: AtomicU64::new(0),
    detector_skips: AtomicU64::new(0),
    detector_failures: AtomicU64::new(0),
    detector_exec_ns_total: AtomicU64::new(0),
    events_emitted: AtomicU64::new(0),
    frame_drops: AtomicU64::new(0),
    clip_drops: AtomicU64::new(0),
    event_drops: AtomicU64::new(0),
});

pub fn render_prometheus() -> String {
    format!(
        "\
rvo_scheduler_ticks {}\n\
rvo_detector_exec_total {}\n\
rvo_detector_skip_total {}\n\
rvo_detector_failure_total {}\n\
rvo_detector_exec_ns_total {}\n\
rvo_events_emitted_total {}\n\
rvo_frame_drops_total {}\n\
rvo_clip_drops_total {}\n\
rvo_event_drops_total {}\n",
        METRICS.scheduler_ticks.load(Ordering::Relaxed),
        METRICS.detector_execs.load(Ordering::Relaxed),
        METRICS.detector_skips.load(Ordering::Relaxed),
        METRICS.detector_failures.load(Ordering::Relaxed),
        METRICS.detector_exec_ns_total.load(Ordering::Relaxed),
        METRICS.events_emitted.load(Ordering::Relaxed),
        METRICS.frame_drops.load(Ordering::Relaxed),
        METRICS.clip_drops.load(Ordering::Relaxed),
        METRICS.event_drops.load(Ordering::Relaxed),
    )
}

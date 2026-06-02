use std::sync::atomic::{AtomicU64, Ordering};
use once_cell::sync::Lazy;

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
    scheduler_ticks:       AtomicU64::new(0),
    detector_execs:        AtomicU64::new(0),
    detector_skips:        AtomicU64::new(0),
    detector_failures:     AtomicU64::new(0),
    detector_exec_ns_total: AtomicU64::new(0),
    events_emitted:        AtomicU64::new(0),
    frame_drops:           AtomicU64::new(0),
    clip_drops:            AtomicU64::new(0),
    event_drops:           AtomicU64::new(0),
});

pub fn render_prometheus() -> String {
    format!(
        "\
rvo_scheduler_ticks {}\n\
rvo_detector_exec_total {}\n\
rvo_detector_skip_total {}\n\
rvo_detector_fa
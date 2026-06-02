use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EventType {
    DummyEvent,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct Event {
    pub event_type: EventType,
    /// Monotonic nanoseconds since scheduler start.
    pub ts_ns: u64,
    /// Confidence in [0.0, 1.0]: elapsed / duration at the time of emission.
    pub confidence: f64,
}

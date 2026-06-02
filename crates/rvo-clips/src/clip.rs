use rvo_events::EventType;
use std::time::Instant;

#[derive(Clone)]
pub struct ClipJob {
    pub event_type: EventType,
    pub event_ts_ns: u64,
    pub start_ts: Instant,
    pub end_ts: Instant,
}

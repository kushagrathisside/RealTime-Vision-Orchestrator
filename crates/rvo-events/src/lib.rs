pub mod condition;
pub mod engine;
pub mod event;
pub mod publisher;

pub use condition::{CompareOp, Condition, SignalPredicate};
pub use engine::EventEngine;
pub use event::{Event, EventType};
pub use publisher::{start_event_file_sink, start_event_logger, EventPublisher};

#[derive(Clone, Debug)]
pub struct EventDefinition {
    pub event_type: EventType,
    /// Compound signal condition. Use `Condition::single_gte` for the common
    /// single-signal case.
    pub condition: Condition,
    /// How long the condition must hold continuously before an event is emitted.
    /// Zero means instant trigger (confidence = 1.0).
    pub duration_ns: u64,
    /// How long to suppress re-emission after an event fires.
    pub cooldown_ns: u64,
}

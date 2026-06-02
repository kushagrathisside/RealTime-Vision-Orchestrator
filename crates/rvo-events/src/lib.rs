pub mod condition;
pub mod event;
pub mod engine;
pub mod publisher;

pub use condition::{CompareOp, Condition, SignalPredicate};
pub use engine::EventEngine;
pub use event::{Event, EventType};
pub use publisher::{start_event_logger, start_event_file_sink, EventPublisher};

#[derive(Clone, Debug)]
pub struct EventDefinition {
    pub event_t
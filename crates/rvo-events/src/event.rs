use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EventType {
    DummyEvent,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct Event {
    
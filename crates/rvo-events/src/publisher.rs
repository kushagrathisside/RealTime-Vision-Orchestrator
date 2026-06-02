use std::fs::OpenOptions;
use std::io::Write as IoWrite;
use std::sync::atomic::Ordering;
use std::thread;

use crossbeam_channel::{Receiver, Sender, TrySendError};

use rvo_metrics::METRICS;

use crate::Event;

pub struct EventPublisher {
    tx: Sender<Event>,
}

impl EventPublisher {
    pub fn new(tx: Sender<Event>) -> Self {
        Self { tx }
    }

    pub fn publish(&self, event: &Event) {
        match self.tx.try_send(*event) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                METRICS.event_drops.fetch_add(1, Ordering::Relaxed);
                eprintln!("[EVENT] Dropped event (queue full)");
            }
            Err(TrySendError::Disconnected(_)) => {
                eprintln!("[EVENT] Publisher channel disconnected");
            }
        }
    }
}

/// Start the event consumer thread.
///
/// Always logs confirmed events to stdout. When `log_path` is `Some`, also
/// appends each even
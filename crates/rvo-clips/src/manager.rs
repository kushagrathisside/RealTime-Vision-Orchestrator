use std::sync::{Arc, Mutex};
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Duration;

use crossbeam_channel::{Sender, TrySendError};

use rvo_buffer::{Frame, FrameBuffer};
use rvo_events::Event;
use rvo_metrics::METRICS;

use crate::clip::ClipJob;

pub struct ClipManager {
    tx: Sender<(ClipJob, Vec<Frame>)>,
    before: Duration,
    after: Duration,
    /// Shared frame buffer. ClipManager holds an Arc so a post-roll thread can
    /// re-read the buffer after the post-event window has elapsed, without
    /// blocking the scheduler or requiring the scheduler to push frames to us.
    buffer: Arc<Mutex<FrameBuffer>>,
}

impl ClipManager {
    pub fn new(
        tx: Sender<(ClipJob, Vec<Frame>)>,
        before: Duration,
        after: Duration,
        buffer: Arc<Mutex<FrameBuffer>>,
    ) -> Self {
        Self { tx, before, after, buffer }
    }

    /// Called by the scheduler when a confirmed event fires.
    ///
    /// Spawns a short-lived thread that sleeps for the `after` window, then
    /// slices the frame buffer to capture both pre-roll and post-roll frames
    /// before sending the job to the encoder queue. This keeps the scheduler
    /// tick non-blocking while still collecting post-event footage.
    ///
    /// If the encoder queue is full the job is dropped and the metric is
    /// incremented. The live pipeline is never stalled.
    pub fn on_event(&self, event: &Event) {
        // Snapshot event_ts from the newest frame available r
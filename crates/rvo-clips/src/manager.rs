use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
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
        Self {
            tx,
            before,
            after,
            buffer,
        }
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
        // Snapshot event_ts from the newest frame available right now.
        // If the buffer is empty (camera not yet ready) we skip evidence
        // collection rather than panic or fabricate a timestamp.
        let event_ts = {
            let buf = self.buffer.lock().unwrap();
            match buf.newest_instant() {
                Some(ts) => ts,
                None => {
                    METRICS.clip_drops.fetch_add(1, Ordering::Relaxed);
                    println!("[CLIP] Skipped clip job (no frames available)");
                    return;
                }
            }
        };

        let start = event_ts.checked_sub(self.before).unwrap_or(event_ts);
        let end = event_ts + self.after;

        let job = ClipJob {
            event_type: event.event_type,
            event_ts_ns: event.ts_ns,
            start_ts: start,
            end_ts: end,
        };

        // Clone what we need to move into the thread.
        let tx = self.tx.clone();
        let buffer = Arc::clone(&self.buffer);
        let after = self.after;

        thread::spawn(move || {
            // Wait for the post-roll window to close so the buffer has had
            // time to accumulate frames up to `end`.
            thread::sleep(after);

            let frames = buffer.lock().unwrap().slice(start, end);

            match tx.try_send((job, frames)) {
                Ok(_) => {}
                Err(TrySendError::Full(_)) => {
                    METRICS.clip_drops.fetch_add(1, Ordering::Relaxed);
                    println!("[CLIP] Dropped clip job (queue full)");
                }
                Err(TrySendError::Disconnected(_)) => {
                    println!("[CLIP] Encoder unavailable");
                }
            }
        });
    }
}

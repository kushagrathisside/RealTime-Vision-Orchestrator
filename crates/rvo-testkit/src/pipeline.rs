use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crossbeam_channel::{bounded, Receiver, Sender};

use rvo_buffer::{Frame, FrameBuffer};
use rvo_clips::clip::ClipJob;
use rvo_clips::ClipManager;
use rvo_detector::detector::DetectorNode;
use rvo_events::{EventDefinition, EventEngine, EventPublisher};
use rvo_scheduler::scheduler::Scheduler;

use crate::capture::EventCapture;

pub struct BuiltPipeline {
    pub scheduler: Scheduler,
    pub frame_buffer: Arc<Mutex<FrameBuffer>>,
    pub frame_tx: Sender<Frame>,
    pub event_capture: EventCapture,
    pub clip_rx: Receiver<(ClipJob, Vec<Frame>)>,
}

impl BuiltPipeline {
    pub fn run_ticks(&mut self, n: u64) {
        for _ in 0..n {
            self.scheduler.tick();
        }
    }

    pub fn run_for(&mut self, duration: Duration) {
        let start = Instant::now();
        while start.elapsed() < duration {
            self.scheduler.tick();
            thread::sleep(Duration::from_millis(1));
        }
    }

    pub fn inject_frame(&self, frame: Frame) {
        self.frame_buffer.lock().unwrap().push(frame);
    }
}

pub struct PipelineBuilder {
    detectors: Vec<Box<dyn DetectorNode>>,
    event_defs: Vec<EventDefinition>,
    frame_buffer_capacity: usize,
    frame_channel_capacity: usize,
    clip_before: Duration,
    clip_after: Duration,
}

impl Default for PipelineBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl PipelineBuilder {
    pub fn new() -> Self {
        Self {
            detectors: Vec::new(),
            event_defs: Vec::new(),
            frame_buffer_capacity: 300,
            frame_channel_capacity: 16,
            clip_before: Duration::from_secs(2),
            clip_after: Duration::from_secs(1),
        }
    }

    pub fn detector(mut self, d: impl DetectorNode + 'static) -> Self {
        self.detectors.push(Box::new(d));
        self
    }

    pub fn detectors(mut self, ds: Vec<Box<dyn DetectorNode>>) -> Self {
        self.detectors.extend(ds);
        self
    }

    pub fn event(mut self, def: EventDefinition) -> Self {
        self.event_defs.push(def);
        self
    }

    pub fn frame_buffer_capacity(mut self, n: usize) -> Self {
        self.frame_buffer_capacity = n;
        self
    }

    pub fn frame_channel_capacity(mut self, n: usize) -> Self {
        self.frame_channel_capacity = n;
        self
    }

    pub fn clip_window(mut self, before: Duration, after: Duration) -> Self {
        self.clip_before = before;
        self.clip_after = after;
        self
    }

    pub fn build(self) -> BuiltPipeline {
        let frame_buffer = Arc::new(Mutex::new(FrameBuffer::new(self.frame_buffer_capacity)));
        let (frame_tx, frame_rx) = bounded(self.frame_channel_capacity);
        let (clip_tx, clip_rx) = bounded(8);
        let (event_tx, event_rx) = bounded(256);

        let clip_manager = ClipManager::new(
            clip_tx,
            self.clip_before,
            self.clip_after,
            Arc::clone(&frame_buffer),
        );
        let event_publisher = EventPublisher::new(event_tx);
        let event_engine = EventEngine::new_many(self.event_defs);
        let event_capture = EventCapture::new(event_rx);

        let scheduler = Scheduler::new(
            self.detectors,
            event_engine,
            frame_rx,
            clip_manager,
            event_publisher,
            Arc::clone(&frame_buffer),
        );

        BuiltPipeline {
            scheduler,
            frame_buffer,
            frame_tx,
            event_capture,
            clip_rx,
        }
    }
}

use std::fs;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crossbeam_channel::bounded;

use rvo_buffer::FrameBuffer;
use rvo_clips::{start_encoder_worker, ClipManager};
use rvo_detector::detector::DetectorNode;
use rvo_events::{
    start_event_file_sink, Condition, EventDefinition, EventEngine, EventPublisher, EventType,
};
use rvo_metrics::start_metrics_server;
use rvo_scheduler::scheduler::Scheduler;
use rvo_signals::store::SignalType;
use rvo_testkit::{ProbabilisticDetector, SyntheticCamera, SyntheticPattern};

fn main() {
    fs::create_dir_all("clips/synthetic").expect("create synthetic clips dir");

    start_metrics_server(9091);

    let frame_buffer = Arc::new(Mutex::new(FrameBuffer::new(300)));
    let (frame_tx, frame_rx) = bounded(5);
    SyntheticCamera::new(
        640,
        480,
        30.0,
        SyntheticPattern::Alternating {
            color_a: (0, 0, 0),
            color_b: (255, 255, 255),
            period_frames: 15,
        },
    )
    .start(frame_tx);

    let detectors = vec![Box::new(ProbabilisticDetector::new(
        "synthetic-always-on",
        30.0,
        SignalType::Dummy,
        1.0,
        1,
        2_000_000_000,
        7,
    )) as Box<dyn DetectorNode>];

    let event_engine = EventEngine::new(EventDefinition {
        event_type: EventType::DummyEvent,
        condition: Condition::single_gte(SignalType::Dummy, 1),
        duration_ns: 3_000_000_000,
        cooldown_ns: 5_000_000_000,
    });

    let (clip_tx, clip_rx) = bounded(8);
    start_encoder_worker(clip_rx, "clips/synthetic".to_string());
    let clip_manager = ClipManager::new(
        clip_tx,
        Duration::from_secs(3),
        Duration::from_secs(2),
        Arc::clone(&frame_buffer),
    );

    let (event_tx, event_rx) = bounded(64);
    start_event_file_sink(event_rx, "events_synthetic.jsonl".to_string());
    let event_publisher = EventPublisher::new(event_tx);

    let mut scheduler = Scheduler::new(
        detectors,
        event_engine,
        frame_rx,
        clip_manager,
        event_publisher,
        frame_buffer,
    );

    println!("[DEMO] Running synthetic pipeline — Ctrl-C to stop");

    loop {
        scheduler.tick();
        thread::sleep(Duration::from_millis(1));
    }
}

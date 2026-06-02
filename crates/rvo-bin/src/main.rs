use std::{thread, time::Duration};
use std::sync::{Arc, Mutex};

use crossbeam_channel::bounded;

use rvo_scheduler::scheduler::Scheduler;
use rvo_metrics::start_metrics_server;

use rvo_detector::DummyDetector;
use rvo_detector::detector::DetectorNode;
use rvo_detector::load::LoadDetector;
use rvo_detector::jitter::JitterDetector;

use rvo_config::{try_load_config, RvoConfig};
use rvo_events::{
    start_event_logger,
    start_event_file_sink,
    Condition,
    EventEngine,
    EventPublisher,
    EventDefinition,
    EventType,
};
use rvo_signals::store::SignalType;
use rvo_buffer::FrameBuffer;

use rvo_camera::{start_camera, CameraConfig, CameraSource};
use rvo_clips::{ClipManager, start_encoder_worker};

/// ---------------- detector factory ----------------
fn build_detectors(cfg: &RvoConfig) -> Result<Vec<Box<dyn DetectorNode>>, String> {
    let mut detectors: Vec<Box<dyn DetectorNode>> = Vec::new();

    for d in &cfg.detectors {
        if !d.enabled {
            continue;
        }

        match d.kind.as_str() {
            "dummy" => detectors.push(Box::new(DummyDetector)),

            "load" => {
                let busy = d.busy_ns.unwrap_or(1_000_000);
                detectors.push(Box::new(LoadDetector::new(busy)));
            }

            "jitter" => detectors.push(Box::new(JitterDetector)),

            other => return Err(format!("Unknown detector kind: {}", other)),
        }
    }

    Ok(detectors)
}

/// ---------------- event engine factory ----------------
fn build_event_engine(cfg: &RvoConfig) -> Result<EventEngine, String> {
    let mut defs = Vec::new();

    for e in &cfg.events {
        let event_type = match e.event_type.as_str() {
            "DummyEvent" => EventType::DummyEvent,
            other => return Err(format!("Unknown event type: {}", other)),
        };

        // Validated by RvoConfig::validate — safe to unwrap the match.
        let signal_type = match e.signal_type.as_str() {
            "Dummy"          => SignalType::Dummy,
            "MotionLevel"    => SignalType::MotionLevel,
            "FacePresent"    => SignalType::FacePresent,
            "PersonDetected" => SignalType::PersonDetected,
            other => return Err(format!("Unknown signal_type: {}", other)),
        };

        defs.push(EventDefinition {
            event_type,
            condition: Condition::single_gte(signal_type, e.signal_threshold),
            duration_ns: e.duration_ms * 1_000_000,
            cooldown_ns: e.cooldown_ms * 1_000_000,
        });
    }

    Ok(EventEngine::new_many(defs))
}

fn build_runtime_config(
    path: &str,
) -> Result<(Vec<Box<dyn DetectorNode>>, EventEngine), String> {
    let cfg = try_load_config(path)?;
    let detectors    = build_detectors(&cfg)?;
    let event_engine = build_event_engine(&cfg)?;
    Ok((detectors, event_engine))
}

#[cfg(unix)]
fn reload_scheduler(
    scheduler: &Arc<Mutex<Scheduler>>,
    path: &str,
) -> Result<(), String> {
    let (detectors, event_engine) = build_runtime_config(path)?;
    let mut sched = scheduler
        .lock()
        .map_err(|_| "Scheduler lock poisoned".to_string())?;
    sched.swap_runtime(detectors, event_engine);
    Ok(())
}

#[cfg(unix)]
fn spawn_reload_thread(scheduler: Arc<Mutex<Scheduler>>, config_path: String) {
    use signal_hook::consts::SIGHUP;
    use signal_hook::iterator::Signals;

    thread::spawn(move || {
        let mut signals = Signals::new([SIGHUP]).expect("signals");

        for _ in signals.forever() {
            println!("[RVO] SIGHUP — reloading config from {}", config_path);

            match reload_scheduler(&scheduler, &config_path) {
                Ok(()) => println!("[RVO] Reload complete"),
                Err(err) => eprintln!("[RVO] Reload failed: {}", err),
            }
        }
    });
}

#[cfg(not(unix))]
fn spawn_reload_thread(_scheduler: Arc<Mutex<Scheduler>>, _config_path: String) {
    println!("[RVO] SIGHUP config reload disabled on this platform");
}

fn main() {
    // ---------------- config path ----------------
    let config_path = std::env::var("RVO_CONFIG")
        .unwrap_or_else(|_| "config/rvo.yaml".to_string());

    // ---------------- metrics ----------------
    start_metrics_server(9090);

    // ---------------- initial config ----------------
    let cfg          = try_load_config(&config_path).expect("initial config");
    let detectors    = build_detectors(&cfg).expect("build detectors");
    let event_engine = build_event_engine(&cfg).expect("build event engine");

    // ---------------- frame buffer ----------------
    let frame_buffer = Arc::new(Mutex::new(FrameBuffer::new(300))); // ~10s @ 30fps

    // ---------------- camera ----------------
    let camera_source = if let 
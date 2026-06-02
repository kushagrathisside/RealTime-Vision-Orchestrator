use rvo_camera::CameraSource;
use rvo_config::{try_load_config, RvoConfig};
use rvo_detector::detector::DetectorNode;
use rvo_detector::jitter::JitterDetector;
use rvo_detector::load::LoadDetector;
use rvo_detector::DummyDetector;
use rvo_events::{Condition, EventDefinition, EventEngine, EventType};
use rvo_signals::store::SignalType;

pub fn build_detectors(cfg: &RvoConfig) -> Result<Vec<Box<dyn DetectorNode>>, String> {
    let mut detectors: Vec<Box<dyn DetectorNode>> = Vec::new();

    for detector in &cfg.detectors {
        if !detector.enabled {
            continue;
        }

        match detector.kind.as_str() {
            "dummy" => detectors.push(Box::new(DummyDetector)),
            "load" => {
                let busy = detector.busy_ns.unwrap_or(1_000_000);
                detectors.push(Box::new(LoadDetector::new(busy)));
            }
            "jitter" => detectors.push(Box::new(JitterDetector)),
            other => return Err(format!("Unknown detector kind: {}", other)),
        }
    }

    Ok(detectors)
}

pub fn build_event_engine(cfg: &RvoConfig) -> Result<EventEngine, String> {
    let mut defs = Vec::new();

    for event in &cfg.events {
        let event_type = match event.event_type.as_str() {
            "DummyEvent" => EventType::DummyEvent,
            other => return Err(format!("Unknown event type: {}", other)),
        };

        let signal_type = match event.signal_type.as_str() {
            "Dummy" => SignalType::Dummy,
            "MotionLevel" => SignalType::MotionLevel,
            "FacePresent" => SignalType::FacePresent,
            "PersonDetected" => SignalType::PersonDetected,
            other => return Err(format!("Unknown signal_type: {}", other)),
        };

        defs.push(EventDefinition {
            event_type,
            condition: Condition::single_gte(signal_type, event.signal_threshold),
            duration_ns: event.duration_ms * 1_000_000,
            cooldown_ns: event.cooldown_ms * 1_000_000,
        });
    }

    Ok(EventEngine::new_many(defs))
}

pub fn build_runtime_config(
    path: &str,
) -> Result<(Vec<Box<dyn DetectorNode>>, EventEngine), String> {
    let cfg = try_load_config(path)?;
    let detectors = build_detectors(&cfg)?;
    let event_engine = build_event_engine(&cfg)?;
    Ok((detectors, event_engine))
}

pub fn build_camera_source(cfg: &RvoConfig) -> CameraSource {
    if let Some(uri) = cfg.camera.source_uri.clone() {
        CameraSource::Uri(uri)
    } else {
        CameraSource::Device(cfg.camera.device_index.unwrap_or(0))
    }
}

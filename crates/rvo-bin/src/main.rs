use std::sync::{Arc, Mutex};
use std::{thread, time::Duration};

use crossbeam_channel::bounded;

use rvo_bin::runtime::{
    build_camera_source, build_detectors, build_event_engine, build_runtime_config,
};
use rvo_metrics::start_metrics_server;
use rvo_scheduler::scheduler::Scheduler;

use rvo_buffer::FrameBuffer;
use rvo_config::try_load_config;
use rvo_events::{start_event_file_sink, start_event_logger, EventPublisher};

use rvo_camera::{start_camera, CameraConfig};
use rvo_clips::{start_encoder_worker, ClipManager};

#[cfg(unix)]
fn reload_scheduler(scheduler: &Arc<Mutex<Scheduler>>, path: &str) -> Result<(), String> {
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
    let config_path = std::env::var("RVO_CONFIG").unwrap_or_else(|_| "config/rvo.yaml".to_string());

    // ---------------- metrics ----------------
    start_metrics_server(9090);

    // ---------------- initial config ----------------
    let cfg = try_load_config(&config_path).expect("initial config");
    let detectors = build_detectors(&cfg).expect("build detectors");
    let event_engine = build_event_engine(&cfg).expect("build event engine");

    // ---------------- frame buffer ----------------
    let frame_buffer = Arc::new(Mutex::new(FrameBuffer::new(300))); // ~10s @ 30fps

    // ---------------- camera ----------------
    let (frame_tx, frame_rx) = bounded(5);
    start_camera(
        CameraConfig {
            source: build_camera_source(&cfg),
        },
        frame_tx,
    );

    // ---------------- clips ----------------
    let (clip_tx, clip_rx) = bounded(8);
    start_encoder_worker(clip_rx, cfg.clips_dir.clone());

    let clip_manager = ClipManager::new(
        clip_tx,
        Duration::from_secs(3),
        Duration::from_secs(2),
        Arc::clone(&frame_buffer),
    );

    // ---------------- events ----------------
    let (event_tx, event_rx) = bounded(64);

    // Single consumer thread handles stdout logging and optional file sink.
    match cfg.event_log {
        Some(log_path) => start_event_file_sink(event_rx, log_path),
        None => start_event_logger(event_rx),
    }

    let event_publisher = EventPublisher::new(event_tx);

    // ---------------- scheduler ----------------
    let scheduler = Arc::new(Mutex::new(Scheduler::new(
        detectors,
        event_engine,
        frame_rx,
        clip_manager,
        event_publisher,
        frame_buffer,
    )));

    println!(
        "[RVO] Started — config={} clips={} metrics=http://127.0.0.1:9090",
        config_path, cfg.clips_dir
    );

    spawn_reload_thread(Arc::clone(&scheduler), config_path);

    // ---------------- main loop ----------------
    loop {
        scheduler.lock().unwrap().tick();
        thread::sleep(Duration::from_millis(1));
    }
}

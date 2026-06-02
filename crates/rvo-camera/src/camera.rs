use std::sync::atomic::Ordering;
use std::thread;
use std::time::{Duration, Instant};

use crossbeam_channel::Sender;
use opencv::{prelude::*, videoio};

use rvo_buffer::Frame;
use rvo_metrics::METRICS;

/// How to open the camera source.
pub enum CameraSource {
    /// Local device by OS index (0 = default webcam).
    Device(i32),
    /// Any URI that OpenCV's VideoCapture accepts: RTSP streams, file paths,
    /// HTTP MJPEG feeds, GStreamer pipelines, etc.
    Uri(String),
}

pub struct CameraConfig {
    pub source: CameraSource,
}

impl CameraConfig {
    /// Convenience constructor for the common local-webcam case.
    pub fn device(index: i32) -> Self {
        Self { source: CameraSource::Device(index) }
    }

    /// Convenience constructor for RTSP and other URI sources.
    pub fn uri(url: impl Into<String>) -> Self {
        Self { source: CameraSource::Uri(url.into()) }
    }
}

pub fn start_camera(cfg: CameraConfig, tx: Sender<Frame>) {
    thread::spawn(move || {
        let mut cam = match &cfg.source {
            CameraSource::Device(idx) => {
                match videoio::VideoCapture::new(*idx, videoio::CAP_ANY) {
                    Ok(c) => c,
                    Err(err) => {
                        eprintln!("[CAMERA] Failed to open device {}: {}", idx, err);
                        return;
                    }
                }
            }
            CameraSource::Uri(uri) => {
                match videoio::VideoCapture::from_file(uri.as_str(), videoio::CAP_ANY) {
                    Ok(c) => c,
                    Err(err) => {
                        eprintln!("[CAMERA] Failed to open URI '{}': {}", uri, err);
                        return;
                    }
                }
        
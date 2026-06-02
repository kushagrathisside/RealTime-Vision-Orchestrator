use std::sync::atomic::Ordering;
use std::thread;
use std::time::{Duration, Instant};

use crossbeam_channel::Sender;
use opencv::core::{Mat, Rect, Scalar};
use opencv::imgproc;
use opencv::prelude::*;
use opencv::videoio;

use rvo_buffer::Frame;
use rvo_metrics::METRICS;

pub enum SyntheticPattern {
    SolidColor {
        r: u8,
        g: u8,
        b: u8,
    },
    Alternating {
        color_a: (u8, u8, u8),
        color_b: (u8, u8, u8),
        period_frames: u64,
    },
    RectOnBlack {
        x: i32,
        y: i32,
        w: i32,
        h: i32,
    },
}

pub struct SyntheticCamera {
    width: i32,
    height: i32,
    fps: f64,
    pattern: SyntheticPattern,
}

impl SyntheticCamera {
    pub fn new(width: i32, height: i32, fps: f64, pattern: SyntheticPattern) -> Self {
        Self {
            width,
            height,
            fps,
            pattern,
        }
    }

    pub fn start(self, tx: Sender<Frame>) {
        thread::spawn(move || {
            let frame_interval = Duration::from_secs_f64(1.0 / self.fps.max(1.0));
            let mut frame_id = 0_u64;

            loop {
                let image =
                    match build_synthetic_frame(self.width, self.height, &self.pattern, frame_id) {
                        Ok(image) => image,
                        Err(err) => {
                            eprintln!("[SYNTHETIC_CAMERA] Failed to render frame: {}", err);
                            thread::sleep(frame_interval);
                            continue;
                        }
                    };

                let frame = Frame {
                    ts: Instant::now(),
                    id: frame_id,
                    image,
                };

                if tx.try_send(frame).is_err() {
                    METRICS.frame_drops.fetch_add(1, Ordering::Relaxed);
                }

                frame_id += 1;
                thread::sleep(frame_interval);
            }
        });
    }
}

pub struct FileCamera {
    path: String,
    looping: bool,
}

impl FileCamera {
    pub fn new(path: impl Into<String>, looping: bool) -> Self {
        Self {
            path: path.into(),
            looping,
        }
    }

    pub fn start(self, tx: Sender<Frame>) {
        thread::spawn(move || {
            let mut frame_id = 0_u64;
            let mut retries = 0_u8;
            let mut capture = match open_file_capture(&self.path) {
                Ok(capture) => capture,
                Err(err) => {
                    eprintln!("[FILE_CAMERA] Failed to open '{}': {}", self.path, err);
                    return;
                }
            };

            loop {
                let fps = capture
                    .get(videoio::CAP_PROP_FPS)
                    .ok()
                    .filter(|value| *value > 0.0)
                    .unwrap_or(30.0);
                let frame_interval = Duration::from_secs_f64(1.0 / fps);

                let mut image = Mat::default();
                let read_ok = capture.read(&mut image).unwrap_or(false);

                if !read_ok || image.empty() {
                    if self.looping {
                        match open_file_capture(&self.path) {
                            Ok(reopened) => {
                                capture = reopened;
                                retries = 0;
                                continue;
                            }
                            Err(err) => {
                                eprintln!(
                                    "[FILE_CAMERA] Failed to reopen '{}': {}",
                                    self.path, err
                                );
                            }
                        }
                    }

                    retries += 1;
                    if retries >= 5 {
                        return;
                    }

                    thread::sleep(Duration::from_millis(10));
                    continue;
                }

                retries = 0;

                let frame = Frame {
                    ts: Instant::now(),
                    id: frame_id,
                    image,
                };

                if tx.try_send(frame).is_err() {
                    METRICS.frame_drops.fetch_add(1, Ordering::Relaxed);
                }

                frame_id += 1;
                thread::sleep(frame_interval);
            }
        });
    }
}

pub fn start_mock_camera(tx: Sender<Frame>) {
    thread::spawn(move || {
        let mut id = 0_u64;
        loop {
            let frame = Frame {
                ts: Instant::now(),
                id,
                image: Mat::default(),
            };
            let _ = tx.try_send(frame);
            id += 1;
            thread::sleep(Duration::from_millis(30));
        }
    });
}

fn build_synthetic_frame(
    width: i32,
    height: i32,
    pattern: &SyntheticPattern,
    frame_id: u64,
) -> opencv::Result<Mat> {
    match pattern {
        SyntheticPattern::SolidColor { r, g, b } => Mat::new_rows_cols_with_default(
            height,
            width,
            opencv::core::CV_8UC3,
            bgr_scalar(*r, *g, *b),
        ),
        SyntheticPattern::Alternating {
            color_a,
            color_b,
            period_frames,
        } => {
            let use_a = *period_frames == 0 || (frame_id / *period_frames).is_multiple_of(2);
            let (r, g, b) = if use_a { *color_a } else { *color_b };
            Mat::new_rows_cols_with_default(
                height,
                width,
                opencv::core::CV_8UC3,
                bgr_scalar(r, g, b),
            )
        }
        SyntheticPattern::RectOnBlack { x, y, w, h } => {
            let mut image = Mat::new_rows_cols_with_default(
                height,
                width,
                opencv::core::CV_8UC3,
                Scalar::new(0.0, 0.0, 0.0, 0.0),
            )?;
            imgproc::rectangle(
                &mut image,
                Rect::new(*x, *y, *w, *h),
                Scalar::new(255.0, 255.0, 255.0, 0.0),
                -1,
                imgproc::LINE_8,
                0,
            )?;
            Ok(image)
        }
    }
}

fn bgr_scalar(r: u8, g: u8, b: u8) -> Scalar {
    Scalar::new(f64::from(b), f64::from(g), f64::from(r), 0.0)
}

fn open_file_capture(path: &str) -> opencv::Result<videoio::VideoCapture> {
    videoio::VideoCapture::from_file(path, videoio::CAP_ANY)
}

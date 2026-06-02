use std::fs;
use std::path::PathBuf;
use std::thread;
use std::time::Instant;

use crossbeam_channel::Receiver;
use opencv::core::Vector;
use opencv::imgcodecs;

use rvo_buffer::Frame;

use crate::clip::ClipJob;

/// Write clip evidence to disk.
///
/// For each incoming job, creates a directory under `clips_dir` named
/// `{EventType}_{event_ts_ns}/` and writes:
///
/// - `frame_NNNN.jpg` — each frame encoded as JPEG (quality 90).
/// - `meta.json`      — event metadata and per-frame timestamps.
///
/// Frames that fail encoding are counted in `meta.json` but do not abort the
/// clip. The worker logs outcome and encoding latency per clip.
///
/// This is a best-effort, blocking worker. I
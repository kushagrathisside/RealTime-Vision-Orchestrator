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
/// This is a best-effort, blocking worker. It never stalls the live path
/// because the encoder queue uses `try_send` with drop-on-overflow.
pub fn start_encoder_worker(rx: Receiver<(ClipJob, Vec<Frame>)>, clips_dir: String) {
    thread::spawn(move || {
        // JPEG encoding params: [IMWRITE_JPEG_QUALITY, 90]
        let jpeg_params = {
            let mut v = Vector::new();
            v.push(imgcodecs::IMWRITE_JPEG_QUALITY);
            v.push(90);
            v
        };

        while let Ok((job, frames)) = rx.recv() {
            let encode_start = Instant::now();

            // Build clip directory name: {EventType}_{ts_ns}
            let dir_name = format!("{:?}_{}", job.event_type, job.event_ts_ns);
            let clip_dir = PathBuf::from(&clips_dir).join(&dir_name);

            if let Err(err) = fs::create_dir_all(&clip_dir) {
                eprintln!(
                    "[ENCODER] Could not create '{}': {}",
                    clip_dir.display(),
                    err
                );
                continue;
            }

            let total_frames = frames.len();
            let mut written_frames = 0usize;
            let mut frame_ts_ns: Vec<u64> = Vec::with_capacity(total_frames);

            for (i, frame) in frames.iter().enumerate() {
                // Store a relative timestamp in nanoseconds from the clip start.
                // (Instant arithmetic — frame.ts is an Instant, not wall time.)
                let rel_ns = frame
                    .ts
                    .checked_duration_since(job.start_ts)
                    .map(|d| d.as_nanos() as u64)
                    .unwrap_or(0);
                frame_ts_ns.push(rel_ns);

                let img_path = clip_dir.join(format!("frame_{:04}.jpg", i));
                let img_path_str = img_path.to_string_lossy().into_owned();

                match imgcodecs::imwrite(&img_path_str, &frame.image, &jpeg_params) {
                    Ok(true) => written_frames += 1,
                    Ok(false) => {
                        eprintln!("[ENCODER] imwrite returned false for frame {}", i);
                    }
                    Err(err) => {
                        eprintln!("[ENCODER] imwrite error on frame {}: {}", i, err);
                    }
                }
            }

            let encode_ms = encode_start.elapsed().as_millis();

            // Write JSON metadata sidecar.
            let meta = serde_json::json!({
                "event_type":     format!("{:?}", job.event_type),
                "event_ts_ns":    job.event_ts_ns,
                "clip_window_ns": {
                    "start": job.start_ts.elapsed().as_nanos() as u64,
                    "end":   job.end_ts.elapsed().as_nanos() as u64,
                },
                "frames_total":   total_frames,
                "frames_written": written_frames,
                "frame_ts_ns":    frame_ts_ns,
                "encode_ms":      encode_ms,
            });

            let meta_path = clip_dir.join("meta.json");
            if let Err(err) = fs::write(&meta_path, meta.to_string()) {
                eprintln!("[ENCODER] Failed to write meta.json: {}", err);
            }

            println!(
                "[ENCODER] Clip '{}' — {}/{} frames written in {}ms",
                dir_name, written_frames, total_frames, encode_ms
            );
        }
    });
}

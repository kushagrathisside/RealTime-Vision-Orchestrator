pub mod camera;
pub mod capture;
pub mod detectors;
pub mod pipeline;

pub use camera::{start_mock_camera, FileCamera, SyntheticCamera, SyntheticPattern};
pub use capture::{EventCapture, MetricsSnapshot};
pub use detectors::{
    ChainedDetector, FailingDetector, LatencyDetector, ProbabilisticDetector, ScriptEntry,
    ScriptedDetector,
};
pub use pipeline::{BuiltPipeline, PipelineBuilder};

use crate::detector::{
    DetectorContext, DetectorCostHint, DetectorHealth, DetectorMeta, DetectorNode, DetectorResult,
};
use rand::{thread_rng, Rng};

pub struct JitterDetector;

impl DetectorNode for JitterDetector {
    fn meta(&self) -> DetectorMeta {
        DetectorMeta {
            id: "jitter",
            max_fps: 30.0,
            dependencies: &[],
            output_signals: &[],
            cost_hint: DetectorCostHint::Medium,
            requires_frame: false,
        }
    }

    fn execute(&mut self, _ctx: &DetectorContext<'_>) -> DetectorResult {
        let jitter = thread_rng().gen_range(0..2_000_000); // up to 2ms
        let start = std::time::Instant::now();

        while start.elapsed().as_nanos() < jitter {}

        DetectorResult {
            signals: vec![],
            health: DetectorHealth::Ok,
        }
    }
}

use std::time::Instant;

use crate::detector::{
    DetectorContext,
    DetectorCostHint,
    DetectorHealth,
    DetectorMeta,
    DetectorNode,
    DetectorResult,
};

pub struct LoadDetector {
    busy_ns: u64,
}

impl LoadDetector {
    pub fn new(busy_ns: u64) -> Self {
        Self { busy_ns }
    }
}

impl DetectorNode for LoadDetector {
    fn meta(&self) -> DetectorMeta {
        DetectorMeta {
            id: "synthetic_load",
            max_fps: 10.0,
            dependencies: &[],
            output_signals: &[],
            cost_hint: DetectorCostHint::High,
            requires_frame: false,
        }
    }

    fn execute(&mut self, _ctx: &DetectorContext<'_>) -> DetectorResult {
        let start = Instant::now();

        while start.elapsed().as_nanos() < self.busy_ns as u128 {}

        DetectorResult {
            signals: Vec::new(),
            health: DetectorHealth::Ok,
        }
    }
}

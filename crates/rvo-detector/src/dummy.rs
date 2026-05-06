use rvo_signals::store::{Signal, SignalType};
use crate::detector::{
    DetectorCostHint,
    DetectorContext,
    DetectorHealth,
    DetectorMeta,
    DetectorNode,
    DetectorResult,
};

pub struct DummyDetector;

const OUTPUT_SIGNALS: &[SignalType] = &[SignalType::Dummy];

impl DetectorNode for DummyDetector {
    fn meta(&self) -> DetectorMeta {
        DetectorMeta {
            id: "dummy-detector",
            max_fps: 30.0,
            dependencies: &[],
            output_signals: OUTPUT_SIGNALS,
            cost_hint: DetectorCostHint::Low,
            requires_frame: false,
        }
    }

    fn execute(&mut self, ctx: &DetectorContext<'_>) -> DetectorResult {
        let signal = Signal {
            signal_type: SignalType::Dummy,
            value: 1,
            ts_ns: ctx.now_ns,
            ttl_ns: 1_000_000_000, // 1 second TTL
        };

        DetectorResult {
            signals: vec![signal],
            health: DetectorHealth::Ok,
        }
    }
}

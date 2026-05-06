use rvo_buffer::Frame;
use rvo_signals::store::Signal;

pub struct DetectorContext<'a> {
    pub now_ns: u64,
    pub frame: Option<&'a Frame>,
}

pub enum DetectorHealth {
    Ok,
    Failed,
}

pub struct DetectorResult {
    pub signals: Vec<Signal>,
    pub health: DetectorHealth,
}

pub trait DetectorNode: Send {
    fn id(&self) -> &'static str;
    fn max_fps(&self) -> f64;
    fn execute(&mut self, ctx: &DetectorContext<'_>) -> DetectorResult;
}

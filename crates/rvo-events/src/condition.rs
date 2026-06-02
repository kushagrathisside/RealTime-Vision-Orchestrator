use rvo_signals::store::{SignalStore, SignalType};

/// Comparison operator for a signal predicate.
#[derive(Clone, Copy, Debug)]
pub enum CompareOp {
    /// `signal.value >= value`
    Gte,
    /// `signal.value > value`
    Gt,
    /// `signal.value == value`
    Eq,
    /// `signal.value < value`
    Lt,
    /// `signal.value <= value`
    Lte,
}

/// A single typed signal check.
///
/// A missing or stale signal evaluates to `false` regardless of operator.
#[derive(Clone, Debug)]
pub struct SignalPredicate {
    pub signal_type: SignalType,
    pub op: CompareOp,
    pub value: u64,
}

impl SignalPredicate {
    pub fn evaluate(&self, signals: &SignalStore, now_ns: u64) -> bool {
        signals
            .get(self.signal_type, now_ns)
            .map(|s| match self.op {
                CompareOp::Gte => s.value >= self.value,
                CompareOp::Gt => s.value > self.value,
                CompareOp::Eq => s.value == self.value,
                CompareOp::Lt => s.value < self.value,
                CompareOp::Lte => s.value <= self.value,
            })
            .unwrap_or(false)
    }
}

/// A compound signal condition.
///
/// `All` requires every predicate to hold (logical AND).
/// `Any` requires at least one predicate to hold (logical OR).
///
/// Nesting (`All` inside `Any`, etc.) is not supported at this level;
/// complex conditions should be expressed as multiple event definitions.
#[derive(Clone, Debug)]
pub enum Condition {
    /// All predicates must be satisfied.
    All(Vec<SignalPredicate>),
    /// At least one predicate must be satisfied.
    Any(Vec<SignalPredicate>),
}

impl Condition {
    /// Evaluate the condition against the current signal store.
    pub fn evaluate(&self, signals: &SignalStore, now_ns: u64) -> bool {
        match self {
            Condition::All(preds) => preds.iter().all(|p| p.evaluate(signals, now_ns)),
            Condition::Any(preds) => preds.iter().any(|p| p.evaluate(signals, now_ns)),
        }
    }

    /// Shorthand: `signal_type >= value`.
    ///
    /// This is the single-signal backward-compatible constructor. It expands
    /// to `All([SignalPredicate { Gte, value }])`.
    pub fn single_gte(signal_type: SignalType, value: u64) -> Self {
        Condition::All(vec![SignalPredicate {
            signal_type,
            op: CompareOp::Gte,
            value,
        }])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rvo_signals::store::{Signal, SignalStore, SignalType};

    fn store_with(signal_type: SignalType, value: u64) -> SignalStore {
        let mut store = SignalStore::new();
        store.upsert(Signal {
            signal_type,
            value,
            ts_ns: 0,
            ttl_ns: 10_000_000_000,
        });
        store
    }

    #[test]
    fn single_gte_passes() {
        let store = store_with(SignalType::Dummy, 5);
        let cond = Condition::single_gte(SignalType::Dummy, 5);
        assert!(cond.evaluate(&store, 1_000));
    }

    #[test]
    fn single_gte_fails_below_threshold() {
        let store = store_with(SignalType::Dummy, 4);
        let cond = Condition::single_gte(SignalType::Dummy, 5);
        assert!(!cond.evaluate(&store, 1_000));
    }

    #[test]
    fn all_requires_every_predicate() {
        let mut store = SignalStore::new();
        store.upsert(Signal {
            signal_type: SignalType::Dummy,
            value: 1,
            ts_ns: 0,
            ttl_ns: 10_000_000_000,
        });
        store.upsert(Signal {
            signal_type: SignalType::FacePresent,
            value: 0,
            ts_ns: 0,
            ttl_ns: 10_000_000_000,
        });

        let cond = Condition::All(vec![
            SignalPredicate {
                signal_type: SignalType::Dummy,
                op: CompareOp::Gte,
                value: 1,
            },
            SignalPredicate {
                signal_type: SignalType::FacePresent,
                op: CompareOp::Eq,
                value: 1,
            },
        ]);

        // FacePresent is 0, not 1 — All fails
        assert!(!cond.evaluate(&store, 1_000));
    }

    #[test]
    fn any_passes_on_first_match() {
        let mut store = SignalStore::new();
        store.upsert(Signal {
            signal_type: SignalType::Dummy,
            value: 0,
            ts_ns: 0,
            ttl_ns: 10_000_000_000,
        });
        store.upsert(Signal {
            signal_type: SignalType::MotionLevel,
            value: 100,
            ts_ns: 0,
            ttl_ns: 10_000_000_000,
        });

        let cond = Condition::Any(vec![
            SignalPredicate {
                signal_type: SignalType::Dummy,
                op: CompareOp::Gte,
                value: 1,
            },
            SignalPredicate {
                signal_type: SignalType::MotionLevel,
                op: CompareOp::Gte,
                value: 50,
            },
        ]);

        // Dummy is 0 (fails), MotionLevel is 100 (passes) — Any passes
        assert!(cond.evaluate(&store, 1_000));
    }

    #[test]
    fn missing_signal_evaluates_false() {
        let store = SignalStore::new(); // empty
        let cond = Condition::single_gte(SignalType::PersonDetected, 1);
        assert!(!cond.evaluate(&store, 1_000));
    }
}

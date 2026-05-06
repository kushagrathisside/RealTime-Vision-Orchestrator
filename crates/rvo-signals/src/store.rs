use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SignalType {
    Dummy,
}

impl SignalType {
    const COUNT: usize = 1;

    fn index(self) -> usize {
        match self {
            SignalType::Dummy => 0,
        }
    }
}

#[derive(Clone, Copy)]
pub struct Signal {
    pub signal_type: SignalType,
    pub value: u64,
    pub ts_ns: u64,
    pub ttl_ns: u64,
}

struct SignalSlot {
    version: AtomicU64,
    signal: Signal,
}

impl SignalSlot {
    fn new() -> Self {
        Self {
            version: AtomicU64::new(0),
            signal: Signal {
                signal_type: SignalType::Dummy,
                value: 0,
                ts_ns: 0,
                ttl_ns: 0,
            },
        }
    }
}

pub struct SignalStore {
    slots: Vec<SignalSlot>,
}

impl SignalStore {
    pub fn new() -> Self {
        Self {
            slots: (0..SignalType::COUNT)
                .map(|_| SignalSlot::new())
                .collect(),
        }
    }

    pub fn upsert(&mut self, signal: Signal) {
        let slot = &mut self.slots[signal.signal_type.index()];
        let v = slot.version.load(Ordering::Relaxed);
        slot.version.store(v + 1, Ordering::Release); // write start
        slot.signal = signal;
        slot.version.store(v + 2, Ordering::Release); // write end
    }

    pub fn get(
        &self,
        signal_type: SignalType,
        now_ns: u64,
    ) -> Option<Signal> {
        let slot = &self.slots[signal_type.index()];
        let v1 = slot.version.load(Ordering::Acquire);
        if v1 % 2 != 0 {
            return None;
        }

        let sig = slot.signal;

        let v2 = slot.version.load(Ordering::Acquire);
        if v1 != v2 {
            return None;
        }

        if sig.ts_ns.saturating_add(sig.ttl_ns) < now_ns {
            None
        } else {
            Some(sig)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gets_fresh_signal_by_type() {
        let mut store = SignalStore::new();

        store.upsert(Signal {
            signal_type: SignalType::Dummy,
            value: 7,
            ts_ns: 1_000,
            ttl_ns: 1_000,
        });

        let signal = store
            .get(SignalType::Dummy, 1_500)
            .expect("fresh signal");

        assert_eq!(signal.value, 7);
    }

    #[test]
    fn expired_signal_is_absent() {
        let mut store = SignalStore::new();

        store.upsert(Signal {
            signal_type: SignalType::Dummy,
            value: 7,
            ts_ns: 1_000,
            ttl_ns: 100,
        });

        assert!(store.get(SignalType::Dummy, 2_000).is_none());
    }
}

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

/// Clock abstraction so tests can accelerate time (spec §3.3/§3.5/§3.6).
/// Production uses wall time; tests advance a mock clock without sleeping.
pub trait Clock: Send + Sync + 'static {
    fn now_millis(&self) -> u64;
}

#[derive(Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_millis(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }
}

/// Mock clock for tests — starts at `initial` and advances via `advance`.
#[derive(Debug)]
pub struct MockClock {
    millis: AtomicU64,
}

impl MockClock {
    pub fn new(initial_millis: u64) -> Self {
        Self { millis: AtomicU64::new(initial_millis) }
    }

    pub fn advance(&self, delta_millis: u64) {
        self.millis.fetch_add(delta_millis, Ordering::SeqCst);
    }

    pub fn set(&self, millis: u64) {
        self.millis.store(millis, Ordering::SeqCst);
    }
}

impl Clock for MockClock {
    fn now_millis(&self) -> u64 {
        self.millis.load(Ordering::SeqCst)
    }
}

impl Clock for Arc<MockClock> {
    fn now_millis(&self) -> u64 {
        self.as_ref().now_millis()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mock_advances() {
        let c = MockClock::new(1000);
        assert_eq!(c.now_millis(), 1000);
        c.advance(500);
        assert_eq!(c.now_millis(), 1500);
    }
}

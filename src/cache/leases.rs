//! Read leases: what is being streamed to a viewer is not evicted (ADR-0017).
//!
//! The cache's clocks measure REQUESTS. `last_touch` moves when a request is
//! answered, and every eviction rule is a function of it — so a response body
//! that streams for longer than the guards' windows loses its protection
//! while it is still being read: past `STAGE_MIN_AGE_MS` its spans become
//! budget-evictable, past `inactive_ttl` the sweep deletes them, and a viewer
//! who pauses between requests loses the window to the next overshoot.
//!
//! A lease is held for the life of a response body that is served from local
//! bytes (`Disk` or `Stage`), and it is released when that body ends or is
//! dropped — including the disconnect case, because the body's drop is what
//! axum does when the viewer goes away. Eviction consults the lease map before
//! taking anything from a key, and keeps doing so for `read_grace_secs` after
//! the last lease ends, so a pause-then-resume does not pay a re-fetch.
//!
//! What a lease does NOT do is outrank the disk. Pressure reclaims strays even
//! while they are being streamed (ADR-0014/0017): the disk is the last resort,
//! and unlinking a file does not cut a stream that already holds its
//! descriptor open. The rule is the one ADR-0012 established for its hold — a
//! deadline, not an exemption.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

/// One key's lease bookkeeping: how many bodies hold it, and when the last one
/// let go.
#[derive(Default)]
struct Entry {
    active: u32,
    last_end_millis: u64,
}

/// The lease map. Locked briefly and never across an await: `Drop` cannot
/// await, and eviction only needs the answer, not a guard.
///
/// `pub` like the rest of the machinery the integration tests drive directly
/// (`flight`, `store`); production reaches it through the response path.
pub struct Leases {
    inner: Mutex<HashMap<String, Entry>>,
    /// How long a key stays protected after its last body ends.
    grace_ms: u64,
}

impl Leases {
    pub fn new(grace_ms: u64) -> Self {
        Self { inner: Mutex::new(HashMap::new()), grace_ms }
    }

    /// Take a lease for `key`; the returned guard releases it on drop. `now`
    /// is a clock read as a closure so the guard stays non-generic and can
    /// ride along with a body of any cache instance.
    pub fn acquire(
        self: &Arc<Self>,
        key: &str,
        now: Arc<dyn Fn() -> u64 + Send + Sync>,
    ) -> LeaseGuard {
        {
            let mut map = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            map.entry(key.to_string()).or_default().active += 1;
        }
        LeaseGuard { leases: Arc::clone(self), key: key.to_string(), now }
    }

    /// The same, for callers holding a typed clock.
    pub fn acquire_at<C: crate::clock::Clock + 'static>(
        self: &Arc<Self>,
        key: &str,
        clock: Arc<C>,
    ) -> LeaseGuard {
        self.acquire(key, Arc::new(move || clock.now_millis()))
    }

    /// Every key eviction must leave alone right now — the view the sweep
    /// consults once per pass instead of asking per candidate. Expired
    /// bookkeeping is pruned here, so the map cannot grow with every key the
    /// node has ever served.
    pub fn protected(&self, now: u64) -> HashSet<String> {
        let mut map = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        map.retain(|_, e| self.protected_entry(e, now) || e.active > 0);
        map.iter()
            .filter(|(_, e)| self.protected_entry(e, now))
            .map(|(k, _)| k.clone())
            .collect()
    }

    fn protected_entry(&self, entry: &Entry, now: u64) -> bool {
        entry.active > 0
            || (entry.last_end_millis > 0
                && now.saturating_sub(entry.last_end_millis) < self.grace_ms)
    }
}

/// One body's lease. Dropping it stamps the end time, which is what gives the
/// viewer a grace window after the stream finishes.
pub struct LeaseGuard {
    leases: Arc<Leases>,
    key: String,
    now: Arc<dyn Fn() -> u64 + Send + Sync>,
}

impl Drop for LeaseGuard {
    fn drop(&mut self) {
        let now = (self.now)();
        let mut map = self.leases.inner.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = map.get_mut(&self.key) {
            entry.active = entry.active.saturating_sub(1);
            if entry.active == 0 {
                entry.last_end_millis = now;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::MockClock;

    #[test]
    fn a_lease_protects_until_the_grace_expires() {
        let now = 1_000_000u64;
        let clock = Arc::new(MockClock::new(now));
        let leases = Arc::new(Leases::new(300_000));
        assert!(!leases.protected(now).contains("a.bin"), "nothing read yet");

        let guard = leases.acquire_at("a.bin", Arc::clone(&clock));
        assert!(leases.protected(now).contains("a.bin"));

        drop(guard);
        assert!(leases.protected(now).contains("a.bin"), "the grace covers the moment a stream ends");
        assert!(leases.protected(now + 299_999).contains("a.bin"), "still inside the grace");
        assert!(!leases.protected(now + 300_001).contains("a.bin"), "the grace is a deadline");
    }

    #[test]
    fn a_zero_grace_protects_only_while_the_body_lives() {
        let now = 5_000u64;
        let clock = Arc::new(MockClock::new(now));
        let leases = Arc::new(Leases::new(0));
        let guard = leases.acquire_at("b.bin", Arc::clone(&clock));
        assert!(leases.protected(now).contains("b.bin"));
        drop(guard);
        assert!(!leases.protected(now).contains("b.bin"), "0 = no grace, only live bodies");
    }

    #[test]
    fn the_map_does_not_grow_with_every_key_ever_read() {
        let now = 10_000u64;
        let clock = Arc::new(MockClock::new(now));
        let leases = Arc::new(Leases::new(1_000));
        for i in 0..100 {
            drop(leases.acquire_at(&format!("k{i}"), Arc::clone(&clock)));
        }
        // A pass long after the grace prunes every stale entry.
        assert!(leases.protected(now + 10_000).is_empty());
        assert_eq!(leases.inner.lock().unwrap().len(), 0);
    }
}

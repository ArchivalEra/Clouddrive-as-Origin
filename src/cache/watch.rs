//! Watches: a key is being VIEWED (ADR-0018).
//!
//! A read lease (ADR-0017) says *a response body is alive right now*, and its
//! horizon is that body plus `read_grace_secs`. That covers a viewer between
//! two requests of the same session, but it is the wrong unit for the problem
//! the cache actually has: a viewing session lasts hours, its bodies last
//! milliseconds (EdgeOne asks for ascending 1 MiB shards, so the origin sees a
//! request per shard), and the thing worth protecting is neither "everything
//! this key has" nor "whatever the last body touched".
//!
//! A WATCH says *this key is being watched*. It survives the gaps between the
//! bodies of one session — a browser pauses, the player waits before its next
//! shard — for `watch_idle_secs`, and it remembers WHERE on the object the
//! viewer is. That position is what lets the eviction paths pin a bounded
//! neighbourhood instead of choosing between two bad rules:
//!
//! - the lease's rule ("a key being read is not evicted at all") protects
//!   spans hundreds of megabytes behind the playhead that will never be read
//!   again, so the magazine's byte budget stops being enforceable against
//!   exactly the key that matters;
//! - the clocks' rule (`last_touch` measured in requests) loses a long watch
//!   halfway through: past the grace the viewer's own window becomes an
//!   ordinary eviction candidate, and a pause longer than the grace throws
//!   away the read-ahead the chain had already fetched.
//!
//! The pin is bounded in bytes by construction (`watch_pin_bytes`, split
//! evenly behind and ahead of the position), and it is a deadline rather than
//! an exemption (ADR-0012's rule, restated by ADR-0018): a key whose pin is
//! the only thing left to take
//! still yields, which is what keeps the budget real for a 200 GB object on a
//! 10 GiB magazine.
//!
//! With `watch_idle_secs = 0` a watch exists only while a body is streaming it
//! and every path here reduces to the lease's rule — the switch that makes the
//! reverse-verification of this module possible.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

/// A watched viewer's neighbourhood, as the eviction paths see it: the bytes
/// to leave alone, and the position they are anchored on.
///
/// The anchor matters because a pin that has to be spent must be spent in the
/// right order. `anchor` is where the viewer will continue FROM (the end of the
/// last response), so the spans *behind* it are what it has already watched and
/// the spans *ahead* are what it is about to need — and forward progress is
/// continuous while a scrub back is deliberate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Pin {
    pub start: u64,
    pub end: u64,
    pub anchor: u64,
}

/// One key's watch bookkeeping.
#[derive(Default)]
struct Entry {
    /// Bodies streaming this key right now.
    readers: u32,
    /// Last activity on the key, millis since the cache's epoch. Stamped by
    /// every request and again when a body ends.
    last_activity_millis: u64,
    /// The byte range of the most recent response: where the viewer is.
    span: Option<(u64, u64)>,
}

/// The watch map. Locked briefly and never across an await: `Drop` cannot
/// await, and eviction only needs the answer, not a guard.
///
/// `pub` like the rest of the machinery the tests drive directly (`leases`,
/// `flight`, `store`); production reaches it through the response path.
pub struct Watches {
    inner: Mutex<HashMap<String, Entry>>,
    /// How long a watch outlives its last body. 0 = only live bodies count.
    idle_ms: u64,
    /// Bytes pinned behind the viewer's position.
    back_bytes: u64,
    /// Bytes pinned ahead of it — the read-ahead the chain may have fetched.
    ahead_bytes: u64,
}

impl Watches {
    /// `pin_bytes` is split evenly around the viewer's position: half behind
    /// (what a small scrub back needs) and half ahead (the window the chain
    /// fetches before the player asks for it).
    pub fn new(idle_ms: u64, pin_bytes: u64) -> Self {
        let back = pin_bytes / 2;
        Self {
            inner: Mutex::new(HashMap::new()),
            idle_ms,
            back_bytes: back,
            ahead_bytes: pin_bytes - back,
        }
    }

    /// Register a response for `key` covering `[start, end)`. The returned
    /// guard keeps the key's readers counted for as long as the body lives,
    /// and stamps the watch again when it ends — so the idle budget runs from
    /// the last byte delivered, not the last request accepted.
    ///
    /// `now` is a clock read as a closure so the guard stays non-generic and
    /// can ride along with a body of any cache instance.
    pub fn acquire(
        self: &Arc<Self>,
        key: &str,
        span: (u64, u64),
        now: Arc<dyn Fn() -> u64 + Send + Sync>,
    ) -> WatchGuard {
        let at = (now)();
        let resumed = {
            let mut map = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            // Whether the key had a watch before this response: a fresh entry
            // is a start, however the clock reads.
            let existed = map.contains_key(key);
            let entry = map.entry(key.to_string()).or_default();
            let resumed = existed && entry.readers == 0 && self.live_entry(entry, at);
            entry.readers += 1;
            entry.last_activity_millis = at;
            entry.span = Some(span);
            resumed
        };
        WatchGuard { watches: Arc::clone(self), key: key.to_string(), span, now, resumed }
    }

    /// The same, for callers holding a typed clock.
    pub fn acquire_at<C: crate::clock::Clock + 'static>(
        self: &Arc<Self>,
        key: &str,
        span: (u64, u64),
        clock: Arc<C>,
    ) -> WatchGuard {
        self.acquire(key, span, Arc::new(move || clock.now_millis()))
    }

    /// Whether the key is currently being watched: a body is streaming it, or
    /// the last activity is still inside the idle budget.
    pub fn live(&self, key: &str, now: u64) -> bool {
        let map = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        map.get(key).is_some_and(|e| self.live_entry(e, now))
    }

    /// The byte neighbourhood to leave alone for this key, if it is watched.
    pub fn pin(&self, key: &str, now: u64) -> Option<Pin> {
        let map = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let entry = map.get(key)?;
        if !self.live_entry(entry, now) {
            return None;
        }
        self.pin_of(entry)
    }

    /// Every watched key's neighbourhood — the view an eviction pass consults
    /// once instead of asking per candidate. Entries whose idle budget has
    /// lapsed are pruned here, so the map cannot grow with every key the node
    /// has ever served.
    pub fn pins(&self, now: u64) -> HashMap<String, Pin> {
        let mut map = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        map.retain(|_, e| self.live_entry(e, now));
        map.iter()
            .filter_map(|(k, e)| self.pin_of(e).map(|p| (k.clone(), p)))
            .collect()
    }

    /// The watched keys, pruned on the way through. The TTL sweep consults
    /// this: a key mid-watch is not idle however long its last request was —
    /// that is the whole point of watching outliving a body.
    pub fn live_keys(&self, now: u64) -> HashSet<String> {
        let mut map = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        map.retain(|_, e| self.live_entry(e, now));
        map.keys().cloned().collect()
    }

    /// Watched keys right now (healthz/metrics).
    pub fn active(&self, now: u64) -> usize {
        self.live_keys(now).len()
    }

    /// Bytes the live pins cover — what the budget is currently committed to
    /// leaving alone (metrics, and the number that says whether pinning has
    /// grown past what the magazine can afford).
    pub fn pinned_bytes(&self, now: u64) -> u64 {
        self.pins(now).values().map(|p| p.end.saturating_sub(p.start)).sum()
    }

    fn live_entry(&self, entry: &Entry, now: u64) -> bool {
        // No "has it ever been touched" sentinel: an entry only exists because
        // a response created it, and a mock clock legitimately reads 0.
        entry.readers > 0 || now.saturating_sub(entry.last_activity_millis) < self.idle_ms
    }

    fn pin_of(&self, entry: &Entry) -> Option<Pin> {
        // A configured pin of 0 means "no pinning", and that has to keep
        // ADR-0017's rule intact rather than leave a live body with no
        // protection at all: the callers fall back to sparing the whole key
        // when there is no pin.
        if self.back_bytes == 0 && self.ahead_bytes == 0 {
            return None;
        }
        // Anchored where the viewer will continue FROM — the end of the last
        // response, which for a shard walk is the frontier the chain fetches
        // ahead of. Anchoring at the response's start would leave the
        // read-ahead window hanging outside the pin by a whole window.
        let (_, end) = entry.span?;
        Some(Pin {
            start: end.saturating_sub(self.back_bytes),
            end: end.saturating_add(self.ahead_bytes),
            anchor: end,
        })
    }
}

/// One response body's watch. Dropping it stamps the activity, which is what
/// makes the idle budget run from the last delivered byte.
pub struct WatchGuard {
    watches: Arc<Watches>,
    key: String,
    span: (u64, u64),
    now: Arc<dyn Fn() -> u64 + Send + Sync>,
    resumed: bool,
}

impl WatchGuard {
    /// Whether this response arrived at a watch that was live with no body
    /// streaming the key: nobody was holding it, the session was.
    ///
    /// Read the metric built on this as "the watch, not a live body, was what
    /// answered" rather than as "the viewer paused": an EdgeOne shard walk
    /// produces one of these per shard, because the origin's bodies for
    /// consecutive shards do not overlap in time. What it measures is how
    /// often the watch bought something, and `miss` on it means that path
    /// still paid an upstream open.
    pub fn resumed(&self) -> bool {
        self.resumed
    }
}

impl Drop for WatchGuard {
    fn drop(&mut self) {
        let now = (self.now)();
        let mut map = self.watches.inner.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = map.get_mut(&self.key) {
            entry.readers = entry.readers.saturating_sub(1);
            entry.last_activity_millis = now;
            entry.span = Some(self.span);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::MockClock;

    fn at(now: u64) -> Arc<MockClock> {
        Arc::new(MockClock::new(now))
    }

    /// A watch outlives its body by the idle budget, and its pin follows the
    /// position the viewer was last at — this is the horizon a read lease
    /// cannot give: protection while nothing is being streamed.
    #[test]
    fn a_watch_outlives_its_body_by_the_idle_budget() {
        let now = 1_000_000u64;
        let clock = at(now);
        let watches = Arc::new(Watches::new(900_000, 200));
        assert!(!watches.live("a.bin", now), "nothing watched yet");

        let guard = watches.acquire_at("a.bin", (5_000, 6_000), Arc::clone(&clock));
        assert!(watches.live("a.bin", now));
        assert_eq!(
            watches.pin("a.bin", now),
            Some(Pin { start: 5_900, end: 6_100, anchor: 6_000 }),
            "100 back, 100 ahead"
        );

        drop(guard);
        assert!(watches.live("a.bin", now + 899_999), "the pause is covered");
        assert!(!watches.live("a.bin", now + 900_001), "and the budget is a deadline");
        assert!(watches.pin("a.bin", now + 900_001).is_none());
    }

    /// The switch that makes the reverse verification possible: with no idle
    /// budget a watch exists only while a body streams it, so every path that
    /// consults a watch falls back to the live-body rule.
    #[test]
    fn a_zero_budget_leaves_only_live_bodies() {
        let now = 4_000u64;
        let clock = at(now);
        let watches = Arc::new(Watches::new(0, 200));
        let guard = watches.acquire_at("b.bin", (0, 100), Arc::clone(&clock));
        assert!(watches.live("b.bin", now));
        drop(guard);
        assert!(!watches.live("b.bin", now), "0 = only live bodies");
    }

    /// A response that arrives at a live-but-idle watch is a resume — the
    /// signal the metrics use to say whether a pause stayed free.
    #[test]
    fn a_second_body_at_an_idle_watch_is_a_resume() {
        let now = 10_000u64;
        let clock = at(now);
        let watches = Arc::new(Watches::new(900_000, 200));
        let first = watches.acquire_at("c.bin", (0, 100), Arc::clone(&clock));
        assert!(!first.resumed(), "the first body is a start, not a resume");
        drop(first);

        let second = watches.acquire_at("c.bin", (100, 200), Arc::clone(&clock));
        assert!(second.resumed(), "the watch was live and idle between them");

        let third = watches.acquire_at("c.bin", (200, 300), Arc::clone(&clock));
        assert!(!third.resumed(), "a body is already streaming it");
    }

    /// The map must not grow with every key the node has ever served.
    #[test]
    fn the_map_does_not_grow_with_every_key_ever_watched() {
        let now = 50_000u64;
        let clock = at(now);
        let watches = Arc::new(Watches::new(1_000, 200));
        for i in 0..100 {
            drop(watches.acquire_at(&format!("k{i}"), (0, 10), Arc::clone(&clock)));
        }
        assert!(watches.pins(now + 10_000).is_empty());
        assert_eq!(watches.inner.lock().unwrap().len(), 0);
    }

    /// The pin is bounded in bytes: a viewer deep inside a big object pins a
    /// neighbourhood, never the key.
    #[test]
    fn the_pin_is_bounded_regardless_of_object_size() {
        let now = 7u64;
        let clock = at(now);
        let watches = Arc::new(Watches::new(900_000, 1_000));
        let _guard = watches.acquire_at("big.bin", (50_000_000, 50_001_000), Arc::clone(&clock));
        let pin = watches.pin("big.bin", now).unwrap();
        assert_eq!(
            pin.end - pin.start,
            1_000,
            "the neighbourhood is the configured size, not the object's"
        );
        assert_eq!((pin.start, pin.end), (50_000_500, 50_001_500));
        assert_eq!(pin.anchor, 50_001_000, "the anchor is the end of the last response");
    }
}

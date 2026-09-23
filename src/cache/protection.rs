//! What is protected right now, and how much of it.
//!
//! Two independent protections guard a key's bytes. A **lease** (a response body
//! is alive — ADR-0017) shelters the whole key while it streams, plus a grace. A
//! **watch** (a key is being VIEWED — ADR-0018) protects a bounded neighbourhood
//! around the viewer's position, and outlives the bodies of one viewing session.
//!
//! Every eviction site needs the same two answers — is this key spared at all,
//! and if it is watched, how much of it must stay — and each of them used to
//! derive both from two receivers it had to know about. That is the shape where a
//! rule goes missing: a new pass that consults only leases silently loses
//! ADR-0018's rule that a pin is a preference to be spent, not an exemption, and
//! nothing about the code says so.
//!
//! So the union has a home. Where a pass asks about one key it asks for a
//! [`Verdict`] (point lookups); where it asks about the whole cache at once — the
//! age sweep — it asks for the set.

use std::collections::HashSet;
use std::sync::Arc;

use super::leases::Leases;
use super::watch::{Pin, Watches};

/// What a BUDGET pass needs to know about one key: whether a live body holds it
/// whole, and where a viewer's neighbourhood is.
///
/// The two protections are not the same question, and this is the difference the
/// budget turns on: a **lease** shelters the whole key, so a pass that may not
/// spend a pin skips it; a **watch** protects only its neighbourhood, and a key
/// watched with NO pin configured (`watch_pin_bytes = 0`) holds nothing back at
/// all — the budget still governs it. Reading the watch as a second reason to
/// spare the key is what this type exists to prevent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Verdict {
    /// A response body is alive on this key (ADR-0017).
    pub leased: bool,
    /// The neighbourhood a viewer is on, when there is one: the part a pass may
    /// still trim AROUND (ADR-0018).
    pub pin: Option<Pin>,
}

/// The two protections behind one interface, cloned into the receivers that make
/// eviction decisions so no caller has to know there are two of them.
#[derive(Clone)]
pub(crate) struct Protection {
    leases: Arc<Leases>,
    watches: Arc<Watches>,
}

impl Protection {
    pub(crate) fn new(leases: Arc<Leases>, watches: Arc<Watches>) -> Self {
        Self { leases, watches }
    }

    /// Keys nothing may sweep for inactivity: every key with a live body, plus
    /// every key a viewer is watching. A set, for the sweep that asks about the
    /// whole cache at once.
    pub(crate) fn spared(&self, now: u64) -> HashSet<String> {
        let mut spared = self.leases.protected(now);
        // A key mid-watch is not idle however old its last request is. That is
        // the whole point of a watch outliving its bodies (ADR-0018): a viewer
        // who pauses for longer than the read grace is still watching.
        spared.extend(self.watches.live_keys(now));
        spared
    }

    /// A budget pass's question about one key, as point lookups — so a pass that
    /// walks the cache asks once per key without building a set per key.
    pub(crate) fn verdict(&self, key: &str, now: u64) -> Verdict {
        Verdict { leased: self.leases.is_protected(key, now), pin: self.watches.pin(key, now) }
    }

    /// Whether ANYTHING is holding this key: a live body or a viewer. This is the
    /// union the age sweep spares and the relief valve consults — deliberately
    /// not the budget pass's question, which is [`Protection::verdict`].
    pub(crate) fn in_use(&self, key: &str, now: u64) -> bool {
        self.leases.is_protected(key, now) || self.watches.live(key, now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The union is BOTH protections: a lease alone spares a key, a watch alone
    /// spares it, and the point answer agrees with the set answer for every key
    /// — which is what lets a sweep use the set and a pass use the verdict.
    #[test]
    fn the_verdict_and_the_set_agree() {
        let leases = Arc::new(Leases::new(1_000));
        let watches = Arc::new(Watches::new(10_000, 4_096));
        let p = Protection::new(Arc::clone(&leases), Arc::clone(&watches));

        let _leased = leases.acquire_at("a.bin", Arc::new(crate::clock::MockClock::new(0)));
        let _watched =
            watches.acquire_at("b.bin", (0, 100), Arc::new(crate::clock::MockClock::new(0)));
        let now = 500;

        let set = p.spared(now);
        assert!(set.contains("a.bin"), "a live body spares the key");
        assert!(set.contains("b.bin"), "a viewing session spares the key");
        assert!(!set.contains("c.bin"), "nothing holds it");

        // The union agrees with the point answer...
        for key in ["a.bin", "b.bin", "c.bin"] {
            assert_eq!(p.in_use(key, now), set.contains(key), "{key} disagrees with the set");
        }
        // ...and the budget's question is a different one: a lease holds the key
        // whole (no pin), while a watch holds only its neighbourhood.
        assert!(p.verdict("a.bin", now).leased, "a live body leases the key");
        assert!(p.verdict("a.bin", now).pin.is_none(), "a lease has no neighbourhood");
        assert!(!p.verdict("b.bin", now).leased, "a viewer is not a lease");
        let pin = p.verdict("b.bin", now).pin.expect("a watch has a neighbourhood");
        assert!(pin.start == 0 && pin.end >= 100, "the pin covers the watched span: {pin:?}");
    }

    /// Both protections expire on their own clocks, and the verdict follows:
    /// this is the pair of rules the eviction passes rely on being current.
    #[test]
    fn a_verdict_is_about_now() {
        let leases = Arc::new(Leases::new(0));
        let watches = Arc::new(Watches::new(1_000, 4_096));
        let p = Protection::new(Arc::clone(&leases), Arc::clone(&watches));
        let _watched =
            watches.acquire_at("b.bin", (0, 100), Arc::new(crate::clock::MockClock::new(0)));
        assert!(p.in_use("b.bin", 500));
        // A watched key with a pin CONFIGURED offers its neighbourhood to the
        // budget: `pin` is that offer, and a budget pass may spend it.
        assert!(
            p.verdict("b.bin", 500).pin.is_some(),
            "a configured pin is the neighbourhood a budget pass may spend"
        );
        // With no pin configured the same watch holds nothing back from the
        // budget, live or idle (ADR-0018's other half) — the budget's question
        // is answered by `pin`, not by "is anybody watching".
        let no_pin_watches = Arc::new(Watches::new(1_000, 0));
        let p_no_pin = Protection::new(Arc::clone(&leases), Arc::clone(&no_pin_watches));
        let _watched_no_pin =
            no_pin_watches.acquire_at("b.bin", (0, 100), Arc::new(crate::clock::MockClock::new(0)));
        assert!(p_no_pin.in_use("b.bin", 500), "the watch is live either way");
        assert!(
            p_no_pin.verdict("b.bin", 500).pin.is_none(),
            "no pin configured means nothing held back"
        );
        drop(_watched);
        assert!(!p.in_use("b.bin", 5_000), "an idle watch stops holding the key");
    }
}

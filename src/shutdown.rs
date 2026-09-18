//! How long a shutdown should take.
//!
//! Pingora's graceful path stops the listener the instant SIGTERM arrives and
//! then sleeps its full grace period (300s by default) with no early exit for
//! an idle server, so a stop costs about five minutes of an origin that is
//! serving nothing. Measured on the oracle node: 305-309s per deploy.
//!
//! Because the listener is already closed when we get here, "nothing in
//! flight for a settle window" means there is nothing left to drain, and the
//! process can exit immediately instead of waiting out the drain. The settle
//! window also covers two smaller things: a connection accepted in the last
//! instant before the listener closed (the gauge increments early in a
//! connection's life but not at the socket), and the access-clock flusher's
//! one-second cadence, so pending LRU timestamps land before we exit.

use std::time::Duration;

/// What a shutdown should do after watching the in-flight count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopAction {
    /// Nothing was in flight for the whole window: exit now rather than
    /// waiting out the graceful drain.
    FastExit,
    /// Something was in flight: let the drain run to completion.
    Drain,
}

/// How long the in-flight count must stay at zero before a fast exit.
///
/// Long enough to cover a connection that was accepted just before the
/// listener closed, and longer than the one-second access-clock flush
/// cadence; short enough that an idle stop is still seconds rather than
/// minutes. A busy node pays this once and then drains as before.
pub const SETTLE_WINDOW: Duration = Duration::from_secs(3);

/// Spacing between samples inside the settle window.
pub const SETTLE_INTERVAL: Duration = Duration::from_millis(250);

/// Decide from an observed sequence of in-flight counts.
///
/// Any non-zero sample means something was in flight during the window, so
/// the drain is kept even if the count returned to zero afterwards: those
/// connections may still be finishing, and that is exactly what the drain
/// exists for. An empty sequence means nothing was observed, which must not
/// be read as "nothing was in flight".
pub fn stop_action(samples: &[i64]) -> StopAction {
    if samples.is_empty() {
        return StopAction::Drain;
    }
    if samples.iter().all(|n| *n <= 0) {
        StopAction::FastExit
    } else {
        StopAction::Drain
    }
}

/// Watch the in-flight count for one settle window, then decide.
///
/// `sample` is called repeatedly; a non-zero reading ends the window early so
/// a busy node starts draining at once instead of sitting out the window
/// first.
pub async fn observe_settle(sample: impl Fn() -> i64) -> StopAction {
    let rounds = (SETTLE_WINDOW.as_millis() / SETTLE_INTERVAL.as_millis().max(1)) as usize;
    let mut samples = Vec::with_capacity(rounds);
    for i in 0..rounds.max(1) {
        samples.push(sample());
        if stop_action(&samples) == StopAction::Drain {
            return StopAction::Drain;
        }
        // No trailing sleep: the last sample needs no wait after it.
        if i + 1 < rounds.max(1) {
            tokio::time::sleep(SETTLE_INTERVAL).await;
        }
    }
    stop_action(&samples)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idle_for_the_whole_window_exits_fast() {
        assert_eq!(stop_action(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]), StopAction::FastExit);
    }

    /// The interesting case: a connection that was in flight when the window
    /// started and finished during it must still keep the drain, because it
    /// may not be the only one and the drain is what protects it.
    #[test]
    fn anything_in_flight_keeps_the_drain() {
        assert_eq!(stop_action(&[0, 1, 0, 0]), StopAction::Drain, "seen then gone");
        assert_eq!(stop_action(&[3, 3, 3]), StopAction::Drain, "still running");
        assert_eq!(stop_action(&[0, 0, 0, 2]), StopAction::Drain, "arrived late");
    }

    /// Observing nothing is not evidence of idleness.
    #[test]
    fn an_empty_window_drains() {
        assert_eq!(stop_action(&[]), StopAction::Drain);
    }

    #[test]
    fn a_negative_reading_is_not_traffic() {
        assert_eq!(stop_action(&[-1, -1]), StopAction::FastExit);
    }

    #[tokio::test(start_paused = true)]
    async fn observe_settle_exits_fast_when_idle() {
        let action = observe_settle(|| 0).await;
        assert_eq!(action, StopAction::FastExit);
        // It really did spend the window, not just one sample.
        assert!(SETTLE_WINDOW >= SETTLE_INTERVAL * 2);
    }

    #[tokio::test(start_paused = true)]
    async fn observe_settle_drains_and_stops_sampling_once_busy() {
        let calls = std::cell::Cell::new(0);
        let action = observe_settle(|| {
            calls.set(calls.get() + 1);
            if calls.get() >= 3 {
                1
            } else {
                0
            }
        })
        .await;
        assert_eq!(action, StopAction::Drain);
        assert_eq!(calls.get(), 3, "sampling stops as soon as traffic is seen");
    }
}

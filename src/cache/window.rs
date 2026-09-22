//! The window decision: how far ahead one run reads.
//!
//! One policy, one home. Before this module the same question was answered in
//! four places — the run's own start (`session::Sessions::start`), the write
//! admission that has to afford it (`cache`), the cap an un-keepable key may
//! hold staged (`staging::working_window_bytes`) and the chain's bound
//! (`CHAIN_KEEP_AHEAD_WINDOWS`) — each deriving it from the config
//! independently. Changing the read-ahead policy meant changing four sites and
//! the tests scattered across them.
//!
//! What the accounts say (measured on the node, 2026-09-22, a real 200 GiB
//! object): a read that JUMPS to a cold offset staged exactly one configured
//! window — 67,108,864 bytes — for a 5 MiB read, a 12.8x amplification, while
//! the next 5 MiB inside that window cost 0.04 s. A jump and a continuation are
//! different shapes of traffic and want different windows; the old policy gave
//! both the full window.
//!
//! The rule here:
//!
//! - **A run with nothing behind it opens the floor** (`window_floor_bytes`,
//!   default 8 MiB): a jump pays for the read-ahead it uses, not for a window
//!   nobody will read.
//! - **A run that replaces one whose window was read out doubles it**, up to
//!   the configured window. A sequential walk therefore climbs to one open per
//!   `session_window_bytes` after a few windows, which is the account ADR-0016
//!   bought, while a scrub-heavy viewer keeps paying the floor per jump.
//! - **A request wider than either gets what it asked for**: the floor is a
//!   floor, never a cap, so `a_need_larger_than_the_window_widens_it` still
//!   holds and one large response still costs one open.
//!
//! The ramp only ever *shrinks* a window relative to the configured value, so
//! the reserve an un-keepable key may hold (`reserve_bytes`) needs no new
//! arithmetic: `watch_pin_bytes + session_window_bytes` stays the upper bound
//! (ADR-0019), and admission keeps asking about the worst case.

/// What the run being replaced achieved, as (`covered`, `consumed`) bytes.
/// `covered` is the window it held; `consumed` is how far its readers actually
/// reached into it. Both are relative to the replaced run's start.
pub(crate) type Behind = (u64, u64);

/// The floor, never more than the configured window: a configuration that asks
/// for a small window keeps its old behaviour exactly (the LAB's 256 KiB
/// configs, and every test that sets a window smaller than the default).
pub(crate) fn floor_bytes(configured: u64, floor: u64) -> u64 {
    floor.min(configured.max(1))
}

/// The window a run should hold: at least `need`, at least the floor, grown on
/// what the run it replaces consumed, never past the end of the object.
pub(crate) fn window_for(
    need: u64,
    remaining: u64,
    configured: u64,
    floor: u64,
    behind: Option<Behind>,
) -> u64 {
    let configured = configured.max(1);
    let base = match behind {
        // Read out: the reader wants the next window as much as it wanted this
        // one, so the read-ahead doubles — capped by what the config allows.
        Some((covered, consumed)) if covered > 0 && consumed >= covered => {
            covered.saturating_mul(2).min(configured)
        }
        // Nothing behind, or a window left half-read: the floor.
        _ => floor_bytes(configured, floor),
    };
    need.max(base).min(remaining)
}

/// Whether a replacement request continues the run it is replacing, and what
/// that run achieved if so.
///
/// A request that begins inside the window it replaces is a continuation (the
/// sequential walk, which arrives exactly at the frontier); one that begins
/// outside it is a jump — a new region, with nothing behind it to ramp on.
pub(crate) fn behind_of(
    start: u64,
    replaced_start: u64,
    replaced_end: u64,
    replaced_playhead: u64,
) -> Option<Behind> {
    if start >= replaced_start && start <= replaced_end {
        Some((
            replaced_end.saturating_sub(replaced_start),
            replaced_playhead.saturating_sub(replaced_start),
        ))
    } else {
        None
    }
}

/// The most a run starting from this request can write — what admission has to
/// afford (ADR-0019 asks about the WRITE).
///
/// The ramp can only shrink the live window below this, so asking for the worst
/// case is conservative by construction. It is deliberately not ramped: the
/// ramp's input (what the run being replaced consumed) is read under the lock
/// that reaps it, which is after admission has already answered, and guessing
/// low there would over-commit the disk.
pub(crate) fn worst_case_bytes(need: u64, remaining: u64, configured: u64) -> u64 {
    need.max(configured.max(1)).min(remaining)
}

/// `watch_pin_bytes + session_window_bytes`: how much of an object the magazine
/// can never hold one key may keep staged (ADR-0019). The CONFIGURED window,
/// not the live one: the ramp only shrinks a window, so this stays the upper
/// bound and the cap is never below the pin by construction.
pub(crate) fn reserve_bytes(watch_pin: u64, configured: u64) -> u64 {
    watch_pin.saturating_add(configured.max(1))
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1024 * 1024;
    const CONFIGURED: u64 = 64 * MIB;
    const FLOOR: u64 = 8 * MIB;
    const HUGE: u64 = 200 * 1024 * MIB;

    #[test]
    fn a_jump_opens_the_floor_and_not_the_configured_window() {
        // The measured case: a 5 MiB read at a cold offset.
        assert_eq!(window_for(5 * MIB, HUGE, CONFIGURED, FLOOR, None), FLOOR);
        // Even a 1 MiB shard, which is what the CDN actually asks for.
        assert_eq!(window_for(MIB, HUGE, CONFIGURED, FLOOR, None), FLOOR);
    }

    #[test]
    fn a_request_wider_than_the_floor_gets_what_it_asked_for() {
        assert_eq!(window_for(20 * MIB, HUGE, CONFIGURED, FLOOR, None), 20 * MIB);
        // And one wider than the configured window still widens it: the floor
        // is a floor, the window is not a cap (ADR-0016).
        assert_eq!(window_for(100 * MIB, HUGE, CONFIGURED, FLOOR, None), 100 * MIB);
    }

    #[test]
    fn a_read_out_window_doubles_and_a_half_read_one_does_not() {
        let read_out = Some((FLOOR, FLOOR));
        assert_eq!(window_for(0, HUGE, CONFIGURED, FLOOR, read_out), 2 * FLOOR);
        let half = Some((FLOOR, FLOOR / 2));
        assert_eq!(window_for(0, HUGE, CONFIGURED, FLOOR, half), FLOOR);
    }

    #[test]
    fn the_ramp_stops_at_the_configured_window() {
        for covered in [CONFIGURED, 2 * CONFIGURED, 100 * MIB] {
            assert_eq!(window_for(0, HUGE, CONFIGURED, FLOOR, Some((covered, covered))), CONFIGURED);
        }
    }

    #[test]
    fn a_need_still_widens_a_ramped_window() {
        // A chained successor is handed its window as its need; one request
        // that wants more than the ramp decided still gets its own length.
        assert_eq!(window_for(3 * FLOOR, HUGE, CONFIGURED, FLOOR, Some((FLOOR, FLOOR))), 3 * FLOOR);
    }

    #[test]
    fn the_object_end_clamps_every_branch() {
        assert_eq!(window_for(MIB, 3 * MIB, CONFIGURED, FLOOR, None), 3 * MIB);
        assert_eq!(window_for(0, MIB, CONFIGURED, FLOOR, Some((CONFIGURED, CONFIGURED))), MIB);
        assert_eq!(window_for(0, 0, CONFIGURED, FLOOR, None), 0);
    }

    #[test]
    fn a_floor_above_the_configured_window_is_clamped_to_it() {
        // A config with a small window keeps its exact old behaviour: the ramp
        // has nothing to give back.
        assert_eq!(floor_bytes(256 * 1024, FLOOR), 256 * 1024);
        assert_eq!(window_for(1024, HUGE, 256 * 1024, FLOOR, None), 256 * 1024);
        assert_eq!(window_for(0, HUGE, 256 * 1024, FLOOR, Some((256 * 1024, 256 * 1024))), 256 * 1024);
    }

    #[test]
    fn a_continuation_ramps_and_a_jump_does_not() {
        // The sequential walk arrives exactly at the frontier.
        assert_eq!(behind_of(1000, 500, 1000, 1000), Some((500, 500)));
        // Inside the replaced window also counts (a reader that fell behind).
        assert_eq!(behind_of(800, 500, 1000, 900), Some((500, 400)));
        // A far seek has nothing behind it.
        assert_eq!(behind_of(999_999, 500, 1000, 1000), None);
        assert_eq!(behind_of(400, 500, 1000, 1000), None);
        // A spent run whose readers never arrived cannot ramp a successor.
        assert_eq!(behind_of(1000, 500, 1000, 500), Some((500, 0)));
    }

    #[test]
    fn admission_asks_for_the_worst_case_the_policy_can_choose() {
        // Worst case = the configured window, whatever the ramp would pick for
        // this particular request.
        assert_eq!(worst_case_bytes(MIB, HUGE, CONFIGURED), CONFIGURED);
        assert_eq!(worst_case_bytes(100 * MIB, HUGE, CONFIGURED), 100 * MIB);
        // An object shorter than the window is all there is to write.
        assert_eq!(worst_case_bytes(MIB, 3 * MIB, CONFIGURED), 3 * MIB);
        assert_eq!(worst_case_bytes(0, 0, CONFIGURED), 0);
    }

    #[test]
    fn the_reserve_keeps_the_configured_window_as_its_upper_bound() {
        // ADR-0019's arithmetic, unchanged by the ramp: pin + configured.
        assert_eq!(reserve_bytes(128 * MIB, CONFIGURED), 192 * MIB);
        assert_eq!(reserve_bytes(0, 0), 1);
    }

    #[test]
    fn constant_decisions_are_pinned() {
        assert_eq!(crate::config::DEFAULT_WINDOW_FLOOR_BYTES, 8 * 1024 * 1024);
        // The floor is meaningful only below the default window; if a change
        // ever made it larger, every default-window jump would silently take
        // the full window again.
        assert!(crate::config::DEFAULT_WINDOW_FLOOR_BYTES < 64 * 1024 * 1024);
    }
}

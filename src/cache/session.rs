//! One upstream stream per key: the RUN (ADR-0016).
//!
//! The efficient profile opened an exact Range per ranged miss, and an
//! upstream `open` costs a fixed ~640 ms however short the range — so a
//! viewer's shard walk paid that cost once per request. `flight` already
//! solved this for whole-file cold pulls (one pump, many readers parked
//! behind a watermark), but a whole-file pump cannot serve an object the node
//! will never hold.
//!
//! A RUN is that idea bounded to a window: one `open` covering
//! `[start, start + window)`, pumped through the existing flight machinery
//! (watermark publication, inactivity-bounded waits, fsync + rename, panic
//! guard), sealed into a `.seg` span when it ends, and shared by every reader
//! whose request falls inside the window. Attaching is an OPTIMIZATION, never
//! a dependency: a reader the run does not cover opens its own Range instead,
//! exactly as before.

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
};

use futures::StreamExt;
use tokio::sync::Mutex;

use crate::{
    backend::{BackendSlot, ByteRange, Key},
    cache::{
        flight::{self, BodyStream, FlightProgress, FlightShared, Flights},
        staging::{FinalizedSpan, Staging},
        store,
    },
    clock::Clock,
    config::Config,
    metrics,
};

/// How far ahead of a reader's own position the chain may run, in windows.
///
/// One: a run may be started at most one window beyond what the playhead has
/// consumed. That bound is what stops a paused or departed viewer from
/// turning "watch the file" into "pull the file" — the chain needs a reader
/// that is *consuming*, not merely attached. Zero would disable chaining
/// (every window waits for a fresh request); two or more buys read-ahead at
/// the cost of buffering it.
const CHAIN_KEEP_AHEAD_WINDOWS: u64 = 1;

/// Everything a successor window needs to be started later: a run may chain
/// the next window once it is sealed and a reader is still consuming, and the
/// decision lives in [`Sessions::tick`] rather than in the run's own driver —
/// a driver that starts its successor would make `start` recursive, which is
/// both a type-inference problem (the future stops being provably `Send`) and
/// a needless coupling of the two windows.
pub(crate) struct Successor {
    slot: Arc<BackendSlot>,
    backend_key: Key,
    upstream_id: String,
    etag: Option<String>,
    total: u64,
}

/// Live run for one cache key.
pub(crate) struct Run {
    pub(crate) start: u64,
    /// Exclusive: the run promises `[start, end)`.
    pub(crate) end: u64,
    pub(crate) shared: Arc<FlightShared>,
    successor: Successor,
    /// Readers attached right now — a chained run is only worth starting while
    /// somebody is still consuming this one.
    readers: AtomicUsize,
    /// Highest absolute offset handed to a reader. A stalled playhead is how
    /// the chain tells a consuming reader from a paused one.
    playhead: AtomicU64,
    /// The driver reached a terminal state. A flag of the run's own rather
    /// than a read of the progress channel: `watch::Sender::send` fails —
    /// leaving the stored value untouched — when no receiver exists, which is
    /// exactly the case for a window nobody is reading.
    finished: std::sync::atomic::AtomicBool,
}

impl Run {
    /// Count a reader in for as long as its body lives.
    fn attach(self: &Arc<Self>) -> ReaderGuard {
        self.readers.fetch_add(1, Ordering::SeqCst);
        ReaderGuard(Arc::clone(self))
    }

    /// Whether the run covers the whole of `[start, end)`. The whole request,
    /// because a response's length is fixed before its first byte goes out.
    pub(crate) fn covers(&self, start: u64, end: u64) -> bool {
        start >= self.start && end <= self.end
    }

    /// Readers attached right now (the chain's condition).
    pub(crate) fn readers(&self) -> usize {
        self.readers.load(Ordering::SeqCst)
    }

    /// Highest offset any reader has consumed.
    pub(crate) fn playhead(&self) -> u64 {
        self.playhead.load(Ordering::SeqCst)
    }

    /// The driver has reached a terminal state: the window is sealed (or
    /// failed), so the entry is spent and a successor may take the key.
    fn is_terminal(&self) -> bool {
        self.finished.load(Ordering::SeqCst)
    }

    /// Whether a still-consuming reader makes the next window worth starting
    /// now, and where it would begin.
    ///
    /// `watched` is the caller's answer to "is this key still being viewed"
    /// (an attached body, or a watch inside its idle budget). It replaces the
    /// readers-only test: a viewer who paused keeps the read-ahead it already
    /// paid for, while the `next <= playhead + keep_ahead` bound — which does
    /// not move while the playhead is stalled — still stops the chain, so
    /// watching a file never becomes pulling it.
    fn wants_successor(&self, watched: bool, configured: u64, floor: u64) -> Option<(u64, u64)> {
        let next = self.end;
        // The ramp's input is this run's own account: how much it held and how
        // far its readers actually got. A window read out doubles the next one
        // (a sequential walk climbs back to one open per configured window); a
        // window left half-read, or a viewer that never arrived, falls back to
        // the floor — which is also what keeps a paused viewer from turning
        // "watch the file" into "pull the file", now in bytes rather than in
        // whole windows.
        let covered = self.end.saturating_sub(self.start);
        let consumed = self.playhead().saturating_sub(self.start);
        let window = super::window::window_for(
            0,
            self.successor.total.saturating_sub(next),
            configured,
            floor,
            Some((covered, consumed)),
        );
        let keep_ahead = window.saturating_mul(CHAIN_KEEP_AHEAD_WINDOWS);
        if next < self.successor.total
            && watched
            && next <= self.playhead().saturating_add(keep_ahead)
        {
            Some((next, window))
        } else {
            None
        }
    }
}

/// Keeps a run's reader count honest for the body's whole life, including the
/// disconnect case (axum drops the body, the guard drops with it).
struct ReaderGuard(Arc<Run>);

impl Drop for ReaderGuard {
    fn drop(&mut self) {
        self.0.readers.fetch_sub(1, Ordering::SeqCst);
    }
}

/// One key has at most one run, and the entry exists before any await so two
/// requests cannot both decide to start one: `Starting` is the reservation the
/// request holds while it waits for a stream permit, and a reader that finds it
/// takes its own Range (the run has no bytes to publish yet).
enum Slot {
    Starting,
    Live(Arc<Run>),
}

/// Per-key runs, keyed by cache key.
pub(crate) struct Sessions<C: Clock> {
    slots: Mutex<HashMap<String, Slot>>,
    flights: Flights,
    staging: Staging,
    config: Arc<Config>,
    clock: Arc<C>,
    /// Watches (ADR-0018). The chain's condition used to be "a reader is
    /// attached", which stops the moment a browser pauses and throws away the
    /// read-ahead it had already paid for; a watch says the *viewing session*
    /// is still there, bounded by its own idle budget.
    watches: Arc<super::watch::Watches>,
}

impl<C: Clock + 'static> Sessions<C> {
    pub(crate) fn new(
        config: Arc<Config>,
        clock: Arc<C>,
        staging: Staging,
        flights: Flights,
        watches: Arc<super::watch::Watches>,
    ) -> Self {
        Self { slots: Mutex::new(HashMap::new()), flights, staging, config, clock, watches }
    }

    /// The live run that can serve all of `[start, end)`, if there is one.
    pub(crate) async fn covering(&self, key: &str, start: u64, end: u64) -> Option<Arc<Run>> {
        let slots = self.slots.lock().await;
        match slots.get(key) {
            // A spent run is not attachable: its reader would park on a channel
            // that will never publish again instead of falling back to its own
            // Range.
            Some(Slot::Live(run)) if !run.is_terminal() && run.covers(start, end) => {
                Some(Arc::clone(run))
            }
            _ => None,
        }
    }

    /// Start a run at `start` covering at least `need` bytes (rounded up to a
    /// window, clamped to the object), or return `None` when a run is already
    /// live or starting for this key.
    ///
    /// One run at a time is what keeps the `.segpart` and the seal
    /// single-writer; a seek the live run does not cover takes the caller's own
    /// exact Range (ADR-0016's escape). That also covers the cold stampede: the
    /// first request reserves the key and pays the stream-permit wait itself,
    /// and its concurrent siblings fall back to one Range each — the same cost
    /// they had before runs existed.
    ///
    /// The run holds ONE stream permit for its whole life, taken here in the
    /// starting request's name, so the per-upstream budget is charged per
    /// window rather than per request.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn start(
        &self,
        slot: &Arc<BackendSlot>,
        key: &str,
        backend_key: Key,
        upstream_id: &str,
        etag: Option<String>,
        total: u64,
        start: u64,
        need: u64,
        // Where the viewer already IS, when the caller knows better than the
        // run's own start: a chained window inherits its predecessor's
        // playhead, which is what keeps a stalled viewer's chain to the
        // read-ahead it is owed. Without it every successor starts at zero,
        // its own end satisfies the bound, and a paused watch pulls the file
        // (measured on the node: 25 opens during one 90 s pause).
        playhead_seed: Option<u64>,
    ) -> Option<Arc<Run>> {
        // What the run we are replacing achieved, when this request continues
        // it. Read here because here is the only place it is visible: the same
        // lock that reaps the spent run is the last moment anybody can ask it
        // what it covered and how far its readers got.
        let mut behind = None;
        {
            let mut slots = self.slots.lock().await;
            match slots.get(key) {
                // Two handovers, one rule: a run this request cannot ride is
                // reaped and replaced here rather than at the next tick.
                //
                // - The run is SPENT (terminal): a finished run still owns the
                //   key until somebody reaps it, and that would make every
                //   request of a sequential walk take the standalone escape
                //   (its own exact Range, no window).
                // - The request begins exactly where a LIVE run's window ends:
                //   the sequential walk crossing a window boundary. Waiting for
                //   that run to seal is what made a small floor cost more opens
                //   than the waste it saved — the LAB measured a 24 MiB walk at
                //   8 opens (one per three shards), because each crossing
                //   escaped to its own Range instead of handing over.
                //
                // Taking the slot does not disturb the predecessor: its driver
                // keeps pumping into its own `.segpart`, seals on its own, and
                // its `finish` is pointer-checked, so it cannot clear the
                // successor's entry. A seek that does NOT continue it (a gap, a
                // far offset) still takes the escape below, which is what keeps
                // several viewers on one key from thrashing the slot.
                Some(Slot::Live(run)) if run.is_terminal() || start == run.end => {
                    behind = super::window::behind_of(start, run.start, run.end, run.playhead());
                    slots.remove(key);
                }
                Some(_) => return None,
                None => {}
            }
            slots.insert(key.to_string(), Slot::Starting);
        }
        // One question, one answer: how far ahead may this run read (see
        // `cache::window`). A jump opens the floor, a continuation ramps on what
        // the run it replaces consumed, and a request wider than either gets
        // its own length — the floor is a floor, not a cap (ADR-0016).
        let window = super::window::window_for(
            need,
            total.saturating_sub(start),
            self.config.session_window_bytes,
            self.config.window_floor_bytes,
            behind,
        );
        let end = start.saturating_add(window).min(total);
        let permit = if end > start {
            Arc::clone(&slot.stream_gate).acquire_owned().await.ok()
        } else {
            None
        };
        let Some(permit) = permit else {
            self.release(key).await;
            return None;
        };
        let tmp = store::segpart_path(&self.config.cache_dir, key, start, end);
        let final_path = store::seg_path(&self.config.cache_dir, key, start, end);
        // The run's own promise is the WINDOW, not the object: the short-body
        // guard must compare against what this stream asked for.
        let want = end - start;
        let cache_dir = self.config.cache_dir.clone();
        let staging = self.staging.clone();
        let clock = Arc::clone(&self.clock);
        let backend = Arc::clone(slot);
        // The handle is built here rather than inside `spawn_solo` so the run
        // exists (and the driver can name it) before the driver starts.
        let shared = Arc::new(FlightShared::new(tmp.clone(), final_path.clone(), flight::DEFAULT_STALL_BUDGET));
        let session_key = key.to_string();
        let backend_id = upstream_id.to_string();
        let shared_for_driver = Arc::clone(&shared);
        let run = Arc::new(Run {
            start,
            end,
            shared: Arc::clone(&shared),
            successor: Successor {
                slot: Arc::clone(slot),
                // The successor's own upstream key: it is what the NEXT window
                // opens against, so this one is live.
                backend_key: backend_key.clone(),
                upstream_id: backend_id.clone(),
                etag: etag.clone(),
                total,
            },
            readers: AtomicUsize::new(0),
            playhead: AtomicU64::new(playhead_seed.unwrap_or(start)),
            finished: std::sync::atomic::AtomicBool::new(false),
        });
        let driver_run = Arc::clone(&run);
        // Live before the driver can run: a driver that fails instantly would
        // otherwise find `Starting` (and clear nothing), leaving a spent entry
        // behind that no later request could replace.
        {
            let mut slots = self.slots.lock().await;
            slots.insert(key.to_string(), Slot::Live(Arc::clone(&run)));
        }
        self.flights.spawn_solo_over(shared_for_driver, move |flight| async move {
            let _permit = permit;
            let src = match backend.backend.open(&backend_key, Some(ByteRange::bounded(start, want))).await {
                Ok(mut s) => {
                    // The run's promise is the WINDOW it asked for. Every backend
                    // here already promises its range (ADR-0021), so this
                    // restates the contract rather than correcting a lie — and a
                    // backend that reported the OBJECT instead would have the
                    // pump accept, and the guard require, far more than this
                    // window.
                    s.promised_len = Some(want);
                    s
                }
                Err(e) => {
                    metrics::observe_session("open_failed");
                    let _ = flight.progress_tx.send(FlightProgress::Failed(e));
                    driver_run.finished.store(true, Ordering::SeqCst);
                    return;
                }
            };
            match flight::pump_and_seal(src, &tmp, &final_path, &flight.progress_tx).await {
                Ok(()) => {
                    let written = tokio::fs::metadata(&final_path).await.map(|m| m.len()).unwrap_or(want);
                    staging
                        .seal_span(FinalizedSpan {
                            cache_dir,
                            key: session_key.clone(),
                            upstream_id: backend_id.clone(),
                            etag: etag.clone(),
                            total,
                            start,
                            end: start + written,
                            bytes: written,
                            now_millis: clock.now_millis(),
                        })
                        .await;
                    metrics::observe_session("sealed");
                    let _ = flight.progress_tx.send(FlightProgress::Done);
                }
                Err(e) => {
                    metrics::observe_session("failed");
                    let _ = flight.progress_tx.send(FlightProgress::Failed(e));
                }
            }
            driver_run.finished.store(true, Ordering::SeqCst);
            // The entry is NOT cleared here: a request arriving between the
            // seal and the next tick still attaches to the sealed window, and
            // whoever needs the key next (the tick's chain decision, or the
            // next request) reaps the finished run itself.
        });
        Some(run)
    }

    /// A body for `[start, end)` served from this run's watermark. The reader
    /// parks until the writer passes its offset (inactivity-bounded, the
    /// flight's own contract) and publishes its progress back into the run.
    pub(crate) fn reader(run: Arc<Run>, start: u64, end: u64) -> BodyStream {
        let guard = run.attach();
        let follow = flight::growing_reader_from(Arc::clone(&run.shared), start - run.start, Some(end - start));
        Box::pin(async_stream::try_stream! {
            let _guard = guard;
            let mut cursor = start;
            let mut inner = follow;
            while let Some(chunk) = inner.next().await {
                let bytes = chunk?;
                cursor += bytes.len() as u64;
                run.playhead.fetch_max(cursor, Ordering::SeqCst);
                yield bytes;
            }
        })
    }

    /// Drive the chain: for every run whose window is sealed, either hand the
    /// key to a successor window (a reader is still consuming and close enough
    /// behind to need it) or clear the entry so the next request plans against
    /// the sealed span on disk.
    ///
    /// The chain is what moves the ~640 ms open off the reader's critical path:
    /// it happens while the reader is still working through the current window.
    /// Its bound is the reader's own position — a paused or departed viewer
    /// stops it — so "watch the file" never becomes "pull the file".
    ///
    /// Driven by a timer (the Cache spawns it) and directly by tests, which is
    /// what makes the chain decision deterministic to assert on.
    pub(crate) async fn tick(&self) {
        let spent: Vec<(String, Arc<Run>)> = {
            let slots = self.slots.lock().await;
            slots
                .iter()
                .filter_map(|(k, s)| match s {
                    Slot::Live(run) if run.is_terminal() => Some((k.clone(), Arc::clone(run))),
                    _ => None,
                })
                .collect()
        };
        for (key, run) in spent {
            // A body still attached counts as watching even with the idle
            // budget turned off, so `watch_idle_secs = 0` is exactly the
            // pre-ADR-0018 rule and the reverse verification is a config flip.
            let watched =
                run.readers() > 0 || self.watches.live(&key, self.clock.now_millis());
            let next = run.wants_successor(
                watched,
                self.config.session_window_bytes,
                self.config.window_floor_bytes,
            );
            // The spent entry goes first: `start` refuses a key that still has
            // one, so chaining before releasing would never fire. `finish` is
            // pointer-checked, and two ticks racing here both release (the
            // second is a no-op) and then race on `start`, where the loser sees
            // the winner's entry and stops.
            self.finish(&key, &run).await;
            if let Some((next_start, window)) = next {
                let successor = &run.successor;
                let started = self
                    .start(
                        &successor.slot,
                        &key,
                        successor.backend_key.clone(),
                        &successor.upstream_id,
                        successor.etag.clone(),
                        successor.total,
                        next_start,
                        window,
                        Some(run.playhead()),
                    )
                    .await
                    .is_some();
                if started {
                    metrics::observe_session("chained");
                }
            }
        }
    }

    /// Drop the reservation when the run could not be started at all.
    async fn release(&self, key: &str) {
        let mut slots = self.slots.lock().await;
        if matches!(slots.get(key), Some(Slot::Starting)) {
            slots.remove(key);
        }
    }

    /// Remove this run's entry once its driver is done. `ptr_eq` keeps a late
    /// finisher from evicting a successor's entry.
    async fn finish(&self, key: &str, run: &Arc<Run>) {
        let mut slots = self.slots.lock().await;
        if let Some(Slot::Live(current)) = slots.get(key) {
            if Arc::ptr_eq(current, run) {
                slots.remove(key);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        cache::store::{self, Coverage},
        clock::MockClock,
        testsupport::SizedBackend,
    };
    use std::collections::HashMap;

    fn sessions(dir: &std::path::Path, window: u64, object_bytes: u64, opens: Arc<AtomicUsize>) -> (Arc<Sessions<MockClock>>, Arc<BackendSlot>) {
        // No idle budget: a watch exists only while a body streams it, which
        // is the pre-ADR-0018 chain rule these tests were written against.
        let (sessions, slot, _watches) = sessions_watching(dir, window, object_bytes, opens, 0);
        (sessions, slot)
    }

    fn sessions_watching(
        dir: &std::path::Path,
        window: u64,
        object_bytes: u64,
        opens: Arc<AtomicUsize>,
        watch_idle_ms: u64,
    ) -> (Arc<Sessions<MockClock>>, Arc<BackendSlot>, Arc<crate::cache::watch::Watches>) {
        sessions_tuned(
            dir,
            window,
            crate::config::DEFAULT_WINDOW_FLOOR_BYTES,
            object_bytes,
            opens,
            watch_idle_ms,
        )
    }

    /// The same harness with the window decision's floor set explicitly. Tests
    /// above leave it at the default, which is clamped up to their small
    /// windows and so invisible to them; a test that asserts on the ramp needs
    /// a window bigger than the floor.
    fn sessions_tuned(
        dir: &std::path::Path,
        window: u64,
        floor: u64,
        object_bytes: u64,
        opens: Arc<AtomicUsize>,
        watch_idle_ms: u64,
    ) -> (Arc<Sessions<MockClock>>, Arc<BackendSlot>, Arc<crate::cache::watch::Watches>) {
        let clock = Arc::new(MockClock::new(0));
        let mut cfg = Config {
            cache_dir: dir.to_path_buf(),
            ..Config::default()
        };
        cfg.session_window_bytes = window;
        cfg.window_floor_bytes = floor;
        let cfg = Arc::new(cfg);
        let coverage = Arc::new(Mutex::new(HashMap::<String, Coverage>::new()));
        let state = Arc::new(tokio::sync::RwLock::new(crate::cache::cache::CacheState::default()));
        let leases = Arc::new(crate::cache::leases::Leases::new(0));
        let watches = Arc::new(crate::cache::watch::Watches::new(watch_idle_ms, 4096));
        let staging = Staging::new(
            Arc::clone(&coverage),
            Arc::clone(&state),
            Arc::clone(&cfg),
            leases,
            Arc::clone(&watches),
        );
        let flights = Flights::new(flight::DEFAULT_STALL_BUDGET);
        let sessions =
            Arc::new(Sessions::new(cfg, Arc::clone(&clock), staging, flights, Arc::clone(&watches)));
        let backend = Arc::new(SizedBackend::new(&[("a.bin", object_bytes)], opens));
        let slot = Arc::new(BackendSlot::new(backend, 3));
        (sessions, slot, watches)
    }

    fn key() -> Key {
        Key::from_validated("a.bin".to_string())
    }

    async fn start(sessions: &Arc<Sessions<MockClock>>, slot: &Arc<BackendSlot>, total: u64) -> Arc<Run> {
        sessions
            .start(slot, "a.bin", key(), "primary", Some("v1".into()), total, 0, 0, None)
            .await
            .expect("a run must start when the key has no live one")
    }

    /// Wait for the driver to reach a terminal state (the seal lands after the
    /// last byte, so tests poll rather than sleep a guessed interval).
    async fn wait_terminal(run: &Arc<Run>) {
        for _ in 0..400 {
            if run.is_terminal() {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        panic!("the run never reached a terminal state");
    }

    /// A reader that is still consuming the sealed window is what makes the
    /// next window worth starting: the tick hands the key over, so the open
    /// happens ahead of the reader instead of at it.
    #[tokio::test]
    async fn a_consuming_reader_chains_the_next_window() {
        let dir = tempfile::tempdir().unwrap();
        let opens = Arc::new(AtomicUsize::new(0));
        let (sessions, slot) = sessions(dir.path(), 4096, 64 * 1024, Arc::clone(&opens));
        let run = start(&sessions, &slot, 64 * 1024).await;

        let mut body = Sessions::<MockClock>::reader(Arc::clone(&run), 0, 4096);
        // Pull one chunk and stop: mid-window is the state a real viewer is in
        // when the window it is reading seals (a body that has been fully
        // consumed drops its reader guard, and there is nothing left to chain
        // for anyway).
        let first = futures::StreamExt::next(&mut body).await.expect("a chunk").unwrap();
        assert!(!first.is_empty() && run.playhead() > 0, "the reader is consuming");
        wait_terminal(&run).await;
        assert!(run.readers() > 0, "the reader is still attached (its body is alive)");

        sessions.tick().await;

        let next = sessions
            .covering("a.bin", 4096, 8192)
            .await
            .expect("a consuming reader must chain the next window");
        // The successor's driver runs detached, so its open lands after the
        // handover: wait for that window to seal before counting opens.
        wait_terminal(&next).await;
        assert_eq!(opens.load(Ordering::SeqCst), 2, "one open per window");
        drop(body);
    }

    /// The window decision at both ends: a jump opens the floor, and a window
    /// a reader reads out doubles the next one. Measured on the node before
    /// this rule existed: a 5 MiB cold jump staged a whole 64 MiB window
    /// (12.8x) while the next 5 MiB inside that window cost 0.04 s.
    ///
    /// The floor here is below one publisher chunk (`ranged::CHUNK`, 256 KiB),
    /// because "read out" has to be reachable by pulling a single chunk: a
    /// window at or below the chunk size arrives in one piece, a bigger one
    /// only ever arrives partly.
    #[tokio::test]
    async fn a_jump_opens_the_floor_and_a_read_out_window_doubles_the_next_one() {
        let dir = tempfile::tempdir().unwrap();
        let opens = Arc::new(AtomicUsize::new(0));
        let floor = 64 * 1024;
        let (sessions, slot, _watches) =
            sessions_tuned(dir.path(), 4 * 1024 * 1024, floor, 16 * 1024 * 1024, Arc::clone(&opens), 0);

        // Nothing behind it: the floor, not the configured window.
        let run = start(&sessions, &slot, 16 * 1024 * 1024).await;
        assert_eq!(run.end - run.start, floor, "a jump opens the floor");

        // Read that window out (one chunk, because the floor IS one chunk) and
        // let the tick chain the next one.
        let mut body = Sessions::<MockClock>::reader(Arc::clone(&run), 0, floor);
        let first = futures::StreamExt::next(&mut body).await.expect("a chunk").unwrap();
        assert_eq!(first.len() as u64, floor, "one chunk reads the floor out");
        wait_terminal(&run).await;
        sessions.tick().await;

        let successor = sessions
            .covering("a.bin", floor, floor * 2)
            .await
            .expect("a read-out window chains the next one");
        assert_eq!(
            successor.end - successor.start,
            floor * 2,
            "a read-out window doubles the next one"
        );
        // The successor's window is capped by the configured one (4 MiB), and
        // the open still happens once per window.
        wait_terminal(&successor).await;
        assert_eq!(opens.load(Ordering::SeqCst), 2, "one open per window");
        drop(body);
    }

    /// A window left partly read ramps nothing: the ramp is a reward for being
    /// read out, so the successor falls back to the floor. This is the byte
    /// version of "a stalled viewer buys the read-ahead it is owed" — the
    /// read-ahead it is owed is now the floor, not a whole window.
    #[tokio::test]
    async fn a_partly_read_window_ramps_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let opens = Arc::new(AtomicUsize::new(0));
        let floor = 1024 * 1024;
        let (sessions, slot, _watches) =
            sessions_tuned(dir.path(), 4 * 1024 * 1024, floor, 16 * 1024 * 1024, Arc::clone(&opens), 0);

        let run = start(&sessions, &slot, 16 * 1024 * 1024).await;
        assert_eq!(run.end - run.start, floor);
        // One chunk of a floor-sized window: the reader is inside it, not
        // through it.
        let mut body = Sessions::<MockClock>::reader(Arc::clone(&run), 0, floor);
        let first = futures::StreamExt::next(&mut body).await.expect("a chunk").unwrap();
        assert!((first.len() as u64) < floor, "the reader stopped inside the window");
        wait_terminal(&run).await;
        sessions.tick().await;

        let successor = sessions
            .covering("a.bin", floor, floor * 2)
            .await
            .expect("the read-ahead a stopped reader is owed is still granted");
        assert_eq!(
            successor.end - successor.start,
            floor,
            "a partly-read window ramps nothing: the next one is the floor again"
        );
        drop(body);
    }

    /// The boundary handover: a request that begins exactly where a run's
    /// window ends takes the key over rather than escaping to its own Range.
    ///
    /// This test pins the outcome (a run comes back, with the ramp's window);
    /// which arm of the handover fires depends on whether the predecessor had
    /// sealed yet, and the LIVE arm is what the LAB's 24-shard walk measures —
    /// before the handover it cost 8 opens, one per three shards, because every
    /// crossing escaped.
    #[tokio::test]
    async fn a_request_at_a_windows_end_takes_the_key_over_instead_of_escaping() {
        let dir = tempfile::tempdir().unwrap();
        let opens = Arc::new(AtomicUsize::new(0));
        let (sessions, slot, _watches) =
            sessions_tuned(dir.path(), 4 * 1024 * 1024, 64 * 1024, 16 * 1024 * 1024, Arc::clone(&opens), 0);

        let run = start(&sessions, &slot, 16 * 1024 * 1024).await;
        assert_eq!(run.end - run.start, 64 * 1024, "the first window is the floor");

        let next = sessions
            .start(
                &slot,
                "a.bin",
                key(),
                "primary",
                Some("v1".into()),
                16 * 1024 * 1024,
                run.end,
                0,
                None,
            )
            .await
            .expect("a request at the window's end hands over instead of escaping");
        assert_eq!(next.start, run.end, "the successor begins where the window ended");
        // Nothing had been read out of the first window, so the ramp stays at
        // the floor: a handover is about the slot, not about read-ahead.
        assert_eq!(next.end - next.start, 64 * 1024);
        wait_terminal(&run).await;
    }

    /// A request that begins OUTSIDE the run it replaces is a jump: it opens
    /// the floor even when that run was read out, so a scrub cannot inherit the
    /// ramp of a region it is not continuing.
    #[tokio::test]
    async fn a_far_seek_after_a_read_out_window_still_opens_the_floor() {
        let dir = tempfile::tempdir().unwrap();
        let opens = Arc::new(AtomicUsize::new(0));
        let floor = 256 * 1024;
        let (sessions, slot, _watches) =
            sessions_tuned(dir.path(), 4 * 1024 * 1024, floor, 16 * 1024 * 1024, Arc::clone(&opens), 0);

        let run = start(&sessions, &slot, 16 * 1024 * 1024).await;
        let mut body = Sessions::<MockClock>::reader(Arc::clone(&run), 0, floor);
        let first = futures::StreamExt::next(&mut body).await.expect("a chunk").unwrap();
        assert!(!first.is_empty(), "the replacement run was being read");
        wait_terminal(&run).await;

        // 4 MiB away: not a continuation of [0, floor).
        let far = sessions
            .start(
                &slot,
                "a.bin",
                key(),
                "primary",
                Some("v1".into()),
                16 * 1024 * 1024,
                4 * 1024 * 1024,
                0,
                None,
            )
            .await
            .expect("a far seek starts its own run");
        assert_eq!(far.end - far.start, floor, "a far seek opens the floor, not the ramp");
        drop(body);
    }

    /// The chain is bounded by the READER'S POSITION, not by "a reader exists":
    /// a viewer that stops consuming (paused, or gone without dropping the
    /// body) buys at most the configured read-ahead and then the chain stops,
    /// so watching a file never becomes pulling it.
    #[tokio::test]
    async fn a_paused_reader_buys_at_most_one_window_of_read_ahead() {
        let dir = tempfile::tempdir().unwrap();
        let opens = Arc::new(AtomicUsize::new(0));
        let (sessions, slot) = sessions(dir.path(), 4096, 64 * 1024, Arc::clone(&opens));
        let run = start(&sessions, &slot, 64 * 1024).await;

        // Attached but never consumed: the playhead stays at the window start.
        let body = Sessions::<MockClock>::reader(Arc::clone(&run), 0, 4096);
        wait_terminal(&run).await;
        sessions.tick().await;
        let successor = sessions
            .covering("a.bin", 4096, 8192)
            .await
            .expect("one window of read-ahead is granted");
        wait_terminal(&successor).await;

        sessions.tick().await;
        assert!(
            sessions.covering("a.bin", 8192, 12288).await.is_none(),
            "the chain must stop one window ahead of a stalled playhead"
        );
        assert_eq!(opens.load(Ordering::SeqCst), 2, "two windows, then nothing");
        assert_eq!(run.playhead(), 0, "the reader never consumed a byte");
        drop(body);
    }

    /// A request near the end of the object stages only to the end: the window
    /// is clamped to what exists, so there is no over-fetch and no phantom
    /// span.
    #[tokio::test]
    async fn the_window_is_clamped_to_the_object() {
        let dir = tempfile::tempdir().unwrap();
        let opens = Arc::new(AtomicUsize::new(0));
        let (sessions, slot) = sessions(dir.path(), 4096, 10_000, Arc::clone(&opens));
        // A seek 1000 bytes before the end asks for a 4096-byte window.
        let run = sessions
            .start(&slot, "a.bin", key(), "primary", Some("v1".into()), 10_000, 9000, 1000, None)
            .await
            .unwrap();
        assert_eq!((run.start, run.end), (9000, 10_000), "clamped to the object");
        wait_terminal(&run).await;
        let spans: Vec<(u64, u64)> = store::segments_for_key(&dir.path().to_path_buf(), "a.bin")
            .into_iter()
            .map(|(s, e, _)| (s, e))
            .collect();
        assert_eq!(spans, vec![(9000, 10_000)]);
    }

    /// A viewer who PAUSES keeps the read-ahead it already paid for: the
    /// chain's condition is the watch, not an attached body, so the next
    /// window is fetched while the viewer is away (ADR-0018). Without this the
    /// pause threw away the window and the resume paid a fresh ~640 ms open.
    #[tokio::test]
    async fn a_paused_viewer_keeps_the_chain_inside_its_watch_budget() {
        let dir = tempfile::tempdir().unwrap();
        let opens = Arc::new(AtomicUsize::new(0));
        // 15 minutes of watch: the pause is inside it.
        let (sessions, slot, watches) =
            sessions_watching(dir.path(), 4096, 64 * 1024, Arc::clone(&opens), 900_000);
        let run = start(&sessions, &slot, 64 * 1024).await;

        // The viewer watches the first window and then goes quiet: the body is
        // dropped, so nothing is streaming the key — only the watch is left.
        let watch = watches.acquire_at("a.bin", (0, 4096), Arc::new(MockClock::new(0)));
        {
            let mut body = Sessions::<MockClock>::reader(Arc::clone(&run), 0, 4096);
            let _ = futures::StreamExt::next(&mut body).await;
        }
        assert_eq!(run.readers(), 0, "the body is gone");
        drop(watch);
        wait_terminal(&run).await;

        sessions.tick().await;
        let next = sessions
            .covering("a.bin", 4096, 8192)
            .await
            .expect("the watch keeps the chain for one more window");
        wait_terminal(&next).await;
        assert_eq!(opens.load(Ordering::SeqCst), 2, "the read-ahead was fetched during the pause");
    }

    /// The chain is bounded by the VIEWER's position, not by the window it is
    /// chaining: a successor inherits its predecessor's playhead, so a stalled
    /// viewer buys the one window of read-ahead it is owed and nothing more.
    /// Without the inheritance every successor starts at zero, its own end
    /// satisfies the bound, and a paused watch pulls the object — measured on
    /// the node as 25 upstream opens during a single 90 s pause.
    #[tokio::test]
    async fn a_stalled_watch_buys_one_window_and_no_more() {
        let dir = tempfile::tempdir().unwrap();
        let opens = Arc::new(AtomicUsize::new(0));
        let (sessions, slot, watches) =
            sessions_watching(dir.path(), 4096, 1 << 20, Arc::clone(&opens), 900_000);
        let run = start(&sessions, &slot, 1 << 20).await;

        // A viewer consumes a little of the first window and then walks away:
        // the body drops, the watch stays.
        let watch = watches.acquire_at("a.bin", (0, 512), Arc::new(MockClock::new(0)));
        {
            let mut body = Sessions::<MockClock>::reader(Arc::clone(&run), 0, 512);
            let _ = futures::StreamExt::next(&mut body).await;
        }
        drop(watch);
        wait_terminal(&run).await;

        // Many ticks over the gap: the chain must not keep going.
        for _ in 0..6 {
            sessions.tick().await;
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert_eq!(
            opens.load(Ordering::SeqCst),
            2,
            "one window, plus the one window of read-ahead a stalled viewer is owed"
        );
    }

    /// The reverse verification for the rule above: with watching turned off
    /// (`watch_idle_secs = 0`) a departed body stops the chain exactly as it
    /// did before ADR-0018. One config flip, two opposite outcomes.
    #[tokio::test]
    async fn a_departed_viewer_stops_the_chain_when_watching_is_off() {
        let dir = tempfile::tempdir().unwrap();
        let opens = Arc::new(AtomicUsize::new(0));
        let (sessions, slot) = sessions(dir.path(), 4096, 64 * 1024, Arc::clone(&opens));
        let run = start(&sessions, &slot, 64 * 1024).await;
        {
            let mut body = Sessions::<MockClock>::reader(Arc::clone(&run), 0, 4096);
            let _ = futures::StreamExt::next(&mut body).await;
        }
        assert_eq!(run.readers(), 0);
        wait_terminal(&run).await;

        sessions.tick().await;
        assert!(
            sessions.covering("a.bin", 4096, 8192).await.is_none(),
            "no watch, no reader: the chain must not run"
        );
        assert_eq!(opens.load(Ordering::SeqCst), 1, "one window, and nothing after it");
    }

    /// A request whose gap is larger than the window widens it: nothing is
    /// fetched twice for one response, so the open count stays one.
    #[tokio::test]
    async fn a_need_larger_than_the_window_widens_it() {
        let dir = tempfile::tempdir().unwrap();
        let opens = Arc::new(AtomicUsize::new(0));
        let (sessions, slot) = sessions(dir.path(), 1024, 1 << 20, Arc::clone(&opens));
        let run = sessions
            .start(&slot, "a.bin", key(), "primary", Some("v1".into()), 1 << 20, 0, 8192, None)
            .await
            .unwrap();
        assert_eq!(run.end, 8192, "the request's own need is the floor");
    }
}

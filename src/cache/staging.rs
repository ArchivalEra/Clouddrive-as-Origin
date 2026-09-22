//! The staging ledger: the efficient profile's transfer history (spec §3.12).
//!
//! One completed staged interval enters here (`seal_span`), the coverage
//! ledger is merged and decayed in a single pass, and reads are planned
//! against it (`plan_staged`) so a request whose bytes are already staged is
//! answered without opening upstream. BOTH sealing paths — the
//! viewer-disconnect watcher and the body's exhaustion tail — go through
//! [`Staging::seal_span`], so the ledger has exactly one writer shape.
//!
//! Since staged spans became directly servable, this ledger IS the cache for
//! objects the magazine cannot hold whole: a sliding window, ejected
//! oldest-touched-first by the tick (ADR-0015). Promotion no longer exists;
//! there is nothing to assemble and nothing to wait for.
//!
//! Lock discipline (C3, ADR-0008): the coverage lock is taken before any
//! state guard, never after; redb and filesystem work happens outside both.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

use crate::{
    cache::{flight::BodyStream, store},
    config::{Config, EvictionPolicy},
};

use super::cache::CacheState;

/// Never evict a staging ledger row younger than this (P56): an active
/// transfer's row is touched continuously and must not be yanked mid-flight.
const STAGE_MIN_AGE_MS: u64 = 60_000;

/// How many trailing spans the HEAT policy compares at a time: the coldest
/// span inside the newest window goes first, and only when that window cannot
/// cover the overshoot does the eviction widen one window back. Heat never
/// compares spans across keys — the row order already carries the cross-key
/// LRU.
const HEAT_TRAILING_WINDOW: usize = 20;

/// One completed staged interval, ready to merge. `now_millis` is supplied by
/// the caller's clock domain — the ledger never reads a clock itself.
pub(crate) struct FinalizedSpan {
    pub(crate) cache_dir: std::path::PathBuf,
    pub(crate) key: String,
    /// Which upstream's profile decides this span's window decay. The
    /// provider-side path is deliberately NOT carried: nothing reads it.
    pub(crate) upstream_id: String,
    pub(crate) etag: Option<String>,
    pub(crate) total: u64,
    pub(crate) start: u64,
    pub(crate) end: u64,
    pub(crate) bytes: u64,
    pub(crate) now_millis: u64,
}

/// The staging ledger's receiver: the pieces every ledger operation needs,
/// taken once instead of re-plumbed through a dozen parameters. Cloned into
/// the sealing watcher and the body, which outlive the request.
#[derive(Clone)]
pub(crate) struct Staging {
    coverage: Arc<Mutex<HashMap<String, store::Coverage>>>,
    state: Arc<RwLock<CacheState>>,
    config: Arc<Config>,
    leases: Arc<super::leases::Leases>,
    /// Watches: a key being VIEWED, which outlives its response bodies
    /// (ADR-0018). A lease says a body is alive; a watch says the viewer is
    /// still there and where — and that position is what turns "spare the
    /// whole key" into a bounded neighbourhood the budget can still spend
    /// around.
    watches: Arc<super::watch::Watches>,
}

impl Staging {
    pub(crate) fn new(
        coverage: Arc<Mutex<HashMap<String, store::Coverage>>>,
        state: Arc<RwLock<CacheState>>,
        config: Arc<Config>,
        leases: Arc<super::leases::Leases>,
        watches: Arc<super::watch::Watches>,
    ) -> Self {
        Self { coverage, state, config, leases, watches }
    }

    /// The etag the ledger last saw for a key: the version gate's peek.
    pub(crate) async fn known_etag(&self, key: &str) -> Option<String> {
        self.coverage.lock().await.get(key).and_then(|e| e.etag.clone())
    }

    /// Ledger + interval counts for healthz.
    pub(crate) async fn summary(&self) -> (usize, usize) {
        let cov = self.coverage.lock().await;
        (cov.len(), cov.values().map(|c| c.intervals.len()).sum())
    }

    /// Ledger rows idle past the inactivity TTL (swept with their sidecars).
    /// A row with a live read lease, or one read inside `read_grace_secs`, is
    /// not idle however old its last request is (ADR-0017).
    pub(crate) async fn expired(&self, ttl_ms: u64, now: u64) -> Vec<String> {
        let mut spared = self.leases.protected(now);
        // A key mid-watch is not idle however old its last request is. That is
        // the whole point of a watch outliving its bodies (ADR-0018): a viewer
        // who pauses for longer than the read grace is still watching.
        spared.extend(self.watches.live_keys(now));
        let cov = self.coverage.lock().await;
        cov.iter()
            .filter(|(_, c)| now.saturating_sub(c.last_touch_millis) >= ttl_ms)
            .filter(|(k, _)| !spared.contains(k.as_str()))
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Evict staged spans until `need_bytes` is freed. ONE mechanism, two
    /// orderings, chosen by `eviction_policy` (ADR-0015):
    ///
    /// - `lru`: the row's stalest span goes first (`t`, the time its bytes
    ///   were staged) — a plain sliding window over what the walk wrote most
    ///   recently;
    /// - `heat`: spans are compared by READ COUNT inside the trailing window
    ///   (the newest [`HEAT_TRAILING_WINDOW`] by offset, measured back from
    ///   the walk's frontier); when that window cannot cover the need, the
    ///   next window back enters, so the next-coldest spans go.
    ///
    /// Both are SPAN-level, and rows are visited least-recently-touched first
    /// (the choice ACROSS keys is LRU under either policy). The row-level
    /// delete this replaced discarded an entire key's window — for the
    /// single-big-object workload that is the whole magazine — on any
    /// overshoot.
    ///
    /// File deletion, the ledger rebuild and the `segment_bytes` subtraction
    /// live in this module and nowhere else: the earlier shape returned span
    /// lists to `tick`, which fed them into the row-level delete path —
    /// undoing the span eviction and subtracting the same bytes twice.
    ///
    /// Returns `(keys touched, bytes freed)`.
    pub(crate) async fn evict_staged(&self, need_bytes: u64, now: u64) -> (usize, u64) {
        let protected = self.leases.protected(now);
        let pins = self.watches.pins(now);
        let order = self.rows_by_age().await;
        let mut freed_total = 0u64;
        let mut touched = 0usize;

        // (1) The global budget. Two passes over the whole cache, not one pass
        // into each row: everything a viewer is NOT looking at first, and the
        // pins only if the rest of the cache could not cover the need. Per row,
        // the first watched row would be trimmed inside its own pin while a
        // later row still had bytes to give — a deadline, but the last one
        // (ADR-0012's rule, restated by ADR-0018).
        for inside_pins in [false, true] {
            for (_, key) in &order {
                if freed_total >= need_bytes {
                    break;
                }
                // A watched key keeps its neighbourhood, not its whole row
                // (ADR-0018). A key with a body but no look at it keeps the
                // older, coarser rule.
                let pin = pins.get(key).copied();
                if pin.is_none() && protected.contains(key) {
                    continue;
                }
                let freed = self
                    .trim_row(key, need_bytes.saturating_sub(freed_total), pin, inside_pins, None, now)
                    .await;
                if freed > 0 {
                    freed_total += freed;
                    touched += 1;
                }
            }
        }

        // (2) The per-key working window (ADR-0019). A key whose object the
        // magazine can never hold whole is capped at `watch_pin_bytes + one
        // window`: the neighbourhood its reader is on, plus the window that is
        // its read-ahead. The cap holds even while the magazine is globally
        // UNDER budget — otherwise "one open per window" for a large object
        // would come with an unbounded disk cost, and a single walk would fill
        // the magazine and evict everyone else.
        //
        // Only rows the ledger already knows are un-keepable are visited, so
        // the hot path above is unchanged and this pass costs nothing on a
        // cache that holds only keepable objects. The cap is >= the pin by
        // construction (pin + one window), so a row within its cap can never
        // force the pin to be spent: the bytes outside the pin are enough.
        let cap = self.working_window_bytes();
        for key in self.unkeepable_rows().await {
            if protected.contains(&key) && !pins.contains_key(&key) {
                continue; // a live body with no watch: ADR-0017's rule
            }
            if freed_total >= need_bytes {
                // The global need is met; the cap is the only reason left.
                // Still enforced, which is the point of this pass.
            }
            let pin = pins.get(&key).copied();
            let freed = self.trim_row(&key, 0, pin, false, Some(cap), now).await;
            if freed > 0 {
                freed_total += freed;
                touched += 1;
                crate::metrics::observe_transient_trim(freed);
            }
        }
        (touched, freed_total)
    }

    /// `watch_pin_bytes + session_window_bytes`: how much one key whose object
    /// the magazine can never hold may keep staged (ADR-0019). >= the pin by
    /// construction, so the cap never forces the pin to be spent.
    fn working_window_bytes(&self) -> u64 {
        super::window::reserve_bytes(self.config.watch_pin_bytes, self.config.session_window_bytes)
    }

    /// How many keys are in the un-keepable class right now (the gauge).
    pub(crate) async fn unkeepable_keys(&self) -> usize {
        self.unkeepable_rows().await.len()
    }

    /// The keys whose object cannot fit the magazine — the working-window
    /// class. Read from the ledger (no disk scan), so a cache of keepable
    /// objects pays nothing for the rule.
    async fn unkeepable_rows(&self) -> Vec<String> {
        let cov = self.coverage.lock().await;
        cov.iter()
            .filter(|(_, c)| c.total > self.config.max_size_bytes)
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Trim one row towards `want` bytes (and, when `cap` is given, towards its
    /// working window). Shared by the global budget pass and the per-key pass so
    /// the selection, the deletion and the single `segment_bytes` subtraction
    /// exist once.
    async fn trim_row(
        &self,
        key: &str,
        want: u64,
        pin: Option<super::watch::Pin>,
        inside_pins: bool,
        cap: Option<u64>,
        now: u64,
    ) -> u64 {
        let prior = {
            let cov = self.coverage.lock().await;
            match cov.get(key) {
                Some(c) => c.intervals.clone(),
                None => return 0,
            }
        };
        let picks = self.pick_spans(key, &prior, want, pin, inside_pins, cap, now).await;
        if picks.is_empty() {
            return 0;
        }
        let freed = self.drop_spans(key, &picks, now).await;
        if freed == 0 {
            return 0;
        }
        {
            let mut s = self.state.write().await;
            s.segment_bytes = s.segment_bytes.saturating_sub(freed);
        }
        // A row that lost every span has nothing left to protect: drop it with
        // its version marker so the next request re-stats upstream instead of
        // gating on an etag for bytes that no longer exist.
        self.drop_empty_row(key).await;
        freed
    }

    /// Rows oldest-touched-first — the cross-key LRU both policies share.
    async fn rows_by_age(&self) -> Vec<(u64, String)> {
        let max = self.config.max_size_bytes;
        let cov = self.coverage.lock().await;
        // The un-keepable class first: a key whose object cannot fit the
        // magazine has no long-term claim on its bytes, so its excess is spent
        // before any keepable key's span (ADR-0019). Within a class this is the
        // cross-key LRU both policies share.
        let mut rows: Vec<(u8, u64, String)> = cov
            .iter()
            .map(|(k, c)| {
                let class = u8::from(c.total <= max);
                (class, c.last_touch_millis, k.clone())
            })
            .collect();
        rows.sort();
        rows.into_iter().map(|(_, t, k)| (t, k)).collect()
    }

    /// The spans to delete, in deletion order, until at least `need_bytes` is
    /// covered.
    ///
    /// **The disk is the authority for what exists.** Candidates are the key's
    /// real `.seg` files, and the ledger only supplies each file's policy
    /// input — the read time (`lru`) and read count (`heat`) of the interval
    /// covering that file's start. Candidates used to come from the ledger's
    /// own bounds, which broke the moment the ledger stopped being 1:1 with the
    /// files: `compact` merges a sequential walk into one interval that owns no
    /// single file, `decay` drops intervals whose bytes are still on disk, and
    /// the ceiling's `drop_coldest` discards records for surviving files. After
    /// any of the three, an exact-bounds lookup named a file that never existed
    /// and the key's staged bytes could not be evicted at all — only the
    /// inactivity sweep could reclaim them.
    ///
    /// A file the ledger has no record of sorts as `(t = 0, reads = 0)`, i.e.
    /// first to go: the only two ways to lose a record are decay and the
    /// ceiling, and both drop exactly the entries nobody was reading.
    async fn pick_spans(
        &self,
        key: &str,
        prior: &[(u64, u64, u64, u64)],
        need_bytes: u64,
        pin: Option<super::watch::Pin>,
        // Whether this pass may spend the pin itself. Pass one may not.
        inside_pins: bool,
        // The row's working window, when it has one (ADR-0019): the span
        // selection is asked for `want` OR for the bytes over this cap,
        // whichever is more.
        cap: Option<u64>,
        now: u64,
    ) -> Vec<(u64, u64)> {
        let files: Vec<(u64, u64)> = store::segments_for_key(&self.config.cache_dir, key)
            .into_iter()
            .map(|(start, end, _)| (start, end))
            .collect();
        if files.is_empty() {
            return Vec::new();
        }
        // Per file: the policy input the ledger remembers about those bytes,
        // plus the position in the file sequence (heat's trailing window is
        // measured on real spans, not on ledger entries).
        let policy: Vec<(u64, u64)> = files
            .iter()
            .map(|(start, end)| match prior.iter().find(|(ps, pe, ..)| ps <= start && end <= pe) {
                Some((_, _, t, r)) => (*t, *r),
                None => (0, 0),
            })
            .collect();
        // A span younger than the guard is not a candidate, however the ROW's
        // own age reads. This replaces a row-level guard that protected the
        // whole row of a key whose `last_touch` is refreshed by every read and
        // every seal — which is every key a viewer is walking — so the budget
        // could never be enforced against exactly the key that was filling the
        // disk, and only the other keys paid (ADR-0019).
        //
        // The age of a span is when it was SEALED (`add_read` counts reads
        // without moving `t`), and the in-flight part is safe by construction:
        // an unsealed `.segpart` is neither in `segment_bytes` nor a candidate,
        // so no transfer is endangered by trimming a row mid-walk.
        let min_sealed = now.saturating_sub(STAGE_MIN_AGE_MS);
        let young = |i: usize| policy[i].0 > 0 && policy[i].0 > min_sealed;

        // The working window: what the caller asked for, or the bytes over the
        // cap, whichever is more.
        let need_bytes = match cap {
            Some(cap) => {
                let row_bytes: u64 = files.iter().map(|(s, e)| e.saturating_sub(*s)).sum();
                need_bytes.max(row_bytes.saturating_sub(cap))
            }
            None => need_bytes,
        };
        let newest = files.len().saturating_sub(1);
        let mut ordered: Vec<usize> = (0..files.len()).collect();
        match self.config.eviction_policy {
            // Stalest first, lowest offset as the tiebreak.
            EvictionPolicy::Lru => {
                ordered.sort_by_key(|i| (policy[*i].0, files[*i].0));
            }
            // Fewest reads inside the trailing window; the window index (how
            // many windows back from the newest span) outranks the count, so
            // an older window never jumps ahead of the trailing one.
            EvictionPolicy::Heat => {
                ordered.sort_by_key(|i| {
                    ((newest - *i) / HEAT_TRAILING_WINDOW, policy[*i].1, policy[*i].0, files[*i].0)
                });
            }
        }
        // A watched key's pin is a PREFERENCE, not an exemption (ADR-0018):
        // spans outside the viewer's neighbourhood
        // go first, however the policy ordered them, and the neighbourhood is
        // taken only when nothing outside it can cover the need. Without the
        // split, "a key being read keeps its spans" made the magazine's budget
        // unenforceable against exactly the key a long watch was filling with
        // bytes that the viewer had already passed.
        let outside = |(start, end): (u64, u64)| match pin {
            Some(p) => end <= p.start || start >= p.end,
            None => true,
        };
        let mut ordered: Vec<usize> = if inside_pins {
            // A pin that has to be spent is spent from the BACK: the spans the
            // viewer has already watched go before the spans it is about to
            // need, because forward progress is continuous while a scrub back
            // is deliberate. Without this the policy's own order decides, and
            // its stalest span is very often the one under the playhead.
            // Behind: nearest-last (farthest behind first). Ahead: nearest
            // kept last. Spans straddling the anchor are the last resort.
            let rank = |(s, e): (u64, u64)| match pin {
                Some(p) if e <= p.anchor => (0u8, std::cmp::Reverse(p.anchor - e)),
                Some(p) if s >= p.anchor => (2u8, std::cmp::Reverse(s - p.anchor)),
                _ => (1u8, std::cmp::Reverse(0u64)),
            };
            ordered.retain(|i| !young(*i));
            ordered.sort_by_key(|i| rank(files[*i]));
            ordered
        } else {
            ordered
                .into_iter()
                .filter(|i| !young(*i) && outside(files[*i]))
                .collect()
        };
        let mut picked = Vec::new();
        let mut acc = 0u64;
        for i in ordered.drain(..) {
            if acc >= need_bytes {
                break;
            }
            let (start, end) = files[i];
            acc += end - start;
            picked.push((start, end));
        }
        picked
    }

    /// Delete the chosen spans' sidecar files, then RE-DERIVE this row's
    /// intervals from the files that remain ([`store::Coverage::adopt_files`],
    /// which reads the row's live intervals as the carry-over source): a merged
    /// interval loses its span-level bounds the moment one of its files goes,
    /// so the row is rebuilt rather than patched. Returns the bytes removed
    /// from disk; the caller subtracts them from `segment_bytes` once.
    async fn drop_spans(&self, key: &str, picks: &[(u64, u64)], now: u64) -> u64 {
        let mut freed = 0u64;
        for (start, end) in picks {
            let path = store::seg_path(&self.config.cache_dir, key, *start, *end);
            freed += tokio::fs::metadata(&path).await.map(|m| m.len()).unwrap_or(0);
            let _ = tokio::fs::remove_file(&path).await;
        }
        let survivors: Vec<(u64, u64)> = store::segments_for_key(&self.config.cache_dir, key)
            .into_iter()
            .map(|(start, end, _)| (start, end))
            .collect();
        {
            let mut cov = self.coverage.lock().await;
            if let Some(entry) = cov.get_mut(key) {
                entry.adopt_files(&survivors, now);
            }
        }
        freed
    }

    /// Drop ledger rows (no files, no accounting — the caller owns both).
    pub(crate) async fn drop_rows(&self, keys: &[String]) {
        let mut cov = self.coverage.lock().await;
        for key in keys {
            cov.remove(key);
        }
    }

    /// Seal a staged span: RENAME the part into place and claim it only when
    /// the rename actually landed. The ledger and `segment_bytes` describe what
    /// is on disk, so a claim written over a failed rename is a phantom span —
    /// invisible until the next restart's scan, and until then bytes an
    /// eviction pass would "free" without a file behind them. A failure is a
    /// real possibility rather than a theoretical one: the part can be gone
    /// (a strays sweep, or a pressure reclaim racing the writer), and the
    /// rename is the point where that becomes visible. Both writers (the
    /// body's exhaustion tail and the disconnect watcher) come through here,
    /// so the order is stated once.
    pub(crate) async fn seal_renamed(
        &self,
        segpart: &std::path::Path,
        seg: &std::path::Path,
        span: FinalizedSpan,
    ) -> bool {
        match tokio::fs::rename(segpart, seg).await {
            Ok(()) => {
                self.seal_span(span).await;
                true
            }
            Err(e) => {
                tracing::warn!(
                    key = %span.key,
                    start = span.start,
                    bytes = span.bytes,
                    error = %e,
                    "staged span could not be sealed: not claiming it"
                );
                let _ = tokio::fs::remove_file(segpart).await;
                false
            }
        }
    }

    /// Full history reset for one key: drop staged files + version marker +
    /// ledger row + accounting. Used on version drift (serve pre-check,
    /// finalize backstop) — never serves mixed versions.
    pub(crate) async fn reset(&self, key: &str) {
        let cache_dir = &self.config.cache_dir;
        let mut freed = 0u64;
        for path in store::key_segment_files(cache_dir, key) {
            freed += std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            let _ = std::fs::remove_file(&path);
        }
        let _ = std::fs::remove_file(store::segmeta_path(cache_dir, key));
        self.coverage.lock().await.remove(key);
        {
            let mut s = self.state.write().await;
            s.segment_bytes = s.segment_bytes.saturating_sub(freed);
        }
    }

    /// One read of `[start, end)` landed. Every span the read actually
    /// TOUCHED gains a count (heat is the eviction policy's input, and this
    /// is the only place it is recorded): a read crossing two staged spans
    /// credits both, and a read that only partly overlaps a span still
    /// credits it. The earlier rule — credit a single span only when it
    /// strictly contained the whole read — meant that cold sequential
    /// playback, where every read is a partial hit against the frontier,
    /// recorded no heat at all.
    pub(crate) async fn add_read(&self, key: &str, start: u64, end: u64) {
        if start >= end {
            return;
        }
        let mut cov = self.coverage.lock().await;
        if let Some(entry) = cov.get_mut(key) {
            for (s, e, _, reads) in entry.intervals.iter_mut() {
                if *s < end && start < *e {
                    *reads = reads.saturating_add(1);
                }
            }
        }
    }

    /// Remove a ledger row that holds no spans any more (the eviction took
    /// them all) together with its version marker. Leaving it would only keep
    /// a stale etag alive to gate a request for bytes that no longer exist.
    async fn drop_empty_row(&self, key: &str) {
        let empty = {
            let mut cov = self.coverage.lock().await;
            match cov.get(key) {
                Some(c) if c.intervals.is_empty() => {
                    cov.remove(key);
                    true
                }
                _ => false,
            }
        };
        if empty {
            let _ = tokio::fs::remove_file(store::segmeta_path(&self.config.cache_dir, key)).await;
        }
    }

    /// A read touched this key: refresh the row's age so an actively watched
    /// window is not swept for inactivity (the per-interval read times are
    /// untouched — window decay stays a function of when bytes were SERVED).
    pub(crate) async fn touch(&self, key: &str, now_millis: u64) {
        if let Some(entry) = self.coverage.lock().await.get_mut(key) {
            entry.last_touch_millis = now_millis;
        }
    }

    /// Merge one completed staged interval — the ONE entry both sealing
    /// paths use.
    ///
    /// Ledger merge + seal rename happen only on exhaustion, so aborts are
    /// sweep-safe; the watcher path covers the disconnect case by sealing
    /// whatever landed before the viewer went away.
    pub(crate) async fn seal_span(&self, span: FinalizedSpan) {
        let FinalizedSpan {
            cache_dir,
            key,
            upstream_id,
            etag,
            total,
            start,
            end,
            bytes,
            now_millis,
        } = span;
        let window_millis = self.config.cache_profile(&upstream_id).coverage_window_secs * 1000;
        {
            let mut cov = self.coverage.lock().await;
            let entry = cov.entry(key.clone()).or_default();
            let version_changed = match (&entry.etag, &etag) {
                (Some(a), Some(b)) => a != b,
                _ => false,
            };
            if version_changed {
                // New bytes already sealed above: keep this file, drop the rest.
                let fresh = store::seg_path(&cache_dir, &key, start, end);
                store::remove_key_segments(&cache_dir, &key, Some(&fresh));
                *entry = store::Coverage::default();
            }
            if etag.is_some() {
                entry.etag = etag.clone();
            }
            if total != 0 {
                entry.total = total;
            }
            entry.add_interval(start, end, now_millis);
            entry.last_touch_millis = now_millis;
            // Window decay: drop intervals whose last read is older than the
            // coverage window, so stale staged bytes stop being served. Disk
            // sidecars stay for the sweep.
            entry.decay_and_covered(now_millis, window_millis);
        }
        let meta = store::SegMeta { etag, total };
        if let Ok(b) = serde_json::to_vec(&meta) {
            let _ = tokio::fs::write(store::segmeta_path(&cache_dir, &key), b).await;
        }
        self.state.write().await.segment_bytes += bytes;
    }
}

/// The staged pieces of `[start, end)` that exist on disk, in order, and the
/// first byte the ledger+disk cannot serve. `pieces` are contiguous from
/// `start`; each is (path, offset-in-file, length). Built from a fresh disk
/// scan (`segments_for_key`), so a span whose file vanished is simply not
/// covered and the gap falls back upstream — the ledger is never trusted
/// over disk.
pub(crate) struct StagedPlan {
    pub(crate) pieces: Vec<(std::path::PathBuf, u64, u64)>,
    pub(crate) frontier: u64,
}

/// Walk the key's sorted segment files and plan `[start, end)`.
pub(crate) fn plan_staged(
    segs: &[(u64, u64, std::path::PathBuf)],
    start: u64,
    end: u64,
) -> StagedPlan {
    let mut pieces = Vec::new();
    let mut cursor = start;
    for (s, e, path) in segs {
        if cursor >= end {
            break;
        }
        if *e <= cursor {
            continue; // wholly before the cursor
        }
        if *s > cursor {
            break; // a gap: stop at the frontier
        }
        let piece_end = (*e).min(end);
        // The file offset is relative to the span's own start: the file
        // holds [s, e), and we want [cursor, piece_end) of it.
        pieces.push((path.clone(), cursor - s, piece_end - cursor));
        cursor = piece_end;
    }
    StagedPlan { pieces, frontier: cursor }
}

/// Serve the planned pieces straight from the sidecar files. No upstream, no
/// stream gate, no magazine admission: a read touches nothing.
pub(crate) fn staged_body(pieces: Vec<(std::path::PathBuf, u64, u64)>) -> BodyStream {
    pieces_then(pieces, Box::pin(futures::stream::empty()))
}

/// A response's staged head followed by a tail from another source — the
/// served spans first, then whatever produces the rest (a run's watermark, or
/// nothing at all for a fully covered range). One shape for both, so the
/// "short sidecar" failure below cannot be lost in a copy.
pub(crate) fn pieces_then(
    pieces: Vec<(std::path::PathBuf, u64, u64)>,
    mut tail: BodyStream,
) -> BodyStream {
    Box::pin(async_stream::try_stream! {
        use tokio::io::{AsyncReadExt, AsyncSeekExt};
        for (path, off, len) in pieces {
            let mut f = tokio::fs::File::open(&path).await?;
            f.seek(std::io::SeekFrom::Start(off)).await?;
            let mut remaining = len;
            while remaining > 0 {
                let want = (256 * 1024).min(remaining as usize);
                let mut buf = bytes::BytesMut::with_capacity(want);
                let n = f.read_buf(&mut buf).await?;
                if n == 0 {
                    // The ledger said covered and the scan said present; a
                    // file that shrank since is a broken sidecar, and a short
                    // response is the honest failure.
                    Err(std::io::Error::other(
                        "staged segment is shorter than the ledger's range",
                    ))?;
                }
                remaining -= n as u64;
                yield buf.freeze();
            }
        }
        while let Some(chunk) = futures::StreamExt::next(&mut tail).await {
            yield chunk?;
        }
    })
}

#[cfg(test)]
mod tests {
    /// The load-bearing numbers that live in prose. Every one of them is quoted
    /// in an ADR or in the spec, and NO behaviour test would notice one of them
    /// changing — so this test exists to make such a change DELIBERATE: it fails
    /// until the author updates the number here and, by habit, the document that
    /// promised it. (Each module pins its own constants; the defaults that come
    /// from config are pinned here, where the staged-byte policy reads them.)
    ///
    /// Asserting values is normally a tautology. That is the point: the survey
    /// found these justified only by comments, and one comment claiming test
    /// coverage it did not have.
    #[test]
    fn constant_decisions_are_pinned() {
        use crate::config::Config;
        let c = Config::default();
        assert_eq!(c.session_window_bytes, 64 * 1024 * 1024, "one second of transfer (ADR-0016)");
        assert_eq!(
            c.window_floor_bytes,
            crate::config::DEFAULT_WINDOW_FLOOR_BYTES,
            "the window decision's floor (one eighth of the window)"
        );
        assert_eq!(c.watch_pin_bytes, 128 * 1024 * 1024, "the viewer's neighbourhood (ADR-0018)");
        assert_eq!(c.watch_idle_secs, 900, "longer than a phone call (ADR-0018)");
        assert_eq!(c.read_grace_secs, 300, "covers a viewer who is thinking (ADR-0017)");
        assert_eq!(c.inactive_ttl_secs, 1200, "the idle TTL the sweep uses");
        assert_eq!(c.concurrency_per_upstream, 3, "the upstream stream budget (ADR-0004)");
        assert_eq!(STAGE_MIN_AGE_MS, 60_000, "the span-level min-age guard (ADR-0019)");
        assert_eq!(HEAT_TRAILING_WINDOW, 20, "spans the heat policy compares (ADR-0015)");
    }

    use super::*;
    use crate::cache::{leases::Leases, watch::Watches};
    use crate::config::EvictionPolicy;
    use std::path::PathBuf;

    /// The staging receiver on its own: the eviction and sealing rules are
    /// exercised here without a request, which is what makes "the pin decides
    /// which span goes" assertable at all.
    struct Harness {
        staging: Staging,
        coverage: Arc<Mutex<HashMap<String, store::Coverage>>>,
        state: Arc<RwLock<CacheState>>,
        leases: Arc<Leases>,
        watches: Arc<Watches>,
        dir: tempfile::TempDir,
    }

    fn harness(policy: EvictionPolicy, pin_bytes: u64, watch_idle_ms: u64) -> Harness {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = Config { cache_dir: dir.path().to_path_buf(), ..Config::default() };
        cfg.eviction_policy = policy;
        let cfg = Arc::new(cfg);
        let coverage = Arc::new(Mutex::new(HashMap::new()));
        let state = Arc::new(RwLock::new(CacheState::default()));
        let leases = Arc::new(Leases::new(0));
        let watches = Arc::new(Watches::new(watch_idle_ms, pin_bytes));
        let staging = Staging::new(
            Arc::clone(&coverage),
            Arc::clone(&state),
            Arc::clone(&cfg),
            Arc::clone(&leases),
            Arc::clone(&watches),
        );
        Harness { staging, coverage, state, leases, watches, dir }
    }

    impl Harness {
        fn cache_dir(&self) -> PathBuf {
            self.dir.path().to_path_buf()
        }

        /// Write one real `.seg` file and give the ledger the policy input the
        /// eviction paths read about it: the disk is the authority for
        /// existence, the row only supplies the ordering.
        async fn stage_span(&self, key: &str, start: u64, end: u64, t: u64, reads: u64) {
            let path = store::seg_path(&self.cache_dir(), key, start, end);
            std::fs::write(&path, vec![b'x'; (end - start) as usize]).unwrap();
            let mut cov = self.coverage.lock().await;
            let row = cov.entry(key.to_string()).or_default();
            row.intervals.push((start, end, t, reads));
            row.intervals.sort();
            row.total = 1_000_000;
            row.etag = Some("v1".into());
            // The ROW's age (the min-age guard and the cross-key LRU) is left
            // old, so the trim is not refused as "actively staging"; the
            // per-interval `t` is the policy input the span ordering uses.
            drop(cov);
            let mut s = self.state.write().await;
            s.segment_bytes += end - start;
        }

        /// Age the ROW without touching its spans: what a walking key looks
        /// like from the trim's point of view (every read and every seal stamps
        /// the row).
        async fn touch_row(&self, key: &str, t: u64) {
            let mut cov = self.coverage.lock().await;
            if let Some(row) = cov.get_mut(key) {
                row.last_touch_millis = t;
            }
        }

        fn spans(&self, key: &str) -> Vec<(u64, u64)> {
            store::segments_for_key(&self.cache_dir(), key)
                .into_iter()
                .map(|(s, e, _)| (s, e))
                .collect()
        }
    }

    /// A watched key's pin is a PREFERENCE, not an exemption (ADR-0018): the
    /// spans outside the viewer's neighbourhood go first even when the policy
    /// would have taken an older one, and the neighbourhood is what survives a
    /// trim. The old rule — "a key being read keeps its spans" — protected the
    /// whole row, so a long watch made the budget unenforceable against
    /// exactly the key that was filling the disk.
    #[tokio::test]
    async fn a_watched_key_gives_up_the_spans_outside_its_neighbourhood() {
        let h = harness(EvictionPolicy::Lru, 200, 900_000);
        for (start, end, t) in [(0u64, 100u64, 1u64), (100, 200, 2), (200, 300, 3), (300, 400, 4)] {
            h.stage_span("a.bin", start, end, t, 1).await;
        }
        // The viewer is at the start of the object: the pin is [0, 200).
        let _watch =
            h.watches.acquire_at("a.bin", (0, 100), Arc::new(crate::clock::MockClock::new(400_000)));
        let now = 400_000u64;

        let (touched, freed) = h.staging.evict_staged(100, now).await;
        assert_eq!(touched, 1);
        assert_eq!(freed, 100, "one span's bytes, exactly");
        assert_eq!(
            h.spans("a.bin"),
            vec![(0, 100), (100, 200), (300, 400)],
            "the LRU-oldest span was NOT the one to go: the pin sent the trim to the tail"
        );
        assert_eq!(h.state.read().await.segment_bytes, 300, "accounting follows the file");
    }

    /// The reverse verification: with the pin switched off, the same setup
    /// takes the policy's own choice — the oldest span. Two opposite
    /// expectations from one input is what says the pin, not the ordering, is
    /// what moved.
    #[tokio::test]
    async fn without_a_pin_the_policy_takes_the_oldest_span() {
        let h = harness(EvictionPolicy::Lru, 0, 900_000);
        for (start, end, t) in [(0u64, 100u64, 1u64), (100, 200, 2), (200, 300, 3), (300, 400, 4)] {
            h.stage_span("a.bin", start, end, t, 1).await;
        }
        let _watch =
            h.watches.acquire_at("a.bin", (0, 100), Arc::new(crate::clock::MockClock::new(400_000)));
        let (touched, freed) = h.staging.evict_staged(100, 400_000).await;
        assert_eq!((touched, freed), (1, 100));
        assert_eq!(
            h.spans("a.bin"),
            vec![(100, 200), (200, 300), (300, 400)],
            "no pin: the stalest span goes, as it always did"
        );
    }

    /// And the pin is a deadline, not immortality: when nothing outside it can
    /// cover the need, it is taken too. Otherwise "watched" would be a second
    /// byte budget that nothing can reclaim.
    #[tokio::test]
    async fn the_pin_yields_when_nothing_outside_it_covers_the_need() {
        let h = harness(EvictionPolicy::Lru, 200, 900_000);
        for (start, end, t) in [(0u64, 100u64, 1u64), (100, 200, 2), (200, 300, 3), (300, 400, 4)] {
            h.stage_span("a.bin", start, end, t, 1).await;
        }
        let _watch =
            h.watches.acquire_at("a.bin", (0, 100), Arc::new(crate::clock::MockClock::new(400_000)));
        // 400 bytes wanted, 200 of them outside the pin.
        let (_, freed) = h.staging.evict_staged(400, 400_000).await;
        assert_eq!(freed, 400, "the whole row goes when the need is bigger than the tail");
        assert!(h.spans("a.bin").is_empty());
        assert_eq!(h.state.read().await.segment_bytes, 0);
    }

    /// A key being walked is trimmable OUTSIDE its pin. The row-level guard this
    /// replaces protected the WHOLE row of a key whose `last_touch` is refreshed
    /// by every read and every seal — which is every key a viewer is walking — so
    /// the budget could never be enforced against the key that was filling the
    /// disk, and only the other keys paid. The in-flight part is already safe
    /// (an unsealed `.segpart` is neither in `segment_bytes` nor a candidate) and
    /// the age of a span is its seal time (`add_read` counts reads without
    /// moving it), so the guard belongs on the span.
    #[tokio::test]
    async fn a_walk_in_progress_is_trimmable_outside_its_pin() {
        let h = harness(EvictionPolicy::Lru, 0, 0);
        let now = 400_000u64;
        h.stage_span("a.bin", 0, 100, now - 120_000, 1).await; // older than the guard
        h.stage_span("a.bin", 100, 200, now, 1).await; // just sealed
        h.touch_row("a.bin", now).await; // ...and the row itself is fresh

        let (_, freed) = h.staging.evict_staged(100, now).await;
        assert_eq!(freed, 100, "the old span goes even though its row was touched a moment ago");
        assert_eq!(
            h.spans("a.bin"),
            vec![(100, 200)],
            "and the span that was just sealed is not a candidate yet"
        );
    }

    /// A leased key with NO watch keeps the older, coarser rule: the whole row
    /// is spared. That is the shape `watch_idle_secs = 0` produces (watching
    /// turned off), so ADR-0017's behaviour survives the change intact.
    #[tokio::test]
    async fn a_leased_key_without_a_watch_keeps_its_whole_row() {
        let h = harness(EvictionPolicy::Lru, 0, 0);
        for (start, end, t) in [(0u64, 100u64, 1u64), (100, 200, 2)] {
            h.stage_span("a.bin", start, end, t, 1).await;
        }
        let _lease = h.leases.acquire("a.bin", Arc::new(|| 400_000));
        let (touched, freed) = h.staging.evict_staged(100, 400_000).await;
        assert_eq!((touched, freed), (0, 0), "a leased key with no watch is not budget material");
        assert_eq!(h.spans("a.bin"), vec![(0, 100), (100, 200)]);
    }

    /// A pin that has to be spent is spent from the BACK: the span the viewer
    /// has already watched goes before the span it is about to need. The
    /// policy's own order is arranged to disagree — lru's stalest span is the
    /// one ahead — so the two possible expectations are opposite.
    #[tokio::test]
    async fn a_spent_pin_gives_up_the_back_before_the_playhead() {
        let h = harness(EvictionPolicy::Lru, 200, 900_000);
        // Outside the pin, to pay with first (t=3, so lru would keep it).
        h.stage_span("a.bin", 0, 100, 3, 1).await;
        // Behind the anchor: already watched, t=2, so lru would keep it too.
        h.stage_span("a.bin", 100, 200, 2, 1).await;
        // Ahead of the anchor, and the STALEST: what the viewer needs next, and
        // what lru would eject first.
        h.stage_span("a.bin", 200, 300, 1, 1).await;
        // The viewer's last response was [100, 200): the anchor is 200 and the
        // pin is [100, 300).
        let _watch =
            h.watches.acquire_at("a.bin", (100, 200), Arc::new(crate::clock::MockClock::new(400_000)));
        assert_eq!(
            h.watches.pin("a.bin", 400_000).map(|p| (p.start, p.end, p.anchor)),
            Some((100, 300, 200))
        );

        // 200 bytes wanted: the span outside the pin pays 100, and the pin pays
        // the other 100 — from its BACK.
        let (_, freed) = h.staging.evict_staged(200, 400_000).await;
        assert_eq!(freed, 200);
        assert_eq!(
            h.spans("a.bin"),
            vec![(200, 300)],
            "the span AHEAD of the viewer survives, not the span the policy would have kept"
        );
    }

    /// A watch keeps a row alive across the idle sweep exactly as a lease does
    /// — that is the horizon a lease cannot give: a viewer who pauses for
    /// longer than the read grace is still watching (ADR-0018).
    #[tokio::test]
    async fn a_watch_spares_a_row_the_idle_sweep_would_take() {
        let h = harness(EvictionPolicy::Lru, 200, 900_000);
        h.stage_span("a.bin", 0, 100, 0, 1).await;
        let now = 1_300_000u64;
        // Nothing has read this key for 1300 s: past the TTL, and the lease is
        // long gone (its grace is 0 here).
        assert_eq!(
            h.staging.expired(1_200_000, now).await,
            vec!["a.bin".to_string()],
            "untouched and unwatched, the row is idle"
        );

        let _body = h.watches.acquire_at("a.bin", (0, 100), Arc::new(crate::clock::MockClock::new(now)));
        assert!(
            h.staging.expired(1_200_000, now).await.is_empty(),
            "the viewing session is still there, so the bytes are not idle"
        );
    }

    /// The seal is the rename AND the claim: a span that did not land is not
    /// recorded. Claiming it anyway left `segment_bytes` and the ledger
    /// describing a file that does not exist — invisible until the next
    /// restart's scan, and until then a phantom the eviction pass would
    /// "free" bytes for.
    #[tokio::test]
    async fn a_seal_that_did_not_land_is_not_claimed() {
        let h = harness(EvictionPolicy::Lru, 0, 0);
        let dir = h.cache_dir();
        let seg = store::seg_path(&dir, "a.bin", 0, 100);
        // The part is gone — exactly what a strays sweep or a pressure
        // reclaim racing the writer leaves behind.
        let missing = store::segpart_path(&dir, "a.bin", 0, 100);
        let claimed = h
            .staging
            .seal_renamed(
                &missing,
                &seg,
                FinalizedSpan {
                    cache_dir: dir.clone(),
                    key: "a.bin".into(),
                    upstream_id: "primary".into(),
                    etag: Some("v1".into()),
                    total: 1_000_000,
                    start: 0,
                    end: 100,
                    bytes: 100,
                    now_millis: 0,
                },
            )
            .await;
        assert!(!claimed, "the rename did not land, so nothing was claimed");
        assert!(!seg.exists());
        assert_eq!(h.state.read().await.segment_bytes, 0);
        assert!(h.spans("a.bin").is_empty());

        // The positive control: with the part present, the same call seals and
        // claims.
        std::fs::write(&missing, vec![b'x'; 100]).unwrap();
        let claimed = h
            .staging
            .seal_renamed(
                &missing,
                &seg,
                FinalizedSpan {
                    cache_dir: dir.clone(),
                    key: "a.bin".into(),
                    upstream_id: "primary".into(),
                    etag: Some("v1".into()),
                    total: 1_000_000,
                    start: 0,
                    end: 100,
                    bytes: 100,
                    now_millis: 0,
                },
            )
            .await;
        assert!(claimed);
        assert!(seg.exists());
        assert_eq!(h.state.read().await.segment_bytes, 100);
        assert_eq!(h.spans("a.bin"), vec![(0, 100)]);
    }
}

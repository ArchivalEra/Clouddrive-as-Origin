//! The staged state: which bytes of a key exist as `.seg` sidecars, under which
//! object version, and what they cost.
//!
//! Before this module the answer lived in four places — an account counter on
//! `CacheState` (`segment_bytes`), a raw map handle cloned into `Staging` and
//! reached from `Cache`, the intervals with their read times inside
//! `store::Coverage`, and a startup scan that wrote to both — and three of them
//! maintained "the account is the sum of disk-backed staged bytes" by hand. The
//! reconciliation rule lived in a comment in `Cache::tick` ("keep them added,
//! not max-ed: they are disjoint").
//!
//! Here that rule is the interface:
//!
//! * **The disk decides what exists.** [`Ledger::adopt`] records a span only
//!   after checking that its file is there, and takes the byte count from the
//!   file rather than the caller's claim, so a record cannot over-claim. Every
//!   account delta is a disk measurement: a file's real length when bytes
//!   arrive, the caller's measured `freed` when they leave, a fresh inventory
//!   at startup. Nothing adds or subtracts a number it computed from the ledger
//!   itself, which is how the two came to drift.
//! * **The ledger never reads a clock.** Every entry takes the millisecond
//!   stamp it should use, so a test can stage at any moment it likes.
//! * **The guard never leaves.** Every query, every selection and every
//!   mutation runs to completion inside this module, so no caller can hold the
//!   staged state across an await or take it in the wrong order: `Cache::tick`'s
//!   rule (ADR-0008 — the ledger lock is taken before any state guard, never
//!   the reverse) stops being prose that every call site must remember.
//!
//! Two read-side rules are deliberately separate, because they answer different
//! questions: [`Ledger::served`] (bytes left the disk for a reader — feeds
//! heat) and [`Ledger::touch`] (the key was reached at all — refreshes the
//! row's age so an actively watched window is not swept for inactivity, and
//! leaves the per-interval read times alone).

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use tokio::sync::Mutex;

use super::store;

/// One staged interval as the LEDGER knows it: the byte span, when it was last
/// read (the window's decay clock), and how many times (heat, the eviction
/// policy's input). A merged interval carries the sum of its parts' reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpanReads {
    pub start: u64,
    pub end: u64,
    pub last_read_millis: u64,
    pub reads: u64,
}

/// What the ledger knows about one key. A value, not a handle: nothing in here
/// can outlive the guard that produced it.
#[derive(Debug, Clone, Default)]
pub(crate) struct RecordView {
    pub spans: Vec<SpanReads>,
    pub etag: Option<String>,
    pub total: u64,
    pub last_touch_millis: u64,
}

/// Transfer history for one key: which byte intervals have been served and
/// staged, under which object version. Private to this module — callers read it
/// through [`Ledger::view`], so its fields stop being a second interface.
#[derive(Debug, Clone, Default)]
struct Coverage {
    etag: Option<String>,
    total: u64,
    /// Merged, sorted, non-overlapping `[start, end)` intervals, each with
    /// the clock-domain time it was last read (window decay) and its READ
    /// COUNT (heat, the eviction policy's input). Heat survives a merge as a
    /// SUM: a merged span was served `reads` times across its whole range.
    intervals: Vec<(u64, u64, u64, u64)>,
    /// Clock-domain last touch (stage, adoption or request time): drives the
    /// age sweep in MockClock-testable time, unlike fs mtime.
    last_touch_millis: u64,
}

/// Ceiling on the intervals one key's ledger may hold. Adjacent staged shards
/// are deliberately kept apart (each keeps its own read time for window
/// decay), so a sequential scrub in 1 MiB shards grew this vector with the
/// request count — and the vector is walked on every staged transfer under
/// the single process-wide ledger lock.
pub(crate) const MAX_INTERVALS_PER_KEY: usize = 4096;

impl Coverage {
    fn span_reads(&self) -> Vec<SpanReads> {
        self.intervals
            .iter()
            .map(|(start, end, last_read_millis, reads)| SpanReads {
                start: *start,
                end: *end,
                last_read_millis: *last_read_millis,
                reads: *reads,
            })
            .collect()
    }

    /// Merge `[start, end)` (empty ranges ignored), stamped with the read
    /// time. Overlapping intervals merge and keep the max timestamp;
    /// adjacent (touching) intervals stay separate so each keeps its own
    /// read time — a stale interval must not be "revived" by a fresh
    /// neighbor (window decay semantics).
    fn add_interval(&mut self, start: u64, end: u64, now_millis: u64) {
        if start >= end {
            return;
        }
        // Insert at the sorted position and merge only the touched
        // neighbours: overlaps can only reach the interval before the
        // insertion point and the ones after it (the list is sorted and
        // non-overlapping), so merge locally instead of rebuilding.
        let idx = self.intervals.partition_point(|(s, ..)| *s < start);
        let (lo, mut merged_end, mut merged_at, mut merged_reads) = if idx > 0
            && self.intervals[idx - 1].1 > start
        {
            let (_, pe, pt, pr) = self.intervals[idx - 1];
            (idx - 1, pe.max(end), pt.max(now_millis), pr)
        } else {
            self.intervals.insert(idx, (start, end, now_millis, 0));
            (idx, end, now_millis, 0)
        };
        let mut hi = lo + 1;
        while hi < self.intervals.len() && self.intervals[hi].0 < merged_end {
            merged_end = merged_end.max(self.intervals[hi].1);
            merged_at = merged_at.max(self.intervals[hi].2);
            merged_reads += self.intervals[hi].3;
            hi += 1;
        }
        if hi > lo + 1 {
            self.intervals.drain(lo + 1..hi);
        }
        if let Some(slot) = self.intervals.get_mut(lo) {
            *slot = (slot.0.min(start), merged_end, merged_at, merged_reads);
        }
        self.compact();
    }

    /// Rebuild this row's intervals from the sidecar files that remain on
    /// disk, carrying the policy input across: each survivor inherits the read
    /// time and count of the interval that covered it before the rebuild (none
    /// ⇒ `(now_millis, 0)` — a file the ledger had no record of, which only
    /// happens when decay or the ceiling dropped that record).
    ///
    /// Existence lives on disk; this list is the policy's map of it.
    fn adopt_files(&mut self, files: &[(u64, u64)], now_millis: u64) {
        let prior = std::mem::take(&mut self.intervals);
        self.intervals = files
            .iter()
            .map(|(start, end)| {
                match prior
                    .iter()
                    .find(|(ps, pe, ..)| *ps <= *start && *end <= *pe)
                {
                    Some((_, _, t, r)) => (*start, *end, *t, *r),
                    None => (*start, *end, now_millis, 0),
                }
            })
            .collect();
        self.compact();
    }

    /// Keep the vector under [`MAX_INTERVALS_PER_KEY`] — exactly, or not at
    /// all, because both steps here are lossless-then-conservative:
    ///
    /// 1. Merge touching pairs (`[a,b) + [b,c) = [a,c)`, read time = max),
    ///    which changes no byte count at all — this is what a sequential
    ///    scrub produces, so it is the step that actually fires.
    /// 2. If spans with GAPS remain (a scrubber jumping around), merging
    ///    them would claim coverage of bytes we do not hold, so the coldest
    ///    spans are dropped instead. Under-reporting coverage is the safe
    ///    direction: the ledger under-reports rather than claiming a gap it
    ///    cannot fill, and this is the same trade window decay already makes.
    fn compact(&mut self) {
        while self.intervals.len() > MAX_INTERVALS_PER_KEY {
            if !self.merge_touching_once() {
                self.drop_coldest(self.intervals.len() - MAX_INTERVALS_PER_KEY);
            }
        }
    }

    /// One halving pass over touching neighbours. Returns whether anything
    /// merged, so the caller can tell "no exact merge left" from "still too
    /// long".
    fn merge_touching_once(&mut self) -> bool {
        let mut out: Vec<(u64, u64, u64, u64)> = Vec::with_capacity(self.intervals.len());
        let mut merged = false;
        let mut i = 0;
        while i < self.intervals.len() {
            let (s, e, t, r) = self.intervals[i];
            match self.intervals.get(i + 1).copied() {
                Some((s2, e2, t2, r2)) if s2 == e => {
                    out.push((s, e2, t.max(t2), r + r2));
                    merged = true;
                    i += 2;
                }
                _ => {
                    out.push((s, e, t, r));
                    i += 1;
                }
            }
        }
        self.intervals = out;
        merged
    }

    /// Drop the `n` spans with the oldest read time (the ones window decay
    /// would take first anyway), then restore start order.
    fn drop_coldest(&mut self, n: usize) {
        self.intervals.sort_by_key(|(.., t, _)| *t);
        self.intervals.drain(..n.min(self.intervals.len()));
        self.intervals.sort_by_key(|(s, ..)| *s);
    }

    /// One pass instead of three: drop intervals whose last read is older
    /// than `window_millis` and return the bytes still covered.
    ///
    /// Window expiry only removes ledger counts — the disk sidecars stay for
    /// the natural sweep.
    fn decay_and_covered(&mut self, now_millis: u64, window_millis: u64) -> u64 {
        if window_millis == 0 {
            return self.covered();
        }
        let cutoff = now_millis.saturating_sub(window_millis);
        let mut kept = 0usize;
        let mut covered = 0u64;
        for i in 0..self.intervals.len() {
            let (s, e, t, r) = self.intervals[i];
            if t >= cutoff {
                self.intervals[kept] = (s, e, t, r);
                kept += 1;
                covered += e - s;
            }
        }
        self.intervals.truncate(kept);
        covered
    }

    fn covered(&self) -> u64 {
        self.intervals.iter().map(|(s, e, ..)| e - s).sum()
    }
}

/// Both halves of the staged state under one lock: the per-key records and the
/// account. They share a lock because the account is a function of the records,
/// and two locks for one fact is how the two came to drift apart.
#[derive(Default)]
struct Inner {
    map: HashMap<String, Coverage>,
    /// The account: staged bytes on this node, every delta a disk
    /// measurement.
    total_staged: u64,
}

/// The receiver that owns the staged state. Every operation takes the lock
/// itself and finishes inside this module.
pub(crate) struct Ledger {
    inner: Mutex<Inner>,
    cache_dir: PathBuf,
}

impl Ledger {
    pub(crate) fn new(cache_dir: PathBuf) -> Self {
        Self { inner: Mutex::new(Inner::default()), cache_dir }
    }

    /// Staged bytes on this node.
    pub(crate) async fn total_staged(&self) -> u64 {
        self.inner.lock().await.total_staged
    }

    /// Rows and intervals, for the operator's view.
    pub(crate) async fn summary(&self) -> (usize, usize) {
        let inner = self.inner.lock().await;
        (inner.map.len(), inner.map.values().map(|c| c.intervals.len()).sum())
    }

    /// What the ledger knows about one key.
    pub(crate) async fn view(&self, key: &str) -> Option<RecordView> {
        let inner = self.inner.lock().await;
        inner.map.get(key).map(|c| RecordView {
            spans: c.span_reads(),
            etag: c.etag.clone(),
            total: c.total,
            last_touch_millis: c.last_touch_millis,
        })
    }

    /// One completed span, recorded in one step: the version gate, the claim
    /// (measured off the disk), the row's age and the coverage window, all under
    /// one guard — so a version change and the span that caused it cannot
    /// interleave with another seal of the same key.
    ///
    /// Returns whether the disk backed the span. A span whose file is not there
    /// records nothing: the rename is the only witness worth trusting, and a
    /// claim written over a failed rename is a phantom span.
    ///
    /// The `stat` is deliberately synchronous. Sealing runs inside the response
    /// body's stream (the tail of `ranged::upstream_body`), and a
    /// `spawn_blocking` round trip from inside a body's poll stalls the drain:
    /// measured at ~35% of runs on `a_read_credits_every_staged_span_it_touches`
    /// before this was changed, 0% since. It is also the house style here — the
    /// sweep, the version-change cleanup and the disk-covers check all use
    /// `std::fs::metadata`.
    pub(crate) async fn seal(
        &self,
        key: &str,
        span: (u64, u64),
        etag: Option<&str>,
        total: u64,
        window_millis: u64,
        at: u64,
    ) -> bool {
        let (start, end) = span;
        let path = store::seg_path(&self.cache_dir, key, start, end);
        let len = match std::fs::metadata(&path) {
            Ok(m) => m.len(),
            Err(_) => return false,
        };
        let end = end.min(start.saturating_add(len));
        if end <= start {
            return false;
        }
        let mut inner = self.inner.lock().await;
        let entry = inner.map.entry(key.to_string()).or_default();
        if let Some(e) = etag {
            entry.etag = Some(e.to_string());
        }
        if total != 0 {
            entry.total = total;
        }
        entry.add_interval(start, end, at);
        entry.last_touch_millis = at;
        entry.decay_and_covered(at, window_millis);
        inner.total_staged = inner.total_staged.saturating_add(len);
        true
    }

    /// Record a span as staged, from what the disk actually holds.
    ///
    /// The rule is one thing: **a claim must be disk-backed.** The cheap check
    /// comes first — the file this call just renamed into place, whose real
    /// length is the number recorded, so a short write claims short. When there
    /// is no single file for the span, the claim is checked against the union of
    /// the key's files instead, which is what a row holds once `compact` has
    /// merged a long sequential walk into one interval; a span the disk does not
    /// cover is refused and records nothing.
    pub(crate) async fn adopt(&self, key: &str, span: (u64, u64), at: u64) -> bool {
        let (start, end) = span;
        let path = store::seg_path(&self.cache_dir, key, start, end);
        let recorded = match std::fs::metadata(&path) {
            Ok(m) => (end.min(start.saturating_add(m.len())), m.len()),
            Err(_) => {
                if !self.disk_covers(key, start, end) {
                    return false;
                }
                // The union path adds no bytes: a claim wider than any single
                // file describes bytes their own files already accounted for
                // (that is the only way the row got this shape). Counting them
                // again would inflate the account by exactly what compaction
                // merged.
                (end, 0)
            }
        };
        let (end, bytes) = recorded;
        if end <= start {
            return false;
        }
        let mut inner = self.inner.lock().await;
        let entry = inner.map.entry(key.to_string()).or_default();
        entry.add_interval(start, end, at);
        entry.last_touch_millis = at;
        inner.total_staged = inner.total_staged.saturating_add(bytes);
        true
    }

    /// Is `[start, end)` covered by the union of this key's files? The
    /// authority question [`Ledger::adopt`] asks when no single file answers it.
    fn disk_covers(&self, key: &str, start: u64, end: u64) -> bool {
        let mut at = start;
        for (s, e, _) in store::segments_for_key(&self.cache_dir, key) {
            if s <= at && at < e {
                at = e;
                if at >= end {
                    return true;
                }
            }
        }
        false
    }

    /// Adopt everything on disk at `at` — the startup rebuild, and how a test
    /// fixture says "this node now holds these spans". The account is
    /// recomputed from the inventory rather than patched.
    pub(crate) async fn adopt_all(&self, at: u64) {
        let files = store::scan_segment_files(&self.cache_dir);
        let mut total = 0u64;
        let mut inner = self.inner.lock().await;
        inner.map.clear();
        for f in files {
            total = total.saturating_add(f.len);
            let entry = inner.map.entry(f.key).or_default();
            if entry.etag.is_none() {
                entry.etag = f.etag;
            }
            if entry.total == 0 {
                entry.total = f.total;
            }
            entry.add_interval(f.start, f.end.min(f.start.saturating_add(f.len)), at);
            entry.last_touch_millis = at;
        }
        inner.total_staged = total;
    }

    /// The row goes, and the account gives up what the caller measured leaving
    /// the disk. Called after a deletion (the age sweep, a version reset): the
    /// freed bytes are the caller's measurement of the files it removed, never
    /// the record's own claim.
    pub(crate) async fn forget(&self, key: &str, freed_bytes: u64) {
        let mut inner = self.inner.lock().await;
        inner.map.remove(key);
        inner.total_staged = inner.total_staged.saturating_sub(freed_bytes);
    }

    /// The eviction took some of this key's spans: give up the bytes it freed,
    /// then re-derive the row from the files that remain, carrying the read
    /// policy across — a merged interval has lost its span-level bounds the
    /// moment one of its files goes, so the row is rebuilt rather than patched.
    pub(crate) async fn re_adopt(&self, key: &str, freed_bytes: u64, now: u64) {
        let survivors: Vec<(u64, u64)> = store::segments_for_key(&self.cache_dir, key)
            .into_iter()
            .map(|(start, end, _)| (start, end))
            .collect();
        let mut inner = self.inner.lock().await;
        inner.total_staged = inner.total_staged.saturating_sub(freed_bytes);
        if let Some(entry) = inner.map.get_mut(key) {
            entry.adopt_files(&survivors, now);
        }
    }

    /// Drop the row when the eviction took its last span, so a stale etag does
    /// not gate a request for bytes that no longer exist. Returns whether the
    /// row went; the caller removes the version marker.
    pub(crate) async fn drop_row_if_empty(&self, key: &str) -> bool {
        let mut inner = self.inner.lock().await;
        match inner.map.get(key) {
            Some(c) if c.intervals.is_empty() => {
                inner.map.remove(key);
                true
            }
            _ => false,
        }
    }

    /// The version gate's side of the record: which object these bytes belong
    /// to. Absent values leave what is there (`None` and `0` mean "nothing to
    /// say", matching the scan's "unknown version" case).
    pub(crate) async fn set_version(&self, key: &str, etag: Option<&str>, total: u64) {
        let mut inner = self.inner.lock().await;
        let entry = inner.map.entry(key.to_string()).or_default();
        if let Some(e) = etag {
            entry.etag = Some(e.to_string());
        }
        if total != 0 {
            entry.total = total;
        }
    }

    /// The key was reached at all: refresh the row's age so an actively watched
    /// window is not swept for inactivity. The per-interval read times are
    /// deliberately untouched — window decay stays a function of when bytes
    /// were SERVED, not of when a request last mentioned the key.
    pub(crate) async fn touch(&self, key: &str, at: u64) {
        if let Some(entry) = self.inner.lock().await.map.get_mut(key) {
            entry.last_touch_millis = at;
        }
    }

    /// Bytes were served from the disk: every span the read TOUCHED gains a
    /// count. Heat is the eviction policy's input and this is the only place it
    /// is recorded. A read crossing two staged spans credits both, and a read
    /// that only partly overlaps a span still credits it — cold sequential
    /// playback is a partial hit against the frontier on every read, and the
    /// earlier strict-containment rule recorded no heat at all for it.
    pub(crate) async fn served(&self, key: &str, start: u64, end: u64) {
        if start >= end {
            return;
        }
        let mut inner = self.inner.lock().await;
        if let Some(entry) = inner.map.get_mut(key) {
            for (s, e, _, reads) in entry.intervals.iter_mut() {
                if *s < end && start < *e {
                    *reads = reads.saturating_add(1);
                }
            }
        }
    }

    /// Bytes this key may still be served from, after the coverage window
    /// dropped what has gone cold.
    pub(crate) async fn decayed_coverage(&self, key: &str, at: u64, window_millis: u64) -> u64 {
        let mut inner = self.inner.lock().await;
        match inner.map.get_mut(key) {
            Some(entry) => entry.decay_and_covered(at, window_millis),
            None => 0,
        }
    }

    /// Rows whose last touch is older than `ttl_ms`, minus the keys the caller
    /// says are spared (a live read lease, a live watch).
    pub(crate) async fn expired_candidates(
        &self,
        ttl_ms: u64,
        at: u64,
        spared: &HashSet<String>,
    ) -> Vec<String> {
        let inner = self.inner.lock().await;
        inner
            .map
            .iter()
            .filter(|(_, c)| at.saturating_sub(c.last_touch_millis) >= ttl_ms)
            .filter(|(k, _)| !spared.contains(k.as_str()))
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Keys whose object can never fit the magazine (ADR-0019).
    pub(crate) async fn unkeepable_rows(&self, max_size_bytes: u64) -> Vec<String> {
        let inner = self.inner.lock().await;
        inner
            .map
            .iter()
            .filter(|(_, c)| c.total > max_size_bytes)
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Rows oldest-touched-first: the cross-key LRU both eviction policies
    /// share, with un-keepable objects first inside it — a key whose object
    /// cannot fit the magazine has no long-term claim on its bytes, so its
    /// excess is spent before any keepable key's span (ADR-0019).
    pub(crate) async fn rows_by_age(&self, max_size_bytes: u64) -> Vec<(u64, String)> {
        let inner = self.inner.lock().await;
        let mut rows: Vec<(u8, u64, String)> = inner
            .map
            .iter()
            .map(|(k, c)| (u8::from(c.total <= max_size_bytes), c.last_touch_millis, k.clone()))
            .collect();
        rows.sort();
        rows.into_iter().map(|(_, t, k)| (t, k)).collect()
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    /// The rules the ledger keeps, tested through the record's own interface:
    /// these came from `store.rs` when the coverage type moved in, and they are
    /// the reason it could move without changing behaviour — nothing here is
    /// about disk layout.

    #[test]
    fn add_interval_merges_locally_and_equivalently() {
        let mut c = Coverage::default();
        c.add_interval(100, 200, 10);
        c.add_interval(300, 400, 20);
        assert_eq!(c.intervals, vec![(100, 200, 10, 0), (300, 400, 20, 0)]);

        // Bridge the gap (overlapping both) -> one merged span, max time.
        c.add_interval(150, 350, 30);
        assert_eq!(c.intervals, vec![(100, 400, 30, 0)]);

        // Adjacent but NOT overlapping: stays a separate interval so it
        // keeps its own read time (window-decay semantics, by design).
        c.add_interval(400, 500, 40);
        assert_eq!(c.intervals, vec![(100, 400, 30, 0), (400, 500, 40, 0)]);

        // An earlier disjoint interval inserts in sorted position.
        c.add_interval(10, 20, 5);
        assert_eq!(c.intervals, vec![(10, 20, 5, 0), (100, 400, 30, 0), (400, 500, 40, 0)]);

        // A contained interval absorbs without changing the bounds.
        c.add_interval(200, 250, 99);
        assert_eq!(c.intervals.len(), 3);
        assert_eq!(c.intervals[1], (100, 400, 99, 0));

        // Empty ranges are ignored.
        let before = c.intervals.clone();
        c.add_interval(600, 600, 1);
        assert_eq!(c.intervals, before);

        // Many sequential disjoint shards stay sorted and single.
        let mut d = Coverage::default();
        for i in 0..1000u64 {
            d.add_interval(i * 10, i * 10 + 5, i);
        }
        assert_eq!(d.intervals.len(), 1000);
        assert!(d.intervals.windows(2).all(|w| w[0].1 <= w[1].0));
    }

    #[test]
    fn coverage_merges_and_reports_covered() {
        let mut c = Coverage::default();
        c.total = 100;
        c.add_interval(0, 30, 1000);
        c.add_interval(50, 80, 2000);
        assert_eq!(c.covered(), 60);
        c.add_interval(20, 60, 3000); // bridges the gap (overlap)
        assert_eq!(c.intervals, vec![(0, 80, 3000, 0)]);
        c.add_interval(80, 100, 4000); // adjacent: stays separate (own ts)
        assert_eq!(c.intervals, vec![(0, 80, 3000, 0), (80, 100, 4000, 0)]);
        assert_eq!(c.covered(), 100);
        c.add_interval(200, 200, 5000); // empty ignored
        assert_eq!(c.intervals, vec![(0, 80, 3000, 0), (80, 100, 4000, 0)]);
    }

    /// The rebuild path: files are existence, the interval list is the
    /// policy's map of them. Survivors inherit the read time and count of the
    /// interval that covered them; bytes the ledger has no record of get a
    /// fresh stamp and zero reads; and the ceiling still holds even when the
    /// disk holds more files than a row may describe.
    #[test]
    fn adopt_files_keeps_the_invariants_and_carries_policy() {
        let mut c = Coverage::default();
        c.add_interval(0, 100, 10); // one merged interval over two files
        c.add_interval(500, 600, 20);
        c.intervals[0].3 = 3; // it was read three times
        c.adopt_files(&[(0, 50), (50, 100), (500, 600)], 99);
        assert_eq!(
            c.intervals,
            vec![(0, 50, 10, 3), (50, 100, 10, 3), (500, 600, 20, 0)],
            "survivors keep the policy input of the interval that named them"
        );

        // No record at all (decay or the ceiling dropped it): fresh, unread.
        c.adopt_files(&[(700, 800)], 77);
        assert_eq!(c.intervals, vec![(700, 800, 77, 0)]);

        // A rebuild can find more files than a row may hold; the ceiling is
        // re-established by the same merge the insert path uses.
        let files: Vec<(u64, u64)> =
            (0..MAX_INTERVALS_PER_KEY as u64 + 10).map(|i| (i, i + 1)).collect();
        c.adopt_files(&files, 5);
        assert!(c.intervals.len() <= MAX_INTERVALS_PER_KEY, "{}", c.intervals.len());
        assert_eq!(c.covered(), MAX_INTERVALS_PER_KEY as u64 + 10);
    }

    #[test]
    fn coverage_window_decay_drops_stale_intervals() {
        let mut c = Coverage::default();
        c.total = 100;
        c.add_interval(0, 30, 1000);
        c.add_interval(50, 80, 2000);
        // Window 1000ms, now=2500: interval [0,30) read at 1000 is stale.
        // One pass reports the surviving coverage, so a caller does not have
        // to walk the ledger again to learn what is left.
        assert_eq!(c.decay_and_covered(2500, 1000), 30);
        assert_eq!(c.intervals, vec![(50, 80, 2000, 0)]);
        assert_eq!(c.covered(), 30);
        // Everything stale: ledger empties, coverage 0.
        assert_eq!(c.decay_and_covered(5000, 1000), 0);
        assert!(c.intervals.is_empty());
        assert_eq!(c.covered(), 0);
        // Zero window = no decay, but coverage is still reported.
        c.add_interval(0, 10, 100);
        assert_eq!(c.decay_and_covered(999999, 0), 10);
        assert_eq!(c.intervals.len(), 1);
    }

    /// A sequential scrub stages adjacent shards, which stay separate on
    /// purpose (each keeps its own read time) — so the vector grew with the
    /// request count and was walked under the one process-wide ledger lock.
    /// Compaction must bound it WITHOUT changing a byte of coverage: the
    /// spans here are contiguous, so merging them is exact.
    #[test]
    fn a_sequential_walk_is_bounded_and_loses_no_coverage() {
        let mut c = Coverage::default();
        const SHARD: u64 = 16;
        let shards = (MAX_INTERVALS_PER_KEY as u64) + 500;
        // The whole object is the walk, so full coverage is the expectation.
        c.total = shards * SHARD;
        for i in 0..shards {
            // Contiguous 16-byte shards, each read at its own time.
            c.add_interval(i * SHARD, (i + 1) * SHARD, i);
        }
        assert!(
            c.intervals.len() <= MAX_INTERVALS_PER_KEY,
            "the ledger must stay under its ceiling, got {}",
            c.intervals.len()
        );
        assert_eq!(
            c.covered(),
            shards * SHARD,
            "compaction must not lose (or invent) coverage"
        );
        assert!(
            c.intervals.windows(2).all(|w| w[0].1 <= w[1].0),
            "still sorted and non-overlapping"
        );
    }

    /// With gaps between the spans there is nothing exact left to merge:
    /// merging across a gap would claim bytes we do not hold, so the coldest
    /// spans are dropped. Under-reporting coverage is the safe direction —
    /// the ledger under-reports rather than claiming a gap it cannot fill.
    #[test]
    fn a_gapped_ledger_is_bounded_by_dropping_the_coldest_spans() {
        let mut c = Coverage::default();
        c.total = 1_000_000;
        let n = MAX_INTERVALS_PER_KEY + 100;
        for i in 0..n as u64 {
            c.add_interval(i * 100, i * 100 + 10, i); // 10 read, 90 gap
        }
        assert!(c.intervals.len() <= MAX_INTERVALS_PER_KEY);
        let covered = c.covered();
        assert!(covered <= n as u64 * 10, "coverage may only be under-reported");
        assert!(
            c.intervals.windows(2).all(|w| w[0].1 <= w[1].0),
            "still sorted and non-overlapping"
        );
        // The surviving spans are the most recently read ones.
        let last = c.intervals.last().unwrap();
        assert_eq!((last.0, last.1), ((n as u64 - 1) * 100, (n as u64 - 1) * 100 + 10));
    }

    /// A claim the disk does not back is refused, and that is the one failure
    /// this module exists to prevent: a record naming bytes nobody can serve.
    #[tokio::test]
    async fn adoption_refuses_a_span_the_disk_does_not_back() {
        let dir = tempfile::tempdir().unwrap();
        let ledger = Ledger::new(dir.path().to_path_buf());

        // Nothing on disk: nothing recorded, nothing accounted.
        assert!(!ledger.adopt("a.bin", (0, 100), 1).await);
        assert_eq!(ledger.total_staged().await, 0, "a refused claim adds no bytes");
        assert!(ledger.view("a.bin").await.is_none(), "and leaves no row behind");

        // A file that IS there: recorded at the FILE's length, not the
        // caller's claim — a short write claims short.
        std::fs::write(store::seg_path(dir.path(), "a.bin", 0, 100), vec![0u8; 40]).unwrap();
        assert!(ledger.adopt("a.bin", (0, 100), 1).await);
        assert_eq!(ledger.total_staged().await, 40);
        let view = ledger.view("a.bin").await.unwrap();
        assert_eq!((view.spans[0].start, view.spans[0].end), (0, 40));

        // A claim wider than the files, on a row that already exists: still
        // refused while the disk does not cover it.
        assert!(!ledger.adopt("a.bin", (0, 200), 1).await);
        assert_eq!(ledger.total_staged().await, 40, "the refused claim changed nothing");

        // Once the files cover it collectively, the same claim is honoured —
        // that is the shape `compact` leaves behind — and it adds no bytes,
        // because those bytes are already accounted by their own files.
        std::fs::write(store::seg_path(dir.path(), "a.bin", 40, 80), vec![0u8; 40]).unwrap();
        assert!(ledger.adopt("a.bin", (40, 80), 1).await);
        assert!(ledger.adopt("a.bin", (0, 80), 1).await, "the union backs the wider claim");
        assert_eq!(ledger.total_staged().await, 80, "and counts each byte once");
    }

    /// The account follows the disk, not the record: decay removes ledger
    /// records and leaves the bytes alone, and `forget` gives up exactly what
    /// the caller measured leaving.
    #[tokio::test]
    async fn the_account_follows_the_disk_not_the_record() {
        let dir = tempfile::tempdir().unwrap();
        let ledger = Ledger::new(dir.path().to_path_buf());
        std::fs::write(store::seg_path(dir.path(), "a.bin", 0, 100), vec![0u8; 100]).unwrap();
        assert!(ledger.adopt("a.bin", (0, 100), 1_000).await);
        assert_eq!(ledger.total_staged().await, 100);

        // Window decay drops the record; the file is still there, so the
        // account keeps counting it.
        assert_eq!(ledger.decayed_coverage("a.bin", 100_000, 1_000).await, 0);
        assert_eq!(ledger.total_staged().await, 100, "decay removes records, not bytes");

        // The sweep measured 100 bytes leaving the disk.
        ledger.forget("a.bin", 100).await;
        assert_eq!(ledger.total_staged().await, 0);
        assert!(ledger.view("a.bin").await.is_none());
    }
}

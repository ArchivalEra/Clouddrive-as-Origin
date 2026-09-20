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
    pub(crate) backend_key: String,
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
}

impl Staging {
    pub(crate) fn new(
        coverage: Arc<Mutex<HashMap<String, store::Coverage>>>,
        state: Arc<RwLock<CacheState>>,
        config: Arc<Config>,
    ) -> Self {
        Self { coverage, state, config }
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
    pub(crate) async fn expired(&self, ttl_ms: u64, now: u64) -> Vec<String> {
        let cov = self.coverage.lock().await;
        cov.iter()
            .filter(|(_, c)| now.saturating_sub(c.last_touch_millis) >= ttl_ms)
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
        let order = self.rows_by_age().await;
        let mut freed_total = 0u64;
        let mut touched = 0usize;
        for (_, key) in order {
            if freed_total >= need_bytes {
                break;
            }
            let (age, prior) = {
                let cov = self.coverage.lock().await;
                match cov.get(&key) {
                    Some(c) => (now.saturating_sub(c.last_touch_millis), c.intervals.clone()),
                    None => continue,
                }
            };
            // Never evict a row that is actively staging right now.
            if age < STAGE_MIN_AGE_MS {
                continue;
            }
            let picks = self.pick_spans(&key, &prior, need_bytes - freed_total).await;
            if picks.is_empty() {
                continue;
            }
            let freed = self.drop_spans(&key, &picks, now).await;
            if freed == 0 {
                continue;
            }
            {
                let mut s = self.state.write().await;
                s.segment_bytes = s.segment_bytes.saturating_sub(freed);
            }
            freed_total += freed;
            touched += 1;
            // A row that lost every span has nothing left to protect: drop it
            // with its version marker so the next request re-stats upstream
            // instead of gating on an etag for bytes that no longer exist.
            self.drop_empty_row(&key).await;
        }
        (touched, freed_total)
    }

    /// Rows oldest-touched-first — the cross-key LRU both policies share.
    async fn rows_by_age(&self) -> Vec<(u64, String)> {
        let cov = self.coverage.lock().await;
        let mut rows: Vec<(u64, String)> =
            cov.iter().map(|(k, c)| (c.last_touch_millis, k.clone())).collect();
        rows.sort();
        rows
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
        let mut picked = Vec::new();
        let mut acc = 0u64;
        for i in ordered {
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
            backend_key,
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
            if !backend_key.is_empty() {
                entry.backend_key = backend_key.clone();
            }
            if !upstream_id.is_empty() {
                entry.upstream_id = upstream_id.clone();
            }
            entry.add_interval(start, end, now_millis);
            entry.last_touch_millis = now_millis;
            // Window decay: drop intervals whose last read is older than the
            // coverage window, so stale staged bytes stop being served. Disk
            // sidecars stay for the sweep.
            entry.decay_and_covered(now_millis, window_millis);
        }
        let meta = store::SegMeta {
            etag,
            total,
            backend_key,
            upstream_id,
        };
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

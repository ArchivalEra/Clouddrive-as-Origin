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
    config::Config,
};

use super::cache::CacheState;

/// Never evict a staging ledger row younger than this (P56): an active
/// transfer's row is touched continuously and must not be yanked mid-flight.
const STAGE_MIN_AGE_MS: u64 = 60_000;

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

    /// Ledger rows to drop when staged bytes overshoot the budget: oldest
    /// touched first, never a row that is actively staging right now.
    /// `overrun` is the magazine's `resident + staged - budget`.
    pub(crate) async fn over_budget_rows(&self, overrun: u64, now: u64) -> Vec<(String, u64)> {
        let cov = self.coverage.lock().await;
        let mut rows: Vec<(u64, String)> =
            cov.iter().map(|(k, c)| (c.last_touch_millis, k.clone())).collect();
        rows.sort();
        let mut over = overrun;
        let mut picked = Vec::new();
        for (_, k) in rows {
            if over == 0 {
                break;
            }
            let sz = cov.get(&k).map(|c| c.covered_bytes()).unwrap_or(0);
            // Never evict a row that is actively staging right now.
            if sz == 0 || now.saturating_sub(cov[&k].last_touch_millis) < STAGE_MIN_AGE_MS {
                continue;
            }
            over = over.saturating_sub(sz);
            picked.push((k, sz));
        }
        picked
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
    })
}

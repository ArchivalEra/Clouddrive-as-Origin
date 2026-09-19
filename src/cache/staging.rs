//! The staging ledger: the efficient profile's transfer history (spec §3.12).
//!
//! One completed staged interval enters here (`seal`), the coverage ledger is
//! merged and decayed in a single pass, and promotion is checked and
//! single-flighted. BOTH sealing paths — the viewer-disconnect watcher and
//! the body's exhaustion tail — go through [`Staging::seal_and_maybe_promote`],
//! so the ledger has exactly one writer shape: before this, that sequence was
//! spelled out twice with nine arguments each, inside one function.
//!
//! Lock discipline (C3, ADR-0008): the coverage lock is taken before any
//! state guard, never after; redb and filesystem work happens outside both.

use std::{collections::{HashMap, HashSet}, sync::Arc};
use tokio::sync::{Mutex, RwLock};

use crate::{
    backend::{BackendRegistry, Key},
    cache::{magazine::Magazine, store},
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
/// taken once instead of re-plumbed through eleven parameters.
#[derive(Clone)]
pub(crate) struct Staging {
    coverage: Arc<Mutex<HashMap<String, store::Coverage>>>,
    state: Arc<RwLock<CacheState>>,
    config: Arc<Config>,
    backends: BackendRegistry,
    promotions: Arc<Mutex<HashSet<String>>>,
    magazine: Magazine,
}

impl Staging {
    pub(crate) fn new(
        coverage: Arc<Mutex<HashMap<String, store::Coverage>>>,
        state: Arc<RwLock<CacheState>>,
        config: Arc<Config>,
        backends: BackendRegistry,
        promotions: Arc<Mutex<HashSet<String>>>,
        magazine: Magazine,
    ) -> Self {
        Self { coverage, state, config, backends, promotions, magazine }
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
    /// finalize backstop, promotion verify) — never assembles mixed versions.
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

    /// Merge one completed staged interval and check promotion — the ONE
    /// entry both sealing paths use. Returns the coverage the merge left.
    ///
    /// Ledger merge + seal rename happen only on exhaustion, so aborts are
    /// sweep-safe; the watcher path covers the disconnect case by sealing
    /// whatever landed before the viewer went away.
    pub(crate) async fn seal_and_maybe_promote(&self, span: FinalizedSpan) {
        let FinalizedSpan { cache_dir, key, backend_key, upstream_id, etag, total, start, end, bytes, now_millis } =
            span;
        let window_millis = self.config.cache_profile(&upstream_id).coverage_window_secs * 1000;
        let covered = self
            .seal(
                FinalizedSpan { cache_dir, key: key.clone(), backend_key, upstream_id: upstream_id.clone(), etag, total, start, end, bytes, now_millis },
                window_millis,
            )
            .await;
        // Coverage-triggered promotion (P2-b): threshold met → background
        // assemble + seal. Fire-and-forget by design.
        self.maybe_promote(&key, &upstream_id, now_millis, covered).await;
    }

    /// Merge one completed staged interval into the coverage ledger (the only
    /// writer besides the startup scan). Etag-locked: a version change with
    /// history present resets (drops staged files + ledger) so promotion can
    /// never assemble a mixed-version file. Unknown etags adopt; totals adopt
    /// when known. Best-effort fs ops — the scan heals any gap.
    async fn seal(&self, span: FinalizedSpan, window_millis: u64) -> u64 {
        let covered = {
            let mut cov = self.coverage.lock().await;
            let entry = cov.entry(span.key.clone()).or_default();
            let version_changed = match (&entry.etag, &span.etag) {
                (Some(a), Some(b)) => a != b,
                _ => false,
            };
            if version_changed {
                // New bytes already sealed above: keep this file, drop the rest.
                let fresh = store::seg_path(&span.cache_dir, &span.key, span.start, span.end);
                store::remove_key_segments(&span.cache_dir, &span.key, Some(&fresh));
                *entry = store::Coverage::default();
            }
            if span.etag.is_some() {
                entry.etag = span.etag.clone();
            }
            if span.total != 0 {
                entry.total = span.total;
            }
            if !span.backend_key.is_empty() {
                entry.backend_key = span.backend_key.clone();
            }
            if !span.upstream_id.is_empty() {
                entry.upstream_id = span.upstream_id.clone();
            }
            entry.add_interval(span.start, span.end, span.now_millis);
            entry.last_touch_millis = span.now_millis;
            // Window decay: drop intervals whose last read is older than the
            // coverage window, so stale staged bytes stop counting toward
            // promotion. Disk sidecars stay for the sweep. One pass returns
            // the surviving coverage, which is what the promotion check needs
            // — it used to walk the same vector twice more.
            entry.decay_and_covered(span.now_millis, window_millis)
        };
        let meta = store::SegMeta {
            etag: span.etag.clone(),
            total: span.total,
            backend_key: span.backend_key.clone(),
            upstream_id: span.upstream_id.clone(),
        };
        if let Ok(b) = serde_json::to_vec(&meta) {
            let _ = tokio::fs::write(store::segmeta_path(&span.cache_dir, &span.key), b).await;
        }
        self.state.write().await.segment_bytes += span.bytes;
        covered
    }

    /// Coverage-triggered promotion check (P2-b): runs at the end of every
    /// staged C transfer. Threshold met → spawn exactly one promotion task
    /// per key (single-flight via `promotions`); anything else → no-op.
    /// Lock discipline: no lock held across the spawn.
    async fn maybe_promote(&self, key: &str, upstream_id: &str, now_millis: u64, covered_bytes: u64) {
        let prof = self.config.cache_profile(upstream_id);
        if !prof.efficient {
            return;
        }
        let ready = {
            let cov = self.coverage.lock().await;
            let c = cov.get(key);
            // Ratio met AND the object can be kept: assembling something larger
            // than the whole budget would consume the staged segments and hand
            // the magazine an entry it must immediately eject -- the merge would
            // destroy warmth it could have kept as segments.
            //
            // `covered_bytes` comes from the seal's decay pass, which ran
            // immediately before this with the same window and the same clock —
            // a second decay here would walk the same vector for nothing.
            let fits = c.is_some_and(|c| c.total > 0 && c.total <= self.config.max_size_bytes);
            let ready =
                c.and_then(|c| c.ratio_of(covered_bytes)).is_some_and(|r| r >= prof.coverage_threshold) && fits;
            ready
        };
        if !ready {
            return;
        }
        {
            let mut p = self.promotions.lock().await;
            if !p.insert(key.to_string()) {
                return;
            }
        }
        let staging = Staging {
            coverage: Arc::clone(&self.coverage),
            state: Arc::clone(&self.state),
            config: Arc::clone(&self.config),
            backends: self.backends.clone(),
            promotions: Arc::clone(&self.promotions),
            magazine: self.magazine.clone(),
        };
        let (key, upstream_id) = (key.to_string(), upstream_id.to_string());
        tokio::spawn(async move {
            staging.promote(&key, &upstream_id, now_millis).await;
            staging.promotions.lock().await.remove(&key);
        });
    }

    /// Assemble a promoted entry: re-verify the version by fresh stat (abort +
    /// reset on ANY drift — never a mixed-version file), copy covered slices
    /// from sidecars, fetch gaps by exact Range, seal, install, clean staged
    /// history. All failures abort silently (segments stay for a later retry).
    async fn promote(&self, key: &str, upstream_id: &str, now_millis: u64) {
        // Snapshot the ledger (unknown version/size or unmapped keys wait for
        // fresh transfers — conservative by design).
        let cov = {
            match self.coverage.lock().await.get(key).cloned() {
                Some(c) if !c.backend_key.is_empty() && c.total != 0 && c.etag.is_some() => c,
                _ => return,
            }
        };
        let etag = cov.etag.clone().unwrap();
        let total = cov.total;
        let slot = match self.backends.get(upstream_id) {
            Some(s) => s,
            None => return,
        };
        let bkey = Key::from_validated(cov.backend_key.clone());
        let cache_dir = self.config.cache_dir.clone();
        let live = {
            let _permit = slot.gate.acquire().await;
            match slot.backend.stat(&bkey).await {
                Ok(m) => m,
                Err(_) => return,
            }
        };
        // Assembly fetches bytes: a stream (B1).
        let _stream_permit = slot.stream_gate.acquire().await;
        if live.etag != Some(etag) || live.size_bytes != total {
            // Drifted under us: drop staged history, start over.
            self.reset(key).await;
            return;
        }
        let tmp = store::tmp_path(&cache_dir, key);
        if !assemble_file(&slot, &bkey, &cache_dir, key, &cov, &tmp).await {
            let _ = std::fs::remove_file(&tmp);
            return;
        }
        let dest = store::file_path(&cache_dir, key);
        if store::install_tmp(&tmp, &dest, &cache_dir).is_err() {
            let _ = std::fs::remove_file(&tmp);
            return;
        }
        // The hold is armed here and only here: a promotion is the one write that
        // paid for many upstream fetches to assemble something, and the inactivity
        // clock cannot see that. 0 (the default off switch) leaves the deadline at
        // zero, which `is_held` reads as "no hold".
        let hold_until_millis = if self.config.promoted_hold_secs == 0 {
            0
        } else {
            now_millis.saturating_add(self.config.promoted_hold_secs.saturating_mul(1000))
        };
        // Promotion never produces a stray: its fit guard refuses an object the
        // magazine cannot hold, so the entry it installs is a magazine member.
        self.magazine
            .install(key, upstream_id, &live, now_millis, hold_until_millis, false)
            .await;
        // History is now redundant: drop sidecars + ledger row.
        self.reset(key).await;
    }
}

/// Fill `tmp` with the full object: covered slices copied from sidecars,
/// gaps fetched by exact Range. Walks the merged intervals in order; every
/// write is an absolute seek, so order is a courtesy, not a requirement.
async fn assemble_file(
    slot: &Arc<crate::backend::BackendSlot>,
    bkey: &Key,
    cache_dir: &std::path::Path,
    key: &str,
    cov: &store::Coverage,
    tmp: &std::path::Path,
) -> bool {
    use tokio::io::AsyncWriteExt;
    // Index segment files by interval; the store parses the names so the
    // filename shape is not re-derived here.
    let segs = store::segments_for_key(cache_dir, key);
    let mut out = match tokio::fs::File::create(tmp).await {
        Ok(f) => f,
        Err(_) => return false,
    };
    let mut buf = vec![0u8; 256 * 1024];
    let mut pos = 0u64;
    for &(s, e, _) in cov.intervals.iter() {
        if pos < s && !fetch_gap(slot, bkey, &mut out, &mut buf, pos, s).await {
            return false;
        }
        if !copy_span(&mut out, &segs, &mut buf, s.max(pos), e).await {
            return false;
        }
        pos = pos.max(e);
    }
    if pos < cov.total && !fetch_gap(slot, bkey, &mut out, &mut buf, pos, cov.total).await {
        return false;
    }
    out.flush().await.is_ok()
}

/// Copy one covered span `[a, b)` from whichever sidecars hold it.
async fn copy_span(
    out: &mut tokio::fs::File,
    segs: &[(u64, u64, std::path::PathBuf)],
    buf: &mut [u8],
    mut a: u64,
    b: u64,
) -> bool {
    use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
    while a < b {
        let holder = segs.iter().find(|(s, e, _)| *s <= a && a < *e);
        let (fs, fe, path) = match holder {
            Some(h) => h,
            None => return false,
        };
        let n = (*fe).min(b) - a;
        let mut f = match tokio::fs::File::open(path).await {
            Ok(f) => f,
            Err(_) => return false,
        };
        if f.seek(std::io::SeekFrom::Start(a - fs)).await.is_err() {
            return false;
        }
        let mut remaining = n;
        out.seek(std::io::SeekFrom::Start(a)).await.ok();
        while remaining > 0 {
            let want = remaining.min(buf.len() as u64) as usize;
            let r = match f.read(&mut buf[..want]).await {
                Ok(0) => return false,
                Ok(r) => r,
                Err(_) => return false,
            };
            if out.write_all(&buf[..r]).await.is_err() {
                return false;
            }
            remaining -= r as u64;
            a += r as u64;
        }
    }
    true
}

/// Fetch one missing span by exact Range into `out` at absolute `start`.
async fn fetch_gap(
    slot: &Arc<crate::backend::BackendSlot>,
    bkey: &Key,
    out: &mut tokio::fs::File,
    buf: &mut [u8],
    start: u64,
    end: u64,
) -> bool {
    use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
    let src = match slot
        .backend
        .open(bkey, Some(crate::backend::ByteRange::bounded(start, end - start)))
        .await
    {
        Ok(s) => s,
        Err(_) => return false,
    };
    let mut stream = src.stream;
    if out.seek(std::io::SeekFrom::Start(start)).await.is_err() {
        return false;
    }
    let mut remaining = end - start;
    while remaining > 0 {
        let want = remaining.min(buf.len() as u64) as usize;
        let n = match stream.read(&mut buf[..want]).await {
            Ok(0) => return false, // short backend read: do not seal a short file
            Ok(n) => n,
            Err(_) => return false,
        };
        if out.write_all(&buf[..n]).await.is_err() {
            return false;
        }
        remaining -= n as u64;
    }
    true
}

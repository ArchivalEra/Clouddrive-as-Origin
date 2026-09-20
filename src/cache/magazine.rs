//! The magazine: everything the byte budget governs (spec §3.4, ADR-0013/0014).
//!
//! Admission (`fits`), installation (`install`), eviction (`evict_budget`),
//! inactivity reaping (`reap`), disk-pressure reclaim (`reclaim_strays`) and
//! the one deletion site (`delete`) live here, behind a receiver — callers no
//! longer re-plumb `CacheState`/`Config`/`MetaStore` into free functions.
//!
//! Lock discipline (C3, ADR-0008): every method that takes the state guard
//! keeps redb and filesystem work OUTSIDE it. `delete` is the one async
//! mutator and must never be called while a guard is held — the compiler
//! cannot see that yet, which is exactly why the guards are taken here and
//! nowhere else.

use std::sync::Arc;
use tokio::sync::RwLock;

use super::cache::CacheState;

use crate::{
    backend::ObjectMeta,
    cache::{meta::EntryMeta, store},
    config::Config,
};

/// Disk headroom held back from cold pulls (P56): the node must keep room
/// for logs, the redb file, and an operator's emergency shell even when the
/// cache is at its configured maximum.
pub(crate) const DISK_RESERVE_BYTES: u64 = 512 * 1024 * 1024;

/// Free-space floor the tick reclaims resident strays down to (ADR-0014).
/// The reserve alone is too tight to be a working margin: strays sit outside
/// the magazine's byte budget, so nothing else bounds how much of the disk
/// they take, and a cold pull that finds less than its own size free would
/// have to either refuse (which the cache never does) or evict a stray in a
/// hurry. Keeping this much free at all times makes the admission path's
/// "make room" step rare instead of routine.
const DISK_PRESSURE_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// Whether the magazine can hold an object of this size at all. An object
/// that cannot fit is never made to fit by evicting anyone, so the byte
/// budget cannot govern it — such an object is admitted as a *resident
/// stray* instead (ADR-0014): cached while the disk allows it, refused by
/// the byte budget neither as a victim nor as pressure.
///
/// One predicate, three call sites that must agree: promotion's fit guard,
/// staging admission, and cold-pull admission (which decides
/// `EntryMeta::oversize`).
pub(crate) fn fits_magazine(size_bytes: u64, config: &Config) -> bool {
    size_bytes > 0 && size_bytes <= config.max_size_bytes
}

/// Bytes the magazine's byte budget governs: everything except resident
/// strays, which the budget neither counts nor evicts (ADR-0014).
fn resident_bytes_of(state: &CacheState) -> u64 {
    state.total_bytes.saturating_sub(
        state
            .entries
            .values()
            .filter(|m| m.oversize)
            .map(|m| m.size_bytes)
            .sum::<u64>(),
    )
}

/// Max-size LRU victim selection — memory only; deletes happen guard-free
/// via [`Magazine::delete`] (C3 lock discipline).
fn evict_pick(
    state: &mut CacheState,
    config: &Config,
    protected: &std::collections::HashSet<String>,
) -> Vec<(String, u64)> {
    // Two independent budgets (P10): bytes AND entry count. Entry rows cost
    // roughly 500 B of RAM each (key stored twice plus five Strings), and
    // max_size_bytes alone let millions of small objects exhaust memory on
    // a 10.9 GB node while sitting far under the byte cap.
    //
    // Resident strays are outside the byte budget (ADR-0014). They must be
    // excluded from BOTH sides of it: counting them would make the cache
    // permanently "over budget" (so every insert would evict someone else),
    // and evicting them would let one oversized pull take the whole magazine
    // down with it — the self-destruct this rule exists to prevent. An
    // object that alone exceeds the budget is never brought into budget by
    // evicting others; it leaves on the inactivity clock or under disk
    // pressure (`pick_strays_for_pressure`).
    let resident = resident_bytes_of(state);
    let over_bytes = resident > config.max_size_bytes;
    let over_entries = config.max_entries > 0 && state.entries.len() > config.max_entries;
    if !over_bytes && !over_entries {
        return Vec::new();
    }

    // How far over we are, then ONE pass ordered by LRU (O2). The previous
    // shape called `min_by_key` inside the eviction loop, so a sweep of k
    // victims rescanned the whole entry map k times: O(entries x victims)
    // under the state write guard, which blocks every hit.
    let bytes_over = resident.saturating_sub(config.max_size_bytes);
    let entries_over = if over_entries {
        state.entries.len().saturating_sub(config.max_entries)
    } else {
        0
    };

    // Collect evictable rows (negative tombstones are not LRU-eligible:
    // they hold no file and expire on their own clock; resident strays are
    // not byte-budget-eligible), ordered oldest first.
    // `sort_unstable_by_key` on the eligibility timestamp gives the same
    // victim order as repeated `min_by_key` did.
    let mut candidates: Vec<(u64, String)> = state
        .entries
        .iter()
        .filter(|(_, m)| m.negative_until_millis.is_none() && !m.oversize)
        // A key a viewer is reading (or read a moment ago) is not budget
        // material: taking its bytes away is what makes the next seek pay a
        // re-fetch (ADR-0017).
        .filter(|(k, _)| !protected.contains(k.as_str()))
        .map(|(k, m)| (m.eligible_at(config.inactive_ttl_secs), k.clone()))
        .collect();
    candidates.sort_unstable_by_key(|(eligible, _)| *eligible);

    let mut out = Vec::new();
    let mut freed = 0u64;
    for (_, key) in candidates {
        // Stop once BOTH budgets are satisfied: whatever drove the sweep is
        // now back under its cap.
        if freed >= bytes_over && out.len() >= entries_over {
            break;
        }
        if let Some(m) = state.entries.remove(&key) {
            state.total_bytes = state.total_bytes.saturating_sub(m.size_bytes);
            freed = freed.saturating_add(m.size_bytes);
            out.push((key, m.size_bytes));
        }
    }
    out
}

/// Disk-pressure victim selection: resident strays only, oldest-touched
/// first, until `need_bytes` are freed. Memory only; deletes happen
/// guard-free via [`Magazine::delete`].
///
/// Strays are the only population this may take. They sit outside the
/// magazine's byte budget (ADR-0014), so they are what the disk was never
/// promised, and evicting them cannot make the magazine's own members
/// re-fetch. A held entry yields here like anywhere else: a hold is a
/// deadline, not immortality.
///
/// Pure in its inputs (a `need_bytes` computed by the caller from a real
/// `statvfs` probe) so it can be tested without a synthetic filesystem.
fn pick_strays_for_pressure(
    state: &mut CacheState,
    config: &Config,
    need_bytes: u64,
) -> Vec<(String, u64)> {
    if need_bytes == 0 {
        return Vec::new();
    }
    let mut candidates: Vec<(u64, String)> = state
        .entries
        .iter()
        .filter(|(_, m)| m.oversize)
        .map(|(k, m)| (m.eligible_at(config.inactive_ttl_secs), k.clone()))
        .collect();
    candidates.sort_unstable_by_key(|(eligible, _)| *eligible);
    let mut out = Vec::new();
    let mut freed = 0u64;
    for (_, key) in candidates {
        if freed >= need_bytes {
            break;
        }
        if let Some(m) = state.entries.remove(&key) {
            state.total_bytes = state.total_bytes.saturating_sub(m.size_bytes);
            freed = freed.saturating_add(m.size_bytes);
            out.push((key, m.size_bytes));
        }
    }
    out
}

/// Inactive-expiry collection — memory only. Persistence and file
/// deletes happen guard-free via [`Magazine::delete`] (C3 lock discipline).
fn reap_collect(
    state: &mut CacheState,
    ttl_ms: u64,
    now: u64,
    protected: &std::collections::HashSet<String>,
) -> Vec<(String, u64)> {
    let expired: Vec<String> = state
        .entries
        .iter()
        .filter(|(_, m)| {
            m.negative_until_millis.map_or_else(
                || now.saturating_sub(m.last_access_millis) >= ttl_ms,
                |until| now >= until,
            )
        })
        // Idleness is measured from the last REQUEST, so a stream longer than
        // the TTL would be swept mid-flight; a lease outranks the clock
        // (ADR-0017). Negative tombstones are not leases: they hold no bytes.
        .filter(|(k, m)| m.negative_until_millis.is_some() || !protected.contains(k.as_str()))
        .map(|(k, _)| k.clone())
        .collect();
    let mut out = Vec::with_capacity(expired.len());
    for k in expired {
        if let Some(m) = state.entries.remove(&k) {
            state.total_bytes = state.total_bytes.saturating_sub(m.size_bytes);
            out.push((k, m.size_bytes));
        }
    }
    out
}

/// The magazine's receiver: the three pieces every budget decision needs,
/// taken once instead of re-plumbed through every free function.
#[derive(Clone)]
pub(crate) struct Magazine {
    state: Arc<RwLock<CacheState>>,
    config: Arc<Config>,
    meta: Arc<crate::cache::persist::MetaStore>,
    /// Read leases: policy eviction spares what a viewer is streaming
    /// (ADR-0017). Disk pressure deliberately does not consult them.
    leases: Arc<super::leases::Leases>,
}

impl Magazine {
    pub(crate) fn new(
        state: Arc<RwLock<CacheState>>,
        config: Arc<Config>,
        meta: Arc<crate::cache::persist::MetaStore>,
        leases: Arc<super::leases::Leases>,
    ) -> Self {
        Self { state, config, meta, leases }
    }

    /// Whether the magazine can hold an object this size (see
    /// [`fits_magazine`]).
    pub(crate) fn fits(&self, size_bytes: u64) -> bool {
        fits_magazine(size_bytes, &self.config)
    }

    /// Of `total_bytes`, how much belongs to resident strays.
    pub(crate) async fn strays_bytes(&self) -> u64 {
        let s = self.state.read().await;
        s.entries
            .values()
            .filter(|m| m.oversize)
            .map(|m| m.size_bytes)
            .sum::<u64>()
    }

    /// Install a completed entry (cold-pull seal or promotion) and run the
    /// eviction sweep it may have triggered. Persist first: on crash between
    /// redb and memory, startup rebuilds memory from redb; the reverse order
    /// would lose the row.
    ///
    /// C3: the state guard never spans the redb commit or the file deletes.
    pub(crate) async fn install(
        &self,
        key: &str,
        upstream_id: &str,
        meta: &ObjectMeta,
        now: u64,
        // Whether this entry was admitted as a resident stray (ADR-0014):
        // larger than the whole magazine budget, so the byte budget neither
        // counts it nor evicts it.
        oversize: bool,
    ) {
        let (old_size, entry) = {
            let s = self.state.read().await;
            let old_size = s.entries.get(key).map(|m| m.size_bytes).unwrap_or(0);
            let entry = EntryMeta {
                version: 1,
                upstream_id: upstream_id.to_string(),
                key: key.to_string(),
                size_bytes: meta.size_bytes,
                etag: meta.etag.clone(),
                last_modified: meta.last_modified.clone(),
                // Raw provider hint: MIME resolution happens once, at read time
                // (hit_meta_*), never at write. Old rows holding resolved
                // values re-resolve idempotently (resolve passes specifics through).
                content_type: meta.mime_hint.clone(),
                created_at_millis: s.entries.get(key).map(|m| m.created_at_millis).unwrap_or(now),
                last_access_millis: now,
                last_revalidated_millis: Some(now),
                negative_until_millis: None,
                oversize,
            };
            (old_size, entry)
        };
        if let Err(e) = self.meta.insert(&entry).await {
            tracing::error!(key = %key, error = %e, "redb insert failed");
        }
        let evicted = {
            let mut s = self.state.write().await;
            s.total_bytes = s.total_bytes.saturating_sub(old_size) + entry.size_bytes;
            s.entries.insert(key.to_string(), entry);
            evict_pick(&mut s, &self.config, &std::collections::HashSet::new())
        };
        self.delete(&evicted).await;
    }

    /// Install a negative 404 tombstone (stampede protection, spec §3.6).
    pub(crate) async fn install_negative(&self, key: &str, upstream_id: &str, now: u64) {
        let until = now + self.config.negative_ttl_secs * 1000;
        let entry = EntryMeta {
            version: 1,
            upstream_id: upstream_id.to_string(),
            key: key.to_string(),
            size_bytes: 0,
            etag: None,
            last_modified: None,
            content_type: None,
            created_at_millis: now,
            last_access_millis: now,
            last_revalidated_millis: None,
            negative_until_millis: Some(until),
            oversize: false,
        };
        let _ = self.meta.insert(&entry).await;
        let mut s = self.state.write().await;
        s.entries.insert(key.to_string(), entry);
    }

    /// Whether `want` bytes can be written to the cache disk, making room
    /// first if needed: it evicts resident strays (oldest-touched first) and
    /// then asks again. Returns false when even that is not enough — the
    /// caller then serves the request WITHOUT caching it rather than
    /// refusing to serve it (ADR-0013).
    ///
    /// Only strays yield here. They sit outside the magazine's byte budget
    /// by definition (ADR-0014), so removing one is the least destructive
    /// way to make room; the magazine's own members leave on their clock.
    /// The tick reclaims strays down to the same floor on its own schedule,
    /// which keeps this path's eviction step rare.
    pub(crate) async fn make_room(&self, want: u64) -> bool {
        if store::has_room_for(&self.config.cache_dir, want, DISK_RESERVE_BYTES) {
            return true;
        }
        let free = store::free_bytes(&self.config.cache_dir).unwrap_or(0);
        let need = DISK_RESERVE_BYTES.saturating_add(want).saturating_sub(free);
        let victims = self.reclaim_strays(need).await;
        if !victims.is_empty() {
            tracing::warn!(
                want,
                free,
                victims = victims.len(),
                "making room for a cold pull by evicting resident strays"
            );
            self.delete(&victims).await;
        }
        // A second probe, not arithmetic on the first: the deletes above are
        // asynchronous and the accounting is the filesystem's, not ours.
        store::has_room_for(&self.config.cache_dir, want, DISK_RESERVE_BYTES)
    }

    /// redb row removal + cache-file deletion for victims collected under a
    /// state guard. Never call this while holding the guard (C3).
    pub(crate) async fn delete(&self, victims: &[(String, u64)]) {
        if victims.is_empty() {
            return;
        }
        // One redb transaction for the whole victim batch (P5), then the file
        // deletes.
        let keys: Vec<String> = victims.iter().map(|(k, _)| k.clone()).collect();
        if let Err(e) = self.meta.remove_batch(&keys).await {
            tracing::warn!(error = %e, "batched redb remove failed");
        }
        for (k, _) in victims {
            let path = store::file_path(&self.config.cache_dir, k);
            // Last-resort guard: this is the only place a key is turned back
            // into a deletion, so a row that names infrastructure is refused
            // here even if it somehow reached the victim list. Losing a reaped
            // object costs a re-fetch; deleting the metadata store costs every
            // row.
            if store::is_meta_store_path(&self.config.cache_dir, &path) {
                tracing::warn!(key = %k, "refusing to reap a metadata store path");
                continue;
            }
            let _ = tokio::fs::remove_file(&path).await;
            store::prune_empty_parents(&self.config.cache_dir, &path);
        }
    }

    /// Persist rebuilt rows and install them into memory (ticket #57), after
    /// metadata loss. Persist-guard-free-then-one-write-stretch, matching the
    /// C3 discipline used by [`Magazine::install`].
    pub(crate) async fn rebuild(&self, rebuilt: Vec<EntryMeta>, loaded: usize) {
        for entry in &rebuilt {
            if let Err(e) = self.meta.insert(entry).await {
                tracing::error!(key = %entry.key, error = %e, "rebuild: redb insert failed");
            }
        }
        {
            let mut s = self.state.write().await;
            for entry in rebuilt {
                s.total_bytes += entry.size_bytes;
                s.entries.insert(entry.key.clone(), entry);
            }
        }
        let n = self.state.read().await.entries.len();
        tracing::info!(rows = n, loaded, "entry rows rebuilt");
    }

    /// Inactive-expiry pass: returns the victims whose redb rows and files
    /// the caller must remove via [`Magazine::delete`].
    pub(crate) async fn reap(&self, ttl_ms: u64, now: u64) -> Vec<(String, u64)> {
        let protected = self.leases.protected(now);
        let mut s = self.state.write().await;
        reap_collect(&mut s, ttl_ms, now, &protected)
    }

    /// Byte/count budget pass: returns the victims that bring the magazine
    /// back under its caps.
    pub(crate) async fn evict_budget(&self, now: u64) -> Vec<(String, u64)> {
        let protected = self.leases.protected(now);
        let mut s = self.state.write().await;
        evict_pick(&mut s, &self.config, &protected)
    }

    /// Disk-pressure pass: returns the resident strays to delete for
    /// `need_bytes` of free space.
    pub(crate) async fn reclaim_strays(&self, need_bytes: u64) -> Vec<(String, u64)> {
        let mut s = self.state.write().await;
        pick_strays_for_pressure(&mut s, &self.config, need_bytes)
    }

    /// Disk-pressure reclaim on the tick's schedule: when free space falls
    /// below the working floor (reserve + pressure margin), evict resident
    /// strays oldest-touched-first until the floor is restored. Strays only:
    /// the byte budget already governs the magazine's own members.
    pub(crate) async fn reclaim_under_pressure(&self) -> Vec<(String, u64)> {
        let free = store::free_bytes(&self.config.cache_dir).unwrap_or(u64::MAX);
        let floor = DISK_RESERVE_BYTES.saturating_add(DISK_PRESSURE_BYTES);
        if free >= floor {
            return Vec::new();
        }
        let victims = self.reclaim_strays(floor - free).await;
        if !victims.is_empty() {
            tracing::warn!(
                victims = victims.len(),
                "evicting resident strays: free space is below the working floor"
            );
        }
        victims
    }

    /// The staged-bytes check the tick makes: is
    /// `resident_bytes + segment_bytes` over the budget, and by how much.
    pub(crate) async fn staged_overrun(&self, segment_bytes: u64) -> u64 {
        let s = self.state.read().await;
        resident_bytes_of(&s)
            .saturating_add(segment_bytes)
            .saturating_sub(self.config.max_size_bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::persist::MetaStore;
    use tempfile::tempdir;

    fn row(key: &str, size: u64, last_access: u64, oversize: bool) -> EntryMeta {
        EntryMeta {
            version: 1,
            upstream_id: "primary".into(),
            key: key.into(),
            size_bytes: size,
            etag: None,
            last_modified: None,
            content_type: None,
            created_at_millis: last_access,
            last_access_millis: last_access,
            last_revalidated_millis: None,
            negative_until_millis: None,
            oversize,
        }
    }

    /// A resident stray is outside the byte budget in both directions
    /// (ADR-0014): it never pushes the magazine over, and the byte budget
    /// never picks it. The shape this replaces let a single oversized object
    /// take the whole cache down with it -- its own size was the overshoot,
    /// so the sweep evicted every other entry AND the object itself, leaving
    /// a 50 GB pull standing on an empty cache.
    #[test]
    fn a_resident_stray_never_evicts_the_magazine() {
        let mut cfg = Config::default();
        cfg.max_size_bytes = 1_000;
        cfg.inactive_ttl_secs = 1200;

        let mut st = CacheState::default();
        st.entries.insert("small.bin".into(), row("small.bin", 100, 10, false));
        st.entries.insert("stray.bin".into(), row("stray.bin", 5_000, 5, true));
        st.total_bytes = 5_100;
        assert_eq!(resident_bytes_of(&st), 100, "the stray is not part of the magazine's bytes");

        // Over the budget only because of the stray: nobody is evicted.
        assert!(
            evict_pick(&mut st, &cfg, &std::collections::HashSet::new()).is_empty(),
            "a stray must not make the magazine look over budget"
        );
        assert!(st.entries.contains_key("small.bin"), "the member survives");
        assert!(st.entries.contains_key("stray.bin"), "and so does the stray");

        // A second MEMBER pushes the magazine over: it is the member that
        // goes, never the stray.
        st.entries.insert("small2.bin".into(), row("small2.bin", 950, 20, false));
        st.total_bytes += 950;
        let victims = evict_pick(&mut st, &cfg, &std::collections::HashSet::new());
        assert_eq!(
            victims.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>(),
            vec!["small.bin"],
            "the byte budget picks only the rows it governs"
        );
        assert!(st.entries.contains_key("stray.bin"));
        assert!(st.entries.contains_key("small2.bin"));
    }

    /// Disk pressure reclaims resident strays, oldest-touched first, and
    /// touches nothing else: the magazine's own members leave on their clock,
    /// and the disk is not a second silent eviction budget for them.
    #[test]
    fn strays_are_reclaimed_under_disk_pressure_oldest_first() {
        let mut cfg = Config::default();
        cfg.inactive_ttl_secs = 1200;

        let mut st = CacheState::default();
        st.entries.insert("stray-cold.bin".into(), row("stray-cold.bin", 3_000, 10, true));
        st.entries.insert("stray-warm.bin".into(), row("stray-warm.bin", 3_000, 900, true));
        st.entries.insert("member.bin".into(), row("member.bin", 3_000, 5, false));
        st.total_bytes = 9_000;

        assert!(
            pick_strays_for_pressure(&mut st, &cfg, 0).is_empty(),
            "nothing to free when nothing is needed"
        );
        let victims = pick_strays_for_pressure(&mut st, &cfg, 3_000);
        assert_eq!(
            victims,
            vec![("stray-cold.bin".to_string(), 3_000)],
            "the colder stray goes first, even though a member is colder still"
        );
        assert!(st.entries.contains_key("stray-warm.bin"), "the warmer stray stays");
        assert!(st.entries.contains_key("member.bin"), "pressure never takes a member");
        assert_eq!(st.total_bytes, 6_000);
        assert_eq!(resident_bytes_of(&st), 3_000, "the member is untouched and still counted");

        let victims = pick_strays_for_pressure(&mut st, &cfg, 99_000);
        assert_eq!(victims.len(), 1, "only one stray is left to take");
        assert!(st.entries.contains_key("member.bin"));
        assert_eq!(st.total_bytes, 3_000);
    }



    /// Last-resort guard on the single deletion site: even if a store key
    /// reaches the victim list, the reaper must refuse it.
    #[tokio::test]
    async fn delete_refuses_a_store_path_and_nested_look_alikes() {
        let dir = tempdir().unwrap();
        let mut cfg = Config::default();
        cfg.cache_dir = dir.path().to_path_buf();
        let live = dir.path().join(store::META_STORE_FILE);
        std::fs::write(&live, b"database bytes").unwrap();
        // A real object alongside it, so the refusal is specific rather
        // than "nothing is ever deleted".
        let victim = dir.path().join("a.bin");
        std::fs::write(&victim, b"obj").unwrap();
        // A nested object that happens to share the store's file name: the
        // node's log showed this being protected too, which leaked disk
        // instead of protecting the database.
        let nested = dir.path().join("bucket/redb.db");
        std::fs::create_dir_all(nested.parent().unwrap()).unwrap();
        std::fs::write(&nested, b"nested object").unwrap();
        let meta = MetaStore::open(&live).unwrap();

        let magazine = Magazine::new(
            Arc::new(RwLock::new(CacheState::default())),
            Arc::new(cfg),
            Arc::new(meta),
            Arc::new(crate::cache::leases::Leases::new(0)),
        );
        magazine
            .delete(&[
                (store::META_STORE_FILE.to_string(), 14),
                ("bucket/redb.db".to_string(), 13),
                ("a.bin".to_string(), 3),
            ])
            .await;

        assert!(live.exists(), "the reaper must not delete the metadata store");
        assert!(!nested.exists(), "a nested object of the same name must still be reaped");
        assert!(!victim.exists(), "ordinary victims are still reaped");
    }
}

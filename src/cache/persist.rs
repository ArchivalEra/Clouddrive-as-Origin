//! redb-backed entry metadata store (spec §3.10, ADR 0002 / R1 schema).
//!
//! One table in one database file:
//! - `entries`: key → serialized EntryMeta (serde_json — zero extra deps,
//!   well within budget at 100k entries)
//!
//! Deliberately no secondary index: the reaper and evictor scan the
//! in-memory state (memory is source of truth for bytes accounting;
//! healthz reads it too), and startup rebuilds from `entries`. An ordered
//! redb index would only pay off past in-memory scale — YAGNI until then.
//!
//! Writes serialize through a tokio Mutex<Database> — miss-driven writes
//! are rare, so the async mutex (not block_in_place) is the right seam.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use redb::ReadableTable;
use tokio::sync::Mutex;

use crate::cache::meta::EntryMeta;

const ENTRIES: redb::TableDefinition<&str, &[u8]> = redb::TableDefinition::new("entries");

/// Run a blocking redb transaction off the async worker (O3). `commit()`
/// is an fsync: on the 2-core node it parks the calling worker for
/// milliseconds and every task scheduled there waits behind it.
/// `block_in_place` first hands the worker's other tasks to a fresh
/// thread, so the runtime keeps serving while the fsync lands. Tests run a
/// current-thread runtime where `block_in_place` panics, so the flavor is
/// checked and the work runs inline there.
fn blocking<T>(f: impl FnOnce() -> T) -> T {
    match tokio::runtime::Handle::try_current() {
        Ok(h) if h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(f)
        }
        _ => f(),
    }
}

/// Where a store that will not open gets moved to. The `<epoch>` in the name
/// is what the runbook tells an operator to look for, and the probe keeps a
/// second failure inside the same second from renaming over the first
/// archive: `rename` replaces silently, so an unlucky second would have cost
/// the evidence of the first failure.
fn quarantine_path(path: &std::path::Path, stamp: u64) -> std::path::PathBuf {
    let first = path.with_extension(format!("db.corrupt-{stamp}"));
    if !first.exists() {
        return first;
    }
    for n in 2u32.. {
        let candidate = path.with_extension(format!("db.corrupt-{stamp}-{n}"));
        if !candidate.exists() {
            return candidate;
        }
    }
    unreachable!("counted far enough to find a free name")
}

/// What happened when the store was opened (C1): healthz reports this, so a
/// quarantined or rebuilt store is visible instead of living only in logs.
/// ADR-0008 says metadata loss costs revalidation rather than correctness --
/// true, but an operator still needs to SEE that it happened.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreState {
    /// Opened the existing database normally.
    Ready,
    /// The file would not open; it was moved aside and a fresh store took
    /// its place. Quarantine path is recorded for the operator.
    Quarantined { moved_to: String },
}

pub struct MetaStore {
    db: Arc<Mutex<redb::Database>>,
    state: StoreState,
}

/// redb 2 forbids a second handle on the same file within one process
/// ("Database already open"). Cache instances in tests (and a future
/// config reload) can coexist on one cache_dir, so handles are shared
/// per path via this process-wide registry.
fn shared_db(path: &Path) -> anyhow::Result<(Arc<Mutex<redb::Database>>, StoreState)> {
    static REGISTRY: std::sync::OnceLock<std::sync::Mutex<HashMap<PathBuf, Arc<Mutex<redb::Database>>>>> =
        std::sync::OnceLock::new();
    let registry = REGISTRY.get_or_init(|| std::sync::Mutex::new(HashMap::new()));
    let mut reg = registry.lock().unwrap();
    if let Some(db) = reg.get(path) {
        // Reopening the same path in-process (tests, a future reload): the
        // state was decided by the first open.
        return Ok((Arc::clone(db), StoreState::Ready));
    }
    let (db, store_state) = match redb::Database::create(path) {
        Ok(db) => (db, StoreState::Ready),
        Err(e) => {
            // Degrade instead of crash-looping (ticket #57). A redb file
            // that will not open (corrupt in a way redb rejects, or not a
            // file at all) used to propagate into `Cache::new`'s expect()
            // and, with Restart=always, a boot loop. Quarantine it aside
            // and start fresh: the cache is rebuildable metadata, and
            // losing it costs revalidation, not correctness.
            tracing::error!(
                path = %path.display(),
                error = %e,
                "metadata store failed to open; quarantining and starting fresh"
            );
            let stamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            let aside = quarantine_path(path, stamp);
            if let Err(re) = std::fs::rename(path, &aside) {
                tracing::error!(error = %re, "could not quarantine the bad metadata file");
            } else {
                tracing::warn!(quarantined = %aside.display(), "bad metadata file moved aside");
            }
            // A directory where the file should be cannot be renamed over;
            // remove it so the retry can create a real file.
            if path.is_dir() {
                let _ = std::fs::remove_dir_all(path);
            }
            let fresh = redb::Database::create(path)?;
            let moved_to = if aside.exists() {
                aside.display().to_string()
            } else {
                String::new()
            };
            (fresh, StoreState::Quarantined { moved_to })
        }
    };
    let db = Arc::new(Mutex::new(db));
    reg.insert(path.to_path_buf(), Arc::clone(&db));
    Ok((db, store_state))
}

impl MetaStore {
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let (db, store_state) = shared_db(path)?;
        {
            // Create tables up front so readers never race table creation.
            let db_guard = futures::executor::block_on(db.lock());
            let txn = db_guard.begin_write()?;
            {
                txn.open_table(ENTRIES)?;
            }
            txn.commit()?;
        }
        Ok(Self { db, state: store_state })
    }

    /// What the open did (C1): `Ready`, or quarantined with the path the bad
    /// file was moved to.
    pub fn state(&self) -> &StoreState {
        &self.state
    }

    /// Load all entries (startup). Missing-table-safe.
    pub async fn load_all(&self) -> anyhow::Result<Vec<EntryMeta>> {
        let db = self.db.lock().await;
        let txn = db.begin_read()?;
        let table = txn.open_table(ENTRIES)?;
        let mut out = Vec::new();
        for row in table.iter()? {
            let (_, v) = row?;
            let meta: EntryMeta = serde_json::from_slice(v.value())?;
            out.push(meta);
        }
        Ok(out)
    }

    /// Insert or replace an entry (single-table put; bytes accounting
    /// lives in memory).
    pub async fn insert(&self, meta: &EntryMeta) -> anyhow::Result<()> {
        let db = self.db.lock().await;
        blocking(|| {
            let txn = db.begin_write()?;
            {
                let mut entries = txn.open_table(ENTRIES)?;
                entries.insert(meta.key.as_str(), serde_json::to_vec(meta)?.as_slice())?;
            }
            txn.commit()?;
            Ok(())
        })
    }

    /// Remove an entry (expiry / eviction / tombstone drop).
    pub async fn remove(&self, key: &str) -> anyhow::Result<Option<EntryMeta>> {
        let db = self.db.lock().await;
        blocking(|| {
            let txn = db.begin_write()?;
            // Copy out of the table before deserializing: guards borrow the
            // table, so the table binding is dropped before leaving scope.
            let removed_bytes: Option<Vec<u8>> = {
                let mut entries = txn.open_table(ENTRIES)?;
                let out = match entries.remove(key) {
                    Ok(Some(v)) => Some(v.value().to_vec()),
                    Ok(None) => None,
                    Err(e) => return Err(e.into()),
                };
                drop(entries);
                out
            };
            txn.commit()?;
            removed_bytes.map(|b| serde_json::from_slice(&b)).transpose().map_err(anyhow::Error::from)
        })
    }

    /// Update last_access for an existing entry (coalesced flush path).
    /// Rewrites the entry row. No-op when absent.
    pub async fn bump_last_access(&self, key: &str, new_millis: u64) -> anyhow::Result<()> {
        self.bump_last_access_batch(&[(key.to_string(), new_millis)]).await
    }

    /// Update last_access for a whole drained batch in ONE write
    /// transaction (P5). The coalescing window bounds ticks, not commits:
    /// per-key commits meant one fsync per dirty key per second.
    pub async fn bump_last_access_batch(&self, batch: &[(String, u64)]) -> anyhow::Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let db = self.db.lock().await;
        blocking(|| {
            let txn = db.begin_write()?;
            {
                let mut entries = txn.open_table(ENTRIES)?;
                for (key, ms) in batch {
                    let old: Option<EntryMeta> = match entries.get(key.as_str())? {
                        Some(v) => Some(serde_json::from_slice(v.value())?),
                        None => None,
                    };
                    if let Some(mut m) = old {
                        if m.last_access_millis != *ms {
                            m.last_access_millis = *ms;
                            entries.insert(key.as_str(), serde_json::to_vec(&m)?.as_slice())?;
                        }
                    }
                }
            }
            txn.commit()?;
            Ok(())
        })
    }

    /// Remove every key in one write transaction (P5): eviction and expiry
    /// batches used to issue one commit (and fsync) per victim.
    pub async fn remove_batch(&self, keys: &[String]) -> anyhow::Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let db = self.db.lock().await;
        blocking(|| {
            let txn = db.begin_write()?;
            {
                let mut entries = txn.open_table(ENTRIES)?;
                for key in keys {
                    entries.remove(key.as_str())?;
                }
            }
            txn.commit()?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn meta(key: &str, size: u64, last_access: u64) -> EntryMeta {
        EntryMeta {
            version: 1,
            upstream_id: "primary".into(),
            key: key.into(),
            size_bytes: size,
            etag: Some("e".into()),
            last_modified: None,
            content_type: None,
            created_at_millis: 0,
            last_access_millis: last_access,
            last_revalidated_millis: None,
            negative_until_millis: None,
        }
    }

    #[tokio::test]
    async fn insert_load_remove_roundtrip() {
        let dir = tempdir().unwrap();
        let store = MetaStore::open(&dir.path().join("redb.db")).unwrap();
        store.insert(&meta("a.png", 100, 1000)).await.unwrap();
        store.insert(&meta("b.png", 200, 2000)).await.unwrap();

        let all = store.load_all().await.unwrap();
        assert_eq!(all.len(), 2);

        let removed = store.remove("a.png").await.unwrap().unwrap();
        assert_eq!(removed.size_bytes, 100);
        let all = store.load_all().await.unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].key, "b.png");
    }

    #[tokio::test]
    async fn replace_overwrites_entry() {
        let dir = tempdir().unwrap();
        let store = MetaStore::open(&dir.path().join("redb.db")).unwrap();
        store.insert(&meta("a.png", 100, 1000)).await.unwrap();
        store.insert(&meta("a.png", 150, 5000)).await.unwrap(); // replace
        let all = store.load_all().await.unwrap();
        assert_eq!(all.len(), 1, "replace must not duplicate the row");
        assert_eq!(all[0].size_bytes, 150, "replaced size wins");
        assert_eq!(all[0].last_access_millis, 5000);
    }

    #[tokio::test]
    async fn bump_rewrites_entry_row() {
        let dir = tempdir().unwrap();
        let store = MetaStore::open(&dir.path().join("redb.db")).unwrap();
        store.insert(&meta("a.png", 100, 1000)).await.unwrap();
        store.bump_last_access("a.png", 9000).await.unwrap();
        let all = store.load_all().await.unwrap();
        assert_eq!(all[0].last_access_millis, 9000);
    }

    /// P5: a whole dirty batch must land in ONE commit, not one per key.
    /// The batch API is the observable half of that: N keys, one call, all
    /// rows updated.
    #[tokio::test]
    async fn batch_bump_and_remove_cover_all_keys_in_one_call() {
        let dir = tempdir().unwrap();
        let store = MetaStore::open(&dir.path().join("redb.db")).unwrap();
        let keys: Vec<String> = (0..200).map(|i| format!("k{i}.bin")).collect();
        for (i, k) in keys.iter().enumerate() {
            store.insert(&meta(k, 10, i as u64)).await.unwrap();
        }

        let batch: Vec<(String, u64)> = keys.iter().map(|k| (k.clone(), 77_000)).collect();
        store.bump_last_access_batch(&batch).await.unwrap();
        let all = store.load_all().await.unwrap();
        assert_eq!(all.len(), 200);
        assert!(all.iter().all(|m| m.last_access_millis == 77_000), "every row in the batch was bumped");

        store.remove_batch(&keys).await.unwrap();
        assert_eq!(store.load_all().await.unwrap().len(), 0, "batch remove clears every key");
    }

    #[test]
    fn quarantine_archives_never_overwrite_each_other() {
        let dir = tempdir().unwrap();
        let store = dir.path().join("redb.db");
        std::fs::write(&store, b"bad").unwrap();

        // First failure in a second uses the documented name.
        let first = quarantine_path(&store, 1000);
        assert_eq!(first.file_name().unwrap().to_string_lossy(), "redb.db.corrupt-1000");

        // A second failure inside the same second must not land on it:
        // rename would replace the archive and lose the first evidence.
        std::fs::write(&first, b"first archive").unwrap();
        let second = quarantine_path(&store, 1000);
        assert_ne!(second, first);
        assert_eq!(second.file_name().unwrap().to_string_lossy(), "redb.db.corrupt-1000-2");

        std::fs::write(&second, b"second archive").unwrap();
        let third = quarantine_path(&store, 1000);
        assert_eq!(third.file_name().unwrap().to_string_lossy(), "redb.db.corrupt-1000-3");

        // The earlier archives are intact.
        assert_eq!(std::fs::read(&first).unwrap(), b"first archive");
        assert_eq!(std::fs::read(&second).unwrap(), b"second archive");
        // A later second still gets the plain name.
        assert_eq!(
            quarantine_path(&store, 1001).file_name().unwrap().to_string_lossy(),
            "redb.db.corrupt-1001"
        );
    }

    #[tokio::test]
    async fn survives_reopen() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("redb.db");
        {
            let store = MetaStore::open(&db_path).unwrap();
            store.insert(&meta("a.png", 100, 1000)).await.unwrap();
        }
        let store = MetaStore::open(&db_path).unwrap();
        let all = store.load_all().await.unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].last_access_millis, 1000);
    }
}

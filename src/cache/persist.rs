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

pub struct MetaStore {
    db: Arc<Mutex<redb::Database>>,
}

/// redb 2 forbids a second handle on the same file within one process
/// ("Database already open"). Cache instances in tests (and a future
/// config reload) can coexist on one cache_dir, so handles are shared
/// per path via this process-wide registry.
fn shared_db(path: &Path) -> anyhow::Result<Arc<Mutex<redb::Database>>> {
    static REGISTRY: std::sync::OnceLock<std::sync::Mutex<HashMap<PathBuf, Arc<Mutex<redb::Database>>>>> =
        std::sync::OnceLock::new();
    let registry = REGISTRY.get_or_init(|| std::sync::Mutex::new(HashMap::new()));
    let mut reg = registry.lock().unwrap();
    if let Some(db) = reg.get(path) {
        return Ok(Arc::clone(db));
    }
    let db = Arc::new(Mutex::new(redb::Database::create(path)?));
    reg.insert(path.to_path_buf(), Arc::clone(&db));
    Ok(db)
}

impl MetaStore {
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let db = shared_db(path)?;
        {
            // Create tables up front so readers never race table creation.
            let db_guard = futures::executor::block_on(db.lock());
            let txn = db_guard.begin_write()?;
            {
                txn.open_table(ENTRIES)?;
            }
            txn.commit()?;
        }
        Ok(Self { db })
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
        let txn = db.begin_write()?;
        {
            let mut entries = txn.open_table(ENTRIES)?;
            entries.insert(meta.key.as_str(), serde_json::to_vec(meta)?.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Remove an entry (expiry / eviction / tombstone drop).
    pub async fn remove(&self, key: &str) -> anyhow::Result<Option<EntryMeta>> {
        let db = self.db.lock().await;
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
    }

    /// Update last_access for an existing entry (coalesced flush path).
    /// Rewrites the entry row. No-op when absent.
    pub async fn bump_last_access(&self, key: &str, new_millis: u64) -> anyhow::Result<()> {
        let db = self.db.lock().await;
        let txn = db.begin_write()?;
        {
            let mut entries = txn.open_table(ENTRIES)?;
            let old: Option<EntryMeta> = match entries.get(key)? {
                Some(v) => Some(serde_json::from_slice(v.value())?),
                None => None,
            };
            if let Some(mut m) = old {
                if m.last_access_millis != new_millis {
                    m.last_access_millis = new_millis;
                    entries.insert(key, serde_json::to_vec(&m)?.as_slice())?;
                }
            }
        }
        txn.commit()?;
        Ok(())
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

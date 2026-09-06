//! redb-backed entry metadata store (spec §3.10, ADR 0002 / R1 schema).
//!
//! Three tables in one database file:
//! - `entries`: key → serialized EntryMeta (serde_json — zero extra deps,
//!   well within budget at 100k entries)
//! - `by_last_access`: BE64(last_access_millis) || 0x00 || key → ()
//!   (lexicographic order == eligible-at order; reaper/evictor walk it)
//! - `globals`: total_bytes / entry_count counters
//!
//! Writes are atomic across all three tables in a single WriteTransaction.
//! Writes serialize through a tokio Mutex<Database> — miss-driven writes
//! are rare, so the async mutex (not block_in_place) is the right seam.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use redb::ReadableTable;
use tokio::sync::Mutex;

use crate::cache::meta::EntryMeta;

const ENTRIES: redb::TableDefinition<&str, &[u8]> = redb::TableDefinition::new("entries");
const BY_LAST_ACCESS: redb::TableDefinition<&[u8], ()> = redb::TableDefinition::new("by_last_access");
const GLOBALS: redb::TableDefinition<&str, &[u8]> = redb::TableDefinition::new("globals");

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

fn index_key(last_access_millis: u64, key: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + 1 + key.len());
    out.extend_from_slice(&last_access_millis.to_be_bytes());
    out.push(0x00);
    out.extend_from_slice(key.as_bytes());
    out
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
                txn.open_table(BY_LAST_ACCESS)?;
                txn.open_table(GLOBALS)?;
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

    /// Insert or replace an entry. Deletes the old by_last_access row when
    /// replacing (the access clock moved). Updates globals atomically.
    pub async fn insert(&self, meta: &EntryMeta) -> anyhow::Result<()> {
        let db = self.db.lock().await;
        let txn = db.begin_write()?;
        {
            let mut entries = txn.open_table(ENTRIES)?;
            let mut index = txn.open_table(BY_LAST_ACCESS)?;
            let mut globals = txn.open_table(GLOBALS)?;
            let key = meta.key.as_str();

            // Size delta + index-row swap for a possible replaced entry.
            let old_size: u64 = match entries.get(key)? {
                Some(v) => {
                    let old: EntryMeta = serde_json::from_slice(v.value())?;
                    index.remove(index_key(old.last_access_millis, key).as_slice())?;
                    old.size_bytes
                }
                None => 0,
            };
            entries.insert(key, serde_json::to_vec(meta)?.as_slice())?;
            index.insert(index_key(meta.last_access_millis, key).as_slice(), &())?;

            let total = globals.get("total_bytes")?.map(|v| u64::from_be_bytes(v.value().try_into().unwrap())).unwrap_or(0);
            let count: u64 = globals.get("entry_count")?.map(|v| u64::from_be_bytes(v.value().try_into().unwrap())).unwrap_or(0);
            let new_count = if old_size == 0 && meta.size_bytes != old_size {
                count.saturating_add(1)
            } else {
                count
            };
            globals.insert("total_bytes", (total + meta.size_bytes).saturating_sub(old_size).to_be_bytes().as_slice())?;
            globals.insert("entry_count", new_count.to_be_bytes().as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Remove an entry (expiry / eviction / tombstone drop).
    pub async fn remove(&self, key: &str) -> anyhow::Result<Option<EntryMeta>> {
        let db = self.db.lock().await;
        let txn = db.begin_write()?;
        let removed = {
            let mut entries = txn.open_table(ENTRIES)?;
            let mut index = txn.open_table(BY_LAST_ACCESS)?;
            let mut globals = txn.open_table(GLOBALS)?;
            let old: Option<EntryMeta> = match entries.remove(key)? {
                Some(v) => Some(serde_json::from_slice(v.value())?),
                None => None,
            };
            if let Some(ref m) = old {
                index.remove(index_key(m.last_access_millis, key).as_slice())?;
                let total = globals.get("total_bytes")?.map(|v| u64::from_be_bytes(v.value().try_into().unwrap())).unwrap_or(0);
                let count = globals.get("entry_count")?.map(|v| u64::from_be_bytes(v.value().try_into().unwrap())).unwrap_or(0);
                globals.insert("total_bytes", total.saturating_sub(m.size_bytes).to_be_bytes().as_slice())?;
                globals.insert("entry_count", count.saturating_sub(1).to_be_bytes().as_slice())?;
            }
            old
        };
        txn.commit()?;
        Ok(removed)
    }

    /// Update last_access for an existing entry (coalesced flush path).
    /// Rewrites the entry and its index row. No-op when absent.
    pub async fn bump_last_access(&self, key: &str, new_millis: u64) -> anyhow::Result<()> {
        let db = self.db.lock().await;
        let txn = db.begin_write()?;
        {
            let mut entries = txn.open_table(ENTRIES)?;
            let mut index = txn.open_table(BY_LAST_ACCESS)?;
            let old: Option<EntryMeta> = match entries.get(key)? {
                Some(v) => Some(serde_json::from_slice(v.value())?),
                None => None,
            };
            if let Some(mut m) = old {
                if m.last_access_millis != new_millis {
                    index.remove(index_key(m.last_access_millis, key).as_slice())?;
                    m.last_access_millis = new_millis;
                    entries.insert(key, serde_json::to_vec(&m)?.as_slice())?;
                    index.insert(index_key(new_millis, key).as_slice(), &())?;
                }
            }
        }
        txn.commit()?;
        Ok(())
    }

    /// Counters for healthz without a full scan.
    pub async fn globals(&self) -> anyhow::Result<(u64, u64)> {
        let db = self.db.lock().await;
        let txn = db.begin_read()?;
        let globals = txn.open_table(GLOBALS)?;
        let total = globals.get("total_bytes")?.map(|v| u64::from_be_bytes(v.value().try_into().unwrap())).unwrap_or(0);
        let count = globals.get("entry_count")?.map(|v| u64::from_be_bytes(v.value().try_into().unwrap())).unwrap_or(0);
        Ok((total, count))
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
        let (total, count) = store.globals().await.unwrap();
        assert_eq!(total, 300);
        assert_eq!(count, 2);

        let removed = store.remove("a.png").await.unwrap().unwrap();
        assert_eq!(removed.size_bytes, 100);
        let (total, count) = store.globals().await.unwrap();
        assert_eq!(total, 200);
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn replace_updates_index_row() {
        let dir = tempdir().unwrap();
        let store = MetaStore::open(&dir.path().join("redb.db")).unwrap();
        store.insert(&meta("a.png", 100, 1000)).await.unwrap();
        store.insert(&meta("a.png", 150, 5000)).await.unwrap(); // replace
        let (total, count) = store.globals().await.unwrap();
        assert_eq!(total, 150, "replaced size must not double-count");
        assert_eq!(count, 1, "replaced entry must not double-count");
    }

    #[tokio::test]
    async fn bump_rewrites_index_row() {
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

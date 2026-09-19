use serde::{Deserialize, Serialize};

/// Versioned metadata for a single cached entry.
/// Serialized with serde_json (see `cache::persist`); the wire format is
/// opaque to callers and versioned by `version`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntryMeta {
    pub version: u32,
    pub upstream_id: String,
    pub key: String,
    pub size_bytes: u64,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub content_type: Option<String>,
    pub created_at_millis: u64,
    pub last_access_millis: u64,
    pub last_revalidated_millis: Option<u64>,
    /// If Some, this is a negative 404 tombstone until that timestamp.
    pub negative_until_millis: Option<u64>,
    /// A promoted entry's hold, in the clock domain: until this timestamp the
    /// entry is immune to both the inactivity TTL and being chosen as an
    /// eviction victim. 0 = no hold.
    ///
    /// `serde(default)` is load-bearing: rows written before this field
    /// existed have no such key, and without a default the whole store would
    /// fail to deserialize on the first start after an upgrade.
    #[serde(default)]
    pub hold_until_millis: u64,
}

impl EntryMeta {
    pub fn is_negative(&self, now_millis: u64) -> bool {
        self.negative_until_millis.map_or(false, |until| now_millis < until)
    }

    /// When this entry becomes eligible for eviction. A held entry reports
    /// its hold deadline instead, so it sorts LAST in the eviction order --
    /// protected, but not immortal: when nothing else can bring the cache
    /// back under budget, the hold yields rather than letting the magazine
    /// run permanently over its cap.
    pub fn eligible_at(&self, inactive_ttl_secs: u64) -> u64 {
        (self.last_access_millis + inactive_ttl_secs * 1000).max(self.hold_until_millis)
    }

    /// Whether the hold is still running.
    pub fn is_held(&self, now_millis: u64) -> bool {
        self.hold_until_millis > 0 && now_millis < self.hold_until_millis
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn negative_tombstone() {
        let m = EntryMeta {
            version: 1,
            upstream_id: "primary".into(),
            key: "missing.png".into(),
            size_bytes: 0,
            etag: None,
            last_modified: None,
            content_type: None,
            created_at_millis: 0,
            last_access_millis: 0,
            last_revalidated_millis: None,
            negative_until_millis: Some(5000),
            hold_until_millis: 0,
        };
        assert!(m.is_negative(4999));
        assert!(!m.is_negative(5000));
        assert!(!m.is_negative(6000));
    }

    /// A row written before `hold_until_millis` existed must still load. The
    /// field is `#[serde(default)]` for exactly this reason: the store is
    /// deserialized at every start, and a missing key would fail the load --
    /// losing every row and rebuilding from the object tree on the first start
    /// after an upgrade. The JSON here is the pre-hold shape.
    #[test]
    fn a_row_written_before_the_hold_field_still_loads() {
        let old = r#"{
            "version": 1,
            "upstream_id": "primary",
            "key": "a.png",
            "size_bytes": 10,
            "etag": "v1",
            "last_modified": null,
            "content_type": null,
            "created_at_millis": 1000,
            "last_access_millis": 2000,
            "last_revalidated_millis": null,
            "negative_until_millis": null
        }"#;
        let m: EntryMeta = serde_json::from_str(old).expect("a pre-hold row must deserialize");
        assert_eq!(m.hold_until_millis, 0, "an absent hold means no hold");
        assert!(!m.is_held(0));
        assert!(!m.is_held(u64::MAX), "0 is never a live hold");
        assert_eq!(m.eligible_at(1200), 2000 + 1200 * 1000, "TTL rules unchanged");
    }

    #[test]
    fn eligible_at_is_last_access_plus_ttl() {
        let m = EntryMeta {
            version: 1,
            upstream_id: "primary".into(),
            key: "a.png".into(),
            size_bytes: 10,
            etag: None,
            last_modified: None,
            content_type: None,
            created_at_millis: 0,
            last_access_millis: 1000,
            last_revalidated_millis: None,
            negative_until_millis: None,
            hold_until_millis: 0,
        };
        assert_eq!(m.eligible_at(1200), 1000 + 1200 * 1000);
    }
}

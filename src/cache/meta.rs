use serde::{Deserialize, Serialize};

/// Versioned metadata for a single cached entry.
/// Serialized via serde (postcard in production via rkyv/bincode choice;
/// serde_json in tests for readability — wire format is opaque).
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
}

impl EntryMeta {
    pub fn is_negative(&self, now_millis: u64) -> bool {
        self.negative_until_millis.map_or(false, |until| now_millis < until)
    }

    pub fn eligible_at(&self, inactive_ttl_secs: u64) -> u64 {
        self.last_access_millis + inactive_ttl_secs * 1000
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
        };
        assert!(m.is_negative(4999));
        assert!(!m.is_negative(5000));
        assert!(!m.is_negative(6000));
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
        };
        assert_eq!(m.eligible_at(1200), 1000 + 1200 * 1000);
    }
}

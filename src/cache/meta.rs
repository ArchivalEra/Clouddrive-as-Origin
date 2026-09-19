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
    /// This entry was admitted KNOWING it is larger than the whole magazine
    /// budget. Such an entry cannot be brought into budget by evicting
    /// anyone — its own size is the overshoot — so the byte budget neither
    /// counts it nor evicts it: it leaves on the inactivity clock, or when
    /// the disk itself runs short.
    ///
    /// Recorded at admission rather than inferred from
    /// `size_bytes > max_size_bytes` at eviction time: an operator lowering
    /// `max_size_bytes` must not turn every existing row into an immortal
    /// one, which is exactly what inference would do.
    #[serde(default)]
    pub oversize: bool,
}

impl EntryMeta {
    pub fn is_negative(&self, now_millis: u64) -> bool {
        self.negative_until_millis.map_or(false, |until| now_millis < until)
    }

    /// When this entry becomes eligible for eviction: last access plus the
    /// inactivity TTL. Aged out by reads stopping, kept alive by reads
    /// landing — there is no other clock.
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
            oversize: false,
        };
        assert!(m.is_negative(4999));
        assert!(!m.is_negative(5000));
        assert!(!m.is_negative(6000));
    }

    /// A row written when the hold field existed must still load now that
    /// the field is gone. serde ignores unknown fields by default, so the
    /// store survives a schema that loses a member in either direction —
    /// pinned here because the whole redb store deserializes at every start.
    #[test]
    fn a_row_written_with_a_hold_field_still_loads_without_it() {
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
            "negative_until_millis": null,
            "hold_until_millis": 999999
        }"#;
        let m: EntryMeta = serde_json::from_str(old).expect("a pre-hold row must deserialize");
        assert_eq!(m.eligible_at(1200), 2000 + 1200 * 1000, "TTL rules unchanged");
        assert!(!m.oversize);
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
            oversize: false,
        };
        assert_eq!(m.eligible_at(1200), 1000 + 1200 * 1000);
    }
}

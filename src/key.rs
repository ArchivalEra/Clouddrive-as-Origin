use thiserror::Error;

use crate::routing::RouteTable;

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum KeyError {
    #[error("empty key")]
    Empty,
    #[error("absolute path not allowed")]
    Absolute,
    #[error("path traversal")]
    Traversal,
    #[error("contains NUL byte")]
    Nul,
    #[error("contains backslash")]
    Backslash,
    #[error("invalid percent encoding")]
    BadPercent,
    #[error("reserved name")]
    ReservedName,
}

/// Validate a cache key per spec §2 / ADR 0001.
/// - Non-empty, not absolute, no `..` segments, no backslash, no NUL.
/// - Percent-encoded traversal (`%2e%2e`, `%2F`, `%5C`, etc.) is rejected.
pub fn validate_key(raw: &str) -> Result<String, KeyError> {
    if raw.is_empty() {
        return Err(KeyError::Empty);
    }
    if raw.contains('\0') {
        return Err(KeyError::Nul);
    }
    if raw.contains('\\') {
        return Err(KeyError::Backslash);
    }
    if raw.starts_with('/') {
        return Err(KeyError::Absolute);
    }

    // Decode percent-encoded bytes and check the decoded form for traversal.
    let decoded = percent_decode(raw)?;
    if decoded.contains('\0') {
        return Err(KeyError::Nul);
    }
    if decoded.contains('\\') {
        return Err(KeyError::Backslash);
    }
    // Reject if decoded form is absolute.
    if decoded.starts_with('/') {
        return Err(KeyError::Absolute);
    }
    for seg in decoded.split('/') {
        if seg == ".." {
            return Err(KeyError::Traversal);
        }
    }
    // Also reject raw `..` segments (defense in depth for encoded slashes).
    for seg in raw.split('/') {
        if seg == ".." || seg == "%2e%2e" || seg == "%2E%2E" {
            return Err(KeyError::Traversal);
        }
    }
    Ok(raw.to_string())
}

/// Reject a CACHE key that would name infrastructure.
///
/// Only the cache identity is checked, never the provider-side key: the
/// cache key is what `store::file_path` joins onto `cache_dir`, while the
/// backend key is only ever sent upstream. Under a bucket alias the two
/// differ -- `/googledrive1/redb.db` has the safe cache key
/// `googledrive1/redb.db` and the backend key `redb.db` -- so checking the
/// backend key would make a legitimately-named upstream object unfetchable.
fn reject_reserved_cache_key(cache_key: &str) -> Result<(), KeyError> {
    if crate::cache::store::is_reserved_key(cache_key) {
        return Err(KeyError::ReservedName);
    }
    Ok(())
}

fn percent_decode(s: &str) -> Result<String, KeyError> {
    let mut out = Vec::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 2 >= bytes.len() {
                return Err(KeyError::BadPercent);
            }
            let hi = hex_val(bytes[i + 1]).ok_or(KeyError::BadPercent)?;
            let lo = hex_val(bytes[i + 2]).ok_or(KeyError::BadPercent)?;
            out.push((hi << 4) | lo);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).map_err(|_| KeyError::BadPercent)
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// A request path fully resolved to its serving coordinates: the cache
/// identity (full path, bucket included — state rows, flights, store
/// paths), the provider-side object path (bucket stripped), and the
/// owning upstream. Built once at the business seam; every downstream
/// caller takes this instead of a `(String, String, String)` triple that
/// compiles when misordered (the P0-a alias collision is the exhibit).
/// Both paths are validated at construction, so holders never re-validate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedKey {
    pub cache_key: String,
    pub backend_key: String,
    pub upstream_id: String,
}

/// Single upstream-resolution seam (C2): bucket alias first (first path
/// segment naming a known upstream pins it — additive to ADR-0001, legacy
/// routes untouched), else longest-prefix route table. Validation of both
/// namespaces happens here, once.
pub fn resolve_key(raw_path: &str, routes: &RouteTable, buckets: &[String]) -> Result<ResolvedKey, KeyError> {
    if let Some((bucket, rest)) = split_bucket(raw_path, buckets) {
        let cache_key = validate_key(raw_path)?;
        reject_reserved_cache_key(&cache_key)?;
        return Ok(ResolvedKey {
            cache_key,
            // Provider-side name only: it never becomes a local path, so a
            // name the cache directory reserves is fine here.
            backend_key: validate_key(rest)?,
            upstream_id: bucket.to_string(),
        });
    }
    let cache_key = validate_key(raw_path)?;
    reject_reserved_cache_key(&cache_key)?;
    let upstream_id = routes.resolve(&cache_key).to_string();
    Ok(ResolvedKey { backend_key: cache_key.clone(), cache_key, upstream_id })
}

/// S3 path-style bucket split: `/{bucket}/{rest}` where the first segment
/// names a known upstream id and `rest` is non-empty. Anything else is a
/// legacy path.
fn split_bucket<'a>(path: &'a str, ids: &[String]) -> Option<(&'a str, &'a str)> {
    let (first, rest) = path.split_once('/')?;
    if rest.is_empty() || !ids.iter().any(|id| id == first) {
        return None;
    }
    Some((first, rest))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_keys() {
        assert!(validate_key("2026/08/image.png").is_ok());
        assert!(validate_key("a/b/c").is_ok());
        assert!(validate_key("file.png").is_ok());
        assert!(validate_key("a%20b/c.png").is_ok());
    }

    #[test]
    fn rejects_traversal() {
        assert_eq!(validate_key("../etc/passwd"), Err(KeyError::Traversal));
        assert_eq!(validate_key("a/../b"), Err(KeyError::Traversal));
        assert_eq!(validate_key("a/.."), Err(KeyError::Traversal));
        assert_eq!(validate_key("%2e%2e%2fetc/passwd"), Err(KeyError::Traversal));
        assert_eq!(validate_key("a/%2E%2E/b"), Err(KeyError::Traversal));
        assert_eq!(validate_key("a%2f..%2fb"), Err(KeyError::Traversal));
    }

    #[test]
    fn rejects_absolute_and_backslash() {
        assert_eq!(validate_key("/etc/passwd"), Err(KeyError::Absolute));
        assert_eq!(validate_key("%2Fetc/passwd"), Err(KeyError::Absolute));
        assert_eq!(validate_key("a\\b"), Err(KeyError::Backslash));
        assert_eq!(validate_key("a%5Cb"), Err(KeyError::Backslash));
    }

    #[test]
    fn rejects_empty_and_nul() {
        assert_eq!(validate_key(""), Err(KeyError::Empty));
        assert_eq!(validate_key("a\0b"), Err(KeyError::Nul));
        assert_eq!(validate_key("a%00b"), Err(KeyError::Nul));
    }

    /// A bare cache key becomes a top-level name in cache_dir, where the
    /// metadata store and the `.tmp`/`.seg` artifacts live; taking one would
    /// let an install overwrite the live database or have the sweeps delete
    /// a cached object.
    ///
    /// Only the CACHE key is reserved. Under a bucket alias the provider-side
    /// key is a different string and never becomes a local path, so an
    /// upstream object named `redb.db` or `.hidden` stays fetchable through
    /// `/googledrive1/...` -- which is the only path the CDN uses.
    #[test]
    fn rejects_cache_keys_that_name_cache_directory_infrastructure() {
        let routes = test_routes();
        for raw in ["redb.db", "redb.db.corrupt-1789556382", ".tmp.a.b.1234", ".seg.x.0-1", ".hidden"] {
            assert_eq!(
                resolve_key(raw, &routes, &["primary".into()]),
                Err(KeyError::ReservedName),
                "{raw} as a cache key"
            );
        }
        // Through the bucket alias the cache key is prefixed, so the same
        // object name is an ordinary key.
        for raw in ["primary/redb.db", "primary/.hidden", "primary/redb.dbase"] {
            let rk = resolve_key(raw, &routes, &["primary".into()]).expect(raw);
            assert_eq!(rk.cache_key, raw);
        }
        // Synthetic cache keys: `validate_key` itself stays syntax-only, so
        // the seam is what holds the reservation, and a stored row's key is
        // still validatable.
        assert!(validate_key("primary/.hidden").is_ok());
    }

    fn test_routes() -> RouteTable {
        RouteTable::new(vec![
            crate::routing::RouteRule { prefix: "".into(), upstream: "primary".into() },
            crate::routing::RouteRule { prefix: "archive/".into(), upstream: "archive".into() },
        ])
    }

    fn buckets() -> Vec<String> {
        vec!["primary".into(), "archive".into()]
    }

    #[test]
    fn bucket_split_rules() {
        assert_eq!(split_bucket("archive/f.bin", &buckets()), Some(("archive", "f.bin")));
        assert_eq!(split_bucket("a.bin", &buckets()), None);
        assert_eq!(split_bucket("unknown/f.bin", &buckets()), None);
        assert_eq!(split_bucket("archive/", &buckets()), None);
    }

    #[test]
    fn resolve_prefers_bucket_alias() {
        // Prefix "archive/" routes elsewhere, but the bucket segment pins
        // the archive upstream and strips the backend path.
        let routes = RouteTable::new(vec![
            crate::routing::RouteRule { prefix: "".into(), upstream: "primary".into() },
            crate::routing::RouteRule { prefix: "archive/".into(), upstream: "deep-archive".into() },
        ]);
        let r = resolve_key("archive/f.bin", &routes, &buckets()).unwrap();
        assert_eq!(
            r,
            ResolvedKey {
                cache_key: "archive/f.bin".into(),
                backend_key: "f.bin".into(),
                upstream_id: "archive".into(),
            }
        );
    }

    #[test]
    fn resolve_legacy_prefix_routing() {
        let r = resolve_key("2026/08/a.png", &test_routes(), &buckets()).unwrap();
        assert_eq!(r.upstream_id, "primary");
        assert_eq!(r.backend_key, "2026/08/a.png");
        assert_eq!(r.cache_key, "2026/08/a.png");
    }

    #[test]
    fn resolve_rejects_traversal_in_either_namespace() {
        assert_eq!(resolve_key("archive/../x", &test_routes(), &buckets()), Err(KeyError::Traversal));
        assert_eq!(resolve_key("../x", &test_routes(), &buckets()), Err(KeyError::Traversal));
        assert_eq!(resolve_key("", &test_routes(), &buckets()), Err(KeyError::Empty));
    }
}

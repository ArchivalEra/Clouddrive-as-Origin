//! Test support: one fixture, one configurable backend.
//!
//! The tree carried the same backend three times (`ProbeBackend` in
//! `business.rs`, `CountingBackend` twice, `MockBackend` in `backend/mod.rs`)
//! and four near-identical fixture constructors — `fixture_efficient` and
//! `fixture_nocache` differed by three lines. Shared here so a change to how a
//! test reaches the lib happens once.
//!
//! **Why plain `pub` and not `#[cfg(test)]`:** integration tests in `tests/`
//! link the lib target built WITHOUT `cfg(test)`, so a `cfg(test)`-gated item
//! is invisible to them. The `#[doc(hidden)] #[cfg(test)] pub use` that
//! `backend.rs` used to carry claimed otherwise and was simply wrong — its
//! only consumers were in-lib test modules. `MockClock` is `pub` for the same
//! reason. The binary never references this module, so the linker drops it.
//!
//! Defaults are chosen to preserve each caller's behaviour: the shared mock
//! defaults to no mime hint and no last-modified, and the business fixture
//! sets both explicitly, because that is what the mocks it replaces did.
//!
//! Split by dependency, not by taste: the mock is plain `pub` so integration
//! tests can reach it, while the fixture below is `#[cfg(test)]` because
//! integration tests do not need one (they build their own state) and it
//! would drag `tempfile` — a dev-dependency — into the shipped lib.
#![doc(hidden)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::backend::{
    BackendError, ByteRange, DirectUrl, Key, ListEntry, ObjectMeta, StreamSource, StorageBackend,
};

/// The one mock backend: fixed bytes, optional failure, optional Tier-1 link,
/// optional listing, and call counters.
///
/// It replaces `ProbeBackend` (counters + recorded ranges), `CountingBackend`
/// (bytes + one shared counter + injectable failure) and the `MockBackend`
/// reference example (bytes + mime + listing). Kept as one implementation
/// because three copies drift: they already disagreed on the out-of-range
/// error (two returned `RangeNotSatisfiable`, one an opaque `Other`), and
/// nothing depended on the disagreement.
#[derive(Clone)]
pub struct MockBackend {
    id: String,
    bytes: Vec<u8>,
    /// Interior-mutable: version-flip tests change the etag mid-test.
    etag: Arc<Mutex<Option<String>>>,
    mime: Option<String>,
    last_modified: Option<String>,
    /// Every key is absent (404 path).
    missing: bool,
    /// Every call fails with this error (retry/negative-cache paths).
    fail: Option<BackendError>,
    /// The Tier-1 link this backend offers; `None` means the Tier-3 fallback.
    direct: Option<String>,
    listing: Vec<ListEntry>,
    stat_calls: Arc<AtomicUsize>,
    open_calls: Arc<AtomicUsize>,
    direct_calls: Arc<AtomicUsize>,
    list_calls: Arc<AtomicUsize>,
    /// Every open's `(offset, length)`, for gap-fetch assertions.
    opens: Arc<Mutex<Vec<(u64, Option<u64>)>>>,
}

impl MockBackend {
    /// The reference constructor: bytes, etag, mime hint. `id` is `"mock"`.
    pub fn new(bytes: &[u8], etag: Option<String>, mime: Option<String>) -> Self {
        Self {
            id: "mock".into(),
            bytes: bytes.to_vec(),
            etag: Arc::new(Mutex::new(etag)),
            mime,
            last_modified: None,
            missing: false,
            fail: None,
            direct: None,
            listing: Vec::new(),
            stat_calls: Arc::new(AtomicUsize::new(0)),
            open_calls: Arc::new(AtomicUsize::new(0)),
            direct_calls: Arc::new(AtomicUsize::new(0)),
            list_calls: Arc::new(AtomicUsize::new(0)),
            opens: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// The `CountingBackend` shape: one counter shared by stat and open, plus
    /// an injectable failure.
    pub fn counting(
        bytes: Vec<u8>,
        etag: Option<String>,
        calls: Arc<AtomicUsize>,
        fail: Option<BackendError>,
    ) -> Self {
        Self {
            stat_calls: Arc::clone(&calls),
            open_calls: Arc::clone(&calls),
            direct_calls: Arc::clone(&calls),
            ..Self::new(&bytes, etag, None)
        }
        .fail(fail)
    }

    pub fn id(mut self, id: &str) -> Self {
        self.id = id.into();
        self
    }
    pub fn mime(mut self, mime: Option<&str>) -> Self {
        self.mime = mime.map(|m| m.into());
        self
    }
    pub fn last_modified(mut self, value: Option<&str>) -> Self {
        self.last_modified = value.map(|v| v.into());
        self
    }
    pub fn missing(mut self, missing: bool) -> Self {
        self.missing = missing;
        self
    }
    pub fn fail(mut self, fail: Option<BackendError>) -> Self {
        self.fail = fail;
        self
    }
    pub fn direct(mut self, url: Option<&str>) -> Self {
        self.direct = url.map(|u| u.into());
        self
    }
    /// Seed an in-memory listing for `list()` tests.
    pub fn listing(mut self, listing: Vec<ListEntry>) -> Self {
        self.listing = listing;
        self
    }
    pub fn with_listing(self, listing: Vec<ListEntry>) -> Self {
        self.listing(listing)
    }
    /// Share the etag handle so a test can flip the version mid-test.
    pub fn etag_handle(mut self, etag: Arc<Mutex<Option<String>>>) -> Self {
        self.etag = etag;
        self
    }
    /// Share all three counters with the caller (the fixtures expose them).
    pub fn counters(
        mut self,
        stat_calls: Arc<AtomicUsize>,
        open_calls: Arc<AtomicUsize>,
        direct_calls: Arc<AtomicUsize>,
        opens: Arc<Mutex<Vec<(u64, Option<u64>)>>>,
    ) -> Self {
        self.stat_calls = stat_calls;
        self.open_calls = open_calls;
        self.direct_calls = direct_calls;
        self.opens = opens;
        self
    }

    pub fn stat_calls(&self) -> usize {
        self.stat_calls.load(Ordering::SeqCst)
    }
    pub fn open_calls(&self) -> usize {
        self.open_calls.load(Ordering::SeqCst)
    }
    pub fn direct_calls(&self) -> usize {
        self.direct_calls.load(Ordering::SeqCst)
    }
    pub fn list_calls(&self) -> usize {
        self.list_calls.load(Ordering::SeqCst)
    }
    pub fn opens(&self) -> Vec<(u64, Option<u64>)> {
        self.opens.lock().unwrap().clone()
    }

    fn slice(&self, range: Option<ByteRange>) -> Result<Vec<u8>, BackendError> {
        let Some(r) = range else { return Ok(self.bytes.clone()) };
        let start = r.offset as usize;
        if start > self.bytes.len() {
            return Err(BackendError::RangeNotSatisfiable);
        }
        Ok(match r.length {
            None => self.bytes[start..].to_vec(),
            Some(len) => {
                let end = (start + len as usize).min(self.bytes.len());
                self.bytes[start..end].to_vec()
            }
        })
    }
}

#[async_trait::async_trait]
impl StorageBackend for MockBackend {
    async fn stat(&self, _key: &Key) -> Result<ObjectMeta, BackendError> {
        self.stat_calls.fetch_add(1, Ordering::SeqCst);
        if let Some(e) = &self.fail {
            return Err(e.clone());
        }
        if self.missing {
            return Err(BackendError::NotFound);
        }
        Ok(ObjectMeta {
            size_bytes: self.bytes.len() as u64,
            etag: self.etag.lock().unwrap().clone(),
            last_modified: self.last_modified.clone(),
            mime_hint: self.mime.clone(),
        })
    }

    async fn open(&self, _key: &Key, range: Option<ByteRange>) -> Result<StreamSource, BackendError> {
        self.open_calls.fetch_add(1, Ordering::SeqCst);
        if let Some(e) = &self.fail {
            return Err(e.clone());
        }
        if self.missing {
            return Err(BackendError::NotFound);
        }
        self.opens
            .lock()
            .unwrap()
            .push(range.map(|r| (r.offset, r.length)).unwrap_or((0, None)));
        Ok(StreamSource {
            stream: Box::new(std::io::Cursor::new(self.slice(range)?)),
            total_len: Some(self.bytes.len() as u64),
        })
    }

    async fn refresh_if_needed(&self) -> Result<(), BackendError> {
        Ok(())
    }

    async fn direct_url(&self, _key: &Key, _viewer_ua: Option<&str>) -> Result<DirectUrl, BackendError> {
        self.direct_calls.fetch_add(1, Ordering::SeqCst);
        self.direct
            .clone()
            .map(|url| DirectUrl { url })
            .ok_or_else(|| BackendError::Other("no link".into()))
    }

    async fn list(&self, folder: &str, recursive: bool) -> Result<Vec<ListEntry>, BackendError> {
        self.list_calls.fetch_add(1, Ordering::SeqCst);
        Ok(self
            .listing
            .iter()
            .filter_map(|e| {
                let rest = e.key.strip_prefix(folder)?;
                // The folder itself is the PROPFIND self entity, never a child.
                if rest.is_empty() {
                    return None;
                }
                (recursive || !rest.trim_end_matches('/').contains('/')).then(|| e.clone())
            })
            .collect())
    }

    fn id(&self) -> &str {
        &self.id
    }
}

/// Everything below needs a temp cache dir and a live `AppState`, so it is
/// `#[cfg(test)]`: visible to this crate's test modules, invisible to the
/// binary, and free to use dev-dependencies.
#[cfg(test)]
pub use fixture_support::*;

#[cfg(test)]
mod fixture_support {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use axum::http::{HeaderMap, StatusCode};

    use super::MockBackend;
    use crate::backend::{BackendError, BackendRegistry, BackendSlot, ListEntry, StorageBackend};
    use crate::business::AppState;
    use crate::cache::cache::Cache;
    use crate::clock::MockClock;
    use crate::config::{CacheProfile, ColdMiss, Config};

/// A live `AppState` over a temp cache dir, plus the counters the tests read.
pub struct Fixture {
    _dir: tempfile::TempDir,
    pub state: AppState<MockClock>,
    pub stat_calls: Arc<AtomicUsize>,
    pub open_calls: Arc<AtomicUsize>,
    pub direct_calls: Arc<AtomicUsize>,
    pub etag: Arc<Mutex<Option<String>>>,
    pub opens: Arc<Mutex<Vec<(u64, Option<u64>)>>>,
    /// The backend behind "primary", for tests that assert on the mock itself.
    pub primary: Arc<MockBackend>,
}

/// One parameterized fixture replacing `fixture` / `fixture_full` /
/// `fixture_efficient` / `fixture_nocache`: those four differed only in
/// profile settings and a handful of mock knobs.
pub struct FixtureBuilder {
    bytes: Vec<u8>,
    etag: Option<String>,
    extra: Vec<(String, Vec<u8>)>,
    missing: bool,
    direct: Option<String>,
    redirect: bool,
    profile: Option<String>,
    coverage: Option<(f64, u64)>,
    mime: Option<String>,
    last_modified: Option<String>,
}

impl FixtureBuilder {
    pub fn new(bytes: &[u8]) -> Self {
        Self {
            bytes: bytes.to_vec(),
            etag: None,
            extra: Vec::new(),
            missing: false,
            direct: None,
            redirect: false,
            profile: None,
            coverage: None,
            mime: None,
            last_modified: None,
        }
    }

    pub fn etag(mut self, etag: Option<&str>) -> Self {
        self.etag = etag.map(|e| e.into());
        self
    }
    /// More upstreams, for the bucket-alias tests.
    pub fn extra(mut self, extra: Vec<(&str, Vec<u8>)>) -> Self {
        self.extra = extra.into_iter().map(|(id, b)| (id.to_string(), b)).collect();
        self
    }
    pub fn missing(mut self, missing: bool) -> Self {
        self.missing = missing;
        self
    }
    /// The Tier-1 link the backend offers (None = Tier-3 fallback).
    pub fn direct(mut self, direct: Option<&str>) -> Self {
        self.direct = direct.map(|d| d.into());
        self
    }
    /// Flip primary to `cold_miss = "redirect"`.
    pub fn redirect(mut self) -> Self {
        self.redirect = true;
        self
    }
    /// `"efficient"` or `"nocache"`.
    pub fn profile(mut self, profile: &str) -> Self {
        self.profile = Some(profile.into());
        self
    }
    /// Efficient-profile knobs; implies `profile("efficient")`.
    pub fn coverage(mut self, threshold: f64, min_file_size: u64) -> Self {
        self.profile = Some("efficient".into());
        self.coverage = Some((threshold, min_file_size));
        self
    }
    pub fn mime(mut self, mime: Option<&str>) -> Self {
        self.mime = mime.map(|m| m.into());
        self
    }
    pub fn last_modified(mut self, value: Option<&str>) -> Self {
        self.last_modified = value.map(|v| v.into());
        self
    }

    pub fn build(self) -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = Config { cache_dir: dir.path().to_path_buf(), ..Config::default() };
        if self.redirect {
            cfg.upstreams[0].cold_miss = ColdMiss::Redirect;
        }
        if let Some(profile) = &self.profile {
            cfg.upstreams[0].cache_profile = profile.clone();
        }
        if let Some((threshold, min_file_size)) = self.coverage {
            cfg.cache_profiles.insert(
                "efficient".into(),
                CacheProfile { coverage_threshold: threshold, min_file_size, coverage_window_secs: 3600 },
            );
        }

        let stat_calls = Arc::new(AtomicUsize::new(0));
        let open_calls = Arc::new(AtomicUsize::new(0));
        let direct_calls = Arc::new(AtomicUsize::new(0));
        let etag = Arc::new(Mutex::new(self.etag.clone()));
        let opens = Arc::new(Mutex::new(Vec::new()));

        let mk = |id: &str, bytes: Vec<u8>| -> Arc<MockBackend> {
            Arc::new(
                MockBackend::new(&bytes, None, None)
                    .id(id)
                    .etag_handle(Arc::clone(&etag))
                    .mime(self.mime.as_deref())
                    .last_modified(self.last_modified.as_deref())
                    .missing(self.missing)
                    .direct(self.direct.as_deref())
                    .counters(
                        Arc::clone(&stat_calls),
                        Arc::clone(&open_calls),
                        Arc::clone(&direct_calls),
                        Arc::clone(&opens),
                    ),
            )
        };

        let primary = mk("primary", self.bytes);
        let mut slots = HashMap::new();
        slots.insert(
            "primary".to_string(),
            Arc::new(BackendSlot::new(Arc::clone(&primary) as Arc<dyn StorageBackend>, 3)),
        );
        for (id, bytes) in self.extra {
            slots.insert(
                id.clone(),
                Arc::new(BackendSlot::new(mk(&id, bytes) as Arc<dyn StorageBackend>, 3)),
            );
        }
        let cache = Arc::new(Cache::new(
            Arc::new(cfg.clone()),
            Arc::new(MockClock::new(0)),
            BackendRegistry::new(slots),
        ));
        Fixture {
            _dir: dir,
            state: AppState { cache, config: Arc::new(cfg), sigv4_config: None },
            stat_calls,
            open_calls,
            direct_calls,
            etag,
            opens,
            primary,
        }
    }
}

/// Header map from pairs, for handler calls.
pub fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
    use axum::http::HeaderName;
    let mut h = HeaderMap::new();
    for (k, v) in pairs {
        h.insert(k.parse::<HeaderName>().unwrap(), v.parse().unwrap());
    }
    h
}

/// Test OriginalUri: a plain absolute path (no percent encoding) so the
/// sigv4 gate decodes it unchanged.
pub static DEFAULT_TEST_URI: std::sync::LazyLock<axum::http::Uri> =
    std::sync::LazyLock::new(|| "/a.bin".parse().unwrap());

/// Split a response into `(status, headers, body)`; body lossily as text.
pub async fn body_text(resp: axum::response::Response) -> (StatusCode, HeaderMap, String) {
    let (mut parts, body) = resp.into_parts();
    let bytes = axum::body::to_bytes(body, 64 * 1024 * 1024).await.unwrap();
    let headers = std::mem::take(&mut parts.headers);
    (parts.status, headers, String::from_utf8_lossy(&bytes).into_owned())
}

/// Wait until `key` has an entry row, or panic. Reads `cache.state` because no
/// public interface exposes "is this installed" — a real gap, recorded here
/// rather than papered over with an accessor invented for tests.
pub async fn wait_installed(fx: &Fixture, key: &str) {
    for _ in 0..200 {
        if fx.state.cache.state.read().await.entries.contains_key(key) {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    panic!("entry {key} never installed");
}

/// Drain a body to nothing (test helper).
pub async fn drain(body: &mut crate::cache::flight::BodyStream) {
    crate::cache::flight::drain(body).await.unwrap();
}

/// Zero the stat/open counters between phases of a test. `direct_calls` is
/// deliberately left alone: the callers that use this assert on direct-link
/// decisions made earlier in the same test.
pub fn reset(fx: &Fixture) {
    fx.stat_calls.store(0, Ordering::SeqCst);
    fx.open_calls.store(0, Ordering::SeqCst);
}

/// Segment sidecars currently staged for `key`. Goes through the store rather
/// than re-deriving the filename shape.
pub fn staged_segments(fx: &Fixture, key: &str) -> Vec<(u64, u64)> {
    crate::cache::store::segments_for_key(&fx.state.config.cache_dir, key)
        .into_iter()
        .map(|(start, end, _)| (start, end))
        .collect()
}

/// Count of files in the cache dir that are not the metadata store.
pub fn stray_cache_files(fx: &Fixture) -> usize {
    std::fs::read_dir(fx.state.config.cache_dir.clone())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy() != crate::cache::store::META_STORE_FILE)
        .count()
}

/// A rejected key must never reach the provider.
pub fn assert_no_backend_calls(fx: &Fixture, what: &str) {
    for (name, n) in [
        ("stat", fx.stat_calls.load(Ordering::SeqCst)),
        ("open", fx.open_calls.load(Ordering::SeqCst)),
        ("direct", fx.direct_calls.load(Ordering::SeqCst)),
    ] {
        assert_eq!(n, 0, "{what}: {name} must not be called for a rejected key");
    }
}

/// `Body` is re-exported so test modules need not import axum themselves.
pub use axum::body::Body as TestBody;
}

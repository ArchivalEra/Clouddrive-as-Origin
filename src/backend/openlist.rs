//! OpenList WebDAV StorageBackend (multi-OpenList adaptation).
//!
//! OpenList (AList community fork) fronts hundreds of cloud drives behind
//! one folder tree and exposes them as WebDAV. Running one or more
//! OpenList instances on loopback turns our StorageBackend into a thin
//! WebDAV client: `PROPFIND Depth:0` = stat, `GET` with `Range` = open.
//! The OpenList WebDAV policy must be "native proxy" so byte streams flow
//! through the instance with Range support (302 mode is unsupported here).
//!
//! Wheel reuse: `reqwest_dav` owns the PROPFIND/XML machinery for stat;
//! the ranged GET uses our own reqwest client because the water-pipe
//! requires a streaming body (reqwest_dav's GET does not stream).
//! Direct links (A relief valve) come from OpenList's `/api/fs/link`
//! endpoint (Tier 1 per R2): issue, normalize, 1-byte-probe. Header-pinned
//! links (Tier 2) are a later ticket — they fail the probe today and fall
//! back to proxying, with zero hardcoded driver names (D2 decision).

use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use tokio_util::io::StreamReader;

use crate::{
    backend::{BackendError, ByteRange, DirectUrl, Key, ListEntry, ObjectMeta, StreamSource, StorageBackend},
    config::UpstreamConfig,
};

/// Guards for recursive listings: a pathological upstream tree must not
/// wedge the proxy. Bounds chosen well above any real media library.
const MAX_LIST_ENTRIES: usize = 50_000;
const MAX_LIST_DEPTH: usize = 64;

/// A WebDAV resource path within the OpenList mount. Kept as a type so
/// backends can never receive raw unvalidated input.
fn dav_path(root: &Option<String>, key: &Key) -> String {
    let mut p = String::from("/");
    if let Some(r) = root {
        let r = r.trim_matches('/');
        if !r.is_empty() {
            p.push_str(r);
            p.push('/');
        }
    }
    p.push_str(key.as_str());
    p
}

/// DAV path of a listing folder: like `dav_path` but for a directory —
/// `folder` is empty (mount root) or ends with `/`.
fn dav_folder(root: &Option<String>, folder: &str) -> String {
    let mut p = String::from("/");
    if let Some(r) = root {
        let r = r.trim_matches('/');
        if !r.is_empty() {
            p.push_str(r);
            p.push('/');
        }
    }
    p.push_str(folder);
    p
}

/// Path component of the WebDAV base URL ("/dav" for
/// "http://127.0.0.1:5244/dav"); empty when the base has no path.
/// PROPFIND hrefs are server-path-absolute, so this prefixes every href.
fn base_url_path(base_url: &str) -> String {
    let rest = base_url.split_once("://").map(|(_, r)| r).unwrap_or(base_url);
    match rest.split_once('/') {
        Some((_, p)) if !p.is_empty() => format!("/{p}"),
        _ => String::new(),
    }
}

/// Decode a PROPFIND href to a raw server path (percent-decoded, always
/// starting with `/`). Full URLs are reduced to their path component.
fn href_to_path(href: &str) -> String {
    let path = match href.split_once("://") {
        Some((_, rest)) => match rest.split_once('/') {
            Some((_, p)) => format!("/{p}"),
            None => "/".to_string(),
        },
        None => href.to_string(),
    };
    percent_encoding::percent_decode_str(&path).decode_utf8_lossy().into_owned()
}

/// Child name of a decoded href relative to the requested folder path:
/// `""` for the folder itself, `None` when the href is outside it.
/// Trailing slashes are normalized away; the caller re-adds them for
/// directories.
fn strip_base(base: &str, href_path: &str) -> Option<String> {
    let h = href_path.trim_end_matches('/');
    let b = base.trim_end_matches('/');
    let rest = h.strip_prefix(b)?;
    if rest.is_empty() {
        return Some(String::new());
    }
    rest.strip_prefix('/').map(|s| s.to_string())
}

pub struct OpenListBackend {
    id: String,
    /// WebDAV base URL, e.g. "http://127.0.0.1:5244/dav" (no trailing slash).
    base_url: String,
    root_path: Option<String>,
    username: String,
    password: String,
    /// Static admin token for `/api/fs/link` (Tier 1 direct links). Read
    /// from `link_api_token_env` when the upstream opts into
    /// `cold_miss = "redirect"`; `None` otherwise (link never attempted).
    api_token: Option<String>,
    /// Shared TLS-configured client: used directly for ranged streaming
    /// GETs and injected into reqwest_dav for PROPFIND (set_agent), so
    /// both paths share identical TLS policy.
    http: reqwest::Client,
}

/// `/api/fs/link` response envelope. `data.header` (server-side replay
/// headers, possibly cookie-bearing) is deliberately ignored: we never
/// forward it to viewers (Tier 2 resolution is a later ticket).
#[derive(Debug, serde::Deserialize)]
struct LinkResponse {
    code: i64,
    #[allow(dead_code)]
    message: String,
    data: Option<LinkData>,
}

#[derive(Debug, serde::Deserialize)]
struct LinkData {
    url: String,
}

/// Origin (scheme + authority) of a URL: the API root OpenList serves
/// REST + WebDAV on, and the self-host comparator for Tier 3 detection.
fn url_origin(url: &str) -> Option<String> {
    let (scheme, rest) = url.split_once("://")?;
    let auth = rest.split('/').next().unwrap_or("");
    if auth.is_empty() {
        return None;
    }
    Some(format!("{scheme}://{auth}"))
}

/// Host of a URL's authority, lowercased, brackets stripped.
fn url_host(url: &str) -> Option<String> {
    let rest = url.split_once("://")?.1;
    let auth = rest.split('/').next().unwrap_or("");
    if auth.is_empty() {
        return None;
    }
    Some(auth.to_ascii_lowercase())
}

impl OpenListBackend {
    pub fn from_config(cfg: &UpstreamConfig) -> Result<Self, String> {
        if cfg.backend_type != "openlist" {
            return Err(format!("upstream {}: unknown type {} (v1 supports \"openlist\")", cfg.id, cfg.backend_type));
        }
        let username = std::env::var(&cfg.username_env)
            .map_err(|_| format!("upstream {}: env {} not set", cfg.id, cfg.username_env))?;
        let password = std::env::var(&cfg.password_env)
            .map_err(|_| format!("upstream {}: env {} not set", cfg.id, cfg.password_env))?;
        let base_url = cfg.base_url.trim_end_matches('/').to_string();
        let mut builder = reqwest::Client::builder()
            // OpenList may be remote; generous timeout for large media.
            // read_timeout is per-read inactivity (resets on every chunk):
            // a flowing cold pull never trips it, while a genuinely stalled
            // stream errors after 30 s instead of hanging the driver — and
            // every reader attached to the flight — forever.
            .connect_timeout(Duration::from_secs(5))
            .read_timeout(Duration::from_secs(30));
        if cfg.accept_invalid_certs {
            builder = builder.danger_accept_invalid_certs(true);
        }
        let http = builder.build().map_err(|e| format!("upstream {}: http client: {e}", cfg.id))?;
        let api_token = match &cfg.link_api_token_env {
            Some(env) => Some(std::env::var(env).map_err(|_| {
                format!("upstream {}: env {} not set (cold_miss redirect needs it)", cfg.id, env)
            })?),
            None => None,
        };
        Ok(Self {
            id: cfg.id.clone(),
            base_url,
            root_path: cfg.root_path.clone(),
            username,
            password,
            api_token,
            http,
        })
    }

    fn full_url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    fn dav_client(&self) -> Result<reqwest_dav::Client, BackendError> {
        reqwest_dav::ClientBuilder::new()
            .set_agent(self.http.clone())
            .set_host(self.base_url.clone())
            .set_auth(reqwest_dav::types::Auth::Basic(
                self.username.clone(),
                self.password.clone(),
            ))
            .build()
            .map_err(|e| BackendError::Other(format!("dav client: {e}")))
    }

    fn map_status(status: u16, body: String) -> BackendError {
        match status {
            404 => BackendError::NotFound,
            429 => BackendError::RateLimited { retry_after_millis: None },
            401 | 403 => BackendError::AuthRequired,
            500..=599 => BackendError::ServerError(format!("{status}: {body}")),
            _ => BackendError::Other(format!("{status}: {body}")),
        }
    }

    /// Defense in depth for listing folders: the business layer already
    /// validates, but a backend must never PROPFIND an unchecked path.
    fn validate_folder(folder: &str) -> Result<(), BackendError> {
        if folder.contains('\0') || folder.contains('\\') || folder.starts_with('/') {
            return Err(BackendError::Other("invalid folder".into()));
        }
        if folder.split('/').any(|s| s == "..") {
            return Err(BackendError::Other("invalid folder".into()));
        }
        Ok(())
    }
}

#[async_trait]
impl StorageBackend for OpenListBackend {
    /// `PROPFIND Depth:0` via reqwest_dav. OpenList reports getetag (often
    /// a content hash per driver), getcontentlength, getlastmodified.
    async fn stat(&self, key: &Key) -> Result<ObjectMeta, BackendError> {
        let dav = self.dav_client()?;
        let path = dav_path(&self.root_path, key);
        let item = dav.list(&path, reqwest_dav::types::Depth::Number(0)).await;
        let items = match item {
            Ok(items) => items,
            Err(e) => return Err(map_dav_error(e)),
        };
        // Depth:0 returns exactly the resource itself; a folder means the
        // caller asked for a directory — we only serve files.
        let file = match items.first() {
            Some(reqwest_dav::types::list_cmd::ListEntity::File(f)) => f,
            Some(_) => return Err(BackendError::NotFound),
            None => return Err(BackendError::NotFound),
        };
        Ok(ObjectMeta {
            size_bytes: file.content_length.max(0) as u64,
            etag: file.tag.clone(),
            // HTTP-date string is what our revalidation compare needs.
            last_modified: Some(file.last_modified.to_rfc2822()),
            mime_hint: if file.content_type.is_empty() { None } else { Some(file.content_type.clone()) },
        })
    }

    /// `GET` with Range through our own client — full streaming body, and
    /// reqwest strips Authorization automatically if OpenList ever 302s.
    async fn open(&self, key: &Key, range: Option<ByteRange>) -> Result<StreamSource, BackendError> {
        let path = dav_path(&self.root_path, key);
        let mut req = self
            .http
            .get(self.full_url(&path))
            .basic_auth(self.username.clone(), Some(self.password.clone()));
        if let Some(r) = &range {
            req = req.header("range", r.http_header_value());
        }
        let resp = req.send().await.map_err(|e| BackendError::ServerError(format!("dav get: {e}")))?;
        let status = resp.status().as_u16();
        if !(status == 200 || status == 206) {
            let body = resp.text().await.unwrap_or_default();
            return Err(Self::map_status(status, body));
        }
        // Full object length from Content-Range total (206) or Content-Length.
        let total_len = resp
            .headers()
            .get("content-range")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.rsplit('/').next())
            .and_then(|t| t.parse::<u64>().ok())
            .or_else(|| {
                resp.headers()
                    .get("content-length")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<u64>().ok())
            });
        let reader = StreamReader::new(
            resp.bytes_stream().map(|r| r.map_err(|e| std::io::Error::other(e.to_string()))),
        );
        Ok(StreamSource { stream: Box::new(reader), total_len })
    }

    /// Tier 1 direct link (A relief valve): issue via `/api/fs/link`,
    /// normalize, accept only foreign https (or loopback-http) targets,
    /// then 1-byte-probe for Range fidelity. Everything else — no token,
    /// link error, empty/self-referential/non-https URL, probe non-206 —
    /// is Tier 3 (`Err` = proxy instead). No driver names anywhere: the
    /// probe result alone decides (D2 decision).
    async fn direct_url(&self, key: &Key, viewer_ua: Option<&str>) -> Result<DirectUrl, BackendError> {
        let no_link = |why: &str| BackendError::Other(format!("openlist: no direct link ({why})"));
        let token = self
            .api_token
            .clone()
            .filter(|t| !t.is_empty())
            .ok_or_else(|| no_link("no link token"))?;
        let api = url_origin(&self.base_url).ok_or_else(|| no_link("bad base_url"))?;
        let mut req = self
            .http
            .post(format!("{api}/api/fs/link"))
            .header("authorization", token)
            .json(&serde_json::json!({ "path": dav_path(&self.root_path, key) }))
            .timeout(Duration::from_secs(5));
        if let Some(ua) = viewer_ua {
            req = req.header("user-agent", ua.to_string());
        }
        let resp = req.send().await.map_err(|e| no_link(&format!("link api unreachable: {e}")))?;
        let link: LinkResponse =
            resp.json().await.map_err(|e| no_link(&format!("link decode: {e}")))?;
        if link.code != 200 {
            return Err(no_link(&format!("link api code {}", link.code)));
        }
        let mut url = link.data.map(|d| d.url).unwrap_or_default();
        if url.is_empty() {
            return Err(no_link("empty link url"));
        }
        if let Some(rest) = url.strip_prefix("//") {
            url = format!("https://{rest}");
        }
        // Tier 3: self-referential (proxy-backed /p or /d on our own host)
        // or anything the redirect policy forbids.
        let host = url_host(&url).ok_or_else(|| no_link("relative link url"))?;
        let own = url_host(&api).unwrap_or_default();
        if host == own {
            return Err(no_link("self-referential link url"));
        }
        if !crate::backend::redirect_target_allowed(&url) {
            return Err(no_link("link target fails redirect policy"));
        }
        // Probe mimics the viewer: 1-byte Range, 206-only acceptance.
        // Redirects are followed; the 307 target is the post-follow final
        // URL (saves the viewer a hop).
        let mut probe =
            self.http.get(&url).header("range", "bytes=0-0").timeout(Duration::from_secs(5));
        if let Some(ua) = viewer_ua {
            probe = probe.header("user-agent", ua.to_string());
        }
        let presp = probe.send().await.map_err(|e| no_link(&format!("probe unreachable: {e}")))?;
        if presp.status().as_u16() != 206 {
            return Err(no_link(&format!("probe status {}", presp.status().as_u16())));
        }
        Ok(DirectUrl { url: presp.url().to_string() })
    }

    async fn refresh_if_needed(&self) -> Result<(), BackendError> {        // OpenList owns provider credential rotation; WebDAV basic auth has
        // no token lifecycle on our side. Health probe = cheap PROPFIND.
        let dav = self.dav_client()?;
        match dav.list("/", reqwest_dav::types::Depth::Number(0)).await {
            Ok(_) => Ok(()),
            Err(e) => Err(map_dav_error(e)),
        }
    }

    /// PROPFIND `Depth:1` per directory. Recursive mode walks the subtree
    /// with manual BFS (one Depth:1 request per directory): `Depth:Infinity`
    /// is optional per RFC 4918 and OpenList drivers commonly reject it,
    /// so we never depend on it. Directories are emitted only in
    /// non-recursive mode (they become CommonPrefixes there); recursive
    /// mode emits files only, matching AWS `Contents`.
    async fn list(&self, folder: &str, recursive: bool) -> Result<Vec<ListEntry>, BackendError> {
        Self::validate_folder(folder)?;
        let dav = self.dav_client()?;
        let href_base = base_url_path(&self.base_url);
        let mut out = Vec::new();
        // BFS work item: DAV path of a directory + the key prefix its
        // children accumulate (always `""` or ends with `/`).
        let mut queue = vec![(dav_folder(&self.root_path, folder), folder.to_string(), 0usize)];
        while let Some((dav_dir, key_prefix, depth)) = queue.pop() {
            let items = dav
                .list(&dav_dir, reqwest_dav::types::Depth::Number(1))
                .await
                .map_err(map_dav_error)?;
            let base = format!("{href_base}{dav_dir}");
            for item in items {
                match item {
                    reqwest_dav::types::list_cmd::ListEntity::File(f) => {
                        let name = strip_base(&base, &href_to_path(&f.href)).unwrap_or_default();
                        if name.is_empty() || name.ends_with('/') {
                            continue;
                        }
                        out.push(ListEntry {
                            key: format!("{key_prefix}{name}"),
                            size: f.content_length.max(0) as u64,
                            etag: f.tag.clone(),
                            last_modified: Some(f.last_modified.to_rfc2822()),
                            is_dir: false,
                        });
                    }
                    reqwest_dav::types::list_cmd::ListEntity::Folder(d) => {
                        let name = strip_base(&base, &href_to_path(&d.href)).unwrap_or_default();
                        let name = name.trim_end_matches('/');
                        if name.is_empty() {
                            continue; // the requested folder itself
                        }
                        if recursive {
                            if depth + 1 >= MAX_LIST_DEPTH {
                                continue;
                            }
                            queue.push((
                                format!("{dav_dir}{name}/"),
                                format!("{key_prefix}{name}/"),
                                depth + 1,
                            ));
                        } else {
                            out.push(ListEntry {
                                key: format!("{key_prefix}{name}/"),
                                size: 0,
                                etag: d.tag.clone(),
                                last_modified: Some(d.last_modified.to_rfc2822()),
                                is_dir: true,
                            });
                        }
                    }
                }
            }
            // Truncate rather than fail the whole request (P9). A bucket
            // past the walk budget used to surface as a 500 InternalError,
            // making the listing path unusable at scale; S3 semantics for
            // an over-large result set are a truncated page, not an error.
            if out.len() > MAX_LIST_ENTRIES {
                tracing::warn!(
                    cap = MAX_LIST_ENTRIES,
                    "listing truncated at the walk budget"
                );
                out.truncate(MAX_LIST_ENTRIES);
                break;
            }
        }
        Ok(out)
    }

    fn id(&self) -> &str {
        &self.id
    }
}

/// Map reqwest_dav's error surface onto our backend taxonomy. PROPFIND on
/// a missing resource surfaces as a StatusMismatched decode error carrying
/// the actual response code (list() expects 207).
fn map_dav_error(e: reqwest_dav::types::Error) -> BackendError {
    match e {
        reqwest_dav::types::Error::Decode(reqwest_dav::types::DecodeError::StatusMismatched(s)) => {
            OpenListBackend::map_status(s.response_code, String::new())
        }
        reqwest_dav::types::Error::Decode(reqwest_dav::types::DecodeError::Server(err)) => {
            BackendError::ServerError(format!("{err:?}"))
        }
        reqwest_dav::types::Error::Reqwest(re) => {
            if re.is_timeout() || re.is_connect() {
                BackendError::ServerError(format!("dav unreachable: {re}"))
            } else {
                BackendError::Other(format!("dav request: {re}"))
            }
        }
        other => BackendError::Other(format!("dav: {other}")),
    }
}

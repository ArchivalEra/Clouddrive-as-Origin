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

use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use tokio_util::io::StreamReader;

use crate::{
    backend::{BackendError, ByteRange, Key, ObjectMeta, StreamSource, StorageBackend},
    config::UpstreamConfig,
};

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

pub struct OpenListBackend {
    id: String,
    /// WebDAV base URL, e.g. "http://127.0.0.1:5244/dav" (no trailing slash).
    base_url: String,
    root_path: Option<String>,
    username: String,
    password: String,
    /// Shared TLS-configured client: used directly for ranged streaming
    /// GETs and injected into reqwest_dav for PROPFIND (set_agent), so
    /// both paths share identical TLS policy.
    http: reqwest::Client,
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
            .connect_timeout(Duration::from_secs(5));
        if cfg.accept_invalid_certs {
            builder = builder.danger_accept_invalid_certs(true);
        }
        let http = builder.build().map_err(|e| format!("upstream {}: http client: {e}", cfg.id))?;
        Ok(Self {
            id: cfg.id.clone(),
            base_url,
            root_path: cfg.root_path.clone(),
            username,
            password,
            http,
        })
    }

    fn full_url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
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
}

#[async_trait]
impl StorageBackend for OpenListBackend {
    /// `PROPFIND Depth:0` via reqwest_dav. OpenList reports getetag (often
    /// a content hash per driver), getcontentlength, getlastmodified.
    async fn stat(&self, key: &Key) -> Result<ObjectMeta, BackendError> {
        let dav = reqwest_dav::ClientBuilder::new()
            .set_agent(self.http.clone())
            .set_host(self.base_url.clone())
            .set_auth(reqwest_dav::types::Auth::Basic(
                self.username.clone(),
                self.password.clone(),
            ))
            .build()
            .map_err(|e| BackendError::Other(format!("dav client: {e}")))?;
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

    async fn refresh_if_needed(&self) -> Result<(), BackendError> {
        // OpenList owns provider credential rotation; WebDAV basic auth has
        // no token lifecycle on our side. Health probe = cheap PROPFIND.
        let dav = reqwest_dav::ClientBuilder::new()
            .set_agent(self.http.clone())
            .set_host(self.base_url.clone())
            .set_auth(reqwest_dav::types::Auth::Basic(
                self.username.clone(),
                self.password.clone(),
            ))
            .build()
            .map_err(|e| BackendError::Other(format!("dav client: {e}")))?;
        match dav.list("/", reqwest_dav::types::Depth::Number(0)).await {
            Ok(_) => Ok(()),
            Err(e) => Err(map_dav_error(e)),
        }
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

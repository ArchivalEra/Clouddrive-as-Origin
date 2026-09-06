use anyhow::Context;
use serde::Deserialize;
use std::{net::SocketAddr, path::PathBuf};

use crate::routing::{RouteRule, RouteTable};

#[derive(Debug, Deserialize, Clone)]
pub struct UpstreamConfig {
    pub id: String,
    /// Upstream kind. v1 ships "openlist" (WebDAV native-proxy against an
    /// OpenList instance — hundreds of cloud drives behind one folder tree).
    #[serde(rename = "type", default = "default_backend_type")]
    pub backend_type: String,
    /// WebDAV base URL of the OpenList instance, e.g. "http://127.0.0.1:5244/dav".
    /// Loopback-only in the reference deployment (OpenList runs beside us).
    pub base_url: String,
    /// Optional subfolder inside the WebDAV mount this upstream serves,
    /// e.g. "music" for /dav/music/<key>. Empty = mount root.
    #[serde(default)]
    pub root_path: Option<String>,
    /// OpenList web-UI username — env reference (spec §2).
    pub username_env: String,
    /// OpenList web-UI password — env reference (spec §2).
    pub password_env: String,
}

fn default_backend_type() -> String {
    "openlist".into()
}

#[derive(Debug, Deserialize, Clone)]
pub struct RawConfig {
    #[serde(default = "default_front_listen")]
    pub front_listen: SocketAddr,
    #[serde(default = "default_listen_addr")]
    pub listen_addr: SocketAddr,
    #[serde(default)]
    pub tls_cert_env: Option<String>,
    #[serde(default)]
    pub tls_key_env: Option<String>,
    #[serde(default = "default_cache_dir")]
    pub cache_dir: PathBuf,

    #[serde(default = "default_max_size")]
    pub max_size_bytes: u64,
    #[serde(default = "default_inactive_ttl")]
    pub inactive_ttl_secs: u64,
    #[serde(default = "default_revalidate_ttl")]
    pub revalidate_ttl_secs: u64,
    #[serde(default = "default_negative_ttl")]
    pub negative_ttl_secs: u64,
    #[serde(default = "default_concurrency")]
    #[serde(alias = "graph_concurrency_per_upstream")]
    pub concurrency_per_upstream: usize,
    #[serde(default = "default_retry_max")]
    pub retry_max_attempts: u32,
    #[serde(default = "default_retry_base")]
    pub retry_base_ms: u64,
    #[serde(default = "default_retry_max_ms")]
    pub retry_max_ms: u64,
    #[serde(default)]
    pub prewarm_shared_secret_env: Option<String>,
    #[serde(default = "default_allowed_suffixes")]
    pub allowed_download_suffixes: Vec<String>,

    #[serde(default)]
    pub upstreams: Vec<UpstreamConfig>,
    #[serde(default)]
    pub routes: Vec<RouteRule>,
}

fn default_front_listen() -> SocketAddr { "127.0.0.1:8443".parse().unwrap() }
fn default_listen_addr() -> SocketAddr { "127.0.0.1:8080".parse().unwrap() }
fn default_cache_dir() -> PathBuf { "/var/lib/origin-cache".into() }
fn default_max_size() -> u64 { 107_374_182_400 }
fn default_inactive_ttl() -> u64 { 1200 }
fn default_revalidate_ttl() -> u64 { 60 }
fn default_negative_ttl() -> u64 { 60 }
fn default_concurrency() -> usize { 3 }
fn default_retry_max() -> u32 { 4 }
fn default_retry_base() -> u64 { 200 }
fn default_retry_max_ms() -> u64 { 30_000 }
fn default_allowed_suffixes() -> Vec<String> {
    vec![
        ".files.1drv.com".into(),
        ".sharepoint.com".into(),
        "storage.live.com".into(),
    ]
}

/// Validated, runtime config.
#[derive(Debug, Clone)]
pub struct Config {
    pub front_listen: SocketAddr,
    pub listen_addr: SocketAddr,
    pub tls_cert_env: Option<String>,
    pub tls_key_env: Option<String>,
    pub cache_dir: PathBuf,
    pub max_size_bytes: u64,
    pub inactive_ttl_secs: u64,
    pub revalidate_ttl_secs: u64,
    pub negative_ttl_secs: u64,
    pub concurrency_per_upstream: usize,
    pub retry_max_attempts: u32,
    pub retry_base_ms: u64,
    pub retry_max_ms: u64,
    pub prewarm_shared_secret_env: Option<String>,
    pub allowed_download_suffixes: Vec<String>,
    pub upstreams: Vec<UpstreamConfig>,
    pub routes: RouteTable,
}

impl Config {
    pub fn from_toml_str(s: &str) -> anyhow::Result<Self> {
        let raw: RawConfig = toml::from_str(s).context("parse TOML config")?;
        Self::from_raw(raw)
    }

    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        let raw = std::fs::read_to_string(path).with_context(|| format!("read config {path}"))?;
        Self::from_toml_str(&raw)
    }

    pub fn from_file_or_default(path: Option<&str>) -> anyhow::Result<Self> {
        match path {
            Some(p) => Self::from_file(p),
            None => Ok(Self::default()),
        }
    }

    fn from_raw(raw: RawConfig) -> anyhow::Result<Self> {
        if raw.upstreams.is_empty() {
            anyhow::bail!("at least one [[upstreams]] required");
        }
        if raw.routes.is_empty() {
            anyhow::bail!("at least one [[routes]] required");
        }
        // Validate routes reference known upstreams.
        let ids: std::collections::HashSet<&str> = raw.upstreams.iter().map(|u| u.id.as_str()).collect();
        for r in &raw.routes {
            if !ids.contains(r.upstream.as_str()) {
                anyhow::bail!("route prefix {:?} references unknown upstream {:?}", r.prefix, r.upstream);
            }
        }
        // Ensure default route exists.
        if !raw.routes.iter().any(|r| r.prefix.is_empty()) {
            anyhow::bail!("at least one [[routes]] with empty prefix (default) required");
        }
        Ok(Self {
            front_listen: raw.front_listen,
            listen_addr: raw.listen_addr,
            tls_cert_env: raw.tls_cert_env,
            tls_key_env: raw.tls_key_env,
            cache_dir: raw.cache_dir,
            max_size_bytes: raw.max_size_bytes,
            inactive_ttl_secs: raw.inactive_ttl_secs,
            revalidate_ttl_secs: raw.revalidate_ttl_secs,
            negative_ttl_secs: raw.negative_ttl_secs,
            concurrency_per_upstream: raw.concurrency_per_upstream,
            retry_max_attempts: raw.retry_max_attempts,
            retry_base_ms: raw.retry_base_ms,
            retry_max_ms: raw.retry_max_ms,
            prewarm_shared_secret_env: raw.prewarm_shared_secret_env,
            allowed_download_suffixes: raw.allowed_download_suffixes,
            upstreams: raw.upstreams,
            routes: RouteTable::new(raw.routes),
        })
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            front_listen: default_front_listen(),
            listen_addr: default_listen_addr(),
            tls_cert_env: None,
            tls_key_env: None,
            cache_dir: default_cache_dir(),
            max_size_bytes: default_max_size(),
            inactive_ttl_secs: default_inactive_ttl(),
            revalidate_ttl_secs: default_revalidate_ttl(),
            negative_ttl_secs: default_negative_ttl(),
            concurrency_per_upstream: default_concurrency(),
            retry_max_attempts: default_retry_max(),
            retry_base_ms: default_retry_base(),
            retry_max_ms: default_retry_max_ms(),
            prewarm_shared_secret_env: None,
            allowed_download_suffixes: default_allowed_suffixes(),
            upstreams: vec![UpstreamConfig {
                id: "primary".into(),
                backend_type: "openlist".into(),
                base_url: "http://127.0.0.1:5244/dav".into(),
                root_path: None,
                username_env: "OPENLIST_USERNAME".into(),
                password_env: "OPENLIST_PASSWORD".into(),
            }],
            routes: RouteTable::new(vec![RouteRule { prefix: "".into(), upstream: "primary".into() }]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_example() {
        let raw = std::fs::read_to_string("config.example.toml").unwrap();
        let cfg = Config::from_toml_str(&raw).unwrap();
        assert_eq!(cfg.routes.resolve("2026/08/a.png"), "media");
    }

    #[test]
    fn rejects_unknown_upstream() {
        let toml = r#"
            [[upstreams]]
            id = "a"
            drive_root_path = "/drive/root:/a"
            client_id_env = "A_ID"
            client_secret_env = "A_SECRET"
            refresh_token_env = "A_TOKEN"
            [[routes]]
            prefix = ""
            upstream = "missing"
        "#;
        assert!(Config::from_toml_str(toml).is_err());
    }

    #[test]
    fn requires_default_route() {
        let toml = r#"
            [[upstreams]]
            id = "a"
            drive_root_path = "/drive/root:/a"
            client_id_env = "A_ID"
            client_secret_env = "A_SECRET"
            refresh_token_env = "A_TOKEN"
            [[routes]]
            prefix = "a/"
            upstream = "a"
        "#;
        assert!(Config::from_toml_str(toml).is_err());
    }
}

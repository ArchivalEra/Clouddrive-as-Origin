use anyhow::Context;
use serde::Deserialize;
use std::{net::SocketAddr, path::PathBuf};

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    /// Pingora front plane listen addr (TLS termination, H2, reverse-proxy).
    /// Skeleton defaults to 127.0.0.1:8443 so it can run without root.
    #[serde(default = "default_front_listen")]
    pub front_listen: SocketAddr,

    /// Business plane (axum) listen addr — loopback only.
    #[serde(default = "default_listen_addr")]
    pub listen_addr: SocketAddr,

    /// TLS cert/key paths — env indirection, never literal in file.
    /// Skeleton ignores them (plain HTTP on front_listen); real binary
    /// will resolve `*_env` via the process environment.
    #[serde(default)]
    pub tls_cert_env: Option<String>,
    #[serde(default)]
    pub tls_key_env: Option<String>,

    #[serde(default = "default_cache_dir")]
    pub cache_dir: PathBuf,
}

fn default_front_listen() -> SocketAddr {
    "127.0.0.1:8443".parse().unwrap()
}
fn default_listen_addr() -> SocketAddr {
    "127.0.0.1:8080".parse().unwrap()
}
fn default_cache_dir() -> PathBuf {
    "/var/lib/origin-cache".into()
}

impl Config {
    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("read config {path}"))?;
        toml::from_str(&raw).context("parse TOML config")
    }

    pub fn from_file_or_default(path: Option<&str>) -> anyhow::Result<Self> {
        match path {
            Some(p) => Self::from_file(p),
            None => Ok(Self::default()),
        }
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
        }
    }
}

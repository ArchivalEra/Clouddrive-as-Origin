use anyhow::Context;
use serde::Deserialize;
use std::{collections::HashMap, net::SocketAddr, path::PathBuf};

use crate::routing::{RouteRule, RouteTable};

#[derive(Debug, Deserialize, Clone)]
pub struct UpstreamConfig {
    pub id: String,
    /// Upstream kind. v1 ships "openlist" (WebDAV native-proxy against an
    /// OpenList instance — hundreds of cloud drives behind one folder tree).
    #[serde(rename = "type", default = "default_backend_type")]
    pub backend_type: String,
    /// WebDAV base URL of the OpenList instance. Loopback deployments may
    /// use plain http (e.g. "http://127.0.0.1:5244/dav"); any non-loopback
    /// host MUST be https (enforced in validation — credentials travel on
    /// this connection).
    pub base_url: String,
    /// Optional subfolder inside the WebDAV mount this upstream serves,
    /// e.g. "music" for /dav/music/<key>. Empty = mount root.
    #[serde(default)]
    pub root_path: Option<String>,
    /// OpenList web-UI username — env reference (spec §2).
    pub username_env: String,
    /// OpenList web-UI password — env reference (spec §2).
    pub password_env: String,
    /// Accept self-signed/invalid TLS certificates on the upstream
    /// connection. Dev/self-hosted escape hatch only — never enable for
    /// third-party hosts. Default false.
    #[serde(default)]
    pub accept_invalid_certs: bool,
    /// Cold-miss strategy. `proxy` (default) water-pipes bytes through us;
    /// `redirect` 307s the viewer to an upstream-issued direct link (A
    /// relief valve) with a background fill, silently falling back to
    /// proxy whenever no link is available. v1 supports redirect on
    /// openlist upstreams only, and requires `link_api_token_env`.
    #[serde(default)]
    pub cold_miss: ColdMiss,
    /// Fill-policy profile name (P2 efficientcache). `"standard"` (default)
    /// = legacy behavior: every miss water-pipes a full file into cache.
    /// Any other name must have a `[cache_profiles.<name>]` table, which
    /// switches this upstream to ranged-passthrough + segment staging +
    /// coverage-triggered promotion.
    #[serde(default = "default_cache_profile")]
    pub cache_profile: String,
    /// OpenList static admin token (Settings → Other → Token) for the
    /// `/api/fs/link` direct-link endpoint — env reference. Only read
    /// when `cold_miss = "redirect"`. Never expires, unlike 48h JWTs.
    #[serde(default)]
    pub link_api_token_env: Option<String>,
}

/// Cold-miss strategy per upstream (A relief valve is opt-in per
/// upstream; default proxy = zero behavior change).
#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum ColdMiss {
    #[default]
    Proxy,
    Redirect,
}

fn default_cache_profile() -> String {
    "standard".into()
}

fn default_coverage_threshold() -> f64 {
    0.8
}

fn default_min_file_size() -> u64 {
    64 * 1024 * 1024
}

/// Fill-policy profile (P2 efficientcache): when ranged misses stage
/// segments instead of full-filing, and what staged coverage promotes a
/// key to a full cache entry. `threshold` ∈ (0, 1]; 1.0 = promote only
/// once every byte has been served.
#[derive(Debug, Deserialize, Clone)]
pub struct RawCacheProfile {
    #[serde(default = "default_coverage_threshold")]
    pub coverage_threshold: f64,
    #[serde(default = "default_min_file_size")]
    pub min_file_size: u64,
}

/// Validated fill-policy profile.
#[derive(Debug, Clone, Copy)]
pub struct CacheProfile {
    pub coverage_threshold: f64,
    pub min_file_size: u64,
}

/// Resolved per-upstream fill behavior: `efficient == false` is legacy
/// standard (full-file water-pipe on every miss).
#[derive(Debug, Clone, Copy)]
pub struct EffectiveProfile {
    pub efficient: bool,
    pub coverage_threshold: f64,
    pub min_file_size: u64,
}

impl EffectiveProfile {
    pub fn standard() -> Self {
        Self { efficient: false, coverage_threshold: default_coverage_threshold(), min_file_size: default_min_file_size() }
    }
}

fn default_backend_type() -> String {
    "openlist".into()
}

/// Loopback/localhost hosts are allowed to speak plain http to us (the
/// reference deployment runs OpenList beside the cache); everything else
/// must be https because WebDAV credentials ride on it.
fn is_loopback_host(host: &str) -> bool {
    let h = host.trim_matches(['[', ']']).to_ascii_lowercase();
    h == "localhost" || h.starts_with("localhost.") || h.starts_with("127.") || h == "::1"
}

fn upstream_url_policy(base_url: &str, upstream_id: &str) -> anyhow::Result<()> {
    let (scheme, rest) = base_url
        .split_once("://")
        .ok_or_else(|| anyhow::anyhow!("upstream {upstream_id}: base_url must be an absolute http(s) URL"))?;
    let authority = rest.split('/').next().unwrap_or("");
    // Strip an optional port, honoring bracketed IPv6 literals.
    let host = if let Some(stripped) = authority.strip_prefix('[') {
        stripped.split(']').next().unwrap_or("")
    } else {
        authority.split(':').next().unwrap_or("")
    };
    match scheme {
        "https" => Ok(()),
        "http" if is_loopback_host(host) => Ok(()),
        "http" => Err(anyhow::anyhow!(
            "upstream {upstream_id}: base_url is http on a non-loopback host ({host}) — credentials would travel in cleartext; use https"
        )),
        _ => Err(anyhow::anyhow!(
            "upstream {upstream_id}: base_url scheme must be http (loopback only) or https"
        )),
    }
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
    /// Named fill-policy profiles (`[cache_profiles.<name>]`). Upstreams
    /// opt in via `cache_profile = "<name>"`; `"standard"` is built-in.
    #[serde(default)]
    pub cache_profiles: HashMap<String, RawCacheProfile>,
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
    pub cache_profiles: HashMap<String, CacheProfile>,
}

impl Config {
    pub fn from_toml_str(s: &str) -> anyhow::Result<Self> {        let raw: RawConfig = toml::from_str(s).context("parse TOML config")?;
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

    /// Look up a validated upstream by id (for per-upstream behavior
    /// switches such as `cold_miss`).
    pub fn upstream(&self, id: &str) -> Option<&UpstreamConfig> {
        self.upstreams.iter().find(|u| u.id == id)
    }

    /// Resolve an upstream's fill behavior. Unknown upstreams and
    /// `"standard"` both yield the legacy profile (defensive: boot
    /// validation already rejects dangling references).
    pub fn cache_profile(&self, upstream_id: &str) -> EffectiveProfile {
        let name = self.upstream(upstream_id).map(|u| u.cache_profile.as_str()).unwrap_or("standard");
        if name == "standard" {
            return EffectiveProfile::standard();
        }
        match self.cache_profiles.get(name) {
            Some(p) => EffectiveProfile {
                efficient: true,
                coverage_threshold: p.coverage_threshold,
                min_file_size: p.min_file_size,
            },
            None => EffectiveProfile::standard(),
        }
    }

    fn from_raw(raw: RawConfig) -> anyhow::Result<Self> {
        if raw.upstreams.is_empty() {
            anyhow::bail!("at least one [[upstreams]] required");
        }
        if raw.routes.is_empty() {
            anyhow::bail!("at least one [[routes]] required");
        }
        // Upstream URL policy: https everywhere except loopback http.
        for u in &raw.upstreams {
            upstream_url_policy(&u.base_url, &u.id)?;
            // A relief valve needs a link source: v1 implements it for
            // openlist only, authenticated by static token. Fail fast at
            // boot instead of silently never redirecting.
            if u.cold_miss == ColdMiss::Redirect {
                if u.backend_type != "openlist" {
                    anyhow::bail!(
                        "upstream {}: cold_miss = \"redirect\" is only supported for openlist upstreams (v1)",
                        u.id
                    );
                }
                if u.link_api_token_env.is_none() {
                    anyhow::bail!(
                        "upstream {}: cold_miss = \"redirect\" requires link_api_token_env",
                        u.id
                    );
                }
            }
        }
        // Fill-policy profiles: threshold sanity + every non-standard
        // reference resolves. Fail fast at boot, not on first miss.
        let mut profiles = HashMap::new();
        for (name, raw) in &raw.cache_profiles {
            if !(raw.coverage_threshold > 0.0 && raw.coverage_threshold <= 1.0) {
                anyhow::bail!(
                    "cache_profiles.{name}: coverage_threshold must be in (0, 1], got {}",
                    raw.coverage_threshold
                );
            }
            profiles.insert(
                name.clone(),
                CacheProfile {
                    coverage_threshold: raw.coverage_threshold,
                    min_file_size: raw.min_file_size,
                },
            );
        }
        for u in &raw.upstreams {
            if u.cache_profile != "standard" && !profiles.contains_key(&u.cache_profile) {
                anyhow::bail!(
                    "upstream {}: cache_profile {:?} has no [cache_profiles.<name>] table",
                    u.id,
                    u.cache_profile
                );
            }
        }
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
            cache_profiles: profiles,
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
                accept_invalid_certs: false,
                cold_miss: ColdMiss::Proxy,
                link_api_token_env: None,
                cache_profile: default_cache_profile(),
            }],
            routes: RouteTable::new(vec![RouteRule { prefix: "".into(), upstream: "primary".into() }]),
            cache_profiles: HashMap::new(),
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
            type = "openlist"
            base_url = "http://127.0.0.1:5244/dav"
            username_env = "A_USER"
            password_env = "A_PASS"
            [[routes]]
            prefix = ""
            upstream = "missing"
        "#;
        assert!(Config::from_toml_str(toml).is_err());
    }

    fn upstream_toml(base_url: &str) -> String {
        format!(
            r#"
            [[upstreams]]
            id = "a"
            type = "openlist"
            base_url = "{base_url}"
            username_env = "A_USER"
            password_env = "A_PASS"
            [[routes]]
            prefix = ""
            upstream = "a"
        "#
        )
    }

    #[test]
    fn loopback_http_allowed() {
        assert!(Config::from_toml_str(&upstream_toml("http://127.0.0.1:5244/dav")).is_ok());
        assert!(Config::from_toml_str(&upstream_toml("http://localhost:5244/dav")).is_ok());
        assert!(Config::from_toml_str(&upstream_toml("http://[::1]:5244/dav")).is_ok());
    }

    #[test]
    fn remote_http_rejected_https_required() {
        assert!(Config::from_toml_str(&upstream_toml("http://media.example.com/dav")).is_err());
        assert!(Config::from_toml_str(&upstream_toml("https://media.example.com/dav")).is_ok());
    }

    #[test]
    fn non_absolute_base_url_rejected() {
        assert!(Config::from_toml_str(&upstream_toml("127.0.0.1:5244/dav")).is_err());
        assert!(Config::from_toml_str(&upstream_toml("ftp://127.0.0.1/dav")).is_err());
    }

    fn redirect_toml(extra: &str) -> String {
        format!(
            r#"
            [[upstreams]]
            id = "a"
            type = "openlist"
            base_url = "http://127.0.0.1:5244/dav"
            username_env = "A_USER"
            password_env = "A_PASS"
            cold_miss = "redirect"
            {extra}
            [[routes]]
            prefix = ""
            upstream = "a"
        "#
        )
    }

    #[test]
    fn redirect_requires_link_token() {
        assert!(Config::from_toml_str(&redirect_toml("")).is_err());
        assert!(Config::from_toml_str(&redirect_toml("link_api_token_env = \"A_TOKEN\"")).is_ok());
    }

    #[test]
    fn proxy_is_default_no_token_needed() {
        assert!(Config::from_toml_str(&upstream_toml("http://127.0.0.1:5244/dav")).is_ok());
    }

    fn profile_toml(profile_section: &str, profile_ref: &str) -> String {
        format!(
            r#"
            [[upstreams]]
            id = "a"
            type = "openlist"
            base_url = "http://127.0.0.1:5244/dav"
            username_env = "A_USER"
            password_env = "A_PASS"
            {profile_ref}
            {profile_section}
            [[routes]]
            prefix = ""
            upstream = "a"
        "#
        )
    }

    #[test]
    fn efficient_profile_validates() {
        let ok = profile_toml("[cache_profiles.efficient]\ncoverage_threshold = 0.8", "cache_profile = \"efficient\"");
        let cfg = Config::from_toml_str(&ok).unwrap();
        let p = cfg.cache_profile("a");
        assert!(p.efficient);
        assert_eq!(p.coverage_threshold, 0.8);
        // Standard is the default everywhere.
        assert!(!Config::from_toml_str(&upstream_toml("http://127.0.0.1:5244/dav")).unwrap().cache_profile("a").efficient);
    }

    #[test]
    fn profile_threshold_and_reference_validated() {
        let bad_threshold = profile_toml(
            "[cache_profiles.efficient]\ncoverage_threshold = 1.5",
            "cache_profile = \"efficient\"",
        );
        assert!(Config::from_toml_str(&bad_threshold).is_err());
        let zero_threshold = profile_toml(
            "[cache_profiles.efficient]\ncoverage_threshold = 0.0",
            "cache_profile = \"efficient\"",
        );
        assert!(Config::from_toml_str(&zero_threshold).is_err());
        let dangling = profile_toml("", "cache_profile = \"ghost\"");
        assert!(Config::from_toml_str(&dangling).is_err());
    }

    #[test]
    fn requires_default_route() {
        let toml = r#"
            [[upstreams]]
            id = "a"
            type = "openlist"
            base_url = "http://127.0.0.1:5244/dav"
            username_env = "A_USER"
            password_env = "A_PASS"
            [[routes]]
            prefix = "a/"
            upstream = "a"
        "#;
        assert!(Config::from_toml_str(toml).is_err());
    }
}

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
    /// = full-file water-pipe into cache, and the default. `"efficient"`
    /// (built in) or a `[cache_profiles.<name>]` table stages the served
    /// windows instead, so a ranged read costs one upstream open per window
    /// rather than one per request (ADR-0016/0019); `"nocache"` writes
    /// nothing at all.
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
    // Efficient by default: a ranged read is the shape every viewer produces,
    // and staging its window is what makes one upstream open serve many
    // requests (ADR-0016/0019). `"standard"` remains available and unchanged
    // for an operator who wants full-file fills.
    "efficient".into()
}

fn default_min_file_size() -> u64 {
    64 * 1024 * 1024
}

/// How long a staged interval keeps counting for the ledger's policy. An
/// interval whose last read is older than this decays out of the ledger, so
/// stale partial reads stop competing with fresh ones for the eviction order
/// (the disk sidecars themselves stay for the age sweep). 0 disables decay.
fn default_coverage_window_secs() -> u64 {
    3600
}

/// Fill-policy profile: what an upstream with a staged-read profile stages.
/// `min_file_size` is the object size below which ranged requests take the
/// ordinary path (a small object is worth a durable entry; a large one is
/// worth windows). `coverage_window_secs` is how long a staged interval
/// keeps counting for the ledger's eviction policy before it decays.
#[derive(Debug, Deserialize, Clone)]
pub struct RawCacheProfile {
    #[serde(default = "default_min_file_size")]
    pub min_file_size: u64,
    #[serde(default = "default_coverage_window_secs")]
    pub coverage_window_secs: u64,
}

/// Validated fill-policy profile.
#[derive(Debug, Clone, Copy)]
pub struct CacheProfile {
    pub min_file_size: u64,
    pub coverage_window_secs: u64,
}

/// Resolved per-upstream fill behavior. `standard` = full-file water-pipe
/// into cache; `efficient` = ranged reads served from staged windows, one
/// upstream open per window (ADR-0016/0019); `nocache` = pure water-pipe,
/// zero disk writes
/// (small-footprint nodes: bytes stream through, metadata stat still
/// happens so ETag/Size/Last-Modified headers render, nothing persists —
/// no entries, no segments, no redb writes, no negative tombstones).
#[derive(Debug, Clone, Copy)]
pub struct EffectiveProfile {
    pub efficient: bool,
    pub nocache: bool,
    pub min_file_size: u64,
    pub coverage_window_secs: u64,
}

impl EffectiveProfile {
    pub fn standard() -> Self {
        Self { efficient: false, nocache: false, min_file_size: default_min_file_size(), coverage_window_secs: default_coverage_window_secs() }
    }

    pub fn nocache() -> Self {
        Self { efficient: false, nocache: true, min_file_size: 0, coverage_window_secs: 0 }
    }

    /// The built-in `efficient` profile: stage every ranged read (no
    /// `min_file_size` floor), with the default ledger window. A
    /// `[cache_profiles.efficient]` table still overrides both knobs.
    pub fn efficient() -> Self {
        Self {
            efficient: true,
            nocache: false,
            min_file_size: 0,
            coverage_window_secs: default_coverage_window_secs(),
        }
    }
}

/// Which span a trim takes first (both are SPAN-level; rows are visited
/// least-recently-touched first, ADR-0015). `lru` takes the stalest span —
/// a plain sliding window. `heat` takes the span with the fewest reads
/// inside the trailing window, so a workload that scrubs back keeps a
/// re-read segment alive even when time has passed it by.
#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum EvictionPolicy {
    #[default]
    Lru,
    Heat,
}

/// One staged-read run fetches this much (ADR-0016).
///
/// Measured: an upstream `open` costs a fixed ~640 ms whatever the range
/// length, and the node pulls ~63 MB/s from the provider, so 64 MiB is about
/// a second of transfer. EdgeOne's sharded origin-pull asks for ascending
/// 1 MiB shards, so a shard's offset is normally behind the run's watermark by
/// the time it is requested (measured wait ~ 0): the window has to exceed the
/// reader's appetite, not the object. Larger = fewer opens and more disk
/// churn per window.
/// How long a key stays protected from policy eviction after the last body
/// reading it ends (ADR-0017). A pause between two requests of the same
/// viewing session must not cost a re-fetch, and the budget's own guard
/// (`STAGE_MIN_AGE_MS`, 60 s) is too short to cover a viewer who is thinking.
fn default_read_grace_secs() -> u64 {
    300
}

fn default_session_window_bytes() -> u64 {
    64 * 1024 * 1024
}

/// How long a key stays watched after its last body ends (ADR-0018). A
/// viewing session lasts hours while its bodies last milliseconds (EdgeOne
/// asks for ascending 1 MiB shards), so the protection a viewer needs has to
/// outlive the requests: a pause inside this budget keeps the key's
/// neighbourhood pinned and the read-ahead in place, and the resume costs no
/// upstream open. It is also a deadline rather than an exemption — past it the
/// key is an ordinary eviction candidate again. Default 900 (15 minutes:
/// longer than a phone call, shorter than the 20-minute idle TTL); 0 disables
/// watching, leaving only live bodies protected.
fn default_watch_idle_secs() -> u64 {
    900
}

/// Bytes pinned around a watched viewer's position (ADR-0018): half behind
/// (what a small scrub back needs) and half ahead (the window the chain
/// fetches before the player asks for it). Default 128 MiB, two default
/// windows. Larger = a smoother scrub back over a bigger object, paid for by
/// a budget the magazine cannot spend.
fn default_watch_pin_bytes() -> u64 {
    128 * 1024 * 1024
}

fn default_backend_type() -> String {
    "openlist".into()
}

/// Loopback/localhost hosts are allowed to speak plain http to us (the
/// reference deployment runs OpenList beside the cache); everything else
/// must be https because WebDAV credentials ride on it. The predicate lives
/// in [`crate::net`] because the redirect policy needs the same one, and two
/// copies is how one of them stays wrong.
use crate::net::is_loopback_host;

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
    /// Prometheus metrics listener for the front plane (e.g.
    /// "127.0.0.1:9090"). Absent = metrics endpoint disabled. Per-process
    /// ports must differ when several instances share a node.
    #[serde(default)]
    pub front_metrics_listen: Option<String>,
    /// Client CIDRs refused at connection time (before the TLS
    /// handshake). A bare IP means a single host.
    #[serde(default)]
    pub front_ip_block: Vec<String>,
    /// Client CIDRs exempt from per-IP rate limiting (ops path is never throttled).
    #[serde(default)]
    pub front_ip_allow: Vec<String>,
    /// Per-client-IP requests/sec ceiling on the front. Absent =
    /// disabled (threshold set after the real-traffic baseline).
    #[serde(default)]
    pub front_rate_rps: Option<u32>,
    /// Proxy service worker threads. Absent = 2 (P6: pingora's default of
    /// 1 serializes TLS/H2 on a single core).
    #[serde(default)]
    pub front_threads: Option<usize>,
    #[serde(default = "default_cache_dir")]
    pub cache_dir: PathBuf,

    #[serde(default = "default_max_size")]
    pub max_size_bytes: u64,
    /// Entry-count ceiling: the second eviction budget (P10). Entry rows
    /// cost ~500 B of RAM each, so a byte-only cap lets many small objects
    /// exhaust memory. 0 disables the count cap (bytes only).
    #[serde(default = "default_max_entries")]
    pub max_entries: usize,
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
    /// Magazine eviction policy for staged spans. Default `lru`.
    #[serde(default)]
    pub eviction_policy: EvictionPolicy,
    /// How much one staged-read run fetches (ADR-0016). Default 64 MiB.
    #[serde(default = "default_session_window_bytes")]
    pub session_window_bytes: u64,
    /// Grace after the last read before a key becomes evictable again
    /// (ADR-0017). Default 300; 0 disables (live bodies are still protected).
    #[serde(default = "default_read_grace_secs")]
    pub read_grace_secs: u64,
    /// How long a key stays watched after its last body ends (ADR-0018).
    /// Default 900; 0 disables watching, leaving a key protected only while a
    /// body streams it (ADR-0017's rule).
    #[serde(default = "default_watch_idle_secs")]
    pub watch_idle_secs: u64,
    /// Bytes pinned around a watching viewer's position (ADR-0018). Default
    /// 128 MiB; 0 disables pinning, which restores ADR-0017's coarser rule —
    /// a key being read keeps all of its spans.
    #[serde(default = "default_watch_pin_bytes")]
    pub watch_pin_bytes: u64,

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
fn default_max_entries() -> usize {
    500_000
}

fn default_max_size() -> u64 { 107_374_182_400 }
fn default_inactive_ttl() -> u64 { 1200 }
fn default_revalidate_ttl() -> u64 { 60 }
fn default_negative_ttl() -> u64 { 60 }
fn default_concurrency() -> usize { 3 }
fn default_retry_max() -> u32 { 4 }
fn default_retry_base() -> u64 { 200 }
fn default_retry_max_ms() -> u64 { 30_000 }
/// Validated, runtime config.
#[derive(Debug, Clone)]
pub struct Config {
    pub front_listen: SocketAddr,
    pub listen_addr: SocketAddr,
    pub tls_cert_env: Option<String>,
    pub tls_key_env: Option<String>,
    pub front_metrics_listen: Option<String>,
    pub front_ip_block: Vec<String>,
    pub front_ip_allow: Vec<String>,
    pub front_rate_rps: Option<u32>,
    pub front_threads: Option<usize>,
    pub cache_dir: PathBuf,
    pub max_size_bytes: u64,
    pub max_entries: usize,
    pub inactive_ttl_secs: u64,
    pub revalidate_ttl_secs: u64,
    pub negative_ttl_secs: u64,
    /// How staged spans are ejected when the budget overshoots (ADR-0015).
    pub eviction_policy: EvictionPolicy,
    /// Bytes one staged-read run fetches (ADR-0016): one upstream `open` per
    /// window, shared by every reader inside it.
    pub session_window_bytes: u64,
    /// Seconds a key stays un-evictable after the last body reading it ended.
    pub read_grace_secs: u64,
    /// Seconds a key stays WATCHED after its last body ended (ADR-0018): its
    /// neighbourhood stays pinned and its read-ahead survives a pause. 0
    /// disables watching.
    pub watch_idle_secs: u64,
    /// Bytes pinned around a watched viewer's position (ADR-0018).
    pub watch_pin_bytes: u64,
    pub concurrency_per_upstream: usize,
    pub retry_max_attempts: u32,
    pub retry_base_ms: u64,
    pub retry_max_ms: u64,
    pub prewarm_shared_secret_env: Option<String>,
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
    /// `"standard"` both yield the legacy profile; `"nocache"` is a
    /// built-in no-disk profile (pure water-pipe). Any other name must
    /// have a `[cache_profiles.<name>]` table (defensive: boot
    /// validation already rejects dangling references).
    pub fn cache_profile(&self, upstream_id: &str) -> EffectiveProfile {
        let name = self.upstream(upstream_id).map(|u| u.cache_profile.as_str()).unwrap_or("standard");
        match name {
            "standard" => EffectiveProfile::standard(),
            "nocache" => EffectiveProfile::nocache(),
            "efficient" => match self.cache_profiles.get("efficient") {
                Some(p) => EffectiveProfile {
                    efficient: true,
                    nocache: false,
                    min_file_size: p.min_file_size,
                    coverage_window_secs: p.coverage_window_secs,
                },
                None => EffectiveProfile::efficient(),
            },
            other => match self.cache_profiles.get(other) {
                Some(p) => EffectiveProfile {
                    efficient: true,
                    nocache: false,
                    min_file_size: p.min_file_size,
                    coverage_window_secs: p.coverage_window_secs,
                },
                None => EffectiveProfile::standard(),
            },
        }
    }

    fn from_raw(raw: RawConfig) -> anyhow::Result<Self> {
        if raw.upstreams.is_empty() {
            anyhow::bail!("at least one [[upstreams]] required");
        }
        if raw.routes.is_empty() {
            anyhow::bail!("at least one [[routes]] required");
        }

        // Zero/duplicate values that silently break serving (ticket #58).
        // Each of these was reached either by measurement or by reading the
        // consumer; every one of them produced a service that looked up but
        // did nothing useful. Fail loudly at boot instead.
        if raw.concurrency_per_upstream == 0 {
            anyhow::bail!(
                "concurrency_per_upstream = 0 would deadlock every request: \
                 the per-upstream semaphores are built with zero permits and \
                 every acquire awaits forever"
            );
        }
        if let Some(0) = raw.front_threads {
            anyhow::bail!(
                "front_threads = 0 is invalid: pingora asserts a non-zero thread \
                 count, and the resulting panic left the process alive with no listener"
            );
        }
        if raw.max_size_bytes == 0 {
            anyhow::bail!(
                "max_size_bytes = 0 would evict every entry on every tick, \
                 leaving the cache permanently empty"
            );
        }
        if raw.inactive_ttl_secs == 0 {
            anyhow::bail!(
                "inactive_ttl_secs = 0 would expire every entry on every tick, \
                 disabling the disk cache (and the staged-segment sweep)"
            );
        }
        {
            // Duplicate ids were silently resolved by "last one wins" in the
            // slot map, so one upstream's config vanished with no message.
            let mut seen = std::collections::HashSet::new();
            for u in &raw.upstreams {
                if u.id.trim().is_empty() {
                    anyhow::bail!("[[upstreams]] entry has an empty id");
                }
                if u.base_url.trim().is_empty() {
                    anyhow::bail!("upstream {}: base_url must not be empty", u.id);
                }
                if !seen.insert(u.id.as_str()) {
                    anyhow::bail!(
                        "duplicate upstream id {:?}: ids must be unique, otherwise \
                         one entry silently overwrites the other",
                        u.id
                    );
                }
            }
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
        // Fill-policy profiles: every non-standard reference resolves. Fail
        // fast at boot, not on first miss.
        let mut profiles = HashMap::new();
        for (name, raw) in &raw.cache_profiles {
            profiles.insert(
                name.clone(),
                CacheProfile {
                    min_file_size: raw.min_file_size,
                    coverage_window_secs: raw.coverage_window_secs,
                },
            );
        }
        for u in &raw.upstreams {
            // Built-in profile names need no [cache_profiles] table;
            // anything else must resolve to a declared table.
            let builtin = u.cache_profile == "standard"
            || u.cache_profile == "nocache"
            || u.cache_profile == "efficient";
            if !builtin && !profiles.contains_key(&u.cache_profile) {
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
            front_metrics_listen: raw.front_metrics_listen,
            front_ip_block: raw.front_ip_block,
            front_ip_allow: raw.front_ip_allow,
            front_rate_rps: raw.front_rate_rps,
            front_threads: raw.front_threads,
            cache_dir: raw.cache_dir,
            max_size_bytes: raw.max_size_bytes,
            max_entries: raw.max_entries,
            inactive_ttl_secs: raw.inactive_ttl_secs,
            revalidate_ttl_secs: raw.revalidate_ttl_secs,
            negative_ttl_secs: raw.negative_ttl_secs,
            eviction_policy: raw.eviction_policy,
            session_window_bytes: raw.session_window_bytes,
            read_grace_secs: raw.read_grace_secs,
            watch_idle_secs: raw.watch_idle_secs,
            watch_pin_bytes: raw.watch_pin_bytes,
            concurrency_per_upstream: raw.concurrency_per_upstream,
            retry_max_attempts: raw.retry_max_attempts,
            retry_base_ms: raw.retry_base_ms,
            retry_max_ms: raw.retry_max_ms,
            prewarm_shared_secret_env: raw.prewarm_shared_secret_env,
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
            front_metrics_listen: None,
            front_ip_block: Vec::new(),
            front_ip_allow: Vec::new(),
            front_rate_rps: None,
            front_threads: None,
            cache_dir: default_cache_dir(),
            max_size_bytes: default_max_size(),
            max_entries: default_max_entries(),
            inactive_ttl_secs: default_inactive_ttl(),
            revalidate_ttl_secs: default_revalidate_ttl(),
            negative_ttl_secs: default_negative_ttl(),
            eviction_policy: EvictionPolicy::default(),
            session_window_bytes: default_session_window_bytes(),
            read_grace_secs: default_read_grace_secs(),
            watch_idle_secs: default_watch_idle_secs(),
            watch_pin_bytes: default_watch_pin_bytes(),
            concurrency_per_upstream: default_concurrency(),
            retry_max_attempts: default_retry_max(),
            retry_base_ms: default_retry_base(),
            retry_max_ms: default_retry_max_ms(),
            prewarm_shared_secret_env: None,
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

    /// Ticket #58: every one of these values produced a service that started
    /// cleanly and then did nothing useful. They must be rejected at boot.
    #[test]
    fn rejects_zero_and_duplicate_values_that_break_serving() {
        // Base config that parses; each case below perturbs exactly one field.
        let base = r#"
            [[upstreams]]
            id = "a"
            type = "openlist"
            base_url = "http://127.0.0.1:5244/dav"
            username_env = "A_USER"
            password_env = "A_PASS"
            [[routes]]
            prefix = ""
            upstream = "a"
        "#;
        assert!(Config::from_toml_str(base).is_ok(), "the baseline must parse");

        // Top-level fields must precede any [table]; prepend, do not insert
        // after [[routes]].
        let with_field = |field: &str| format!("{field}\n{base}");
        for (field, needle) in [
            // Semaphore::new(0): every acquire awaits forever.
            ("concurrency_per_upstream = 0", "concurrency_per_upstream"),
            // pingora asserts a non-zero thread count; the panic left no listener.
            ("front_threads = 0", "front_threads"),
            // Evicts every entry on every tick.
            ("max_size_bytes = 0", "max_size_bytes"),
            // Expires every entry on every tick.
            ("inactive_ttl_secs = 0", "inactive_ttl_secs"),
        ] {
            let err = Config::from_toml_str(&with_field(field))
                .unwrap_err()
                .to_string();
            assert!(err.contains(needle), "{field} must be rejected; got: {err}");
        }

        // Duplicate upstream ids: one silently overwrote the other.
        let toml = r#"
            [[upstreams]]
            id = "dup"
            type = "openlist"
            base_url = "http://127.0.0.1:5244/dav"
            username_env = "A_USER"
            password_env = "A_PASS"
            [[upstreams]]
            id = "dup"
            type = "openlist"
            base_url = "http://127.0.0.1:5245/dav"
            username_env = "B_USER"
            password_env = "B_PASS"
            [[routes]]
            prefix = ""
            upstream = "dup"
        "#;
        let err = Config::from_toml_str(toml).unwrap_err().to_string();
        assert!(err.contains("duplicate upstream id"), "got: {err}");

        // Empty id and empty base_url are equally unusable.
        let toml = base.replace("id = \"a\"", "id = \"\"");
        assert!(Config::from_toml_str(&toml).is_err(), "empty id must be rejected");
        let toml = base.replace("http://127.0.0.1:5244/dav", "");
        assert!(Config::from_toml_str(&toml).is_err(), "empty base_url must be rejected");
    }

    /// The zero semantics that ARE documented must keep working: these are
    /// feature switches, not mistakes.
    #[test]
    fn documented_zero_semantics_still_accepted() {
        // max_entries = 0 disables the entry-count cap (bytes-only budget).
        let toml = r#"
            max_entries = 0

            [[upstreams]]
            id = "a"
            type = "openlist"
            base_url = "http://127.0.0.1:5244/dav"
            username_env = "A_USER"
            password_env = "A_PASS"
            [[routes]]
            prefix = ""
            upstream = "a"
        "#;
        assert!(Config::from_toml_str(toml).is_ok(), "max_entries = 0 is a documented switch");

        // coverage_window_secs = 0 means "no decay".
        let toml = r#"
            [[upstreams]]
            id = "a"
            type = "openlist"
            base_url = "http://127.0.0.1:5244/dav"
            username_env = "A_USER"
            password_env = "A_PASS"
            cache_profile = "eff"

            [cache_profiles.eff]
            coverage_window_secs = 0

            [[routes]]
            prefix = ""
            upstream = "a"
        "#;
        assert!(Config::from_toml_str(toml).is_ok(), "window 0 means no decay");
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

    /// A host that merely STARTS with a loopback name is somebody else's
    /// host: `127.evil.com` and `localhost.evil.com` are public DNS names,
    /// so accepting plain http to them would put the WebDAV credentials on
    /// the wire. The redirect policy has the mirror of this test, and both
    /// hold because they call one shared predicate.
    #[test]
    fn hosts_that_merely_look_loopback_do_not_get_plain_http() {
        for host in [
            "127.evil.com",
            "localhost.evil.com",
            "127.0.0.1.evil.com",
            "localhosts",
            "128.0.0.1",
        ] {
            assert!(
                Config::from_toml_str(&upstream_toml(&format!("http://{host}:5244/dav"))).is_err(),
                "{host} must not be accepted over plain http"
            );
        }
        // The genuine loopback spellings still are, including the RFC 6761
        // `.localhost` TLD.
        for host in ["127.0.0.1", "localhost", "[::1]", "openlist.localhost"] {
            assert!(
                Config::from_toml_str(&upstream_toml(&format!("http://{host}:5244/dav"))).is_ok(),
                "{host} must stay usable over plain http"
            );
        }
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
        let ok = profile_toml("[cache_profiles.efficient]\nmin_file_size = 4", "cache_profile = \"efficient\"");
        let cfg = Config::from_toml_str(&ok).unwrap();
        let p = cfg.cache_profile("a");
        assert!(p.efficient);
        assert_eq!(p.min_file_size, 4, "a table of that name tunes the built-in");
        // Efficient IS the default now (a ranged read is the shape every viewer
        // produces), and the built-in stages everything: no min_file_size floor.
        let default = Config::from_toml_str(&upstream_toml("http://127.0.0.1:5244/dav")).unwrap();
        let p = default.cache_profile("a");
        assert!(p.efficient, "the default profile must be efficient");
        assert_eq!(p.min_file_size, 0);
        // ...and `standard` is still available, unchanged.
        let std_toml = profile_toml("", "cache_profile = \"standard\"");
        let std = Config::from_toml_str(&std_toml).unwrap();
        assert!(!std.cache_profile("a").efficient);
    }

    #[test]
    fn profile_reference_validated() {
        let dangling = profile_toml("", "cache_profile = \"ghost\"");
        assert!(Config::from_toml_str(&dangling).is_err());
    }

    #[test]
    fn nocache_is_builtin_profile() {
        // The built-in nocache profile needs no [cache_profiles] table
        // (real bug caught by the CDN-LAB dry-run: boot rejected
        // cache_profile = "nocache" with a dangling-table error).
        let toml = format!(
            r#"
            [[upstreams]]
            id = "a"
            type = "openlist"
            base_url = "http://127.0.0.1:5244/dav"
            username_env = "A_USER"
            password_env = "A_PASS"
            cache_profile = "nocache"
            [[routes]]
            prefix = ""
            upstream = "a"
        "#
        );
        let cfg = Config::from_toml_str(&toml).unwrap();
        let p = cfg.cache_profile("a");
        assert!(p.nocache);
        assert!(!p.efficient);
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

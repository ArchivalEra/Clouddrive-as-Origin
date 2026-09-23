//! Front plane: Pingora-based TLS termination + HTTP/2 reverse proxy to
//! the business plane on loopback. Owns no cache semantics — it only
//! moves bytes. TLS material comes from `tls_cert_env` / `tls_key_env`
//! paths; absent material = plaintext proxy + boot warning (EdgeOne is
//! expected to carry public HTTPS in that deployment).
//!
//! Pingora (Cloudflare's proxy framework) provides native HTTP/2
//! multiplexing, TLS termination, and graceful shutdown — replacing the
//! hand-rolled rustls byte proxy.
//!
//! Observability: an optional Prometheus listener reports three static
//! metrics (requests by proto/method/status, active downstream
//! connections, upstream reuse) via `pingora::apps`' PrometheusHttpApp,
//! which serves the crate-wide default registry.

use std::net::{IpAddr, SocketAddr};
use std::time::Instant;

use anyhow::Context;
use ipnet::IpNet;
use pingora::{
    proxy::{ProxyHttp, Session},
    server::Server,
    upstreams::peer::{HttpPeer, Peer},
};
use pingora_error::{Error, ErrorType, Result as ProxyResult};
use prometheus::{
    register_histogram_vec, register_int_counter_vec, register_int_gauge, HistogramVec,
    IntCounterVec, IntGauge,
};
use tracing::{info, warn};

/// Prewarm is a tiny authenticated POST; anything materially larger at
/// the front is abuse and gets rejected before reaching the business
/// plane (whose token check stays authoritative).
pub const PREWARM_MAX_BODY: usize = 64 * 1024;

/// Everything the front plane needs at boot. CIDR lists are plain
/// strings here; parsing (and its errors) happen in this crate so the
/// config layer stays serializable.
pub struct FrontOptions {
    pub front: SocketAddr,
    pub business: SocketAddr,
    /// Some((cert, key)) = TLS termination + h2 ALPN; None = plaintext.
    pub tls: Option<(String, String)>,
    /// Prometheus listener (loopback); None disables the endpoint.
    pub metrics: Option<String>,
    /// Client CIDRs refused at connection time (before TLS handshake).
    pub ip_block: Vec<String>,
    /// Client CIDRs exempt from per-IP rate limiting (ops path is never throttled).
    pub ip_allow: Vec<String>,
    /// Per-client-IP requests/sec ceiling; None disables rate limiting
    /// (threshold lands with the real-traffic baseline, map ticket 45).
    pub rate_rps: Option<u32>,
    /// Admission control (R4): `Some((header, value))` requires `header` to
    /// carry `value` on every request from a peer outside `origin_token_exempt`.
    /// None = off, which serves whoever reaches the port.
    pub origin_token: Option<(String, String)>,
    /// Client CIDRs exempt from the origin token: the node's own probes
    /// (`accept.sh`, the LAB, the watchdog) come from loopback.
    pub origin_token_exempt: Vec<String>,
    /// Service worker threads for the proxy listener. Pingora's default is
    /// 1, which serializes all TLS/H2/byte movement on one core (P6).
    /// None keeps the framework default; callers size it to the box.
    pub threads: Option<usize>,
}

/// Parse a CIDR allow/block entry; a bare IP is treated as /32 (or /128).
fn parse_cidr(s: &str) -> anyhow::Result<IpNet> {
    if s.contains('/') {
        s.parse::<IpNet>()
            .with_context(|| format!("parse CIDR {s:?}"))
    } else {
        let ip: std::net::IpAddr = s
            .parse()
            .with_context(|| format!("parse IP {s:?}"))?;
        Ok(IpNet::from(ip))
    }
}

fn parse_cidrs(list: &[String]) -> anyhow::Result<Vec<IpNet>> {
    list.iter().map(|s| parse_cidr(s)).collect()
}

/// Match a peer address against a CIDR list, canonicalizing first.
///
/// The front listens on `[::]`, so an IPv4 peer arrives as an IPv4-mapped IPv6
/// address (`::ffff:a.b.c.d`) and an IPv4 CIDR does not contain it. Measured on
/// the node: the loopback exemption for the origin token missed `accept.sh`
/// entirely, and the same hole was latent in `front_ip_block` and
/// `front_ip_allow`, where an operator writes a v4 CIDR that then never matches.
/// Mapping the form back is what makes a list mean what it says whichever
/// socket the peer came in on.
fn ip_in_any(nets: &[IpNet], ip: &IpAddr) -> bool {
    let canonical = match ip {
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => IpAddr::V4(v4),
            None => IpAddr::V6(*v6),
        },
        v4 => *v4,
    };
    nets.iter().any(|n| n.contains(&canonical))
}

/// Connection-time gate: refused CIDRs never reach the TLS handshake.
/// The allow list plays no role here — it only exempts from rate
/// limiting — so an allow entry can never accidentally widen access.
#[derive(Debug)]
struct IpFilter {
    block: Vec<IpNet>,
}

impl IpFilter {
    fn accepts(&self, addr: &SocketAddr) -> bool {
        !ip_in_any(&self.block, &addr.ip())
    }
}

#[async_trait::async_trait]
impl pingora::listeners::ConnectionFilter for IpFilter {
    async fn should_accept(&self, addr: Option<&SocketAddr>) -> bool {
        match addr {
            Some(a) => {
                let accept = self.accepts(a);
                if !accept {
                    warn!(peer = %a, "connection refused by ip blocklist");
                }
                accept
            }
            // No peer address available — do not guess-block.
            None => true,
        }
    }
}

/// Per-client-IP rate limiter (sliding window from pingora-limits).
/// `exempt` CIDRs (the ops path) bypass it entirely.
struct RateGate {
    rate: pingora_limits::rate::Rate,
    rps: u32,
    exempt: Vec<IpNet>,
}

impl RateGate {
    fn exceeds(&self, addr: &pingora::protocols::l4::socket::SocketAddr) -> bool {
        // UDS peers have no IP to rate-limit; only inet sockets gate.
        let Some(std_addr) = addr.as_inet() else {
            return false;
        };
        if ip_in_any(&self.exempt, &std_addr.ip()) {
            return false;
        }
        // rps == 0 means rate limiting is disabled — never gate.
        // `observe` counts this request in the 1s window and returns the
        // running count; strictly over the ceiling means refuse.
        if self.rps == 0 {
            return false;
        }
        self.rate.observe(&std_addr.ip().to_string(), 1) > self.rps as isize
    }
}

/// Admission control (R4): the edge stamps every request it forwards with a
/// secret header (`ModifyRequestHeader` on the CDN rule), so a request that does
/// not carry it did not come from the edge — it came straight at the origin,
/// past the CDN and past its accounting. Refusing those is what closes that
/// bypass, and it does so without depending on the pull nodes' addresses, which
/// are not published on this plan (docs/security-hardening.md R3).
///
/// `exempt` CIDRs are the peers that legitimately carry no stamp. A peer we
/// cannot name is NOT exempt: no address, no trust.
struct OriginTokenGate {
    header: http::HeaderName,
    value: Vec<u8>,
    exempt: Vec<IpNet>,
}

impl OriginTokenGate {
    fn new(header: &str, value: &str, exempt: Vec<IpNet>) -> anyhow::Result<Self> {
        if value.is_empty() {
            anyhow::bail!("origin token value is empty");
        }
        Ok(Self {
            header: header
                .parse()
                .with_context(|| format!("parse origin token header name {header:?}"))?,
            value: value.as_bytes().to_vec(),
            exempt,
        })
    }

    /// Must this peer carry the stamp?
    fn requires(&self, peer: Option<&pingora::protocols::l4::socket::SocketAddr>) -> bool {
        match peer.and_then(|a| a.as_inet()) {
            Some(std_addr) => !ip_in_any(&self.exempt, &std_addr.ip()),
            None => true,
        }
    }

    fn accepts(&self, headers: &http::HeaderMap) -> bool {
        let got = headers
            .get(&self.header)
            .map(http::HeaderValue::as_bytes)
            .unwrap_or(b"");
        constant_time_eq(got, &self.value)
    }
}

/// Compare a presented secret with the expected one without leaking how much of
/// it matched: `==` on slices returns at the first difference, and a timing
/// signal over a secret is a way to learn it one byte at a time. The length
/// check is not constant-time; lengths are not the secret. This duplicates the
/// business plane's `sigv4::constant_time_eq` deliberately — this crate does not
/// depend on the plane's surface (see the `/_internal/` note above), and a
/// shared utility is where that coupling would start.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Per-request front state. `start` exists for the access-log duration
/// (map ticket 00); the connection gauge lives on `new_ctx`/`logging`
/// bookends which are both guaranteed to run. `prewarm_body_bytes`
/// backs the chunked-body cap (content-length lies are caught here).
///
/// Latency attribution (map #47): `connected_at` is stamped once the
/// upstream connection is established, `upstream_ttfb_at` when the
/// upstream response header arrives — the gaps between these three
/// instants are what `front_upstream_connect_seconds` and
/// `front_upstream_ttfb_seconds` report.
pub struct FrontCtx {
    pub start: Instant,
    connected_at: Option<Instant>,
    upstream_ttfb_at: Option<Instant>,
    prewarm_body_bytes: usize,
}

/// Latency histogram buckets, milliseconds through minutes: the edge
/// path spans µs (loopback) to minutes (cold multi-GB pull).
const LATENCY_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 180.0,
];

// Static metrics land in the global default registry — the same registry
// PrometheusHttpApp serves (prometheus 0.13, same version pingora uses).
pub static REQUESTS_TOTAL: std::sync::LazyLock<IntCounterVec> = std::sync::LazyLock::new(|| {
    register_int_counter_vec!(
        "front_requests_total",
        "front requests by protocol, method and response status",
        &["proto", "method", "status"]
    )
    .expect("register front_requests_total")
});
pub static CONNECTIONS_ACTIVE: std::sync::LazyLock<IntGauge> = std::sync::LazyLock::new(|| {
    register_int_gauge!("front_connections_active", "active downstream connections")
        .expect("register front_connections_active")
});
pub static UPSTREAM_REUSED_TOTAL: std::sync::LazyLock<IntCounterVec> =
    std::sync::LazyLock::new(|| {
        register_int_counter_vec!(
            "front_upstream_reused_total",
            "upstream connections by reuse state",
            &["reused"]
        )
        .expect("register front_upstream_reused_total")
    });
pub static REQUEST_DURATION: std::sync::LazyLock<HistogramVec> = std::sync::LazyLock::new(|| {
    register_histogram_vec!(
        "front_request_duration_seconds",
        "total front request duration",
        &["proto", "method", "status"],
        LATENCY_BUCKETS.to_vec()
    )
    .expect("register front_request_duration_seconds")
});
pub static UPSTREAM_CONNECT: std::sync::LazyLock<HistogramVec> = std::sync::LazyLock::new(|| {
    register_histogram_vec!(
        "front_upstream_connect_seconds",
        "time from request start to upstream connection established",
        &["proto"],
        LATENCY_BUCKETS.to_vec()
    )
    .expect("register front_upstream_connect_seconds")
});
pub static UPSTREAM_TTFB: std::sync::LazyLock<HistogramVec> = std::sync::LazyLock::new(|| {
    register_histogram_vec!(
        "front_upstream_ttfb_seconds",
        "time from request start to upstream response header",
        &["proto"],
        LATENCY_BUCKETS.to_vec()
    )
    .expect("register front_upstream_ttfb_seconds")
});

fn proto_of(session: &Session) -> &'static str {
    // RequestHeader derefs to http::request::Parts, which carries the
    // negotiated version.
    if session.req_header().version == http::Version::HTTP_2 {
        "h2"
    } else {
        "h1"
    }
}

fn status_of(session: &Session) -> String {
    session
        .response_written()
        .map(|h| h.status.as_u16().to_string())
        .unwrap_or_else(|| "0".to_string())
}

/// What the configured env-var NAMES say to do about TLS, decided without
/// touching the environment. Split out so all three arms are testable: the
/// lookup itself reads process-global state that parallel tests share, and a
/// test that sets env vars to check a decision races every other test.
#[derive(Debug, PartialEq, Eq)]
pub enum TlsChoice<'a> {
    /// Both names given: read the cert path from the first, the key from the second.
    FromEnv(&'a str, &'a str),
    /// Neither given: plaintext proxy (warned, not failed — loopback and
    /// edge-terminated deployments are both shapes this repo runs).
    Plaintext,
}

/// The TLS decision for a pair of env-var names. Exactly one name is a
/// configuration mistake, not a preference: it would silently run plaintext for
/// an operator who wrote half of the pair.
pub fn tls_choice<'a>(
    tls_cert_env: Option<&'a str>,
    tls_key_env: Option<&'a str>,
) -> anyhow::Result<TlsChoice<'a>> {
    match (tls_cert_env, tls_key_env) {
        (Some(cert_env), Some(key_env)) => Ok(TlsChoice::FromEnv(cert_env, key_env)),
        (None, None) => Ok(TlsChoice::Plaintext),
        _ => anyhow::bail!("tls_cert_env and tls_key_env must be set together"),
    }
}

/// Resolve TLS material from config env pointers. `Ok(None)` = plaintext proxy.
pub fn acceptor_from_env(
    tls_cert_env: Option<&str>,
    tls_key_env: Option<&str>,
) -> anyhow::Result<Option<(String, String)>> {
    match tls_choice(tls_cert_env, tls_key_env)? {
        TlsChoice::FromEnv(cert_env, key_env) => {
            let cert_path = std::env::var(cert_env)
                .with_context(|| format!("env {cert_env} not set"))?;
            let key_path = std::env::var(key_env)
                .with_context(|| format!("env {key_env} not set"))?;
            info!(cert = %cert_path, "front plane TLS enabled");
            Ok(Some((cert_path, key_path)))
        }
        TlsChoice::Plaintext => {
            warn!("front plane without TLS (plaintext proxy) — terminate HTTPS at the edge");
            Ok(None)
        }
    }
}

/// The reverse proxy: forwards every request to the business plane on
/// loopback. No cache semantics here — pure byte movement.
pub struct BusinessProxy {
    pub business: SocketAddr,
    rate: Option<std::sync::Arc<RateGate>>,
    origin_token: Option<OriginTokenGate>,
}

#[async_trait::async_trait]
impl ProxyHttp for BusinessProxy {
    type CTX = FrontCtx;

    fn new_ctx(&self) -> Self::CTX {
        CONNECTIONS_ACTIVE.inc();
        FrontCtx {
            start: Instant::now(),
            connected_at: None,
            upstream_ttfb_at: None,
            prewarm_body_bytes: 0,
        }
    }

    /// Prewarm body-size gate, declared-length path: an over-cap
    /// content-length is rejected before reading a single body byte.
    /// Uses the error path (not the Ok(true) short-circuit) so that
    /// `logging` still runs — the connection gauge stays paired and the
    /// 413 shows up in metrics and the access log. The per-IP rate gate
    /// shares this error-path discipline (429s must be observable).
    async fn request_filter(
        &self,
        session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> ProxyResult<bool> {
        // Everything under /_internal/ is the node's own surface, and exactly
        // ONE entry in it belongs on the public hostname: prewarm, an
        // authenticated write-side entry point the upload pipeline calls there,
        // with every deployed config setting its shared secret. healthz -- and
        // whatever internal route is added next -- discloses the upstream
        // topology and configuration and is read on the business plane's
        // loopback port instead. Refusing it here removes the path
        // structurally rather than adding a token that can be leaked or
        // misconfigured.
        //
        // The rule is the PREFIX, not a name. It used to name healthz exactly,
        // so this front had to be told about every internal route the business
        // plane grew -- and a rename on either side silently republished the
        // route, because the two crates cannot see each other's spelling (this
        // crate does not depend on the plane at all). A refusal that has to be
        // kept in sync is not a refusal.
        //
        // 404, not 403: a caller should not learn that a private surface exists
        // here at all.
        let path = session.req_header().uri.path();
        if path.starts_with("/_internal/") && !path.starts_with("/_internal/prewarm/") {
            return Err(Error::new(ErrorType::HTTPStatus(404)));
        }
        // R4 admission control, ahead of the rate gate: a request that is not
        // from the edge must not spend the budget that exists to protect the
        // origin, and the answer depends on nothing the request says beyond the
        // stamp. 403 says "not for you" without describing what lives here.
        if let Some(gate) = self.origin_token.as_ref() {
            if gate.requires(session.client_addr()) && !gate.accepts(&session.req_header().headers) {
                return Err(Error::new(ErrorType::HTTPStatus(403)));
            }
        }
        if let Some(gate) = self.rate.as_ref() {
            if let Some(addr) = session.client_addr() {
                if gate.exceeds(addr) {
                    return Err(Error::new(ErrorType::HTTPStatus(429)));
                }
            }
        }
        if session.req_header().uri.path().starts_with("/_internal/prewarm/") {
            let declared = session
                .req_header()
                .headers
                .get(http::header::CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<usize>().ok());
            if declared.is_some_and(|v| v > PREWARM_MAX_BODY) {
                return Err(Error::new(ErrorType::HTTPStatus(413)));
            }
        }
        Ok(false)
    }

    /// Prewarm body-size gate, chunked backstop: content-length can lie
    /// or be absent, so accumulate actual bytes seen. The error bubbles
    /// to fail_to_proxy, which writes the 413.
    async fn request_body_filter(
        &self,
        session: &mut Session,
        body: &mut Option<bytes::Bytes>,
        _end_of_stream: bool,
        ctx: &mut Self::CTX,
    ) -> ProxyResult<()> {
        if session.req_header().uri.path().starts_with("/_internal/prewarm/") {
            if let Some(b) = body {
                ctx.prewarm_body_bytes = ctx.prewarm_body_bytes.saturating_add(b.len());
            }
            if ctx.prewarm_body_bytes > PREWARM_MAX_BODY {
                return Err(Error::new(ErrorType::HTTPStatus(413)));
            }
        }
        Ok(())
    }

    /// Forward to the business plane (loopback, plain HTTP). Timeouts
    /// are idle-semantic (interval between adjacent reads/writes), never
    /// total-transfer bounds — a multi-GB cold pull takes minutes and
    /// any overall deadline would kill it.
    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> ProxyResult<Box<HttpPeer>> {
        let mut peer = Box::new(HttpPeer::new(
            self.business.to_string(),
            false,
            String::new(),
        ));
        if let Some(opts) = peer.get_mut_peer_options() {
            // The upstream here is the business plane on loopback — the
            // same process, not a hostile peer. read_timeout is per-read
            // inactivity, not a total body cap: the business plane streams
            // progress chunks while a cold pull runs, so a flowing pull
            // never trips it, while a stalled body ends after 30 s instead
            // of hanging every attached client forever.
            opts.connection_timeout = Some(std::time::Duration::from_secs(3));
            opts.idle_timeout = Some(std::time::Duration::from_secs(60)); // pool keepalive
            opts.read_timeout = Some(std::time::Duration::from_secs(30));
        }
        Ok(peer)
    }

    /// Connection accounting: new vs reused upstream connections, plus
    /// the connect latency stamp (request start → connection up).
    async fn connected_to_upstream(
        &self,
        session: &mut Session,
        reused: bool,
        _peer: &HttpPeer,
        #[cfg(unix)] _fd: std::os::unix::io::RawFd,
        #[cfg(windows)] _sock: std::os::windows::io::RawSocket,
        _digest: Option<&pingora::protocols::Digest>,
        ctx: &mut Self::CTX,
    ) -> ProxyResult<()> {
        UPSTREAM_REUSED_TOTAL
            .with_label_values(&[if reused { "true" } else { "false" }])
            .inc();
        let now = Instant::now();
        ctx.connected_at = Some(now);
        UPSTREAM_CONNECT
            .with_label_values(&[proto_of(session)])
            .observe((now - ctx.start).as_secs_f64());
        Ok(())
    }

    /// Upstream response header arrived (phase #13, before caching) —
    /// stamp the TTFB point: request start → first upstream byte of
    /// header. This is the core attribution metric: if it is large while
    /// the business plane's own serve time is small, the upstream (cloud
    /// drive) is the bottleneck.
    async fn upstream_response_filter(
        &self,
        session: &mut Session,
        _upstream_response: &mut pingora::http::ResponseHeader,
        ctx: &mut Self::CTX,
    ) -> ProxyResult<()> {
        let now = Instant::now();
        ctx.upstream_ttfb_at = Some(now);
        UPSTREAM_TTFB
            .with_label_values(&[proto_of(session)])
            .observe((now - ctx.start).as_secs_f64());
        Ok(())
    }

    /// Guaranteed final per-request hook (runs even on failures) — the
    /// only place it is safe to decrement the connection gauge, count
    /// the request outcome, and emit the one-line access log.
    async fn logging(
        &self,
        session: &mut Session,
        e: Option<&pingora::Error>,
        ctx: &mut Self::CTX,
    ) where
        Self::CTX: Send + Sync,
    {
        REQUESTS_TOTAL
            .with_label_values(&[proto_of(session), session.req_header().method.as_str(), &status_of(session)])
            .inc();
        REQUEST_DURATION
            .with_label_values(&[
                proto_of(session),
                session.req_header().method.as_str(),
                &status_of(session),
            ])
            .observe(ctx.start.elapsed().as_secs_f64());
        CONNECTIONS_ACTIVE.dec();
        let xff = session
            .req_header()
            .headers
            .get("x-forwarded-for")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("-");
        tracing::info!(
            method = %session.req_header().method,
            path = %session.req_header().uri.path(),
            status = status_of(session),
            bytes = session.body_bytes_sent(),
            duration_ms = ctx.start.elapsed().as_millis(),
            proto = proto_of(session),
            xff = %xff,
            err = ?e.as_ref().map(|e| e.to_string()),
            "front access",
        );
    }
}

/// Run the Pingora front plane (see [`FrontOptions`]). Synchronous:
/// Pingora manages its own runtime and signal handling (SIGTERM/SIGINT
/// graceful shutdown) — must NOT be called from inside a tokio runtime
/// (run_forever panics with "Cannot start a runtime from within a
/// runtime"). Call from a dedicated std::thread.
pub fn run_front(opts: FrontOptions) -> anyhow::Result<()> {
    let mut server = Server::new(None).context("create pingora server")?;
    server.bootstrap();

    let ip_allow = parse_cidrs(&opts.ip_allow)?;
    // R4: build the admission gate from plain strings, the same way the CIDR
    // lists are built here rather than in the config layer.
    let origin_token = match &opts.origin_token {
        Some((header, value)) => Some(OriginTokenGate::new(
            header,
            value,
            parse_cidrs(&opts.origin_token_exempt)?,
        )?),
        None => None,
    };
    match &origin_token {
        Some(g) => info!(
            header = %g.header,
            exempt = opts.origin_token_exempt.len(),
            "front origin-token admission control active"
        ),
        // Off is a choice, not a default to be discovered later: whoever reaches
        // this port without the stamp is served, so say it at boot.
        None => warn!("front origin-token admission control OFF: any peer that reaches this port is served"),
    }
    let rate = opts.rate_rps.map(|rps| {
        std::sync::Arc::new(RateGate {
            rate: pingora_limits::rate::Rate::new(std::time::Duration::from_secs(1)),
            rps,
            exempt: ip_allow,
        })
    });

    let proxy = BusinessProxy {
        business: opts.business,
        rate,
        origin_token,
    };
    // Build the proxy service directly rather than via
    // `http_proxy_service`: its builder path leaves `h2_options` at None
    // in pingora 0.8.1 (the field exists but has no setter and is marked
    // TODO upstream), so downstream H2 tuning is unreachable that way.
    let mut http_proxy = pingora::proxy::HttpProxy::new(proxy, server.configuration.clone());
    // Downstream H2 flow control. Pingora's upstream H2 client uses an
    // 8 MiB window with 64 KiB frames, while its downstream default is the
    // h2 crate's 64 KiB / 16 KiB — so many concurrent range streams share
    // a small connection window. Match the upstream side (P6).
    let mut h2 = pingora::protocols::http::v2::server::H2Options::default();
    h2.initial_window_size(1 << 23)
        .initial_connection_window_size(1 << 23)
        .max_frame_size(1 << 16);
    http_proxy.h2_options = Some(h2);
    let mut service =
        pingora::services::listening::Service::new("origin-front".to_string(), http_proxy);
    if let Some(threads) = opts.threads {
        // Pingora resolves `service.threads().unwrap_or(conf.threads)`;
        // the default is 1.
        service.threads = Some(threads);
    }
    if !opts.ip_block.is_empty() {
        let filter = std::sync::Arc::new(IpFilter {
            block: parse_cidrs(&opts.ip_block)?,
        });
        service.set_connection_filter(filter);
        info!(count = opts.ip_block.len(), "front ip blocklist active");
    }
    if let Some(rps) = opts.rate_rps {
        info!(rps, "front per-ip rate limit active (allow-exempt)");
    }
    match &opts.tls {
        Some((cert, key)) => {
            // add_tls_with_settings + enable_h2: ALPN advertises h2 +
            // http/1.1 so EdgeOne origin-pull negotiates HTTP/2.
            let mut tls_settings = pingora::listeners::tls::TlsSettings::intermediate(cert, key)
                .with_context(|| format!("load tls material {cert} / {key}"))?;
            tls_settings.enable_h2();
            service.add_tls_with_settings(&opts.front.to_string(), None, tls_settings);
        }
        None => {
            service.add_tcp(&opts.front.to_string());
        }
    }
    server.add_service(service);

    if let Some(metrics_addr) = &opts.metrics {
        let mut metrics_service =
            pingora::services::listening::Service::prometheus_http_service();
        metrics_service.add_tcp(metrics_addr);
        server.add_service(metrics_service);
        info!(metrics = %metrics_addr, "front plane metrics enabled");
    }

    server.run_forever();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn addr(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    #[test]
    fn cidr_parsing_and_matching() {
        let net = parse_cidr("10.1.2.0/24").unwrap();
        assert!(net.contains(&addr("10.1.2.99:1").ip()));
        assert!(!net.contains(&addr("10.1.3.1:1").ip()));
        // bare IP = single-host net
        let host = parse_cidr("192.0.2.7").unwrap();
        assert!(host.contains(&addr("192.0.2.7:9").ip()));
        assert!(!host.contains(&addr("192.0.2.8:9").ip()));
        // v6
        let v6 = parse_cidr("2001:db8::/32").unwrap();
        assert!(v6.contains(&addr("[2001:db8::1]:80").ip()));
        assert!(parse_cidr("not-an-ip").is_err());
    }

    #[test]
    fn ip_filter_blocks_only_listed() {
        let f = IpFilter {
            block: vec![parse_cidr("203.0.113.0/24").unwrap()],
        };
        assert!(!f.accepts(&addr("203.0.113.5:1")));
        assert!(f.accepts(&addr("198.51.100.5:1")));
    }

    /// R4: the gate accepts exactly one value, under any spelling of the header
    /// name, and nothing else — no header, a wrong value, or a value that is a
    /// prefix of the secret all fail.
    #[test]
    fn origin_token_gate_accepts_only_the_stamp() {
        let gate = OriginTokenGate::new(
            "X-Origin-Token",
            "s3cret-value",
            vec![parse_cidr("127.0.0.0/8").unwrap()],
        )
        .unwrap();
        let mut h = http::HeaderMap::new();
        assert!(!gate.accepts(&h), "no header must not pass");
        h.insert(
            http::HeaderName::from_static("x-origin-token"),
            "wrong".parse().unwrap(),
        );
        assert!(!gate.accepts(&h), "a wrong value must not pass");
        h.insert(
            http::HeaderName::from_static("x-origin-token"),
            "s3cret".parse().unwrap(),
        );
        assert!(!gate.accepts(&h), "a prefix of the secret must not pass");
        h.insert(
            http::HeaderName::from_static("x-origin-token"),
            "s3cret-value".parse().unwrap(),
        );
        assert!(gate.accepts(&h), "the stamped value must pass");
        // The edge writes the header name in its own spelling. HTTP field names
        // are case-insensitive, and this is the spelling the node's config
        // carries ("X-Origin-Token"), so the lookup has to be too. (from_static
        // refuses mixed case by design; `parse` is the case-insensitive path.)
        let mut spelled = http::HeaderMap::new();
        spelled.insert(
            "X-Origin-Token".parse::<http::HeaderName>().unwrap(),
            "s3cret-value".parse().unwrap(),
        );
        assert!(gate.accepts(&spelled));
        // An empty configured value is refused at construction, not accepted as
        // "send the header empty-handed".
        assert!(OriginTokenGate::new("X-Origin-Token", "", vec![]).is_err());
    }

    /// R4: who has to carry the stamp. Loopback is the node's own probes; a peer
    /// we cannot name is refused (fail closed); an empty exempt list means even
    /// loopback must be stamped, which is the arm the LAB exercises.
    #[test]
    fn origin_token_gate_exempts_loopback_only() {
        use pingora::protocols::l4::socket::SocketAddr as PSockAddr;
        let gate = OriginTokenGate::new(
            "X-Origin-Token",
            "s3cret-value",
            vec![
                parse_cidr("127.0.0.1/32").unwrap(),
                parse_cidr("::1/128").unwrap(),
            ],
        )
        .unwrap();
        assert!(
            !gate.requires(Some(&PSockAddr::Inet(addr("127.0.0.1:5")))),
            "loopback is the node's own probes"
        );
        assert!(!gate.requires(Some(&PSockAddr::Inet(addr("[::1]:5")))));
        assert!(
            gate.requires(Some(&PSockAddr::Inet(addr("203.0.113.9:5")))),
            "a remote peer must be stamped"
        );
        assert!(gate.requires(None), "no address, no trust");
        let strict = OriginTokenGate::new("X-Origin-Token", "s3cret-value", vec![]).unwrap();
        assert!(
            strict.requires(Some(&PSockAddr::Inet(addr("127.0.0.1:5")))),
            "with no exempt list even loopback is stamped"
        );
        // The form the dual-stack listener actually reports for a v4 peer, and
        // the reason every list here goes through `ip_in_any`: measured on the
        // node, this exact address failed to match `127.0.0.1/32` and the
        // exemption silently did not apply.
        assert!(
            !gate.requires(Some(&PSockAddr::Inet(addr("[::ffff:127.0.0.1]:5")))),
            "an IPv4-mapped loopback peer is loopback"
        );
    }

    /// Every CIDR list in this crate means the same thing on either socket:
    /// `[::]` reports an IPv4 peer in the mapped form, so an IPv4 CIDR has to
    /// match it. This is the regression that cost the loopback exemption on the
    /// node (A8's second arm), where `accept.sh` was refused at its own front.
    #[test]
    fn cidr_lists_match_mapped_ipv4_peers() {
        let v4 = vec![parse_cidr("127.0.0.1/32").unwrap()];
        assert!(ip_in_any(&v4, &addr("127.0.0.1:1").ip()));
        assert!(
            ip_in_any(&v4, &addr("[::ffff:127.0.0.1]:1").ip()),
            "a mapped v4 peer must match a v4 CIDR"
        );
        assert!(!ip_in_any(&v4, &addr("[::1]:1").ip()));
        let block = vec![parse_cidr("203.0.113.0/24").unwrap()];
        assert!(ip_in_any(&block, &addr("[::ffff:203.0.113.9]:1").ip()));
        let v6 = vec![parse_cidr("::1/128").unwrap()];
        assert!(ip_in_any(&v6, &addr("[::1]:1").ip()));
        assert!(!ip_in_any(&v6, &addr("127.0.0.1:1").ip()));
    }

    #[test]
    fn constant_time_eq_matches_only_equal_bytes() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
        assert!(!constant_time_eq(b"abc", b""));
        assert!(constant_time_eq(b"", b""));
    }

    #[test]
    fn rate_gate_exempts_allowlist() {
        use pingora::protocols::l4::socket::SocketAddr as PSockAddr;
        let g = RateGate {
            rate: pingora_limits::rate::Rate::new(std::time::Duration::from_secs(1)),
            rps: 1,
            exempt: vec![parse_cidr("127.0.0.0/8").unwrap()],
        };
        let loopback = PSockAddr::Inet(addr("127.0.0.1:1"));
        for _ in 0..10 {
            assert!(!g.exceeds(&loopback), "exempt ip must never exceed");
        }
        // a non-exempt ip exceeds on the 2nd event within the window
        // (observe counts the current event: 1st = 1 <= rps, 2nd = 2 > rps)
        let other = PSockAddr::Inet(addr("192.0.2.1:1"));
        assert!(!g.exceeds(&other));
        assert!(g.exceeds(&other));
    }

    fn rendered() -> String {
        use prometheus::Encoder as _;
        let families = prometheus::gather();
        let mut buf = vec![];
        prometheus::TextEncoder::new()
            .encode(&families, &mut buf)
            .unwrap();
        String::from_utf8(buf).unwrap()
    }

    /// The three TLS arms, decided without touching the environment — plus the
    /// lookup's own failure, which is the one an operator actually meets: a
    /// config that names an env var the unit never sets.
    #[test]
    fn tls_choice_covers_all_three_arms() {
        assert_eq!(
            tls_choice(Some("CERT_ENV"), Some("KEY_ENV")).unwrap(),
            TlsChoice::FromEnv("CERT_ENV", "KEY_ENV")
        );
        assert_eq!(tls_choice(None, None).unwrap(), TlsChoice::Plaintext);
        assert!(tls_choice(Some("CERT_ENV"), None).is_err(), "half a pair is a config bug");
        assert!(tls_choice(None, Some("KEY_ENV")).is_err(), "half a pair is a config bug");
    }

    /// A missing env var is an error with the variable's NAME in it, not a
    /// silent fallback to plaintext. The name below is never set, so this test
    /// touches no shared state and cannot race the rest of the suite.
    #[test]
    fn a_named_but_unset_env_var_is_an_error() {
        let err = acceptor_from_env(Some("ORIGIN_TEST_ABSENT_CERT"), Some("ORIGIN_TEST_ABSENT_KEY"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("ORIGIN_TEST_ABSENT_CERT"), "{err}");
    }

    #[test]
    fn metrics_exposed_and_incremented() {
        REQUESTS_TOTAL
            .with_label_values(&["h1", "GET", "200"])
            .inc();
        UPSTREAM_REUSED_TOTAL.with_label_values(&["true"]).inc();
        CONNECTIONS_ACTIVE.inc();
        CONNECTIONS_ACTIVE.dec();
        let text = rendered();
        assert!(text.contains(r#"front_requests_total{method="GET",proto="h1",status="200"}"#));
        assert!(text.contains(r#"front_upstream_reused_total{reused="true"}"#));
        assert!(text.contains("front_connections_active"));
    }

    /// T1: the three latency histograms are registered in the same
    /// process-global registry and expose _bucket/_sum/_count lines.
    #[test]
    fn latency_histograms_exposed() {
        REQUEST_DURATION
            .with_label_values(&["h2", "GET", "200"])
            .observe(0.042);
        UPSTREAM_CONNECT.with_label_values(&["h2"]).observe(0.0003);
        UPSTREAM_TTFB.with_label_values(&["h2"]).observe(0.85);
        let text = rendered();
        for name in [
            "front_request_duration_seconds",
            "front_upstream_connect_seconds",
            "front_upstream_ttfb_seconds",
        ] {
            assert!(
                text.contains(&format!("{name}_bucket")),
                "{name} missing _bucket"
            );
            assert!(text.contains(&format!("{name}_sum")), "{name} missing _sum");
            assert!(
                text.contains(&format!("{name}_count")),
                "{name} missing _count"
            );
        }
        // Presence only — exact counts race with other tests sharing the
        // process-global registry.
        assert!(text.contains(r#"front_upstream_ttfb_seconds_count{proto="h2"}"#));
    }

    /// T3 guard: latency label sets are bounded — no path/key/ip labels
    /// anywhere in the front metrics (high-cardinality = series explosion).
    #[test]
    fn latency_labels_are_bounded() {
        let text = rendered();
        for line in text.lines().filter(|l| l.starts_with("front_")) {
            for forbidden in ["path=", "key=", "ip=", "addr="] {
                assert!(
                    !line.contains(forbidden),
                    "high-cardinality label {forbidden} in: {line}"
                );
            }
        }
    }
}

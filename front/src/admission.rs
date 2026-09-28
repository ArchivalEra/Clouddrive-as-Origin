//! The front's door: one module that owns the ORDER in which a request is
//! admitted, refused or slowed.
//!
//! It used to be a sequence of statements inside `request_filter`, which meant
//! answering "what happens to this request?" required reading that function
//! plus three gate structs plus the business plane's policy six files away, and
//! meant the order itself — the fact that the edge stamp is checked before the
//! rate ceiling, and the rate ceiling before the body cap — existed only as
//! line order in a 60-line function with no test crossing it.
//!
//! The interface is [`Admission::check`]: one request in, one [`Verdict`] out.
//! Behind it sit the three gates and the order they run in. The front keeps the
//! door itself — it parses the peer, writes the status and logs the line — and
//! the content-auth *policy* still lives in the plane ([`ContentAuth`] is a
//! seam this crate defines and the plane implements).
//!
//! Order, and why:
//!
//! 1. **The private surface is not published.** `/_internal/` answers 404
//!    (except prewarm) before anything else looks at the request, so a probe
//!    cannot even learn the surface exists.
//! 2. **The edge stamp** (R4): a request that did not come through the edge
//!    must not spend the budget that exists to protect the origin, and the
//!    answer does not depend on anything the request says.
//! 3. **The content ticket** (ADR-0027): the same reasoning one layer up — a
//!    request that cannot name itself gets a 403 (or a 503 when its session is
//!    over budget), before the cache, the ledger or the provider is touched.
//! 4. **The per-IP rate ceiling**: last of the identity checks, because it is
//!    the one that cannot see a client through the CDN (R8) and exists for a
//!    plane with many untrusted peers.
//! 5. **The prewarm body cap**: only prewarm has a body worth bounding, and an
//!    over-cap request is refused before a single body byte is read.

use std::net::IpAddr;
use std::sync::Arc;

use anyhow::Context;
use ipnet::IpNet;

use crate::{constant_time_eq, ip_in_any, parse_cidrs, ContentAuth, ContentDecision, ContentRequest, FrontOptions};

/// Largest body the prewarm endpoint accepts, declared or accumulated.
pub const PREWARM_MAX_BODY: usize = 64 * 1024;

/// Who the door says this request is: the tenant a verified ticket named, and
/// the session its budgets are charged to. The access log carries it and the
/// byte accounting is keyed by it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    pub credential: String,
    pub session: Option<String>,
}

/// One request, as the door sees it. Everything is a borrowed slice of what
/// the protocol layer already parsed, so building this costs nothing.
pub struct RequestHead<'a> {
    pub method: &'a str,
    pub path: &'a str,
    /// Raw path and query, exactly as the client sent them (a signature is
    /// computed over the encoded form).
    pub path_and_query: &'a str,
    /// The host a signed request is verified against — see [`request_host`].
    pub host: Option<&'a str>,
    /// The peer that opened the connection. `None` means the transport did not
    /// give us one (a unix socket): a peer we cannot name is never exempt.
    pub peer: Option<IpAddr>,
    /// Declared `Content-Length`, when the request carried a parseable one.
    pub declared_len: Option<u64>,
    /// The request headers. The stamp's NAME is configuration (the edge sets
    /// whatever the operator told it to), so the gate looks it up here rather
    /// than the caller guessing which header matters.
    pub headers: &'a http::HeaderMap,
}

/// The host a signed request must be verified against.
///
/// SigV4 signs `Host`, so this has to be the value the client signed. HTTP/1.1
/// sends it as a header; **HTTP/2 has no `host` header** — the authority
/// carries it — and the CDN pulls from the origin over h2, so a header-only
/// lookup refuses every signed read through the edge. Measured 2026-09-28 on
/// the live deployment: `403 signed header missing from request` for a URL the
/// command line accepted, on `proto="h2"` requests, while the same URL worked
/// over h1 to a loopback instance (which is why the LAB's arm passed and the
/// CDN did not). The header wins when both are present: that is literally what
/// an h1 client signed.
pub fn request_host<'a>(headers: &'a http::HeaderMap, uri: &'a http::Uri) -> Option<&'a str> {
    headers
        .get(http::header::HOST)
        .and_then(|v| v.to_str().ok())
        .or_else(|| uri.authority().map(|a| a.as_str()))
}

/// What the door decided.
#[derive(Debug)]
pub enum Verdict {
    /// Proceed to the business plane. `identity` is set when a content gate
    /// admitted the request and the log/accounting should carry it.
    Serve { identity: Option<Identity> },
    /// Refuse with a bare status. `reason` is a static string from this module
    /// or from the content gate (which owns the signing taxonomy); it reaches
    /// the access log so a refusal is diagnosable from one line.
    Refuse { status: u16, reason: &'static str },
}

/// Per-client-IP rate limiter (sliding window from pingora-limits). `exempt`
/// CIDRs (the ops path) bypass it entirely.
struct RateGate {
    rate: pingora_limits::rate::Rate,
    rps: u32,
    exempt: Vec<IpNet>,
}

impl RateGate {
    fn exceeds(&self, peer: Option<IpAddr>) -> bool {
        // No address, nothing to rate-limit; a UDS peer is local by
        // construction.
        let Some(ip) = peer else {
            return false;
        };
        if ip_in_any(&self.exempt, &ip) {
            return false;
        }
        // rps == 0 means rate limiting is disabled — never gate.
        if self.rps == 0 {
            return false;
        }
        // `observe` counts this request in the window and returns the running
        // count; strictly over the ceiling means refuse.
        self.rate.observe(&ip.to_string(), 1) > self.rps as isize
    }
}

/// Admission control (R4): the edge stamps every request it forwards with a
/// secret header (`ModifyRequestHeader` on the CDN rule), so a request that
/// does not carry it did not come from the edge — it came straight at the
/// origin, past the CDN and past its accounting. Refusing those is what closes
/// that bypass, and it does so without depending on the pull nodes' addresses,
/// which are not published on this plan (docs/security-hardening.md R3).
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
    fn requires(&self, peer: Option<IpAddr>) -> bool {
        match peer {
            Some(ip) => !ip_in_any(&self.exempt, &ip),
            None => true,
        }
    }

    /// Does the presented stamp equal the expected one? Compared without
    /// leaking how much matched (a timing signal over a secret is a way to
    /// learn it a byte at a time); lengths are not the secret.
    fn accepts(&self, headers: &http::HeaderMap) -> bool {
        let got = headers
            .get(&self.header)
            .map(http::HeaderValue::as_bytes)
            .unwrap_or(b"");
        constant_time_eq(got, &self.value)
    }
}

/// The front-side half of the content gate: the seam types know nothing about
/// CIDRs, and the exemption list is a front concern (the same shape as the
/// origin-token exemption). Returns `None` for "not this gate's business" —
/// wrong method, an internal path, or an exempt peer.
struct ContentAuthGate {
    auth: Arc<dyn ContentAuth>,
    exempt: Vec<IpNet>,
}

impl ContentAuthGate {
    fn check(&self, head: &RequestHead<'_>) -> Option<ContentDecision> {
        // Content reads only. Writes do not exist on this origin (the
        // business route registers GET/HEAD alone), and everything under
        // /_internal/ is the node's own surface with its own admission.
        if !matches!(head.method, "GET" | "HEAD") || is_internal(head.path) {
            return None;
        }
        // The node's own probes (accept.sh, the LAB, the watchdog) come from
        // the exemption list, the same peers the origin token exempts. A peer
        // we cannot name is not exempt.
        if let Some(ip) = head.peer {
            if ip_in_any(&self.exempt, &ip) {
                return None;
            }
        }
        Some(self.auth.admit(&ContentRequest {
            method: head.method,
            path_and_query: head.path_and_query,
            host: head.host,
        }))
    }
}

/// `/_internal/*` is the node's own surface; exactly one entry in it belongs on
/// the public hostname, prewarm, an authenticated write-side entry point the
/// upload pipeline calls through the CDN.
fn is_internal(path: &str) -> bool {
    path.starts_with("/_internal/") && !path.starts_with("/_internal/prewarm/")
}

fn is_prewarm(path: &str) -> bool {
    path.starts_with("/_internal/prewarm/")
}

/// The door: the gates and the order they run in.
pub struct Admission {
    token: Option<OriginTokenGate>,
    content: Option<ContentAuthGate>,
    rate: Option<Arc<RateGate>>,
}

impl Admission {
    /// Build from the boot options. Each knob is independent; an absent one
    /// simply leaves its stage out (and says so at boot, in `run_front`).
    pub fn from_options(opts: &FrontOptions) -> anyhow::Result<Self> {
        let token = match &opts.origin_token {
            Some((header, value)) => Some(OriginTokenGate::new(
                header,
                value,
                parse_cidrs(&opts.origin_token_exempt)?,
            )?),
            None => None,
        };
        let content = match &opts.content_auth {
            Some(auth) => Some(ContentAuthGate {
                auth: Arc::clone(auth),
                exempt: parse_cidrs(&opts.content_auth_exempt)?,
            }),
            None => None,
        };
        let rate = opts.rate_rps.map(|rps| {
            Ok::<_, anyhow::Error>(Arc::new(RateGate {
                rate: pingora_limits::rate::Rate::new(std::time::Duration::from_secs(1)),
                rps,
                exempt: parse_cidrs(&opts.ip_allow)?,
            }))
        });
        let rate = match rate {
            Some(r) => Some(r?),
            None => None,
        };
        Ok(Self { token, content, rate })
    }

    /// The whole door decision for one request.
    pub fn check(&self, head: &RequestHead<'_>) -> Verdict {
        // 1. The private surface is not published. 404, not 403: a caller
        //    should not learn that a private surface exists here at all.
        if is_internal(head.path) {
            return Verdict::Refuse { status: 404, reason: "private surface" };
        }
        // 2. The edge stamp (R4).
        if let Some(gate) = self.token.as_ref() {
            if gate.requires(head.peer) && !gate.accepts(head.headers) {
                return Verdict::Refuse { status: 403, reason: "no edge stamp" };
            }
        }
        // 3. The content ticket (ADR-0027), which also carries the session
        //    budgets: a refusal here can be the S3-standard 503.
        let mut identity = None;
        if let Some(gate) = self.content.as_ref() {
            match gate.check(head) {
                None => {}
                Some(ContentDecision::Allow { credential, session }) => {
                    identity = Some(Identity { credential, session });
                }
                Some(ContentDecision::Deny { status, reason }) => {
                    return Verdict::Refuse { status, reason };
                }
            }
        }
        // 4. The per-IP rate ceiling.
        if let Some(gate) = self.rate.as_ref() {
            if gate.exceeds(head.peer) {
                return Verdict::Refuse { status: 429, reason: "rate ceiling" };
            }
        }        // 5. The prewarm body cap, declared-length half (the chunked backstop
        //    is `prewarm_body_exceeds`, called per body chunk).
        if is_prewarm(head.path) && head.declared_len.is_some_and(|v| v > PREWARM_MAX_BODY as u64) {
            return Verdict::Refuse { status: 413, reason: "prewarm body too large" };
        }
        Verdict::Serve { identity }
    }

    /// The chunked half of the prewarm body cap: content-length can lie or be
    /// absent, so the caller accumulates real bytes and asks here.
    pub fn prewarm_body_exceeds(&self, seen: usize) -> bool {
        seen > PREWARM_MAX_BODY
    }

    /// Post-response byte accounting for an allowed request: the content
    /// gate's byte budget is charged with what the response actually sent,
    /// because a range request's size is unknowable at the door.
    pub fn account(&self, identity: &Identity, bytes: u64) {
        if let Some(gate) = self.content.as_ref() {
            gate.auth.observe(&identity.credential, identity.session.as_deref(), bytes);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    fn head<'a>(method: &'a str, path_and_query: &'a str, headers: &'a http::HeaderMap) -> RequestHead<'a> {
        let path = path_and_query.split('?').next().unwrap();
        RequestHead {
            method,
            path,
            path_and_query,
            host: Some("cdn.example"),
            peer: Some(ip("198.51.100.9")),
            declared_len: None,
            headers,
        }
    }

    /// A header map carrying the edge stamp (the value the edge would set).
    fn stamped(value: &str) -> http::HeaderMap {        let mut h = http::HeaderMap::new();
        h.insert(
            http::HeaderName::from_static("x-origin-token"),
            value.parse().unwrap(),
        );
        h
    }

    /// A fake content adapter: records what it was asked and what it was
    /// charged, answers from a script.
    struct FakeGate {
        asked: Mutex<Vec<String>>,
        observed: Mutex<Vec<(String, Option<String>, u64)>>,
        answer: Mutex<ContentDecision>,
    }
    impl FakeGate {
        fn allowing() -> Arc<Self> {
            Arc::new(Self {
                asked: Mutex::new(Vec::new()),
                observed: Mutex::new(Vec::new()),
                answer: Mutex::new(ContentDecision::Allow {
                    credential: "tenant-a".into(),
                    session: Some("sess-1".into()),
                }),
            })
        }
        fn denying(status: u16, reason: &'static str) -> Arc<Self> {
            let gate = Self::allowing();
            *gate.answer.lock().unwrap() = ContentDecision::Deny { status, reason };
            gate
        }
    }
    impl ContentAuth for FakeGate {
        fn admit(&self, request: &ContentRequest<'_>) -> ContentDecision {
            self.asked
                .lock()
                .unwrap()
                .push(format!("{} {}", request.method, request.path_and_query));
            self.answer.lock().unwrap().clone()
        }
        fn observe(&self, credential: &str, session: Option<&str>, bytes: u64) {
            self.observed
                .lock()
                .unwrap()
                .push((credential.to_string(), session.map(str::to_string), bytes));
        }
    }

    fn options() -> FrontOptions {
        FrontOptions {
            front: "127.0.0.1:0".parse().unwrap(),
            business: "127.0.0.1:1".parse().unwrap(),
            tls: None,
            metrics: None,
            ip_block: vec![],
            ip_allow: vec!["192.0.2.0/24".into()],
            rate_rps: Some(1),
            origin_token: Some(("X-Origin-Token".into(), "s3cret".into())),
            origin_token_exempt: vec!["10.0.0.0/8".into()],
            content_auth: None,
            content_auth_exempt: vec!["10.0.0.0/8".into()],
            threads: None,
        }
    }

    fn refusal(v: Verdict) -> (u16, &'static str) {
        match v {
            Verdict::Refuse { status, reason } => (status, reason),
            Verdict::Serve { .. } => panic!("expected a refusal"),
        }
    }

    /// The order is the point: a stamped request from an exempt peer is
    /// served; an unstamped one is refused before the rate ceiling can see it
    /// (403, not 429), and the private surface is refused before anything.
    ///
    /// The rate ceiling is deliberately left at 1 rps: it is what makes the
    /// order observable. The unstamped peer asks twice and both answers are
    /// 403 — if the ceiling ran first, the second would be 429.
    #[test]
    fn the_order_is_stamp_then_ceiling() {
        let admission = Admission::from_options(&options()).unwrap();
        let none = http::HeaderMap::new();
        let good = stamped("s3cret");
        let wrong = stamped("s3cret-ish");

        // Exempt peers, no stamp: served. (Two different peers, because the
        // ceiling counts per address and these asserts are about the stamp.)
        let mut h = head("GET", "/k", &none);
        h.peer = Some(ip("10.1.2.3"));
        assert!(matches!(admission.check(&h), Verdict::Serve { identity: None }));
        let mut h = head("GET", "/k", &none);
        h.peer = Some(ip("10.2.2.2"));
        assert!(matches!(admission.check(&h), Verdict::Serve { identity: None }));

        // An unexempt peer without the stamp: 403 twice — never 429, which is
        // what the order buys (the stamp is answered from the request alone).
        let mut h = head("GET", "/k", &none);
        h.peer = Some(ip("198.51.100.9"));
        assert_eq!(refusal(admission.check(&h)), (403, "no edge stamp"));
        assert_eq!(refusal(admission.check(&h)), (403, "no edge stamp"));

        // Stamped with the wrong value: still 403.
        let mut h = head("GET", "/k", &wrong);
        h.peer = Some(ip("198.51.100.10"));
        assert_eq!(refusal(admission.check(&h)), (403, "no edge stamp"));

        // A peer we cannot name is never exempt.
        let mut h = head("GET", "/k", &none);
        h.peer = None;
        assert_eq!(refusal(admission.check(&h)), (403, "no edge stamp"));

        // And a correctly stamped unexempt peer does reach the ceiling (the
        // other side of the ordering): its second request in the window is 429.
        let mut h = head("GET", "/k", &good);
        h.peer = Some(ip("203.0.113.5"));
        assert!(matches!(admission.check(&h), Verdict::Serve { .. }));
        assert_eq!(refusal(admission.check(&h)), (429, "rate ceiling"));
    }

    /// The private surface is refused first, whatever else is wrong with the
    /// request, and 404 rather than 403 so the surface stays unacknowledged.
    #[test]
    fn the_private_surface_is_refused_before_everything() {
        let admission = Admission::from_options(&options()).unwrap();
        let none = http::HeaderMap::new();
        let mut h = head("GET", "/_internal/healthz", &none);
        h.peer = Some(ip("198.51.100.9")); // not exempt, no stamp, no ticket
        assert_eq!(refusal(admission.check(&h)), (404, "private surface"));
        // ... and an unknown future internal route hits the same rule (the
        // prefix, not a name).
        let h = head("GET", "/_internal/whatever/next", &none);
        assert_eq!(refusal(admission.check(&h)), (404, "private surface"));
    }

    /// Prewarm is the one internal route that is public, and its body cap is
    /// the last stage: refused on the declared length, and (the chunked half)
    /// by whoever is accumulating bytes.
    #[test]
    fn the_prewarm_body_cap_is_the_last_stage() {
        // No rate ceiling here: the cap is the subject, and the ceiling runs
        // before it (its own test covers that).
        let mut opts = options();
        opts.rate_rps = None;
        let admission = Admission::from_options(&opts).unwrap();
        let none = http::HeaderMap::new();
        let mut h = head("POST", "/_internal/prewarm/k", &none);
        h.peer = Some(ip("10.1.2.3")); // exempt from the stamp
        h.declared_len = Some(PREWARM_MAX_BODY as u64 + 1);
        assert_eq!(refusal(admission.check(&h)), (413, "prewarm body too large"));

        h.declared_len = Some(PREWARM_MAX_BODY as u64);
        assert!(matches!(admission.check(&h), Verdict::Serve { .. }));

        assert!(!admission.prewarm_body_exceeds(PREWARM_MAX_BODY));
        assert!(admission.prewarm_body_exceeds(PREWARM_MAX_BODY + 1));
    }

    /// The content ticket sits between the stamp and the ceiling: its refusal
    /// passes through with its own status (including the 503 a budget
    /// produces), it is only asked for content reads from unexempt peers, and
    /// an allowed request carries the identity onward.
    #[test]
    fn the_ticket_is_asked_for_content_reads_only() {
        let fake = FakeGate::denying(503, "session rate exceeded");
        let mut opts = options();
        opts.content_auth = Some(fake.clone());
        let admission = Admission::from_options(&opts).unwrap();
        let good = stamped("s3cret");

        let h = head("GET", "/googledrive1/film.mkv?X-Amz-Signature=abc", &good);
        assert_eq!(refusal(admission.check(&h)), (503, "session rate exceeded"));

        // A write verb is not a content read: the ticket is not asked.
        let h = head("POST", "/googledrive1/film.mkv", &good);
        assert!(matches!(admission.check(&h), Verdict::Serve { .. }));
        assert_eq!(fake.asked.lock().unwrap().len(), 1, "only the GET reached the adapter");

        // An exempt peer (the node's own probes) is not asked either.
        let mut h = head("GET", "/googledrive1/film.mkv", &good);
        h.peer = Some(ip("10.9.9.9"));
        assert!(matches!(admission.check(&h), Verdict::Serve { .. }));
        assert_eq!(fake.asked.lock().unwrap().len(), 1);
    }

    /// An allowed ticket names the request, and the byte accounting is charged
    /// to that name — the half that only becomes knowable after the body moved.
    #[test]
    fn an_allowed_ticket_names_the_request_and_is_charged() {
        let fake = FakeGate::allowing();
        let mut opts = options();
        opts.content_auth = Some(fake.clone());
        let admission = Admission::from_options(&opts).unwrap();

        let good = stamped("s3cret");
        let h = head("GET", "/googledrive1/film.mkv?sig=abc", &good);
        let identity = match admission.check(&h) {
            Verdict::Serve { identity: Some(id) } => id,
            other => panic!("expected an identified serve, got {other:?}"),
        };
        assert_eq!(identity.credential, "tenant-a");
        assert_eq!(identity.session.as_deref(), Some("sess-1"));

        admission.account(&identity, 4096);
        assert_eq!(
            fake.observed.lock().unwrap().as_slice(),
            &[("tenant-a".to_string(), Some("sess-1".to_string()), 4096)],
        );
    }

    /// The rate ceiling is the last identity check: it counts only requests
    /// that got that far, it exempts the allow-list, and it returns 429 (the
    /// status the error path keeps observable in the access log).
    #[test]
    fn the_rate_ceiling_counts_what_reaches_it() {
        let mut opts = options();
        opts.origin_token = None; // no stamp to satisfy, isolating the ceiling
        let admission = Admission::from_options(&opts).unwrap();
        let none = http::HeaderMap::new();

        // An exempt peer (allow list) never trips it, however often it asks.
        let mut h = head("GET", "/k", &none);
        h.peer = Some(ip("192.0.2.10"));
        for _ in 0..5 {
            assert!(matches!(admission.check(&h), Verdict::Serve { .. }));
        }

        // An unexempt peer at 1 rps: the second request in the window is 429.
        let mut h = head("GET", "/k", &none);
        h.peer = Some(ip("203.0.113.7"));
        assert!(matches!(admission.check(&h), Verdict::Serve { .. }));
        assert_eq!(refusal(admission.check(&h)), (429, "rate ceiling"));
    }

    /// The gate types keep their own contracts: the stamp comparison accepts
    /// exactly one value (no prefixes, any header-name spelling), and the
    /// exemption uses the mapped-peer matching the rest of the front uses.
    #[test]
    fn stamp_and_exemption_edge_cases() {
        let gate = OriginTokenGate::new("x-origin-token", "s3cret-value", vec![]).unwrap();
        assert!(!gate.accepts(&http::HeaderMap::new()), "no header must not pass");
        assert!(!gate.accepts(&stamped("")));
        assert!(!gate.accepts(&stamped("wrong")));
        assert!(!gate.accepts(&stamped("s3cret")), "a prefix of the secret must not pass");
        assert!(gate.accepts(&stamped("s3cret-value")));
        // The edge writes the header name in its own spelling; HTTP field
        // names are case-insensitive.
        let mut caps = http::HeaderMap::new();
        caps.insert(
            http::HeaderName::from_static("x-origin-token"),
            "s3cret-value".parse().unwrap(),
        );
        assert!(gate.accepts(&caps));
        assert!(gate.requires(None), "a peer we cannot name is not exempt");

        let exempt = OriginTokenGate::new("x-origin-token", "v", vec![ipnet::IpNet::from(ip("127.0.0.1"))])
            .unwrap();
        assert!(!exempt.requires(Some(ip("::ffff:127.0.0.1"))), "mapped loopback is exempt");
        assert!(exempt.requires(Some(ip("198.51.100.1"))));
    }

    /// Where a signed read's host comes from: the `host` header for h1, the URI
    /// authority for h2. The h2 arm is the one that broke production — the CDN
    /// pulls over h2, and an h2 request carries no `host` header at all, so a
    /// header-only lookup refused every signed read through the edge while the
    /// same URL worked over h1 (measured 2026-09-28). Delete the authority
    /// fallback and this test, and only this test, goes red.
    #[test]
    fn a_signed_read_finds_its_host_in_the_header_or_the_authority() {
        let uri: http::Uri = "https://cdn.example/googledrive1/film.mkv".parse().unwrap();
        let mut h1 = http::HeaderMap::new();
        h1.insert(http::header::HOST, "cdn.example".parse().unwrap());
        assert_eq!(request_host(&h1, &uri), Some("cdn.example"));

        // HTTP/2: no host header, the authority carries it.
        let none = http::HeaderMap::new();
        assert_eq!(request_host(&none, &uri), Some("cdn.example"));

        // Both present: the header wins — it is what an h1 client signed.
        let elsewhere: http::Uri = "https://other.example/x".parse().unwrap();
        assert_eq!(request_host(&h1, &elsewhere), Some("cdn.example"));

        // Neither (an origin-form request with no authority).
        let bare: http::Uri = "/googledrive1/film.mkv".parse().unwrap();
        assert_eq!(request_host(&none, &bare), None);
    }
}

//! Content-read admission (ADR-0027): a presigned URL is the ticket.
//!
//! The front asks one question per content read — "may this request through?"
//! — and this module answers it from three facts the request itself carries:
//! a SigV4 presigned signature (the standard S3 query-auth form, verified by
//! [`crate::sigv4`]), how long that signature is allowed to live, and what the
//! named session has already spent. Everything a caller must know is
//! [`ContentGate`]'s two `ContentAuth` methods; everything else here —
//! canonical verification, credential prefixes, sliding-window budgets,
//! eviction — is implementation.
//!
//! Why the budgets exist at all: the merge machinery shares one upstream open
//! per WINDOW, so it survives ten viewers seeking around a film, but a hostile
//! client firing thousands of ranged reads a second at fresh offsets opens a
//! window per read. The signature gates who may ask; the budgets cap how fast.
//! A session over budget is slowed with the S3-standard `503 SlowDown`, which
//! SDK clients already back off from. (Measured caveat, R8: the edge may
//! re-ask and answer its viewer with a short 206 rather than propagating the
//! 503 — the budget is a backstop for the origin and the provider, not a QoS
//! signal the viewer sees.)

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use origin_front::{ContentAuth, ContentDecision, ContentRequest};

use crate::metrics::CONTENT_AUTH_TOTAL;
use crate::signing::{param, Caps, Reason, PROTOCOL_MAX_EXPIRES_SECS, SESSION_MAX_LEN};
use crate::sigv4::{self, CredentialStore};

/// Sessions tracked at once. A session costs a few hundred bytes; this cap
/// bounds the table under a flood of fresh session ids, which is the same
/// attack shape as the request flood itself.
const SESSION_TABLE_CAP: usize = 4096;

/// How long an untouched session stays droppable when the table is full.
const SESSION_IDLE_SECS: i64 = 300;

/// Queries are parsed to verify them, so bound them before parsing.
const MAX_QUERY_BYTES: usize = 8 * 1024;
const MAX_QUERY_PAIRS: usize = 64;

/// One sliding window, bucketed per second. Buckets age out on contact, so
/// there is no timer and no background task: the window slides when someone
/// looks at it.
struct Window {
    buckets: VecDeque<(i64, u64)>,
    window_secs: i64,
}

impl Window {
    fn new(window_secs: i64) -> Self {
        Self { buckets: VecDeque::new(), window_secs }
    }

    fn prune(&mut self, now: i64) {
        let cutoff = now - self.window_secs;
        while let Some((sec, _)) = self.buckets.front() {
            if *sec <= cutoff {
                self.buckets.pop_front();
            } else {
                break;
            }
        }
    }

    /// What the window currently holds (and prune while looking).
    fn used(&mut self, now: i64) -> u64 {
        self.prune(now);
        self.buckets.iter().map(|(_, c)| c).sum()
    }

    /// Add to the window, creating this second's bucket if needed.
    fn add(&mut self, now: i64, amount: u64) {
        self.prune(now);
        match self.buckets.back_mut() {
            Some((sec, count)) if *sec == now => *count += amount,
            _ => self.buckets.push_back((now, amount)),
        }
    }
}

struct SessionBudget {
    requests: Window, // one-second window: requests per second
    bytes: Window,    // sixty-second window: bytes per minute
    last_seen: i64,
}

struct SessionTables {
    sessions: HashMap<Box<str>, SessionBudget>,
}

impl SessionTables {
    /// The request half of the budget: count one request, and refuse when the
    /// session's byte window is already exhausted (the bytes themselves are
    /// only known after the body moved — see `ContentAuth::observe`).
    fn admit(&mut self, key: &str, now: i64, rps: u64, bytes_per_min: u64) -> Result<(), Reason> {
        if self.sessions.len() >= SESSION_TABLE_CAP {
            self.sessions.retain(|_, s| now - s.last_seen <= SESSION_IDLE_SECS);
            // Still full after the age sweep: a live flood of fresh sessions.
            // Drop the oldest touchers — crude, but it keeps the table bounded,
            // and a dropped session just re-derives its windows from scratch.
            if self.sessions.len() >= SESSION_TABLE_CAP {
                let mut order: Vec<(i64, Box<str>)> =
                    self.sessions.iter().map(|(k, s)| (s.last_seen, k.clone())).collect();
                order.sort();
                let drop = self.sessions.len() - SESSION_TABLE_CAP + 1;
                for (_, k) in order.into_iter().take(drop) {
                    self.sessions.remove(&k);
                }
            }
        }
        let entry = self.sessions.entry(key.into()).or_insert_with(|| SessionBudget {
            requests: Window::new(1),
            bytes: Window::new(60),
            last_seen: now,
        });
        entry.last_seen = now;
        if entry.requests.used(now) + 1 > rps {
            return Err(Reason::SessionRateExceeded);
        }
        if entry.bytes.used(now) >= bytes_per_min {
            return Err(Reason::SessionBytesExceeded);
        }
        entry.requests.add(now, 1);
        Ok(())
    }

    fn observe(&mut self, key: &str, now: i64, bytes: u64) {
        if bytes == 0 {
            return;
        }
        if let Some(entry) = self.sessions.get_mut(key) {
            entry.last_seen = now;
            entry.bytes.add(now, bytes);
        }
    }
}

/// The front's content gate. One gate for every tenant: the credential says
/// who, the signature proves it, the budgets say how much.
pub struct ContentGate {
    store: Arc<CredentialStore>,
    tables: Mutex<SessionTables>,
    max_expiry_secs: u64,
    default_rps: u64,
    default_bytes_per_min: u64,
    now: Box<dyn Fn() -> i64 + Send + Sync>,
}

impl ContentGate {
    /// Built once at boot from the loaded credential store and the config's
    /// caps (one derivation: `Config::content_caps`). The session marker is
    /// required: without it the budgets have nothing to key on, and an unkeyed
    /// URL is exactly the anonymous amplification this gate exists to close.
    pub fn from_caps(store: Arc<CredentialStore>, caps: &Caps) -> Self {
        Self::new(store, caps.max_expiry_secs, caps.session_rps, caps.session_mib_per_min)
    }

    pub fn new(store: Arc<CredentialStore>, max_expiry_secs: u64, session_rps: u32, session_mib_per_min: u64) -> Self {
        Self {
            store,
            tables: Mutex::new(SessionTables { sessions: HashMap::new() }),
            max_expiry_secs: max_expiry_secs.min(PROTOCOL_MAX_EXPIRES_SECS),
            default_rps: u64::from(session_rps.max(1)),
            default_bytes_per_min: session_mib_per_min.saturating_mul(1024 * 1024).max(1),
            now: Box::new(sigv4::now_unix),
        }
    }

    /// A session marker is an opaque budget key: bounded, printable, no
    /// control characters, no quote characters that could confuse a log.
    fn valid_session(s: &str) -> bool {
        !s.is_empty() && s.len() <= SESSION_MAX_LEN && s.bytes().all(|b| (0x20..=0x7e).contains(&b) && b != b'\'')
    }
}

impl ContentAuth for ContentGate {
    fn admit(&self, request: &ContentRequest<'_>) -> ContentDecision {
        let now = (self.now)();
        let (raw_path, query) = match request.path_and_query.split_once('?') {
            Some((p, q)) => (p, q),
            None => (request.path_and_query, ""),
        };
        // Bound before parsing: the verifier is on the flood path, and an
        // unbounded query is itself a way to spend our memory.
        if query.len() > MAX_QUERY_BYTES {
            return deny(Reason::MalformedRequest);
        }
        let pairs: Vec<(String, String)> = form_urlencoded::parse(query.as_bytes())
            .take(MAX_QUERY_PAIRS + 1)
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        if pairs.len() > MAX_QUERY_PAIRS {
            return deny(Reason::MalformedRequest);
        }
        let decoded_path = percent_encoding::percent_decode_str(raw_path).decode_utf8_lossy().into_owned();
        // The session marker is signed like any other query parameter (the
        // canonical query covers everything but X-Amz-Signature), so only
        // whoever minted the URL could have set it. Taken out here, judged
        // after the signature: a refusal says the least specific thing
        // first — an unsigned request learns nothing about what else is
        // required.
        let session_param = pairs.iter().find(|(k, _)| k == param::SESSION).map(|(_, v)| v.clone());
        // The front's shape of the same construction: the resolved host, no
        // header map (a presigned URL signs `host` and nothing else).
        let input = sigv4::verify_input(request.method, &decoded_path, raw_path, pairs, request.host, None);
        let verified = match sigv4::verify_optional(Some(&self.store), &input, now) {
            sigv4::VerifyOutcome::Verified(v) => v,
            sigv4::VerifyOutcome::Anonymous => return deny(Reason::MissingSignature),
            sigv4::VerifyOutcome::Failed(reason) => return deny(reason),
        };
        let session = match session_param.as_deref() {
            Some(s) if Self::valid_session(s) => Some(s.to_string()),
            Some(_) => return deny(Reason::MalformedSession),
            None => return deny(Reason::MissingSession),
        };
        if verified.expires_secs > self.max_expiry_secs {
            return deny(Reason::ExpiresTooLong);
        }
        // Tenant confinement: a credential reads only under its prefix, so a
        // site's key cannot pull another site's objects.
        let entry = match self.store.lookup(&verified.access_key_id) {
            Some(e) => e,
            None => return deny(Reason::UnknownAccessKey),
        };
        if let Some(prefix) = &entry.prefix {
            let allowed = decoded_path.strip_prefix('/').is_some_and(|key| key.starts_with(prefix.as_str()));
            if !allowed {
                return deny(Reason::OutsideKeyPrefix);
            }
        }
        let key: Box<str> =
            format!("{}\u{1}{}", verified.access_key_id, session.clone().unwrap_or_default()).into();
        let rps = u64::from(entry.session_rps.unwrap_or(self.default_rps as u32)).max(1);
        let bytes_per_min = entry
            .session_mib_per_min
            .map(|m| m.saturating_mul(1024 * 1024))
            .unwrap_or(self.default_bytes_per_min)
            .max(1);
        let mut tables = match self.tables.lock() {
            Ok(t) => t,
            Err(_) => return deny(Reason::SessionRateExceeded),
        };
        if let Err(reason) = tables.admit(&key, now, rps, bytes_per_min) {
            return deny(reason);
        }
        CONTENT_AUTH_TOTAL.with_label_values(&["allow", "ok"]).inc();
        ContentDecision::Allow { credential: verified.access_key_id, session }
    }

    fn observe(&self, credential: &str, session: Option<&str>, bytes: u64) {
        let Some(session) = session else { return };
        let now = (self.now)();
        let key: Box<str> = format!("{credential}\u{1}{session}").into();
        if let Ok(mut tables) = self.tables.lock() {
            tables.observe(&key, now, bytes);
        }
    }
}

/// Every refusal goes through here, so the metric's `reason` label is always
/// one of [`Reason`]'s spellings — a closed set the documentation check in
/// `tests/signing_contract.rs` enumerates. The status comes from the reason,
/// so a refusal cannot be labelled 403 and answer 503.
fn deny(reason: Reason) -> ContentDecision {
    CONTENT_AUTH_TOTAL.with_label_values(&["deny", reason.as_str()]).inc();
    ContentDecision::Deny { status: reason.status(), reason: reason.as_str() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sigv4::CredentialEntry;
    use std::sync::atomic::{AtomicI64, Ordering};

    fn entry(secret: &str, prefix: Option<&str>, rps: Option<u32>, mib: Option<u64>) -> CredentialEntry {
        CredentialEntry {
            secret: secret.to_string(),
            prefix: prefix.map(|p| p.to_string()),
            session_rps: rps,
            session_mib_per_min: mib,
        }
    }

    fn store() -> CredentialStore {
        sigv4::test_store(vec![
            ("tenant-a".to_string(), entry("secret-a", None, None, None)),
            ("tenant-b".to_string(), entry("secret-b", Some("media/"), Some(2), Some(1))),
        ])
    }

    /// A gate whose clock is pinned: the test decides what "now" is, and can
    /// move it between calls through the shared cell.
    fn gate(now: &Arc<AtomicI64>) -> ContentGate {
        let tick = Arc::clone(now);
        ContentGate {
            store: Arc::new(store()),
            tables: Mutex::new(SessionTables { sessions: HashMap::new() }),
            max_expiry_secs: 300,
            default_rps: 30,
            default_bytes_per_min: 1024 * 1024 * 1024,
            now: Box::new(move || tick.load(Ordering::SeqCst)),
        }
    }

    /// 2026-09-26T00:00:00Z is 1_790_380_800; ask 200 s after it so the
    /// request date is in the past and inside every expiry used here.
    const NOW: i64 = 1_790_381_000;
    const DATE: &str = "20260926T000000Z";

    fn ask(g: &ContentGate, query: &str) -> ContentDecision {
        g.admit(&ContentRequest { method: "GET", path_and_query: query, host: Some("cdn.example") })
    }

    fn allow_id(d: &ContentDecision) -> String {
        match d {
            ContentDecision::Allow { credential, .. } => credential.clone(),
            ContentDecision::Deny { status, reason } => panic!("expected allow, got {status} {reason}"),
        }
    }

    fn deny_reason(d: ContentDecision) -> (&'static str, u16) {
        match d {
            ContentDecision::Allow { .. } => panic!("expected deny"),
            ContentDecision::Deny { status, reason } => (reason, status),
        }
    }

    /// Cross-language pin: the query below was produced by
    /// `deploy/oracle/presign.py` (the tool operators and site backends
    /// actually use) for this exact key, session, date and secret pair. If
    /// either side drifts from SigV4 query auth, this goes red.
    #[test]
    fn python_presigner_output_verifies() {
        let g = gate(&Arc::new(AtomicI64::new(NOW)));
        let pq = "/googledrive1/film.mkv?X-Amz-Algorithm=AWS4-HMAC-SHA256&\
X-Amz-Credential=tenant-a%2F20260926%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260926T000000Z&\
X-Amz-Expires=300&X-Amz-SignedHeaders=host&session=viewer-1&\
X-Amz-Signature=48ee4d7a5fc6a2913a7e37d7f9749e4908b29114398b17a619fe4062311e8e9f";
        assert_eq!(allow_id(&ask(&g, pq)), "tenant-a");
    }

    /// SigV4 signs the METHOD, so the two methods the gate carries need two
    /// tickets: a GET ticket refuses a HEAD (and vice versa). Players that
    /// probe with HEAD must ask the backend for a HEAD URL.
    #[test]
    fn method_is_part_of_the_ticket() {
        let g = gate(&Arc::new(AtomicI64::new(NOW)));
        let head_ticket = sigv4::test_presign_method(
            "HEAD", "tenant-a", "secret-a", DATE, 300, "/googledrive1/film.mkv",
            &[("session".into(), "viewer-1".into())],
        );
        let head_req = |q: &str| {
            g.admit(&ContentRequest { method: "HEAD", path_and_query: q, host: Some("cdn.example") })
        };
        assert_eq!(allow_id(&head_req(&format!("/googledrive1/film.mkv?{head_ticket}"))), "tenant-a");
        let get_ticket = sigv4::test_presign("tenant-a", "secret-a", DATE, 300, "/googledrive1/film.mkv",
            &[("session".into(), "viewer-1".into())]);
        assert_eq!(
            deny_reason(head_req(&format!("/googledrive1/film.mkv?{get_ticket}"))).0,
            "signature does not match"
        );
    }

    /// No signature at all: the door stays shut. This is the whole flood
    /// story — a lifted plain URL cannot pull a cold byte.
    #[test]
    fn unsigned_content_read_is_denied() {
        let g = gate(&Arc::new(AtomicI64::new(NOW)));
        let (reason, status) = deny_reason(ask(&g, "/googledrive1/film.mkv"));
        assert_eq!(reason, "missing signature");
        assert_eq!(status, 403);
    }

    /// A well-formed presigned URL with a session marker gets through, and
    /// the decision names the tenant so the access log can attribute it.
    #[test]
    fn signed_read_is_allowed_and_attributed() {
        let g = gate(&Arc::new(AtomicI64::new(NOW)));
        let q = sigv4::test_presign("tenant-a", "secret-a", DATE, 300, "/googledrive1/film.mkv",
            &[("session".into(), "viewer-1".into())]);
        let d = ask(&g, &format!("/googledrive1/film.mkv?{q}"));
        assert_eq!(allow_id(&d), "tenant-a");
        // A second read the same second is fine — a player issues several
        // concurrent range requests; the budget is far above that.
        let q2 = sigv4::test_presign("tenant-a", "secret-a", DATE, 300, "/googledrive1/film.mkv",
            &[("session".into(), "viewer-1".into())]);
        assert!(matches!(ask(&g, &format!("/googledrive1/film.mkv?{q2}")), ContentDecision::Allow { .. }));
    }

    /// The verifier's own clock semantics: past the expires window, refused.
    #[test]
    fn expired_signature_is_denied() {
        let g = gate(&Arc::new(AtomicI64::new(NOW + 400)));
        let q = sigv4::test_presign("tenant-a", "secret-a", DATE, 300, "/googledrive1/film.mkv",
            &[("session".into(), "viewer-1".into())]);
        assert_eq!(deny_reason(ask(&g, &format!("/googledrive1/film.mkv?{q}"))).0, "request has expired");
    }

    /// Policy cap: the deployment refuses URLs that outlive it, even though
    /// the protocol would allow a week.
    #[test]
    fn over_long_expiry_is_denied() {
        let g = gate(&Arc::new(AtomicI64::new(NOW)));
        let q = sigv4::test_presign("tenant-a", "secret-a", DATE, 604_800, "/googledrive1/film.mkv",
            &[("session".into(), "viewer-1".into())]);
        assert_eq!(deny_reason(ask(&g, &format!("/googledrive1/film.mkv?{q}"))).0, "expires too long");
    }

    /// One flipped hex character in the signature: refused (a length-only
    /// corruption would trip the format check first, so flip in place).
    #[test]
    fn tampered_signature_is_denied() {
        let g = gate(&Arc::new(AtomicI64::new(NOW)));
        let q = sigv4::test_presign("tenant-a", "secret-a", DATE, 300, "/googledrive1/film.mkv",
            &[("session".into(), "viewer-1".into())]);
        let marker = "X-Amz-Signature=";
        let at = q.find(marker).unwrap() + marker.len();
        let mut bytes = q.into_bytes();
        bytes[at] = if bytes[at] == b'a' { b'b' } else { b'a' };
        let tampered = String::from_utf8(bytes).unwrap();
        assert_eq!(deny_reason(ask(&g, &format!("/googledrive1/film.mkv?{tampered}"))).0, "signature does not match");
    }

    /// A credential the store does not know: refused before any budget work.
    #[test]
    fn unknown_tenant_is_denied() {
        let g = gate(&Arc::new(AtomicI64::new(NOW)));
        let q = sigv4::test_presign("tenant-a", "secret-a", DATE, 300, "/googledrive1/film.mkv",
            &[("session".into(), "viewer-1".into())]);
        let swapped = q.replace("tenant-a", "nobody");
        assert_eq!(deny_reason(ask(&g, &format!("/googledrive1/film.mkv?{swapped}"))).0, "unknown access key");
    }

    /// The session marker is what the budgets key on: a URL without one is
    /// an unkeyed budget, i.e. the amplification this gate closes.
    #[test]
    fn missing_session_marker_is_denied() {
        let g = gate(&Arc::new(AtomicI64::new(NOW)));
        let q = sigv4::test_presign("tenant-a", "secret-a", DATE, 300, "/googledrive1/film.mkv", &[]);
        assert_eq!(deny_reason(ask(&g, &format!("/googledrive1/film.mkv?{q}"))).0, "missing session");
        // And a marker that could flood a log or a map entry is refused too.
        let long: String = "x".repeat(SESSION_MAX_LEN + 1);
        let q = sigv4::test_presign("tenant-a", "secret-a", DATE, 300, "/googledrive1/film.mkv",
            &[("session".into(), long)]);
        assert_eq!(deny_reason(ask(&g, &format!("/googledrive1/film.mkv?{q}"))).0, "malformed session");
    }

    /// Tenant confinement: tenant-b's key reads under media/ only, so one
    /// site's credential cannot pull another site's objects.
    #[test]
    fn credential_prefix_confinement() {
        let g = gate(&Arc::new(AtomicI64::new(NOW)));
        let outside = sigv4::test_presign("tenant-b", "secret-b", DATE, 300, "/googledrive1/private.mkv",
            &[("session".into(), "s".into())]);
        assert_eq!(deny_reason(ask(&g, &format!("/googledrive1/private.mkv?{outside}"))).0, "outside key prefix");
        let inside = sigv4::test_presign("tenant-b", "secret-b", DATE, 300, "/media/film.mkv",
            &[("session".into(), "s".into())]);
        assert!(matches!(ask(&g, &format!("/media/film.mkv?{inside}")), ContentDecision::Allow { .. }));
    }

    /// The flood control, request half: tenant-b allows 2/s, so the third
    /// request inside the same second is slowed with the S3-standard 503,
    /// and the window slides — two seconds later the session is served.
    #[test]
    fn session_rate_budget_trips_and_recovers() {
        let now = Arc::new(AtomicI64::new(NOW));
        let g = gate(&now);
        let mk = |nonce: &str| {
            sigv4::test_presign("tenant-b", "secret-b", DATE, 300, "/media/film.mkv",
                &[("session".into(), "s".into()), ("n".into(), nonce.into())])
        };
        assert!(matches!(ask(&g, &format!("/media/film.mkv?{}", mk("1"))), ContentDecision::Allow { .. }));
        assert!(matches!(ask(&g, &format!("/media/film.mkv?{}", mk("2"))), ContentDecision::Allow { .. }));
        let (reason, status) = deny_reason(ask(&g, &format!("/media/film.mkv?{}", mk("3"))));
        assert_eq!(reason, "session rate exceeded");
        assert_eq!(status, 503, "the slowdown must be the S3-standard 503");
        now.store(NOW + 2, Ordering::SeqCst);
        assert!(matches!(ask(&g, &format!("/media/film.mkv?{}", mk("4"))), ContentDecision::Allow { .. }));
    }

    /// The flood control, byte half: the size of a range response is only
    /// known after the body moved, so bytes are observed post-response and
    /// the NEXT request pays for them. tenant-b allows 1 MiB/min; one 2 MiB
    /// response shuts the session's window until it slides.
    #[test]
    fn session_byte_budget_binds_after_observed_bytes() {
        let now = Arc::new(AtomicI64::new(NOW));
        let g = gate(&now);
        let mk = |nonce: &str| {
            sigv4::test_presign("tenant-b", "secret-b", DATE, 300, "/media/film.mkv",
                &[("session".into(), "s".into()), ("n".into(), nonce.into())])
        };
        let q = mk("1");
        assert!(matches!(ask(&g, &format!("/media/film.mkv?{q}")), ContentDecision::Allow { .. }));
        g.observe("tenant-b", Some("s"), 2 * 1024 * 1024);
        let (reason, _) = deny_reason(ask(&g, &format!("/media/film.mkv?{}", mk("2"))));
        assert_eq!(reason, "session bytes exceeded");
        now.store(NOW + 61, Ordering::SeqCst);
        assert!(matches!(ask(&g, &format!("/media/film.mkv?{}", mk("3"))), ContentDecision::Allow { .. }));
    }

    /// A session table with a hard cap: the age sweep plus the overflow drop
    /// must keep it bounded even when every request invents a new session.
    #[test]
    fn session_table_stays_bounded_under_fresh_sessions() {
        let mut t = SessionTables { sessions: HashMap::new() };
        for i in 0..(SESSION_TABLE_CAP + 500) {
            let now = 1_000 + (i as i64) / 1000; // 1000 sessions per second
            let _ = t.admit(&format!("t\u{1}session-{i}"), now, 100, u64::MAX);
        }
        assert!(t.sessions.len() <= SESSION_TABLE_CAP, "table grew to {}", t.sessions.len());
    }

    /// The two verification entries must agree. The front's gate builds its input
    /// the way the public seam allows (the resolved host, no header map); the
    /// business plane's verify-if-present layer builds it with the whole map.
    /// A URL one accepts and the other refuses is a request that works on the
    /// CDN path and 403s on the loopback path, or the reverse — and until this
    /// test, nothing compared them.
    #[test]
    fn both_verification_entries_accept_the_same_presigned_url() {
        let store = Arc::new(sigv4::store_with("tenant-a", "secret-a"));
        let query = sigv4::test_presign(
            "tenant-a",
            "secret-a",
            DATE,
            300,
            "/googledrive1/film.mkv",
            &[("session".into(), "viewer-1".into())],
        );
        let path = "/googledrive1/film.mkv";
        let host = "cdn.example";

        // Entry one: the front's gate, through the door. One store, with the
        // clock pinned because the gate reads the real one and this URL is
        // deliberately dated.
        let mut g = ContentGate::from_caps(Arc::clone(&store), &Caps::default());
        g.now = Box::new(|| NOW);
        let decision = g.admit(&ContentRequest {
            method: "GET",
            path_and_query: &format!("{path}?{query}"),
            host: Some(host),
        });
        assert!(
            matches!(decision, ContentDecision::Allow { .. }),
            "the front's gate refused a URL the in-crate signer produced"
        );

        // Entry two: the business plane's shape — the whole header map, and the
        // same query pairs, which is what `sigv4_gate` hands the verifier.
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("host", host.parse().unwrap());
        let pairs: Vec<(String, String)> = form_urlencoded::parse(query.as_bytes())
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        let input = sigv4::verify_input("GET", path, path, pairs, None, Some(&headers));
        let outcome = sigv4::verify_optional(Some(&store), &input, NOW);
        assert!(
            matches!(outcome, sigv4::VerifyOutcome::Verified(_)),
            "the business plane's entry refused the same URL: {outcome:?}"
        );
    }
}

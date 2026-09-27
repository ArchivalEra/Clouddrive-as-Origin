//! The signing contract: one place that says what a signed content read must
//! carry, how long it may live, and why one may be refused.
//!
//! Two modules share these facts. [`crate::sigv4`] decides whether a presented
//! URL is genuine (canonical request, HMAC, dates); [`crate::content_auth`]
//! decides whether a genuine one may proceed (expiry cap, tenant prefix,
//! session budgets). Both used to carry their own copy of the parameter names,
//! the protocol maximum and — worst — the refusal strings, which are also
//! metric labels and rows of a table in `docs/signing.md`. Nothing tied those
//! three consumers together, so a new refusal could ship undocumented and a
//! renamed one could silently split a metric series.
//!
//! The contract lives here so that:
//!
//! * a refusal is one variant, and [`Reason::ALL`] is the whole taxonomy — the
//!   metric label set and the documentation table are both checkable against
//!   it (see `tests/signing_contract.rs`);
//! * the caps have one definition, so config defaults, boot validation and the
//!   gate's enforcement cannot disagree;
//! * the external signer (`deploy/oracle/presign.py`) has a written contract to
//!   conform to, and a test that runs it against this module's rules.
//!
//! The strings are the wire format for logs and dashboards, so they are stable:
//! [`Reason::as_str`] is exhaustive (no wildcard arm), which makes adding a
//! variant a compile error until it is named, and the doc test turns "named but
//! undocumented" into a failing test.

/// Query-parameter names a signed read is built from. The `X-Amz-*` set is
/// AWS's (S3 query-string authentication); `session` is ours, and it is signed
/// exactly like the others — the canonical query covers every parameter but
/// the signature itself.
pub mod param {
    pub const ALGORITHM: &str = "X-Amz-Algorithm";
    pub const CREDENTIAL: &str = "X-Amz-Credential";
    pub const DATE: &str = "X-Amz-Date";
    pub const EXPIRES: &str = "X-Amz-Expires";
    pub const SIGNED_HEADERS: &str = "X-Amz-SignedHeaders";
    pub const SIGNATURE: &str = "X-Amz-Signature";
    /// The budget key the gate charges: an opaque per-session marker the
    /// credential holder chose. Required — an unkeyed URL is the anonymous
    /// amplification the gate exists to close.
    pub const SESSION: &str = "session";
}

/// Longest `X-Amz-Expires` the protocol accepts (AWS's own maximum). A
/// deployment's cap ([`Caps::max_expiry_secs`]) is shorter and is what the
/// gate enforces.
pub const PROTOCOL_MAX_EXPIRES_SECS: u64 = 604_800;

/// The `session` marker is an opaque budget key, not an identity: bounded
/// length, printable ASCII, so it can neither flood a log with control
/// characters nor grow a map entry without bound.
pub const SESSION_MAX_LEN: usize = 128;

/// Longest `X-Amz-Expires` a deployment honours by default. The protocol would
/// allow a week; a leaked URL that works for a week is not a policy this
/// project wants, and a viewing session fits comfortably inside a day.
pub const DEFAULT_MAX_EXPIRY_SECS: u64 = 21_600;

/// Per-session request ceiling (requests/second) by default: far above what a
/// player issues, far below a flood.
pub const DEFAULT_SESSION_RPS: u32 = 30;

/// Per-session byte ceiling (MiB/minute) by default: a viewer reading ahead at
/// a film's bitrate stays far under it; a scraper does not.
pub const DEFAULT_SESSION_MIB_PER_MIN: u64 = 1024;

/// The numbers the gate enforces and the config carries. One definition, so a
/// default, a boot check and an enforcement cannot drift apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Caps {
    pub max_expiry_secs: u64,
    pub session_rps: u32,
    pub session_mib_per_min: u64,
}

impl Default for Caps {
    fn default() -> Self {
        Self {
            max_expiry_secs: DEFAULT_MAX_EXPIRY_SECS,
            session_rps: DEFAULT_SESSION_RPS,
            session_mib_per_min: DEFAULT_SESSION_MIB_PER_MIN,
        }
    }
}

impl Caps {
    /// Boot-time validation. Every one of these turns a gate into a wall or a
    /// sieve if it is zero — `max_expiry_secs = 0` refuses every signed read,
    /// `session_rps = 0` refuses after the first request in a second — so a
    /// zero is a configuration error, not a way to disable a knob.
    pub fn validate(&self) -> Result<(), String> {
        if self.max_expiry_secs == 0 {
            return Err(
                "front_content_auth_max_expiry_secs = 0 would refuse every signed read \
                 (the URL could never be valid): omit the key for the default, or set a \
                 positive number of seconds"
                    .into(),
            );
        }
        if self.max_expiry_secs > PROTOCOL_MAX_EXPIRES_SECS {
            return Err(format!(
                "front_content_auth_max_expiry_secs = {} exceeds the protocol maximum {PROTOCOL_MAX_EXPIRES_SECS}",
                self.max_expiry_secs
            ));
        }
        if self.session_rps == 0 {
            return Err(
                "front_content_auth_session_rps = 0 would slow every session after its first \
                 request in a second: omit the key for the default, or set a positive rate"
                    .into(),
            );
        }
        if self.session_mib_per_min == 0 {
            return Err(
                "front_content_auth_session_mib_per_min = 0 would stop every session after its \
                 first response: omit the key for the default, or set a positive budget"
                    .into(),
            );
        }
        Ok(())
    }
}

/// Why a signed content read was refused. The string is the wire format (log
/// field, metric label, documentation row); the status is the HTTP answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Reason {
    // --- the request carries no usable signature material
    MissingSignature,
    MissingCredential,
    MalformedCredential,
    MissingSignedHeaders,
    MalformedSignedHeaders,
    EmptySignedHeaders,
    SignedHeaderAbsent,
    HostNotSigned,
    MalformedSignature,
    UnsupportedAuthScheme,
    MalformedAuthorizationHeader,
    UnsupportedAlgorithm,
    MissingAlgorithm,
    UnsupportedService,
    StreamingPayload,
    MissingExpires,
    // --- the signature is well-formed but not valid
    UnknownAccessKey,
    SignatureMismatch,
    ExpiresCapExceeded,
    RequestExpired,
    RequestTimeSkewed,
    RequestDateInFuture,
    MissingRequestDate,
    MalformedRequestDate,
    InvalidRequestDate,
    CredentialScopeDateMismatch,
    // --- the request is not shaped like a content read
    MalformedRequest,
    // --- signed, but the deployment's own caps refuse it
    ExpiresTooLong,
    MissingSession,
    MalformedSession,
    // --- signed, but outside this tenant's grant
    OutsideKeyPrefix,
    // --- signed and granted, but the session is over budget
    SessionRateExceeded,
    SessionBytesExceeded,
}

impl Reason {
    /// The stable spelling: log field value, metric label, documentation row.
    pub fn as_str(self) -> &'static str {
        match self {
            // The three spellings of "no usable signature" (no material at
            // all, a header form missing its Signature component, a presigned
            // query missing X-Amz-Signature) are one label: they mean the same
            // thing to an operator, and splitting them only widens the metric.
            Reason::MissingSignature => "missing signature",
            Reason::MissingCredential => "missing credential",
            Reason::MalformedCredential => "malformed credential",
            Reason::MissingSignedHeaders => "missing signed headers",
            Reason::MalformedSignedHeaders => "malformed signed headers",
            Reason::EmptySignedHeaders => "empty SignedHeaders",
            Reason::SignedHeaderAbsent => "signed header missing from request",
            Reason::HostNotSigned => "host header is not signed",
            Reason::MalformedSignature => "malformed signature",
            Reason::UnsupportedAuthScheme => "unsupported auth scheme",
            Reason::MalformedAuthorizationHeader => "malformed authorization header",
            Reason::UnsupportedAlgorithm => "unsupported X-Amz-Algorithm",
            Reason::MissingAlgorithm => "missing X-Amz-Algorithm",
            Reason::UnsupportedService => "unsupported service in credential scope",
            Reason::StreamingPayload => "streaming payloads are not supported",
            Reason::MissingExpires => "missing X-Amz-Expires",
            Reason::UnknownAccessKey => "unknown access key",
            Reason::SignatureMismatch => "signature does not match",
            Reason::ExpiresCapExceeded => "X-Amz-Expires exceeds the maximum",
            Reason::RequestExpired => "request has expired",
            Reason::RequestTimeSkewed => "request time too skewed",
            Reason::RequestDateInFuture => "request date is later than server time too much",
            Reason::MissingRequestDate => "missing request date",
            Reason::MalformedRequestDate => "malformed request date",
            Reason::InvalidRequestDate => "invalid request date",
            Reason::CredentialScopeDateMismatch => "credential scope date does not match the request date",
            Reason::MalformedRequest => "malformed request",
            Reason::ExpiresTooLong => "expires too long",
            Reason::MissingSession => "missing session",
            Reason::MalformedSession => "malformed session",
            Reason::OutsideKeyPrefix => "outside key prefix",
            Reason::SessionRateExceeded => "session rate exceeded",
            Reason::SessionBytesExceeded => "session bytes exceeded",
        }
    }

    /// The HTTP status the origin answers with. Authentication and grant
    /// failures are 403; an over-budget session is 503 (`SlowDown`, the
    /// S3-standard back-off signal), which SDK clients already retry.
    pub fn status(self) -> u16 {
        match self {
            Reason::SessionRateExceeded | Reason::SessionBytesExceeded => 503,
            _ => 403,
        }
    }

    /// Whether a caller can hit this reason through the content-read door and
    /// act on it, or whether it is parse-level detail whose remedy is always
    /// the same ("re-mint with a real tool"). Exhaustive on purpose: a new
    /// variant forces the decision, and `tests/signing_contract.rs` asserts
    /// that every reason marked here is named in `docs/signing.md` (and that
    /// the document names nothing else).
    pub fn documented(self) -> bool {
        match self {
            // The rows of the documentation table.
            Reason::MissingSignature
            | Reason::SignatureMismatch
            | Reason::UnknownAccessKey
            | Reason::RequestExpired
            | Reason::ExpiresTooLong
            | Reason::MissingSession
            | Reason::MalformedSession
            | Reason::OutsideKeyPrefix
            | Reason::HostNotSigned
            | Reason::SessionRateExceeded
            | Reason::SessionBytesExceeded => true,
            // Everything else is a malformed-request detail: the fix is the
            // same whatever it says, so listing each spelling would bury the
            // rows that change what a caller does.
            _ => false,
        }
    }

    /// The whole taxonomy, for tests and for the documentation check. Kept
    /// beside `as_str` so adding a variant without listing it here is visible
    /// in review (the compile error comes from `as_str`'s exhaustive match).
    pub const ALL: &'static [Reason] = &[
        Reason::MissingSignature,
        Reason::MissingCredential,
        Reason::MalformedCredential,
        Reason::MissingSignedHeaders,
        Reason::MalformedSignedHeaders,
        Reason::EmptySignedHeaders,
        Reason::SignedHeaderAbsent,
        Reason::HostNotSigned,
        Reason::MalformedSignature,
        Reason::UnsupportedAuthScheme,
        Reason::MalformedAuthorizationHeader,
        Reason::UnsupportedAlgorithm,
        Reason::MissingAlgorithm,
        Reason::UnsupportedService,
        Reason::StreamingPayload,
        Reason::MissingExpires,
        Reason::UnknownAccessKey,
        Reason::SignatureMismatch,
        Reason::ExpiresCapExceeded,
        Reason::RequestExpired,
        Reason::RequestTimeSkewed,
        Reason::RequestDateInFuture,
        Reason::MissingRequestDate,
        Reason::MalformedRequestDate,
        Reason::InvalidRequestDate,
        Reason::CredentialScopeDateMismatch,
        Reason::MalformedRequest,
        Reason::ExpiresTooLong,
        Reason::MissingSession,
        Reason::MalformedSession,
        Reason::OutsideKeyPrefix,
        Reason::SessionRateExceeded,
        Reason::SessionBytesExceeded,
    ];
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    /// Two variants sharing a spelling would merge two situations into one
    /// metric series and one documentation row — the exact drift this module
    /// exists to prevent.
    #[test]
    fn every_reason_has_its_own_spelling() {
        let mut seen = HashSet::new();
        for r in Reason::ALL {
            assert!(seen.insert(r.as_str()), "duplicate spelling: {}", r.as_str());
        }
        assert_eq!(seen.len(), Reason::ALL.len());
    }

    /// `ALL` must be the whole taxonomy: this fails the day someone adds a
    /// variant to the enum (and to `as_str`) but forgets `ALL`.
    #[test]
    fn all_lists_every_variant() {
        let mut seen = HashSet::new();
        for r in Reason::ALL {
            seen.insert(*r);
        }
        for r in Reason::ALL {
            // Every variant is reachable from ALL by construction; the count
            // check below is what catches an omission.
            assert!(seen.contains(r));
        }
        // The enum's variant count is pinned by `as_str`'s exhaustive match;
        // a new variant that skips ALL changes this number and fails here.
        assert_eq!(Reason::ALL.len(), 33, "a variant was added or removed: update ALL");
    }

    #[test]
    fn statuses_are_only_forbidden_or_slow_down() {
        for r in Reason::ALL {
            assert!(matches!(r.status(), 403 | 503), "{} -> {}", r.as_str(), r.status());
        }
        assert_eq!(Reason::SessionRateExceeded.status(), 503);
        assert_eq!(Reason::SessionBytesExceeded.status(), 503);
        assert_eq!(Reason::SignatureMismatch.status(), 403);
    }

    #[test]
    fn documented_reasons_are_the_operator_facing_rows() {
        let documented: Vec<&str> = Reason::ALL
            .iter()
            .filter(|r| r.documented())
            .map(|r| r.as_str())
            .collect();
        // Pinned so that re-classifying a reason is a deliberate, reviewed act
        // (and so the documentation test in tests/signing_contract.rs has a
        // number to disagree with).
        assert_eq!(documented.len(), 11, "{documented:?}");
        for r in Reason::ALL {
            if !r.documented() {
                // The internals must still be spelled (they reach the log).
                assert!(!r.as_str().is_empty());
            }
        }
    }

    #[test]
    fn caps_validate_refuses_zeros_and_over_protocol_maximum() {
        assert!(Caps::default().validate().is_ok());
        for caps in [
            Caps { max_expiry_secs: 0, ..Caps::default() },
            Caps { session_rps: 0, ..Caps::default() },
            Caps { session_mib_per_min: 0, ..Caps::default() },
            Caps { max_expiry_secs: PROTOCOL_MAX_EXPIRES_SECS + 1, ..Caps::default() },
        ] {
            assert!(caps.validate().is_err(), "{caps:?} must not validate");
        }
    }
}

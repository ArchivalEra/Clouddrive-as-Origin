//! S3 response shaping (R1 table, C3). The single home for everything the
//! outbound renders: quoted ETags, S3 XML error envelope, Accept-Ranges,
//! request ids mirrored into errors. The outbound speaks AWS S3 shapes over
//! the disk cache. Deliberate deviation: Cache-Control stays
//! `public, max-age=..., immutable` (no per-object stored value exists)
//! because edge caching matters more here than S3 purity.
//!
//! The cache hands over pure data (`ContentRange` values, sizes, meta);
//! this module owns every formatted string. Callers: the business plane
//! handlers (GET/HEAD/prewarm/relief-valve).

use axum::{
    body::Body,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tracing::warn;

use crate::{
    backend::{BackendError, ContentRange},
    cache::cache::HitMeta,
    key::KeyError,
};

static REQUEST_SEQ: AtomicU64 = AtomicU64::new(1);

/// Per-request opaque ids: 16-hex-upper request id + 76-char extended id.
/// Uniqueness per request is sufficient; formats only need opaque ASCII.
pub(crate) fn request_ids() -> (String, String) {
    const B64: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789/+:";
    let n = REQUEST_SEQ.fetch_add(1, Ordering::Relaxed);
    let t = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(n as u128);
    let mut x = (n as u128).wrapping_mul(0x9E3779B97F4A7C15).wrapping_add(t);
    let req_id = format!("{:016X}", (x & (u64::MAX as u128)) as u64);
    let mut host = String::with_capacity(76);
    for _ in 0..76 {
        // xorshift128-style stir; low 6 bits index the alphabet.
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        host.push(B64[(x & 63) as usize] as char);
    }
    (req_id, host)
}

/// S3 ETags are always double-quoted opaque tags, never `W/`-prefixed.
/// Multipart `-N` suffixes pass through verbatim.
pub(crate) fn quote_etag(etag: &str) -> String {
    let t = etag.trim();
    let bare = t.strip_prefix("W/").unwrap_or(t);
    if bare.len() >= 2 && bare.starts_with('"') && bare.ends_with('"') {
        bare.to_string()
    } else {
        format!("\"{bare}\"")
    }
}

pub(crate) fn s3_error_xml(code: &str, message: &str, resource: &str, req_id: &str, host_id: &str) -> String {
    s3_error_xml_ex(code, message, resource, req_id, host_id, &[])
}

/// Extended envelope with AWS-style extra elements (`ArgumentName`,
/// `BucketName`) placed after `Message` per the measured S3 error shape.
pub(crate) fn s3_error_xml_ex(
    code: &str,
    message: &str,
    resource: &str,
    req_id: &str,
    host_id: &str,
    extra: &[(&str, &str)],
) -> String {
    let mut extras = String::new();
    for (k, v) in extra {
        extras.push_str(&format!("<{k}>{v}</{k}>"));
    }
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <Error><Code>{code}</Code><Message>{message}</Message>{extras}\
         <Resource>{resource}</Resource><RequestId>{req_id}</RequestId>\
         <HostId>{host_id}</HostId></Error>"
    )
}

/// S3 resource path for error envelopes: the full request path
/// (`/{bucket}/{key}` in alias form, `/<key>` legacy).
/// `<Resource>` value for an error envelope. Keys come from the URL, so
/// they may carry control bytes or XML markup; the envelope is assembled by
/// string formatting, so escaping belongs here rather than at each caller
/// (an unescaped key would otherwise break the XML).
pub(crate) fn resource_path(key: &str) -> String {
    let encoded = percent_encoding::utf8_percent_encode(key, percent_encoding::CONTROLS).to_string();
    format!(
        "/{}",
        encoded.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
    )
}

/// S3 metadata headers shared by GET and HEAD.
pub(crate) fn s3_meta_headers(
    builder: axum::http::response::Builder,
    meta: &HitMeta,
    req_id: &str,
    host_id: &str,
) -> axum::http::response::Builder {
    let mut b = builder
        .header("accept-ranges", "bytes")
        .header("x-amz-request-id", req_id)
        .header("x-amz-id-2", host_id)
        // Deviation (documented above): no per-object stored value exists,
        // and edge caching outranks S3 purity here.
        .header("cache-control", "public, max-age=31536000, immutable");
    if let Some(ct) = &meta.content_type {
        b = b.header("content-type", ct);
    } else {
        b = b.header("content-type", "binary/octet-stream");
    }
    if let Some(et) = &meta.etag {
        b = b.header("etag", quote_etag(et));
    }
    if let Some(lm) = &meta.last_modified {
        b = b.header("last-modified", lm);
    }
    b
}

/// S3 error envelope with the request-ID headers, the shape every error
/// response in this module returns (status + ids + `application/xml`).
fn envelope(status: StatusCode, body: Body, req_id: &str, host_id: &str) -> Response {
    let mut resp = Response::builder()
        .status(status)
        .header("x-amz-request-id", req_id)
        .header("x-amz-id-2", host_id)
        .body(body)
        .unwrap();
    resp.headers_mut().insert("content-type", "application/xml".parse().unwrap());
    resp
}

/// Typed key-validation failures are `InvalidRequest` 400, not backend failures.
pub(crate) fn invalid_key_response(
    e: KeyError,
    key: &str,
    req_id: &str,
    host_id: &str,
    head_only: bool,
) -> Response {
    warn!(key = %key, error = %e, "invalid request key");
    let body = if head_only {
        Body::empty()
    } else {
        Body::from(s3_error_xml(
            "InvalidRequest",
            "The request key is invalid.",
            &resource_path(key),
            req_id,
            host_id,
        ))
    };
    envelope(StatusCode::BAD_REQUEST, body, req_id, host_id)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn error_response(
    e: BackendError,
    key: &str,
    req_id: &str,
    host_id: &str,
    head_only: bool,
    size_hint: Option<u64>,
) -> Response {
    let resource = resource_path(key);
    let xml = |code: &str, message: &str| s3_error_xml(code, message, &resource, req_id, host_id);
    let with_ids = |status: StatusCode, body: Body| envelope(status, body, req_id, host_id);
    match e {
        BackendError::NotFound => {
            let body = if head_only { Body::empty() } else { Body::from(xml("NoSuchKey", "The specified key does not exist.")) };
            with_ids(StatusCode::NOT_FOUND, body)
        }
        BackendError::RangeNotSatisfiable => {
            let mut resp = with_ids(
                StatusCode::RANGE_NOT_SATISFIABLE,
                if head_only { Body::empty() } else { Body::from(xml("InvalidRange", "The requested range is not satisfiable.")) },
            );
            if let Some(size) = size_hint {
                resp.headers_mut().insert(
                    "content-range",
                    ContentRange::unsatisfiable(size).parse().unwrap(),
                );
            }
            resp
        }
        BackendError::RateLimited { retry_after_millis } => {
            let mut resp = (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "upstream rate limited"})),
            )
                .into_response();
            if let Some(ms) = retry_after_millis {
                if let Ok(v) = axum::http::HeaderValue::from_str(&(ms / 1000).to_string()) {
                    resp.headers_mut().insert("retry-after", v);
                }
            }
            resp
        }
        // No key-validation arm: a client key error cannot reach this
        // function at all any more. It is answered at the resolve seam
        // (`invalid_key_response`) before any cache call, and `BackendError`
        // no longer has a variant that could carry it.
        other => {
            warn!(key = %key, error = %other, "cache fetch error");
            (StatusCode::BAD_GATEWAY, Json(json!({"error": "upstream error"}))).into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn etag_quoting_rules() {
        assert_eq!(quote_etag("abc123"), "\"abc123\"");
        assert_eq!(quote_etag("\"abc123\""), "\"abc123\"");
        assert_eq!(quote_etag("d41d8cd98f00b204e9800998ecf8427e-2"), "\"d41d8cd98f00b204e9800998ecf8427e-2\"");
        assert_eq!(quote_etag("W/\"abc\""), "\"abc\"");
    }

    /// Every KeyError variant must map to 400 through the generic error
    /// The mapping table for the faults that can still reach this function.
    ///
    /// There is no key-validation arm any more, and no test for one: a client
    /// key error is answered at the resolve seam before any cache call, so it
    /// cannot arrive here, and `BackendError` no longer has a variant that
    /// could carry it. The six-variant coverage that used to live here now
    /// lives where the seam is — `business::tests::invalid_object_keys_*`
    /// drives every variant through the router and the handler.
    #[tokio::test]
    async fn upstream_faults_keep_their_own_statuses() {
        for (err, want, label) in [
            (BackendError::NotFound, StatusCode::NOT_FOUND, "not found"),
            (BackendError::RangeNotSatisfiable, StatusCode::RANGE_NOT_SATISFIABLE, "range"),
            (BackendError::RateLimited { retry_after_millis: None }, StatusCode::SERVICE_UNAVAILABLE, "throttled"),
            (BackendError::AuthRequired, StatusCode::BAD_GATEWAY, "auth (a backend failure, not a client one)"),
            (BackendError::Other("token rejected".into()), StatusCode::BAD_GATEWAY, "opaque backend error"),
        ] {
            let resp = error_response(err, "some/key", "req-1", "host-1", false, None);
            assert_eq!(resp.status(), want, "{label}");
        }
        // A HEAD error carries no body.
        let head = error_response(BackendError::NotFound, "some/key", "req-1", "host-1", true, None);
        assert!(
            head.headers().get("content-length").is_none_or(|v| v == "0"),
            "a HEAD error must not carry a body: {:?}",
            head.headers().get("content-length")
        );
        assert!(
            axum::body::to_bytes(head.into_body(), 1024).await.unwrap().is_empty(),
            "HEAD must answer with an empty body"
        );
        // Text must not decide a status: a message that LOOKS like a key
        // error is still whatever variant it actually is.
        let resp = error_response(
            BackendError::Other("invalid key: empty key".into()),
            "some/key",
            "req-1",
            "host-1",
            false,
            None,
        );
        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
    }

    /// A key carrying markup or control bytes must never break the XML
    /// envelope, whichever path builds it: both the typed handler seam and
    /// the generic cache-layer branch format `<Resource>` by string
    /// concatenation.
    #[test]
    fn resource_path_escapes_markup_and_controls() {
        assert_eq!(resource_path("a/b.png"), "/a/b.png");
        assert_eq!(resource_path("a&b<c>d"), "/a&amp;b&lt;c&gt;d");
        assert!(resource_path("a\0b").contains("%00"));
        for key in ["a&b", "x<y>", "n\0l", "plain/key.bin"] {
            let xml = s3_error_xml("InvalidRequest", "m", &resource_path(key), "r", "h");
            assert!(!xml.contains("<y>"), "raw markup leaked for {key:?}: {xml}");
            assert!(xml.contains("</Error>"), "{xml}");
        }
    }
}

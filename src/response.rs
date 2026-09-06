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
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <Error><Code>{code}</Code><Message>{message}</Message>\
         <Resource>{resource}</Resource><RequestId>{req_id}</RequestId>\
         <HostId>{host_id}</HostId></Error>"
    )
}

/// S3 resource path for error envelopes: the full request path
/// (`/{bucket}/{key}` in alias form, `/<key>` legacy).
pub(crate) fn resource_path(key: &str) -> String {
    format!("/{key}")
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
    let with_ids = |status: StatusCode, body: Body| {
        let mut resp = Response::builder()
            .status(status)
            .header("x-amz-request-id", req_id)
            .header("x-amz-id-2", host_id)
            .body(body)
            .unwrap();
        resp.headers_mut().insert("content-type", "application/xml".parse().unwrap());
        resp
    };
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
        BackendError::Other(msg)
            if msg.contains("traversal") || msg.contains("Empty") || msg.contains("Absolute") =>
        {
            let body = if head_only { Body::empty() } else { Body::from(xml("InvalidRequest", "The request key is invalid.")) };
            with_ids(StatusCode::BAD_REQUEST, body)
        }
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
}

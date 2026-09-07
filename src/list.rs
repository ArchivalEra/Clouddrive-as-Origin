//! ListObjectsV2 endpoint (map #24: tickets #26 contract, #29 contract).
//!
//! AWS-authoritative semantics measured against real S3 buckets by R:
//! - `list-type=2` exact match selects V2; anything else silently
//!   degrades to V1 `ListObjects` (Marker/NextMarker).
//! - Item sequence is the Contents block (sorted) followed by the
//!   CommonPrefixes block (sorted) — NOT one merged lexicographic stream.
//! - `start-after` filters by string comparison on both lists.
//! - Tokens encode the last emitted item (Contents key or CP prefix) and
//!   are self-validating: base64 payload + truncated HMAC-SHA256 with a
//!   process-boot random key. Invalid tokens are 400, never a crash.
//! - `encoding-type=url` uses the form-urlencoded set (space -> `+`,
//!   `~` -> `%7E`) with `/` literal, applied to Key/Prefix/StartAfter/
//!   Delimiter/CP prefixes; tokens are never encoded.
//!
//! The listing path is metadata-only: it never touches the object cache
//! (no flights, no staging, no prewarm).

use std::sync::OnceLock;

use axum::{
    body::Body,
    http::{header::CONTENT_TYPE, StatusCode},
    response::Response,
};
use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::{
    backend::{BackendError, ListEntry},
    business::AppState,
    clock::Clock,
    response::{quote_etag, s3_error_xml_ex},
};

/// Deliberately-visible items per page: the AWS hard cap. The XML echoes
/// the client's requested `max-keys` even when this cap clips the page.
const MAX_KEYS_CAP: usize = 1000;

// ---------------------------------------------------------------------------
// Boot-scoped token key (process-opaque: restarts invalidate tokens, and
// clients re-list — the contract accepts this).
// ---------------------------------------------------------------------------

fn token_key() -> &'static [u8; 32] {
    static KEY: OnceLock<[u8; 32]> = OnceLock::new();
    KEY.get_or_init(|| {
        let mut k = [0u8; 32];
        getrandom::getrandom(&mut k).expect("os randomness for list token key");
        k
    })
}

fn sign_token(payload: &str) -> String {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(token_key()).expect("hmac key");
    mac.update(payload.as_bytes());
    let sig = mac.finalize().into_bytes();
    let mut bytes = Vec::with_capacity(payload.len() + 10);
    bytes.extend_from_slice(payload.as_bytes());
    bytes.extend_from_slice(&sig[..10]);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// Verify + decode a continuation token into its `(kind, name)` payload.
fn open_token(token: &str) -> Option<String> {
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(token).ok()?;
    if bytes.len() < 10 {
        return None;
    }
    let (payload, sig) = bytes.split_at(bytes.len() - 10);
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(token_key()).expect("hmac key");
    mac.update(payload);
    // Constant-time comparison (subtle via hmac's verify or manual fold).
    let expected = mac.finalize().into_bytes();
    let mut diff = 0u8;
    for (a, b) in expected.iter().zip(sig.iter()) {
        diff |= a ^ b;
    }
    if diff != 0 {
        return None;
    }
    String::from_utf8(payload.to_vec()).ok()
}

// ---------------------------------------------------------------------------
// Query parsing.
// ---------------------------------------------------------------------------

/// Parsed list request. `list_type` selects the wire dialect: V2 only for
/// the exact string "2"; everything else (including absent) is V1.
pub(crate) struct ListParams {
    pub is_v2: bool,
    pub prefix: String,
    pub delimiter: Option<String>,
    /// Raw `max-keys` string as sent (echoed in XML); empty = absent.
    pub max_keys_raw: Option<String>,
    pub continuation_token: Option<String>,
    pub start_after: Option<String>,
    pub marker: Option<String>,
    pub encoding_url: bool,
    pub fetch_owner: bool,
}

/// First value wins for duplicated keys (list clients never duplicate).
fn query_map(query: &str) -> Vec<(String, String)> {
    form_urlencoded::parse(query.as_bytes())
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect()
}

fn query_get<'a>(pairs: &'a [(String, String)], key: &str) -> Option<&'a str> {
    pairs.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str())
}

pub(crate) fn is_list_query(query: Option<&str>) -> bool {
    let Some(q) = query else { return false };
    const TRIGGERS: &[&str] = &[
        "list-type",
        "prefix",
        "delimiter",
        "max-keys",
        "continuation-token",
        "start-after",
        "marker",
        "encoding-type",
    ];
    query_map(q).iter().any(|(k, _)| TRIGGERS.contains(&k.as_str()))
}

/// Parse errors that map to AWS `InvalidArgument` 400s.
pub(crate) enum ListParamError {
    EncodingType,
    MaxKeysNotInteger,
    MaxKeysNegative,
    ContinuationToken,
}

impl ListParams {
    pub(crate) fn parse(query: &str) -> Result<Self, ListParamError> {
        let pairs = query_map(query);
        let encoding_type = query_get(&pairs, "encoding-type");
        let encoding_url = match encoding_type {
            None => false,
            Some("url") => true,
            Some(_) => return Err(ListParamError::EncodingType),
        };
        let max_keys_raw = query_get(&pairs, "max-keys").filter(|s| !s.is_empty()).map(str::to_string);
        if let Some(raw) = &max_keys_raw {
            match raw.parse::<i64>() {
                Err(_) => return Err(ListParamError::MaxKeysNotInteger),
                Ok(n) if n < 0 => return Err(ListParamError::MaxKeysNegative),
                Ok(n) if n > i32::MAX as i64 => return Err(ListParamError::MaxKeysNotInteger),
                Ok(_) => {}
            }
        }
        Ok(Self {
            is_v2: query_get(&pairs, "list-type") == Some("2"),
            prefix: query_get(&pairs, "prefix").unwrap_or("").to_string(),
            delimiter: query_get(&pairs, "delimiter").filter(|s| !s.is_empty()).map(str::to_string),
            max_keys_raw,
            continuation_token: query_get(&pairs, "continuation-token").map(str::to_string),
            start_after: query_get(&pairs, "start-after").map(str::to_string),
            marker: query_get(&pairs, "marker").map(str::to_string),
            encoding_url,
            fetch_owner: query_get(&pairs, "fetch-owner") == Some("true"),
        })
    }

    /// Effective per-page cap (client value clipped at 1000).
    fn cap(&self) -> usize {
        let n = self.max_keys_raw.as_deref().and_then(|s| s.parse::<usize>().ok()).unwrap_or(MAX_KEYS_CAP);
        n.min(MAX_KEYS_CAP)
    }
}

// ---------------------------------------------------------------------------
// Bucket resolution (path-style alias only — #29 contract).
// ---------------------------------------------------------------------------

/// `GET /{bucket}?list-type=2` pins the upstream by bucket alias;
/// `GET /?list-type=2` lists the default upstream's root. Anything else
/// (unknown first segment) is `NoSuchBucket`.
fn resolve_list_bucket(path_key: &str, state: &AppState<impl Clock + Clone>) -> Option<String> {
    let path = path_key.trim_matches('/');
    if path.is_empty() {
        return Some(state.config.routes.resolve("").to_string());
    }
    let first = path.split('/').next().unwrap_or(path);
    if state.config.upstreams.iter().any(|u| u.id == first) {
        return Some(first.to_string());
    }
    None
}

// ---------------------------------------------------------------------------
// Entry selection: the AWS item sequence is Contents-block then
// CommonPrefixes-block, each sorted in UTF-8 binary order.
// ---------------------------------------------------------------------------

struct PageItems {
    contents: Vec<ListEntry>,
    common_prefixes: Vec<String>,
}

fn select_items(entries: Vec<ListEntry>, params: &ListParams) -> PageItems {
    let prefix = params.prefix.as_str();
    let mut contents: Vec<ListEntry> = entries
        .iter()
        .filter(|e| !e.is_dir && e.key.starts_with(prefix))
        .cloned()
        .collect();
    contents.sort_by(|a, b| a.key.cmp(&b.key));

    let mut common_prefixes: Vec<String> = Vec::new();
    if params.delimiter.is_some() {
        let mut dirs: Vec<String> =
            entries.iter().filter(|e| e.is_dir && e.key.starts_with(prefix)).map(|e| e.key.clone()).collect();
        dirs.sort();
        dirs.dedup();
        common_prefixes = dirs;
    }
    // start-after (V2): string-compare filter on both lists (measured AWS
    // behavior: CPs not greater than StartAfter are dropped). Ignored when
    // a continuation token is present.
    if params.is_v2 && params.continuation_token.is_none() {
        if let Some(after) = &params.start_after {
            contents.retain(|e| e.key.as_str() > after.as_str());
            common_prefixes.retain(|p| p.as_str() > after.as_str());
        }
    }
    PageItems { contents, common_prefixes }
}

/// Resume coordinates inside the block-ordered sequence.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Resume {
    Start,
    /// Emit Contents strictly after this key, then every CP.
    AfterContent(String),
    /// Contents are exhausted; emit CPs strictly after this prefix.
    AfterPrefix(String),
}

fn resume_from(params: &ListParams) -> Option<Result<Resume, ()>> {
    if params.is_v2 {
        let token = params.continuation_token.as_deref()?;
        return Some(match open_token(token).and_then(|p| {
            let (kind, name) = p.split_once('|')?;
            match kind {
                "c" => Some(Resume::AfterContent(name.to_string())),
                "p" => Some(Resume::AfterPrefix(name.to_string())),
                _ => None,
            }
        }) {
            Some(r) => Ok(r),
            None => Err(()), // invalid token -> 400
        });
    }
    // V1 marker: a marker ending with the delimiter resumes in the CP
    // block (files can never end with '/' in WebDAV-backed listings);
    // otherwise it was a Contents key.
    let marker = params.marker.as_deref()?;
    let in_cps = marker.ends_with('/') && params.delimiter.is_some();
    Some(Ok(if in_cps {
        Resume::AfterPrefix(marker.to_string())
    } else {
        Resume::AfterContent(marker.to_string())
    }))
}

// ---------------------------------------------------------------------------
// The endpoint.
// ---------------------------------------------------------------------------

/// Dispatch seam for `get_key`: returns `Some(response)` when the request
/// is a list operation, `None` to proceed with the object pipeline.
pub(crate) async fn try_list<C: Clock + Clone>(
    state: &AppState<C>,
    path_key: &str,
    query: Option<&str>,
    req_id: &str,
    host_id: &str,
) -> Option<Response> {
    if !is_list_query(query) {
        return None;
    }
    let query = query.unwrap_or("");
    let params = match ListParams::parse(query) {
        Ok(p) => p,
        Err(e) => return Some(invalid_argument(&e, path_key, req_id, host_id)),
    };
    let Some(upstream_id) = resolve_list_bucket(path_key, state) else {
        return Some(no_such_bucket(path_key, req_id, host_id));
    };
    let Some(slot) = state.cache.backends.get(&upstream_id) else {
        return Some(no_such_bucket(path_key, req_id, host_id));
    };

    // PROPFIND folder: the longest directory prefix of `prefix`. A prefix
    // that ends mid-segment ("2026/0") lists its parent and string-filters.
    let prefix = params.prefix.as_str();
    let folder = match prefix.rfind('/') {
        Some(i) => &prefix[..=i],
        None => "",
    };
    let recursive = params.delimiter.is_none();

    let entries = match slot.backend.list(folder, recursive).await {
        Ok(e) => e,
        // A missing folder is an empty listing: S3 prefixes are string
        // filters, not containers.
        Err(BackendError::NotFound) => Vec::new(),
        Err(BackendError::AuthRequired) => {
            return Some(list_error("AccessDenied", "Access Denied", path_key, req_id, host_id, &[]))
        }
        Err(e) => {
            tracing::warn!(upstream = %upstream_id, prefix = %prefix, error = %e, "upstream listing failed");
            return Some(list_error(
                "InternalError",
                "We encountered an internal error. Please try again.",
                path_key,
                req_id,
                host_id,
                &[],
            ));
        }
    };

    let page = select_items(entries, &params);
    match resume_from(&params) {
        Some(Err(())) => {
            return Some(invalid_argument(
                &ListParamError::ContinuationToken,
                path_key,
                req_id,
                host_id,
            ))
        }
        Some(Ok(resume)) => Some(render_page(state, upstream_id, &params, page, resume, req_id, host_id)),
        None => Some(render_page(state, upstream_id, &params, page, Resume::Start, req_id, host_id)),
    }
}

fn render_page<C: Clock + Clone>(
    _state: &AppState<C>,
    upstream_id: String,
    params: &ListParams,
    page: PageItems,
    resume: Resume,
    req_id: &str,
    host_id: &str,
) -> Response {
    let cap = params.cap();
    // AWS measured behavior: max-keys=0 yields an empty page with
    // IsTruncated=false (no infinite pagination loop).
    if cap == 0 {
        return render_page_response(
            &upstream_id,
            params,
            Vec::new(),
            Vec::new(),
            false,
            None,
            req_id,
            host_id,
        );
    }
    let (contents, common_prefixes, truncated) = match resume {
        Resume::Start => {
            let total = page.contents.len() + page.common_prefixes.len();
            let mut c = page.contents;
            c.truncate(cap);
            let mut p = page.common_prefixes;
            let rest = cap - c.len();
            p.truncate(rest);
            let emitted = c.len() + p.len();
            (c, p, total > emitted)
        }
        Resume::AfterContent(key) => {
            let files: Vec<ListEntry> =
                page.contents.into_iter().filter(|e| e.key.as_str() > key.as_str()).collect();
            let total = files.len() + page.common_prefixes.len();
            let mut c = files;
            c.truncate(cap);
            let mut p = page.common_prefixes;
            let rest = cap - c.len();
            p.truncate(rest);
            let emitted = c.len() + p.len();
            (c, p, total > emitted)
        }
        Resume::AfterPrefix(prefix) => {
            let cps: Vec<String> =
                page.common_prefixes.into_iter().filter(|p| p.as_str() > prefix.as_str()).collect();
            let truncated = cps.len() > cap;
            let mut p = cps;
            p.truncate(cap);
            (Vec::new(), p, truncated)
        }
    };

    // Next token only when truncated; it names the last emitted item.
    let next_token = truncated.then(|| {
        match common_prefixes.last() {
            Some(p) => sign_token(&format!("p|{p}")),
            None => match contents.last() {
                Some(c) => sign_token(&format!("c|{}", c.key)),
                // Unreachable in practice (cap==0 forces truncated=false
                // above); a defensive content resume at the start.
                None => sign_token("c|"),
            },
        }
    });
    render_page_response(
        &upstream_id,
        params,
        contents,
        common_prefixes,
        truncated,
        next_token.as_deref(),
        req_id,
        host_id,
    )
}

fn render_page_response(
    bucket: &str,
    params: &ListParams,
    contents: Vec<ListEntry>,
    common_prefixes: Vec<String>,
    truncated: bool,
    next_token: Option<&str>,
    req_id: &str,
    host_id: &str,
) -> Response {
    let body = render_xml(bucket, params, &contents, &common_prefixes, truncated, next_token);
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/xml")
        .header("cache-control", "private, max-age=5")
        .header("x-amz-request-id", req_id)
        .header("x-amz-id-2", host_id)
        .body(Body::from(body))
        .unwrap()
}

// ---------------------------------------------------------------------------
// XML rendering (#26 contract element order).
// ---------------------------------------------------------------------------

fn xml_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(c),
        }
    }
    out
}

/// AWS `encoding-type=url`: form-urlencoded set over UTF-8 bytes — every
/// character except `A-Z a-z 0-9 - _ . /` is percent-encoded, space
/// becomes `+`. Measured char-by-char against real S3.
fn aws_url_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'/' => {
                out.push(b as char)
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn enc_maybe(params: &ListParams, s: &str) -> String {
    let raw = if params.encoding_url { aws_url_encode(s) } else { xml_escape(s) };
    raw
}

fn iso8601(rfc2822: &str) -> Option<String> {
    chrono::DateTime::parse_from_rfc2822(rfc2822)
        .ok()
        .map(|d| d.with_timezone(&chrono::Utc).to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
}

#[allow(clippy::too_many_arguments)]
fn render_xml(
    bucket: &str,
    params: &ListParams,
    contents: &[ListEntry],
    common_prefixes: &[String],
    truncated: bool,
    next_token: Option<&str>,
) -> String {
    let mut x = String::with_capacity(1024 + contents.len() * 128);
    x.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    x.push_str("<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
    x.push_str(&format!("<Name>{}</Name>", xml_escape(bucket)));
    x.push_str(&format!("<Prefix>{}</Prefix>", enc_maybe(params, &params.prefix)));
    if params.is_v2 {
        if let Some(sa) = &params.start_after {
            x.push_str(&format!("<StartAfter>{}</StartAfter>", enc_maybe(params, sa)));
        }
        if let Some(t) = &params.continuation_token {
            // Tokens are never encoding-type'd (#26).
            x.push_str(&format!("<ContinuationToken>{}</ContinuationToken>", xml_escape(t)));
        }
        if let Some(t) = next_token {
            x.push_str(&format!("<NextContinuationToken>{}</NextContinuationToken>", xml_escape(t)));
        }
        let key_count = contents.len() + common_prefixes.len();
        x.push_str(&format!("<KeyCount>{key_count}</KeyCount>"));
        let max_keys_echo = params.max_keys_raw.as_deref().unwrap_or("1000");
        x.push_str(&format!("<MaxKeys>{}</MaxKeys>", xml_escape(max_keys_echo)));
        if let Some(d) = &params.delimiter {
            x.push_str(&format!("<Delimiter>{}</Delimiter>", enc_maybe(params, d)));
        }
        if params.encoding_url {
            x.push_str("<EncodingType>url</EncodingType>");
        }
    } else {
        let max_keys_echo = params.max_keys_raw.as_deref().unwrap_or("1000");
        x.push_str(&format!("<MaxKeys>{}</MaxKeys>", xml_escape(max_keys_echo)));
        if let Some(m) = &params.marker {
            x.push_str(&format!("<Marker>{}</Marker>", enc_maybe(params, m)));
        }
        if truncated && params.delimiter.is_some() {
            let next = common_prefixes
                .last()
                .cloned()
                .or_else(|| contents.last().map(|c| c.key.clone()))
                .unwrap_or_default();
            x.push_str(&format!("<NextMarker>{}</NextMarker>", enc_maybe(params, &next)));
        }
        if let Some(d) = &params.delimiter {
            x.push_str(&format!("<Delimiter>{}</Delimiter>", enc_maybe(params, d)));
        }
        if params.encoding_url {
            x.push_str("<EncodingType>url</EncodingType>");
        }
    }
    x.push_str(&format!("<IsTruncated>{}</IsTruncated>", truncated));
    for c in contents {
        x.push_str("<Contents>");
        x.push_str(&format!("<Key>{}</Key>", enc_maybe(params, &c.key)));
        if let Some(lm) = &c.last_modified {
            if let Some(iso) = iso8601(lm) {
                x.push_str(&format!("<LastModified>{iso}</LastModified>"));
            }
        }
        if let Some(et) = &c.etag {
            x.push_str(&format!("<ETag>{}</ETag>", xml_escape(&quote_etag(et))));
        }
        x.push_str(&format!("<Size>{}</Size>", c.size));
        if params.fetch_owner {
            x.push_str("<Owner><ID>origin-cache</ID><DisplayName>origin-cache</DisplayName></Owner>");
        }
        x.push_str("<StorageClass>STANDARD</StorageClass>");
        x.push_str("</Contents>");
    }
    for p in common_prefixes {
        x.push_str("<CommonPrefixes>");
        x.push_str(&format!("<Prefix>{}</Prefix>", enc_maybe(params, p)));
        x.push_str("</CommonPrefixes>");
    }
    x.push_str("</ListBucketResult>");
    x
}

// ---------------------------------------------------------------------------
// Error shapes (#26 section 8).
// ---------------------------------------------------------------------------

fn list_error(
    code: &str,
    message: &str,
    path_key: &str,
    req_id: &str,
    host_id: &str,
    extra: &[(&str, &str)],
) -> Response {
    let status = match code {
        "NoSuchBucket" => StatusCode::NOT_FOUND,
        "AccessDenied" => StatusCode::FORBIDDEN,
        "InternalError" => StatusCode::INTERNAL_SERVER_ERROR,
        _ => StatusCode::BAD_REQUEST,
    };
    let resource = format!("/{}/", path_key.trim_matches('/'));
    let xml = s3_error_xml_ex(code, message, &resource, req_id, host_id, extra);
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/xml")
        .header("x-amz-request-id", req_id)
        .header("x-amz-id-2", host_id)
        .body(Body::from(xml))
        .unwrap()
}

impl ListParamError {
    fn code_and_message(&self) -> (&'static str, &'static str, &'static str) {
        match self {
            Self::EncodingType => (
                "InvalidArgument",
                "Invalid Encoding Method specified in Request",
                "encoding-type",
            ),
            Self::MaxKeysNotInteger => (
                "InvalidArgument",
                "Provided max-keys not an integer or within integer range",
                "max-keys",
            ),
            Self::MaxKeysNegative => (
                "InvalidArgument",
                "Argument maxKeys must be an integer between 0 and 2147483647",
                "max-keys",
            ),
            Self::ContinuationToken => (
                "InvalidArgument",
                "The continuation token provided is incorrect",
                "continuation-token",
            ),
        }
    }
}

fn invalid_argument(e: &ListParamError, path_key: &str, req_id: &str, host_id: &str) -> Response {
    let (code, message, arg) = e.code_and_message();
    list_error(code, message, path_key, req_id, host_id, &[("ArgumentName", arg)])
}

fn no_such_bucket(path_key: &str, req_id: &str, host_id: &str) -> Response {
    let bucket = xml_escape(path_key.trim_matches('/').split('/').next().unwrap_or(""));
    list_error(
        "NoSuchBucket",
        "The specified bucket does not exist",
        path_key,
        req_id,
        host_id,
        &[("BucketName", &bucket)],
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        backend::{BackendRegistry, BackendSlot, TestMockBackend},
        clock::MockClock,
        config::Config,
        response::request_ids,
    };
    use std::{collections::HashMap, sync::Arc};
    use tokio::sync::Semaphore;

    fn entry(key: &str, size: u64, is_dir: bool) -> ListEntry {
        ListEntry {
            key: key.to_string(),
            size,
            etag: (!is_dir).then(|| format!("\"etag-{key}\"")),
            last_modified: (!is_dir).then(|| "Wed, 01 Jan 2025 00:00:00 GMT".to_string()),
            is_dir,
        }
    }

    /// Single default upstream "primary" plus optional bucket-alias
    /// upstreams, each backed by a MockBackend seeded with `listing`.
    fn fixture(primary: Vec<ListEntry>, extras: Vec<(&str, Vec<ListEntry>)>) -> AppState<MockClock> {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = Config { cache_dir: dir.path().to_path_buf(), ..Config::default() };
        let mut slots: HashMap<String, Arc<BackendSlot>> = HashMap::new();
        slots.insert(
            "primary".into(),
            Arc::new(BackendSlot {
                backend: Arc::new(TestMockBackend::new(b"x", None, None).with_listing(primary)),
                gate: Arc::new(Semaphore::new(3)),
            }),
        );
        for (id, listing) in extras {
            let mut u = cfg.upstreams[0].clone();
            u.id = id.to_string();
            cfg.upstreams.push(u);
            slots.insert(
                id.into(),
                Arc::new(BackendSlot {
                    backend: Arc::new(TestMockBackend::new(b"x", None, None).with_listing(listing)),
                    gate: Arc::new(Semaphore::new(3)),
                }),
            );
        }
        let cache = Arc::new(crate::cache::cache::Cache::new(
            Arc::new(cfg.clone()),
            Arc::new(MockClock::new(0)),
            BackendRegistry::new(slots),
        ));
        AppState { cache, config: Arc::new(cfg) }
    }

    async fn list_at(state: &AppState<MockClock>, path: &str, query: &str) -> (StatusCode, axum::http::HeaderMap, String) {
        let (req_id, host_id) = request_ids();
        let resp = try_list(state, path, Some(query), &req_id, &host_id)
            .await
            .expect("expected a list response");
        let status = resp.status();
        let headers = resp.headers().clone();
        let body = axum::body::to_bytes(resp.into_body(), 4 * 1024 * 1024).await.unwrap();
        (status, headers, String::from_utf8_lossy(&body).into_owned())
    }

    fn root_listing() -> Vec<ListEntry> {
        vec![
            entry("2026/08/a.png", 10, false),
            entry("2026/08/b.png", 20, false),
            entry("2026/index.html", 30, false),
            entry("media/x.mp4", 40, false),
            entry("zz.txt", 50, false),
            entry("media/", 0, true),
            entry("2026/", 0, true),
            entry("2026/08/", 0, true),
        ]
    }

    #[tokio::test]
    async fn v2_root_delimiter_folding_block_order() {
        let state = fixture(root_listing(), vec![]);
        let (status, h, body) = list_at(&state, "", "list-type=2&delimiter=/").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(h.get("content-type").unwrap(), "application/xml");
        assert_eq!(h.get("cache-control").unwrap(), "private, max-age=5");
        // Depth:1 semantics for delimiter mode: only the root-level file is
        // Contents; nested keys fold into the seeded child dirs.
        assert!(body.contains("<Key>zz.txt</Key>"));
        assert!(!body.contains("<Key>2026/08/a.png</Key>"), "{body}");
        // Contents block entirely before CommonPrefixes (measured AWS wire
        // order), each sorted.
        let contents_end = body.find("</Contents>").unwrap();
        let cp_start = body.find("<CommonPrefixes>").unwrap();
        assert!(contents_end < cp_start, "{body}");
        assert!(body.contains("<CommonPrefixes><Prefix>2026/</Prefix></CommonPrefixes>"));
        assert!(body.contains("<CommonPrefixes><Prefix>media/</Prefix></CommonPrefixes>"));
        // KeyCount = 1 file + 2 CPs; element order around KeyCount.
        assert!(body.contains("<KeyCount>3</KeyCount>"));
        assert!(body.contains("<MaxKeys>1000</MaxKeys>"));
        assert!(body.contains("<Delimiter>/</Delimiter>"));
        assert!(body.contains("<IsTruncated>false</IsTruncated>"));
        assert!(body.contains("<Name>primary</Name>"));
        assert!(!body.contains("NextContinuationToken"));
        // ETag is quoted.
        assert!(body.contains("<ETag>&quot;etag-zz.txt&quot;</ETag>"), "{body}");
    }

    #[tokio::test]
    async fn v2_prefix_scopes_listing() {
        let state = fixture(root_listing(), vec![]);
        let (_, _, body) = list_at(&state, "", "list-type=2&prefix=2026/&delimiter=/").await;
        assert!(body.contains("<Prefix>2026/</Prefix>"));
        // Depth:1 at "2026/": the immediate file + the immediate dir.
        assert!(body.contains("<Key>2026/index.html</Key>"));
        assert!(body.contains("<CommonPrefixes><Prefix>2026/08/</Prefix></CommonPrefixes>"));
        assert!(!body.contains("media"));
        assert!(body.contains("<KeyCount>2</KeyCount>"));
    }

    /// Partial-segment prefix ("2026/0"): parent Depth:1 + string filter.
    #[tokio::test]
    async fn v2_partial_segment_prefix() {
        let state = fixture(root_listing(), vec![]);
        let (_, _, body) = list_at(&state, "", "list-type=2&prefix=2026/0").await;
        assert!(body.contains("<Key>2026/08/a.png</Key>"));
        assert!(body.contains("<Key>2026/08/b.png</Key>"));
        assert!(!body.contains("index.html"));
        assert!(body.contains("<KeyCount>2</KeyCount>"));
    }

    #[tokio::test]
    async fn v2_pagination_token_round_trip() {
        let state = fixture(root_listing(), vec![]);
        // No delimiter: recursive mode, all 5 files page through.
        let (_, _, b1) = list_at(&state, "", "list-type=2&max-keys=2").await;
        assert!(b1.contains("<IsTruncated>true</IsTruncated>"), "{b1}");
        assert!(b1.contains("<MaxKeys>2</MaxKeys>"));
        assert!(b1.contains("<KeyCount>2</KeyCount>"));
        let start = b1.find("<NextContinuationToken>").unwrap() + "<NextContinuationToken>".len();
        let end = b1[start..].find("</NextContinuationToken>").unwrap();
        let token = &b1[start..start + end];
        // Token is opaque base64url, no XML specials.
        assert!(!token.contains('<'), "{token}");

        // Page 2: resume strictly after the token position.
        let (_, _, b2) = list_at(&state, "", &format!("list-type=2&max-keys=2&continuation-token={token}")).await;
        assert!(b2.contains("<IsTruncated>true</IsTruncated>"), "{b2}");
        assert!(b2.contains("<ContinuationToken>"), "{b2}");

        // Page 3: the tail.
        let start = b2.find("<NextContinuationToken>").unwrap() + "<NextContinuationToken>".len();
        let end = b2[start..].find("</NextContinuationToken>").unwrap();
        let token2 = &b2[start..start + end];
        let (_, _, b3) = list_at(&state, "", &format!("list-type=2&max-keys=2&continuation-token={token2}")).await;
        assert!(b3.contains("<IsTruncated>false</IsTruncated>"), "{b3}");
        // All 5 items seen exactly once across the three pages.
        let collect = |b: &str| {
            b.matches("<Key>").map(|_| ()).count() + b.matches("<CommonPrefixes>").count()
        };
        assert_eq!(collect(&b1) + collect(&b2) + collect(&b3), 5);
        // Pages do not repeat keys.
        let first_page_key = b1.split("<Key>").nth(1).unwrap().split("</Key>").next().unwrap();
        assert!(!b2.contains(&format!("<Key>{first_page_key}</Key>")), "{b2}");
    }

    #[tokio::test]
    async fn invalid_token_is_invalid_argument() {
        let state = fixture(root_listing(), vec![]);
        let (status, _, body) =
            list_at(&state, "", "list-type=2&continuation-token=garbage").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("<Code>InvalidArgument</Code>"), "{body}");
        assert!(body.contains("The continuation token provided is incorrect"), "{body}");
        assert!(body.contains("<ArgumentName>continuation-token</ArgumentName>"), "{body}");
    }

    #[tokio::test]
    async fn start_after_filters_and_yields_to_token() {
        let state = fixture(root_listing(), vec![]);
        let (_, _, body) =
            list_at(&state, "", "list-type=2&delimiter=/&start-after=media/x.mp4").await;
        // Files > start-after only; CP "media/" is NOT greater -> dropped
        // (measured AWS behavior).
        assert!(body.contains("<Key>zz.txt</Key>"));
        assert!(!body.contains("<Key>2026"), "{body}");
        assert!(!body.contains("media/</Prefix>"), "{body}");

        // Token overrides start-after: page 1 without start-after yields a
        // content token; page 2 sends BOTH token and a start-after that
        // would exclude everything — the token position wins.
        let (_, _, b1) = list_at(&state, "", "list-type=2&max-keys=1").await;
        let start = b1.find("<NextContinuationToken>").unwrap() + "<NextContinuationToken>".len();
        let end = b1[start..].find("</NextContinuationToken>").unwrap();
        let token = &b1[start..start + end];
        let (_, _, b2) = list_at(
            &state,
            "",
            &format!("list-type=2&start-after=zzzz&continuation-token={token}"),
        )
        .await;
        // Resume applies the token position, not start-after.
        assert!(b2.contains("<Key>2026/08/b.png</Key>"), "{b2}");
    }

    #[tokio::test]
    async fn max_keys_zero_is_empty_and_not_truncated() {
        let state = fixture(root_listing(), vec![]);
        let (_, _, body) = list_at(&state, "", "list-type=2&max-keys=0").await;
        assert!(body.contains("<KeyCount>0</KeyCount>"), "{body}");
        assert!(body.contains("<MaxKeys>0</MaxKeys>"));
        assert!(body.contains("<IsTruncated>false</IsTruncated>"));
        assert!(!body.contains("<Contents>"));
    }

    #[tokio::test]
    async fn max_keys_error_shapes() {
        let state = fixture(root_listing(), vec![]);
        let (status, _, body) = list_at(&state, "", "list-type=2&max-keys=abc").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("Provided max-keys not an integer or within integer range"), "{body}");
        let (_, _, body) = list_at(&state, "", "list-type=2&max-keys=-1").await;
        assert!(body.contains("Argument maxKeys must be an integer between 0 and 2147483647"), "{body}");
        let (_, _, body) = list_at(&state, "", "list-type=2&max-keys=9999999999").await;
        assert!(body.contains("Provided max-keys not an integer or within integer range"), "{body}");
    }

    #[tokio::test]
    async fn encoding_type_url_form_set() {
        let state = fixture(
            vec![entry("my song~a/file (1).png", 5, false), entry("plain.txt", 1, false)],
            vec![],
        );
        // No delimiter: recursive mode reaches the nested key.
        let (_, _, body) = list_at(&state, "", "list-type=2&encoding-type=url").await;
        // space -> +, ~ -> %7E, ( -> %28, / literal.
        assert!(body.contains("<Key>my+song%7Ea/file+%281%29.png</Key>"), "{body}");
        assert!(body.contains("<EncodingType>url</EncodingType>"));
        assert!(body.contains("<Key>plain.txt</Key>"));
    }

    #[tokio::test]
    async fn encoding_type_invalid_is_400() {
        let state = fixture(root_listing(), vec![]);
        let (status, _, body) = list_at(&state, "", "list-type=2&encoding-type=base64").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("Invalid Encoding Method specified in Request"), "{body}");
    }

    #[tokio::test]
    async fn v1_degrade_for_missing_or_wrong_list_type() {
        let state = fixture(root_listing(), vec![]);
        // No list-type: V1 shapes (Marker/NextMarker, no KeyCount).
        let (_, _, b1) = list_at(&state, "", "delimiter=/&max-keys=2").await;
        assert!(!b1.contains("<KeyCount>"), "{b1}");
        assert!(!b1.contains("ContinuationToken"), "{b1}");
        assert!(b1.contains("<NextMarker>"), "{b1}");
        // list-type=3 also degrades (measured: byte-identical to absent).
        let (_, _, b3) = list_at(&state, "", "list-type=3&delimiter=/&max-keys=2").await;
        assert!(!b3.contains("<KeyCount>"), "{b3}");
        // V1 marker paging: marker = last key.
        let start = b1.find("<NextMarker>").unwrap() + "<NextMarker>".len();
        let end = b1[start..].find("</NextMarker>").unwrap();
        let marker = &b1[start..start + end];
        let (_, _, b2) = list_at(&state, "", &format!("delimiter=/&max-keys=2&marker={marker}")).await;
        assert!(!b2.contains(&format!("<Key>{marker}</Key>")), "{b2}");
    }

    #[tokio::test]
    async fn bucket_alias_pins_upstream_and_unknown_is_nosuchbucket() {
        let state = fixture(
            vec![entry("top.bin", 1, false)],
            vec![("archive", vec![entry("a1.bin", 2, false), entry("deep/d2.bin", 3, false)])],
        );
        let (_, _, body) = list_at(&state, "archive", "list-type=2").await;
        assert!(body.contains("<Name>archive</Name>"), "{body}");
        // No delimiter: recursive Contents (dirs never emitted).
        assert!(body.contains("<Key>a1.bin</Key>"));
        assert!(body.contains("<Key>deep/d2.bin</Key>"));
        // Trailing slash on the bucket path is equivalent.
        let (_, _, body2) = list_at(&state, "archive/", "list-type=2").await;
        assert!(body2.contains("<Key>a1.bin</Key>"));
        // Unknown first segment: NoSuchBucket with BucketName.
        let (status, _, body3) = list_at(&state, "ghost", "list-type=2").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(body3.contains("<Code>NoSuchBucket</Code>"), "{body3}");
        assert!(body3.contains("<BucketName>ghost</BucketName>"), "{body3}");
    }

    #[tokio::test]
    async fn missing_prefix_folder_is_empty_200() {
        let state = fixture(root_listing(), vec![]);
        let (status, _, body) = list_at(&state, "", "list-type=2&prefix=nothing/here/").await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("<KeyCount>0</KeyCount>"), "{body}");
        assert!(body.contains("<Prefix>nothing/here/</Prefix>"));
    }

    #[tokio::test]
    async fn fetch_owner_adds_owner_element() {
        let state = fixture(vec![entry("a.bin", 1, false)], vec![]);
        let (_, _, body) = list_at(&state, "", "list-type=2&fetch-owner=true").await;
        assert!(body.contains("<Owner><ID>origin-cache</ID>"), "{body}");
        let (_, _, body2) = list_at(&state, "", "list-type=2").await;
        assert!(!body2.contains("<Owner>"), "{body2}");
    }

    #[tokio::test]
    async fn xml_escaping_and_object_path_trigger() {
        let state = fixture(vec![entry("a&b<c>.png", 1, false)], vec![]);
        let (_, _, body) = list_at(&state, "", "list-type=2").await;
        assert!(body.contains("<Key>a&amp;b&lt;c&gt;.png</Key>"), "{body}");
        // Not a list request: try_list declines (None).
        assert!(try_list(&state, "a.bin", Some("download=1"), "r", "h").await.is_none());
        assert!(try_list(&state, "a.bin", None, "r", "h").await.is_none());
    }

    #[test]
    fn token_signing_is_self_validating() {
        let t = sign_token("c|some/key");
        assert_eq!(open_token(&t).unwrap(), "c|some/key");
        // Tampered payload fails verification.
        let mut bad = t.clone();
        bad.pop();
        assert!(open_token(&bad).is_none());
        assert!(open_token("nonsense").is_none());
    }
}

//! OpenList backend contract tests (wiremock): PROPFIND stat mapping,
//! ranged GET passthrough, error mapping (404 → NotFound, 401 →
//! AuthRequired), and the OpenList server-side quirks we depend on.

use bytes::Bytes;
use futures::StreamExt;

use origin_cache::{
    backend::{BackendError, ByteRange, DirectUrl, Key, ObjectMeta, StreamSource, StorageBackend},
    config::{ColdMiss, UpstreamConfig},
    mime,
};

use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn upstream(base: String) -> UpstreamConfig {
    UpstreamConfig {
        id: "media".into(),
        backend_type: "openlist".into(),
        base_url: base,
        root_path: None,
        username_env: "OPENLIST_USERNAME".into(),
        password_env: "OPENLIST_PASSWORD".into(),
        accept_invalid_certs: false,
        cold_miss: ColdMiss::Proxy,
        link_api_token_env: Some("OPENLIST_LINK_TOKEN".into()),
    }
}

fn backend_for(base: String) -> origin_cache::backend::OpenListBackend {
    std::env::set_var("OPENLIST_USERNAME", "test-user");
    std::env::set_var("OPENLIST_PASSWORD", "test-pass");
    std::env::set_var("OPENLIST_LINK_TOKEN", "test-token");
    origin_cache::backend::OpenListBackend::from_config(&upstream(base)).unwrap()
}

/// Minimal multistatus XML for a single file — the shape OpenList returns.
fn propfind_file_xml(etag: &str, size: u64) -> String {
    format!(
        r#"<?xml version="1.0" encoding="utf-8"?>
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>/dav/media/a.flac</D:href>
    <D:propstat>
      <D:prop>
        <D:getetag>{etag}</D:getetag>
        <D:getcontentlength>{size}</D:getcontentlength>
        <D:getcontenttype>application/octet-stream</D:getcontenttype>
        <D:getlastmodified>Wed, 21 Oct 2026 07:28:00 GMT</D:getlastmodified>
        <D:resourcetype/>
      </D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"#
    )
}

async fn read_all(src: StreamSource) -> Vec<u8> {
    use tokio::io::AsyncReadExt;
    let mut s = src.stream;
    let mut out = Vec::new();
    s.read_to_end(&mut out).await.unwrap();
    out
}

#[tokio::test]
async fn stat_maps_propfind_to_object_meta() {
    let server = MockServer::start().await;
    // PROPFIND is a custom method; wiremock matches it explicitly.
    Mock::given(method("PROPFIND"))
        .and(path("/media/a.flac"))
        .and(header("Authorization", "Basic dGVzdC11c2VyOnRlc3QtcGFzcw=="))
        .respond_with(
            ResponseTemplate::new(207)
                .set_body_string(propfind_file_xml("\"abc123\"", 1234)),
        )
        .mount(&server)
        .await;

    let b = backend_for(server.uri());
    let meta = b.stat(&Key::from_validated("media/a.flac".into())).await.unwrap();
    assert_eq!(meta.size_bytes, 1234);
    assert_eq!(meta.etag.as_deref(), Some("\"abc123\""));
    // OpenList hints octet-stream for media — our MIME fallback fixes it
    // at the HitMeta layer (tested in cache); here the raw hint passes.
    assert_eq!(meta.mime_hint.as_deref(), Some("application/octet-stream"));
    assert!(meta.last_modified.is_some());
    // The resolved content-type for the business plane:
    assert_eq!(mime::resolve("media/a.flac", &meta.mime_hint), Some("audio/flac".to_string()));
}

#[tokio::test]
async fn stat_missing_key_maps_to_not_found() {
    let server = MockServer::start().await;
    Mock::given(method("PROPFIND"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;
    let b = backend_for(server.uri());
    let err = b.stat(&Key::from_validated("nope.png".into())).await.unwrap_err();
    assert!(matches!(err, BackendError::NotFound), "got {err:?}");
}

#[tokio::test]
async fn stat_bad_credentials_map_to_auth_required() {
    let server = MockServer::start().await;
    Mock::given(method("PROPFIND"))
        .respond_with(ResponseTemplate::new(401))
        .mount(&server)
        .await;
    let b = backend_for(server.uri());
    let err = b.stat(&Key::from_validated("a.png".into())).await.unwrap_err();
    assert!(matches!(err, BackendError::AuthRequired), "got {err:?}");
}

#[tokio::test]
async fn open_passes_range_header_and_streams() {
    let server = MockServer::start().await;
    let payload = b"0123456789".to_vec();
    Mock::given(method("GET"))
        .and(path("/media/big.bin"))
        .and(header("range", "bytes=3-"))
        .respond_with(
            ResponseTemplate::new(206)
                .set_body_bytes(Bytes::from_static(b"3456789"))
                .insert_header("content-range", "bytes 3-9/10"),
        )
        .mount(&server)
        .await;
    let b = backend_for(server.uri());

    let src = b
        .open(&Key::from_validated("media/big.bin".into()), Some(ByteRange::from_offset(3)))
        .await
        .unwrap();
    assert_eq!(src.total_len, Some(10), "Content-Range total is the full object size");
    assert_eq!(read_all(src).await, b"3456789");
}

#[tokio::test]
async fn open_without_range_streams_whole_object() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/media/full.bin"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(Bytes::from_static(b"whole")))
        .mount(&server)
        .await;
    let b = backend_for(server.uri());
    let src = b.open(&Key::from_validated("media/full.bin".into()), None).await.unwrap();
    assert_eq!(src.total_len, Some(5));
    assert_eq!(read_all(src).await, b"whole");
}

#[tokio::test]
async fn health_probe_uses_propfind() {
    let server = MockServer::start().await;
    Mock::given(method("PROPFIND"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(207).set_body_string(propfind_file_xml("\"e\"", 1)))
        .mount(&server)
        .await;
    let b = backend_for(server.uri());
    b.refresh_if_needed().await.unwrap();
}

#[tokio::test]
async fn unknown_type_is_rejected() {
    std::env::set_var("OPENLIST_USERNAME", "u");
    std::env::set_var("OPENLIST_PASSWORD", "p");
    let cfg = UpstreamConfig {
        id: "bad".into(),
        backend_type: "onedrive".into(),
        base_url: "http://127.0.0.1:5244/dav".into(),
        root_path: None,
        username_env: "OPENLIST_USERNAME".into(),
        password_env: "OPENLIST_PASSWORD".into(),
        accept_invalid_certs: false,
        cold_miss: ColdMiss::Proxy,
        link_api_token_env: None,
    };
    assert!(origin_cache::backend::OpenListBackend::from_config(&cfg).is_err());
}

/// Mount a link mock returning `link_url` for POST /api/fs/link.
async fn mock_link(server: &MockServer, link_url: &str) {
    Mock::given(method("POST"))
        .and(path("/api/fs/link"))
        .and(header("authorization", "test-token"))
        .respond_with(ResponseTemplate::new(200).set_body_string(format!(
            r#"{{"code":200,"message":"success","data":{{"url":{link_url:?},"header":{{}}}}}}"#
        )))
        .mount(server)
        .await;
}

/// Second mock server playing the foreign CDN: 206 answer to the 1-byte
/// probe (loopback http is allowed; foreign https in production).
async fn mock_cdn_206(cdn: &MockServer, body: &[u8]) {
    Mock::given(method("GET"))
        .and(path("/f.bin"))
        .and(header("range", "bytes=0-0"))
        .respond_with(
            ResponseTemplate::new(206)
                .insert_header("content-range", "bytes 0-0/10")
                .set_body_bytes(body.to_vec()),
        )
        .mount(cdn)
        .await;
}

#[tokio::test]
async fn direct_url_tier1_link_probed_ok() {
    let server = MockServer::start().await;
    let cdn = MockServer::start().await;
    mock_link(&server, &format!("{}/f.bin?sign=x", cdn.uri())).await;
    mock_cdn_206(&cdn, b"x").await;
    let b = backend_for(server.uri());
    let got: DirectUrl =
        b.direct_url(&Key::from_validated("f.bin".into()), Some("test-agent/1.0")).await.unwrap();
    assert_eq!(got.url, format!("{}/f.bin?sign=x", cdn.uri()));
}

#[tokio::test]
async fn direct_url_follows_probe_redirect_to_final() {
    let server = MockServer::start().await;
    let cdn = MockServer::start().await;
    mock_link(&server, &format!("{}/hop.bin", cdn.uri())).await;
    // Intermediate redirector: one hop, then the real 206.
    Mock::given(method("GET"))
        .and(path("/hop.bin"))
        .respond_with(ResponseTemplate::new(302).insert_header("location", "/real.bin"))
        .mount(&cdn)
        .await;
    Mock::given(method("GET"))
        .and(path("/real.bin"))
        .and(header("range", "bytes=0-0"))
        .respond_with(
            ResponseTemplate::new(206)
                .insert_header("content-range", "bytes 0-0/10")
                .set_body_bytes(b"x".to_vec()),
        )
        .mount(&cdn)
        .await;
    let b = backend_for(server.uri());
    let got: DirectUrl = b.direct_url(&Key::from_validated("f.bin".into()), None).await.unwrap();
    assert_eq!(got.url, format!("{}/real.bin", cdn.uri()));
}

#[tokio::test]
async fn direct_url_tier3_self_referential_falls_back() {
    let server = MockServer::start().await;
    // Proxy-backed storages answer with our own /p URL — not redirectable.
    mock_link(&server, &format!("{}/p/f.bin?d&sign=s", server.uri())).await;
    let b = backend_for(server.uri());
    let err = b.direct_url(&Key::from_validated("f.bin".into()), None).await.unwrap_err();
    assert!(matches!(err, BackendError::Other(_)), "got {err:?}");
}

#[tokio::test]
async fn direct_url_tier3_link_error_falls_back() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/fs/link"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"code":500,"message":"failed link: storage not found","data":null}"#,
        ))
        .mount(&server)
        .await;
    let b = backend_for(server.uri());
    let err = b.direct_url(&Key::from_validated("f.bin".into()), None).await.unwrap_err();
    assert!(matches!(err, BackendError::Other(_)), "got {err:?}");
}

#[tokio::test]
async fn direct_url_tier3_probe_without_range_falls_back() {
    let server = MockServer::start().await;
    let cdn = MockServer::start().await;
    mock_link(&server, &format!("{}/f.bin", cdn.uri())).await;
    // 200 to a Range probe = Range ignored → unusable for seeking viewers.
    Mock::given(method("GET"))
        .and(path("/f.bin"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"0123456789".to_vec()))
        .mount(&cdn)
        .await;
    let b = backend_for(server.uri());
    let err = b.direct_url(&Key::from_validated("f.bin".into()), None).await.unwrap_err();
    assert!(matches!(err, BackendError::Other(_)), "got {err:?}");
}

#[tokio::test]
async fn direct_url_without_token_is_unavailable() {
    let server = MockServer::start().await;
    std::env::set_var("OPENLIST_USERNAME", "u");
    std::env::set_var("OPENLIST_PASSWORD", "p");
    let mut cfg = upstream(server.uri());
    cfg.link_api_token_env = None;
    let b = origin_cache::backend::OpenListBackend::from_config(&cfg).unwrap();
    let err = b.direct_url(&Key::from_validated("f.bin".into()), None).await.unwrap_err();
    assert!(matches!(err, BackendError::Other(_)), "got {err:?}");
}

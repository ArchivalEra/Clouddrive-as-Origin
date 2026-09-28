//! The ticket endpoint (deploy/ticket-worker/) is the site backend's minting
//! half of ADR-0027 — the piece that must hold the tenant secret so the client
//! never does. Three things have to hold, and all of them run through `node`
//! (the Workers runtime's globals — crypto.subtle, Request, Response, URL —
//! are node globals from 18 on, so the worker runs unmodified under both):
//!
//! 1. the worker's mint is BYTE-IDENTICAL to the reference signer
//!    (deploy/oracle/presign.py), because two minters that disagree only in a
//!    canonicalisation corner would both "look fine" and one of them would 403
//!    in production;
//! 2. what the worker mints is ACCEPTED by the origin's gate — the real
//!    contract, not equality for its own sake;
//! 3. the HTTP behaviour matches what the blog client implemented against the
//!    agreed contract (same-origin only, host and prefix checks, a cookie that
//!    keeps one session per viewer, a rate valve, no-store).
//!
//! Without node the file prints why and skips; CI runs node, so the gate
//! cannot silently disappear there (the python signing contract's discipline).

use std::process::Command;
use std::sync::Arc;

use origin_cache::config::Config;
use origin_cache::content_auth::ContentGate;
use origin_cache::sigv4::CredentialStore;
use origin_front::{ContentAuth, ContentDecision, ContentRequest};

const WORKER: &str = "deploy/ticket-worker/worker.mjs";
const HOST: &str = "cdn.example";
const KEY: &str = "googledrive1/film.mkv";
const SESSION: &str = "viewer-1";
const TENANT_ID: &str = "tenant-a";
const TENANT_SECRET: &str = "secret-a";
const EXPIRES: u64 = 600;
const FIXED_DATE: &str = "20260926T000000Z";

fn repo(path: &str) -> String {
    format!("{}/{path}", env!("CARGO_MANIFEST_DIR"))
}

fn have(tool: &str) -> bool {
    Command::new("sh")
        .arg("-c")
        .arg(format!("command -v {tool} >/dev/null 2>&1"))
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Run a node module script. The worker's path arrives as `process.env.WORKER`
/// (a file URL), so the script never needs escaping.
fn run_node(script: &str) -> String {
    let worker = format!("file://{}", repo(WORKER));
    let out = Command::new("node")
        .arg("--input-type=module")
        .arg("-e")
        .arg(script)
        .env("WORKER", worker)
        .output()
        .expect("run node");
    assert!(
        out.status.success(),
        "node failed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// The gate as the binary builds it, with the tenant the worker is configured
/// for and the real clock (the worker mints with `now`, so expiry is honest).
fn gate() -> (ContentGate, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let creds = dir.path().join("content-auth.json");
    std::fs::write(&creds, format!(r#"[{{"id": "{TENANT_ID}", "secret": "{TENANT_SECRET}"}}]"#)).expect("write");
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&creds, std::fs::Permissions::from_mode(0o600)).expect("chmod 600");
    let toml = format!(
        r#"
front_content_auth = true
front_content_auth_max_expiry_secs = 3600
sigv4_credentials_path = "{}"
listen_addr = "127.0.0.1:0"
cache_dir = "{}"

[[upstreams]]
id = "media"
type = "openlist"
base_url = "http://127.0.0.1:5244/dav"
root_path = "media"
username_env = "TICKET_TEST_USER"
password_env = "TICKET_TEST_PASS"

[[routes]]
prefix = ""
upstream = "media"
"#,
        creds.display(),
        dir.path().join("cache").display()
    );
    let cfg = Config::from_toml_str(&toml).expect("the ticket test config must parse");
    let store = CredentialStore::load(Some(&creds.to_string_lossy()))
        .expect("load credentials")
        .expect("credentials present");
    (ContentGate::from_config(Arc::new(store), &cfg), dir)
}

fn path_and_query(url: &str) -> String {
    let after_scheme = url.split_once("://").expect("an absolute url").1;
    after_scheme[after_scheme.find('/').expect("a path")..].to_string()
}

fn allow(d: ContentDecision) -> (String, Option<String>) {
    match d {
        ContentDecision::Allow { credential, session } => (credential, session),
        ContentDecision::Deny { status, reason } => panic!("expected allow, got {status} {reason}"),
    }
}

/// Mint with the worker: one line of stdout, the presigned URL.
fn worker_mint(expires: u64, fixed_date: bool) -> String {
    let now = if fixed_date {
        r#"now: new Date("2026-09-26T00:00:00Z"),"#.to_string()
    } else {
        String::new()
    };
    run_node(&format!(
        r#"
const {{ signUrl }} = await import(process.env.WORKER);
console.log(await signUrl({{
    host: "{HOST}", key: "{KEY}", session: "{SESSION}", expires: {expires},
    accessKeyId: "{TENANT_ID}", secret: "{TENANT_SECRET}", {now}
}}));
"#
    ))
}

#[test]
fn the_worker_mints_byte_identical_urls_to_the_reference_signer() {
    if !have("node") {
        eprintln!("skipping: node not installed");
        return;
    }
    let python_url = Command::new("python3")
        .arg(repo("deploy/oracle/presign.py"))
        .args([
            "--scheme",
            "https",
            "--host",
            HOST,
            "--key",
            KEY,
            "--session",
            SESSION,
            "--expires",
            &EXPIRES.to_string(),
            "--id",
            TENANT_ID,
            "--secret",
            TENANT_SECRET,
            "--date",
            FIXED_DATE,
        ])
        .output()
        .expect("run presign.py");
    assert!(python_url.status.success(), "presign.py failed");
    let python_url = String::from_utf8_lossy(&python_url.stdout).trim().to_string();

    let worker_url = worker_mint(EXPIRES, true);
    assert_eq!(
        worker_url, python_url,
        "two minters that disagree in a canonicalisation corner both look fine and one of them 403s in production"
    );
}

#[test]
fn the_origin_accepts_what_the_worker_mints() {
    if !have("node") {
        eprintln!("skipping: node not installed");
        return;
    }
    let (gate, _dir) = gate();
    let url = worker_mint(EXPIRES, false);
    let (credential, session) = allow(gate.admit(&ContentRequest {
        method: "GET",
        path_and_query: &path_and_query(&url),
        host: Some(HOST),
    }));
    assert_eq!(credential, TENANT_ID);
    assert_eq!(session.as_deref(), Some(SESSION));
}

/// The HTTP contract the blog client implemented against. One node harness
/// drives the worker's handler through the sequence and prints observations;
/// the assertions live here, in the same file as the rest of the ladder.
#[test]
fn the_endpoint_contract_matches_what_the_blog_client_implements() {
    if !have("node") {
        eprintln!("skipping: node not installed");
        return;
    }
    let blob = run_node(
        r#"
const { handleTicketRequest } = await import(process.env.WORKER);
const env = {
    TICKET_HOST: "cdn.example",
    TICKET_ID: "tenant-a",
    TICKET_PREFIX: "googledrive1/",
    TICKET_SECRET: "secret-a",
    ALLOWED_ORIGIN: "https://site.example",
    TICKET_RATE_PER_MIN: "3",
};
const SRC = JSON.stringify({ src: "https://cdn.example/googledrive1/film.mkv" });
let now = Date.parse("2026-09-28T12:00:00Z");
const deps = { now: () => new Date(now) };
const call = async (method, headers, body) => {
    const r = await handleTicketRequest(new Request("https://worker.example/mp-ticket", { method, headers, body }), env, deps);
    const headers_out = {};
    r.headers.forEach((v, k) => (headers_out[k] = v));
    return { status: r.status, headers: headers_out, body: await r.text() };
};
const post = (extra, body = SRC) => call("POST", { "content-type": "application/json", ...extra }, body);
const ORIGIN = { origin: "https://site.example" };
const sessionOf = (body) => new URL(JSON.parse(body).url).searchParams.get("session");

const out = {};
out.wrongMethod = (await call("GET", ORIGIN)).status;
out.foreignOrigin = (await post({ origin: "https://elsewhere.example" })).status;
out.noOrigin = (await post({})).status;
out.wrongHost = (await post(ORIGIN, JSON.stringify({ src: "https://elsewhere.example/googledrive1/film.mkv" }))).status;
out.outsidePrefix = (await post(ORIGIN, JSON.stringify({ src: "https://cdn.example/archive/film.mkv" }))).status;
out.srcWithQuery = (await post(ORIGIN, JSON.stringify({ src: "https://cdn.example/googledrive1/film.mkv?x=1" }))).status;
out.badBody = (await post(ORIGIN, "not json")).status;

const first = await post(ORIGIN);
const setCookie = first.headers["set-cookie"] ?? null;
const cookie = setCookie?.split(";")[0] ?? null;
out.first = {
    status: first.status,
    setCookie,
    noStore: first.headers["cache-control"],
    urlHost: new URL(JSON.parse(first.body).url).host,
    urlSession: sessionOf(first.body),
    expiresAtIso: !Number.isNaN(Date.parse(JSON.parse(first.body).expiresAt)),
};

// Every later mint derives its session from the cookie: same viewer, same
// session, across as many refreshes as the client likes.
const renewed = await post({ ...ORIGIN, cookie }, SRC);
out.renewedSessionSame = sessionOf(renewed.body) === sessionOf(first.body);

// The valve: the deployment allows 3 mints/minute, the first mint (the one
// that created the session) already spent one, so of these four exactly the
// first is allowed and the rest are refused inside the same minute.
out.afterLimit = [];
for (let i = 0; i < 4; i++) out.afterLimit.push((await post({ ...ORIGIN, cookie }, SRC)).status);

console.log(JSON.stringify(out));
"#,
    );

    let v: serde_json::Value = serde_json::from_str(&blob).expect("the harness prints one JSON object");
    assert_eq!(v["wrongMethod"], 405, "only POST mints");
    assert_eq!(v["foreignOrigin"], 403, "a foreign origin learns nothing");
    assert_eq!(v["noOrigin"], 403, "no Origin and no Referer is not the site");
    assert_eq!(v["wrongHost"], 403, "src must name the host viewers type");
    assert_eq!(v["outsidePrefix"], 403, "the tenant is confined to its prefix");
    assert_eq!(v["srcWithQuery"], 400, "embeds are bare; a src with a query is a mistake");
    assert_eq!(v["badBody"], 400, "not JSON is not a ticket request");

    let first = &v["first"];
    assert_eq!(first["status"], 200);
    assert_eq!(first["noStore"], "no-store", "a ticket response must not be cached");
    assert_eq!(first["urlHost"], HOST);
    assert_eq!(first["expiresAtIso"], true, "the client schedules renewal by expiresAt");
    let cookie = first["setCookie"].as_str().expect("a first mint sets the session cookie");
    assert!(cookie.starts_with("ticket_session="), "the cookie carries the session id");
    for flag in ["HttpOnly", "Secure", "SameSite=Lax"] {
        assert!(cookie.contains(flag), "the session cookie must be {flag}: {cookie}");
    }
    // The first mint GENERATES the session: it sets the cookie and signs the
    // URL with the same value (that is the whole mechanism by which one cookie
    // means one session, and one session keeps its budget).
    let cookie_value = cookie
        .strip_prefix("ticket_session=")
        .and_then(|c| c.split(';').next())
        .expect("the cookie carries the session id");
    assert_eq!(first["urlSession"], serde_json::json!(cookie_value), "the URL is signed with the session the cookie now carries");

    assert_eq!(v["renewedSessionSame"], true, "one cookie = one session, across refreshes");
    // The valve's shape, deterministically: the per-minute count (3) minus the
    // mint that created the session leaves exactly one allowed mint in this
    // window, then every further mint in the same minute is refused.
    let codes = v["afterLimit"].as_array().expect("an array");
    assert_eq!(codes.len(), 4);
    assert_eq!(codes[0], 200, "the valve allows up to the per-minute count");
    for code in &codes[1..] {
        assert_eq!(code, 429, "inside the minute, over budget is refused");
    }
}

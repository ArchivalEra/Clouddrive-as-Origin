//! The signing contract, executed: the signer against the gate, and the
//! documentation against the taxonomy.
//!
//! `src/signing.rs` states what a signed content read must carry and why one
//! may be refused; the verifier and the gate enforce it; `presign.py` (outside
//! the binary, on purpose — the caller signs locally) mints the URLs; and
//! `docs/signing.md` explains them to the people who will hit a refusal. Five
//! statements of one protocol, and until this file existed nothing tied them
//! together: the signer could mint a URL the gate always refused, and a
//! refusal could ship undocumented. Both are now test failures.
//!
//! Python is required for the tool half. Where it is missing the tool tests
//! skip with a printed reason (the house rule for external tools, see
//! `tests/watchdog_script.rs`); CI runs a `python3 --version` step so the gate
//! cannot quietly disappear there. The documentation half needs nothing and
//! always runs.

use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::process::Command;
use std::sync::Arc;

use origin_cache::config::Config;
use origin_cache::content_auth::ContentGate;
use origin_cache::signing::{Reason, PROTOCOL_MAX_EXPIRES_SECS};
use origin_cache::sigv4::CredentialStore;
use origin_front::{ContentAuth, ContentDecision, ContentRequest};

const TENANT_A: &str = "tenant-a";
const SECRET_A: &str = "secret-a";
const TENANT_B: &str = "tenant-b";
const SECRET_B: &str = "secret-b";
const CAP_SECS: u64 = 300;
const HOST: &str = "cdn.example";

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

/// A credentials file the way the operator writes one: 0600, one JSON object
/// per tenant. Written into a temp dir; the store parses it at load, so the
/// file need not outlive this call.
fn credentials_file() -> std::path::PathBuf {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.keep().join("content-auth.json");
    let body = format!(
        r#"[
 {{"id": "{TENANT_A}", "secret": "{SECRET_A}"}},
 {{"id": "{TENANT_B}", "secret": "{SECRET_B}", "prefix": "media/", "session_rps": 5}}
]"#
    );
    let mut f = std::fs::File::create(&path).expect("create credentials file");
    f.write_all(body.as_bytes()).expect("write credentials");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).expect("chmod 600");
    path
}

/// The gate as the binary builds it: a TOML config parsed by the real
/// `Config` (so the config→gate mapping is under test, not bypassed), a
/// credential store loaded from a 0600 file, and the caps the config names.
fn gate_from_config(rps: u32, mib_per_min: u64) -> (ContentGate, tempfile::TempDir) {
    let creds = credentials_file();
    let dir = tempfile::tempdir().expect("tempdir");
    let toml = format!(
        r#"
front_content_auth = true
front_content_auth_max_expiry_secs = {CAP_SECS}
front_content_auth_session_rps = {rps}
front_content_auth_session_mib_per_min = {mib_per_min}
sigv4_credentials_path = "{}"
listen_addr = "127.0.0.1:0"
cache_dir = "{}"

[[upstreams]]
id = "media"
type = "openlist"
base_url = "http://127.0.0.1:5244/dav"
root_path = "media"
username_env = "CONTRACT_TEST_USER"
password_env = "CONTRACT_TEST_PASS"

[[routes]]
prefix = ""
upstream = "media"
"#,
        creds.display(),
        dir.path().join("cache").display()
    );
    let cfg = Config::from_toml_str(&toml).expect("the contract test config must parse");
    let store = CredentialStore::load(Some(&creds.to_string_lossy()))
        .expect("load credentials")
        .expect("credentials present");
    (ContentGate::from_caps(Arc::new(store), &cfg.content_caps()), dir)
}

/// Run the real signer. `Ok(url)` on success; `Err(stderr)` when it refuses.
fn presign(args: &[&str]) -> Result<String, String> {
    let out = Command::new("python3")
        .arg(repo("deploy/oracle/presign.py"))
        .args(args)
        .output()
        .expect("run presign.py");
    if out.status.success() {
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    } else {
        Err(String::from_utf8_lossy(&out.stderr).to_string())
    }
}

/// Mint a URL with the usual arguments (host, key, session, tenant A).
fn mint(method: &str, key: &str, session: &str, expires: u64) -> String {
    presign(&[
        "--scheme", "http", "--host", HOST, "--key", key, "--method", method,
        "--session", session, "--expires", &expires.to_string(),
        "--id", TENANT_A, "--secret", SECRET_A,
    ])
    .expect("the signer must mint this URL")
}

/// The URL's path-and-query, which is what the gate sees (the scheme and host
/// travel separately as the Host header).
fn path_and_query(url: &str) -> &str {
    let after_scheme = url.split_once("://").expect("scheme").1;
    let at = after_scheme.find('/').expect("path");
    &after_scheme[at..]
}

fn ask(gate: &ContentGate, method: &str, url: &str) -> ContentDecision {
    gate.admit(&ContentRequest { method, path_and_query: path_and_query(url), host: Some(HOST) })
}

fn allowed_credential(d: &ContentDecision) -> &str {
    match d {
        ContentDecision::Allow { credential, .. } => credential,
        ContentDecision::Deny { status, reason } => panic!("expected allow, got {status} {reason}"),
    }
}

fn refused_reason(d: &ContentDecision) -> &'static str {
    match d {
        ContentDecision::Allow { .. } => panic!("expected a refusal"),
        ContentDecision::Deny { reason, .. } => reason,
    }
}

/// The happy path, end to end through the tool and the gate, and the identity
/// the access log will carry.
#[test]
fn the_tool_mints_a_url_the_gate_accepts() {
    let (gate, _dir) = gate_from_config(30, 1024);
    if !have("python3") {
        eprintln!("skipping the signer half: python3 not available");
        return;
    }
    let url = mint("GET", "googledrive1/film.mkv", "viewer-1", 60);
    assert_eq!(allowed_credential(&ask(&gate, "GET", &url)), TENANT_A);
}

/// SigV4 signs the HTTP method, so the two methods need two tickets — the
/// trap a player hits when it probes with HEAD before reading.
#[test]
fn head_and_get_are_separate_tickets() {
    let (gate, _dir) = gate_from_config(30, 1024);
    if !have("python3") {
        eprintln!("skipping the signer half: python3 not available");
        return;
    }
    let head_url = mint("HEAD", "googledrive1/film.mkv", "viewer-1", 60);
    let get_url = mint("GET", "googledrive1/film.mkv", "viewer-1", 60);
    allowed_credential(&ask(&gate, "HEAD", &head_url));
    assert_eq!(refused_reason(&ask(&gate, "HEAD", &get_url)), Reason::SignatureMismatch.as_str());
    assert_eq!(refused_reason(&ask(&gate, "GET", &head_url)), Reason::SignatureMismatch.as_str());
}

/// The tool cannot mint a URL the gate always refuses: no session, no URL.
/// `--no-session` produces the refused shape on purpose, which is how the
/// refusal itself stays testable.
#[test]
fn the_tool_refuses_to_mint_without_a_session() {
    if !have("python3") {
        eprintln!("skipping the signer half: python3 not available");
        return;
    }
    let err = presign(&[
        "--scheme", "http", "--host", HOST, "--key", "k", "--expires", "60",
        "--id", TENANT_A, "--secret", SECRET_A,
    ])
    .expect_err("a sessionless mint must fail");
    assert!(err.contains("--session is required"), "{err}");

    let (gate, _dir) = gate_from_config(30, 1024);
    let url = presign(&[
        "--scheme", "http", "--host", HOST, "--key", "k", "--no-session", "--expires", "60",
        "--id", TENANT_A, "--secret", SECRET_A,
    ])
    .expect("--no-session mints the refused shape");
    assert_eq!(refused_reason(&ask(&gate, "GET", &url)), Reason::MissingSession.as_str());
}

/// Every way a signature can be wrong lands on the same refusal, and a host
/// mismatch is one of them (the signature covers the Host header).
#[test]
fn wrong_secret_wrong_host_and_edits_are_refused() {
    let (gate, _dir) = gate_from_config(30, 1024);
    if !have("python3") {
        eprintln!("skipping the signer half: python3 not available");
        return;
    }
    let wrong_secret = presign(&[
        "--scheme", "http", "--host", HOST, "--key", "k", "--session", "s", "--expires", "60",
        "--id", TENANT_A, "--secret", "not-the-secret",
    ])
    .expect("minting with a wrong secret still produces a URL");
    assert_eq!(refused_reason(&ask(&gate, "GET", &wrong_secret)), Reason::SignatureMismatch.as_str());

    let wrong_host = presign(&[
        "--scheme", "http", "--host", "elsewhere.example", "--key", "k", "--session", "s",
        "--expires", "60", "--id", TENANT_A, "--secret", SECRET_A,
    ])
    .expect("minting for another host produces a URL");
    assert_eq!(refused_reason(&ask(&gate, "GET", &wrong_host)), Reason::SignatureMismatch.as_str());

    let url = mint("GET", "k", "s", 60);
    let marker = "X-Amz-Signature=";
    let at = url.find(marker).expect("signature in the URL") + marker.len();
    let mut bytes = url.into_bytes();
    bytes[at] = if bytes[at] == b'a' { b'b' } else { b'a' };
    let tampered = String::from_utf8(bytes).expect("utf8");
    assert_eq!(refused_reason(&ask(&gate, "GET", &tampered)), Reason::SignatureMismatch.as_str());
}

/// The two clocks: past the URL's window it is dead, and a URL that asks for
/// longer than the deployment allows is refused even though the protocol would
/// accept it.
#[test]
fn expiry_is_enforced_both_ways() {
    let (gate, _dir) = gate_from_config(30, 1024);
    if !have("python3") {
        eprintln!("skipping the signer half: python3 not available");
        return;
    }
    let old = (chrono::Utc::now() - chrono::Duration::seconds(400)).format("%Y%m%dT%H%M%SZ").to_string();
    let expired = presign(&[
        "--scheme", "http", "--host", HOST, "--key", "k", "--session", "s", "--expires", "300",
        "--id", TENANT_A, "--secret", SECRET_A, "--date", &old,
    ])
    .expect("mint a stale URL");
    assert_eq!(refused_reason(&ask(&gate, "GET", &expired)), Reason::RequestExpired.as_str());

    let over_cap = mint("GET", "k", "s", CAP_SECS + 1);
    assert_eq!(refused_reason(&ask(&gate, "GET", &over_cap)), Reason::ExpiresTooLong.as_str());
    // Exactly at the cap is fine (off-by-one guard).
    let at_cap = mint("GET", "k", "s", CAP_SECS);
    allowed_credential(&ask(&gate, "GET", &at_cap));
    // Over the PROTOCOL maximum the verifier refuses before the gate's cap.
    let over_protocol = mint("GET", "k", "s", PROTOCOL_MAX_EXPIRES_SECS + 1);
    assert_eq!(refused_reason(&ask(&gate, "GET", &over_protocol)), Reason::ExpiresCapExceeded.as_str());
}

/// The gate built straight from `ContentGate::new` clamps its cap to the
/// protocol maximum, so a nonsensical value cannot widen the window.
#[test]
fn the_gate_clamps_its_cap_to_the_protocol_maximum() {
    if !have("python3") {
        eprintln!("skipping the signer half: python3 not available");
        return;
    }
    let creds = credentials_file();
    let store = CredentialStore::load(Some(&creds.to_string_lossy())).expect("load").expect("present");
    let gate = ContentGate::new(Arc::new(store), u64::MAX, 30, 1024);
    let at_protocol_max = mint("GET", "k", "s", PROTOCOL_MAX_EXPIRES_SECS);
    allowed_credential(&ask(&gate, "GET", &at_protocol_max));
}

/// A tenant reads only under its prefix: the grant is checked before routing,
/// so a confined key cannot even probe for another site's objects.
#[test]
fn a_tenants_prefix_is_enforced() {
    let (gate, _dir) = gate_from_config(30, 1024);
    if !have("python3") {
        eprintln!("skipping the signer half: python3 not available");
        return;
    }
    let inside = presign(&[
        "--scheme", "http", "--host", HOST, "--key", "media/film.mkv", "--session", "s",
        "--expires", "60", "--id", TENANT_B, "--secret", SECRET_B,
    ])
    .expect("mint inside the prefix");
    allowed_credential(&ask(&gate, "GET", &inside));
    let outside = presign(&[
        "--scheme", "http", "--host", HOST, "--key", "googledrive1/other.mkv", "--session", "s",
        "--expires", "60", "--id", TENANT_B, "--secret", SECRET_B,
    ])
    .expect("mint outside the prefix");
    assert_eq!(refused_reason(&ask(&gate, "GET", &outside)), Reason::OutsideKeyPrefix.as_str());
}

/// The flood control, through the gate the binary builds: a session over its
/// request budget is slowed with the S3-standard 503, and the byte budget
/// binds on the bytes the origin actually sent.
#[test]
fn session_budgets_slow_a_flood_and_recover() {
    let (gate, _dir) = gate_from_config(2, 1);
    if !have("python3") {
        eprintln!("skipping the signer half: python3 not available");
        return;
    }
    // Two requests a second for tenant A (tenant B has its own rps in the
    // credentials file, which is the per-tenant override doing its job).
    //
    // The window is wall-clock, so the test fires a burst and requires at
    // least one slowdown rather than a specific request index: minting spawns
    // a process per URL, and on a busy box a pair can straddle a second and
    // reset the counter. The LAB arm asserts the same way (twelve rapid reads,
    // at least one 503).
    let mut allowed = 0;
    let mut slowed = 0;
    for _ in 0..10 {
        let url = mint("GET", "k", "flood", 60);
        match ask(&gate, "GET", &url) {
            ContentDecision::Allow { .. } => allowed += 1,
            ContentDecision::Deny { status, reason } => {
                assert_eq!(reason, Reason::SessionRateExceeded.as_str());
                assert_eq!(status, 503, "an over-budget session is slowed, not forbidden");
                slowed += 1;
            }
        }
    }
    assert!(slowed >= 1, "ten rapid reads at 2 req/s must trip the budget at least once");
    assert!(allowed >= 1, "the budget must not refuse everything");

    // The window slides: after a pause the same session is served again.
    std::thread::sleep(std::time::Duration::from_millis(1200));
    let url = mint("GET", "k", "flood", 60);
    allowed_credential(&ask(&gate, "GET", &url));

    // Byte budget (1 MiB/min for tenant A): a response of 2 MiB exhausts it.
    let bytes_session = "bytes";
    let url = mint("GET", "k", bytes_session, 60);
    allowed_credential(&ask(&gate, "GET", &url));
    gate.observe(TENANT_A, Some(bytes_session), 2 * 1024 * 1024);
    let next = mint("GET", "k", bytes_session, 60);
    assert_eq!(refused_reason(&ask(&gate, "GET", &next)), Reason::SessionBytesExceeded.as_str());
}

/// The documentation table is a fixture: every reason an integrator can act on
/// is named in `docs/signing.md` with the status the code answers, and the
/// document names nothing the code cannot produce (a stale row is rot too).
#[test]
fn the_documentation_table_matches_the_taxonomy() {
    let doc = std::fs::read_to_string(repo("docs/signing.md")).expect("read docs/signing.md");
    let mut rows: Vec<(String, String)> = Vec::new();
    for line in doc.lines() {
        let cells: Vec<&str> = line.split('|').map(str::trim).collect();
        // Table rows are "| 403 | `reason` | ... |" — cells[0] is empty. One
        // cell may name two reasons separated by "/" (the doc groups the two
        // session-marker failures, which share a remedy); each is a row here.
        if cells.len() >= 4 && (cells[1] == "403" || cells[1] == "503") {
            for token in cells[2].split('/') {
                let reason = token.trim().trim_matches('`').trim();
                if !reason.is_empty() {
                    rows.push((cells[1].to_string(), reason.to_string()));
                }
            }
        }
    }

    let documented: Vec<Reason> = Reason::ALL.iter().copied().filter(|r| r.documented()).collect();
    let mut missing: Vec<&str> =
        documented.iter().map(|r| r.as_str()).filter(|s| !rows.iter().any(|(_, r)| r == s)).collect();
    missing.sort_unstable();
    assert!(
        missing.is_empty(),
        "these refusals can reach a caller but are not in docs/signing.md: {missing:?}"
    );

    let known: Vec<&str> = Reason::ALL.iter().map(|r| r.as_str()).collect();
    let stale: Vec<&str> = rows
        .iter()
        .map(|(_, r)| r.as_str())
        .filter(|r| !known.contains(r))
        .collect();
    assert!(stale.is_empty(), "docs/signing.md documents reasons the code cannot produce: {stale:?}");

    for (status, reason) in &rows {
        let code = Reason::ALL
            .iter()
            .find(|r| r.as_str() == reason)
            .unwrap_or_else(|| panic!("no Reason spells {reason:?}"));
        assert_eq!(
            status,
            &code.status().to_string(),
            "docs/signing.md says {status} for {reason:?} but the code answers {}",
            code.status()
        );
    }
}

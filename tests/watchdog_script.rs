//! The deployment scripts are part of production: `watchdog.sh` is the
//! node's only reporter and its only death detector. Its rules used to live
//! in a doc and be verified by hand, which is how the release that shipped
//! them also shipped a false-alarm path.
//!
//! The behaviour lives in a shell script, so the test runs the real script
//! against stubs (see `deploy/oracle/test-watchdog.sh`) rather than
//! reimplementing its logic in Rust. It is skipped, not failed, where the
//! harness tools are missing: `bash` on Windows, or `jq` absent — though
//! `jq` is also required on the node, so its absence is worth knowing about.

use std::process::Command;

fn have(tool: &str) -> bool {
    Command::new("sh")
        .arg("-c")
        .arg(format!("command -v {tool} >/dev/null 2>&1"))
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

#[test]
fn watchdog_script_behaviour() {
    let script = concat!(env!("CARGO_MANIFEST_DIR"), "/deploy/oracle/test-watchdog.sh");
    if !std::path::Path::new(script).exists() {
        eprintln!("skipping: {script} not present");
        return;
    }
    for tool in ["bash", "jq", "curl"] {
        if !have(tool) {
            eprintln!("skipping watchdog script test: {tool} not available");
            return;
        }
    }

    let out = Command::new("bash")
        .arg(script)
        .output()
        .expect("run the watchdog test script");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "watchdog behaviour test failed\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}"
    );
    // A script that silently does nothing would otherwise pass.
    assert!(
        stdout.contains("PASS=") && !stdout.contains("SKIP:"),
        "the watchdog test did not actually run:\n{stdout}"
    );
}

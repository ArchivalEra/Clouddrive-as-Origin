//! The viewer bundle is a product of this repository, and this is the gate
//! that keeps it one.
//!
//! The bundle shipped to the machine that runs viewer load tests used to be a
//! hand-maintained copy of the driver, the report and the shard tools, plus a
//! handful of wrapper scripts that existed only in the copy. It drifted (a
//! driver was edited on the far side and back-ported by hand), and the repo
//! grew tolerance code for a page variant that lived nowhere else.
//!
//! Now `deploy/lab/viewer/make-bundle.sh` assembles it from the repo, and this
//! test exercises the generator the way an operator would: build it, verify its
//! manifest, ask it whether an existing copy is current, and refuse to write
//! inside the repo. It also asserts the generated tree carries no deployment
//! identifier — the bundle is shipped to a box, so any hostname that leaked
//! into it would travel.
//!
//! Skips (with a printed reason) where `bash`/`sha256sum` are missing, the
//! house rule for tests that drive external tools (`tests/watchdog_script.rs`);
//! CI runs them on every push.

use std::process::Command;

fn have(tool: &str) -> bool {
    Command::new("sh")
        .arg("-c")
        .arg(format!("command -v {tool} >/dev/null 2>&1"))
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn run(args: &[&str]) -> std::process::Output {
    Command::new("bash")
        .arg(format!("{}/deploy/lab/viewer/make-bundle.sh", env!("CARGO_MANIFEST_DIR")))
        .args(args)
        .output()
        .expect("run make-bundle.sh")
}

fn manifest_hash(dir: &str) -> String {
    let out = Command::new("sha256sum")
        .arg(format!("{dir}/MANIFEST.sha256"))
        .output()
        .expect("sha256sum");
    String::from_utf8_lossy(&out.stdout).split_whitespace().next().unwrap_or("").to_string()
}

#[test]
fn the_generator_builds_a_verifiable_bundle_and_checks_it() {
    for tool in ["bash", "sha256sum", "sed", "diff"] {
        if !have(tool) {
            eprintln!("skipping the bundle generator test: {tool} not available");
            return;
        }
    }
    let out = tempfile::tempdir().expect("tempdir");
    let dir = out.path().join("bundle");
    let dir_s = dir.to_string_lossy().to_string();

    // Build.
    let built = run(&[&dir_s]);
    assert!(
        built.status.success(),
        "generator failed:\n{}\n{}",
        String::from_utf8_lossy(&built.stdout),
        String::from_utf8_lossy(&built.stderr)
    );

    // The pieces the README tells the operator to run must exist, with the
    // names it uses (the generator is where the repo name and the bundle name
    // are reconciled).
    for f in [
        "swarm.mjs",
        "report.mjs",
        "video-page.html",
        "run.sh",
        "preflight.sh",
        "detach.sh",
        "monitor.sh",
        "install.sh",
        "shard-sweep.sh",
        "presign.py",
        "origin-side/shard-sweep-origin.sh",
        "README.md",
        "MANIFEST.sha256",
    ] {
        assert!(dir.join(f).is_file(), "the bundle is missing {f}");
    }

    // The product must be runnable where it lands: a syntax check for every
    // interpreter the bundle uses (skipped individually when the tool is
    // missing — the same rule as above).
    for f in ["swarm.mjs", "report.mjs"] {
        if have("node") {
            let out = Command::new("node").arg("--check").arg(dir.join(f)).output().expect("node --check");
            assert!(out.status.success(), "node --check {f}:\n{}", String::from_utf8_lossy(&out.stderr));
        }
    }
    for f in ["run.sh", "preflight.sh", "detach.sh", "monitor.sh", "install.sh", "shard-sweep.sh", "origin-side/shard-sweep-origin.sh"] {
        let out = Command::new("bash").arg("-n").arg(dir.join(f)).output().expect("bash -n");
        assert!(out.status.success(), "bash -n {f}:\n{}", String::from_utf8_lossy(&out.stderr));
    }
    if have("python3") {
        let out = Command::new("python3")
            .args(["-c", "import ast,sys; ast.parse(open(sys.argv[1]).read())"])
            .arg(dir.join("presign.py"))
            .output()
            .expect("python3 parse");
        assert!(out.status.success(), "presign.py does not parse:\n{}", String::from_utf8_lossy(&out.stderr));
    }

    // Its own manifest must verify (the same command the README gives).
    let verify = Command::new("sha256sum")
        .arg("-c")
        .arg("MANIFEST.sha256")
        .current_dir(&dir)
        .output()
        .expect("sha256sum -c");
    assert!(
        verify.status.success(),
        "the generated manifest does not verify:\n{}",
        String::from_utf8_lossy(&verify.stdout)
    );

    // `--check` says an identical copy is current...
    let checked = run(&["--check", &dir_s]);
    assert!(
        checked.status.success(),
        "--check rejected a freshly generated copy:\n{}",
        String::from_utf8_lossy(&checked.stderr)
    );

    // ... and says so when it is not (this is the drift detector: the far-side
    // copy is compared against the repo, not against memory).
    let drift = dir.join("swarm.mjs");
    let mut text = std::fs::read_to_string(&drift).expect("read generated driver");
    text.push_str("\n// drift\n");
    std::fs::write(&drift, text).expect("write drift");
    let stale = run(&["--check", &dir_s]);
    assert!(!stale.status.success(), "--check accepted a modified copy");
    assert!(
        String::from_utf8_lossy(&stale.stderr).contains("swarm.mjs"),
        "the drift report should name the file:\n{}",
        String::from_utf8_lossy(&stale.stderr)
    );

    // No deployment identifier travels with the bundle, and this is how that is
    // kept true after the 2026-09-28 history scrub removed the last one: every
    // host the shipped files name must be loopback (their own page server),
    // elided (`https://...`), a documentation placeholder (`your-cdn-host`, or
    // a `{template}` the caller fills, as in presign.py), or on the short list
    // below. Anything else is somebody's deployment getting into a copy that is
    // handed to another machine -- a deliberate decision, so it fails here
    // first.
    const ALLOWED_HOSTS: [&str; 3] = ["127.0.0.1", "localhost", "nodejs.org"];
    for entry in walk(&dir) {
        let text = std::fs::read_to_string(&entry).unwrap_or_default();
        for (at, _) in text.match_indices("://") {
            let host: String = text[at + 3..].chars().take_while(|c| !"/\"' \n)".contains(*c)).collect();
            let named = ALLOWED_HOSTS.iter().any(|h| host.starts_with(h));
            assert!(
                named || ["...", "{", "your-"].iter().any(|p| host.starts_with(p)),
                "{} names a host that is not loopback, elided, a placeholder or allow-listed: {host}",
                entry.display()
            );
        }
    }
}

#[test]
fn the_generator_refuses_to_write_inside_the_repo() {
    if !have("bash") {
        eprintln!("skipping: bash not available");
        return;
    }
    // Both spellings: the bundle is deliberately not a repo artefact, and the
    // first version of the check let a relative path through.
    for target in [
        format!("{}/deploy/lab/viewer/out-should-not-exist", env!("CARGO_MANIFEST_DIR")),
        "deploy/lab/viewer/out-should-not-exist".to_string(),
    ] {
        let out = run(&[&target]);
        assert!(!out.status.success(), "the generator accepted {target}");
        assert!(
            String::from_utf8_lossy(&out.stderr).contains("refusing"),
            "expected a refusal, got: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    assert!(
        !std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/deploy/lab/viewer/out-should-not-exist"
        ))
        .exists(),
        "the generator created a directory inside the repo"
    );
}

#[test]
fn the_generated_names_are_the_repo_names_renamed() {
    if !have("bash") {
        eprintln!("skipping: bash not available");
        return;
    }
    let out = tempfile::tempdir().expect("tempdir");
    let dir = out.path().join("bundle");
    assert!(run(&[&dir.to_string_lossy()]).status.success());

    // The driver is the repo's driver, renamed (a rename rule that stops
    // matching must fail the build, which the generator asserts internally).
    let repo_driver = std::fs::read_to_string(format!(
        "{}/deploy/lab/viewer/player-swarm.mjs",
        env!("CARGO_MANIFEST_DIR")
    ))
    .expect("repo driver");
    let bundled = std::fs::read_to_string(dir.join("swarm.mjs")).expect("bundled driver");
    assert_eq!(
        bundled,
        repo_driver.replace("node player-swarm.mjs", "node swarm.mjs"),
        "the bundle's driver is not the repo's driver with the documented rename"
    );
    // And the manifest hash is stable across two builds of the same tree.
    let dir2 = out.path().join("bundle2");
    assert!(run(&[&dir2.to_string_lossy()]).status.success());
    assert_eq!(
        manifest_hash(&dir.to_string_lossy()),
        manifest_hash(&dir2.to_string_lossy()),
        "two builds of the same repo produced different manifests"
    );
}

fn walk(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut files = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for entry in std::fs::read_dir(&d).expect("read_dir") {
            let path = entry.expect("dir entry").path();
            if path.is_dir() {
                stack.push(path);
            } else {
                files.push(path);
            }
        }
    }
    files
}

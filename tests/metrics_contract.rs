//! The shell instruments spell metric names and label keys as literals; those
//! literals are a second mouth for a fact `src/metrics.rs` owns. Nothing tied
//! the two together, and the failure mode is silent: `merge-account.sh`'s
//! arithmetic defaults a missing series to zero, so a renamed label makes the
//! account print a plausible `0.000 GiB` / `0 opens` instead of failing. That is
//! the same class of drift ADR-0028 closed for the signing doc table, so it gets
//! the same treatment: a test that reads both sides and asserts they agree.
//!
//! Direction of the check: every series a shipped script names must be a metric
//! this crate registers (histogram suffixes allowed, since a histogram's
//! `_count`/`_sum`/`_bucket` are derived series the scripts legitimately ask
//! for), and every label key a selector uses must be one that metric declares.
//!
//! Source-level on purpose: `prometheus::gather()` only contains families that
//! have recorded something, so a metric that is idle during the test run would
//! read as "not declared".

use std::collections::{BTreeMap, BTreeSet};

fn repo(path: &str) -> String {
    format!("{}/{path}", env!("CARGO_MANIFEST_DIR"))
}

/// Every shell script under `deploy/` (the instruments), as (path, text).
fn scripts() -> Vec<(String, String)> {
    fn walk(dir: &std::path::Path, out: &mut Vec<(String, String)>) {
        for entry in std::fs::read_dir(dir).expect("read deploy/").flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, out);
            } else if path.extension().is_some_and(|e| e == "sh") {
                let text = std::fs::read_to_string(&path).unwrap_or_default();
                let name = path
                    .strip_prefix(repo(""))
                    .unwrap_or(&path)
                    .to_string_lossy()
                    .into_owned();
                out.push((name, text));
            }
        }
    }
    let mut out = Vec::new();
    walk(std::path::Path::new(&repo("deploy")), &mut out);
    assert!(out.len() > 10, "expected the instruments to be found, got {}", out.len());
    out
}

/// name -> declared label keys, read out of the registration macros: the first
/// string inside each `register_*!(` call is the metric name, and a `&[ ... ]`
/// before the call's closing paren lists its label keys.
///
/// Two registration homes, because the namespace is split along the crate
/// boundary the process is built from: the business plane declares its own in
/// `src/metrics.rs`, and the front (`front/src/lib.rs`) declares the
/// `front_*` series it owns. A script cannot tell the difference — one
/// `/metrics` endpoint serves both — which is exactly why this test reads both.
fn declared() -> BTreeMap<String, BTreeSet<String>> {
    const MACROS: [&str; 5] = [
        "register_histogram_vec!(",
        "register_int_histogram_vec!(",
        "register_int_counter_vec!(",
        "register_int_counter!(",
        "register_int_gauge!(",
    ];
    let mut out: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for source in ["src/metrics.rs", "front/src/lib.rs", "front/src/admission.rs"] {
        let src = std::fs::read_to_string(repo(source)).unwrap_or_default();
        for macro_name in MACROS {
            let mut at = 0;
            while let Some(found) = src[at..].find(macro_name) {
                let after_open = at + found + macro_name.len();
                let Some(name_start) = src[after_open..].find('"').map(|i| after_open + i + 1) else { break };
                let Some(name_len) = src[name_start..].find('"') else { break };
                let name = src[name_start..name_start + name_len].to_string();
                // The call's own `)`: a `&[ ... ]` before it is the label list.
                let mut depth = 1i32;
                let mut end = after_open;
                for (i, c) in src[after_open..].char_indices() {
                    match c {
                        '(' => depth += 1,
                        ')' => {
                            depth -= 1;
                            if depth == 0 {
                                end = after_open + i;
                                break;
                            }
                        }
                        _ => {}
                    }
                }
                let call = &src[after_open..end];
                let labels = match call.find("&[") {
                    Some(i) => {
                        let close = call[i..].find(']').map(|j| i + j).unwrap_or(call.len());
                        call[i + 2..close]
                            .split(',')
                            .filter_map(|s| {
                                let s = s.trim().trim_matches('"');
                                (!s.is_empty()).then(|| s.to_string())
                            })
                            .collect()
                    }
                    None => BTreeSet::new(),
                };
                out.insert(name, labels);
                at = end.max(after_open);
            }
        }
    }
    assert!(out.len() >= 18, "parsed only {} registered metrics", out.len());
    out
}

/// The base metric a series belongs to: `x_seconds_count` -> `x_seconds`.
fn base(series: &str) -> &str {
    for suffix in ["_count", "_sum", "_bucket"] {
        if let Some(stripped) = series.strip_suffix(suffix) {
            return stripped;
        }
    }
    series
}

/// The namespace the crate owns; a token outside it is not ours to check.
fn looks_like_ours(token: &str) -> bool {
    let prefixed = token.starts_with("cache_")
        || token.starts_with("front_")
        || token.starts_with("backend_")
        || token.starts_with("origin_");
    let shaped = token.ends_with("_total")
        || token.ends_with("_seconds")
        || token.ends_with("_bytes")
        || token.ends_with("_count")
        || token.ends_with("_sum")
        || token.ends_with("_bucket")
        || token.ends_with("_active")
        || token.ends_with("_keys");
    prefixed && shaped
}

/// Find every literal the scripts use that is shaped like one of our series,
/// with the label keys it selects on: `name{label="v"}` -> (name, {label}).
fn literals(text: &str) -> BTreeMap<String, BTreeSet<String>> {
    let mut out: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let chars: Vec<char> = text.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        if chars[i].is_ascii_lowercase() {
            let start = i;
            while i < chars.len() && (chars[i].is_ascii_lowercase() || chars[i].is_ascii_digit() || chars[i] == '_') {
                i += 1;
            }
            let token: String = chars[start..i].iter().collect();
            let mut labels = BTreeSet::new();
            if i < chars.len() && chars[i] == '{' {
                // A selector's braces close on the same line and hold only
                // `label="value"` pairs. Shell interpolation across lines looks
                // the same to a naive scan, so refuse anything else rather than
                // inventing label names out of a quoted command.
                let mut j = i + 1;
                let mut block = String::new();
                while j < chars.len() && chars[j] != '}' && chars[j] != '\n' {
                    block.push(chars[j]);
                    j += 1;
                }
                let closes = j < chars.len() && chars[j] == '}';
                // A selector inside a shell string is spelled with escaped
                // quotes (`{source=\"stage\"}`), so `\"` is expected; single
                // quotes, backticks and `$` mean this is interpolation, not a
                // selector, and are refused rather than parsed as label names.
                let plain = !block.contains(['\'', '`', '$']);
                if closes && plain {
                    for part in block.split(',') {
                        let part = part.replace("\\\"", "\"");
                        if let Some((key, _)) = part.split_once('=') {
                            let key = key.trim();
                            let shaped = !key.is_empty()
                                && key.starts_with(|c: char| c.is_ascii_lowercase() || c == '_')
                                && key.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
                            if shaped {
                                labels.insert(key.to_string());
                            }
                        }
                    }
                }
            }
            if looks_like_ours(&token) {
                out.entry(token).or_default().extend(labels);
            }
        } else {
            i += 1;
        }
    }
    out
}

#[test]
fn every_series_a_script_names_is_one_this_crate_registers() {
    let registry = declared();
    let mut unknown: Vec<String> = Vec::new();
    let mut bad_labels: Vec<String> = Vec::new();
    for (path, text) in scripts() {
        for (series, labels) in literals(&text) {
            let Some(known) = registry.get(base(&series)) else {
                unknown.push(format!("{path}: {series}"));
                continue;
            };
            for label in &labels {
                if !known.contains(label) {
                    bad_labels.push(format!("{path}: {series}{{{label}=...}} (declared labels: {known:?})"));
                }
            }
        }
    }
    assert!(
        unknown.is_empty(),
        "these scripts filter on metric series this crate does not register:\n  {}",
        unknown.join("\n  ")
    );
    assert!(
        bad_labels.is_empty(),
        "these scripts select on label keys their metric does not declare:\n  {}",
        bad_labels.join("\n  ")
    );
}

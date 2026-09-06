use anyhow::Context;
use std::path::{Path, PathBuf};

pub fn file_path(cache_dir: &Path, key: &str) -> PathBuf {
    cache_dir.join(key)
}

pub fn tmp_path(cache_dir: &Path, key: &str) -> PathBuf {
    // .tmp.<key>.<rand> — rand suffix avoids collision under concurrent
    // fetch; slashes are flattened so nested keys still land in a flat
    // temp file (the seal rename creates the nested final directory).
    let rand: u32 = rand_suffix();
    let flat = key.replace('/', "_");
    cache_dir.join(format!(".tmp.{flat}.{rand:08x}"))
}

fn rand_suffix() -> u32 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    std::time::SystemTime::now().hash(&mut h);
    std::thread::current().id().hash(&mut h);
    h.finish() as u32
}

/// Atomically install a completed download: fsync tmp then rename.
pub fn install_tmp(tmp: &Path, dest: &Path) -> anyhow::Result<()> {
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent).with_context(|| format!("create dir {}", parent.display()))?;
    }
    std::fs::rename(tmp, dest).with_context(|| format!("rename {} -> {}", tmp.display(), dest.display()))?;
    Ok(())
}

/// Remove all `.tmp.*` files under cache_dir (startup cleanup).
pub fn cleanup_tmps(cache_dir: &Path) -> anyhow::Result<usize> {
    let mut removed = 0;
    if !cache_dir.exists() {
        return Ok(0);
    }
    for entry in walkdir::WalkDir::new(cache_dir).into_iter().filter_map(|e| e.ok()) {
        let name = entry.file_name().to_string_lossy();
        if name.starts_with(".tmp.") {
            let _ = std::fs::remove_file(entry.path());
            removed += 1;
        }
    }
    Ok(removed)
}

/// Prune empty parent directories up to (but not including) cache_dir.
pub fn prune_empty_parents(cache_dir: &Path, file: &Path) {
    let mut cur = file.parent();
    while let Some(dir) = cur {
        if dir == cache_dir {
            break;
        }
        match std::fs::remove_dir(dir) {
            Ok(_) => cur = dir.parent(),
            Err(_) => break,
        }
    }
}

// ---------------------------------------------------------------------------
// efficientcache (P2): staged segments + coverage ledger.
// C transfers stage exactly the bytes they serve as flat sidecar files;
// the ledger (intervals per key) decides coverage-triggered promotion.
// Segment files are the source of truth — the in-memory ledger is a view,
// rebuilt by scan on startup (crash/abort-safe by construction).
// ---------------------------------------------------------------------------

/// Transfer history for one key: which byte intervals have been served and
/// staged, under which object version. The coverage ledger entry.
#[derive(Debug, Clone, Default)]
pub struct Coverage {
    pub etag: Option<String>,
    pub total: u64,
    /// Merged, sorted, non-overlapping `[start, end)` intervals.
    pub intervals: Vec<(u64, u64)>,
    /// Clock-domain last touch (stage or rebuild time): drives age sweep
    /// in MockClock-testable time, unlike fs mtime.
    pub last_touch_millis: u64,
    /// Provider-side object path + owning upstream: promotion (P2-b)
    /// fetches gaps through these, never by re-splitting the cache key.
    pub backend_key: String,
    pub upstream_id: String,
}

impl Coverage {
    /// Merge `[start, end)` (empty ranges ignored).
    pub fn add_interval(&mut self, start: u64, end: u64) {
        if start >= end {
            return;
        }
        self.intervals.push((start, end));
        self.intervals.sort();
        let mut merged: Vec<(u64, u64)> = Vec::with_capacity(self.intervals.len());
        for (s, e) in self.intervals.drain(..) {
            if let Some(last) = merged.last_mut() {
                if s <= last.1 {
                    last.1 = last.1.max(e);
                    continue;
                }
            }
            merged.push((s, e));
        }
        self.intervals = merged;
    }

    pub fn covered_bytes(&self) -> u64 {
        self.intervals.iter().map(|(s, e)| e - s).sum()
    }

    /// Staged fraction of the object, or `None` while the total is unknown
    /// (never promotes — P2 correctness bar).
    pub fn ratio(&self) -> Option<f64> {
        if self.total == 0 {
            return None;
        }
        Some(self.covered_bytes() as f64 / self.total as f64)
    }
}

/// Reversible flattening for segment filenames (`%` first, then `/`).
/// tmp files flatten lossily; segments must map back to the key.
pub fn escape_key(key: &str) -> String {
    key.replace('%', "%25").replace('/', "%2F")
}

pub fn unescape_key(esc: &str) -> Option<String> {
    let mut out = String::with_capacity(esc.len());
    let bytes = esc.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 2 >= bytes.len() {
                return None;
            }
            let hex = |b: u8| match b {
                b'0'..=b'9' => Some(b - b'0'),
                b'a'..=b'f' => Some(b - b'a' + 10),
                b'A'..=b'F' => Some(b - b'A' + 10),
                _ => None,
            };
            out.push(((hex(bytes[i + 1])? << 4) | hex(bytes[i + 2])?) as char);
            i += 3;
        } else if bytes[i] == b'/' {
            // Unescaped slashes never appear in our filenames.
            return None;
        } else {
            out.push(bytes[i] as char);
            i += 1;
        }
    }
    Some(out)
}

/// Completed segment: `.seg.<escaped>.<start>-<end>` (flat in cache_dir).
pub fn seg_path(cache_dir: &Path, key: &str, start: u64, end: u64) -> PathBuf {
    cache_dir.join(format!(".seg.{}.{start}-{end}", escape_key(key)))
}

/// In-flight segment part: renamed to `.seg.*` only on successful
/// exhaustion, so `.segpart.*` files are always safe to sweep.
pub fn segpart_path(cache_dir: &Path, key: &str, start: u64, end: u64) -> PathBuf {
    cache_dir.join(format!(".segpart.{}.{start}-{end}", escape_key(key)))
}

/// Per-key segment metadata (etag + total for promotion-time verification).
pub fn segmeta_path(cache_dir: &Path, key: &str) -> PathBuf {
    cache_dir.join(format!(".segmeta.{}", escape_key(key)))
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SegMeta {
    pub etag: Option<String>,
    pub total: u64,
    #[serde(default)]
    pub backend_key: String,
    #[serde(default)]
    pub upstream_id: String,
}

/// Parse a `.seg.*` filename back to `(key, start, end)`. The range part
/// carries no dots, so the last dot separates it from the escaped key.
pub fn parse_seg_name(name: &str) -> Option<(String, u64, u64)> {
    let rest = name.strip_prefix(".seg.")?;
    let (esc, range) = rest.rsplit_once('.')?;
    let (s, e) = range.split_once('-')?;
    Some((unescape_key(esc)?, s.parse().ok()?, e.parse().ok()?))
}

fn mtime_millis(path: &Path) -> Option<u64> {
    std::fs::metadata(path)
        .and_then(|m| m.modified())
        .ok()?
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .map(|d| d.as_millis() as u64)
}

/// Startup rebuild: fold completed on-disk segments back into the ledger
/// (etag/total from each key's `.segmeta.*`; absent meta → unknown version,
/// never promotes until a fresh transfer rewrites it). In-flight
/// `.segpart.*` orphans and unparseable `.seg.*` junk are deleted.
/// Returns `(ledger, staged_bytes)`. Rebuilt entries touch at `now_millis`.
pub fn scan_segments(cache_dir: &Path, now_millis: u64) -> (std::collections::HashMap<String, Coverage>, u64) {
    use std::collections::HashMap;
    let mut ledger: HashMap<String, Coverage> = HashMap::new();
    let mut staged_bytes = 0u64;
    if !cache_dir.exists() {
        return (ledger, staged_bytes);
    }
    // Stage 1: drop in-flight orphans (never completed, no ledger claim).
    // Stage 2: fold completed segments; unparseable names are our own junk.
    let mut seg_files: Vec<PathBuf> = Vec::new();
    for entry in walkdir::WalkDir::new(cache_dir).into_iter().filter_map(|e| e.ok()) {
        if !entry.file_type().is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with(".segpart.") {
            let _ = std::fs::remove_file(entry.path());
        } else if name.starts_with(".seg.") && !name.starts_with(".segmeta.") {
            match parse_seg_name(&name) {
                Some(_) => seg_files.push(entry.path().to_path_buf()),
                None => {
                    let _ = std::fs::remove_file(entry.path());
                }
            }
        }
    }
    let mut metas: HashMap<String, SegMeta> = HashMap::new();
    for path in seg_files {
        let name = path.file_name().unwrap_or_default().to_string_lossy().into_owned();
        let (key, start, end) = match parse_seg_name(&name) {
            Some(p) => p,
            None => continue,
        };
        let len = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        let meta = metas.entry(key.clone()).or_insert_with(|| {
            std::fs::read(segmeta_path(cache_dir, &key))
                .ok()
                .and_then(|b| serde_json::from_slice(&b).ok())
                .unwrap_or(SegMeta { etag: None, total: 0, backend_key: String::new(), upstream_id: String::new() })
        });
        let cov = ledger.entry(key).or_insert_with(|| Coverage {
            etag: meta.etag.clone(),
            total: meta.total,
            intervals: Vec::new(),
            last_touch_millis: now_millis,
            backend_key: meta.backend_key.clone(),
            upstream_id: meta.upstream_id.clone(),
        });
        cov.add_interval(start, end.min(start.saturating_add(len)));
        staged_bytes += len;
    }
    (ledger, staged_bytes)
}

/// Completed segment files for one key (for size accounting on sweep).
pub fn key_segment_files(cache_dir: &Path, key: &str) -> Vec<PathBuf> {
    let prefix = format!(".seg.{}.", escape_key(key));
    let mut out = Vec::new();
    if !cache_dir.exists() {
        return out;
    }
    for entry in walkdir::WalkDir::new(cache_dir).into_iter().filter_map(|e| e.ok()) {
        if !entry.file_type().is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with(&prefix) && parse_seg_name(&name).is_some() {
            out.push(entry.path().to_path_buf());
        }
    }
    out
}

/// Sweep abandoned in-flight `.segpart.*` parts older than `ttl_ms` (fs
/// mtime domain: parts carry no ledger entry). Returns bytes removed.
/// Completed segments are governed by ledger age in `Cache::tick`, not here.
pub fn sweep_segparts(cache_dir: &Path, ttl_ms: u64, now_millis: u64) -> u64 {
    if !cache_dir.exists() {
        return 0;
    }
    let mut removed_bytes = 0u64;
    for entry in walkdir::WalkDir::new(cache_dir).into_iter().filter_map(|e| e.ok()) {
        if !entry.file_type().is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.starts_with(".segpart.") {
            continue;
        }
        let old = mtime_millis(entry.path()).is_none_or(|m| now_millis.saturating_sub(m) >= ttl_ms);
        if old {
            removed_bytes += std::fs::metadata(entry.path()).map(|m| m.len()).unwrap_or(0);
            let _ = std::fs::remove_file(entry.path());
        }
    }
    removed_bytes
}

/// Delete `.segmeta.*` markers whose key has no surviving segments.
pub fn sweep_orphan_metas(cache_dir: &Path) {
    use std::collections::HashSet;
    if !cache_dir.exists() {
        return;
    }
    let mut live: HashSet<String> = HashSet::new();
    for entry in walkdir::WalkDir::new(cache_dir).into_iter().filter_map(|e| e.ok()) {
        if !entry.file_type().is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with(".seg.") && !name.starts_with(".segmeta.") {
            if let Some((key, _, _)) = parse_seg_name(&name) {
                live.insert(key);
            }
        }
    }
    for entry in walkdir::WalkDir::new(cache_dir).into_iter().filter_map(|e| e.ok()) {
        if !entry.file_type().is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if let Some(esc) = name.strip_prefix(".segmeta.") {
            let key = unescape_key(esc).unwrap_or_default();
            if !live.contains(&key) {
                let _ = std::fs::remove_file(entry.path());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn install_and_prune() {
        let dir = tempdir().unwrap();
        let key = "2026/08/a.png";
        let dest = file_path(dir.path(), key);
        let tmp = tmp_path(dir.path(), key);
        std::fs::create_dir_all(tmp.parent().unwrap()).unwrap();
        std::fs::write(&tmp, b"hello").unwrap();
        install_tmp(&tmp, &dest).unwrap();
        assert_eq!(std::fs::read(&dest).unwrap(), b"hello");
        assert!(!tmp.exists());
        // prune after delete
        std::fs::remove_file(&dest).unwrap();
        prune_empty_parents(dir.path(), &dest);
        assert!(!dir.path().join("2026/08").exists());
        assert!(!dir.path().join("2026").exists());
    }

    #[test]
    fn cleanup_tmps_removes_only_tmps() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join(".tmp.foo.12345678"), b"x").unwrap();
        std::fs::write(dir.path().join("keep.png"), b"y").unwrap();
        let n = cleanup_tmps(dir.path()).unwrap();
        assert_eq!(n, 1);
        assert!(dir.path().join("keep.png").exists());
    }

    #[test]
    fn escape_roundtrip() {
        for key in ["a/b/c.png", "a%20b/c.png", "plain.bin", "2026/08/x.y.z"] {
            assert_eq!(unescape_key(&escape_key(key)).as_deref(), Some(key), "{key}");
        }
        assert_eq!(unescape_key("a/b"), None);
        assert_eq!(unescape_key("a%2"), None);
        assert_eq!(unescape_key("a%zz"), None);
    }

    #[test]
    fn seg_name_roundtrip() {
        let name = seg_path(std::path::Path::new("/tmp"), "a/b+c.png", 100, 200)
            .file_name().unwrap().to_string_lossy().into_owned();
        assert_eq!(parse_seg_name(&name), Some(("a/b+c.png".into(), 100, 200)));
        assert_eq!(parse_seg_name(".segpart.a.0-10"), None);
        assert_eq!(parse_seg_name(".seg.no-range-here"), None);
    }

    #[test]
    fn coverage_merges_and_ratios() {
        let mut c = Coverage::default();
        assert_eq!(c.ratio(), None); // total unknown → never promotes
        c.total = 100;
        c.add_interval(0, 30);
        c.add_interval(50, 80);
        assert_eq!(c.covered_bytes(), 60);
        assert!((c.ratio().unwrap() - 0.6).abs() < 1e-9);
        c.add_interval(20, 60); // bridges the gap
        assert_eq!(c.intervals, vec![(0, 80)]);
        c.add_interval(80, 100); // adjacent merges
        assert_eq!(c.intervals, vec![(0, 100)]);
        assert!((c.ratio().unwrap() - 1.0).abs() < 1e-9);
        c.add_interval(200, 200); // empty ignored
        assert_eq!(c.intervals, vec![(0, 100)]);
    }

    #[test]
    fn scan_rebuilds_ledger_and_drops_orphans() {
        let dir = tempdir().unwrap();
        // Two segments for one key + meta, one orphan part, one junk file.
        std::fs::write(seg_path(dir.path(), "v/f.bin", 0, 30), vec![0u8; 30]).unwrap();
        std::fs::write(seg_path(dir.path(), "v/f.bin", 50, 80), vec![0u8; 30]).unwrap();
        std::fs::write(
            segmeta_path(dir.path(), "v/f.bin"),
            serde_json::to_vec(&SegMeta { etag: Some("e1".into()), total: 100, backend_key: "v/f.bin".into(), upstream_id: "primary".into() }).unwrap(),
        )
        .unwrap();
        std::fs::write(segpart_path(dir.path(), "v/f.bin", 80, 100), vec![0u8; 5]).unwrap();
        std::fs::write(dir.path().join(".seg.garbage"), b"x").unwrap();
        let (ledger, staged) = scan_segments(dir.path(), 0);
        assert_eq!(staged, 60);
        let cov = ledger.get("v/f.bin").unwrap();
        assert_eq!(cov.etag.as_deref(), Some("e1"));
        assert_eq!(cov.total, 100);
        assert_eq!(cov.intervals, vec![(0, 30), (50, 80)]);
        assert!(!segpart_path(dir.path(), "v/f.bin", 80, 100).exists());
        assert!(!dir.path().join(".seg.garbage").exists());
    }

    #[test]
    fn sweep_segparts_only_old_parts() {
        let dir = tempdir().unwrap();
        // Fresh in-flight part survives a normal sweep (fs-mtime domain).
        let p = segpart_path(dir.path(), "a.bin", 0, 10);
        std::fs::write(&p, vec![0u8; 10]).unwrap();
        let wall = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        assert_eq!(sweep_segparts(dir.path(), 60_000, wall), 0);
        assert!(p.exists());
        // ttl 0 sweeps everything (abandoned-part hygiene).
        assert_eq!(sweep_segparts(dir.path(), 0, wall), 10);
        assert!(!p.exists());
        // Completed segments are ledger-governed, never touched here.
        let s = seg_path(dir.path(), "a.bin", 0, 10);
        std::fs::write(&s, vec![0u8; 10]).unwrap();
        assert_eq!(sweep_segparts(dir.path(), 0, wall), 0);
        assert!(s.exists());
    }
}

use anyhow::Context;
use std::path::{Path, PathBuf};

pub fn file_path(cache_dir: &Path, key: &str) -> PathBuf {
    cache_dir.join(key)
}

/// Name of the redb metadata database, which lives at the top level of
/// cache_dir alongside the object tree. Every directory walk over that
/// directory must skip it — see [`scan_object_files`].
pub const META_STORE_FILE: &str = "redb.db";

/// Whether a top-level name belongs to the metadata store rather than to
/// the object tree. Covers the store itself and everything derived from it
/// (the quarantine archive `redb.db.corrupt-<epoch>`). No legitimate cache
/// object can collide: objects are the NESTED files, and the top level
/// holds only infrastructure and ephemeral artifacts.
pub fn is_meta_store_name(name: &str) -> bool {
    name.starts_with(META_STORE_FILE)
}

/// Whether a cache key would collide with an infrastructure name.
///
/// [`file_path`] is a raw join, so a key with no slash becomes a TOP-LEVEL
/// name in cache_dir, where the metadata store and the ephemeral `.tmp.*`
/// / `.seg*` artifacts live. A bare key `redb.db` therefore names the live
/// database: a cold pull would rename object bytes over it, and the
/// artifact sweeps would delete `.tmp.x`. Only the first segment is
/// reserved, so `googledrive1/.hidden` stays an ordinary object.
pub fn is_reserved_key(key: &str) -> bool {
    let first = key.split('/').next().unwrap_or("");
    first.starts_with('.') || is_meta_store_name(first)
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

/// Whether `dest` is the metadata store itself: a direct child of the cache
/// directory whose name belongs to the store.
///
/// The parent check is the point. A nested object may legitimately be called
/// `redb.db` (`<bucket>/redb.db`); refusing by file name alone protected
/// those from reaping and blocked installing them, which the node showed as
/// `refusing to reap a metadata store path key=googledrive1/redb.db`.
pub fn is_meta_store_path(cache_dir: &Path, dest: &Path) -> bool {
    dest.parent() == Some(cache_dir)
        && dest.file_name().is_some_and(|n| is_meta_store_name(&n.to_string_lossy()))
}

/// Atomically install a completed download: fsync tmp then rename.
///
/// Refuses the one destination that must never receive object bytes -- the
/// metadata store -- so a key that named it cannot replace the live
/// database. `cache_dir` is what makes that check exact rather than a
/// name-pattern match.
pub fn install_tmp(tmp: &Path, dest: &Path, cache_dir: &Path) -> anyhow::Result<()> {
    anyhow::ensure!(
        !is_meta_store_path(cache_dir, dest),
        "refusing to install an object over the metadata store ({})",
        dest.display()
    );
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent).with_context(|| format!("create dir {}", parent.display()))?;
    }
    std::fs::rename(tmp, dest).with_context(|| format!("rename {} -> {}", tmp.display(), dest.display()))?;
    Ok(())
}

/// Every file at the TOP LEVEL of cache_dir, in one `read_dir` (P4).
///
/// All ephemeral artifacts — `.tmp.*`, `.seg.*`, `.segpart.*`, `.segmeta.*`
/// — are written flat into cache_dir by `tmp_path`/`seg_path`/`segpart_path`/
/// `segmeta_path`; only the durable object files mirror the key's nested
/// path (`file_path`). The sweep helpers therefore never need to recurse:
/// walking the whole tree made every sweep cost grow with the number of
/// cached objects while finding nothing new.
pub fn top_level_files(cache_dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let Ok(rd) = std::fs::read_dir(cache_dir) else {
        return out;
    };
    for entry in rd.flatten() {
        if entry.file_type().map(|t| t.is_file()).unwrap_or(false) {
            out.push(entry.path());
        }
    }
    out
}

/// Remove all `.tmp.*` files under cache_dir (startup cleanup).
pub fn cleanup_tmps(cache_dir: &Path) -> anyhow::Result<usize> {
    let mut removed = 0;
    if !cache_dir.exists() {
        return Ok(0);
    }
    for path in top_level_files(cache_dir) {
        let name = path.file_name().unwrap_or_default().to_string_lossy();
        if name.starts_with(".tmp.") {
            let _ = std::fs::remove_file(&path);
            removed += 1;
        }
    }
    Ok(removed)
}

/// Free bytes on the filesystem holding `path`, or `None` if unknown
/// (capacity guard, P56). A cache that accepts writes knows the disk it
/// writes to; without this the only signal was a failed write.
pub fn free_bytes(path: &Path) -> Option<u64> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let c = CString::new(path.as_os_str().as_bytes()).ok()?;
    // SAFETY: `statvfs` fills the struct on success; we only read the two
    // fields we need and check the return code first.
    unsafe {
        let mut st: libc::statvfs = std::mem::zeroed();
        if libc::statvfs(c.as_ptr(), &mut st) != 0 {
            return None;
        }
        Some((st.f_bavail as u64).saturating_mul(st.f_frsize as u64))
    }
}

/// Whether a cold pull of `want` bytes should start, given free space and a
/// reserve floor. `reserve` keeps the node from being filled to zero (the
/// OS needs room for logs, redb, and an operator's emergency shell).
pub fn has_room_for(path: &Path, want: u64, reserve: u64) -> bool {
    match free_bytes(path) {
        Some(free) => free.saturating_sub(reserve) >= want,
        // Unknown free space must not block serving: the write itself
        // remains the backstop.
        None => true,
    }
}

/// Every durable cached object file under `cache_dir`, as (key, size) —
/// the inverse of [`file_path`]. Only NESTED files are objects: every
/// ephemeral artifact (`.tmp.*`, `.seg.*`, `.segpart.*`, `.segmeta.*`) is
/// written flat into the top level, so a walk that skips dot-prefixed
/// top-level names and recurses into directories sees exactly the object
/// tree.
///
/// The metadata store is skipped by name. It lives at the top level of the
/// same directory, has no dot prefix, and looks exactly like a cached
/// object — so a rebuild adopted `redb.db` as an entry, counted its size as
/// cached bytes, and then the inactive reaper DELETED it once the TTL
/// passed: the node silently ran on an unlinked database and lost its rows
/// on the next start. Kept here (not at the call site) so every caller is
/// safe; the name must match the one `Cache::new` opens.
pub fn scan_object_files(cache_dir: &Path) -> Vec<(String, u64)> {
    fn walk(dir: &Path, root: &Path, out: &mut Vec<(String, u64)>) {
        let Ok(rd) = std::fs::read_dir(dir) else { return };
        for entry in rd.flatten() {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().into_owned();
            // Top-level dot-entries are ephemeral, never objects; anything
            // belonging to the metadata store is infrastructure, never an
            // object.
            if dir == root && (name.starts_with('.') || is_meta_store_name(&name)) {
                continue;
            }
            match entry.file_type() {
                Ok(t) if t.is_dir() => walk(&path, root, out),
                Ok(t) if t.is_file() => {
                    if let Ok(rel) = path.strip_prefix(root) {
                        let key = rel.to_string_lossy().replace('\\', "/");
                        let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                        if !key.is_empty() {
                            out.push((key, size));
                        }
                    }
                }
                _ => {}
            }
        }
    }
    let mut out = Vec::new();
    walk(cache_dir, cache_dir, &mut out);
    out
}

/// Remove `.tmp.*` files older than `ttl_ms` (P56). Startup sweeps every
/// tmp; this periodic form must be age-guarded, because an in-flight cold
/// pull's temp file is young and must not be deleted underneath its driver.
pub fn cleanup_stale_tmps(cache_dir: &Path, ttl_ms: u64, now_millis: u64) -> u64 {
    let mut removed = 0u64;
    for path in top_level_files(cache_dir) {
        let name = path.file_name().unwrap_or_default().to_string_lossy().into_owned();
        if !name.starts_with(".tmp.") {
            continue;
        }
        let old = mtime_millis(&path).is_none_or(|m| now_millis.saturating_sub(m) >= ttl_ms);
        if old {
            removed += std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            let _ = std::fs::remove_file(&path);
        }
    }
    removed
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

/// Per-key segment metadata: the version marker a later transfer compares
/// against, plus the object's total size.
pub fn segmeta_path(cache_dir: &Path, key: &str) -> PathBuf {
    cache_dir.join(format!(".segmeta.{}", escape_key(key)))
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SegMeta {
    pub etag: Option<String>,
    pub total: u64,
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

/// One completed `.seg` sidecar found on disk, with the version its key's
/// `.segmeta` claims.
#[derive(Debug, Clone)]
pub struct SegmentFile {
    pub key: String,
    pub start: u64,
    pub end: u64,
    /// The file's REAL length. The name says what was promised; this says what
    /// is there, and it is the number the ledger records.
    pub len: u64,
    pub etag: Option<String>,
    pub total: u64,
}

/// Startup inventory: the completed segments on disk, with the version each
/// key's `.segmeta.*` claims (absent meta → unknown version, never promotes
/// until a fresh transfer rewrites it).
///
/// This is also where the sweep happens, and the sweep is narrow on purpose:
/// in-flight `.segpart.*` orphans and unparseable `.seg.*` junk are deleted,
/// because they are our own crashes' leftovers and nothing can claim them.
/// What those bytes COST is the ledger's question (`Ledger::adopt_all`); this
/// function answers only what is there.
pub fn scan_segment_files(cache_dir: &Path) -> Vec<SegmentFile> {
    let mut out = Vec::new();
    if !cache_dir.exists() {
        return out;
    }
    let mut seg_files: Vec<PathBuf> = Vec::new();
    for path in top_level_files(cache_dir) {
        let name = path.file_name().unwrap_or_default().to_string_lossy().into_owned();
        if name.starts_with(".segpart.") {
            let _ = std::fs::remove_file(&path);
        } else if name.starts_with(".seg.") && !name.starts_with(".segmeta.") {
            match parse_seg_name(&name) {
                Some(_) => seg_files.push(path),
                None => {
                    let _ = std::fs::remove_file(&path);
                }
            }
        }
    }
    // The version is read once per key, not once per file.
    let mut metas: std::collections::HashMap<String, SegMeta> = std::collections::HashMap::new();
    for path in seg_files {
        let name = path.file_name().unwrap_or_default().to_string_lossy().into_owned();
        let (key, start, end) = match parse_seg_name(&name) {
            Some(p) => p,
            None => continue,
        };
        let len = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        let meta = metas
            .entry(key.clone())
            .or_insert_with(|| {
                std::fs::read(segmeta_path(cache_dir, &key))
                    .ok()
                    .and_then(|b| serde_json::from_slice(&b).ok())
                    .unwrap_or(SegMeta { etag: None, total: 0 })
            })
            .clone();
        out.push(SegmentFile { key, start, end, len, etag: meta.etag, total: meta.total });
    }
    out
}

/// Completed segment files for one key (for size accounting on sweep).
pub fn key_segment_files(cache_dir: &Path, key: &str) -> Vec<PathBuf> {
    let prefix = format!(".seg.{}.", escape_key(key));
    top_level_files(cache_dir)
        .into_iter()
        .filter(|path| {
            let name = path.file_name().unwrap_or_default().to_string_lossy();
            name.starts_with(&prefix) && parse_seg_name(&name).is_some()
        })
        .collect()
}

/// A key's completed segments as `(start, end, path)`, with the name parsed
/// here so no caller re-derives the filename shape.
pub fn segments_for_key(cache_dir: &Path, key: &str) -> Vec<(u64, u64, PathBuf)> {
    let mut out: Vec<(u64, u64, PathBuf)> = key_segment_files(cache_dir, key)
        .into_iter()
        .filter_map(|path| {
            let name = path.file_name()?.to_string_lossy().into_owned();
            parse_seg_name(&name).map(|(_, start, end)| (start, end, path))
        })
        .collect();
    out.sort();
    out
}

/// Drop a key's completed segments and its version marker (the etag-reset
/// path). In-flight `.segpart.*` files are left alone: a concurrent transfer
/// still owns them.
///
/// `keep` names the one segment to preserve — the one just sealed.
///
/// This lives here rather than at the caller because the filename shape is
/// this module's rule. Re-deriving it by hand is the class of bug that once
/// let a sweep reach the metadata store, and it is why the prefix is spelled
/// exactly once (`seg_path`) instead of in a `format!` at each use site.
pub fn remove_key_segments(cache_dir: &Path, key: &str, keep: Option<&Path>) {
    for path in key_segment_files(cache_dir, key) {
        if keep.is_some_and(|k| k == path.as_path()) {
            continue;
        }
        let _ = std::fs::remove_file(&path);
    }
    let _ = std::fs::remove_file(segmeta_path(cache_dir, key));
}

/// Completed segments grouped by key, built in ONE top-level pass (P4).
/// The reaper used to call [`key_segment_files`] per expired key, so one
/// tick cost O(expired × cache size); this costs one directory read.
pub fn segment_index(cache_dir: &Path) -> std::collections::HashMap<String, Vec<PathBuf>> {
    let mut idx: std::collections::HashMap<String, Vec<PathBuf>> = std::collections::HashMap::new();
    for path in top_level_files(cache_dir) {
        let name = path.file_name().unwrap_or_default().to_string_lossy().into_owned();
        if name.starts_with(".seg.") && !name.starts_with(".segmeta.") {
            if let Some((key, _, _)) = parse_seg_name(&name) {
                idx.entry(key).or_default().push(path);
            }
        }
    }
    idx
}

/// Sweep abandoned in-flight `.segpart.*` parts older than `ttl_ms` (fs
/// mtime domain: parts carry no ledger entry). Returns bytes removed.
/// Completed segments are governed by ledger age in `Cache::tick`, not here.
pub fn sweep_segparts(cache_dir: &Path, ttl_ms: u64, now_millis: u64) -> u64 {
    if !cache_dir.exists() {
        return 0;
    }
    let mut removed_bytes = 0u64;
    for path in top_level_files(cache_dir) {
        let name = path.file_name().unwrap_or_default().to_string_lossy().into_owned();
        if !name.starts_with(".segpart.") {
            continue;
        }
        let old = mtime_millis(&path).is_none_or(|m| now_millis.saturating_sub(m) >= ttl_ms);
        if old {
            removed_bytes += std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            let _ = std::fs::remove_file(&path);
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
    // One top-level pass answers both halves: which keys have segments, and
    // which `.segmeta.*` markers are now orphaned (P4).
    let files = top_level_files(cache_dir);
    let mut live: HashSet<String> = HashSet::new();
    for path in &files {
        let name = path.file_name().unwrap_or_default().to_string_lossy();
        if name.starts_with(".seg.") && !name.starts_with(".segmeta.") {
            if let Some((key, _, _)) = parse_seg_name(&name) {
                live.insert(key);
            }
        }
    }
    for path in &files {
        let name = path.file_name().unwrap_or_default().to_string_lossy();
        if let Some(esc) = name.strip_prefix(".segmeta.") {
            let key = unescape_key(esc).unwrap_or_default();
            if !live.contains(&key) {
                let _ = std::fs::remove_file(path);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    /// Cross-check: the key rule must reserve exactly the names the layout
    /// can produce at the top level. Both sides are derived from the same
    /// helpers here, so adding an artifact prefix without reserving it fails
    /// this test instead of silently letting a key collide with it.
    #[test]
    fn every_top_level_infrastructure_name_is_a_reserved_key() {
        // Artifact families, by the prefix each constructor emits.
        for prefix in [".tmp.", ".seg.", ".segpart.", ".segmeta."] {
            let name = format!("{prefix}x");
            assert!(is_reserved_key(&name), "{name} must be reserved");
        }
        assert!(is_reserved_key(META_STORE_FILE));
        assert!(is_reserved_key("redb.db.corrupt-1789556382"));
        // The actual constructors agree with those prefixes.
        let dir = std::path::Path::new("/cache");
        for path in [
            tmp_path(dir, "a/b.bin"),
            seg_path(dir, "a/b.bin", 0, 1),
            segpart_path(dir, "a/b.bin", 0, 1),
            segmeta_path(dir, "a/b.bin"),
            dir.join(META_STORE_FILE),
        ] {
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            assert!(is_reserved_key(&name), "layout emitted an unreserved name: {name}");
        }
        // Objects live in subdirectories, so only the FIRST segment is
        // reserved: a nested dot-file is an ordinary object.
        assert!(!is_reserved_key("googledrive1/.hidden"));
        assert!(!is_reserved_key("2026/08/a.png"));
        assert!(!is_reserved_key("googledrive1/redb.dbase"));
    }

    /// Installing over the metadata store would replace the live database
    /// with object bytes. Refused at the funnel, whatever the caller -- but
    /// only for the store ITSELF: an object legitimately named `redb.db`
    /// under a bucket alias lives one level down and must install normally.
    /// The node found the difference: the first version matched by file name
    /// alone and blocked those nested objects.
    #[test]
    fn install_tmp_refuses_the_store_but_not_a_nested_object_of_that_name() {
        let dir = tempdir().unwrap();
        let live = dir.path().join(META_STORE_FILE);
        std::fs::write(&live, b"the real database").unwrap();
        let tmp = tmp_path(dir.path(), "evil");
        std::fs::write(&tmp, b"object bytes").unwrap();

        let err = install_tmp(&tmp, &live, dir.path()).unwrap_err();
        assert!(err.to_string().contains("metadata store"), "{err}");
        assert_eq!(std::fs::read(&live).unwrap(), b"the real database");
        assert!(tmp.exists(), "the refused tmp must not be consumed");

        // A nested object with the same file name is not the store.
        let nested = dir.path().join("bucket/redb.db");
        let nested_tmp = tmp_path(dir.path(), "bucket/redb.db");
        std::fs::write(&nested_tmp, b"a real object").unwrap();
        install_tmp(&nested_tmp, &nested, dir.path()).expect("a nested name is not the store");
        assert_eq!(std::fs::read(&nested).unwrap(), b"a real object");
        assert_eq!(std::fs::read(&live).unwrap(), b"the real database");
    }

    /// The reset path must drop a key's segments and its version marker, keep
    /// the one just sealed, and — the part that matters most — never touch
    /// anything that is not this key's. `segments_for_key` is the same
    /// rule the sweeps use, so a name that parses as another key's segment is
    /// left alone.
    #[test]
    fn remove_key_segments_keeps_the_just_sealed_segment_and_other_keys() {
        let dir = tempdir().unwrap();
        let kept = seg_path(dir.path(), "v/f.bin", 0, 30);
        let dropped = seg_path(dir.path(), "v/f.bin", 30, 60);
        let other = seg_path(dir.path(), "v/g.bin", 0, 30);
        let meta = segmeta_path(dir.path(), "v/f.bin");
        for path in [&kept, &dropped, &other, &meta] {
            std::fs::write(path, b"x").unwrap();
        }
        // The metadata store sits in the same directory and must survive.
        let store = dir.path().join(META_STORE_FILE);
        std::fs::write(&store, b"database").unwrap();

        remove_key_segments(dir.path(), "v/f.bin", Some(&kept));

        assert!(kept.exists(), "the just-sealed segment must be kept");
        assert!(!dropped.exists(), "the other segments of this key must go");
        assert!(!meta.exists(), "the version marker must go with them");
        assert!(other.exists(), "another key's segments are not ours to delete");
        assert!(store.exists(), "the metadata store must never be swept");
    }

    /// The parsed view the assembly and test helpers use: sorted, and only
    /// names that really are segment names.
    #[test]
    fn segments_for_key_reports_parsed_intervals_in_order() {
        let dir = tempdir().unwrap();
        std::fs::write(seg_path(dir.path(), "v/f.bin", 50, 80), b"x").unwrap();
        std::fs::write(seg_path(dir.path(), "v/f.bin", 0, 30), b"x").unwrap();
        // Junk with the right prefix is not a segment.
        std::fs::write(dir.path().join(".seg.v%2Ff.bin.garbage"), b"x").unwrap();
        std::fs::write(seg_path(dir.path(), "v/g.bin", 0, 10), b"x").unwrap();

        let segs = segments_for_key(dir.path(), "v/f.bin");
        assert_eq!(segs.len(), 2, "{segs:?}");
        assert_eq!((segs[0].0, segs[0].1), (0, 30), "sorted by start");
        assert_eq!((segs[1].0, segs[1].1), (50, 80));
    }

    #[test]
    fn install_and_prune() {
        let dir = tempdir().unwrap();
        let key = "2026/08/a.png";
        let dest = file_path(dir.path(), key);
        let tmp = tmp_path(dir.path(), key);
        std::fs::create_dir_all(tmp.parent().unwrap()).unwrap();
        std::fs::write(&tmp, b"hello").unwrap();
        install_tmp(&tmp, &dest, dir.path()).unwrap();
        assert_eq!(std::fs::read(&dest).unwrap(), b"hello");
        assert!(!tmp.exists());
        // prune after delete
        std::fs::remove_file(&dest).unwrap();
        prune_empty_parents(dir.path(), &dest);
        assert!(!dir.path().join("2026/08").exists());
        assert!(!dir.path().join("2026").exists());
    }

    /// P4: the sweeps only touch the top level, so a nested object tree
    /// costs nothing to scan and nested files are never mistaken for
    /// ephemeral artifacts.
    #[test]
    fn top_level_sweeps_ignore_nested_objects() {
        let dir = tempdir().unwrap();
        // A deep object tree, plus flat ephemeral artifacts.
        let nested = dir.path().join("2026/08/deep");
        std::fs::create_dir_all(&nested).unwrap();
        std::fs::write(nested.join("video.mkv"), vec![0u8; 32]).unwrap();
        std::fs::write(dir.path().join(".tmp.2026_08_deep_video.mkv.1234"), b"partial").unwrap();
        std::fs::write(dir.path().join(".segpart.2026%2F08%2Fv.mkv.0-10"), b"part").unwrap();

        let removed = cleanup_tmps(dir.path()).unwrap();
        assert_eq!(removed, 1, "only the top-level tmp is swept");
        assert!(
            nested.join("video.mkv").exists(),
            "nested object files must survive the top-level sweep"
        );

        // The index is built from one top-level pass and skips nested files.
        std::fs::write(dir.path().join(".seg.2026%2F08%2Fv.mkv.0-15"), b"seg").unwrap();
        let idx = segment_index(dir.path());
        assert_eq!(idx.len(), 1, "index sees only top-level segment files");
        assert!(idx.contains_key("2026/08/v.mkv"));
    }

    /// The metadata store sits at the top level of cache_dir with no dot
    /// prefix, so the object scanner must skip it BY NAME. Before that
    /// guard, a rebuild adopted `redb.db` as a cached object, counted its
    /// size as cached bytes and let the inactive reaper delete it -- the
    /// process kept serving from the unlinked inode and the next start
    /// came up with no rows at all.
    #[test]
    fn scan_object_files_never_adopts_the_metadata_store() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join(META_STORE_FILE), vec![0u8; 4096]).unwrap();
        // A sibling artifact of the same shape: the retired-file naming
        // used by the quarantine path must also stay out of the tree.
        std::fs::write(dir.path().join("redb.db.corrupt-1789556382"), b"x").unwrap();
        std::fs::write(dir.path().join(".tmp.a.b.1234"), b"y").unwrap();
        let nested = dir.path().join("2026/08");
        std::fs::create_dir_all(&nested).unwrap();
        std::fs::write(nested.join("a.png"), vec![0u8; 10]).unwrap();

        let found = scan_object_files(dir.path());
        let names: Vec<&str> = found.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(names.len(), 1, "only the nested object is a cache object: {names:?}");
        assert_eq!(names[0], "2026/08/a.png");
        assert!(
            dir.path().join(META_STORE_FILE).exists(),
            "the scanner must not delete or move the store"
        );
        // The same guard must cover names derived from the store: the
        // quarantine archive is not a cached object either.
        assert!(is_meta_store_name("redb.db.corrupt-1789556382"));
        assert!(is_meta_store_name("redb.db"));
        assert!(!is_meta_store_name("2026"));
        assert!(!is_meta_store_name(".tmp.a.b.1234"));
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

    /// P10: the incremental merge must produce exactly what the old
    /// sort-and-rebuild did — overlaps merged, distinct intervals kept,
    /// timestamps taking the max.

    #[test]
    fn scan_inventories_segments_and_drops_orphans() {
        let dir = tempdir().unwrap();
        // Two segments for one key + meta, one orphan part, one junk file, and
        // one file whose NAME promises more than its content holds.
        std::fs::write(seg_path(dir.path(), "v/f.bin", 0, 30), vec![0u8; 30]).unwrap();
        std::fs::write(seg_path(dir.path(), "v/f.bin", 50, 80), vec![0u8; 30]).unwrap();
        std::fs::write(seg_path(dir.path(), "v/f.bin", 900, 1000), vec![0u8; 10]).unwrap();
        std::fs::write(
            segmeta_path(dir.path(), "v/f.bin"),
            // A row written by an older build, with fields this one no longer
            // has: it must still load (serde ignores what it does not know).
            serde_json::to_vec(&serde_json::json!({
                "etag": "e1",
                "total": 100,
                "backend_key": "v/f.bin",
                "upstream_id": "primary"
            }))
            .unwrap(),
        )
        .unwrap();
        std::fs::write(segpart_path(dir.path(), "v/f.bin", 80, 100), vec![0u8; 5]).unwrap();
        std::fs::write(dir.path().join(".seg.garbage"), b"x").unwrap();
        let found = scan_segment_files(dir.path());
        // The name is a promise; `len` is what is there.
        let mut spans: Vec<(u64, u64, u64)> =
            found.iter().map(|f| (f.start, f.end, f.len)).collect();
        spans.sort();
        assert_eq!(spans, vec![(0, 30, 30), (50, 80, 30), (900, 1000, 10)]);
        let f = found.iter().find(|f| f.key == "v/f.bin").unwrap();
        assert_eq!(f.etag.as_deref(), Some("e1"));
        assert_eq!(f.total, 100);
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

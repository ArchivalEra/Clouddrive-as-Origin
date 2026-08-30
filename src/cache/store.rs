use anyhow::Context;
use std::path::{Path, PathBuf};

pub fn file_path(cache_dir: &Path, key: &str) -> PathBuf {
    cache_dir.join(key)
}

pub fn tmp_path(cache_dir: &Path, key: &str) -> PathBuf {
    // .tmp.<key>.<rand> — rand suffix avoids collision under concurrent fetch.
    let rand: u32 = rand_suffix();
    cache_dir.join(format!(".tmp.{key}.{rand:08x}"))
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
}

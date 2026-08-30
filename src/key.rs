use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum KeyError {
    #[error("empty key")]
    Empty,
    #[error("absolute path not allowed")]
    Absolute,
    #[error("path traversal")]
    Traversal,
    #[error("contains NUL byte")]
    Nul,
    #[error("contains backslash")]
    Backslash,
    #[error("invalid percent encoding")]
    BadPercent,
}

/// Validate a cache key per spec §2 / ADR 0001.
/// - Non-empty, not absolute, no `..` segments, no backslash, no NUL.
/// - Percent-encoded traversal (`%2e%2e`, `%2F`, `%5C`, etc.) is rejected.
pub fn validate_key(raw: &str) -> Result<String, KeyError> {
    if raw.is_empty() {
        return Err(KeyError::Empty);
    }
    if raw.contains('\0') {
        return Err(KeyError::Nul);
    }
    if raw.contains('\\') {
        return Err(KeyError::Backslash);
    }
    if raw.starts_with('/') {
        return Err(KeyError::Absolute);
    }

    // Decode percent-encoded bytes and check the decoded form for traversal.
    let decoded = percent_decode(raw)?;
    if decoded.contains('\0') {
        return Err(KeyError::Nul);
    }
    if decoded.contains('\\') {
        return Err(KeyError::Backslash);
    }
    // Reject if decoded form is absolute.
    if decoded.starts_with('/') {
        return Err(KeyError::Absolute);
    }
    for seg in decoded.split('/') {
        if seg == ".." {
            return Err(KeyError::Traversal);
        }
    }
    // Also reject raw `..` segments (defense in depth for encoded slashes).
    for seg in raw.split('/') {
        if seg == ".." || seg == "%2e%2e" || seg == "%2E%2E" {
            return Err(KeyError::Traversal);
        }
    }
    Ok(raw.to_string())
}

fn percent_decode(s: &str) -> Result<String, KeyError> {
    let mut out = Vec::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 2 >= bytes.len() {
                return Err(KeyError::BadPercent);
            }
            let hi = hex_val(bytes[i + 1]).ok_or(KeyError::BadPercent)?;
            let lo = hex_val(bytes[i + 2]).ok_or(KeyError::BadPercent)?;
            out.push((hi << 4) | lo);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).map_err(|_| KeyError::BadPercent)
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_keys() {
        assert!(validate_key("2026/08/image.png").is_ok());
        assert!(validate_key("a/b/c").is_ok());
        assert!(validate_key("file.png").is_ok());
        assert!(validate_key("a%20b/c.png").is_ok());
    }

    #[test]
    fn rejects_traversal() {
        assert_eq!(validate_key("../etc/passwd"), Err(KeyError::Traversal));
        assert_eq!(validate_key("a/../b"), Err(KeyError::Traversal));
        assert_eq!(validate_key("a/.."), Err(KeyError::Traversal));
        assert_eq!(validate_key("%2e%2e%2fetc/passwd"), Err(KeyError::Traversal));
        assert_eq!(validate_key("a/%2E%2E/b"), Err(KeyError::Traversal));
        assert_eq!(validate_key("a%2f..%2fb"), Err(KeyError::Traversal));
    }

    #[test]
    fn rejects_absolute_and_backslash() {
        assert_eq!(validate_key("/etc/passwd"), Err(KeyError::Absolute));
        assert_eq!(validate_key("%2Fetc/passwd"), Err(KeyError::Absolute));
        assert_eq!(validate_key("a\\b"), Err(KeyError::Backslash));
        assert_eq!(validate_key("a%5Cb"), Err(KeyError::Backslash));
    }

    #[test]
    fn rejects_empty_and_nul() {
        assert_eq!(validate_key(""), Err(KeyError::Empty));
        assert_eq!(validate_key("a\0b"), Err(KeyError::Nul));
        assert_eq!(validate_key("a%00b"), Err(KeyError::Nul));
    }
}

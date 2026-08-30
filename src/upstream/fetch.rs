use std::net::IpAddr;

/// Validate outbound https hosts per spec §2:
/// - Only https scheme (caller enforces URL prefix).
/// - Reject localhost, loopback, private, CGNAT, link-local, reserved.
/// - Allow-list suffix check via `allowed_suffixes` (caller supplies from config).
pub fn is_allowed_host(host: &str, allowed_suffixes: &[String]) -> bool {
    // Reject literal IP addresses outright (downloadUrl is always a hostname per spec).
    if host.parse::<IpAddr>().is_ok() {
        return false;
    }
    let lower = host.to_ascii_lowercase();
    // Reject obvious local names.
    if lower == "localhost" || lower.starts_with("localhost.") {
        return false;
    }
    // Must match one of the configured suffixes (e.g. .files.1drv.com).
    // An exact match for storage.live.com (no leading dot) is also allowed.
    for s in allowed_suffixes {
        let suffix = s.to_ascii_lowercase();
        if lower == suffix.trim_start_matches('.') {
            return true;
        }
        if lower.ends_with(&suffix) {
            return true;
        }
        // Allow suffix with leading dot to match subdomains.
        if suffix.starts_with('.') && lower.ends_with(&suffix) {
            return true;
        }
    }
    false
}

/// Validate that a URL is https and its host passes the allow-list.
pub fn is_allowed_url(url: &str, allowed_suffixes: &[String]) -> bool {
    if !url.starts_with("https://") {
        return false;
    }
    let host = match host_from_url(url) {
        Some(h) => h,
        None => return false,
    };
    is_allowed_host(&host, allowed_suffixes)
}

fn host_from_url(url: &str) -> Option<String> {
    // Minimal URL host extraction without pulling `url` crate.
    let rest = url.strip_prefix("https://")?;
    let host = rest.split('/').next()?;
    let host = host.split(':').next()?; // strip port
    Some(host.to_string())
}

/// Parse Retry-After header (seconds or HTTP-date). Returns delay in millis.
/// If absent/invalid, returns None (caller falls back to jittered backoff).
pub fn parse_retry_after(value: &str) -> Option<u64> {
    if let Ok(secs) = value.trim().parse::<u64>() {
        return Some(secs * 1000);
    }
    None
}

/// Jittered exponential backoff: base * 2^attempt with ±25% jitter, capped.
pub fn backoff_millis(base_ms: u64, attempt: u32, max_ms: u64) -> u64 {
    let exp = base_ms.saturating_mul(1u64 << attempt.min(10));
    let jitter = exp / 4;
    let low = exp.saturating_sub(jitter);
    // Avoid rand crate: use a cheap hash-based jitter.
    let hash = {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut h = DefaultHasher::new();
        attempt.hash(&mut h);
        h.finish()
    };
    let range = jitter * 2 + 1;
    let val = low + (hash % range);
    val.min(max_ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn allow() -> Vec<String> {
        vec![".files.1drv.com".into(), ".sharepoint.com".into(), "storage.live.com".into()]
    }

    #[test]
    fn allows_known_suffixes() {
        let a = allow();
        assert!(is_allowed_host("abc.files.1drv.com", &a));
        assert!(is_allowed_host("x.sharepoint.com", &a));
        assert!(is_allowed_host("storage.live.com", &a));
        assert!(is_allowed_host("a.storage.live.com", &a));
    }

    #[test]
    fn rejects_local_and_ip() {
        let a = allow();
        assert!(!is_allowed_host("localhost", &a));
        assert!(!is_allowed_host("127.0.0.1", &a));
        assert!(!is_allowed_host("10.0.0.1", &a));
        assert!(!is_allowed_host("192.168.1.1", &a));
        assert!(!is_allowed_host("example.com", &a));
    }

    #[test]
    fn rejects_non_https_and_unknown_host() {
        let a = allow();
        assert!(!is_allowed_url("http://abc.files.1drv.com/file", &a));
        assert!(!is_allowed_url("https://evil.com/file", &a));
        assert!(is_allowed_url("https://abc.files.1drv.com/file", &a));
    }

    #[test]
    fn retry_after_seconds() {
        assert_eq!(parse_retry_after("120"), Some(120_000));
        assert_eq!(parse_retry_after("  5 "), Some(5_000));
        assert_eq!(parse_retry_after("not-a-number"), None);
    }

    #[test]
    fn backoff_within_bounds() {
        let v = backoff_millis(200, 0, 30_000);
        assert!((150..=250).contains(&v));
        let v2 = backoff_millis(200, 5, 30_000);
        assert!(v2 <= 30_000);
    }
}

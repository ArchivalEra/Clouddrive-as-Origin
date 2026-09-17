//! Host classification shared by the two places that decide whether plain
//! http is acceptable: the upstream `base_url` policy (config.rs) and the
//! redirect-target policy (backend/mod.rs). Both used to carry their own
//! copy of the check, so a fix in one would silently miss the other — and
//! both used `starts_with("127.")` / `starts_with("localhost.")`, which
//! treats `127.evil.com` and `localhost.evil.com` as loopback.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::str::FromStr;

/// Whether `host` (a bare host, no port, no userinfo) is a loopback name.
///
/// Only two things count: a literal loopback IP, or the `localhost` name
/// (plus RFC 6761's `*.localhost`). A non-literal name is never resolved —
/// looking it up would let an attacker point a hostname at 127.0.0.1 and
/// have us trust the plaintext hop.
pub fn is_loopback_host(host: &str) -> bool {
    let h = host.trim().trim_matches(['[', ']']);
    if h.is_empty() {
        return false;
    }
    // Names first, so `127.evil.com` and `localhost.evil.com` cannot fall
    // through to the IP checks below.
    let lower = h.to_ascii_lowercase();
    if lower == "localhost" || lower.ends_with(".localhost") {
        return true;
    }
    match IpAddr::from_str(h) {
        Ok(IpAddr::V4(v4)) => v4.is_loopback(),
        Ok(IpAddr::V6(v6)) => {
            v6.is_loopback()
                // A v4-mapped v6 literal is still a v4 address: `[::ffff:127.0.0.1]`
                // reaches the same interface.
                || v6.to_ipv4_mapped().is_some_and(|v4| v4.is_loopback())
        }
        Err(_) => false,
    }
}

/// True only for the 127.0.0.0/8 block, for callers that need the check
/// spelled out (kept here so the reasoning lives in one place).
pub fn is_loopback_v4(addr: Ipv4Addr) -> bool {
    addr.is_loopback()
}

/// True for `::1` (and v4-mapped loopback), for symmetry with
/// [`is_loopback_v4`].
pub fn is_loopback_v6(addr: Ipv6Addr) -> bool {
    addr.is_loopback() || addr.to_ipv4_mapped().is_some_and(|v4| v4.is_loopback())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_real_loopback_names_and_addresses() {
        for host in ["localhost", "LOCALHOST", "127.0.0.1", "127.1.2.3", "127.255.255.254", "::1", "[::1]"] {
            assert!(is_loopback_host(host), "{host} must be loopback");
        }
        // RFC 6761 reserves the whole .localhost TLD for loopback.
        assert!(is_loopback_host("openlist.localhost"));
        assert!(is_loopback_host("a.b.localhost"));
    }

    /// The prefix checks this replaces called `127.evil.com` and
    /// `localhost.evil.com` loopback, which for the redirect policy meant an
    /// upstream could steer a plaintext http fetch to a host it controls.
    #[test]
    fn rejects_names_that_merely_start_with_a_loopback_name() {
        for host in [
            "127.evil.com",
            "localhost.evil.com",
            "127.0.0.1.evil.com",
            "localhosts",
            "notlocalhost",
            "128.0.0.1",
            "0.0.0.0",
            "10.0.0.1",
            "example.com",
            "",
        ] {
            assert!(!is_loopback_host(host), "{host} must NOT be loopback");
        }
    }

    #[test]
    fn accepts_v4_mapped_v6_loopback() {
        assert!(is_loopback_host("::ffff:127.0.0.1"));
        assert!(is_loopback_v6("::ffff:127.0.0.1".parse().unwrap()));
        assert!(is_loopback_v4("127.0.0.1".parse().unwrap()));
        assert!(!is_loopback_v6("::2".parse().unwrap()));
        assert!(!is_loopback_v4("10.0.0.1".parse().unwrap()));
    }
}

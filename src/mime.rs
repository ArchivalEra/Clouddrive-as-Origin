//! MIME fallback (spec §3.9): many provider APIs return generic
//! `application/octet-stream` for media (`.flac`, `.webp`, `.avif`,
//! `.lrc`, ...), which makes browsers pop a download dialog instead of
//! streaming in-player.
//!
//! Wheel reuse: `mime_guess` provides the standard extension table; this
//! module only adds the "override generic hints" policy and a tiny
//! fallback list for formats `mime_guess` misses (e.g. `.lrc`).

//! Formats the standard tables commonly miss are patched below.

/// Resolve the effective Content-Type for a key given the provider hint.
/// Overrides only when the hint is missing or generic; otherwise the
/// provider's answer is passed through untouched.
pub fn resolve(key: &str, hint: &Option<String>) -> Option<String> {
    match hint.as_deref() {
        Some(h) if !h.is_empty() && !h.eq_ignore_ascii_case(GENERIC_MIME) => Some(h.to_string()),
        _ => {
            // Standard table first.
            let guessed = mime_guess::from_path(key).first().map(|m| m.to_string());
            match guessed {
                // Trust the standard table unless it fell back to octet-stream.
                Some(g) if !g.eq_ignore_ascii_case(GENERIC_MIME) => Some(g),
                _ => fallback(key).map(|s| s.to_string()),
            }
        }
    }
}

/// Formats the standard tables commonly miss.
fn fallback(key: &str) -> Option<&'static str> {
    let ext = key.rsplit('.').next()?.to_ascii_lowercase();
    Some(match ext.as_str() {
        "lrc" => "text/plain",
        "flac" => "audio/flac",
        "jxl" => "image/jxl",
        _ => return None,
    })
}

pub const GENERIC_MIME: &str = "application/octet-stream";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overrides_generic_hint() {
        assert_eq!(resolve("a.flac", &Some("application/octet-stream".into())), Some("audio/flac".into()));
        assert_eq!(resolve("a.webp", &None), Some("image/webp".into()));
        assert_eq!(resolve("x/y.lrc", &Some(GENERIC_MIME.into())), Some("text/plain".into()));
    }

    #[test]
    fn passes_through_specific_hints() {
        assert_eq!(resolve("a.png", &Some("image/custom".into())), Some("image/custom".into()));
    }

    #[test]
    fn unknown_extension_yields_none() {
        assert_eq!(resolve("a.weird", &None), None);
        assert_eq!(resolve("noext", &None), None);
    }
}

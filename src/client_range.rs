//! The client's Range request: what it asked for, and which bytes that is.
//!
//! Two decisions, both pure, both about the REQUEST rather than about serving:
//! what shape the header is ([`parse`]), and — once the object's size is known —
//! which span the response carries or whether the request is unsatisfiable
//! ([`resolve`]).
//!
//! They live together because they were answered in two places: `GET` resolved
//! the suffix form itself (it needs the size up front, so it takes one stat
//! before serving) and `HEAD` resolved every form itself (it needs the length and
//! the content-range for the response), with the suffix arithmetic and the 416
//! arms written out twice. The two copies had already started to drift: the
//! comment above one of them claimed a 416 precedence that `Cache::resolve` runs
//! ahead of.
//!
//! `GET` still hands a single range to the serve path unresolved, and that is
//! deliberate rather than a second home: the serve path resolves it against the
//! size it will actually use — the one its own version gate just confirmed —
//! instead of a size read separately a moment earlier. The suffix form needs a
//! size before it can even be a range, which is why it comes through here.

use axum::http::HeaderMap;

use crate::backend::ByteRange;

#[derive(Debug, Clone, Copy)]
pub(crate) enum ClientRange {
    Absent,
    Single(ByteRange),
    /// Suffix request `bytes=-N` (N >= 1; `bytes=-0` is malformed → 416).
    Suffix(u64),
    Multi,
}

/// Parse the Range header. `Err` = syntactically malformed → 416
/// InvalidRange (AWS behavior; a server MAY ignore Range, S3 does not).
pub(crate) fn parse(headers: &HeaderMap) -> Result<ClientRange, ()> {
    let raw = match headers.get("range").and_then(|v| v.to_str().ok()) {
        None => return Ok(ClientRange::Absent),
        Some(r) => r,
    };
    let parsed = http_range_header::parse_range_header(raw).map_err(|_| ())?;
    if parsed.ranges.len() > 1 {
        // S3 has no multipart/byteranges: reject, do not coalesce.
        return Ok(ClientRange::Multi);
    }
    let r = &parsed.ranges[0];
    match (r.start, r.end) {
        (http_range_header::StartPosition::FromLast(n), _) => Ok(ClientRange::Suffix(n)),
        (http_range_header::StartPosition::Index(s), http_range_header::EndPosition::LastByte) => {
            Ok(ClientRange::Single(ByteRange::from_offset(s)))
        }
        (http_range_header::StartPosition::Index(s), http_range_header::EndPosition::Index(e)) => {
            if s > e {
                // Reversed (`bytes=100-50`): unsatisfiable. ByteRange cannot
                // express it (length would underflow), so reject at the parse.
                return Err(());
            }
            Ok(ClientRange::Single(ByteRange::bounded(s, e - s + 1)))
        }
    }
}

/// Which span the response carries, once the object's size is known.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RequestedSpan {
    /// No Range header: the whole representation, as a 200.
    Whole,
    /// `[offset, offset + len)` — always within the object, and never empty.
    Span { offset: u64, len: u64 },
    /// 416: the request names no byte of this object.
    Unsatisfiable,
}

/// The requested-range decision: `size` bytes exist, so what does this request
/// ask for? RFC 9110's rules, in one place:
///
/// * a suffix longer than the representation is the whole representation, still
///   a 206 rather than a 200;
/// * `bytes=-0` and a suffix against an empty object are unsatisfiable;
/// * a range that starts at or past the end is unsatisfiable;
/// * a range that runs past the end is truncated to it.
///
/// `Multi` is rejected by [`parse`] before this is reached, and `Absent` needs no
/// size at all — both are here so a caller can pass whatever it parsed.
pub(crate) fn resolve(parsed: &ClientRange, size: u64) -> RequestedSpan {
    match parsed {
        ClientRange::Absent => RequestedSpan::Whole,
        ClientRange::Multi => RequestedSpan::Unsatisfiable,
        ClientRange::Suffix(n) => {
            if size == 0 || *n == 0 {
                return RequestedSpan::Unsatisfiable;
            }
            let len = (*n).min(size);
            RequestedSpan::Span { offset: size - len, len }
        }
        ClientRange::Single(r) => {
            if r.offset >= size {
                return RequestedSpan::Unsatisfiable;
            }
            let end = r.length.map_or(size, |l| (r.offset + l).min(size));
            RequestedSpan::Span { offset: r.offset, len: end - r.offset }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header(value: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert("range", value.parse().unwrap());
        h
    }

    fn span(parsed: &ClientRange, size: u64) -> (u64, u64) {
        match resolve(parsed, size) {
            RequestedSpan::Span { offset, len } => (offset, len),
            other => panic!("expected a span, got {other:?}"),
        }
    }

    #[test]
    fn the_header_shapes_parse() {
        assert!(matches!(parse(&HeaderMap::new()).unwrap(), ClientRange::Absent));
        assert!(matches!(parse(&header("bytes=10-20")).unwrap(), ClientRange::Single(_)));
        assert!(matches!(parse(&header("bytes=-30")).unwrap(), ClientRange::Suffix(30)));
        assert!(matches!(parse(&header("bytes=5-")).unwrap(), ClientRange::Single(_)));
        assert!(matches!(parse(&header("bytes=0-1,3-4")).unwrap(), ClientRange::Multi));
        assert!(parse(&header("bytes=100-50")).is_err(), "reversed");
        assert!(parse(&header("bytes=-0")).is_err(), "an empty suffix is malformed");
        assert!(parse(&header("items=0-1")).is_err(), "another unit");
    }

    /// The rules the response's span is clamped by — the ones the two handlers
    /// used to hold separately, which is why four of them had no test.
    #[test]
    fn the_requested_span_is_clamped_by_the_size() {
        let size = 100;

        // No header: the whole thing, and the size is irrelevant.
        assert_eq!(resolve(&ClientRange::Absent, 0), RequestedSpan::Whole);
        assert_eq!(resolve(&ClientRange::Absent, size), RequestedSpan::Whole);

        // A single range: inside, open-ended, running past the end, and at the
        // edge of both ends.
        assert_eq!(span(&ClientRange::Single(ByteRange::bounded(10, 20)), size), (10, 20));
        assert_eq!(span(&ClientRange::Single(ByteRange::from_offset(10)), size), (10, 90));
        assert_eq!(
            span(&ClientRange::Single(ByteRange::bounded(90, 50)), size),
            (90, 10),
            "a range past the end is truncated to it"
        );
        assert_eq!(span(&ClientRange::Single(ByteRange::bounded(99, 1)), size), (99, 1));
        assert_eq!(
            resolve(&ClientRange::Single(ByteRange::from_offset(100)), size),
            RequestedSpan::Unsatisfiable,
            "starting at the end names no byte"
        );
        assert_eq!(
            resolve(&ClientRange::Single(ByteRange::bounded(500, 1)), size),
            RequestedSpan::Unsatisfiable
        );

        // The suffix form: shorter than the object, longer than it (the whole
        // object, still a 206), empty, and against an empty object.
        assert_eq!(span(&ClientRange::Suffix(30), size), (70, 30));
        assert_eq!(span(&ClientRange::Suffix(100), size), (0, 100), "exactly the object");
        assert_eq!(span(&ClientRange::Suffix(500), size), (0, 100), "longer than it");
        assert_eq!(
            resolve(&ClientRange::Suffix(0), size),
            RequestedSpan::Unsatisfiable,
            "bytes=-0 asks for nothing"
        );
        assert_eq!(resolve(&ClientRange::Suffix(10), 0), RequestedSpan::Unsatisfiable);
        assert_eq!(
            resolve(&ClientRange::Single(ByteRange::from_offset(0)), 0),
            RequestedSpan::Unsatisfiable,
            "no bytes exist, so no range is satisfiable"
        );

        // Multi is rejected at the parse, and unsatisfiable if it ever gets here.
        assert_eq!(resolve(&ClientRange::Multi, size), RequestedSpan::Unsatisfiable);
    }

    /// A span that resolves is never empty and never runs past the object: the
    /// two properties every caller's arithmetic assumes.
    #[test]
    fn a_resolved_span_is_inside_the_object() {
        for size in [1u64, 2, 7, 100, u32::MAX as u64] {
            for parsed in [
                ClientRange::Absent,
                ClientRange::Single(ByteRange::from_offset(0)),
                ClientRange::Single(ByteRange::bounded(0, 1)),
                ClientRange::Single(ByteRange::from_offset(size - 1)),
                ClientRange::Single(ByteRange::bounded(size / 2, size)),
                ClientRange::Suffix(1),
                ClientRange::Suffix(size),
                ClientRange::Suffix(size * 2),
                ClientRange::Suffix(0),
                ClientRange::Multi,
            ] {
                if let RequestedSpan::Span { offset, len } = resolve(&parsed, size) {
                    assert!(len > 0, "an empty span is not a span: {parsed:?} size={size}");
                    assert!(
                        offset.saturating_add(len) <= size,
                        "{parsed:?} size={size} resolved past the end: {offset}+{len}"
                    );
                }
            }
        }
    }
}

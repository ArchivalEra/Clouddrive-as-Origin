//! One builder for the upstream half of a ranged response (ADR-0021).
//!
//! Three paths serve "[staged pieces +] one upstream Range": the run-refused
//! escape, the plain passthrough, and the standalone escape that stages what it
//! serves. Each carried its own copy of the 256 KiB pump, and when ADR-0019
//! taught the escape not to re-fetch a staged prefix the fix landed in one of
//! the three. That is the argument for this module: the shape is built once,
//! and the one path that writes while it serves says so with `sink: Some(..)`
//! instead of owning a private loop.
//!
//! What stays at the call site is the policy AROUND the body: which range to
//! open, whether a NotFound installs a negative tombstone, whether the request
//! may be served at all, and the seal watcher that covers a viewer which
//! disconnects mid-stream.

use std::path::PathBuf;
use std::sync::Arc;

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::sync::OwnedSemaphorePermit;

use crate::cache::flight::BodyStream;
use crate::cache::staging::{FinalizedSpan, Staging};
use crate::cache::store;
use crate::clock::Clock;

/// Bytes moved per read. Deliberately a second literal next to `flight.rs`'s
/// and `pieces_then`'s: they are three different jobs (an upstream pump, a disk
/// slice, a sidecar replay), and one shared constant would invite a change to
/// one to be justified by another (ADR-0021, "Not covered here").
const CHUNK: usize = 256 * 1024;

/// Take one upstream stream permit (ADR-0004), for as long as the body this is
/// handed to can move bytes.
///
/// `acquire_owned` is fallible only on a CLOSED semaphore, and nothing in this
/// crate closes one — the call sites used to hold the `Result` and so got the
/// permit by accident while checking nothing. A closed gate means the node is
/// shutting down, which is a request error, not a panic and not silence.
pub(crate) async fn stream_permit(
    slot: &Arc<crate::backend::BackendSlot>,
) -> Result<OwnedSemaphorePermit, crate::backend::BackendError> {
    Arc::clone(&slot.stream_gate)
        .acquire_owned()
        .await
        .map_err(|_| crate::backend::BackendError::Other("upstream stream gate is closed".into()))
}

/// Where a served tail is ALSO written, and what the ledger is told once the
/// stream exhausts. `None` on every path that writes nothing (ADR-0019: a
/// request too large to keep is still served, it just leaves no trace).
pub(crate) struct StageSink<C: Clock> {
    /// The in-flight file. It becomes the span's `.seg` by rename at the seal.
    pub(crate) segpart: PathBuf,
    pub(crate) staging: Staging,
    pub(crate) cache_dir: PathBuf,
    pub(crate) key: String,
    pub(crate) upstream_id: String,
    pub(crate) etag: Option<String>,
    pub(crate) total: u64,
    /// The offset the first served-tail byte belongs to: the span's start.
    pub(crate) start: u64,
    pub(crate) clock: Arc<C>,
}

/// The upstream half of a ranged response: bytes from ONE open, at most `want`
/// of them, with the stream gate held by the transfer rather than by the
/// request that built it (ADR-0004).
///
/// That gate is the budget that bounds bandwidth-bound upstream work, and it
/// has to live exactly as long as the body can move bytes and not one request
/// longer: a passthrough that skipped it left upstream pressure set by the
/// client count — the same range requested twice opened two upstream streams.
///
/// `want` is `Some` only where the provider's 206 may not signal EOF at the
/// Content-Length boundary (keep-alive reuse, e.g. rclone serve webdav):
/// waiting for EOF there would hang the staging loop and leave the `.segpart`
/// unsealed forever.
pub(crate) fn upstream_body<C: Clock + 'static>(
    mut stream: Box<dyn AsyncRead + Send + Unpin>,
    want: Option<u64>,
    permit: OwnedSemaphorePermit,
    sink: Option<StageSink<C>>,
) -> BodyStream {
    Box::pin(async_stream::try_stream! {
        // The gate is held by the transfer itself.
        let _stream_permit = permit;
        let sink = sink;
        let mut file: Option<tokio::fs::File> = None;
        let mut written: u64 = 0;
        loop {
            if let Some(w) = want {
                if written >= w {
                    break;
                }
            }
            // Read straight into a fresh buffer and freeze it (P8): the served
            // bytes are handed over with no copy step.
            let mut chunk = BytesMut::with_capacity(CHUNK);
            let n = stream.read_buf(&mut chunk).await?;
            if n == 0 {
                break;
            }
            if let Some(s) = sink.as_ref() {
                // The staged copy has to reach the sidecar file AND the
                // viewer, so this is the one path that needs both a write and
                // a served buffer.
                if file.is_none() {
                    if let Some(parent) = s.segpart.parent() {
                        tokio::fs::create_dir_all(parent).await?;
                    }
                    file = Some(tokio::fs::File::create(&s.segpart).await?);
                }
                file.as_mut().unwrap().write_all(&chunk).await?;
            }
            written += n as u64;
            yield chunk.freeze();
        }
        if let Some(s) = sink {
            // Close before sealing: the ledger claims a file that is whole on
            // disk, not one still open for writing.
            drop(file);
            seal(s, written).await;
        }
    })
}

/// Merge the served bytes into the ledger and rename `.segpart` to `.seg` as
/// ONE step ([`Staging::seal_renamed`]): a failure swallowed between those two
/// is what leaves the ledger describing a span with no file behind it.
async fn seal<C: Clock>(sink: StageSink<C>, written: u64) {
    if written == 0 {
        // Nothing arrived, so no file was ever created: nothing to claim.
        return;
    }
    let seg = store::seg_path(&sink.cache_dir, &sink.key, sink.start, sink.start + written);
    sink.staging
        .seal_renamed(
            &sink.segpart,
            &seg,
            FinalizedSpan {
                cache_dir: sink.cache_dir,
                key: sink.key,
                upstream_id: sink.upstream_id,
                etag: sink.etag,
                total: sink.total,
                start: sink.start,
                end: sink.start + written,
                bytes: written,
                now_millis: sink.clock.now_millis(),
            },
        )
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The read granularity this module moves bytes in — pinned so a change is
    /// deliberate (see the note in `staging.rs`), and because it is duplicated
    /// on purpose next to `flight.rs`'s and `pieces_then`'s.
    #[test]
    fn constant_decisions_are_pinned() {
        assert_eq!(CHUNK, 256 * 1024, "read granularity");
    }
}

//! In-flight download plumbing: one driver task pumps a remote object into
//! a temp file while any number of readers stream the file as it grows
//! (water-pipe, spec §3.1). The watch channel is the single source of
//! truth for progress; readers never talk to the backend.

use bytes::Bytes;
use futures::stream::BoxStream;
use tokio::sync::watch;

use crate::{
    backend::{BackendError, ObjectMeta, StreamSource},
    cache::store,
};

/// Body stream handed to the business plane.
pub type BodyStream = BoxStream<'static, Result<Bytes, std::io::Error>>;

/// Convert a backend stream directly into a body (dual-channel Range
/// passthrough, spec §3.8: the client stream starts at the requested
/// offset immediately while the full-file flight fills the cache).
pub fn passthrough_body(mut src: StreamSource) -> BodyStream {
    Box::pin(async_stream::try_stream! {
        let mut buf = vec![0u8; 256 * 1024];
        use tokio::io::AsyncReadExt;
        loop {
            let n = src.stream.read(&mut buf).await.map_err(|e| std::io::Error::other(e.to_string()))?;
            if n == 0 {
                break;
            }
            yield Bytes::copy_from_slice(&buf[..n]);
        }
    })
}

#[derive(Debug, Clone)]
pub enum FlightProgress {
    /// Driver started; metadata not yet available.
    Pending,
    /// stat() resolved — headers can go out (Content-Length = total).
    Meta(ObjectMeta),
    /// Download in progress; bytes written so far.
    Growing(u64),
    /// File sealed (renamed into place) and meta installed.
    Done,
    /// Download failed; the temp file is garbage (startup sweep removes it).
    Failed(BackendError),
}

/// Shared state of one in-flight (or completed) cold-miss download.
/// Created synchronously *before* any await so a sequential stampede
/// always attaches to the same flight — no TOCTOU window.
pub struct FlightShared {
    pub tmp_path: std::path::PathBuf,
    pub final_path: std::path::PathBuf,
    pub progress_tx: watch::Sender<FlightProgress>,
}

impl FlightShared {
    pub fn new(tmp_path: std::path::PathBuf, final_path: std::path::PathBuf) -> Self {
        let (progress_tx, _) = watch::channel(FlightProgress::Pending);
        Self { tmp_path, final_path, progress_tx }
    }

    pub fn subscribe(&self) -> watch::Receiver<FlightProgress> {
        self.progress_tx.subscribe()
    }
}

/// Stream the final cache file (already complete on disk), optionally
/// from `offset` for `len` bytes (Range hits; len = bytes from offset).
pub fn file_body(path: std::path::PathBuf, offset: u64, len: u64) -> BodyStream {
    Box::pin(async_stream::try_stream! {
        let file = tokio::fs::File::open(&path).await?;
        use tokio::io::AsyncSeekExt;
        let mut reader = tokio::io::BufReader::with_capacity(256 * 1024, file);
        reader.seek(std::io::SeekFrom::Start(offset)).await?;
        let mut remaining = len;
        let mut buf = vec![0u8; 256 * 1024];
        while remaining > 0 {
            let want = buf.len().min(remaining as usize);
            let n = tokio::io::AsyncReadExt::read(&mut reader, &mut buf[..want]).await?;
            if n == 0 {
                break; // short file — tolerate
            }
            remaining -= n as u64;
            yield Bytes::copy_from_slice(&buf[..n]);
        }
    })
}

/// Stream a file while its driver writes it. Retries `File::open` until
/// the driver creates the temp file, follows the growing length, and
/// terminates on Done (EOF) or Failed.
pub fn growing_reader(flight: std::sync::Arc<FlightShared>) -> BodyStream {
    Box::pin(async_stream::try_stream! {
        let mut rx = flight.subscribe();
        let mut pos: u64 = 0;
        let mut buf = vec![0u8; 256 * 1024];
        let mut file: Option<tokio::fs::File> = None;
        loop {
            // Ensure the temp file exists (driver creates it right after
            // publishing Meta). After the seal-rename, late openers fall
            // back to the final path.
            if file.is_none() {
                match tokio::fs::File::open(&flight.tmp_path).await {
                    Ok(f) => file = Some(f),
                    Err(_) => {
                        let st = rx.borrow().clone();
                        match st {
                            FlightProgress::Done => {
                                match tokio::fs::File::open(&flight.final_path).await {
                                    Ok(f) => file = Some(f),
                                    Err(e) => Err(e)?,
                                }
                            }
                            FlightProgress::Failed(e) => {
                                Err(std::io::Error::other(format!("upstream download failed: {e}")))?;
                            }
                            _ => {
                                if rx.changed().await.is_err() {
                                    break; // sender dropped without Done
                                }
                            }
                        }
                        continue;
                    }
                }
            }
            let f = file.as_mut().unwrap();
            use tokio::io::{AsyncReadExt, AsyncSeekExt};
            f.seek(std::io::SeekFrom::Start(pos)).await?;
            let n = f.read(&mut buf).await?;
            if n > 0 {
                pos += n as u64;
                yield Bytes::copy_from_slice(&buf[..n]);
                continue;
            }
            // Caught up with the writer.
            let st = rx.borrow().clone();
            match st {
                FlightProgress::Done => break, // EOF + sealed = complete
                FlightProgress::Failed(e) => {
                    Err(std::io::Error::other(format!("upstream download failed: {e}")))?;
                }
                _ => {
                    if rx.changed().await.is_err() {
                        break; // sender dropped; treat as end
                    }
                }
            }
        }
    })
}

/// Pump a remote stream into the temp file, publishing progress; then
/// seal (fsync + rename) and report Done. Returns the meta handed in —
/// the CALLER installs metadata (it knows upstream id / key).
pub async fn pump_and_seal(
    mut src: StreamSource,
    tmp_path: &std::path::Path,
    final_path: &std::path::Path,
    tx: &watch::Sender<FlightProgress>,
) -> Result<(), BackendError> {
    let mut out = tokio::fs::File::create(tmp_path)
        .await
        .map_err(|e| BackendError::Other(format!("create tmp: {e}")))?;
    let mut written: u64 = 0;
    let mut buf = vec![0u8; 256 * 1024];
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    loop {
        let n = src
            .stream
            .read(&mut buf)
            .await
            .map_err(|e| BackendError::ServerError(format!("read stream: {e}")))?;
        if n == 0 {
            break;
        }
        out.write_all(&buf[..n])
            .await
            .map_err(|e| BackendError::Other(format!("write tmp: {e}")))?;
        written += n as u64;
        let _ = tx.send(FlightProgress::Growing(written));
    }
    out.flush()
        .await
        .map_err(|e| BackendError::Other(format!("flush tmp: {e}")))?;
    out.sync_all()
        .await
        .map_err(|e| BackendError::Other(format!("fsync tmp: {e}")))?;
    drop(out);
    store::install_tmp(tmp_path, final_path).map_err(|e| BackendError::Other(e.to_string()))?;
    // NOTE: the driver sends FlightProgress::Done *after* installing the
    // metadata row, so readers that finish streaming always see a
    // consistent cache state. Pump alone only guarantees the rename.
    Ok(())
}

/// Drain a body to nothing (prewarm / internal fetches).
pub async fn drain(body: &mut BodyStream) -> Result<u64, BackendError> {
    use futures::StreamExt;
    let mut total = 0u64;
    while let Some(chunk) = body.next().await {
        match chunk {
            Ok(b) => total += b.len() as u64,
            Err(e) => return Err(BackendError::ServerError(format!("body error: {e}"))),
        }
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn file_body_streams_exact_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("x.bin");
        std::fs::write(&p, b"hello-stream").unwrap();
        let mut body = file_body(p, 0, 12);
        use futures::StreamExt;
        let mut out = Vec::new();
        while let Some(chunk) = body.next().await {
            out.extend_from_slice(&chunk.unwrap());
        }
        assert_eq!(out, b"hello-stream");
    }

    #[tokio::test]
    async fn file_body_range_slices() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("x.bin");
        std::fs::write(&p, b"hello-stream").unwrap();
        let mut body = file_body(p, 6, 6); // "stream"
        use futures::StreamExt;
        let mut out = Vec::new();
        while let Some(chunk) = body.next().await {
            out.extend_from_slice(&chunk.unwrap());
        }
        assert_eq!(out, b"stream");
    }

    #[tokio::test]
    async fn growing_reader_follows_writer() {
        let dir = tempfile::tempdir().unwrap();
        let tmp = dir.path().join(".tmp.x");
        let finalp = dir.path().join("x.bin");
        let flight = std::sync::Arc::new(FlightShared::new(tmp.clone(), finalp.clone()));
        let mut body = growing_reader(flight.clone());
        let mut body2 = growing_reader(flight.clone());

        let payload: Vec<u8> = (0..600_000u32).map(|i| (i % 251) as u8).collect();
        let driver = {
            let flight = flight.clone();
            let payload = payload.clone();
            tokio::spawn(async move {
                // Simulate a backend delivering one full stream; the pump
                // writes through our tmp path and seals into final_path.
                let src = StreamSource {
                    stream: Box::new(std::io::Cursor::new(payload)),
                    total_len: Some(600_000),
                };
                let meta = ObjectMeta { size_bytes: 600_000, etag: None, last_modified: None, mime_hint: None };
                let _ = flight.progress_tx.send(FlightProgress::Meta(meta));
                pump_and_seal(src, &tmp, &finalp, &flight.progress_tx).await.unwrap();
                let _ = flight.progress_tx.send(FlightProgress::Done);
            })
        };
        // Both readers must see the full payload despite starting mid-write.
        use futures::StreamExt;
        let mut out1 = Vec::new();
        while let Some(c) = body.next().await {
            out1.extend_from_slice(&c.unwrap());
        }
        let mut out2 = Vec::new();
        while let Some(c) = body2.next().await {
            out2.extend_from_slice(&c.unwrap());
        }
        driver.await.unwrap();
        assert_eq!(out1, payload);
        assert_eq!(out2, payload);
    }
}

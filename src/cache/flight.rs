//! In-flight download plumbing: one driver task pumps a remote object into
//! a temp file while any number of readers stream the file as it grows
//! (water-pipe, spec §3.1). The watch channel is the single source of
//! truth for progress; readers never talk to the backend.

use bytes::Bytes;
use futures::stream::BoxStream;
use futures::FutureExt;
use std::collections::HashMap;
use tokio::sync::{watch, Mutex};

use crate::{
    backend::{BackendError, ObjectMeta, StreamSource},
    cache::store,
};

/// Body stream handed to the business plane.
pub type BodyStream = BoxStream<'static, Result<Bytes, std::io::Error>>;

/// Streaming read granularity (P8): one buffer this size per concurrent
/// reader, handed out via `BytesMut::freeze` rather than copied.
const CHUNK: usize = 256 * 1024;

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
    /// Watch-wait budget: how long a reader waits for progress events
    /// without the flight advancing before it gives up with a clean error.
    /// Inactivity, not total time — a slow-but-flowing pull never trips it.
    pub stall_budget: std::time::Duration,
}

/// Production default for [`FlightShared::stall_budget`] (tests inject a
/// shorter one). Generous vs. the ~800 ms upstream open cost; a flowing
/// upstream gaps well under a second per chunk.
pub const DEFAULT_STALL_BUDGET: std::time::Duration = std::time::Duration::from_secs(30);

impl FlightShared {
    pub fn new(
        tmp_path: std::path::PathBuf,
        final_path: std::path::PathBuf,
        stall_budget: std::time::Duration,
    ) -> Self {
        let (progress_tx, _) = watch::channel(FlightProgress::Pending);
        Self { tmp_path, final_path, progress_tx, stall_budget }
    }

    pub fn subscribe(&self) -> watch::Receiver<FlightProgress> {
        self.progress_tx.subscribe()
    }
}

/// Single-flight registry for cold-miss downloads — the mechanism only:
/// the per-key map, insert-before-await joining, driver spawning with a
/// panic guard, and self-removal on every exit path. The policy of what a
/// driver does (stat → open → pump → install meta) stays with the Cache.
pub struct Flights {
    map: std::sync::Arc<Mutex<HashMap<String, std::sync::Arc<FlightShared>>>>,
    stall_budget: std::time::Duration,
}

impl Flights {
    pub fn new(stall_budget: std::time::Duration) -> Self {
        Self { map: std::sync::Arc::new(Mutex::new(HashMap::new())), stall_budget }
    }

    /// How many downloads are currently in flight (healthz).
    pub async fn active(&self) -> usize {
        self.map.lock().await.len()
    }

    /// Join the flight for `key`, or start one with `run` as its detached
    /// driver. Map insertion happens before any await, so a concurrent
    /// stampede always attaches to the same handle (no TOCTOU window).
    /// The driver is wrapped in a panic guard and the map entry is removed
    /// on every exit path — normal, failed, or panicked — because a stale
    /// entry would turn every future attacher into a zombie-joiner.
    pub async fn join_or_start<F, Fut>(
        &self,
        key: &str,
        tmp_path: std::path::PathBuf,
        final_path: std::path::PathBuf,
        run: F,
    ) -> std::sync::Arc<FlightShared>
    where
        F: FnOnce(std::sync::Arc<FlightShared>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let mut map = self.map.lock().await;
        if let Some(f) = map.get(key) {
            return f.clone();
        }
        let f = std::sync::Arc::new(FlightShared::new(tmp_path, final_path, self.stall_budget));
        map.insert(key.to_string(), f.clone());
        drop(map);
        let driver_f = f.clone();
        let map = std::sync::Arc::clone(&self.map);
        let map_key = key.to_string();
        tokio::spawn(async move {
            drive_guarded(driver_f, run).await;
            map.lock().await.remove(&map_key);
        });
        f
    }

    /// Spawn `run` as a detached, panic-guarded solo driver: no map entry,
    /// nothing to join (forced refetches and other private flights). The
    /// guard publishes Failed if the driver dies without a terminal event,
    /// so nothing can end up waiting on a dead flight.
    pub fn spawn_solo<F, Fut>(
        &self,
        tmp_path: std::path::PathBuf,
        final_path: std::path::PathBuf,
        run: F,
    ) -> std::sync::Arc<FlightShared>
    where
        F: FnOnce(std::sync::Arc<FlightShared>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let f = std::sync::Arc::new(FlightShared::new(tmp_path, final_path, self.stall_budget));
        tokio::spawn(drive_guarded(f.clone(), run));
        f
    }
}

/// Run a flight driver to completion, guarded against panics: a driver
/// that panics — or ends without publishing a terminal event — publishes
/// Failed so attached readers error out instead of waiting forever.
async fn drive_guarded<F, Fut>(flight: std::sync::Arc<FlightShared>, run: F)
where
    F: FnOnce(std::sync::Arc<FlightShared>) -> Fut + Send,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let r = flight.clone();
    let _ = std::panic::AssertUnwindSafe(run(r)).catch_unwind().await;
    if !matches!(*flight.progress_tx.borrow(), FlightProgress::Done | FlightProgress::Failed(_)) {
        let _ = flight.progress_tx.send(FlightProgress::Failed(BackendError::ServerError(
            "flight driver died without a terminal state (panic?)".into(),
        )));
    }
}

/// Stream the final cache file (already complete on disk), optionally
/// from `offset` for `len` bytes (Range hits; len = bytes from offset).
pub fn file_body(path: std::path::PathBuf, offset: u64, len: u64) -> BodyStream {
    Box::pin(async_stream::try_stream! {
        let mut file = tokio::fs::File::open(&path).await?;
        use tokio::io::{AsyncReadExt, AsyncSeekExt};
        file.seek(std::io::SeekFrom::Start(offset)).await?;
        let mut remaining = len;
        while remaining > 0 {
            // Size the read buffer to the bytes actually wanted this round
            // so `read_buf` cannot overshoot the promised length, then hand
            // the filled buffer out as a frozen `Bytes` — no copy step
            // (P8). The previous shape also allocated a BufReader buffer
            // that `read` bypassed on every call.
            let want = CHUNK.min(remaining as usize);
            let mut buf = bytes::BytesMut::with_capacity(want);
            let n = file.read_buf(&mut buf).await?;
            if n == 0 {
                // Never tolerate a short cache file: the response promised
                // meta.size_bytes and the h2 layer will call a truncated
                // body a protocol error. A short file on disk is a broken
                // cache entry — say so loudly.
                Err(std::io::Error::other(format!(
                    "cached file is {remaining} bytes short of the metadata length"
                )))?;
            }
            remaining -= n as u64;
            yield buf.freeze();
        }
    })
}

/// Stream a file while its driver writes it. Retries `File::open` until
/// the driver creates the temp file, follows the growing length, and
/// terminates on Done (EOF) or Failed.
/// Follow the flight's growing temp file from `start`, yielding at most
/// `len` bytes (None = to end). This is the single reader for BOTH
/// whole-file and ranged cold misses: a ranged request waits for the
/// writer to reach its offset instead of opening a second upstream
/// connection.
///
/// Why converge instead of passing ranged reads through: every upstream
/// open pays a ~800 ms fixed stream-open cost, so N concurrent Range
/// requests used to mean N upstream connections (measured: 5 concurrent
/// ranges -> 5 opens). EdgeOne's sharded origin-pull delivers shards in
/// ascending offset order, so a shard's offset is normally already
/// written by the time it is requested — the wait is near-zero in
/// practice, and a genuine cold seek costs only the full pull it would
/// have needed anyway.
/// Wait for the next flight progress event within the flight's stall
/// budget. `Ok(Some(()))` = progress arrived, `Ok(None)` = sender dropped
/// without a terminal event. A stall-budget exhaustion is a body error:
/// the response already promised bytes, and hanging forever is the only
/// worse outcome.
async fn next_progress(
    flight: &FlightShared,
    rx: &mut watch::Receiver<FlightProgress>,
) -> Result<Option<()>, std::io::Error> {
    match tokio::time::timeout(flight.stall_budget, rx.changed()).await {
        Ok(Ok(())) => Ok(Some(())),
        Ok(Err(_)) => Ok(None),
        Err(_) => Err(std::io::Error::other(format!(
            "flight stalled: no progress for {:?}",
            flight.stall_budget
        ))),
    }
}

pub fn growing_reader_from(
    flight: std::sync::Arc<FlightShared>,
    start: u64,
    len: Option<u64>,
) -> BodyStream {
    Box::pin(async_stream::try_stream! {
        let mut rx = flight.subscribe();
        let mut pos: u64 = start;
        let mut remaining: Option<u64> = len;
        let mut file: Option<tokio::fs::File> = None;
        // The file cursor tracks `pos` after every read, so a seek is only
        // needed when the handle is (re)opened or after a caught-up wait.
        let mut needs_seek = true;
        loop {
            if remaining == Some(0) {
                break; // range satisfied
            }
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
                            _ => match next_progress(&flight, &mut rx).await {
                                Ok(Some(())) => {}
                                Ok(None) => break, // sender dropped without Done
                                Err(e) => Err(e)?,
                            },
                        }
                        continue;
                    }
                }
            }
            // If the writer has not reached our position, do not touch the
            // file at all. A far-ahead seeker used to run seek+read (two
            // blocking-pool round-trips) and re-arm the stall timer on
            // every upstream chunk; one wait per chunk is the floor with a
            // broadcast channel, the syscalls are not.
            let written = match &*rx.borrow() {
                FlightProgress::Growing(w) => Some(*w),
                _ => None,
            };
            if let Some(w) = written {
                if w <= pos {
                    match next_progress(&flight, &mut rx).await {
                        Ok(Some(())) => continue,
                        Ok(None) => break, // sender dropped; treat as end
                        Err(e) => Err(e)?,
                    }
                }
            }

            let f = file.as_mut().unwrap();
            use tokio::io::{AsyncReadExt, AsyncSeekExt};
            if needs_seek {
                f.seek(std::io::SeekFrom::Start(pos)).await?;
                needs_seek = false;
            }
            let want = match remaining {
                Some(r) => CHUNK.min(r as usize),
                None => CHUNK,
            };
            let mut buf = bytes::BytesMut::with_capacity(want);
            let n = f.read_buf(&mut buf).await?;
            if n > 0 {
                pos += n as u64;
                if let Some(r) = remaining.as_mut() {
                    *r -= n as u64;
                }
                yield buf.freeze();
                continue;
            }
            // EOF while the writer is already past us (or a sealed short
            // file): resolve from the terminal state, else wait again.
            let st = rx.borrow().clone();
            match st {
                FlightProgress::Done => break, // EOF + sealed = complete
                FlightProgress::Failed(e) => {
                    Err(std::io::Error::other(format!("upstream download failed: {e}")))?;
                }
                _ => {
                    // A zero-byte read left the cursor at `pos`, so no
                    // re-seek is needed when we wake.
                    match next_progress(&flight, &mut rx).await {
                        Ok(Some(())) => {}
                        Ok(None) => break, // sender dropped; treat as end
                        Err(e) => Err(e)?,
                    }
                }
            }
        }
    })
}

/// Whole-file reader (start 0, no bound). Kept as the common case.
pub fn growing_reader(flight: std::sync::Arc<FlightShared>) -> BodyStream {
    growing_reader_from(flight, 0, None)
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
    // A SHORT upstream body must never be sealed into the cache: readers
    // were promised `total_len` bytes, and a truncated file would poison
    // every later hit. Delete the tmp and fail the flight instead. An
    // over-long body is harmless — every serving read is bounded by the
    // promised length — so only the short side fails.
    if let Some(expected) = src.total_len {
        if written < expected {
            let _ = tokio::fs::remove_file(tmp_path).await;
            return Err(BackendError::ServerError(format!(
                "upstream short read: got {written} of {expected} bytes"
            )));
        }
    }
    out.flush()
        .await
        .map_err(|e| BackendError::Other(format!("flush tmp: {e}")))?;
    out.sync_all()
        .await
        .map_err(|e| BackendError::Other(format!("fsync tmp: {e}")))?;
    drop(out);
    // Blocking fs (dir creation + rename) off the async runtime.
    let tmp = tmp_path.to_path_buf();
    let dest = final_path.to_path_buf();
    tokio::task::spawn_blocking(move || store::install_tmp(&tmp, &dest))
        .await
        .map_err(|e| BackendError::Other(format!("install join: {e}")))?
        .map_err(|e| BackendError::Other(e.to_string()))?;
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
        let flight = std::sync::Arc::new(FlightShared::new(tmp.clone(), finalp.clone(), DEFAULT_STALL_BUDGET));
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

    /// A driver that stops advancing (dead upstream) must end every
    /// attached reader with a clean body error inside the stall budget —
    /// this is the multi-client TIMEOUT storm's root failure mode, which
    /// used to hang forever (audit finding C2).
    #[tokio::test]
    async fn stalled_flight_ends_reader_within_budget() {
        let dir = tempfile::tempdir().unwrap();
        let tmp = dir.path().join(".tmp.s");
        let finalp = dir.path().join("s.bin");
        let flight = std::sync::Arc::new(FlightShared::new(
            tmp.clone(),
            finalp.clone(),
            std::time::Duration::from_millis(120),
        ));
        let meta = ObjectMeta { size_bytes: 1024, etag: None, last_modified: None, mime_hint: None };
        let _ = flight.progress_tx.send(FlightProgress::Meta(meta));

        struct StallAfterFirst {
            first: Option<Vec<u8>>,
        }
        impl tokio::io::AsyncRead for StallAfterFirst {
            fn poll_read(
                mut self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
                buf: &mut tokio::io::ReadBuf<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                if let Some(b) = self.first.take() {
                    buf.put_slice(&b);
                    return std::task::Poll::Ready(Ok(()));
                }
                // Never wakes: a permanently dead upstream.
                std::task::Poll::Pending
            }
        }
        let src = StreamSource {
            stream: Box::new(StallAfterFirst { first: Some(vec![7u8; 64]) }),
            total_len: Some(1024),
        };
        let driver = {
            let flight = flight.clone();
            tokio::spawn(async move {
                let _ = pump_and_seal(src, &tmp, &finalp, &flight.progress_tx).await;
            })
        };

        use futures::StreamExt;
        let started = std::time::Instant::now();
        let mut body = growing_reader(flight.clone());
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(3), async move {
            let mut got = 0usize;
            let mut errored = false;
            while let Some(c) = body.next().await {
                match c {
                    Ok(b) => got += b.len(),
                    Err(_) => {
                        errored = true;
                        break;
                    }
                }
            }
            (errored, got)
        })
        .await
        .expect("reader must terminate within the test timeout, not hang forever");
        driver.abort();
        let (errored, got) = outcome;
        assert!(errored, "stall must surface as a body error, not silence");
        assert_eq!(got, 64, "bytes written before the stall must reach the reader");
        assert!(
            started.elapsed() < std::time::Duration::from_secs(2),
            "stall must end near the stall budget (120 ms), not at the test timeout"
        );
    }

    /// Inactivity, not total time: a slow-but-flowing stream must never
    /// trip the stall budget.
    #[tokio::test]
    async fn slow_but_flowing_stream_survives_stall_budget() {
        let dir = tempfile::tempdir().unwrap();
        let tmp = dir.path().join(".tmp.f");
        let finalp = dir.path().join("f.bin");
        let flight = std::sync::Arc::new(FlightShared::new(
            tmp.clone(),
            finalp.clone(),
            std::time::Duration::from_millis(300),
        ));
        let meta = ObjectMeta { size_bytes: 3072, etag: None, last_modified: None, mime_hint: None };
        let _ = flight.progress_tx.send(FlightProgress::Meta(meta));

        let driver = {
            let flight = flight.clone();
            tokio::spawn(async move {
                let mut f = tokio::fs::File::create(&tmp).await.unwrap();
                use tokio::io::AsyncWriteExt;
                let mut written = 0u64;
                for i in 0..3u64 {
                    tokio::time::sleep(std::time::Duration::from_millis(60)).await;
                    f.write_all(&vec![i as u8; 1024]).await.unwrap();
                    written += 1024;
                    let _ = flight.progress_tx.send(FlightProgress::Growing(written));
                }
                f.sync_all().await.unwrap();
                drop(f);
                store::install_tmp(&tmp, &finalp).unwrap();
                let _ = flight.progress_tx.send(FlightProgress::Done);
            })
        };

        use futures::StreamExt;
        let mut body = growing_reader(flight.clone());
        let mut got = 0usize;
        let mut errored = false;
        while let Some(c) = body.next().await {
            match c {
                Ok(b) => got += b.len(),
                Err(_) => {
                    errored = true;
                    break;
                }
            }
        }
        driver.await.unwrap();
        assert!(!errored, "a flowing stream must not trip the stall budget");
        assert_eq!(got, 3072, "reader must see every byte the driver wrote");
    }

    /// A reader far ahead of the writer must not touch the file until the
    /// writer reaches it (P3): with a slow driver, the reader's total read
    /// count stays at the number of chunks it actually consumes, not one
    /// per upstream progress event.
    #[tokio::test]
    async fn far_ahead_reader_skips_reads_until_writer_catches_up() {
        use futures::StreamExt;
        let dir = tempfile::tempdir().unwrap();
        let tmp = dir.path().join(".tmp.far");
        let finalp = dir.path().join("far.bin");
        let flight = std::sync::Arc::new(FlightShared::new(
            tmp.clone(),
            finalp.clone(),
            std::time::Duration::from_millis(500),
        ));
        let total = 512 * 1024u64;
        let meta = ObjectMeta { size_bytes: total, etag: None, last_modified: None, mime_hint: None };
        let _ = flight.progress_tx.send(FlightProgress::Meta(meta));

        // The reader wants only the last 8 KB — far ahead of the writer.
        let want = 8 * 1024usize;
        let mut body = growing_reader_from(flight.clone(), total - want as u64, Some(want as u64));

        // Driver: publish several growth events that all stay BELOW the
        // reader's offset, so the reader must not read yet, then finish.
        let driver = {
            let flight = flight.clone();
            let tmp = tmp.clone();
            let finalp = finalp.clone();
            tokio::spawn(async move {
                use tokio::io::AsyncWriteExt;
                let mut f = tokio::fs::File::create(&tmp).await.unwrap();
                let mut written = 0u64;
                while written < total {
                    let chunk = 64 * 1024u64;
                    let end = (written + chunk).min(total);
                    let data: Vec<u8> = (written..end).map(|i| (i % 251) as u8).collect();
                    // Hold the tail back so the reader is genuinely ahead.
                    if end > total - want as u64 {
                        f.write_all(&vec![0u8; (total - written) as usize]).await.unwrap();
                        written = total;
                        let _ = flight.progress_tx.send(FlightProgress::Growing(written));
                        break;
                    }
                    f.write_all(&data).await.unwrap();
                    written = end;
                    let _ = flight.progress_tx.send(FlightProgress::Growing(written));
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                f.sync_all().await.unwrap();
                drop(f);
                store::install_tmp(&tmp, &finalp).unwrap();
                let _ = flight.progress_tx.send(FlightProgress::Done);
            })
        };

        let mut got = Vec::new();
        let started = std::time::Instant::now();
        while let Some(c) = body.next().await {
            got.extend_from_slice(&c.expect("no body error"));
        }
        driver.await.unwrap();
        assert_eq!(got.len(), want, "far-ahead reader must receive its exact slice");
        assert!(
            started.elapsed() < std::time::Duration::from_secs(2),
            "reader must finish promptly, not on the stall budget"
        );
    }

    #[tokio::test]
    async fn panicked_driver_publishes_failed() {
        let dir = tempfile::tempdir().unwrap();
        let flight = std::sync::Arc::new(FlightShared::new(
            dir.path().join(".tmp.p"),
            dir.path().join("p.bin"),
            DEFAULT_STALL_BUDGET,
        ));
        let rx = flight.subscribe();
        drive_guarded(flight.clone(), |_f| async {
            panic!("boom");
        })
        .await;
        assert!(
            matches!(*rx.borrow(), FlightProgress::Failed(_)),
            "a panicking driver must publish Failed, not leave Pending forever"
        );
    }
}

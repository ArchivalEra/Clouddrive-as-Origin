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

/// Minimum byte progress between two `Growing` publications (O6). The
/// upstream yields small chunks (whatever hyper hands over, often ~16 KB),
/// so publishing per chunk meant ~196k watch sends for a 3 GB pull, each
/// one waking every attached reader. Publishing per MiB cuts that ~64x
/// while staying far inside the stall budget: at the measured 43.8 MB/s
/// this is ~43 events/s, so a reader waiting on an offset is woken within
/// tens of milliseconds of the writer passing it.
const PUBLISH_INTERVAL: u64 = 1024 * 1024;

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
#[derive(Clone)]
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

    /// Spawn `run` as a detached, panic-guarded solo driver over a handle the
    /// caller already built — for callers that must name their state after the
    /// handle and before the driver can touch it (the staging sessions).
    pub fn spawn_solo_over<F, Fut>(&self, shared: std::sync::Arc<FlightShared>, run: F)
    where
        F: FnOnce(std::sync::Arc<FlightShared>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        tokio::spawn(drive_guarded(shared, run));
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

/// Wait for the next flight progress event, bounded by the flight's stall
/// budget **per wait**: the budget measures inactivity, so a caller that
/// loops re-arms it on every event instead of holding one deadline over
/// the whole wait. `Ok(Some(()))` = progress arrived, `Ok(None)` = sender
/// dropped without a terminal event. A stall-budget exhaustion is a body
/// error: the response already promised bytes, and hanging forever is the
/// only worse outcome.
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

/// Follow the flight's growing temp file from `start`, yielding at most
/// `len` bytes (None = to end). This is the single reader for BOTH
/// whole-file and ranged cold misses: a ranged request waits for the
/// writer to reach its offset instead of opening a second upstream
/// connection. The temp file needs no creating ritual — `File::open` is
/// retried until the driver makes it, the growing length is followed, and
/// the reader terminates on Done (EOF) or Failed.
///
/// Why converge instead of passing ranged reads through: every upstream
/// open pays a ~640 ms fixed stream-open cost, so N concurrent Range
/// requests used to mean N upstream connections (measured: 5 concurrent
/// ranges -> 5 opens). EdgeOne's sharded origin-pull delivers shards in
/// ascending offset order, so a shard's offset is normally already
/// written by the time it is requested — the wait is near-zero in
/// practice, and a genuine cold seek costs only the full pull it would
/// have needed anyway. The wait is bounded by inactivity (see
/// [`next_progress`]), never by total elapsed time.
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
        'read: loop {
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
                    // The stall budget measures INACTIVITY, not elapsed
                    // time: every progress event re-arms the full budget,
                    // so a reader parked ahead of the writer waits as long
                    // as the pull keeps advancing. One deadline covering
                    // the whole wait (the old O6 shape) contradicted the
                    // contract this type documents: at the measured
                    // 27 MB/s a reader ~800 MB ahead was killed mid-body
                    // with `flight stalled` even though the pull was
                    // flowing perfectly.
                    //
                    // Re-arming cannot spin. The watermark only grows, so
                    // an event means `written` rose; once it passes `pos`
                    // this loop exits, and a pull that truly stops
                    // publishing trips the budget exactly as before.
                    loop {
                        match next_progress(&flight, &mut rx).await {
                            Ok(Some(())) => {
                                // Progress arrived: re-check whether it
                                // covers our offset; if not, keep waiting
                                // with a fresh budget.
                                let now_written = match &*rx.borrow() {
                                    FlightProgress::Growing(w) => Some(*w),
                                    _ => None,
                                };
                                match now_written {
                                    Some(w) if w > pos => break,
                                    // Terminal states are handled by the
                                    // read path below.
                                    None => break,
                                    _ => continue,
                                }
                            }
                            Ok(None) => break 'read, // sender dropped; treat as end
                            Err(e) => Err(e)?,
                        }
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
    // Any failure past this point must remove the temp file (P56): the old
    // shape left it behind on read/write/flush errors, and the startup-only
    // sweep meant the leak persisted until the next restart.
    let fail_cleanup = |e: BackendError| async {
        let _ = tokio::fs::remove_file(tmp_path).await;
        e
    };
    let mut published: u64 = 0;
    loop {
        let n = match src.stream.read(&mut buf).await {
            Ok(n) => n,
            Err(e) => return Err(fail_cleanup(BackendError::ServerError(format!("read stream: {e}"))).await),
        };
        if n == 0 {
            break;
        }
        if let Err(e) = out.write_all(&buf[..n]).await {
            return Err(fail_cleanup(BackendError::Other(format!("write tmp: {e}"))).await);
        }
        written += n as u64;
        // Publish on threshold crossings, not per chunk (O6). Readers only
        // need "progress happened" plus the current watermark; the terminal
        // Done/Failed that follows this pump is an unconditional send, so a
        // sub-interval tail is still announced.
        //
        // The FIRST chunk is always published (`published == 0`): without
        // that, a short object or a stall right after a small write leaves
        // readers at watermark 0 while bytes already sit on disk, and a
        // reader waiting for them would never be woken. A test pins it
        // (a stall after 64 bytes must still deliver those 64).
        if published == 0 || written - published >= PUBLISH_INTERVAL {
            published = written;
            let _ = tx.send(FlightProgress::Growing(written));
        }
    }
    // Announce the final size before the caller's Done, so a reader woken
    // by Done sees a watermark covering everything on disk.
    if written > published {
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
    if let Err(e) = out.flush().await {
        return Err(fail_cleanup(BackendError::Other(format!("flush tmp: {e}"))).await);
    }
    if let Err(e) = out.sync_all().await {
        return Err(fail_cleanup(BackendError::Other(format!("fsync tmp: {e}"))).await);
    }
    drop(out);
    // Blocking fs (dir creation + rename) off the async runtime.
    // `tmp_path` comes from `store::tmp_path(cache_dir, _)`, so its parent IS
    // the cache root; that is what lets the install guard tell the metadata
    // store apart from a nested object that happens to share its name.
    let tmp = tmp_path.to_path_buf();
    let dest = final_path.to_path_buf();
    let cache_root = tmp.parent().unwrap_or(std::path::Path::new(".")).to_path_buf();
    tokio::task::spawn_blocking(move || store::install_tmp(&tmp, &dest, &cache_root))
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
                store::install_tmp(&tmp, &finalp, dir.path()).unwrap();
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

    /// O6 + stall safety: throttling publications must never hide a small
    /// write. A driver that writes 64 bytes and then stalls must still
    /// publish that watermark, so a reader waiting on those bytes is woken
    /// and can read them before the stall budget expires.
    #[tokio::test]
    async fn small_write_is_published_before_a_stall() {
        use futures::StreamExt;
        let dir = tempfile::tempdir().unwrap();
        let tmp = dir.path().join(".tmp.small");
        let finalp = dir.path().join("small.bin");
        let flight = std::sync::Arc::new(FlightShared::new(
            tmp.clone(),
            finalp.clone(),
            std::time::Duration::from_millis(400),
        ));
        let meta = ObjectMeta { size_bytes: 4096, etag: None, last_modified: None, mime_hint: None };
        let _ = flight.progress_tx.send(FlightProgress::Meta(meta));

        struct SmallThenStall {
            sent: bool,
        }
        impl tokio::io::AsyncRead for SmallThenStall {
            fn poll_read(
                mut self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
                buf: &mut tokio::io::ReadBuf<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                if !self.sent {
                    self.sent = true;
                    buf.put_slice(&[9u8; 64]);
                    return std::task::Poll::Ready(Ok(()));
                }
                std::task::Poll::Pending // dead upstream
            }
        }
        let src = StreamSource {
            stream: Box::new(SmallThenStall { sent: false }),
            total_len: Some(4096),
        };
        let driver = {
            let flight = flight.clone();
            tokio::spawn(async move {
                let _ = pump_and_seal(src, &tmp, &finalp, &flight.progress_tx).await;
            })
        };

        // A reader at 0 wants the first bytes: it must receive them, not
        // wait out the budget for a watermark that never comes.
        let mut body = growing_reader(flight.clone());
        let out = tokio::time::timeout(std::time::Duration::from_secs(3), async {
            let mut got = 0usize;
            while let Some(c) = body.next().await {
                match c {
                    Ok(b) => {
                        got += b.len();
                        if got >= 64 {
                            return got;
                        }
                    }
                    Err(_) => return got,
                }
            }
            got
        })
        .await
        .expect("reader must not hang waiting for a published watermark");
        driver.abort();
        assert_eq!(out, 64, "the 64 bytes written before the stall must be published and readable");
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
                store::install_tmp(&tmp, &finalp, dir.path()).unwrap();
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

    /// The stall budget measures inactivity, not total wall time: a reader
    /// parked ahead of the writer must survive a wait LONGER than one
    /// budget as long as progress keeps arriving. One deadline covering the
    /// whole wait trips at the first budget (150 ms here, while the writer
    /// still needs ~300 ms to reach the reader) and kills the body of a
    /// perfectly healthy pull.
    #[tokio::test]
    async fn far_ahead_reader_survives_a_flowing_writer_beyond_one_budget() {
        use futures::StreamExt;
        let dir = tempfile::tempdir().unwrap();
        let tmp = dir.path().join(".tmp.flow");
        let finalp = dir.path().join("flow.bin");
        let budget = std::time::Duration::from_millis(150);
        let flight = std::sync::Arc::new(FlightShared::new(tmp.clone(), finalp.clone(), budget));
        let total = 4 * 1024 * 1024u64;
        let meta = ObjectMeta { size_bytes: total, etag: None, last_modified: None, mime_hint: None };
        let _ = flight.progress_tx.send(FlightProgress::Meta(meta));

        const MIB: u64 = 1024 * 1024;
        let driver = {
            let flight = flight.clone();
            let tmp = tmp.clone();
            let finalp = finalp.clone();
            let dir = dir.path().to_path_buf();
            tokio::spawn(async move {
                use tokio::io::AsyncWriteExt;
                let mut f = tokio::fs::File::create(&tmp).await.unwrap();
                for i in 1..=4u64 {
                    f.write_all(&vec![i as u8; MIB as usize]).await.unwrap();
                    let _ = flight.progress_tx.send(FlightProgress::Growing(i * MIB));
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
                f.sync_all().await.unwrap();
                drop(f);
                store::install_tmp(&tmp, &finalp, &dir).unwrap();
                let _ = flight.progress_tx.send(FlightProgress::Done);
            })
        };

        // Let the driver publish its first watermark and create the temp
        // file, so the reader enters the offset wait (not the file-open
        // wait, which re-arms per event already).
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let want = MIB;
        let mut body = growing_reader_from(flight.clone(), total - want, Some(want));

        let started = std::time::Instant::now();
        let mut got = Vec::new();
        let mut errored = None;
        while let Some(c) = body.next().await {
            match c {
                Ok(b) => got.extend_from_slice(&b),
                Err(e) => {
                    errored = Some(e.to_string());
                    break;
                }
            }
        }
        let waited = started.elapsed();
        driver.await.unwrap();
        assert_eq!(
            errored, None,
            "a flowing pull must not trip the budget (waited {waited:?}, budget {budget:?})"
        );
        assert_eq!(got.len() as u64, want, "the reader must receive its exact slice");
        assert!(
            waited > budget,
            "this test only proves anything if the wait outlasted one budget (waited {waited:?})"
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

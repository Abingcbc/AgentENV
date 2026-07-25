use crate::backend::local::LocalFile;
use crate::backend::switch::SwitchFile;
use crate::config::DownloadConfig;
use crate::io::virtual_file::VirtualFile;
use crate::layer::layer_metadata::COMMIT_FILE_NAME;
use anyhow::{bail, Context, Result};
use parking_lot::Mutex;
use reqwest::Client;
use serde::Serialize;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::fmt;
use std::fs::File as StdFile;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use storage_util::io_ring::IoRingHandle;
use tokio::task::JoinSet;

const ALIGNMENT: u64 = 4096;
const DOWNLOAD_TMP_NAME: &str = ".download";
const POLL_INTERVAL: Duration = Duration::from_millis(200);
const SHA256_BUFFER_SIZE: usize = 64 * 1024;

fn locked_download_dirs() -> &'static Mutex<HashSet<PathBuf>> {
    static LOCKED_DOWNLOAD_DIRS: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();
    LOCKED_DOWNLOAD_DIRS.get_or_init(|| Mutex::new(HashSet::new()))
}

fn p2p_publish_client() -> &'static Client {
    static CLIENT: OnceLock<Client> = OnceLock::new();
    CLIENT.get_or_init(Client::new)
}

fn encode_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

fn sha256sum(path: PathBuf) -> Result<String> {
    let mut file = StdFile::open(&path)?;
    let mut context = ring::digest::Context::new(&ring::digest::SHA256);
    let mut buffer = vec![0u8; SHA256_BUFFER_SIZE];

    loop {
        let got = file.read(&mut buffer)?;
        if got == 0 {
            break;
        }
        context.update(&buffer[..got]);
    }

    Ok(format!("sha256:{}", encode_hex(context.finish().as_ref())))
}

struct DownloadDirLock {
    dir: PathBuf,
}

impl DownloadDirLock {
    fn try_lock(dir: &Path) -> Option<Self> {
        let mut guard = locked_download_dirs().lock();
        if !guard.insert(dir.to_path_buf()) {
            return None;
        }
        Some(Self {
            dir: dir.to_path_buf(),
        })
    }
}

impl Drop for DownloadDirLock {
    fn drop(&mut self) {
        locked_download_dirs().lock().remove(&self.dir);
    }
}

/// Shared rate limiter across the concurrent block tasks of one layer
/// download. Created only when max_mbps > 0.
struct BkThrottle {
    started: std::time::Instant,
    downloaded: u64,
    limit_bps: u64,
}

fn throttle_sleep_for(started: std::time::Instant, downloaded: u64, limit_bps: u64) -> Duration {
    let expected = downloaded as f64 / limit_bps as f64;
    let overdue = expected - started.elapsed().as_secs_f64();
    if overdue <= 0.0 {
        Duration::ZERO
    } else {
        Duration::from_secs_f64(overdue)
    }
}

/// Process-wide cap on in-flight background-download block I/O. Bounds total
/// scratch memory (permits × block_size) shared by every concurrent layer
/// download in this process, independent of the per-image concurrency knob.
/// Sized from the `maxInflightBlocks` download config (default 16) of the
/// first download to run; in practice every layer on a node shares the same
/// configured value. Larger per-image concurrency simply queues on this
/// semaphore (tested: concurrency 24 against 16 slots cannot deadlock).
static GLOBAL_BK_BLOCK_SLOTS: std::sync::OnceLock<tokio::sync::Semaphore> =
    std::sync::OnceLock::new();

fn global_bk_block_slots(configured: usize) -> &'static tokio::sync::Semaphore {
    GLOBAL_BK_BLOCK_SLOTS.get_or_init(|| tokio::sync::Semaphore::new(configured.max(1)))
}

/// Straggler hedge: a block read taking longer than this is abandoned (only
/// the HTTP stream is dropped — the task-owned scratch and io_uring are
/// untouched) and reissued on a fresh pooled connection, at most
/// HEDGE_MAX_ATTEMPTS times. 6s is far above the observed p95 block time
/// (~1s) yet kills the per-connection throttling stragglers (~2MB/s × 45s+)
/// seen on both public and internal OSS endpoints.
const HEDGE_TIMEOUT: Duration = Duration::from_secs(6);
const HEDGE_MAX_ATTEMPTS: u32 = 3;

pub struct BkDownload {
    dir: PathBuf,
    pub try_cnt: u32,
    switch_file: Arc<SwitchFile>,
    src_file: Arc<dyn VirtualFile>,
    file_size: u64,
    digest: String,
    url: String,
    block_size: u32,
    concurrency: usize,
    max_mbps: i32,
    max_inflight_blocks: usize,
    hedge_timeout: Duration,
    retry_attempts: u32,
    force_download: bool,
    trailing_digest: Option<String>,
    io_ring: IoRingHandle,
    p2p_publish_url: Option<String>,
}

#[derive(Debug, Clone)]
pub struct BkDownloadConfig {
    pub dir: PathBuf,
    pub digest: String,
    pub url: String,
    pub try_cnt: i32,
    pub max_mbps: i32,
    pub block_size: u32,
    pub concurrency: usize,
    pub max_inflight_blocks: usize,
    pub p2p_publish_url: Option<String>,
}

impl fmt::Debug for BkDownload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BkDownload")
            .field("remaining_attempts", &self.try_cnt)
            .field("file_size", &self.file_size)
            .field("block_size", &self.block_size)
            .field("concurrency", &self.concurrency)
            .finish_non_exhaustive()
    }
}

impl BkDownload {
    pub fn new(
        switch_file: Arc<SwitchFile>,
        src_file: Arc<dyn VirtualFile>,
        file_size: u64,
        cfg: BkDownloadConfig,
        io_ring: IoRingHandle,
    ) -> Self {
        let BkDownloadConfig {
            dir,
            digest,
            url,
            try_cnt,
            max_mbps,
            block_size,
            concurrency,
            max_inflight_blocks,
            p2p_publish_url,
        } = cfg;

        Self {
            dir,
            try_cnt: try_cnt.max(1) as u32,
            switch_file,
            src_file,
            file_size,
            digest,
            url,
            block_size: block_size.max(ALIGNMENT as u32),
            concurrency: concurrency.max(1),
            max_mbps,
            max_inflight_blocks: max_inflight_blocks.max(1),
            retry_attempts: 0,
            hedge_timeout: HEDGE_TIMEOUT,
            force_download: false,
            trailing_digest: None,
            io_ring,
            p2p_publish_url,
        }
    }

    #[cfg(test)]
    fn set_hedge_timeout(&mut self, timeout: Duration) {
        self.hedge_timeout = timeout;
    }

    fn commit_path(&self) -> PathBuf {
        self.dir.join(COMMIT_FILE_NAME)
    }

    fn tmp_path(&self) -> PathBuf {
        self.dir.join(DOWNLOAD_TMP_NAME)
    }

    async fn switch_to_local_file(&self) -> Result<()> {
        self.switch_file
            .set_switch_file(self.commit_path(), self.io_ring.clone())
            .await
    }

    /// Return whether the blob at `path` matches the expected layer digest.
    async fn checksum_matches(&self, path: &Path) -> Result<bool> {
        let path = path.to_path_buf();
        let checksum = tokio::task::spawn_blocking(move || sha256sum(path))
            .await
            .context("join sha256 worker failed")??;
        Ok(checksum == self.digest)
    }

    async fn download_done(&mut self) -> Result<bool> {
        let checksum = match self.trailing_digest.take() {
            Some(digest) => digest,
            None => {
                let old_name = self.tmp_path();
                tokio::task::spawn_blocking(move || sha256sum(old_name))
                    .await
                    .context("join sha256 worker failed")??
            }
        };
        if checksum != self.digest {
            self.force_download = true;
            tracing::warn!(
                error = "download checksum mismatch",
                "background download attempt requires retry"
            );
            return Ok(false);
        }
        tokio::fs::rename(self.tmp_path(), self.commit_path()).await?;
        Ok(true)
    }

    async fn validate_existing_commit(&mut self, commit_path: &Path) -> Result<bool> {
        if self.checksum_matches(commit_path).await? {
            return Ok(true);
        }
        self.force_download = true;
        tokio::fs::remove_file(commit_path).await.with_context(|| {
            format!("remove invalid downloaded commit {}", commit_path.display())
        })?;
        tracing::warn!(
            error = "existing commit checksum mismatch",
            "invalid existing commit removed; continuing with fresh download"
        );
        Ok(false)
    }

    async fn publish_completed_layer(&self) {
        let Some(publish_url) = self.p2p_publish_url.as_deref() else {
            return;
        };
        let request = PublishLayerRequest {
            path: self.commit_path(),
            digest: self.digest.clone(),
            size: self.file_size,
            source_url: Some(self.url.clone()),
        };
        match p2p_publish_client()
            .post(publish_url)
            .json(&request)
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => {
                tracing::debug!("published completed overlaybd layer to p2p");
            }
            Ok(response) => {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                tracing::warn!(
                    %status,
                    body,
                    "failed to publish completed overlaybd layer to p2p: unsuccessful status"
                );
            }
            Err(error) => {
                tracing::warn!(
                    %error,
                    "failed to publish completed overlaybd layer to p2p: request failed"
                );
            }
        }
    }

    async fn should_skip_block(&self, dst: &LocalFile, offset: u64, count: usize) -> Result<bool> {
        if self.force_download {
            return Ok(false);
        }
        match dst.seek_hole(offset).await {
            Ok(Some(hole_pos)) => Ok(hole_pos >= offset.saturating_add(count as u64)),
            Ok(None) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    async fn download_blob(&mut self, running: Arc<AtomicBool>) -> Result<bool> {
        tokio::fs::create_dir_all(&self.dir).await?;
        let dst = Arc::new(LocalFile::open_rw(self.tmp_path(), true, self.io_ring.clone()).await?);
        dst.truncate(self.file_size).await?;

        // Stage 1: scan block ranges sequentially and collect the pending
        // list, preserving SEEK_HOLE resume semantics without concurrent
        // hole scanning. Ranges found already present are seeded into the
        // trailing hasher's coverage so retried/partial layers hash correctly.
        let block_size = u64::from(self.block_size.max(ALIGNMENT as u32));
        let mut pending = Vec::new();
        let mut seed = CompletedBlocks::default();
        let mut offset = 0u64;
        while offset < self.file_size {
            if !running.load(Ordering::SeqCst) {
                return Ok(false);
            }
            let count = usize::try_from((self.file_size - offset).min(block_size))
                .context("download block too large")?;
            if !self.should_skip_block(&dst, offset, count).await? {
                pending.push((offset, count));
            } else {
                seed.insert(offset, offset + count as u64);
            }
            offset = offset.saturating_add(count as u64);
        }

        // Start the trailing hasher before dispatching block tasks: it streams
        // the tmp file in order up to the contiguous downloaded high-water,
        // overlapped with the download, so the digest is (nearly) complete
        // when the last block lands.
        let completed: SharedCompleted = Arc::new((Mutex::new(seed), tokio::sync::Notify::new()));
        let hash_task = tokio::task::spawn(hash_trailing(
            self.tmp_path(),
            self.file_size,
            completed.clone(),
            running.clone(),
        ));

        // Stage 2: spawn block tasks with bounded concurrency on the
        // multi-thread runtime so TLS/body handling parallelizes across cores.
        //
        // Dropping an in-flight task is unsafe: its scratch buffer (and the
        // destination fd) may still be referenced by a submitted io_uring
        // operation. So on the first failure OR cancellation we stop
        // dispatching new blocks but always drive already-spawned tasks to
        // natural completion via join_next before returning — never let the
        // JoinSet abort live tasks on drop. Both inner block errors and
        // JoinErrors (panics) are recorded and the drain continues.
        // The pacing clock starts when the download actually begins, not at
        // BkDownload construction: the readiness wait and the configured
        // delay would otherwise bank limit×wait bytes of burst credit.
        let throttle = (self.max_mbps > 0).then(|| {
            Arc::new(Mutex::new(BkThrottle {
                started: std::time::Instant::now(),
                downloaded: 0,
                limit_bps: self.max_mbps as u64 * 1024 * 1024,
            }))
        });
        let ctx = Arc::new(BlockTaskCtx {
            src_file: self.src_file.clone(),
            dst,
            throttle,
            running: running.clone(),
            hedge_timeout: self.hedge_timeout,
            max_inflight_blocks: self.max_inflight_blocks,
            completed: completed.clone(),
        });
        let mut blocks = pending.into_iter();
        let mut tasks = JoinSet::new();
        let mut first_error: Option<anyhow::Error> = None;
        loop {
            while first_error.is_none() && tasks.len() < self.concurrency {
                let Some((offset, count)) = blocks.next() else {
                    break;
                };
                if !running.load(Ordering::SeqCst) {
                    first_error = Some(anyhow::anyhow!(
                        "image file exited when background downloading"
                    ));
                    break;
                }
                let ctx = ctx.clone();
                tasks.spawn(async move {
                    let result = ctx.download_block(offset, count).await;
                    // Throttle after releasing the slot so pacing sleeps never
                    // hold an in-flight I/O slot hostage.
                    if let Ok(read) = &result {
                        ctx.throttle_after_block(*read as u64).await;
                    }
                    result
                });
            }
            match tasks.join_next().await {
                Some(Ok(Ok(_))) => {}
                Some(Ok(Err(error))) => {
                    first_error.get_or_insert(error);
                }
                Some(Err(join_error)) => {
                    // A panicked block task must not short-circuit the drain:
                    // record it and keep joining the remaining tasks.
                    first_error.get_or_insert(anyhow::anyhow!(
                        "background download block task failed: {join_error}"
                    ));
                }
                None => break,
            }
        }
        if let Some(error) = first_error {
            // The trailing hasher is a pure reader; aborting it mid-read is
            // always safe (unlike the io_uring block tasks).
            hash_task.abort();
            return Err(error);
        }

        let blocks_done_at = std::time::Instant::now();
        self.trailing_digest = Some(hash_task.await.context("join trailing hasher failed")??);
        tracing::info!(
            dir = %self.dir.display(),
            hash_wait_secs = format_args!("{:.1}", blocks_done_at.elapsed().as_secs_f64()),
            "background download blocks drained and hashed"
        );
        Ok(true)
    }
}

/// Shared context for one spawned block-download task. Everything the task
/// needs is owned (Arc/clone), so tasks are `Send + 'static` and can run on
/// any worker of the multi-thread runtime.
struct BlockTaskCtx {
    src_file: Arc<dyn VirtualFile>,
    dst: Arc<LocalFile>,
    throttle: Option<Arc<Mutex<BkThrottle>>>,
    running: Arc<AtomicBool>,
    hedge_timeout: Duration,
    max_inflight_blocks: usize,
    completed: SharedCompleted,
}

impl BlockTaskCtx {
    /// Fetch one block from the remote source and persist it at its positional
    /// offset in `dst`. Returns the number of bytes persisted.
    async fn download_block(&self, offset: u64, count: usize) -> Result<usize> {
        // Scratch is allocated once per block and reused across hedge
        // attempts: only the bytes a successful attempt reports as read are
        // ever written out, so stale content from a failed attempt is
        // unreachable.
        let mut scratch = vec![0u8; count];
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            match self.download_block_attempt(offset, &mut scratch).await {
                Ok(read) => return Ok(read),
                Err(error) => {
                    let timed_out = error.is::<tokio::time::error::Elapsed>();
                    if !timed_out
                        || attempt >= HEDGE_MAX_ATTEMPTS
                        || !self.running.load(Ordering::SeqCst)
                    {
                        return Err(error);
                    }
                    // A straggler block is almost always one throttled
                    // connection; reissuing lands on a different pooled
                    // connection. Back off briefly (with a per-block spread)
                    // before the next attempt.
                    let backoff = Duration::from_millis(500 * u64::from(attempt) + offset % 500);
                    tracing::warn!(
                        offset,
                        attempt,
                        "block read timed out after {:?}; reissuing on a fresh request",
                        self.hedge_timeout
                    );
                    tokio::time::sleep(backoff).await;
                }
            }
        }
    }

    /// One attempt at a block. The read phase is wrapped in a timeout: at
    /// this point the scratch is task-owned and no io_uring write has been
    /// submitted, so dropping a slow HTTP read is memory-safe (the gate and
    /// slot permits are RAII and released on drop). The source is also
    /// structurally an HTTP file — `prepare_bk_download` only wires sources
    /// opened via `open_source_blob` (registryfs/oss) — so an io_uring-backed
    /// local source can never reach the timeout-drop branch.
    async fn download_block_attempt(&self, offset: u64, scratch: &mut [u8]) -> Result<usize> {
        // Gate first, before taking a global slot or allocating scratch:
        // park while foreground reads are in flight (never holding the slot).
        // The backoff loop also watches `running`, so a parked task never
        // delays the drain path by more than one backoff interval.
        let Some(_gate_permit) = crate::download_gate::gate_block_read(&self.running).await else {
            bail!("image file exited when background downloading");
        };
        // Acquire the process-wide slot inside the task, before allocating
        // scratch. The dispatch loop must never block on the semaphore: with
        // self.concurrency greater than the permit count, acquiring in the
        // dispatch loop would suspend download_blob before it ever polls the
        // in-flight tasks, deadlocking both.
        let _slot = global_bk_block_slots(self.max_inflight_blocks)
            .acquire()
            .await
            .expect("global background-download slot semaphore closed");
        let read = match tokio::time::timeout(
            self.hedge_timeout,
            self.read_block_with_retry(offset, scratch),
        )
        .await
        {
            Ok(result) => result?,
            Err(elapsed) => return Err(elapsed.into()),
        };
        self.write_block_with_retry(self.dst.as_ref(), offset, &scratch[..read])
            .await?;
        // Register the block only after its write fully landed — the trailing
        // hasher must never see a half-written extent.
        record_block_done(&self.completed, offset, read as u64);
        Ok(read)
    }

    async fn read_block_with_retry(&self, offset: u64, dst: &mut [u8]) -> Result<usize> {
        let mut attempts = 3u8;
        loop {
            if !self.running.load(Ordering::SeqCst) {
                bail!("image file exited when background downloading");
            }
            match self.src_file.read_at_into(offset, dst).await {
                Ok(read) if read == dst.len() => {
                    return Ok(read);
                }
                Ok(read) => {
                    attempts = attempts.saturating_sub(1);
                    if attempts == 0 {
                        bail!(
                            "short read at offset {offset}: expected {count}, got {read}",
                            count = dst.len()
                        );
                    }
                }
                Err(err) => {
                    // Hard errors bubble straight to the layer-level retry
                    // instead of multiplying requests through nested retry loop.
                    return Err(err);
                }
            }
            if !self.running.load(Ordering::SeqCst) {
                bail!("image file exited when background downloading");
            }
        }
    }

    async fn write_block_with_retry(
        &self,
        dst: &dyn VirtualFile,
        offset: u64,
        data: &[u8],
    ) -> Result<()> {
        let mut attempts = 3u8;
        loop {
            match dst.write_at(offset, data).await {
                Ok(written) if written >= data.len() => return Ok(()),
                Ok(written) => {
                    attempts = attempts.saturating_sub(1);
                    if attempts == 0 {
                        bail!(
                            "short write at offset {offset}: expected {}, got {written}",
                            data.len()
                        );
                    }
                }
                Err(err) => {
                    attempts = attempts.saturating_sub(1);
                    if attempts == 0 {
                        return Err(err);
                    }
                }
            }
        }
    }

    /// Rate-limit the whole layer download by pacing after each completed
    /// block. The limiter is shared by all concurrent block tasks so the
    /// aggregate rate honors max_mbps regardless of the concurrency knob.
    async fn throttle_after_block(&self, bytes: u64) {
        let Some(throttle) = &self.throttle else {
            return;
        };
        let sleep_for = {
            let mut guard = throttle.lock();
            guard.downloaded += bytes;
            throttle_sleep_for(guard.started, guard.downloaded, guard.limit_bps)
        };
        if !sleep_for.is_zero() {
            tokio::time::sleep(sleep_for).await;
        }
    }
}

/// Ranges of the tmp download file that are fully written. Seeded with the
/// ranges the SEEK_HOLE resume scan found already present (so retried and
/// partially-downloaded layers hash correctly), then extended as block
/// writes complete. Shared with the trailing hasher.
#[derive(Default)]
struct CompletedBlocks {
    ranges: BTreeMap<u64, u64>,
}

type SharedCompleted = Arc<(Mutex<CompletedBlocks>, tokio::sync::Notify)>;

impl CompletedBlocks {
    fn insert(&mut self, start: u64, end: u64) {
        let mut start = start;
        let mut end = end;
        if let Some((&next_start, &next_end)) = self.ranges.range(start..).next() {
            if next_start <= end {
                end = end.max(next_end);
                self.ranges.remove(&next_start);
            }
        }
        if let Some((&prev_start, &prev_end)) = self.ranges.range(..=start).next_back() {
            if prev_end >= start {
                start = prev_start;
                end = end.max(prev_end);
                self.ranges.remove(&prev_start);
            }
        }
        self.ranges.insert(start, end);
    }

    /// Length of the contiguous completed coverage starting at `pos`.
    fn covered_from(&self, pos: u64) -> u64 {
        if let Some((&start, &end)) = self.ranges.range(..=pos).next_back() {
            if start <= pos && end > pos {
                return end - pos;
            }
        }
        0
    }
}

fn record_block_done(completed: &SharedCompleted, offset: u64, len: u64) {
    completed.0.lock().insert(offset, offset + len);
    completed.1.notify_waiters();
}

const HASH_TRAIL_CHUNK: usize = 4 * 1024 * 1024;
const HASH_TRAIL_POLL: Duration = Duration::from_millis(20);

/// Stream the tmp file from offset 0, but only up to the currently contiguous
/// downloaded high-water mark, hashing as it goes. Runs overlapped with the
/// whole download: it reads warm pages that were just written, so by the time
/// the last block lands the digest is already (nearly) complete. A pure
/// reader, so cancelling it is always safe.
async fn hash_trailing(
    tmp_path: PathBuf,
    file_size: u64,
    completed: SharedCompleted,
    running: Arc<AtomicBool>,
) -> Result<String> {
    let mut context = ring::digest::Context::new(&ring::digest::SHA256);
    let mut pos = 0u64;
    let file = Arc::new(StdFile::open(&tmp_path)?);
    while pos < file_size {
        if !running.load(Ordering::SeqCst) {
            bail!("image file exited when background downloading");
        }
        let covered = completed.0.lock().covered_from(pos);
        if covered == 0 {
            let notified = completed.1.notified();
            tokio::select! {
                _ = notified => {}
                _ = tokio::time::sleep(HASH_TRAIL_POLL) => {}
            }
            continue;
        }
        let want = covered.min(HASH_TRAIL_CHUNK as u64) as usize;
        let buffer = vec![0u8; want];
        let file2 = file.clone();
        let (buffer, read_result) = tokio::task::spawn_blocking(move || {
            let mut buffer = buffer;
            let read = std::os::unix::fs::FileExt::read_at(file2.as_ref(), &mut buffer, pos);
            (buffer, read)
        })
        .await
        .context("join trailing hasher reader failed")?;
        let read = read_result?;
        if read == 0 {
            tokio::time::sleep(HASH_TRAIL_POLL).await;
            continue;
        }
        context.update(&buffer[..read]);
        pos += read as u64;
    }
    Ok(format!("sha256:{}", encode_hex(context.finish().as_ref())))
}

impl BkDownload {
    pub async fn download(&mut self, running: Arc<AtomicBool>) -> Result<bool> {
        if self.try_cnt == 0 {
            return Ok(false);
        }
        self.try_cnt -= 1;
        self.download_inner(running).await
    }

    async fn download_inner(&mut self, running: Arc<AtomicBool>) -> Result<bool> {
        if !running.load(Ordering::SeqCst) {
            return Ok(false);
        }
        let commit_path = self.commit_path();
        match tokio::fs::metadata(&commit_path).await {
            Ok(metadata) if metadata.is_file() && metadata.len() == self.file_size => {
                if self.validate_existing_commit(&commit_path).await? {
                    self.switch_to_local_file().await?;
                    self.publish_completed_layer().await;
                    return Ok(true);
                }
                // A checksum mismatch removes the stale commit and continues the
                // same outer attempt with a fresh download.
            }
            Ok(metadata) if !metadata.is_file() => {
                bail!(
                    "download target {} exists but is not a regular file",
                    commit_path.display()
                );
            }
            Ok(_) => {
                tokio::fs::remove_file(&commit_path)
                    .await
                    .with_context(|| {
                        format!("remove stale downloaded commit {}", commit_path.display())
                    })?;
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("read downloaded commit metadata {}", commit_path.display())
                })
            }
        }

        if !self.download_blob(running.clone()).await? {
            return Ok(false);
        }
        if !self.download_done().await? {
            return Ok(false);
        }
        self.switch_to_local_file().await?;
        self.publish_completed_layer().await;
        Ok(true)
    }
}

#[derive(Debug, Serialize)]
struct PublishLayerRequest {
    path: PathBuf,
    digest: String,
    size: u64,
    source_url: Option<String>,
}

pub struct BackgroundDownloadThread {
    running: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl fmt::Debug for BackgroundDownloadThread {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BackgroundDownloadThread")
            .field("running", &self.running.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl BackgroundDownloadThread {
    pub fn start(
        downloads: Vec<BkDownload>,
        cfg: &DownloadConfig,
        device_key: Option<PathBuf>,
    ) -> Result<Option<Self>> {
        if downloads.is_empty() {
            return Ok(None);
        }

        let running = Arc::new(AtomicBool::new(true));
        let worker_running = running.clone();
        let mut queue: VecDeque<BkDownload> = downloads.into();
        let delay_secs = background_download_delay(cfg);
        let device_key = device_key.map(|path| path.to_string_lossy().into_owned());
        let handle = thread::Builder::new()
            .name("overlaybd-bk-download".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(4)
                    .enable_all()
                    .build()
                    .expect("build background download runtime");
                runtime.block_on(async move {
                    // Hold sandbox-bound downloads until envd is ready (or the
                    // fallback elapses). `delay` then applies as a post-ready
                    // delay. Non-sandbox consumers (device_key = None) skip the
                    // wait entirely.
                    if let Some(device_key) = device_key {
                        crate::download_gate::wait_sandbox_ready(
                            &device_key,
                            crate::download_gate::SANDBOX_READY_FALLBACK,
                            &worker_running,
                        )
                        .await;
                    }
                    bk_download_proc(&mut queue, delay_secs, worker_running).await;
                });
            })?;

        Ok(Some(Self {
            running,
            handle: Some(handle),
        }))
    }

    pub fn stop(&mut self) {
        self.running.store(false, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for BackgroundDownloadThread {
    fn drop(&mut self) {
        self.stop();
    }
}

fn download_error_category(error: &anyhow::Error) -> &'static str {
    for cause in error.chain() {
        if let Some(io_error) = cause.downcast_ref::<std::io::Error>() {
            return match io_error.kind() {
                std::io::ErrorKind::NotFound => "io_not_found",
                std::io::ErrorKind::PermissionDenied => "io_permission_denied",
                _ => "io_error",
            };
        }
        if cause.is::<tokio::task::JoinError>() {
            return "worker_join_error";
        }
    }
    "download_error"
}

fn background_download_delay(cfg: &DownloadConfig) -> u64 {
    let base = cfg.delay.max(0) as u64;
    // Random extra delay is drawn from [0, delay_extra). Values <= 1 (including
    // zero and negatives) always yield zero jitter — there is no hidden
    // fallback jitter, matching the documented `[0, delay_extra)` semantics.
    let extra = cfg.delay_extra.max(1) as u64;
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as u64;
    base + (nanos % extra)
}

/// Exponential backoff with jitter between layer-level download attempts:
/// attempt n waits 2^(n-1) seconds plus up to one second of jitter, capped at
/// 30s. Layer retries are the single place that re-paces hard failures.
/// Jitter is seeded from the layer digest and attempt so simultaneous
/// failures spread across layers (and processes) instead of syncing on the
/// shared wall clock.
fn retry_backoff(attempt: u32, seed: &str) -> Duration {
    let base = 1u64
        .checked_shl(attempt.saturating_sub(1))
        .unwrap_or(u64::MAX)
        .min(30);
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in seed.as_bytes().iter().chain(attempt.to_le_bytes().iter()) {
        h = (h ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3);
    }
    Duration::from_secs(base) + Duration::from_millis(h % 1000)
}

/// Sleep in POLL_INTERVAL slices so daemon shutdown stays responsive even
/// during long retry backoffs.
async fn interruptible_sleep(duration: Duration, running: &AtomicBool) {
    let deadline = tokio::time::Instant::now() + duration;
    loop {
        let now = tokio::time::Instant::now();
        if !running.load(Ordering::SeqCst) || now >= deadline {
            return;
        }
        tokio::time::sleep(POLL_INTERVAL.min(deadline - now)).await;
    }
}

async fn bk_download_proc(
    queue: &mut VecDeque<BkDownload>,
    delay_secs: u64,
    running: Arc<AtomicBool>,
) {
    interruptible_sleep(Duration::from_secs(delay_secs), &running).await;

    while running.load(Ordering::SeqCst) {
        let Some(mut item) = queue.pop_front() else {
            break;
        };

        tokio::time::sleep(POLL_INTERVAL).await;
        let Some(_guard) = DownloadDirLock::try_lock(&item.dir) else {
            queue.push_back(item);
            continue;
        };

        let (succeeded, error_category) = match item.download(running.clone()).await {
            Ok(true) => (true, "none"),
            Ok(false) => (false, "incomplete"),
            Err(error) => (false, download_error_category(&error)),
        };
        if !running.load(Ordering::SeqCst) {
            break;
        }
        if succeeded {
            continue;
        }

        let remaining_attempts = item.try_cnt;
        if remaining_attempts > 0 {
            item.retry_attempts += 1;
            let backoff = retry_backoff(item.retry_attempts, &item.digest);
            tracing::warn!(
                remaining_attempts,
                backoff_secs = backoff.as_secs(),
                error_category,
                "background download will retry"
            );
            interruptible_sleep(backoff, &running).await;
            queue.push_back(item);
        } else {
            tracing::warn!(
                remaining_attempts,
                error_category,
                "background download terminally failed"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::local::LocalFile;
    use crate::backend::switch::new_switch_file;
    use crate::backend::tar::{new_tar_file_adaptor, new_tar_file_create};
    use crate::test_utils::test_io_ring;
    use std::sync::atomic::AtomicUsize;
    use tempfile::tempdir;

    async fn write_all_at(file: &dyn VirtualFile, data: &[u8], mut offset: u64) -> Result<()> {
        let mut remain = data;
        while !remain.is_empty() {
            let written = file.write_at(offset, remain).await?;
            if written == 0 {
                bail!("failed to advance write");
            }
            offset = offset.saturating_add(written as u64);
            remain = &remain[written..];
        }
        Ok(())
    }

    fn digest_of(data: &[u8]) -> String {
        let mut context = ring::digest::Context::new(&ring::digest::SHA256);
        context.update(data);
        format!("sha256:{}", encode_hex(context.finish().as_ref()))
    }

    async fn create_plain_tar_file(path: &Path, data: &[u8]) -> Result<()> {
        let dst: Arc<dyn VirtualFile> = Arc::new(LocalFile::new(path, test_io_ring()).await?);
        let tar = new_tar_file_create(dst).await?;
        write_all_at(tar.as_ref(), data, 0).await?;
        tar.close().await
    }

    /// Minimal remote source stub. Serves zeroed reads so switch-file type
    /// probing passes; the only consumer fails before any block I/O, so
    /// content never matters.
    struct StubFile;

    #[async_trait::async_trait]
    impl VirtualFile for StubFile {
        async fn read_at(&self, _offset: u64, len: usize) -> Result<bytes::Bytes> {
            Ok(bytes::Bytes::from(vec![0u8; len]))
        }

        async fn read_at_into(&self, _offset: u64, dst: &mut [u8]) -> Result<usize> {
            dst.fill(0);
            Ok(dst.len())
        }

        async fn write_at(&self, _offset: u64, _data: &[u8]) -> Result<usize> {
            bail!("write_at is not used by this test")
        }

        async fn size(&self) -> Result<u64> {
            Ok(4096)
        }
    }

    struct TarLayerDownload {
        bk: BkDownload,
        switch: Arc<SwitchFile>,
        blob_bytes: Vec<u8>,
        payload: Vec<u8>,
    }

    /// Build a real tar layer source plus a BkDownload pointed at `layer_dir`.
    async fn setup_tar_layer_download(
        root: &Path,
        layer_dir: PathBuf,
        concurrency: usize,
    ) -> TarLayerDownload {
        let source_path = root.join("remote.blob");
        let payload: Vec<u8> = (0..8192).map(|idx| (idx % 251) as u8).collect();
        create_plain_tar_file(&source_path, &payload)
            .await
            .expect("create remote tar");

        let blob_bytes = std::fs::read(&source_path).expect("read blob bytes");
        let digest = digest_of(&blob_bytes);

        let ring = test_io_ring();
        let runtime_source: Arc<dyn VirtualFile> = Arc::new(
            LocalFile::open_ro(&source_path, ring.clone())
                .await
                .expect("open runtime source"),
        );
        let runtime_tar = new_tar_file_adaptor(runtime_source)
            .await
            .expect("open runtime tar");
        let switch = new_switch_file(runtime_tar, false, Some("http://registry/blob"))
            .await
            .expect("new switch file");

        let download_source: Arc<dyn VirtualFile> = Arc::new(
            LocalFile::open_ro(&source_path, ring.clone())
                .await
                .expect("open source for download"),
        );
        let bk = BkDownload::new(
            switch.clone(),
            download_source,
            blob_bytes.len() as u64,
            BkDownloadConfig {
                dir: layer_dir,
                digest,
                url: "http://registry/blob".to_string(),
                try_cnt: 1,
                max_mbps: 0,
                block_size: 4096,
                concurrency,
                max_inflight_blocks: 16,
                p2p_publish_url: None,
            },
            ring,
        );
        TarLayerDownload {
            bk,
            switch,
            blob_bytes,
            payload,
        }
    }

    #[tokio::test]
    async fn test_bk_download_downloads_and_switches_to_local_commit() {
        let temp = tempdir().expect("tempdir");
        let layer_dir = temp.path().join("layer");
        let TarLayerDownload {
            mut bk,
            switch,
            blob_bytes,
            payload,
        } = setup_tar_layer_download(temp.path(), layer_dir.clone(), 2).await;
        // A corrupt pre-existing commit must be detected and re-downloaded.
        std::fs::create_dir_all(&layer_dir).expect("create layer directory");
        std::fs::write(
            layer_dir.join(COMMIT_FILE_NAME),
            vec![0u8; blob_bytes.len()],
        )
        .expect("write corrupt existing commit");

        let running = Arc::new(AtomicBool::new(true));
        assert!(bk.download(running.clone()).await.expect("run download"));
        assert!(layer_dir.join(COMMIT_FILE_NAME).exists());
        assert_eq!(
            switch.filepath().await.expect("filepath"),
            layer_dir.join(COMMIT_FILE_NAME).to_string_lossy().as_ref()
        );
        assert_eq!(
            switch
                .read_at(0, payload.len())
                .await
                .expect("read switched payload"),
            bytes::Bytes::from(payload)
        );
    }

    #[tokio::test]
    async fn test_bk_download_limits_metadata_errors_to_try_count_and_terminal_failure() {
        let temp = tempdir().expect("tempdir");
        let source: Arc<dyn VirtualFile> = Arc::new(StubFile);
        let switch = new_switch_file(source.clone(), false, Some("https://registry/blob"))
            .await
            .expect("new switch file");
        let mut bk = BkDownload::new(
            switch,
            source,
            4096,
            BkDownloadConfig {
                dir: temp.path().join("layer"),
                digest: "sha256:test".to_string(),
                url: "https://registry/blob".to_string(),
                try_cnt: 2,
                max_mbps: 0,
                block_size: 4096,
                concurrency: 1,
                max_inflight_blocks: 16,
                p2p_publish_url: None,
            },
            test_io_ring(),
        );
        tokio::fs::create_dir_all(bk.commit_path())
            .await
            .expect("create invalid commit target");
        let running = Arc::new(AtomicBool::new(true));
        assert!(bk.download(running.clone()).await.is_err());
        assert!(bk.download(running.clone()).await.is_err());
        assert_eq!(bk.try_cnt, 0);
        assert!(!bk
            .download(running.clone())
            .await
            .expect("no third attempt"));
    }

    #[test]
    fn test_download_error_category_is_safe() {
        let sensitive = anyhow::anyhow!(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "https://user:pass@example/blob sha256:secret"
        ));
        assert_eq!(download_error_category(&sensitive), "io_permission_denied");
        assert!(!download_error_category(&sensitive).contains("https"));
        assert!(!download_error_category(&sensitive).contains("secret"));
    }

    struct TrackingReadSource {
        delay: Duration,
        fail_offsets: HashSet<u64>,
        panic_offsets: HashSet<u64>,
        slow_once_offsets: Mutex<HashSet<u64>>,
        slow_once_delay: Duration,
        slow_always_offsets: HashSet<u64>,
        slow_always_delay: Duration,
        flip_running: Option<Arc<AtomicBool>>,
        inflight: AtomicUsize,
        max_inflight: AtomicUsize,
        read_offsets: Mutex<Vec<u64>>,
    }

    impl TrackingReadSource {
        fn new(delay: Duration) -> Self {
            Self {
                delay,
                fail_offsets: HashSet::new(),
                panic_offsets: HashSet::new(),
                slow_once_offsets: Mutex::new(HashSet::new()),
                slow_once_delay: Duration::ZERO,
                slow_always_offsets: HashSet::new(),
                slow_always_delay: Duration::ZERO,
                flip_running: None,
                inflight: AtomicUsize::new(0),
                max_inflight: AtomicUsize::new(0),
                read_offsets: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl VirtualFile for TrackingReadSource {
        async fn read_at(&self, _offset: u64, _len: usize) -> Result<bytes::Bytes> {
            bail!("read_at is not used by this test")
        }

        async fn read_at_into(&self, offset: u64, dst: &mut [u8]) -> Result<usize> {
            self.read_offsets.lock().push(offset);
            if self.panic_offsets.contains(&offset) {
                panic!("scripted panic at offset {offset}");
            }
            if self.fail_offsets.contains(&offset) {
                bail!("scripted read error at offset {offset}");
            }
            if self.slow_always_offsets.contains(&offset) {
                tokio::time::sleep(self.slow_always_delay).await;
            }
            if self.slow_once_offsets.lock().remove(&offset) {
                tokio::time::sleep(self.slow_once_delay).await;
            }
            if let Some(running) = &self.flip_running {
                running.store(false, Ordering::SeqCst);
            }
            let inflight = self.inflight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_inflight.fetch_max(inflight, Ordering::SeqCst);
            tokio::time::sleep(self.delay).await;
            // Content is a pure function of the offset so partially
            // prepopulated files and downloaded blocks agree byte for byte.
            dst.fill((offset / 4096) as u8);
            self.inflight.fetch_sub(1, Ordering::SeqCst);
            Ok(dst.len())
        }

        async fn write_at(&self, _offset: u64, _data: &[u8]) -> Result<usize> {
            bail!("write_at is not used by this test")
        }

        async fn size(&self) -> Result<u64> {
            Ok(8 * 4096)
        }
    }

    async fn tracking_download(
        root: &Path,
        src_file: Arc<dyn VirtualFile>,
        file_size: u64,
        try_cnt: i32,
        block_size: u32,
        concurrency: usize,
    ) -> (BkDownload, Arc<SwitchFile>) {
        let runtime_path = root.join("runtime.blob");
        create_plain_tar_file(&runtime_path, b"runtime")
            .await
            .expect("create runtime tar");
        let ring = test_io_ring();
        let runtime_source: Arc<dyn VirtualFile> = Arc::new(
            LocalFile::open_ro(&runtime_path, ring.clone())
                .await
                .expect("open runtime source"),
        );
        let switch = new_switch_file(runtime_source, false, Some("https://registry/blob"))
            .await
            .expect("new switch file");
        let bk = BkDownload::new(
            switch.clone(),
            src_file,
            file_size,
            BkDownloadConfig {
                dir: root.join("layer"),
                digest: "sha256:test".to_string(),
                url: "https://registry/blob".to_string(),
                try_cnt,
                max_mbps: 0,
                block_size,
                concurrency,
                max_inflight_blocks: 16,
                p2p_publish_url: None,
            },
            ring,
        );
        (bk, switch)
    }

    #[tokio::test]
    async fn test_bk_download_downloads_blocks_concurrently_and_preserves_content() {
        let temp = tempdir().expect("tempdir");
        let source = Arc::new(TrackingReadSource::new(Duration::from_millis(100)));
        let (mut bk, _switch) =
            tracking_download(temp.path(), source.clone(), 8 * 4096, 1, 4096, 4).await;
        let running = Arc::new(AtomicBool::new(true));
        assert!(bk
            .download_blob(running.clone())
            .await
            .expect("download blocks"));
        let max_inflight = source.max_inflight.load(Ordering::SeqCst);
        assert!(
            max_inflight >= 4,
            "expected 4 concurrent block reads, observed {max_inflight}"
        );
        let expected: Vec<u8> = (0..8 * 4096u64).map(|pos| (pos / 4096) as u8).collect();
        assert_eq!(
            std::fs::read(bk.tmp_path()).expect("read downloaded tmp file"),
            expected
        );
    }

    #[tokio::test]
    async fn test_bk_download_failed_block_never_switches_to_local_commit() {
        let temp = tempdir().expect("tempdir");
        let mut source = TrackingReadSource::new(Duration::from_millis(1));
        source.fail_offsets.insert(4096);
        let (mut bk, switch) =
            tracking_download(temp.path(), Arc::new(source), 2 * 4096, 1, 4096, 2).await;
        let running = Arc::new(AtomicBool::new(true));
        assert!(bk.download(running.clone()).await.is_err());
        assert!(!bk.commit_path().exists());
        let current = switch.filepath().await.expect("switch filepath");
        let commit = bk.commit_path().to_string_lossy().into_owned();
        assert_ne!(
            current, commit,
            "foreground source must not switch to local commit on block failure"
        );
    }

    #[tokio::test]
    async fn test_bk_download_cancellation_stops_new_block_work() {
        let temp = tempdir().expect("tempdir");
        let running = Arc::new(AtomicBool::new(true));
        let mut source = TrackingReadSource::new(Duration::from_millis(10));
        source.flip_running = Some(running.clone());
        let source = Arc::new(source);
        let (mut bk, _switch) =
            tracking_download(temp.path(), source.clone(), 4 * 4096, 1, 4096, 2).await;
        assert!(bk.download(running.clone()).await.is_err());
        let reads_started = source.read_offsets.lock().len();
        assert!(
            reads_started < 4,
            "cancellation must stop new block work, reads started: {reads_started}"
        );
        assert!(!bk.commit_path().exists());
    }

    #[tokio::test]
    async fn test_bk_download_failed_block_drains_started_blocks_before_returning_error() {
        let temp = tempdir().expect("tempdir");
        // The block at offset 0 fails immediately; the slow block at 4096 is
        // in flight at the same time and must always be driven to completion.
        let mut source = TrackingReadSource::new(Duration::from_millis(50));
        source.fail_offsets.insert(0);
        let source = Arc::new(source);
        let (mut bk, _switch) =
            tracking_download(temp.path(), source.clone(), 4 * 4096, 1, 4096, 2).await;
        let running = Arc::new(AtomicBool::new(true));
        bk.download_blob(running.clone())
            .await
            .expect_err("failing block aborts the blob download");
        // Dispatch stopped at the first failure: blocks at 8192/12288 never started.
        let mut offsets = source.read_offsets.lock().clone();
        offsets.sort_unstable();
        assert_eq!(offsets, vec![0, 4096]);
        // The drained slow block still landed its bytes — in-flight I/O is
        // never dropped mid-flight (io_uring may still reference its buffer).
        let tmp = std::fs::read(bk.tmp_path()).expect("read tmp file");
        assert_eq!(&tmp[4096..8192], &[1u8; 4096]);
    }

    #[test]
    fn throttle_sleep_for_paces_aggregate_bytes() {
        let started = std::time::Instant::now() - Duration::from_secs(1);
        // On pace: 8 bytes after 1s at 8 B/s.
        assert_eq!(throttle_sleep_for(started, 8, 8), Duration::ZERO);
        // Behind pace by ~1s: 16 bytes after 1s at 8 B/s.
        let sleep = throttle_sleep_for(started, 16, 8);
        assert!(sleep >= Duration::from_millis(900));
        assert!(sleep <= Duration::from_millis(1100));
        // Nothing downloaded: no sleep.
        assert_eq!(throttle_sleep_for(started, 0, 8), Duration::ZERO);
        // Shared pacing staggers near-simultaneous completions instead of
        // granting every concurrent task the same sleep: 32 bytes at 8 B/s is
        // ~3s behind pace, i.e. the aggregate rate converges to limit_bps
        // (block_size / staggered sleep), not N x limit.
        let sleep = throttle_sleep_for(started, 32, 8);
        assert!(sleep >= Duration::from_millis(2900));
        assert!(sleep <= Duration::from_millis(3100));
    }

    #[tokio::test]
    async fn test_bk_download_concurrency_beyond_global_slots_does_not_deadlock() {
        let temp = tempdir().expect("tempdir");
        // 32 blocks with per-image concurrency 24 while only 16 process-wide
        // slots exist: tasks must queue on the semaphore inside the inflight
        // set instead of suspending the dispatch loop into a deadlock.
        let source = Arc::new(TrackingReadSource::new(Duration::from_millis(1)));
        let (mut bk, _switch) =
            tracking_download(temp.path(), source.clone(), 32 * 4096, 1, 4096, 24).await;
        let running = Arc::new(AtomicBool::new(true));
        assert!(bk
            .download_blob(running.clone())
            .await
            .expect("download blocks"));
        assert!(
            source.max_inflight.load(Ordering::SeqCst) <= 16,
            "slot semaphore must cap complete I/O concurrency at 16"
        );
        let expected: Vec<u8> = (0..32 * 4096u64).map(|pos| (pos / 4096) as u8).collect();
        assert_eq!(
            std::fs::read(bk.tmp_path()).expect("read downloaded tmp file"),
            expected
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_bk_download_panicked_block_task_is_drained_not_aborted() {
        let temp = tempdir().expect("tempdir");
        // The block at offset 0 panics inside its spawned task (JoinError
        // path); the slow block at 4096 must still be driven to completion
        // and the panic must surface only after everything in flight finished
        // — JoinSet drop must never abort live tasks.
        let mut source = TrackingReadSource::new(Duration::from_millis(50));
        source.panic_offsets.insert(0);
        let source = Arc::new(source);
        let (mut bk, _switch) =
            tracking_download(temp.path(), source.clone(), 2 * 4096, 1, 4096, 2).await;
        let running = Arc::new(AtomicBool::new(true));
        let error = bk
            .download_blob(running.clone())
            .await
            .expect_err("panicked block aborts the blob download");
        assert!(
            format!("{error:#}").contains("block task failed"),
            "unexpected error: {error:#}"
        );
        let tmp = std::fs::read(bk.tmp_path()).expect("read tmp file");
        assert_eq!(&tmp[4096..8192], &[1u8; 4096]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_bk_download_hedge_reissues_straggler_block() {
        let temp = tempdir().expect("tempdir");
        // Block at offset 4096 is throttled on its first read (far beyond the
        // injected hedge timeout); the hedge must abandon that read and
        // reissue, completing the block on the retry.
        let mut source = TrackingReadSource::new(Duration::from_millis(1));
        source.slow_once_offsets.lock().insert(4096);
        source.slow_once_delay = Duration::from_secs(3);
        let source = Arc::new(source);
        let (mut bk, _switch) =
            tracking_download(temp.path(), source.clone(), 4 * 4096, 1, 4096, 2).await;
        bk.set_hedge_timeout(Duration::from_millis(50));
        let running = Arc::new(AtomicBool::new(true));
        let started = std::time::Instant::now();
        assert!(bk
            .download_blob(running.clone())
            .await
            .expect("hedged download succeeds"));
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_secs(2),
            "hedge must not wait out the straggler: {elapsed:?}"
        );
        let offsets = source.read_offsets.lock().clone();
        assert!(
            offsets.iter().filter(|&&o| o == 4096).count() >= 2,
            "straggler block must be re-read: {offsets:?}"
        );
        let expected: Vec<u8> = (0..4 * 4096u64).map(|pos| (pos / 4096) as u8).collect();
        assert_eq!(
            std::fs::read(bk.tmp_path()).expect("read downloaded tmp file"),
            expected
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_bk_download_hedge_exhaustion_fails_but_drains() {
        let temp = tempdir().expect("tempdir");
        // Block at offset 0 times out on every attempt (always slower than
        // the hedge window). The blob must fail after HEDGE_MAX_ATTEMPTS
        // while the fast block at 4096 still drains to completion.
        let mut source = TrackingReadSource::new(Duration::from_millis(1));
        source.slow_always_offsets.insert(0);
        source.slow_always_delay = Duration::from_millis(100);
        let source = Arc::new(source);
        let (mut bk, _switch) =
            tracking_download(temp.path(), source.clone(), 2 * 4096, 1, 4096, 2).await;
        bk.set_hedge_timeout(Duration::from_millis(10));
        let running = Arc::new(AtomicBool::new(true));
        bk.download_blob(running.clone())
            .await
            .expect_err("exhausted hedge attempts must fail the blob");
        let tmp = std::fs::read(bk.tmp_path()).expect("read tmp file");
        assert_eq!(&tmp[4096..8192], &[1u8; 4096]);
        assert!(!bk.commit_path().exists());
    }

    #[test]
    fn sha256_ring_matches_known_vectors() {
        assert_eq!(
            digest_of(b""),
            "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            digest_of(b"abc"),
            "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        // Cross-check the file-path helper (ring) against a known large input
        // digest computed independently with the digest_of helper itself.
        let data: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
        let temp = tempfile::NamedTempFile::new().expect("tempfile");
        std::fs::write(temp.path(), &data).expect("write");
        assert_eq!(
            sha256sum(temp.path().to_path_buf()).expect("sha256sum"),
            digest_of(&data)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_bk_download_trailing_hash_covers_resume_seeded_blocks() {
        use std::os::unix::fs::FileExt;

        let temp = tempdir().expect("tempdir");
        // Prepopulate blocks 0 and 1 on disk (a previous attempt's residue);
        // the trailing hasher must hash them from disk even though no task
        // downloads them this time.
        let file_size = 4 * 4096u64;
        let layer_dir = temp.path().join("layer");
        std::fs::create_dir_all(&layer_dir).expect("create layer dir");
        let tmp_path = layer_dir.join(DOWNLOAD_TMP_NAME);
        let tmp = std::fs::File::create(&tmp_path).expect("create tmp download file");
        tmp.set_len(file_size).expect("size tmp download file");
        tmp.write_all_at(&[0u8; 4096], 0)
            .expect("prepopulate block 0");
        tmp.write_all_at(&[1u8; 4096], 4096)
            .expect("prepopulate block 1");
        drop(tmp);

        let source = Arc::new(TrackingReadSource::new(Duration::from_millis(5)));
        let (mut bk, _switch) =
            tracking_download(temp.path(), source.clone(), file_size, 1, 4096, 4).await;
        let running = Arc::new(AtomicBool::new(true));
        assert!(bk
            .download_blob(running.clone())
            .await
            .expect("download remaining blocks"));
        let digest = bk.trailing_digest.take().expect("trailing digest");
        let expected: Vec<u8> = (0..file_size).map(|pos| (pos / 4096) as u8).collect();
        assert_eq!(digest, digest_of(&expected));
        // Only the two missing blocks were downloaded; resume blocks were not
        // re-read, and the final file content is intact.
        let mut offsets = source.read_offsets.lock().clone();
        offsets.sort_unstable();
        assert_eq!(offsets, vec![8192, 12288]);
        assert_eq!(std::fs::read(&tmp_path).expect("read tmp file"), expected);
    }
}

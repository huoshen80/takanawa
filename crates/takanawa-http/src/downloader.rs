use std::collections::VecDeque;
use std::future::Future;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::StreamExt;
use reqwest::header::{
    ACCEPT_ENCODING, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_RANGE, ETAG, HeaderMap,
    LAST_MODIFIED, RANGE,
};
use reqwest::{Client, StatusCode};
use takanawa_core::{
    Chunk, ChunkPlan, DEFAULT_CHUNK_SIZE, HashConfig, PartFile, PartMetadata, RemoteInfo, Result,
    TakanawaError,
};
use tokio::runtime::Runtime;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;

use crate::content_range::{parse_content_range, parse_unsatisfied_total};
use crate::limiter::IoLimiter;
use crate::state::{
    DownloadSnapshot, DownloadSpeedSnapshot, ProgressCallback, SharedState, SpeedCallback,
};

const DEFAULT_MAX_RETRIES: u32 = 4;
const DEFAULT_BACKOFF_INITIAL: Duration = Duration::from_millis(100);
const DEFAULT_BACKOFF_MAX: Duration = Duration::from_secs(3);
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const WRITE_QUEUE_DEPTH_PER_CHUNK: usize = 8;

/// Configuration for a resumable HTTP range download.
#[derive(Debug, Clone)]
pub struct DownloadConfig {
    /// Source URL to download.
    pub url: String,
    /// Final output path.
    pub target_path: PathBuf,
    /// Requested chunk size in bytes. `0` selects the default chunk size.
    pub chunk_size: u64,
    /// Requested chunk parallelism. `0` lets the engine choose a default.
    pub parallelism: usize,
    /// Maximum chunks to download at the same time. `0` falls back to `parallelism`.
    pub max_parallel_chunks: usize,
    /// Retry policy for probe and chunk requests.
    pub retry: RetryConfig,
    /// Request timeout policy.
    pub timeout: TimeoutConfig,
    /// Aggregate response-body bandwidth limit in bytes per second. `0` disables limiting.
    pub bytes_per_second_limit: u64,
    /// Optional final-file hash verification.
    pub hash: HashConfig,
}

impl DownloadConfig {
    #[must_use]
    /// Returns a copy with zero-valued defaults filled in.
    pub fn normalized(mut self) -> Self {
        if self.chunk_size == 0 {
            self.chunk_size = DEFAULT_CHUNK_SIZE;
        }
        self.retry = self.retry.normalized();
        self.timeout = self.timeout.normalized();
        self
    }
}

/// Retry configuration for remote probe and chunk requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryConfig {
    /// Number of retries after the initial attempt.
    pub max_retries: u32,
    /// Initial exponential-backoff delay. `0` selects the default.
    pub backoff_initial: Duration,
    /// Maximum exponential-backoff delay. `0` selects the default.
    pub backoff_max: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: DEFAULT_MAX_RETRIES,
            backoff_initial: DEFAULT_BACKOFF_INITIAL,
            backoff_max: DEFAULT_BACKOFF_MAX,
        }
    }
}

impl RetryConfig {
    #[must_use]
    /// Returns a retry config with zero-valued backoff durations filled in.
    pub fn normalized(self) -> Self {
        let default = Self::default();
        let backoff_initial = if self.backoff_initial.is_zero() {
            default.backoff_initial
        } else {
            self.backoff_initial
        };
        let backoff_max = if self.backoff_max.is_zero() {
            default.backoff_max
        } else {
            self.backoff_max.max(backoff_initial)
        };
        Self {
            max_retries: self.max_retries,
            backoff_initial,
            backoff_max,
        }
    }

    fn max_attempts(self) -> u32 {
        self.max_retries.saturating_add(1).max(1)
    }
}

/// Timeout configuration for HTTP requests.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TimeoutConfig {
    /// Connection timeout. `0` selects the default.
    pub connect: Duration,
    /// Per-read timeout. `0` disables this timeout.
    pub read: Duration,
    /// Total timeout for each probe or chunk attempt. `0` disables this timeout.
    pub total: Duration,
}

impl TimeoutConfig {
    #[must_use]
    /// Returns a timeout config with zero-valued defaults filled in.
    pub fn normalized(self) -> Self {
        Self {
            connect: if self.connect.is_zero() {
                DEFAULT_CONNECT_TIMEOUT
            } else {
                self.connect
            },
            read: self.read,
            total: self.total,
        }
    }
}

/// Cloneable HTTP download engine shared by download handles.
#[derive(Debug, Clone)]
pub struct DownloadEngine {
    client: Client,
    client_mode: ClientMode,
    limiter: IoLimiter,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClientMode {
    Internal,
    External,
}

impl DownloadEngine {
    /// Creates a download engine with the given maximum in-flight I/O count.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP client cannot be constructed.
    pub fn new(max_io: usize) -> Result<Self> {
        let client = build_client(TimeoutConfig::default().normalized())?;
        Ok(Self {
            client,
            client_mode: ClientMode::Internal,
            limiter: IoLimiter::new(max_io.max(1)),
        })
    }

    #[must_use]
    /// Creates a download engine backed by a caller-provided HTTP client.
    ///
    /// Per-download timeout settings do not rebuild or alter an injected client.
    /// Configure proxy, TLS, headers, and timeouts on the client before passing it
    /// to this method.
    pub fn with_client(client: Client, max_io: usize) -> Self {
        Self {
            client,
            client_mode: ClientMode::External,
            limiter: IoLimiter::new(max_io.max(1)),
        }
    }

    #[must_use]
    /// Returns the engine-wide maximum number of in-flight I/O operations.
    ///
    /// # Panics
    ///
    /// Panics if the shared limiter mutex is poisoned.
    pub fn max_io(&self) -> usize {
        self.limiter.max()
    }

    /// Updates the engine-wide maximum number of in-flight I/O operations.
    ///
    /// A `max_io` value of `0` is normalized to `1`.
    ///
    /// # Panics
    ///
    /// Panics if the shared limiter mutex is poisoned.
    pub fn set_max_io(&self, max_io: usize) {
        self.limiter.set_max(max_io);
    }

    fn default_parallelism(&self) -> usize {
        self.max_io().clamp(1, 4)
    }

    fn with_timeout(&self, timeout: TimeoutConfig) -> Result<Self> {
        if self.client_mode == ClientMode::External {
            return Ok(self.clone());
        }
        Ok(Self {
            client: build_client(timeout)?,
            client_mode: ClientMode::Internal,
            limiter: self.limiter.clone(),
        })
    }
}

fn client_builder() -> reqwest::ClientBuilder {
    let builder = Client::builder();
    #[cfg(feature = "tls-platform-native")]
    {
        return builder.tls_backend_native();
    }
    #[cfg(all(feature = "tls-rustls", not(feature = "tls-platform-native")))]
    {
        let roots = rustls::RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
        };
        let tls_config = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        return builder.tls_backend_preconfigured(tls_config);
    }
    #[allow(unreachable_code)]
    builder
}

fn build_client(timeout: TimeoutConfig) -> Result<Client> {
    let mut builder = client_builder()
        .user_agent("takanawa/0.1")
        .connect_timeout(timeout.connect);
    if !timeout.read.is_zero() {
        builder = builder.read_timeout(timeout.read);
    }
    builder
        .build()
        .map_err(|err| TakanawaError::InvalidConfig(format!("failed to build HTTP client: {err}")))
}

/// Stateful download handle that can be started, paused, cancelled, and observed.
#[derive(Debug)]
pub struct DownloadHandle {
    engine: DownloadEngine,
    config: DownloadConfig,
    state: SharedState,
    control: Arc<Control>,
    join: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

#[derive(Debug, Default)]
struct Control {
    pause: AtomicBool,
    cancel: AtomicBool,
}

impl DownloadHandle {
    #[must_use]
    /// Creates a download handle from an engine and config.
    pub fn new(engine: DownloadEngine, config: DownloadConfig) -> Self {
        Self {
            engine,
            config: config.normalized(),
            state: SharedState::new(),
            control: Arc::new(Control::default()),
            join: Mutex::new(None),
        }
    }

    /// Starts or resumes the download on an existing Tokio runtime.
    ///
    /// The work runs in a background task owned by this handle.
    ///
    /// # Errors
    ///
    /// Returns an error if a previous start is still running.
    ///
    /// # Panics
    ///
    /// Panics if the join-handle mutex is poisoned.
    pub fn start_on(&self, runtime: &Runtime) -> Result<()> {
        let mut join = self.join.lock().expect("download join mutex poisoned");
        if join.as_ref().is_some_and(|handle| !handle.is_finished()) {
            return Err(TakanawaError::AlreadyStarted);
        }
        if join
            .as_ref()
            .is_some_and(tokio::task::JoinHandle::is_finished)
        {
            let _ = join.take();
        }

        self.control.pause.store(false, Ordering::Relaxed);
        self.control.cancel.store(false, Ordering::Relaxed);
        self.state.clear_error();
        self.state.request_start();

        let engine = self.engine.clone();
        let config = self.config.clone();
        let state = self.state.clone();
        let control = Arc::clone(&self.control);
        *join = Some(runtime.spawn(async move {
            if let Err(err) = run_download(engine, config, state.clone(), control).await {
                match err {
                    TakanawaError::Cancelled => state.mark_cancelled(),
                    err => state.mark_failed(&err),
                }
            }
        }));
        Ok(())
    }

    /// Requests the download to pause after current in-flight work winds down.
    ///
    /// # Errors
    ///
    /// This method currently does not fail, but returns `Result` for API
    /// symmetry with other control operations.
    pub fn pause(&self) -> Result<()> {
        self.control.pause.store(true, Ordering::Relaxed);
        self.state.request_pause();
        Ok(())
    }

    /// Requests cancellation of the download.
    ///
    /// # Errors
    ///
    /// This method currently does not fail, but returns `Result` for API
    /// symmetry with other control operations.
    ///
    /// # Panics
    ///
    /// Panics if the join-handle mutex is poisoned.
    pub fn cancel(&self) -> Result<()> {
        self.control.cancel.store(true, Ordering::Relaxed);
        self.state.request_cancel();
        if self
            .join
            .lock()
            .expect("download join mutex poisoned")
            .as_ref()
            .is_none_or(tokio::task::JoinHandle::is_finished)
        {
            self.state.mark_cancelled();
        }
        Ok(())
    }

    /// Installs or removes a progress callback.
    pub fn set_progress_callback(&self, callback: Option<ProgressCallback>) {
        self.state.set_progress_callback(callback);
    }

    /// Installs or removes a response-body speed callback.
    pub fn set_speed_callback(&self, callback: Option<SpeedCallback>) {
        self.state.set_speed_callback(callback);
    }

    #[must_use]
    /// Returns the latest progress snapshot.
    ///
    /// # Panics
    ///
    /// Panics if the shared progress mutex is poisoned.
    pub fn snapshot(&self) -> DownloadSnapshot {
        self.state.snapshot()
    }

    #[must_use]
    /// Returns the latest response-body speed sample.
    ///
    /// # Panics
    ///
    /// Panics if the shared speed mutex is poisoned.
    pub fn speed_snapshot(&self) -> DownloadSpeedSnapshot {
        self.state.speed_snapshot()
    }

    #[must_use]
    /// Returns the current serialized completion bitmap.
    ///
    /// # Panics
    ///
    /// Panics if the shared progress mutex is poisoned.
    pub fn bitmap(&self) -> Vec<u8> {
        self.state.bitmap()
    }
}

impl Drop for DownloadHandle {
    fn drop(&mut self) {
        self.control.cancel.store(true, Ordering::Relaxed);
        if let Some(join) = self
            .join
            .lock()
            .expect("download join mutex poisoned")
            .take()
        {
            join.abort();
        }
    }
}

/// Downloads a resource to completion and returns its final progress snapshot.
///
/// # Errors
///
/// Returns an error if probing the remote resource, validating HTTP range
/// responses, writing/resuming the part file, finalizing the target, or hash
/// verification fails.
///
/// # Panics
///
/// Panics if shared progress state is poisoned while creating the final
/// snapshot.
pub async fn download_to_completion(
    engine: DownloadEngine,
    config: DownloadConfig,
) -> Result<DownloadSnapshot> {
    let state = SharedState::new();
    let control = Arc::new(Control::default());
    run_download(engine, config.normalized(), state.clone(), control).await?;
    Ok(state.snapshot())
}

#[allow(clippy::too_many_lines)]
async fn run_download(
    engine: DownloadEngine,
    config: DownloadConfig,
    state: SharedState,
    control: Arc<Control>,
) -> Result<()> {
    let config = config.normalized();
    let engine = engine.with_timeout(config.timeout)?;
    let bandwidth = Arc::new(BandwidthLimiter::new(config.bytes_per_second_limit));
    state.request_start();
    if complete_startup_control_request(&state, &control)? {
        return Ok(());
    }
    let remote = probe_with_retry(&engine, &config, &state, &control).await?;
    if complete_startup_control_request(&state, &control)? {
        return Ok(());
    }
    let chunk_plan = ChunkPlan::new(remote.content_len, config.chunk_size)?;
    let target_path = config.target_path.clone();
    let url = config.url.clone();
    let hash = config.hash;
    let chunk_size = config.chunk_size;

    state.mark_allocating();
    let part = tokio::task::spawn_blocking(move || {
        PartFile::open_or_create(&target_path, &url, &remote, chunk_size, hash)
    })
    .await
    .map_err(|err| TakanawaError::Ffi(format!("part open task failed: {err}")))??;
    state.update_from_metadata(part.metadata());
    if complete_startup_control_request(&state, &control)? {
        return Ok(());
    }
    state.mark_running();

    if part.metadata().all_complete() {
        finalize_part(part, &config, &state, &control).await?;
        return Ok(());
    }

    let mut pending: VecDeque<u64> = part.incomplete_chunks().into();
    let requested_parallelism = if config.max_parallel_chunks == 0 {
        config.parallelism
    } else {
        config.max_parallel_chunks
    };
    let parallelism = if requested_parallelism == 0 {
        engine.default_parallelism()
    } else {
        requested_parallelism.max(1)
    };
    let writer_capacity = parallelism
        .max(1)
        .saturating_mul(WRITE_QUEUE_DEPTH_PER_CHUNK);
    let (writer_tx, writer_join) = spawn_part_writer(part, writer_capacity);
    let mut tasks = JoinSet::new();

    loop {
        if control.cancel.load(Ordering::Relaxed) {
            tasks.shutdown().await;
            let _part = finish_part_writer(writer_tx, writer_join).await?;
            state.mark_cancelled();
            return Err(TakanawaError::Cancelled);
        }
        if control.pause.load(Ordering::Relaxed) {
            tasks.shutdown().await;
            let _part = finish_part_writer(writer_tx, writer_join).await?;
            state.mark_paused();
            return Ok(());
        }

        while !control.pause.load(Ordering::Relaxed)
            && !control.cancel.load(Ordering::Relaxed)
            && tasks.len() < parallelism
        {
            let Some(index) = pending.pop_front() else {
                break;
            };
            let chunk = chunk_plan.chunk(index)?;
            let engine = engine.clone();
            let config = config.clone();
            let state = state.clone();
            let control = Arc::clone(&control);
            let bandwidth = Arc::clone(&bandwidth);
            let writer_tx = writer_tx.clone();
            tasks.spawn(async move {
                let result = fetch_chunk_with_retry(
                    &engine, &config, chunk, &state, &control, &bandwidth, &writer_tx,
                )
                .await?;
                Ok::<_, TakanawaError>(result)
            });
        }

        if tasks.is_empty() {
            break;
        }

        let Some(result) = tasks.join_next().await else {
            break;
        };
        let task_result = match result {
            Ok(Ok(task_result)) => task_result,
            Ok(Err(err)) => {
                tasks.shutdown().await;
                let err = finish_part_writer_after_error(writer_tx, writer_join, err).await;
                return Err(err);
            }
            Err(err) => {
                tasks.shutdown().await;
                let err = finish_part_writer_after_error(
                    writer_tx,
                    writer_join,
                    TakanawaError::Ffi(format!("download task failed: {err}")),
                )
                .await;
                return Err(err);
            }
        };
        match task_result {
            ChunkTaskResult::Committed(metadata) => state.update_from_metadata(&metadata),
            ChunkTaskResult::Paused => {
                tasks.shutdown().await;
                let _part = finish_part_writer(writer_tx, writer_join).await?;
                state.mark_paused();
                return Ok(());
            }
        }

        if control.pause.load(Ordering::Relaxed) && tasks.is_empty() {
            let _part = finish_part_writer(writer_tx, writer_join).await?;
            state.mark_paused();
            return Ok(());
        }
    }

    let part = finish_part_writer(writer_tx, writer_join).await?;

    if control.pause.load(Ordering::Relaxed) {
        state.mark_paused();
        return Ok(());
    }

    finalize_part(part, &config, &state, &control).await
}

fn complete_startup_control_request(state: &SharedState, control: &Control) -> Result<bool> {
    if control.cancel.load(Ordering::Relaxed) {
        state.mark_cancelled();
        return Err(TakanawaError::Cancelled);
    }
    if control.pause.load(Ordering::Relaxed) {
        state.mark_paused();
        return Ok(true);
    }
    Ok(false)
}

enum ChunkTaskResult {
    Committed(Box<PartMetadata>),
    Paused,
}

enum FetchChunkStatus {
    Complete,
    Paused,
}

enum WriterCommand {
    Write {
        index: u64,
        chunk_offset: u64,
        bytes: Bytes,
    },
    Commit {
        index: u64,
        result: oneshot::Sender<Result<PartMetadata>>,
    },
}

fn spawn_part_writer(
    part: PartFile,
    capacity: usize,
) -> (
    mpsc::Sender<WriterCommand>,
    tokio::task::JoinHandle<Result<PartFile>>,
) {
    let (writer_tx, mut writer_rx) = mpsc::channel(capacity.max(1));
    let writer_join = tokio::task::spawn_blocking(move || {
        let mut part = part;
        while let Some(command) = writer_rx.blocking_recv() {
            match command {
                WriterCommand::Write {
                    index,
                    chunk_offset,
                    bytes,
                } => {
                    part.write_chunk_bytes(index, chunk_offset, &bytes)?;
                }
                WriterCommand::Commit { index, result } => {
                    let metadata = match part.commit_chunk(index) {
                        Ok(()) => part.metadata().clone(),
                        Err(err) => {
                            let message = err.to_string();
                            let _ = result.send(Err(err));
                            return Err(TakanawaError::Ffi(format!(
                                "part writer commit failed: {message}"
                            )));
                        }
                    };
                    let _ = result.send(Ok(metadata));
                }
            }
        }
        Ok(part)
    });
    (writer_tx, writer_join)
}

async fn finish_part_writer(
    writer_tx: mpsc::Sender<WriterCommand>,
    writer_join: tokio::task::JoinHandle<Result<PartFile>>,
) -> Result<PartFile> {
    drop(writer_tx);
    writer_join
        .await
        .map_err(|err| TakanawaError::Ffi(format!("part writer task failed: {err}")))?
}

async fn finish_part_writer_after_error(
    writer_tx: mpsc::Sender<WriterCommand>,
    writer_join: tokio::task::JoinHandle<Result<PartFile>>,
    err: TakanawaError,
) -> TakanawaError {
    match finish_part_writer(writer_tx, writer_join).await {
        Err(writer_err) if matches!(err, TakanawaError::Ffi(_)) => writer_err,
        Ok(_) | Err(TakanawaError::Ffi(_)) => err,
        Err(writer_err) => writer_err,
    }
}

async fn finalize_part(
    part: PartFile,
    config: &DownloadConfig,
    state: &SharedState,
    control: &Control,
) -> Result<()> {
    if control.cancel.load(Ordering::Relaxed) {
        state.mark_cancelled();
        return Err(TakanawaError::Cancelled);
    }
    if control.pause.load(Ordering::Relaxed) {
        state.mark_paused();
        return Ok(());
    }
    state.mark_verifying();
    let target_path = config.target_path.clone();
    tokio::task::spawn_blocking(move || part.finalize(&target_path))
        .await
        .map_err(|err| TakanawaError::Ffi(format!("finalize task failed: {err}")))??;
    state.mark_completed();
    Ok(())
}

async fn probe_with_retry(
    engine: &DownloadEngine,
    config: &DownloadConfig,
    state: &SharedState,
    control: &Control,
) -> Result<RemoteInfo> {
    let retry = config.retry.normalized();
    let mut delay = retry.backoff_initial;
    for attempt in 1..=retry.max_attempts() {
        if control.cancel.load(Ordering::Relaxed) {
            return Err(TakanawaError::Cancelled);
        }
        match with_total_timeout(config.timeout.total, probe_once(engine, &config.url, state)).await
        {
            Ok(remote) => return Ok(remote),
            Err(err) if err.is_retryable() && attempt < retry.max_attempts() => {
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(retry.backoff_max);
            }
            Err(err) => return Err(err),
        }
    }
    Err(TakanawaError::Network(
        "probe exhausted retry attempts".to_owned(),
    ))
}

async fn fetch_chunk_with_retry(
    engine: &DownloadEngine,
    config: &DownloadConfig,
    chunk: Chunk,
    state: &SharedState,
    control: &Control,
    bandwidth: &BandwidthLimiter,
    writer_tx: &mpsc::Sender<WriterCommand>,
) -> Result<ChunkTaskResult> {
    let retry = config.retry.normalized();
    let mut delay = retry.backoff_initial;
    for attempt in 1..=retry.max_attempts() {
        if control.cancel.load(Ordering::Relaxed) {
            return Err(TakanawaError::Cancelled);
        }
        if control.pause.load(Ordering::Relaxed) {
            return Ok(ChunkTaskResult::Paused);
        }
        match with_total_timeout(
            config.timeout.total,
            fetch_chunk_once(
                engine,
                &config.url,
                chunk,
                state,
                control,
                bandwidth,
                writer_tx,
            ),
        )
        .await
        {
            Ok(FetchChunkStatus::Complete) => {
                if control.cancel.load(Ordering::Relaxed) {
                    return Err(TakanawaError::Cancelled);
                }
                if control.pause.load(Ordering::Relaxed) {
                    return Ok(ChunkTaskResult::Paused);
                }
                let metadata = commit_written_chunk(writer_tx, chunk.index).await?;
                return Ok(ChunkTaskResult::Committed(Box::new(metadata)));
            }
            Ok(FetchChunkStatus::Paused) => return Ok(ChunkTaskResult::Paused),
            Err(err) if err.is_retryable() && attempt < retry.max_attempts() => {
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(retry.backoff_max);
            }
            Err(err) => return Err(err),
        }
    }
    Err(TakanawaError::Network(format!(
        "chunk {} exhausted retry attempts",
        chunk.index
    )))
}

async fn probe_once(engine: &DownloadEngine, url: &str, state: &SharedState) -> Result<RemoteInfo> {
    let _permit = engine.limiter.acquire().await;
    let _active_io = ActiveIoGuard::new(state.clone());
    let response = engine
        .client
        .get(url)
        .header(RANGE, "bytes=0-0")
        .header(ACCEPT_ENCODING, "identity")
        .send()
        .await
        .map_err(map_reqwest_error)?;

    if response.status() == StatusCode::RANGE_NOT_SATISFIABLE {
        let total = response
            .headers()
            .get(CONTENT_RANGE)
            .ok_or_else(|| {
                TakanawaError::HttpProtocol("416 response missing Content-Range".to_owned())
            })?
            .to_str()
            .map_err(|err| {
                TakanawaError::HttpProtocol(format!("invalid Content-Range header: {err}"))
            })
            .and_then(parse_unsatisfied_total)?;
        if total == 0 {
            return Ok(RemoteInfo {
                content_len: 0,
                etag: header_to_string(response.headers(), ETAG)?,
                last_modified: header_to_string(response.headers(), LAST_MODIFIED)?,
            });
        }
        return Err(TakanawaError::HttpProtocol(format!(
            "probe range was unsatisfied for non-empty resource length {total}"
        )));
    }

    validate_status(response.status())?;
    validate_identity(response.headers())?;
    let range = response_content_range(&response, 0, 0)?;
    let content_len = response_content_length(&response)?;
    if content_len != 1 {
        return Err(TakanawaError::HttpProtocol(format!(
            "probe Content-Length mismatch: expected 1, got {content_len}"
        )));
    }
    let headers = response.headers().clone();
    let body = response.bytes().await.map_err(map_reqwest_error)?;
    if body.len() != 1 {
        return Err(TakanawaError::HttpProtocol(format!(
            "probe body length mismatch: expected 1, got {}",
            body.len()
        )));
    }

    Ok(RemoteInfo {
        content_len: range.total,
        etag: header_to_string(&headers, ETAG)?,
        last_modified: header_to_string(&headers, LAST_MODIFIED)?,
    })
}

async fn fetch_chunk_once(
    engine: &DownloadEngine,
    url: &str,
    chunk: Chunk,
    state: &SharedState,
    control: &Control,
    bandwidth: &BandwidthLimiter,
    writer_tx: &mpsc::Sender<WriterCommand>,
) -> Result<FetchChunkStatus> {
    let _permit = engine.limiter.acquire().await;
    let _active_io = ActiveIoGuard::new(state.clone());
    let response = engine
        .client
        .get(url)
        .header(RANGE, format!("bytes={}-{}", chunk.start, chunk.end))
        .header(ACCEPT_ENCODING, "identity")
        .send()
        .await
        .map_err(map_reqwest_error)?;

    validate_status(response.status())?;
    validate_identity(response.headers())?;
    let _range = response_content_range(&response, chunk.start, chunk.end)?;
    let content_len = response_content_length(&response)?;
    if content_len != chunk.len {
        return Err(TakanawaError::HttpProtocol(format!(
            "chunk {} Content-Length mismatch: expected {}, got {content_len}",
            chunk.index, chunk.len
        )));
    }
    stream_body_to_writer(response, chunk, state, control, bandwidth, writer_tx).await
}

async fn stream_body_to_writer(
    response: reqwest::Response,
    chunk: Chunk,
    state: &SharedState,
    control: &Control,
    bandwidth: &BandwidthLimiter,
    writer_tx: &mpsc::Sender<WriterCommand>,
) -> Result<FetchChunkStatus> {
    let mut written = 0_u64;
    let mut stream = response.bytes_stream();
    while let Some(bytes) = stream.next().await {
        if control.cancel.load(Ordering::Relaxed) {
            return Err(TakanawaError::Cancelled);
        }
        if control.pause.load(Ordering::Relaxed) {
            return Ok(FetchChunkStatus::Paused);
        }
        let bytes = bytes.map_err(map_reqwest_error)?;
        if bytes.is_empty() {
            continue;
        }
        let len = u64::try_from(bytes.len()).map_err(|_| {
            TakanawaError::HttpProtocol(format!(
                "chunk {} body fragment length does not fit in file offsets",
                chunk.index
            ))
        })?;
        let next_written = written.checked_add(len).ok_or_else(|| {
            TakanawaError::HttpProtocol(format!("chunk {} body length overflow", chunk.index))
        })?;
        if next_written > chunk.len {
            return Err(TakanawaError::HttpProtocol(format!(
                "chunk {} body length exceeded expected {} bytes",
                chunk.index, chunk.len
            )));
        }
        bandwidth.throttle(bytes.len()).await;
        send_writer_write(writer_tx, chunk.index, written, bytes).await?;
        state.record_body_bytes(len);
        written = next_written;
    }

    if written != chunk.len {
        return Err(TakanawaError::HttpProtocol(format!(
            "chunk {} body length mismatch: expected {}, got {}",
            chunk.index, chunk.len, written
        )));
    }
    Ok(FetchChunkStatus::Complete)
}

async fn send_writer_write(
    writer_tx: &mpsc::Sender<WriterCommand>,
    index: u64,
    chunk_offset: u64,
    bytes: Bytes,
) -> Result<()> {
    writer_tx
        .send(WriterCommand::Write {
            index,
            chunk_offset,
            bytes,
        })
        .await
        .map_err(|_| TakanawaError::Ffi("part writer stopped before write".to_owned()))
}

async fn commit_written_chunk(
    writer_tx: &mpsc::Sender<WriterCommand>,
    index: u64,
) -> Result<PartMetadata> {
    let (result_tx, result_rx) = oneshot::channel();
    writer_tx
        .send(WriterCommand::Commit {
            index,
            result: result_tx,
        })
        .await
        .map_err(|_| TakanawaError::Ffi("part writer stopped before commit".to_owned()))?;
    result_rx
        .await
        .map_err(|_| TakanawaError::Ffi("part writer stopped during commit".to_owned()))?
}

async fn with_total_timeout<T>(
    timeout: Duration,
    future: impl Future<Output = Result<T>>,
) -> Result<T> {
    if timeout.is_zero() {
        return future.await;
    }
    tokio::time::timeout(timeout, future).await.map_err(|_| {
        TakanawaError::Network(format!("request exceeded {} ms", timeout.as_millis()))
    })?
}

#[derive(Debug)]
struct BandwidthLimiter {
    bytes_per_second: u64,
    next_available: Mutex<Instant>,
}

impl BandwidthLimiter {
    fn new(bytes_per_second: u64) -> Self {
        Self {
            bytes_per_second,
            next_available: Mutex::new(Instant::now()),
        }
    }

    async fn throttle(&self, bytes: usize) {
        if self.bytes_per_second == 0 || bytes == 0 {
            return;
        }

        let now = Instant::now();
        let wait_until = {
            let mut next_available = self
                .next_available
                .lock()
                .expect("bandwidth limiter mutex poisoned");
            let start = (*next_available).max(now);
            let nanos = (bytes as u128)
                .saturating_mul(1_000_000_000)
                .div_ceil(u128::from(self.bytes_per_second));
            let duration = Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX));
            *next_available = start + duration;
            start
        };

        if wait_until > now {
            tokio::time::sleep_until(tokio::time::Instant::from_std(wait_until)).await;
        }
    }
}

fn validate_status(status: StatusCode) -> Result<()> {
    if status == StatusCode::PARTIAL_CONTENT {
        return Ok(());
    }
    if status == StatusCode::OK {
        return Err(TakanawaError::RangeNotHonored {
            status: status.as_u16(),
        });
    }
    if status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
    {
        return Err(TakanawaError::RetryableHttpStatus(status.as_u16()));
    }
    Err(TakanawaError::HttpProtocol(format!(
        "expected 206 Partial Content, got {status}"
    )))
}

fn validate_identity(headers: &HeaderMap) -> Result<()> {
    if let Some(value) = headers.get(CONTENT_ENCODING) {
        let value = value.to_str().map_err(|err| {
            TakanawaError::HttpProtocol(format!("invalid Content-Encoding: {err}"))
        })?;
        if !value.eq_ignore_ascii_case("identity") {
            return Err(TakanawaError::HttpProtocol(format!(
                "unexpected Content-Encoding {value}"
            )));
        }
    }
    Ok(())
}

fn response_content_range(
    response: &reqwest::Response,
    start: u64,
    end: u64,
) -> Result<crate::content_range::ContentRange> {
    let value = response
        .headers()
        .get(CONTENT_RANGE)
        .ok_or_else(|| TakanawaError::HttpProtocol("missing Content-Range".to_owned()))?
        .to_str()
        .map_err(|err| {
            TakanawaError::HttpProtocol(format!("invalid Content-Range header: {err}"))
        })?;
    let range = parse_content_range(value)?;
    if range.start != start || range.end != end {
        return Err(TakanawaError::HttpProtocol(format!(
            "Content-Range mismatch: expected {start}-{end}, got {}-{}",
            range.start, range.end
        )));
    }
    Ok(range)
}

fn response_content_length(response: &reqwest::Response) -> Result<u64> {
    response
        .headers()
        .get(CONTENT_LENGTH)
        .ok_or_else(|| TakanawaError::HttpProtocol("missing Content-Length".to_owned()))?
        .to_str()
        .map_err(|err| {
            TakanawaError::HttpProtocol(format!("invalid Content-Length header: {err}"))
        })?
        .parse::<u64>()
        .map_err(|err| TakanawaError::HttpProtocol(format!("invalid Content-Length: {err}")))
}

fn header_to_string(
    headers: &HeaderMap,
    name: reqwest::header::HeaderName,
) -> Result<Option<String>> {
    headers
        .get(name)
        .map(|value| {
            value.to_str().map(str::to_owned).map_err(|err| {
                TakanawaError::HttpProtocol(format!("invalid response header: {err}"))
            })
        })
        .transpose()
}

#[allow(clippy::needless_pass_by_value)]
fn map_reqwest_error(err: reqwest::Error) -> TakanawaError {
    if err.is_timeout() || err.is_connect() || err.is_request() || err.is_body() || err.is_decode()
    {
        TakanawaError::Network(err.to_string())
    } else {
        TakanawaError::HttpProtocol(err.to_string())
    }
}

struct ActiveIoGuard {
    state: SharedState,
}

impl ActiveIoGuard {
    fn new(state: SharedState) -> Self {
        state.increment_active_io();
        Self { state }
    }
}

impl Drop for ActiveIoGuard {
    fn drop(&mut self) {
        self.state.decrement_active_io();
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use sha2::{Digest, Sha256};
    use tempfile::TempDir;

    #[test]
    fn injected_client_is_preserved_when_applying_download_timeouts() {
        let engine = DownloadEngine::with_client(Client::new(), 7);
        let configured = engine
            .with_timeout(TimeoutConfig {
                connect: Duration::from_millis(1),
                read: Duration::from_millis(1),
                total: Duration::from_millis(1),
            })
            .unwrap();

        assert_eq!(configured.client_mode, ClientMode::External);
        assert_eq!(configured.max_io(), 7);
    }

    #[tokio::test]
    async fn injected_client_routes_download_through_http_proxy() {
        let data = Arc::new(b"abcdefghij".to_vec());
        let proxy_addr = spawn_range_server(Arc::clone(&data), false);
        let client = Client::builder()
            .proxy(reqwest::Proxy::all(format!("http://{proxy_addr}")).unwrap())
            .build()
            .unwrap();
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("out.bin");
        let engine = DownloadEngine::with_client(client, DEFAULT_MAX_IO);
        let config = DownloadConfig {
            url: "http://origin.invalid/file".to_owned(),
            target_path: target.clone(),
            chunk_size: 5,
            parallelism: 2,
            max_parallel_chunks: 0,
            retry: RetryConfig::default(),
            timeout: TimeoutConfig::default(),
            bytes_per_second_limit: 0,
            hash: HashConfig::None,
        };

        let snapshot = download_to_completion(engine, config).await.unwrap();

        assert_eq!(snapshot.phase, DownloadPhase::Completed);
        assert_eq!(std::fs::read(target).unwrap(), data.as_slice());
    }

    use super::*;
    use crate::DownloadPhase;
    use crate::limiter::DEFAULT_MAX_IO;

    #[tokio::test]
    async fn downloads_file_with_ranges() {
        let data = Arc::new(b"abcdefghijklmnopqrstuvwxyz".to_vec());
        let addr = spawn_range_server(Arc::clone(&data), false);
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("out.bin");
        let engine = DownloadEngine::new(DEFAULT_MAX_IO).unwrap();
        let config = DownloadConfig {
            url: format!("http://{addr}/file"),
            target_path: target.clone(),
            chunk_size: 5,
            parallelism: 2,
            max_parallel_chunks: 0,
            retry: RetryConfig::default(),
            timeout: TimeoutConfig::default(),
            bytes_per_second_limit: 0,
            hash: HashConfig::None,
        };

        let snapshot = download_to_completion(engine, config).await.unwrap();

        assert_eq!(snapshot.phase, DownloadPhase::Completed);
        assert_eq!(std::fs::read(target).unwrap(), data.as_slice());
    }

    #[test]
    fn start_reports_starting_before_async_probe_completes() {
        let data = Arc::new(b"abcdefghij".to_vec());
        let addr = spawn_delayed_chunk_server(Arc::clone(&data), Duration::from_millis(200));
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("out.bin");
        let engine = DownloadEngine::new(DEFAULT_MAX_IO).unwrap();
        let runtime = Runtime::new().unwrap();
        let download = DownloadHandle::new(
            engine,
            DownloadConfig {
                url: format!("http://{addr}/file"),
                target_path: target,
                chunk_size: 5,
                parallelism: 1,
                max_parallel_chunks: 0,
                retry: RetryConfig::default(),
                timeout: TimeoutConfig::default(),
                bytes_per_second_limit: 0,
                hash: HashConfig::None,
            },
        );

        download.start_on(&runtime).unwrap();
        assert_eq!(download.snapshot().phase, DownloadPhase::Starting);

        download.cancel().unwrap();
        assert_eq!(
            wait_for_phase(&download, DownloadPhase::Cancelled).phase,
            DownloadPhase::Cancelled
        );
    }

    #[tokio::test]
    async fn rejects_ignored_range() {
        let data = Arc::new(b"abcdef".to_vec());
        let addr = spawn_range_server(Arc::clone(&data), true);
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("out.bin");
        let engine = DownloadEngine::new(DEFAULT_MAX_IO).unwrap();
        let config = DownloadConfig {
            url: format!("http://{addr}/file"),
            target_path: target,
            chunk_size: 3,
            parallelism: 1,
            max_parallel_chunks: 0,
            retry: RetryConfig::default(),
            timeout: TimeoutConfig::default(),
            bytes_per_second_limit: 0,
            hash: HashConfig::None,
        };

        let err = download_to_completion(engine, config).await.unwrap_err();

        assert!(matches!(
            err,
            TakanawaError::RangeNotHonored { status: 200 }
        ));
    }

    #[tokio::test]
    async fn resumes_from_existing_part() {
        let data = Arc::new(b"abcdefghijklmnopqrstuvwxyz".to_vec());
        let addr = spawn_range_server(Arc::clone(&data), false);
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("out.bin");
        let remote = RemoteInfo {
            content_len: data.len() as u64,
            etag: None,
            last_modified: None,
        };
        let mut part = PartFile::open_or_create(
            &target,
            &format!("http://{addr}/file"),
            &remote,
            5,
            HashConfig::None,
        )
        .unwrap();
        part.write_chunk(0, &data[..5]).unwrap();
        drop(part);

        let engine = DownloadEngine::new(DEFAULT_MAX_IO).unwrap();
        let config = DownloadConfig {
            url: format!("http://{addr}/file"),
            target_path: target.clone(),
            chunk_size: 5,
            parallelism: 2,
            max_parallel_chunks: 0,
            retry: RetryConfig::default(),
            timeout: TimeoutConfig::default(),
            bytes_per_second_limit: 0,
            hash: HashConfig::None,
        };

        let snapshot = download_to_completion(engine, config).await.unwrap();

        assert_eq!(snapshot.phase, DownloadPhase::Completed);
        assert_eq!(std::fs::read(target).unwrap(), data.as_slice());
    }

    #[tokio::test]
    async fn retries_after_partial_stream_write() {
        let data = Arc::new(b"abcdefghij".to_vec());
        let addr = spawn_truncated_once_server(Arc::clone(&data), 2);
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("out.bin");
        let engine = DownloadEngine::new(DEFAULT_MAX_IO).unwrap();
        let config = DownloadConfig {
            url: format!("http://{addr}/file"),
            target_path: target.clone(),
            chunk_size: 5,
            parallelism: 1,
            max_parallel_chunks: 0,
            retry: RetryConfig {
                max_retries: 1,
                backoff_initial: Duration::from_millis(1),
                backoff_max: Duration::from_millis(1),
            },
            timeout: TimeoutConfig::default(),
            bytes_per_second_limit: 0,
            hash: HashConfig::None,
        };

        let snapshot = download_to_completion(engine, config).await.unwrap();

        assert_eq!(snapshot.phase, DownloadPhase::Completed);
        assert_eq!(std::fs::read(target).unwrap(), data.as_slice());
    }

    #[test]
    fn pause_discards_in_flight_chunk() {
        let data = Arc::new(b"abcdefghij".to_vec());
        let addr = spawn_delayed_chunk_server(Arc::clone(&data), Duration::from_millis(300));
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("out.bin");
        let engine = DownloadEngine::new(DEFAULT_MAX_IO).unwrap();
        let runtime = Runtime::new().unwrap();
        let download = DownloadHandle::new(
            engine,
            DownloadConfig {
                url: format!("http://{addr}/file"),
                target_path: target,
                chunk_size: 5,
                parallelism: 1,
                max_parallel_chunks: 0,
                retry: RetryConfig::default(),
                timeout: TimeoutConfig::default(),
                bytes_per_second_limit: 0,
                hash: HashConfig::None,
            },
        );

        download.start_on(&runtime).unwrap();
        thread::sleep(Duration::from_millis(100));
        download.pause().unwrap();

        let snapshot = wait_for_phase_and_idle(&download, DownloadPhase::Paused);

        assert_eq!(snapshot.completed_chunks, 0);
        assert_eq!(snapshot.downloaded_bytes, 0);
    }

    #[test]
    fn pause_mid_stream_discards_uncommitted_bytes_and_resume_completes() {
        let data = Arc::new(b"abcdefghij".to_vec());
        let addr = spawn_split_body_server(Arc::clone(&data), 2, Duration::from_millis(300));
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("out.bin");
        let engine = DownloadEngine::new(DEFAULT_MAX_IO).unwrap();
        let runtime = Runtime::new().unwrap();
        let download = DownloadHandle::new(
            engine,
            DownloadConfig {
                url: format!("http://{addr}/file"),
                target_path: target.clone(),
                chunk_size: 5,
                parallelism: 1,
                max_parallel_chunks: 0,
                retry: RetryConfig::default(),
                timeout: TimeoutConfig::default(),
                bytes_per_second_limit: 0,
                hash: HashConfig::None,
            },
        );

        download.start_on(&runtime).unwrap();
        thread::sleep(Duration::from_millis(100));
        download.pause().unwrap();

        let snapshot = wait_for_phase_and_idle(&download, DownloadPhase::Paused);

        assert_eq!(snapshot.completed_chunks, 0);
        assert_eq!(snapshot.downloaded_bytes, 0);

        download.start_on(&runtime).unwrap();
        let snapshot = wait_for_phase(&download, DownloadPhase::Completed);

        assert_eq!(snapshot.phase, DownloadPhase::Completed);
        assert_eq!(std::fs::read(target).unwrap(), data.as_slice());
    }

    #[tokio::test]
    async fn rejects_existing_target() {
        let data = Arc::new(b"abcdef".to_vec());
        let addr = spawn_range_server(Arc::clone(&data), false);
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("out.bin");
        std::fs::write(&target, b"already here").unwrap();
        let engine = DownloadEngine::new(DEFAULT_MAX_IO).unwrap();
        let config = DownloadConfig {
            url: format!("http://{addr}/file"),
            target_path: target,
            chunk_size: 3,
            parallelism: 1,
            max_parallel_chunks: 0,
            retry: RetryConfig::default(),
            timeout: TimeoutConfig::default(),
            bytes_per_second_limit: 0,
            hash: HashConfig::None,
        };

        let err = download_to_completion(engine, config).await.unwrap_err();

        assert!(matches!(err, TakanawaError::TargetExists(_)));
    }

    #[tokio::test]
    async fn verifies_sha256_before_finalize() {
        let data = Arc::new(b"abcdef".to_vec());
        let addr = spawn_range_server(Arc::clone(&data), false);
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("out.bin");
        let expected: [u8; 32] = Sha256::digest(data.as_slice()).into();
        let engine = DownloadEngine::new(DEFAULT_MAX_IO).unwrap();
        let config = DownloadConfig {
            url: format!("http://{addr}/file"),
            target_path: target.clone(),
            chunk_size: 3,
            parallelism: 1,
            max_parallel_chunks: 0,
            retry: RetryConfig::default(),
            timeout: TimeoutConfig::default(),
            bytes_per_second_limit: 0,
            hash: HashConfig::Sha256(expected),
        };

        download_to_completion(engine, config).await.unwrap();

        assert_eq!(std::fs::read(target).unwrap(), data.as_slice());
    }

    #[tokio::test]
    async fn total_timeout_aborts_slow_chunk() {
        let data = Arc::new(b"abcdef".to_vec());
        let addr = spawn_delayed_chunk_server(Arc::clone(&data), Duration::from_millis(300));
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("out.bin");
        let engine = DownloadEngine::new(DEFAULT_MAX_IO).unwrap();
        let config = DownloadConfig {
            url: format!("http://{addr}/file"),
            target_path: target,
            chunk_size: 3,
            parallelism: 1,
            max_parallel_chunks: 0,
            retry: RetryConfig {
                max_retries: 0,
                backoff_initial: Duration::from_millis(1),
                backoff_max: Duration::from_millis(1),
            },
            timeout: TimeoutConfig {
                connect: Duration::from_secs(30),
                read: Duration::ZERO,
                total: Duration::from_millis(50),
            },
            bytes_per_second_limit: 0,
            hash: HashConfig::None,
        };

        let err = download_to_completion(engine, config).await.unwrap_err();

        assert!(matches!(err, TakanawaError::Network(_)));
    }

    fn spawn_range_server(data: Arc<Vec<u8>>, ignore_range: bool) -> SocketAddr {
        spawn_range_server_with_chunk_delay(data, ignore_range, None)
    }

    fn spawn_delayed_chunk_server(data: Arc<Vec<u8>>, delay: Duration) -> SocketAddr {
        spawn_range_server_with_chunk_delay(data, false, Some(delay))
    }

    fn spawn_split_body_server(
        data: Arc<Vec<u8>>,
        first_body_bytes: usize,
        delay: Duration,
    ) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let data = Arc::clone(&data);
                thread::spawn(move || {
                    handle_split_body_connection(stream, &data, first_body_bytes, delay);
                });
            }
        });
        addr
    }

    fn spawn_truncated_once_server(
        data: Arc<Vec<u8>>,
        body_bytes_before_close: usize,
    ) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let truncated = Arc::new(std::sync::atomic::AtomicBool::new(false));
        thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let data = Arc::clone(&data);
                let truncated = Arc::clone(&truncated);
                thread::spawn(move || {
                    handle_truncated_once_connection(
                        stream,
                        &data,
                        body_bytes_before_close,
                        &truncated,
                    );
                });
            }
        });
        addr
    }

    fn spawn_range_server_with_chunk_delay(
        data: Arc<Vec<u8>>,
        ignore_range: bool,
        chunk_delay: Option<Duration>,
    ) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let data = Arc::clone(&data);
                thread::spawn(move || handle_connection(stream, &data, ignore_range, chunk_delay));
            }
        });
        addr
    }

    fn handle_connection(
        mut stream: std::net::TcpStream,
        data: &[u8],
        ignore_range: bool,
        chunk_delay: Option<Duration>,
    ) {
        let mut buffer = [0; 4096];
        let read = stream.read(&mut buffer).unwrap_or(0);
        let request = String::from_utf8_lossy(&buffer[..read]);
        let range = request_range(&request);

        if ignore_range {
            let response = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", data.len());
            stream.write_all(response.as_bytes()).unwrap();
            stream.write_all(data).unwrap();
            return;
        }

        let Some((start, end)) = range else {
            stream
                .write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n")
                .unwrap();
            return;
        };
        if start >= data.len() {
            let response = format!(
                "HTTP/1.1 416 Range Not Satisfiable\r\nContent-Range: bytes */{}\r\nContent-Length: 0\r\n\r\n",
                data.len()
            );
            stream.write_all(response.as_bytes()).unwrap();
            return;
        }
        let end = end.min(data.len() - 1);
        let body = &data[start..=end];
        if let Some(delay) = chunk_delay {
            if !(start == 0 && end == 0) {
                thread::sleep(delay);
            }
        }
        let response = format!(
            "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes {start}-{end}/{}\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\n\r\n",
            data.len(),
            body.len()
        );
        stream.write_all(response.as_bytes()).unwrap();
        stream.write_all(body).unwrap();
    }

    fn handle_split_body_connection(
        mut stream: std::net::TcpStream,
        data: &[u8],
        first_body_bytes: usize,
        delay: Duration,
    ) {
        let Some((start, end)) = read_request_range(&mut stream) else {
            let _ = stream.write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n");
            return;
        };
        let Some(body) = range_body(data, start, end, &mut stream) else {
            return;
        };
        let response = format!(
            "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes {start}-{}/{}\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\n\r\n",
            start + body.len() - 1,
            data.len(),
            body.len()
        );
        let _ = stream.write_all(response.as_bytes());
        if body.len() <= 1 {
            let _ = stream.write_all(body);
            return;
        }
        let split_at = first_body_bytes.clamp(1, body.len());
        let _ = stream.write_all(&body[..split_at]);
        let _ = stream.flush();
        thread::sleep(delay);
        let _ = stream.write_all(&body[split_at..]);
    }

    fn handle_truncated_once_connection(
        mut stream: std::net::TcpStream,
        data: &[u8],
        body_bytes_before_close: usize,
        truncated: &std::sync::atomic::AtomicBool,
    ) {
        let Some((start, end)) = read_request_range(&mut stream) else {
            let _ = stream.write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n");
            return;
        };
        let Some(body) = range_body(data, start, end, &mut stream) else {
            return;
        };
        let response = format!(
            "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes {start}-{}/{}\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\n\r\n",
            start + body.len() - 1,
            data.len(),
            body.len()
        );
        let _ = stream.write_all(response.as_bytes());
        if body.len() > 1 && !truncated.swap(true, std::sync::atomic::Ordering::SeqCst) {
            let end = body_bytes_before_close.min(body.len());
            let _ = stream.write_all(&body[..end]);
            return;
        }
        let _ = stream.write_all(body);
    }

    fn read_request_range(stream: &mut std::net::TcpStream) -> Option<(usize, usize)> {
        let mut buffer = [0; 4096];
        let read = stream.read(&mut buffer).ok()?;
        let request = String::from_utf8_lossy(&buffer[..read]);
        request_range(&request)
    }

    fn request_range(request: &str) -> Option<(usize, usize)> {
        let range = request.lines().find_map(|line| {
            let (name, value) = line.split_once(':')?;
            if name.eq_ignore_ascii_case("range") {
                value.trim().strip_prefix("bytes=")
            } else {
                None
            }
        })?;
        let (start, end) = range.split_once('-')?;
        Some((start.parse().ok()?, end.parse().ok()?))
    }

    fn range_body<'a>(
        data: &'a [u8],
        start: usize,
        end: usize,
        stream: &mut std::net::TcpStream,
    ) -> Option<&'a [u8]> {
        if start >= data.len() {
            let response = format!(
                "HTTP/1.1 416 Range Not Satisfiable\r\nContent-Range: bytes */{}\r\nContent-Length: 0\r\n\r\n",
                data.len()
            );
            let _ = stream.write_all(response.as_bytes());
            return None;
        }
        let end = end.min(data.len() - 1);
        Some(&data[start..=end])
    }

    fn wait_for_phase(download: &DownloadHandle, phase: DownloadPhase) -> DownloadSnapshot {
        for _ in 0..100 {
            let snapshot = download.snapshot();
            if snapshot.phase == phase {
                return snapshot;
            }
            thread::sleep(Duration::from_millis(20));
        }
        download.snapshot()
    }

    fn wait_for_phase_and_idle(
        download: &DownloadHandle,
        phase: DownloadPhase,
    ) -> DownloadSnapshot {
        for _ in 0..500 {
            let snapshot = download.snapshot();
            let idle = download
                .join
                .lock()
                .expect("download join mutex poisoned")
                .as_ref()
                .is_none_or(tokio::task::JoinHandle::is_finished);
            if snapshot.phase == phase && idle {
                return snapshot;
            }
            thread::sleep(Duration::from_millis(20));
        }
        panic!(
            "download did not reach {phase:?} and idle state; latest snapshot: {:?}",
            download.snapshot()
        );
    }
}

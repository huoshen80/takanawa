use std::collections::VecDeque;
use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use takanawa_core::{PartMetadata, TakanawaError};

/// Lifecycle phase reported for a download.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownloadPhase {
    /// The download handle has been created but not started.
    Created = 0,
    /// The download is actively probing, fetching, or finalizing.
    Running = 1,
    /// The download is paused and can be started again.
    Paused = 2,
    /// The download has been cancelled.
    Cancelled = 3,
    /// The download finished successfully.
    Completed = 4,
    /// The download failed.
    Failed = 5,
    /// A pause was requested and in-flight work is winding down.
    Pausing = 6,
    /// A cancellation was requested and in-flight work is winding down.
    Cancelling = 7,
    /// A start or resume request was accepted and background work is starting.
    Starting = 8,
    /// The download is opening, validating, or allocating its part file.
    Allocating = 9,
    /// The completed part file is being verified before promotion.
    Verifying = 10,
}

/// Point-in-time download progress.
#[derive(Debug, Clone)]
pub struct DownloadSnapshot {
    /// Current lifecycle phase.
    pub phase: DownloadPhase,
    /// Total content length in bytes, or `0` before the remote probe completes.
    pub content_len: u64,
    /// Number of bytes represented by committed chunks.
    pub downloaded_bytes: u64,
    /// Chunk size in bytes, or `0` before metadata is available.
    pub chunk_size: u64,
    /// Total chunk count, or `0` before metadata is available.
    pub chunk_count: u64,
    /// Number of chunks committed complete.
    pub completed_chunks: u64,
    /// Current number of active I/O operations.
    pub active_io: usize,
    /// Last failure message, when the download failed.
    pub last_error: Option<String>,
    /// Stable status code for the last failure, when the download failed.
    pub last_error_code: Option<i32>,
}

/// Callback invoked when download progress changes.
pub type ProgressCallback = Arc<dyn Fn(DownloadSnapshot) + Send + Sync + 'static>;

/// Point-in-time download speed sample.
#[derive(Debug, Clone)]
pub struct DownloadSpeedSnapshot {
    /// Current lifecycle phase.
    pub phase: DownloadPhase,
    /// Total content length in bytes, or `0` before the remote probe completes.
    pub content_len: u64,
    /// Bytes represented by committed chunks plus response-body bytes observed for this task.
    pub received_bytes: u64,
    /// Bytes observed in the current speed smoothing window.
    pub interval_bytes: u64,
    /// Milliseconds covered by the current speed smoothing window.
    pub elapsed_millis: u64,
    /// Current transfer speed in bytes per second for the smoothing window.
    pub bytes_per_second: f64,
    /// Current number of active I/O operations.
    pub active_io: usize,
}

/// Callback invoked when response-body bytes are received.
pub type SpeedCallback = Arc<dyn Fn(DownloadSpeedSnapshot) + Send + Sync + 'static>;

const CALLBACK_EVENTS_PER_SECOND: u64 = 60;
const CALLBACK_THROTTLE_INTERVAL: Duration =
    Duration::from_nanos(1_000_000_000 / CALLBACK_EVENTS_PER_SECOND);
const SPEED_CALLBACK_SMOOTHING_WINDOW: Duration = Duration::from_secs(1);

#[derive(Debug, Clone)]
pub(crate) struct SharedState {
    inner: Arc<Inner>,
}

struct Inner {
    progress: Mutex<Progress>,
    speed: Mutex<SpeedProgress>,
    active_io: AtomicUsize,
    progress_callback: Mutex<Option<ProgressCallback>>,
    speed_callback: Mutex<Option<SpeedCallback>>,
    progress_throttle: Mutex<CallbackThrottle>,
    speed_throttle: Mutex<CallbackThrottle>,
}

impl fmt::Debug for Inner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Inner")
            .field("progress", &self.progress)
            .field("speed", &self.speed)
            .field("active_io", &self.active_io)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone)]
struct Progress {
    lifecycle: DownloadLifecycle,
    content_len: u64,
    downloaded_bytes: u64,
    chunk_size: u64,
    chunk_count: u64,
    completed_chunks: u64,
    bitmap: Vec<u8>,
    last_error: Option<String>,
    last_error_code: Option<i32>,
    last_http_status: Option<u16>,
}

#[derive(Debug, Clone)]
struct SpeedProgress {
    content_len: u64,
    received_bytes: u64,
    interval_bytes: u64,
    elapsed_millis: u64,
    bytes_per_second: f64,
    samples: VecDeque<SpeedSample>,
    started_at: Option<Instant>,
}

#[derive(Debug, Clone, Copy)]
struct SpeedSample {
    observed_at: Instant,
    bytes: u64,
}

impl SpeedProgress {
    fn record_body_bytes(&mut self, bytes: u64, now: Instant) {
        self.received_bytes = self.received_bytes.saturating_add(bytes);
        self.started_at = Some(self.started_at.unwrap_or(now));
        self.samples.push_back(SpeedSample {
            observed_at: now,
            bytes,
        });
        self.refresh_window(now);
    }

    fn refresh_window(&mut self, now: Instant) {
        let cutoff = now
            .checked_sub(SPEED_CALLBACK_SMOOTHING_WINDOW)
            .unwrap_or(now);
        while self
            .samples
            .front()
            .is_some_and(|sample| sample.observed_at < cutoff)
        {
            self.samples.pop_front();
        }

        let window_bytes = self
            .samples
            .iter()
            .fold(0_u64, |sum, sample| sum.saturating_add(sample.bytes));
        let window_start = self
            .started_at
            .map_or(cutoff, |started_at| started_at.max(cutoff));
        let elapsed = now.saturating_duration_since(window_start);
        self.interval_bytes = window_bytes;
        self.elapsed_millis = elapsed.as_millis().try_into().unwrap_or(u64::MAX);
        self.bytes_per_second = if elapsed.is_zero() {
            0.0
        } else {
            u64_to_f64(window_bytes) / elapsed.as_secs_f64()
        };
    }

    fn reset_window(&mut self, now: Instant) {
        self.interval_bytes = 0;
        self.elapsed_millis = 0;
        self.bytes_per_second = 0.0;
        self.samples.clear();
        self.started_at = Some(now);
    }
}

#[derive(Debug, Default)]
struct CallbackThrottle {
    last_emit_at: Option<Instant>,
    trailing_scheduled: bool,
    generation: u64,
}

impl CallbackThrottle {
    fn reset(&mut self) {
        self.last_emit_at = None;
        self.trailing_scheduled = false;
        self.generation = self.generation.wrapping_add(1);
    }
}

#[derive(Debug)]
enum CallbackThrottleAction {
    Emit,
    Schedule { delay: Duration, generation: u64 },
    Skip,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DownloadLifecycle {
    Created,
    Starting,
    Allocating,
    Running,
    Pausing,
    Paused,
    Verifying,
    Cancelling,
    Cancelled,
    Completed,
    Failed,
}

impl DownloadLifecycle {
    const fn phase(self) -> DownloadPhase {
        match self {
            Self::Created => DownloadPhase::Created,
            Self::Starting => DownloadPhase::Starting,
            Self::Allocating => DownloadPhase::Allocating,
            Self::Running => DownloadPhase::Running,
            Self::Pausing => DownloadPhase::Pausing,
            Self::Paused => DownloadPhase::Paused,
            Self::Verifying => DownloadPhase::Verifying,
            Self::Cancelling => DownloadPhase::Cancelling,
            Self::Cancelled => DownloadPhase::Cancelled,
            Self::Completed => DownloadPhase::Completed,
            Self::Failed => DownloadPhase::Failed,
        }
    }

    const fn request_start(self) -> Self {
        match self {
            Self::Cancelling => Self::Cancelling,
            Self::Starting | Self::Allocating | Self::Running | Self::Pausing | Self::Verifying => {
                self
            }
            Self::Created | Self::Paused | Self::Cancelled | Self::Completed | Self::Failed => {
                Self::Starting
            }
        }
    }

    const fn mark_allocating(self) -> Self {
        match self {
            Self::Starting | Self::Running => Self::Allocating,
            _ => self,
        }
    }

    const fn mark_running(self) -> Self {
        match self {
            Self::Starting | Self::Allocating | Self::Paused => Self::Running,
            _ => self,
        }
    }

    const fn mark_verifying(self) -> Self {
        match self {
            Self::Running => Self::Verifying,
            _ => self,
        }
    }

    const fn request_pause(self) -> Self {
        match self {
            Self::Starting | Self::Allocating | Self::Running | Self::Pausing => Self::Pausing,
            _ => self,
        }
    }

    const fn mark_paused(self) -> Self {
        match self {
            Self::Starting | Self::Allocating | Self::Running | Self::Pausing => Self::Paused,
            _ => self,
        }
    }

    const fn request_cancel(self) -> Self {
        match self {
            Self::Created => Self::Cancelled,
            Self::Starting | Self::Allocating | Self::Running | Self::Pausing | Self::Paused => {
                Self::Cancelling
            }
            _ => self,
        }
    }

    const fn mark_cancelled(self) -> Self {
        match self {
            Self::Created
            | Self::Starting
            | Self::Allocating
            | Self::Running
            | Self::Pausing
            | Self::Paused
            | Self::Cancelling => Self::Cancelled,
            _ => self,
        }
    }

    const fn mark_completed(self) -> Self {
        match self {
            Self::Running | Self::Verifying => Self::Completed,
            _ => self,
        }
    }

    const fn mark_failed(self) -> Self {
        match self {
            Self::Created
            | Self::Starting
            | Self::Allocating
            | Self::Running
            | Self::Pausing
            | Self::Paused
            | Self::Verifying
            | Self::Cancelling
            | Self::Cancelled
            | Self::Completed
            | Self::Failed => Self::Failed,
        }
    }
}

impl SharedState {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                progress: Mutex::new(Progress {
                    lifecycle: DownloadLifecycle::Created,
                    content_len: 0,
                    downloaded_bytes: 0,
                    chunk_size: 0,
                    chunk_count: 0,
                    completed_chunks: 0,
                    bitmap: Vec::new(),
                    last_error: None,
                    last_error_code: None,
                    last_http_status: None,
                }),
                speed: Mutex::new(SpeedProgress {
                    content_len: 0,
                    received_bytes: 0,
                    interval_bytes: 0,
                    elapsed_millis: 0,
                    bytes_per_second: 0.0,
                    samples: VecDeque::new(),
                    started_at: None,
                }),
                active_io: AtomicUsize::new(0),
                progress_callback: Mutex::new(None),
                speed_callback: Mutex::new(None),
                progress_throttle: Mutex::new(CallbackThrottle::default()),
                speed_throttle: Mutex::new(CallbackThrottle::default()),
            }),
        }
    }

    pub fn set_progress_callback(&self, callback: Option<ProgressCallback>) {
        let should_notify = callback.is_some();
        let previous = {
            let mut progress_callback = self
                .inner
                .progress_callback
                .lock()
                .expect("download callback mutex poisoned");
            std::mem::replace(&mut *progress_callback, callback)
        };
        self.inner
            .progress_throttle
            .lock()
            .expect("download progress callback throttle mutex poisoned")
            .reset();
        drop(previous);
        if should_notify {
            self.notify_progress();
        }
    }

    pub fn set_speed_callback(&self, callback: Option<SpeedCallback>) {
        let should_notify = callback.is_some();
        let previous = {
            let mut speed_callback = self
                .inner
                .speed_callback
                .lock()
                .expect("download speed callback mutex poisoned");
            std::mem::replace(&mut *speed_callback, callback)
        };
        self.inner
            .speed_throttle
            .lock()
            .expect("download speed callback throttle mutex poisoned")
            .reset();
        drop(previous);
        if should_notify {
            self.notify_speed();
        }
    }

    pub fn request_start(&self) {
        self.reset_speed_window();
        self.transition(DownloadLifecycle::request_start);
    }

    pub fn mark_allocating(&self) {
        self.transition(DownloadLifecycle::mark_allocating);
    }

    pub fn mark_running(&self) {
        self.transition(DownloadLifecycle::mark_running);
    }

    pub fn mark_verifying(&self) {
        self.transition(DownloadLifecycle::mark_verifying);
    }

    pub fn request_pause(&self) {
        self.transition(DownloadLifecycle::request_pause);
    }

    pub fn mark_paused(&self) {
        self.transition(DownloadLifecycle::mark_paused);
    }

    pub fn request_cancel(&self) {
        self.transition(DownloadLifecycle::request_cancel);
    }

    pub fn mark_cancelled(&self) {
        self.transition(DownloadLifecycle::mark_cancelled);
    }

    pub fn mark_completed(&self) {
        self.transition(DownloadLifecycle::mark_completed);
    }

    fn transition(&self, transition: impl FnOnce(DownloadLifecycle) -> DownloadLifecycle) {
        let changed = {
            let mut progress = self
                .inner
                .progress
                .lock()
                .expect("download state mutex poisoned");
            let next = transition(progress.lifecycle);
            let changed = progress.lifecycle != next;
            progress.lifecycle = next;
            changed
        };
        if changed {
            self.notify_progress();
        }
    }

    pub fn mark_failed(&self, error: &TakanawaError) {
        {
            let mut progress = self
                .inner
                .progress
                .lock()
                .expect("download state mutex poisoned");
            progress.lifecycle = progress.lifecycle.mark_failed();
            progress.last_error = Some(error.to_string());
            progress.last_error_code = Some(error.status_code());
            progress.last_http_status = error.http_status();
        }
        self.notify_progress();
    }

    pub fn clear_error(&self) {
        let mut progress = self
            .inner
            .progress
            .lock()
            .expect("download state mutex poisoned");
        progress.last_error = None;
        progress.last_error_code = None;
        progress.last_http_status = None;
    }

    #[must_use]
    pub fn last_http_status(&self) -> Option<u16> {
        self.inner
            .progress
            .lock()
            .expect("download state mutex poisoned")
            .last_http_status
    }

    pub fn update_from_metadata(&self, metadata: &PartMetadata) {
        {
            let mut progress = self
                .inner
                .progress
                .lock()
                .expect("download state mutex poisoned");
            progress.content_len = metadata.content_len;
            progress.downloaded_bytes = metadata.completed_bytes();
            progress.chunk_size = metadata.chunk_size;
            progress.chunk_count = metadata.chunk_count;
            progress.completed_chunks = metadata.completed_chunks();
            progress.bitmap = metadata.bitmap.as_bytes().to_vec();
        }
        self.update_speed_metadata(metadata.content_len, metadata.completed_bytes());
        self.notify_progress();
    }

    pub fn record_body_bytes(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        self.record_body_bytes_at(bytes, Instant::now());
    }

    fn record_body_bytes_at(&self, bytes: u64, now: Instant) {
        {
            let mut speed = self
                .inner
                .speed
                .lock()
                .expect("download speed mutex poisoned");
            speed.record_body_bytes(bytes, now);
        }
        self.notify_speed();
    }

    pub fn increment_active_io(&self) {
        self.inner.active_io.fetch_add(1, Ordering::Relaxed);
        self.notify_progress();
    }

    pub fn decrement_active_io(&self) {
        self.inner.active_io.fetch_sub(1, Ordering::Relaxed);
        self.notify_progress();
    }

    #[must_use]
    pub fn snapshot(&self) -> DownloadSnapshot {
        let progress = self
            .inner
            .progress
            .lock()
            .expect("download state mutex poisoned")
            .clone();
        DownloadSnapshot {
            phase: progress.lifecycle.phase(),
            content_len: progress.content_len,
            downloaded_bytes: progress.downloaded_bytes,
            chunk_size: progress.chunk_size,
            chunk_count: progress.chunk_count,
            completed_chunks: progress.completed_chunks,
            active_io: self.inner.active_io.load(Ordering::Relaxed),
            last_error: progress.last_error,
            last_error_code: progress.last_error_code,
        }
    }

    #[must_use]
    pub fn speed_snapshot(&self) -> DownloadSpeedSnapshot {
        self.speed_snapshot_at(Instant::now())
    }

    fn speed_snapshot_at(&self, now: Instant) -> DownloadSpeedSnapshot {
        let phase = self
            .inner
            .progress
            .lock()
            .expect("download state mutex poisoned")
            .lifecycle
            .phase();
        let speed = {
            let mut speed = self
                .inner
                .speed
                .lock()
                .expect("download speed mutex poisoned");
            speed.refresh_window(now);
            speed.clone()
        };
        DownloadSpeedSnapshot {
            phase,
            content_len: speed.content_len,
            received_bytes: speed.received_bytes,
            interval_bytes: speed.interval_bytes,
            elapsed_millis: speed.elapsed_millis,
            bytes_per_second: speed.bytes_per_second,
            active_io: self.inner.active_io.load(Ordering::Relaxed),
        }
    }

    #[must_use]
    pub fn bitmap(&self) -> Vec<u8> {
        self.inner
            .progress
            .lock()
            .expect("download state mutex poisoned")
            .bitmap
            .clone()
    }

    fn notify_progress(&self) {
        if self
            .inner
            .progress_callback
            .lock()
            .expect("download callback mutex poisoned")
            .is_none()
        {
            return;
        }
        match self.progress_throttle_action(Instant::now()) {
            CallbackThrottleAction::Emit => self.emit_progress_callback(),
            CallbackThrottleAction::Schedule { delay, generation } => {
                self.schedule_progress_callback(delay, generation);
            }
            CallbackThrottleAction::Skip => {}
        }
    }

    fn notify_speed(&self) {
        if self
            .inner
            .speed_callback
            .lock()
            .expect("download speed callback mutex poisoned")
            .is_none()
        {
            return;
        }
        match self.speed_throttle_action(Instant::now()) {
            CallbackThrottleAction::Emit => self.emit_speed_callback(),
            CallbackThrottleAction::Schedule { delay, generation } => {
                self.schedule_speed_callback(delay, generation);
            }
            CallbackThrottleAction::Skip => {}
        }
    }

    fn progress_throttle_action(&self, now: Instant) -> CallbackThrottleAction {
        let mut throttle = self
            .inner
            .progress_throttle
            .lock()
            .expect("download progress callback throttle mutex poisoned");
        Self::throttle_action(&mut throttle, now)
    }

    fn speed_throttle_action(&self, now: Instant) -> CallbackThrottleAction {
        let mut throttle = self
            .inner
            .speed_throttle
            .lock()
            .expect("download speed callback throttle mutex poisoned");
        Self::throttle_action(&mut throttle, now)
    }

    fn throttle_action(throttle: &mut CallbackThrottle, now: Instant) -> CallbackThrottleAction {
        if throttle.trailing_scheduled {
            return CallbackThrottleAction::Skip;
        }

        let Some(last_emit_at) = throttle.last_emit_at else {
            throttle.last_emit_at = Some(now);
            return CallbackThrottleAction::Emit;
        };

        let elapsed = now.saturating_duration_since(last_emit_at);
        if elapsed >= CALLBACK_THROTTLE_INTERVAL {
            throttle.last_emit_at = Some(now);
            CallbackThrottleAction::Emit
        } else {
            throttle.trailing_scheduled = true;
            CallbackThrottleAction::Schedule {
                delay: CALLBACK_THROTTLE_INTERVAL
                    .checked_sub(elapsed)
                    .expect("elapsed must be less than the callback throttle interval"),
                generation: throttle.generation,
            }
        }
    }

    fn schedule_progress_callback(&self, delay: Duration, generation: u64) {
        let inner = Arc::downgrade(&self.inner);
        Self::schedule_callback(delay, move || {
            if let Some(inner) = inner.upgrade() {
                Self { inner }.emit_scheduled_progress_callback(generation);
            }
        });
    }

    fn schedule_speed_callback(&self, delay: Duration, generation: u64) {
        let inner = Arc::downgrade(&self.inner);
        Self::schedule_callback(delay, move || {
            if let Some(inner) = inner.upgrade() {
                Self { inner }.emit_scheduled_speed_callback(generation);
            }
        });
    }

    fn schedule_callback(callback_delay: Duration, callback: impl FnOnce() + Send + 'static) {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let _task = handle.spawn(async move {
                tokio::time::sleep(callback_delay).await;
                callback();
            });
        } else {
            let _handle = thread::spawn(move || {
                thread::sleep(callback_delay);
                callback();
            });
        }
    }

    fn emit_scheduled_progress_callback(&self, generation: u64) {
        {
            let mut throttle = self
                .inner
                .progress_throttle
                .lock()
                .expect("download progress callback throttle mutex poisoned");
            if throttle.generation != generation {
                return;
            }
            throttle.trailing_scheduled = false;
            throttle.last_emit_at = Some(Instant::now());
        }
        self.emit_progress_callback();
    }

    fn emit_scheduled_speed_callback(&self, generation: u64) {
        {
            let mut throttle = self
                .inner
                .speed_throttle
                .lock()
                .expect("download speed callback throttle mutex poisoned");
            if throttle.generation != generation {
                return;
            }
            throttle.trailing_scheduled = false;
            throttle.last_emit_at = Some(Instant::now());
        }
        self.emit_speed_callback();
    }

    fn emit_progress_callback(&self) {
        let callback = self
            .inner
            .progress_callback
            .lock()
            .expect("download callback mutex poisoned")
            .clone();
        if let Some(callback) = callback {
            let snapshot = self.snapshot();
            let _ = catch_unwind(AssertUnwindSafe(|| callback(snapshot)));
        }
    }

    fn emit_speed_callback(&self) {
        let callback = self
            .inner
            .speed_callback
            .lock()
            .expect("download speed callback mutex poisoned")
            .clone();
        if let Some(callback) = callback {
            let snapshot = self.speed_snapshot();
            let _ = catch_unwind(AssertUnwindSafe(|| callback(snapshot)));
        }
    }

    fn reset_speed_window(&self) {
        let now = Instant::now();
        let mut speed = self
            .inner
            .speed
            .lock()
            .expect("download speed mutex poisoned");
        speed.reset_window(now);
    }

    fn update_speed_metadata(&self, content_len: u64, completed_bytes: u64) {
        {
            let mut speed = self
                .inner
                .speed
                .lock()
                .expect("download speed mutex poisoned");
            speed.content_len = content_len;
            speed.received_bytes = speed.received_bytes.max(completed_bytes);
        }
        self.notify_speed();
    }
}

fn u64_to_f64(value: u64) -> f64 {
    let high = u32::try_from(value >> 32).expect("high u64 word must fit in u32");
    let low = u32::try_from(value & u64::from(u32::MAX)).expect("low u64 word must fit in u32");
    f64::from(high) * 4_294_967_296.0 + f64::from(low)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wait_for_len<T>(values: &Mutex<Vec<T>>, len: usize) {
        let deadline = Instant::now() + Duration::from_secs(1);
        while Instant::now() < deadline {
            if values.lock().unwrap().len() >= len {
                return;
            }
            thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn lifecycle_reports_transitional_pause_and_cancel_phases() {
        let state = SharedState::new();

        assert_eq!(state.snapshot().phase, DownloadPhase::Created);
        state.request_start();
        assert_eq!(state.snapshot().phase, DownloadPhase::Starting);
        state.mark_allocating();
        assert_eq!(state.snapshot().phase, DownloadPhase::Allocating);
        state.mark_running();
        assert_eq!(state.snapshot().phase, DownloadPhase::Running);
        state.request_pause();
        assert_eq!(state.snapshot().phase, DownloadPhase::Pausing);
        state.mark_paused();
        assert_eq!(state.snapshot().phase, DownloadPhase::Paused);
        state.request_start();
        assert_eq!(state.snapshot().phase, DownloadPhase::Starting);
        state.mark_running();
        assert_eq!(state.snapshot().phase, DownloadPhase::Running);
        state.request_cancel();
        assert_eq!(state.snapshot().phase, DownloadPhase::Cancelling);
        state.mark_cancelled();
        assert_eq!(state.snapshot().phase, DownloadPhase::Cancelled);
    }

    #[test]
    fn lifecycle_reports_verifying_before_completion() {
        let state = SharedState::new();

        state.request_start();
        state.mark_allocating();
        state.mark_running();
        state.mark_verifying();
        assert_eq!(state.snapshot().phase, DownloadPhase::Verifying);
        state.mark_completed();

        assert_eq!(state.snapshot().phase, DownloadPhase::Completed);
    }

    #[test]
    fn lifecycle_pauses_and_cancels_during_startup_phases() {
        let state = SharedState::new();

        state.request_start();
        state.request_pause();
        assert_eq!(state.snapshot().phase, DownloadPhase::Pausing);
        state.mark_paused();
        assert_eq!(state.snapshot().phase, DownloadPhase::Paused);

        state.request_start();
        state.mark_allocating();
        state.request_cancel();
        assert_eq!(state.snapshot().phase, DownloadPhase::Cancelling);
        state.mark_cancelled();
        assert_eq!(state.snapshot().phase, DownloadPhase::Cancelled);
    }

    #[test]
    fn lifecycle_keeps_terminal_states_stable_for_late_events() {
        let state = SharedState::new();

        state.request_start();
        state.mark_allocating();
        state.mark_running();
        state.mark_verifying();
        state.mark_completed();
        state.request_pause();
        state.request_cancel();

        assert_eq!(state.snapshot().phase, DownloadPhase::Completed);
    }

    #[test]
    fn progress_callback_receives_current_and_latest_changed_snapshots() {
        let state = SharedState::new();
        let phases = Arc::new(Mutex::new(Vec::new()));
        let callback_phases = Arc::clone(&phases);

        state.set_progress_callback(Some(Arc::new(move |snapshot| {
            callback_phases.lock().unwrap().push(snapshot.phase);
        })));
        state.request_start();
        state.mark_running();
        state.mark_completed();
        wait_for_len(&phases, 2);
        state.set_progress_callback(None);
        state.mark_failed(&TakanawaError::InvalidConfig("ignored".to_owned()));

        assert_eq!(
            *phases.lock().unwrap(),
            vec![DownloadPhase::Created, DownloadPhase::Completed]
        );
    }

    #[test]
    fn speed_callback_receives_body_byte_samples() {
        let state = SharedState::new();
        let samples = Arc::new(Mutex::new(Vec::new()));
        let callback_samples = Arc::clone(&samples);

        state.set_speed_callback(Some(Arc::new(move |snapshot| {
            callback_samples
                .lock()
                .unwrap()
                .push((snapshot.received_bytes, snapshot.interval_bytes));
        })));
        state.record_body_bytes(10);
        state.record_body_bytes(15);
        wait_for_len(&samples, 2);
        state.set_speed_callback(None);
        state.record_body_bytes(20);

        assert_eq!(*samples.lock().unwrap(), vec![(0, 0), (25, 25)]);
    }

    #[test]
    fn speed_snapshot_smooths_samples_over_last_second() {
        let state = SharedState::new();
        let started_at = Instant::now();

        state.record_body_bytes_at(100, started_at);
        state.record_body_bytes_at(100, started_at + Duration::from_millis(500));
        let snapshot = state.speed_snapshot_at(started_at + Duration::from_millis(500));

        assert_eq!(snapshot.received_bytes, 200);
        assert_eq!(snapshot.interval_bytes, 200);
        assert_eq!(snapshot.elapsed_millis, 500);
        assert!((snapshot.bytes_per_second - 400.0).abs() < f64::EPSILON);

        state.record_body_bytes_at(50, started_at + Duration::from_millis(1_200));
        let snapshot = state.speed_snapshot_at(started_at + Duration::from_millis(1_200));

        assert_eq!(snapshot.received_bytes, 250);
        assert_eq!(snapshot.interval_bytes, 150);
        assert_eq!(snapshot.elapsed_millis, 1_000);
        assert!((snapshot.bytes_per_second - 150.0).abs() < f64::EPSILON);
    }
}

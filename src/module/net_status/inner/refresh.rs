//! Bounded, serialized Windows sampling. Workers only return values; the source
//! coordinator alone can commit them after validating lifecycle and cancellation.
use std::cell::Cell;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

use super::facade::NetworkObservationOwner;
use crate::error::{ErrorKind, ErrorStage, NetError};
use crate::net_status::NetworkRefreshResult;

fn error(kind: ErrorKind) -> NetError {
    NetError::from(kind).with_stage(ErrorStage::NetworkMonitor)
}

thread_local! {
    static IS_SAMPLER_THREAD: Cell<bool> = const { Cell::new(false) };
}

pub(crate) fn refresh_blocking(
    owner: Arc<NetworkObservationOwner>,
    timeout: Duration,
) -> Result<NetworkRefreshResult, NetError> {
    wait_for_refresh(refresh(owner, timeout))
}

pub(crate) fn refresh_reachability_blocking(
    owner: Arc<NetworkObservationOwner>,
    timeout: Duration,
) -> Result<NetworkRefreshResult, NetError> {
    wait_for_refresh(refresh_reachability(owner, timeout))
}

// This driver is private to refresh: its futures use SDK-owned timers and
// sampler threads, never the blocked caller's executor. Do not expose it as a
// general-purpose block_on, or use a Tokio/futures executor's nested block_on.
fn wait_for_refresh<T>(future: impl Future<Output = Result<T, NetError>>) -> Result<T, NetError> {
    if crate::common::common_engine::is_engine_runtime_thread() || IS_SAMPLER_THREAD.with(Cell::get)
    {
        return Err(NetError::input(
            "network_status.refresh_blocking.context",
            "blocking refresh cannot run on an SDK runtime or sampling thread",
        )
        .with_stage(ErrorStage::NetworkMonitor));
    }

    struct RefreshWake {
        thread: std::thread::Thread,
        notified: AtomicBool,
    }
    impl Wake for RefreshWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.notified.store(true, Ordering::Release);
            self.thread.unpark();
        }
    }

    let signal = Arc::new(RefreshWake {
        thread: std::thread::current(),
        notified: AtomicBool::new(false),
    });
    let waker = Waker::from(signal.clone());
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(result) => return result,
            Poll::Pending => {
                // Keep a separate wake flag: a spurious unpark or a nested
                // wait during poll must not consume this future's notification.
                while !signal.notified.swap(false, Ordering::AcqRel) {
                    std::thread::park();
                }
            }
        }
    }
}

pub(crate) async fn refresh(
    owner: Arc<NetworkObservationOwner>,
    timeout: Duration,
) -> Result<NetworkRefreshResult, NetError> {
    refresh_reachability(owner, timeout).await
}

pub(crate) async fn refresh_reachability(
    owner: Arc<NetworkObservationOwner>,
    timeout: Duration,
) -> Result<NetworkRefreshResult, NetError> {
    let deadline = deadline(timeout)?;
    let target = owner
        .refresh_target()
        .map_err(|error| error.with_stage(ErrorStage::NetworkMonitor))?;
    #[cfg(target_os = "windows")]
    {
        let sampler = owner.context().windows_reachability_sampler()?;
        refresh_with_sampler(owner, target, sampler, deadline).await
    }
    #[cfg(not(target_os = "windows"))]
    {
        let _ = (deadline, target);
        Err(error(ErrorKind::Unsupported))
    }
}

fn deadline(timeout: Duration) -> Result<Instant, NetError> {
    if timeout.is_zero() {
        return Err(error(ErrorKind::InvalidInput));
    }
    Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| error(ErrorKind::InvalidInput))
}

#[cfg(any(target_os = "windows", test))]
use crate::common::retirement::ThreadRegistry;
#[cfg(any(target_os = "windows", test))]
use crate::net_status::{NetworkIpStackSource, NetworkNameSource, NetworkStatus};
#[cfg(any(target_os = "windows", test))]
use std::sync::{atomic::AtomicU64, mpsc, Mutex};
#[cfg(any(target_os = "windows", test))]
use std::time::SystemTime;

#[cfg(any(target_os = "windows", test))]
pub(super) struct Cancellation {
    pub(super) flag: Arc<AtomicBool>,
}
#[cfg(any(target_os = "windows", test))]
impl Cancellation {
    pub(super) fn new() -> Self {
        Self {
            flag: Arc::new(AtomicBool::new(false)),
        }
    }
    pub(super) fn check(&self, deadline: Instant) -> Result<(), NetError> {
        check(deadline, &self.flag)
    }
}
#[cfg(any(target_os = "windows", test))]
impl Drop for Cancellation {
    fn drop(&mut self) {
        self.flag.store(true, Ordering::Release);
    }
}
#[cfg(any(target_os = "windows", test))]
fn check(deadline: Instant, cancelled: &AtomicBool) -> Result<(), NetError> {
    if cancelled.load(Ordering::Acquire) {
        return Err(error(ErrorKind::Cancelled));
    }
    if Instant::now() >= deadline {
        return Err(error(ErrorKind::TimedOut));
    }
    Ok(())
}

#[cfg(any(target_os = "windows", test))]
pub(super) struct SampleObservation {
    pub(super) reachability: NetworkStatus,
}
#[cfg(any(target_os = "windows", test))]
pub(super) struct CompletedSample {
    pub(super) id: u64,
    pub(super) started_at: SystemTime,
    pub(super) sampled_at: SystemTime,
    pub(super) observation: SampleObservation,
}
#[cfg(any(target_os = "windows", test))]
type Provider = dyn Fn(Instant, &AtomicBool) -> Result<SampleObservation, NetError> + Send + Sync;
#[cfg(any(target_os = "windows", test))]
struct Request {
    deadline: Instant,
    cancelled: Arc<AtomicBool>,
    reply: tokio::sync::oneshot::Sender<Result<CompletedSample, NetError>>,
    _permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

/// One lazy ordinary thread per sampling lane, with a bounded waiting queue.
/// Explicit and background reachability share this lane; a stuck native call
/// retains its slot and timed-out callers cannot spawn replacements. The lane
/// has no access to the separate network-name provider.
#[cfg(any(target_os = "windows", test))]
pub(crate) struct WindowsSampler {
    state: Arc<Mutex<SamplerState>>,
    threads: Arc<ThreadRegistry>,
    provider: Arc<Provider>,
    admission: Arc<tokio::sync::Semaphore>,
    next_id: Arc<AtomicU64>,
}
#[cfg(any(target_os = "windows", test))]
#[derive(Default)]
struct SamplerState {
    closed: bool,
    sender: Option<Result<mpsc::SyncSender<Request>, NetError>>,
    running: Option<Arc<AtomicBool>>,
}
#[cfg(any(target_os = "windows", test))]
impl WindowsSampler {
    pub(crate) fn new(threads: Arc<ThreadRegistry>) -> Arc<Self> {
        Arc::new(Self::with_registry(
            Self::provider(),
            Arc::new(AtomicU64::new(0)),
            threads,
        ))
    }
    fn provider() -> Arc<Provider> {
        Arc::new(move |deadline, cancelled| {
            #[cfg(target_os = "windows")]
            {
                let value = super::platform::windows::sample_reachability(deadline, cancelled)?;
                Ok(SampleObservation {
                    reachability: value.reachability,
                })
            }
            #[cfg(not(target_os = "windows"))]
            {
                let _ = (deadline, cancelled);
                Err(error(ErrorKind::Unsupported))
            }
        })
    }
    #[cfg(test)]
    pub(super) fn with_provider(provider: Arc<Provider>) -> Self {
        Self::with_counter(provider, Arc::new(AtomicU64::new(0)))
    }
    #[cfg(test)]
    fn with_counter(provider: Arc<Provider>, next_id: Arc<AtomicU64>) -> Self {
        Self::with_registry(provider, next_id, Arc::new(ThreadRegistry::default()))
    }
    fn with_registry(
        provider: Arc<Provider>,
        next_id: Arc<AtomicU64>,
        threads: Arc<ThreadRegistry>,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(SamplerState::default())),
            threads,
            provider,
            admission: Arc::new(tokio::sync::Semaphore::new(16)),
            next_id,
        }
    }
    fn start_worker(&self) -> Result<mpsc::SyncSender<Request>, NetError> {
        // One additional queue slot is reserved for the background monitor.
        let (sender, receiver) = mpsc::sync_channel::<Request>(17);
        let provider = self.provider.clone();
        let next_id = self.next_id.clone();
        let state = self.state.clone();
        self.threads
            .spawn("open-net-windows-sampler", move || {
                IS_SAMPLER_THREAD.with(|role| role.set(true));
                while let Ok(request) = receiver.recv() {
                    if request.reply.is_closed() {
                        continue;
                    }
                    let closed = {
                        let mut state = match state.lock() {
                            Ok(state) => state,
                            Err(poisoned) => poisoned.into_inner(),
                        };
                        if state.closed {
                            true
                        } else {
                            state.running = Some(request.cancelled.clone());
                            false
                        }
                    };
                    if closed {
                        let _ = request.reply.send(Err(error(ErrorKind::Closed)));
                        continue;
                    }
                    let result = (|| {
                        check(request.deadline, &request.cancelled)?;
                        let id = next_id
                            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                                value.checked_add(1)
                            })
                            .map(|previous| previous + 1)
                            .map_err(|_| error(ErrorKind::ResourceExhausted))?;
                        let started_at = SystemTime::now();
                        let observation =
                            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                provider(request.deadline, &request.cancelled)
                            }))
                            .map_err(|_| error(ErrorKind::Internal))??;
                        let sampled_at = SystemTime::now();
                        check(request.deadline, &request.cancelled)?;
                        Ok(CompletedSample {
                            id,
                            started_at,
                            sampled_at,
                            observation,
                        })
                    })();
                    {
                        let mut state = match state.lock() {
                            Ok(state) => state,
                            Err(poisoned) => poisoned.into_inner(),
                        };
                        state.running = None;
                    }
                    let _ = request.reply.send(result);
                }
            })
            .map_err(|cause| {
                NetError::with_source(ErrorKind::RuntimeUnavailable, cause)
                    .with_stage(ErrorStage::NetworkMonitor)
            })?;
        Ok(sender)
    }
    pub(crate) fn close(&self) {
        self.admission.close();
        let sender = {
            let mut state = match self.state.lock() {
                Ok(state) => state,
                Err(poisoned) => poisoned.into_inner(),
            };
            state.closed = true;
            if let Some(cancelled) = &state.running {
                cancelled.store(true, Ordering::Release);
            }
            state.sender.take()
        };
        // Dropping the last sender releases an idle worker. In-flight native
        // work owns its cancellation flag and remains covered by engine join.
        drop(sender);
    }
    pub(super) async fn sample(
        &self,
        deadline: Instant,
        cancelled: Arc<AtomicBool>,
    ) -> Result<CompletedSample, NetError> {
        let permit = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|cause| {
                error(match cause {
                    tokio::sync::TryAcquireError::Closed => ErrorKind::Closed,
                    tokio::sync::TryAcquireError::NoPermits => ErrorKind::QueueFull,
                })
            })?;
        self.sample_inner(deadline, cancelled, Some(permit)).await
    }
    #[cfg(any(target_os = "windows", test))]
    pub(super) async fn sample_background(
        &self,
        deadline: Instant,
        cancelled: Arc<AtomicBool>,
    ) -> Result<CompletedSample, NetError> {
        self.sample_inner(deadline, cancelled, None).await
    }
    async fn sample_inner(
        &self,
        deadline: Instant,
        cancelled: Arc<AtomicBool>,
        permit: Option<tokio::sync::OwnedSemaphorePermit>,
    ) -> Result<CompletedSample, NetError> {
        check(deadline, &cancelled)?;
        let (reply, result) = tokio::sync::oneshot::channel();
        let submitted = {
            let mut state = self.state.lock().map_err(NetError::from_poison)?;
            if state.closed {
                return Err(error(ErrorKind::Closed));
            }
            if state.sender.is_none() {
                state.sender = Some(self.start_worker());
            }
            let sender = state
                .sender
                .as_ref()
                .ok_or_else(|| error(ErrorKind::Internal))?
                .as_ref()
                .map_err(Clone::clone)?;
            sender.try_send(Request {
                deadline,
                cancelled,
                reply,
                _permit: permit,
            })
        };
        submitted.map_err(|cause| match cause {
            mpsc::TrySendError::Full(_) => error(ErrorKind::QueueFull),
            mpsc::TrySendError::Disconnected(_) => error(ErrorKind::RuntimeUnavailable),
        })?;
        result
            .await
            .map_err(|_| error(ErrorKind::RuntimeUnavailable))?
    }
}

#[cfg(any(target_os = "windows", test))]
impl Drop for WindowsSampler {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(any(target_os = "windows", test))]
async fn refresh_with_sampler(
    owner: Arc<NetworkObservationOwner>,
    mut target: super::network_status_snapshot::NetworkRefreshTarget,
    sampler: Arc<WindowsSampler>,
    deadline: Instant,
) -> Result<NetworkRefreshResult, NetError> {
    let cancel = Cancellation::new();
    let engine = owner.context().common_engine()?;
    // Construct the timer on the library runtime, also allowing an executor
    // other than Tokio to poll this public future.
    let sampling = {
        let handle = engine.runtime_handle();
        let _entered = handle.enter();
        tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            sampler.sample(deadline, cancel.flag.clone()),
        )
    };
    let sampled = tokio::select! {
        biased;
        _ = target.invalidated() => {
            return Err(error(if matches!(owner.snapshot()?.state, crate::net_status::MonitorState::Closed) {
                ErrorKind::Closed
            } else { ErrorKind::Cancelled }));
        }
        result = sampling => result.map_err(|_| error(ErrorKind::TimedOut))??,
    };
    let observation = sampled.observation;
    let snapshot = owner
        .commit_refresh(
            target,
            sampled.id,
            observation.reachability,
            None,
            sampled.sampled_at,
            || cancel.check(deadline),
        )
        .map_err(|error| error.with_stage(ErrorStage::NetworkMonitor))?;
    Ok(NetworkRefreshResult {
        snapshot,
        sampled_reachability: observation.reachability,
        sample_id: sampled.id,
        sample_started_at: sampled.started_at,
        sampled_at: sampled.sampled_at,
        network_name_source: NetworkNameSource::NotCollected,
        network_name_diagnostics: Vec::new(),
        ip_stack_source: NetworkIpStackSource::BackgroundObservation,
    })
}

#[cfg(test)]
#[path = "refresh_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "refresh_worker_tests.rs"]
mod worker_tests;

#[cfg(test)]
#[path = "refresh_reachability_tests.rs"]
mod reachability_tests;

#[cfg(test)]
#[path = "refresh_blocking_tests.rs"]
mod blocking_tests;

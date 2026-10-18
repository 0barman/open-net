//! Shared ownership and independent supervision for lazy HTTP response streams.
use super::*;
use crate::api::http::{HttpByteStream, TimeoutPhase};
use bytes::Bytes;
use futures_util::task::AtomicWaker;
use futures_util::Stream;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

pub(super) use crate::module::http::http_timeout::TimeoutBudget as StreamBudget;
pub(super) type StreamSupervisor = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// Opening futures and delivered streams use the same lifecycle owner. Capacity
/// is returned before an observer can reenter the client.
pub(super) struct StreamOwner {
    pub(super) lease: Option<StreamRegistrationLease>,
    pub(super) supervisor_slot: Option<mpsc::OwnedPermit<StreamSupervisor>>,
    pub(super) lifecycle: Option<RetryObserverLifecycle>,
}
impl StreamOwner {
    pub(super) fn new(policy: &crate::api::http::RetryPolicy, start: Instant) -> Self {
        Self {
            lease: None,
            supervisor_slot: None,
            lifecycle: Some(RetryObserverLifecycle::new(policy.observer(), start)),
        }
    }
    pub(super) fn complete(
        &mut self,
        result: Result<http::StatusCode, &NetError>,
        exhausted: bool,
    ) {
        drop(self.supervisor_slot.take());
        drop(self.lease.take());
        if let Some(mut lifecycle) = self.lifecycle.take() {
            lifecycle.complete_summary(result, exhausted);
        }
    }
}
impl Drop for StreamOwner {
    fn drop(&mut self) {
        let error = NetError::from(ErrorKind::Cancelled).with_stage(ErrorStage::Receive);
        self.complete(Err(&error), false);
    }
}

struct BodyState {
    body: Option<HttpByteStream>,
    owner: Option<StreamOwner>,
    base_budget: StreamBudget,
    budget: StreamBudget,
    idle_timeout: Option<Duration>,
    finished: bool,
    terminal_error: Option<NetError>,
}

struct SharedBody {
    state: Mutex<BodyState>,
    control: Arc<CancellationSignal>,
    progress: Notify,
    consumer: AtomicWaker,
    request_id: HttpRequestId,
    attempt: u32,
    status: http::StatusCode,
    exhausted: bool,
}

/// All potentially reentrant destruction and callbacks happen after unlocking.
struct Cleanup {
    body: Option<HttpByteStream>,
    owner: Option<StreamOwner>,
    error: Option<NetError>,
    status: http::StatusCode,
    exhausted: bool,
}
impl Cleanup {
    fn run(self) {
        let Self {
            body,
            owner,
            error,
            status,
            exhausted,
        } = self;
        drop(body);
        if let Some(mut owner) = owner {
            owner.complete(error.as_ref().map_or(Ok(status), Err), exhausted);
        }
    }
}

impl SharedBody {
    fn with_state<T>(&self, operation: impl FnOnce(&mut BodyState) -> T) -> T {
        // No user code runs in this critical section. Recovering a poisoned
        // mutex still allows a future cleanup to return its owned resources.
        let (result, poisoned) = {
            let (mut guard, poisoned) = match self.state.lock() {
                Ok(guard) => (guard, false),
                Err(error) => (error.into_inner(), true),
            };
            (operation(&mut guard), poisoned)
        };
        if poisoned {
            crate::log_e!(crate::LogType::HTTP; "http.stream_body.with_state", "error|operation", "state_poisoned", "provided");
        }
        result
    }

    fn annotate(&self, error: NetError) -> NetError {
        error
            .with_http_request_id(self.request_id)
            .with_http_attempt(self.attempt)
    }

    fn cancelled(&self) -> NetError {
        self.annotate(NetError::from(ErrorKind::Cancelled).with_stage(ErrorStage::Receive))
    }

    fn boundary_error(&self, state: &BodyState, now: Instant) -> Option<NetError> {
        // Cancellation visible at the arbitration point wins every deadline.
        if self.control.is_cancelled() {
            Some(self.cancelled())
        } else {
            state
                .budget
                .expired(now, ErrorStage::Receive)
                .map(|error| self.annotate(error))
        }
    }

    fn finish_locked(&self, state: &mut BodyState, error: Option<NetError>) -> Option<Cleanup> {
        if state.finished {
            return None;
        }
        state.finished = true;
        state.terminal_error = error.clone();
        Some(Cleanup {
            body: state.body.take(),
            owner: state.owner.take(),
            error,
            status: self.status,
            exhausted: self.exhausted,
        })
    }

    fn clean_and_wake(&self, cleanup: Option<Cleanup>) {
        if let Some(cleanup) = cleanup {
            cleanup.run();
            self.consumer.wake();
            self.progress.notify_one();
        }
    }

    fn cancel(&self) {
        let cleanup = self.with_state(|state| self.finish_locked(state, Some(self.cancelled())));
        self.clean_and_wake(cleanup);
    }
}

/// A supervisor dropped by the runtime also releases its network resources,
/// including when it is dropped before its first poll.
struct SupervisorGuard(Arc<SharedBody>);
impl Drop for SupervisorGuard {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

pub(super) struct FusedHttpBody {
    shared: Arc<SharedBody>,
}
impl FusedHttpBody {
    /// Keep the reservation with body ownership through every fallible handoff.
    /// Completion drops the slot before notifying a potentially reentrant observer.
    pub(super) fn publish_supervisor(&self, supervisor: StreamSupervisor) -> Result<(), NetError> {
        let slot = self
            .shared
            .with_state(|state| {
                state
                    .owner
                    .as_mut()
                    .and_then(|owner| owner.supervisor_slot.take())
            })
            .ok_or_else(|| NetError::from(ErrorKind::Internal).with_stage(ErrorStage::Dispatch))?;
        drop(slot.send(supervisor));
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        body: HttpByteStream,
        control: Arc<CancellationSignal>,
        budget: StreamBudget,
        owner: StreamOwner,
        status: http::StatusCode,
        exhausted: bool,
        request_id: HttpRequestId,
        attempt: u32,
        body_start: Instant,
        idle_timeout: Option<Duration>,
    ) -> Result<(Self, StreamSupervisor), NetError> {
        let active_budget = budget.with_phase(body_start, idle_timeout, TimeoutPhase::IdleRead)?;
        let shared = Arc::new(SharedBody {
            state: Mutex::new(BodyState {
                body: Some(body),
                owner: Some(owner),
                base_budget: budget,
                budget: active_budget,
                idle_timeout,
                finished: false,
                terminal_error: None,
            }),
            control,
            progress: Notify::new(),
            consumer: AtomicWaker::new(),
            request_id,
            attempt,
            status,
            exhausted,
        });
        let guard = SupervisorGuard(Arc::clone(&shared));
        let supervisor = Box::pin(async move {
            let shared = &guard.0;
            loop {
                // notify_one retains a permit when progress arrives between the
                // snapshot and this wait. Every wake rechecks the current budget,
                // so an old idle timer cannot terminate a newly advanced stream.
                let progress = shared.progress.notified();
                let (finished, budget, cleanup) = shared.with_state(|state| {
                    let cleanup = if state.finished {
                        None
                    } else {
                        shared
                            .boundary_error(state, Instant::now())
                            .and_then(|error| shared.finish_locked(state, Some(error)))
                    };
                    (state.finished, state.budget, cleanup)
                });
                shared.clean_and_wake(cleanup);
                if finished {
                    return;
                }
                let mut timer = budget.timer();
                tokio::select! {
                    biased;
                    _ = shared.control.wait() => {},
                    _ = progress => {},
                    _ = wait_budget(&mut timer) => {},
                }
            }
        });
        Ok((Self { shared }, supervisor))
    }
}

impl Stream for FusedHttpBody {
    type Item = Result<Bytes, NetError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let shared = &self.get_mut().shared;
        shared.consumer.register(cx.waker());
        let (mut body, terminal, cleanup) = shared.with_state(|state| {
            let cleanup = if state.finished {
                None
            } else {
                shared
                    .boundary_error(state, Instant::now())
                    .and_then(|error| shared.finish_locked(state, Some(error)))
            };
            if state.finished {
                (None, state.terminal_error.take(), cleanup)
            } else {
                (state.body.take(), None, cleanup)
            }
        });
        shared.clean_and_wake(cleanup);
        let result = match body.as_mut() {
            Some(body) => body.as_mut().poll_next(cx),
            None => return Poll::Ready(terminal.map(Err)),
        };
        // Transport polling is outside the mutex. A concurrent supervisor may
        // commit a terminal state while poll is running; discard any late bytes.
        let (finished, terminal, cleanup, progressed) = shared.with_state(|state| {
            let mut cleanup = None;
            let mut progressed = false;
            if !state.finished {
                if let Some(error) = shared.boundary_error(state, Instant::now()) {
                    cleanup = shared.finish_locked(state, Some(error));
                } else {
                    match &result {
                        Poll::Ready(Some(Err(error))) => {
                            cleanup =
                                shared.finish_locked(state, Some(shared.annotate(error.clone())));
                        }
                        Poll::Ready(None) => cleanup = shared.finish_locked(state, None),
                        Poll::Ready(Some(Ok(bytes))) if !bytes.is_empty() => {
                            match state.base_budget.with_phase(
                                Instant::now(),
                                state.idle_timeout,
                                TimeoutPhase::IdleRead,
                            ) {
                                Ok(budget) => {
                                    state.budget = budget;
                                    progressed = true;
                                }
                                Err(error) => {
                                    cleanup =
                                        shared.finish_locked(state, Some(shared.annotate(error)))
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
            if !state.finished {
                state.body = body.take();
            }
            (
                state.finished,
                state.terminal_error.take(),
                cleanup,
                progressed,
            )
        });
        // Destroy the checked-out body before completing the observer lifecycle.
        drop(body);
        shared.clean_and_wake(cleanup);
        if progressed {
            shared.progress.notify_one();
        }
        if finished {
            Poll::Ready(terminal.map(Err))
        } else {
            result
        }
    }
}
impl Drop for FusedHttpBody {
    fn drop(&mut self) {
        self.shared.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{check, paused_worker};
    use super::*;
    use std::collections::VecDeque;
    use std::sync::atomic::AtomicUsize;

    type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

    struct ProbeBody {
        items: VecDeque<Result<Bytes, NetError>>,
        eof: bool,
        polls: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
    }
    impl Stream for ProbeBody {
        type Item = Result<Bytes, NetError>;
        fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let this = self.get_mut();
            this.polls.fetch_add(1, Ordering::SeqCst);
            match this.items.pop_front() {
                Some(item) => Poll::Ready(Some(item)),
                None if this.eof => Poll::Ready(None),
                None => Poll::Pending,
            }
        }
    }
    impl Drop for ProbeBody {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct Fixture {
        worker: Arc<HttpClientInner>,
        stream: FusedHttpBody,
        supervisor: StreamSupervisor,
        control: Arc<CancellationSignal>,
        request_id: HttpRequestId,
        polls: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
        completed: Arc<AtomicUsize>,
        clean_at_observer: Arc<AtomicBool>,
    }
    fn fixture(
        items: Vec<Result<Bytes, NetError>>,
        eof: bool,
        idle: Option<Duration>,
        total: Option<Duration>,
    ) -> Result<Fixture, Box<dyn std::error::Error + Send + Sync>> {
        let (worker, _receiver) = paused_worker()?;
        let worker = Arc::new(worker);
        let polls = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let completed = Arc::new(AtomicUsize::new(0));
        let clean_at_observer = Arc::new(AtomicBool::new(false));
        let callback_worker = Arc::clone(&worker);
        let callback_completed = Arc::clone(&completed);
        let callback_clean = Arc::clone(&clean_at_observer);
        let callback_drops = Arc::clone(&drops);
        let policy = crate::api::http::RetryPolicy::builder()
            .observer(move |event: &RetryEvent| {
                if matches!(event, RetryEvent::Completed(_)) {
                    callback_completed.fetch_add(1, Ordering::SeqCst);
                    callback_clean.store(
                        callback_worker.registry.is_empty().is_ok_and(|empty| empty)
                            && callback_worker.in_flight.available_permits() == 128
                            && callback_drops.load(Ordering::SeqCst) == 1,
                        Ordering::SeqCst,
                    );
                }
            })
            .build()?;
        let (registered, registration) = worker.register_request(None)?;
        registration.commit();
        let control = registered.control;
        let start = Instant::now();
        let mut owner = StreamOwner::new(&policy, start);
        owner.lease = Some(StreamRegistrationLease {
            registry: worker.registry(),
            request_id: registered.id,
            control: Arc::clone(&control),
            permit: Some(Arc::new(worker.try_acquire_permit()?)),
        });
        let body = ProbeBody {
            items: items.into(),
            eof,
            polls: Arc::clone(&polls),
            drops: Arc::clone(&drops),
        };
        let (stream, supervisor) = FusedHttpBody::new(
            Box::pin(body),
            Arc::clone(&control),
            StreamBudget::total(start, total)?,
            owner,
            http::StatusCode::OK,
            false,
            registered.id,
            2,
            start,
            idle,
        )?;
        Ok(Fixture {
            worker,
            stream,
            supervisor,
            control,
            request_id: registered.id,
            polls,
            drops,
            completed,
            clean_at_observer,
        })
    }

    #[tokio::test(start_paused = true)]
    async fn supervisor_handoff_and_prepublication_cancel_release_before_observer() -> TestResult {
        for publish in [false, true] {
            let (worker, _requests) = paused_worker()?;
            let worker = Arc::new(worker);
            let (supervisor_tx, mut supervisor_rx) = mpsc::channel::<StreamSupervisor>(1);
            let drops = Arc::new(AtomicUsize::new(0));
            let completed = Arc::new(AtomicUsize::new(0));
            let clean_at_observer = Arc::new(AtomicBool::new(false));
            let callback_worker = Arc::clone(&worker);
            let callback_supervisor_tx = supervisor_tx.clone();
            let callback_drops = Arc::clone(&drops);
            let callback_completed = Arc::clone(&completed);
            let callback_clean = Arc::clone(&clean_at_observer);
            let policy = crate::api::http::RetryPolicy::builder()
                .observer(move |event: &RetryEvent| {
                    if matches!(event, RetryEvent::Completed(_)) {
                        callback_completed.fetch_add(1, Ordering::SeqCst);
                        callback_clean.store(
                            callback_worker.registry.is_empty().is_ok_and(|empty| empty)
                                && callback_worker.in_flight.available_permits() == 128
                                && callback_supervisor_tx.try_reserve().is_ok()
                                && callback_drops.load(Ordering::SeqCst) == 1,
                            Ordering::SeqCst,
                        );
                    }
                })
                .build()?;
            let (registered, registration) = worker.register_request(None)?;
            registration.commit();
            let control = registered.control;
            let start = Instant::now();
            let mut owner = StreamOwner::new(&policy, start);
            owner.supervisor_slot = Some(
                supervisor_tx
                    .clone()
                    .try_reserve_owned()
                    .map_err(|_| std::io::Error::other("supervisor reservation failed"))?,
            );
            owner.lease = Some(StreamRegistrationLease {
                registry: worker.registry(),
                request_id: registered.id,
                control: Arc::clone(&control),
                permit: Some(Arc::new(worker.try_acquire_permit()?)),
            });
            let body = ProbeBody {
                items: VecDeque::new(),
                eof: false,
                polls: Arc::new(AtomicUsize::new(0)),
                drops: Arc::clone(&drops),
            };
            let (mut stream, supervisor) = FusedHttpBody::new(
                Box::pin(body),
                Arc::clone(&control),
                StreamBudget::total(start, None)?,
                owner,
                http::StatusCode::OK,
                false,
                registered.id,
                1,
                start,
                None,
            )?;
            check(
                supervisor_tx.capacity() == 0,
                "opening must retain its supervisor slot until handoff or cleanup",
            )?;
            let supervisor = if publish {
                stream.publish_supervisor(supervisor)?;
                supervisor_rx
                    .recv()
                    .await
                    .ok_or_else(|| std::io::Error::other("supervisor was not published"))?
            } else {
                supervisor
            };
            control.cancel();
            tokio::time::timeout(Duration::from_secs(1), supervisor).await?;
            check(
                clean_at_observer.load(Ordering::SeqCst),
                "observer ran before supervisor slot, permit, registry, and body were released",
            )?;
            check(
                completed.load(Ordering::SeqCst) == 1 && supervisor_tx.capacity() == 1,
                "supervisor handoff cleanup was duplicated or retained lane capacity",
            )?;
            check(
                matches!(stream.next().await, Some(Err(error)) if error.kind() == ErrorKind::Cancelled),
                "cancelled handoff lost the terminal stream error",
            )?;
            drop(stream);
            check(
                completed.load(Ordering::SeqCst) == 1 && drops.load(Ordering::SeqCst) == 1,
                "dropping the terminated stream repeated completion or body cleanup",
            )?;
        }
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn unpolled_idle_expiry_releases_resources_and_preserves_one_error() -> TestResult {
        let Fixture {
            worker,
            mut stream,
            supervisor,
            control,
            request_id,
            polls,
            drops,
            completed,
            clean_at_observer,
        } = fixture(vec![], false, Some(Duration::from_secs(5)), None)?;
        let task = tokio::spawn(supervisor);
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(5)).await;
        task.await?;
        check(
            polls.load(Ordering::SeqCst) == 0,
            "supervisor prefetched body",
        )?;
        check(
            drops.load(Ordering::SeqCst) == 1,
            "unpolled transport was not dropped",
        )?;
        check(
            worker.registry.is_empty()? && worker.in_flight.available_permits() == 128,
            "unpolled expiry retained registration or capacity",
        )?;
        check(
            clean_at_observer.load(Ordering::SeqCst),
            "observer ran before cleanup",
        )?;
        // A cancellation after the committed timeout cannot rewrite the result.
        control.cancel();
        check(
            matches!(stream.next().await, Some(Err(error))
            if error.timeout_phase() == Some(TimeoutPhase::IdleRead)
                && error.context().http_request_id == Some(request_id)
                && error.context().http_attempt == Some(2)),
            "timeout lost phase or request context",
        )?;
        check(stream.next().await.is_none(), "timeout error was not fused")?;
        drop(stream);
        check(
            completed.load(Ordering::SeqCst) == 1 && drops.load(Ordering::SeqCst) == 1,
            "termination happened more than once",
        )
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_wins_ready_deadline_and_works_without_any_timeout() -> TestResult {
        for timeout in [None, Some(Duration::from_secs(5))] {
            let Fixture {
                mut stream,
                supervisor,
                control,
                drops,
                ..
            } = fixture(vec![], false, timeout, timeout)?;
            tokio::time::advance(Duration::from_secs(5)).await;
            control.cancel();
            tokio::spawn(supervisor).await?;
            check(
                drops.load(Ordering::SeqCst) == 1,
                "cancellation retained transport",
            )?;
            check(
                matches!(stream.next().await, Some(Err(error)) if error.kind() == ErrorKind::Cancelled),
                "visible cancellation lost to the deadline",
            )?;
            check(
                stream.next().await.is_none(),
                "cancelled stream was not fused",
            )?;
        }
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn only_nonempty_delivery_renews_idle_and_total_still_wins_ties() -> TestResult {
        let Fixture {
            mut stream,
            supervisor,
            drops,
            ..
        } = fixture(
            vec![Ok(Bytes::from_static(b"progress")), Ok(Bytes::new())],
            false,
            Some(Duration::from_secs(5)),
            Some(Duration::from_secs(9)),
        )?;
        let task = tokio::spawn(supervisor);
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(4)).await;
        check(
            matches!(stream.next().await, Some(Ok(bytes)) if bytes == Bytes::from_static(b"progress")),
            "nonempty delivery failed",
        )?;
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;
        check(
            drops.load(Ordering::SeqCst) == 0,
            "stale idle timer terminated progress",
        )?;
        check(
            matches!(stream.next().await, Some(Ok(bytes)) if bytes.is_empty()),
            "empty delivery failed",
        )?;
        let waker = futures_util::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        for _ in 0..3 {
            check(
                Pin::new(&mut stream).poll_next(&mut cx).is_pending(),
                "body was not pending",
            )?;
        }
        tokio::time::advance(Duration::from_secs(3)).await;
        task.await?;
        check(
            matches!(stream.next().await, Some(Err(error))
            if error.timeout_phase() == Some(TimeoutPhase::Total)),
            "total did not win an equal idle deadline",
        )?;
        check(
            stream.next().await.is_none(),
            "terminal stream was not fused",
        )
    }

    #[tokio::test(start_paused = true)]
    async fn empty_chunks_and_pending_polls_do_not_renew_idle() -> TestResult {
        let Fixture {
            mut stream,
            supervisor,
            ..
        } = fixture(
            vec![Ok(Bytes::new())],
            false,
            Some(Duration::from_secs(5)),
            None,
        )?;
        let task = tokio::spawn(supervisor);
        tokio::time::advance(Duration::from_secs(4)).await;
        check(
            matches!(stream.next().await, Some(Ok(bytes)) if bytes.is_empty()),
            "empty chunk failed",
        )?;
        let waker = futures_util::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        check(
            Pin::new(&mut stream).poll_next(&mut cx).is_pending(),
            "body was not pending",
        )?;
        tokio::time::advance(Duration::from_secs(1)).await;
        task.await?;
        check(
            matches!(stream.next().await, Some(Err(error))
            if error.timeout_phase() == Some(TimeoutPhase::IdleRead)),
            "empty chunk or pending poll renewed idle",
        )
    }

    #[tokio::test(start_paused = true)]
    async fn eof_error_drop_and_abandoned_supervisor_release_once() -> TestResult {
        for body_error in [false, true] {
            let items = if body_error {
                vec![Err(
                    NetError::from(ErrorKind::Io).with_stage(ErrorStage::Receive)
                )]
            } else {
                vec![]
            };
            let Fixture {
                mut stream,
                supervisor,
                control,
                completed,
                clean_at_observer,
                ..
            } = fixture(items, true, None, None)?;
            let task = tokio::spawn(supervisor);
            let next = stream.next().await;
            check(
                if body_error {
                    matches!(next, Some(Err(_)))
                } else {
                    next.is_none()
                },
                "unexpected terminal result",
            )?;
            check(
                clean_at_observer.load(Ordering::SeqCst),
                "cleanup followed observer",
            )?;
            control.cancel();
            check(stream.next().await.is_none(), "EOF or error did not fuse")?;
            drop(stream);
            task.await?;
            check(
                completed.load(Ordering::SeqCst) == 1,
                "observer completed more than once",
            )?;
        }
        let Fixture {
            stream,
            supervisor,
            drops,
            completed,
            ..
        } = fixture(vec![], false, None, None)?;
        drop(stream);
        tokio::spawn(supervisor).await?;
        check(
            drops.load(Ordering::SeqCst) == 1 && completed.load(Ordering::SeqCst) == 1,
            "stream drop did not terminate supervision once",
        )?;
        let Fixture {
            mut stream,
            supervisor,
            drops,
            ..
        } = fixture(vec![], false, None, None)?;
        drop(supervisor);
        check(
            drops.load(Ordering::SeqCst) == 1,
            "unpolled supervisor drop retained transport",
        )?;
        check(
            matches!(stream.next().await, Some(Err(error)) if error.kind() == ErrorKind::Cancelled),
            "abandoned supervisor did not signal cancellation",
        )
    }

    #[tokio::test(start_paused = true)]
    async fn ready_chunk_checks_cancellation_after_polling_outside_the_mutex() -> TestResult {
        let Fixture {
            mut stream,
            supervisor,
            control,
            ..
        } = fixture(vec![], false, None, None)?;
        let weak = Arc::downgrade(&stream.shared);
        let unlocked = Arc::new(AtomicBool::new(false));
        let poll_unlocked = Arc::clone(&unlocked);
        let body = futures_util::stream::poll_fn(move |_| {
            if let Some(shared) = weak.upgrade() {
                poll_unlocked.store(shared.state.try_lock().is_ok(), Ordering::SeqCst);
            }
            control.cancel();
            Poll::Ready(Some(Ok(Bytes::from_static(b"late bytes"))))
        });
        let previous = stream
            .shared
            .with_state(|state| state.body.replace(Box::pin(body)));
        drop(previous);
        check(
            matches!(stream.next().await, Some(Err(error)) if error.kind() == ErrorKind::Cancelled),
            "late bytes crossed visible cancellation",
        )?;
        check(
            unlocked.load(Ordering::SeqCst),
            "body poll ran under the shared mutex",
        )?;
        check(
            stream.next().await.is_none(),
            "late ready chunk was retained",
        )?;
        tokio::spawn(supervisor).await?;
        Ok(())
    }
}

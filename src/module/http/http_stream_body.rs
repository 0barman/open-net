//! Private ownership and absolute budgets for the lazy HTTP response stream.
use super::*;
use crate::api::http::HttpByteStream;
use bytes::Bytes;
use futures_util::Stream;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

#[derive(Clone, Copy, Debug)]
pub(super) struct StreamBudget {
    // Only the earliest deadline can win. Total is inserted first to win ties.
    earliest: Option<(Instant, ErrorKind)>,
}
impl StreamBudget {
    pub(super) fn total(start: Instant, duration: Option<Duration>) -> Result<Self, NetError> {
        let earliest = duration
            .map(|duration| {
                start
                    .checked_add(duration)
                    .map(|at| (at, ErrorKind::DeadlineExceeded))
                    .ok_or_else(invalid_budget)
            })
            .transpose()?;
        Ok(Self { earliest })
    }
    pub(super) fn attempt(
        self,
        now: Instant,
        timeout: Option<Duration>,
        config: Duration,
    ) -> Result<Self, NetError> {
        let mut budget = self;
        for duration in timeout.into_iter().chain(std::iter::once(config)) {
            let at = now.checked_add(duration).ok_or_else(invalid_budget)?;
            if budget.earliest.is_none_or(|(existing, _)| at < existing) {
                budget.earliest = Some((at, ErrorKind::TimedOut));
            }
        }
        Ok(budget)
    }
    pub(super) fn deadline(self) -> Option<Instant> {
        self.earliest.map(|(at, _)| at)
    }
    pub(super) fn expired(self, now: Instant, stage: ErrorStage) -> Option<NetError> {
        self.earliest
            .filter(|(at, _)| now >= *at)
            .map(|(_, kind)| NetError::from(kind).with_stage(stage))
    }
    pub(super) fn timer(self) -> Option<Pin<Box<tokio::time::Sleep>>> {
        self.deadline()
            .map(|deadline| Box::pin(tokio::time::sleep_until(deadline.into())))
    }
}
fn invalid_budget() -> NetError {
    NetError::from(ErrorKind::InvalidPolicy).with_stage(ErrorStage::Configuration)
}

/// Explicit owner also covers a polled opening future dropped during admission,
/// headers, or backoff. Capacity is returned before the synchronous observer runs.
pub(super) struct StreamOwner {
    pub(super) lease: Option<StreamRegistrationLease>,
    pub(super) permit: Option<OwnedSemaphorePermit>,
    pub(super) lifecycle: Option<RetryObserverLifecycle>,
}
impl StreamOwner {
    pub(super) fn new(policy: &crate::api::http::RetryPolicy, start: Instant) -> Self {
        Self {
            lease: None,
            permit: None,
            lifecycle: Some(RetryObserverLifecycle::new(policy.observer(), start)),
        }
    }
    pub(super) fn complete(
        &mut self,
        result: Result<http::StatusCode, &NetError>,
        exhausted: bool,
    ) {
        drop(self.lease.take());
        drop(self.permit.take());
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

pub(super) struct FusedHttpBody {
    body: Option<HttpByteStream>,
    timer: Option<Pin<Box<tokio::time::Sleep>>>,
    cancellation: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    control: Arc<CancellationSignal>,
    budget: StreamBudget,
    owner: StreamOwner,
    status: http::StatusCode,
    exhausted: bool,
}
impl FusedHttpBody {
    pub(super) fn new(
        body: HttpByteStream,
        timer: Option<Pin<Box<tokio::time::Sleep>>>,
        control: Arc<CancellationSignal>,
        budget: StreamBudget,
        owner: StreamOwner,
        status: http::StatusCode,
        exhausted: bool,
    ) -> Self {
        let wait_control = Arc::clone(&control);
        let cancellation = Box::pin(async move { wait_control.wait().await });
        Self {
            body: Some(body),
            timer,
            cancellation: Some(cancellation),
            control,
            budget,
            owner,
            status,
            exhausted,
        }
    }
    fn finish(&mut self, error: Option<&NetError>) {
        // Mark terminal before dropping anything that can invoke a waker.
        let body = self.body.take();
        let timer = self.timer.take();
        let cancellation = self.cancellation.take();
        drop(body);
        drop(timer);
        drop(cancellation);
        let result = error.map_or(Ok(self.status), Err);
        self.owner.complete(result, self.exhausted);
    }
}
impl Stream for FusedHttpBody {
    type Item = Result<Bytes, NetError>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.body.is_none() {
            return Poll::Ready(None);
        }
        // Absolute time is checked even when the body already has ready bytes.
        let mut error = this.budget.expired(Instant::now(), ErrorStage::Receive);
        if error.is_none() && this.control.is_cancelled() {
            error = Some(NetError::from(ErrorKind::Cancelled).with_stage(ErrorStage::Receive));
        }
        if error.is_none() {
            if let Some(timer) = this.timer.as_mut() {
                if timer.as_mut().poll(cx).is_ready() {
                    error = this.budget.expired(Instant::now(), ErrorStage::Receive);
                }
            }
            if let Some(wait) = this.cancellation.as_mut() {
                if wait.as_mut().poll(cx).is_ready() && error.is_none() {
                    error =
                        Some(NetError::from(ErrorKind::Cancelled).with_stage(ErrorStage::Receive));
                }
            }
        }
        if let Some(error) = error {
            this.finish(Some(&error));
            return Poll::Ready(Some(Err(error)));
        }
        let result = match this.body.as_mut() {
            Some(body) => body.as_mut().poll_next(cx),
            None => return Poll::Ready(None),
        };
        // Decoding a ready body can itself consume time. Apply the same absolute
        // boundary before handing any bytes or EOF to the caller.
        if matches!(result, Poll::Ready(Some(Ok(_))) | Poll::Ready(None)) {
            if let Some(error) = this.budget.expired(Instant::now(), ErrorStage::Receive) {
                this.finish(Some(&error));
                return Poll::Ready(Some(Err(error)));
            }
        }
        match &result {
            Poll::Ready(Some(Err(error))) => this.finish(Some(error)),
            Poll::Ready(None) => this.finish(None),
            _ => {}
        }
        result
    }
}
impl Drop for FusedHttpBody {
    fn drop(&mut self) {
        if self.body.is_some() {
            let error = NetError::from(ErrorKind::Cancelled).with_stage(ErrorStage::Receive);
            self.finish(Some(&error));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{check, paused_worker};
    use super::*;
    use futures_util::task::{waker, ArcWake};
    use std::sync::atomic::AtomicUsize;
    type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

    #[test]
    fn budget_earliest_wins_total_wins_ties_and_overflow_errors() -> TestResult {
        let start = Instant::now();
        let total = StreamBudget::total(start, Some(Duration::from_secs(2)))?;
        for timeout in [
            None,
            Some(Duration::from_secs(2)),
            Some(Duration::from_secs(3)),
        ] {
            let budget = total.attempt(start, timeout, Duration::from_secs(3))?;
            let after = start
                .checked_add(Duration::from_secs(4))
                .ok_or_else(|| std::io::Error::other("clock overflow"))?;
            check(
                matches!(budget.expired(after, ErrorStage::Receive), Some(error) if error.kind() == ErrorKind::DeadlineExceeded),
                "total deadline must win",
            )?;
        }
        let budget = total.attempt(start, Some(Duration::from_secs(1)), Duration::from_secs(3))?;
        let after = start
            .checked_add(Duration::from_secs(4))
            .ok_or_else(|| std::io::Error::other("clock overflow"))?;
        check(
            matches!(budget.expired(after, ErrorStage::Receive), Some(error) if error.kind() == ErrorKind::TimedOut),
            "earlier attempt must win when both expired",
        )?;
        check(
            StreamBudget::total(start, Some(Duration::MAX)).is_err(),
            "deadline overflow accepted",
        )
    }

    struct WakeCounter(AtomicUsize);
    impl ArcWake for WakeCounter {
        fn wake_by_ref(arc: &Arc<Self>) {
            arc.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn cancellation_wait_is_retained_and_terminal_poll_releases_lease() -> TestResult {
        let (worker, _receiver) = paused_worker()?;
        let (registered, registration) = worker.register_request(None)?;
        registration.commit();
        let control = Arc::clone(&registered.control);
        let mut owner =
            StreamOwner::new(&crate::api::http::RetryPolicy::no_retry(), Instant::now());
        owner.lease = Some(StreamRegistrationLease {
            registry: worker.registry(),
            request_id: registered.id,
            control: Arc::clone(&control),
            _permit: worker.try_acquire_permit()?,
        });
        let budget = StreamBudget::total(Instant::now(), None)?;
        let mut stream = FusedHttpBody::new(
            Box::pin(futures_util::stream::pending()),
            None,
            Arc::clone(&control),
            budget,
            owner,
            http::StatusCode::OK,
            true,
        );
        let counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
        let waker = waker(Arc::clone(&counter));
        let mut cx = Context::from_waker(&waker);
        check(
            Pin::new(&mut stream).poll_next(&mut cx).is_pending(),
            "body was not pending",
        )?;
        // No active next_chunk future remains here; the stream retains its waiter.
        control.cancel();
        check(
            counter.0.load(Ordering::SeqCst) > 0,
            "cancellation waiter was lost",
        )?;
        check(
            matches!(Pin::new(&mut stream).poll_next(&mut cx), Poll::Ready(Some(Err(error))) if error.kind() == ErrorKind::Cancelled),
            "cancel did not terminate pending body",
        )?;
        check(
            worker.registry.is_empty()? && worker.in_flight.available_permits() == 128,
            "terminal poll retained quota or registry",
        )?;
        check(
            matches!(Pin::new(&mut stream).poll_next(&mut cx), Poll::Ready(None)),
            "stream was not fused",
        )
    }

    #[tokio::test]
    async fn body_error_returns_capacity_before_observer_reentry() -> TestResult {
        let (worker, _receiver) = paused_worker()?;
        let worker = Arc::new(worker);
        let observed = Arc::new(AtomicBool::new(false));
        let inspected = Arc::clone(&observed);
        let callback_worker = Arc::clone(&worker);
        let policy = crate::api::http::RetryPolicy::builder()
            .observer(move |event: &RetryEvent| {
                if matches!(event, RetryEvent::Completed(_)) {
                    let empty = callback_worker.registry.is_empty().is_ok_and(|value| value);
                    inspected.store(
                        empty && callback_worker.in_flight.available_permits() == 128,
                        Ordering::SeqCst,
                    );
                }
            })
            .build()?;
        let (registered, registration) = worker.register_request(None)?;
        registration.commit();
        let control = registered.control;
        let mut owner = StreamOwner::new(&policy, Instant::now());
        owner.lease = Some(StreamRegistrationLease {
            registry: worker.registry(),
            request_id: registered.id,
            control: Arc::clone(&control),
            _permit: worker.try_acquire_permit()?,
        });
        let body = futures_util::stream::iter(vec![Err(
            NetError::from(ErrorKind::Io).with_stage(ErrorStage::Receive)
        )]);
        let budget = StreamBudget::total(Instant::now(), None)?;
        let mut stream = FusedHttpBody::new(
            Box::pin(body),
            None,
            control,
            budget,
            owner,
            http::StatusCode::OK,
            true,
        );
        check(
            matches!(stream.next().await, Some(Err(_))),
            "missing body error",
        )?;
        check(
            observed.load(Ordering::SeqCst),
            "observer ran before resource cleanup",
        )?;
        check(stream.next().await.is_none(), "body error did not fuse")
    }
}

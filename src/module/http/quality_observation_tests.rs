#![cfg(test)]

use super::*;
use crate::api::quality::{QualityDoctorConfig, QualityQuery};
use crate::module::http::connect_timeout::{ConnectTimedOut, ConnectTimeoutLayer};
use std::{
    error::Error,
    fmt,
    future::Future,
    pin::Pin,
    sync::{atomic::AtomicUsize, Arc},
    task::{Context, Poll},
    time::Duration,
};
use tower_layer::Layer;
use tower_service::Service;

type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

#[derive(Debug)]
struct OriginalError(Arc<()>);
impl fmt::Display for OriginalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("original fake connector error")
    }
}
impl Error for OriginalError {}

#[derive(Default)]
struct Calls {
    ready: AtomicUsize,
    calls: AtomicUsize,
    polls: AtomicUsize,
    dropped: AtomicUsize,
}

struct FakeService {
    calls: Arc<Calls>,
    ready: bool,
    result: Option<Result<u8, OriginalError>>,
}
struct FakeFuture {
    calls: Arc<Calls>,
    result: Option<Result<u8, OriginalError>>,
}
impl Future for FakeFuture {
    type Output = Result<u8, OriginalError>;
    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        self.calls.polls.fetch_add(1, Ordering::SeqCst);
        self.result.take().map_or(Poll::Pending, Poll::Ready)
    }
}
impl Drop for FakeFuture {
    fn drop(&mut self) {
        self.calls.dropped.fetch_add(1, Ordering::SeqCst);
    }
}
impl Service<()> for FakeService {
    type Response = u8;
    type Error = OriginalError;
    type Future = FakeFuture;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.calls.ready.fetch_add(1, Ordering::SeqCst);
        self.ready = true;
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, _: ()) -> Self::Future {
        // A clone/new instance or an extra call loses the readiness token.
        assert!(std::mem::take(&mut self.ready));
        self.calls.calls.fetch_add(1, Ordering::SeqCst);
        FakeFuture {
            calls: self.calls.clone(),
            result: self.result.take(),
        }
    }
}

fn fake(result: Option<Result<u8, OriginalError>>) -> (FakeService, Arc<Calls>) {
    let calls = Arc::new(Calls::default());
    (
        FakeService {
            calls: calls.clone(),
            ready: false,
            result,
        },
        calls,
    )
}

async fn ready<S: Service<()>>(service: &mut S) -> Result<(), S::Error> {
    futures_util::future::poll_fn(|cx| service.poll_ready(cx)).await
}

#[tokio::test]
async fn connector_service_keeps_same_ready_instance_and_original_result() -> TestResult {
    for activate in [false, true] {
        for success in [false, true] {
            let owner = QualityOwner::new(QualityDoctorConfig::detailed());
            let doctor = activate.then(|| owner.doctor()).flatten();
            let identity = Arc::new(());
            let result = if success {
                Ok(7)
            } else {
                Err(OriginalError(identity.clone()))
            };
            let (inner, calls) = fake(Some(result));
            let mut service = ConnectorLayer::new(owner.context(), 17).layer(inner);
            ready(&mut service).await?;
            match service.call(()).await {
                Ok(value) => assert!(success && value == 7),
                Err(error) => assert!(!success && Arc::ptr_eq(&identity, &error.0)),
            }
            assert_eq!(calls.ready.load(Ordering::SeqCst), 1);
            assert_eq!(calls.calls.load(Ordering::SeqCst), 1);
            assert_eq!(calls.polls.load(Ordering::SeqCst), 1);
            assert_eq!(calls.dropped.load(Ordering::SeqCst), 1);
            if let Some(doctor) = doctor {
                let snapshot = doctor.snapshot(QualityQuery::default())?;
                assert_eq!(snapshot.reports().len(), 1);
                let report = &snapshot.reports()[0];
                assert_eq!(
                    report.outcome(),
                    Some(if success {
                        QualityOutcome::Success
                    } else {
                        QualityOutcome::Failed
                    })
                );
                assert!(report.finished_offset().is_some());
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn dropping_unpolled_and_pending_connector_drops_original_future_immediately() -> TestResult {
    for poll_once in [false, true] {
        let owner = QualityOwner::new(QualityDoctorConfig::detailed());
        let doctor = owner.doctor().ok_or("doctor absent")?;
        let (inner, calls) = fake(None);
        let mut service = ConnectorLayer::new(owner.context(), 18).layer(inner);
        ready(&mut service).await?;
        let mut future = Box::pin(service.call(()));
        if poll_once {
            assert!(futures_util::poll!(future.as_mut()).is_pending());
        }
        drop(future);
        assert_eq!(calls.calls.load(Ordering::SeqCst), 1);
        assert_eq!(calls.polls.load(Ordering::SeqCst), usize::from(poll_once));
        assert_eq!(calls.dropped.load(Ordering::SeqCst), 1);
        let snapshot = doctor.snapshot(QualityQuery::default())?;
        let report = &snapshot.reports()[0];
        assert_eq!(report.outcome(), Some(QualityOutcome::Abandoned));
        assert!(report.finished_offset().is_some());
        assert_eq!(
            report
                .phases()
                .iter()
                .any(|p| p.kind() == PhaseKind::ConnectorPolled),
            poll_once
        );
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn observation_preserves_absolute_connect_deadline_and_late_ready_priority() -> TestResult {
    for response in [None, Some(Ok(7))] {
        let owner = QualityOwner::new(QualityDoctorConfig::detailed());
        let _doctor = owner.doctor();
        let (inner, calls) = fake(response);
        let timeout = Duration::from_secs(3);
        let started = tokio::time::Instant::now();
        let mut service = ConnectorLayer::new(owner.context(), 19)
            .layer(ConnectTimeoutLayer::new(timeout).layer(inner));
        ready(&mut service).await?;
        let future = service.call(());
        // The timeout starts at call, even when observation's first poll is late.
        tokio::time::advance(timeout).await;
        let error = future.await.expect_err("late connector must not succeed");
        let marker = error
            .downcast_ref::<ConnectTimedOut>()
            .ok_or("timeout type lost")?;
        assert_eq!(marker.deadline, started + timeout);
        assert_eq!(calls.calls.load(Ordering::SeqCst), 1);
        assert_eq!(calls.dropped.load(Ordering::SeqCst), 1);
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn observation_keeps_connect_error_identity_and_cancels_composed_future() -> TestResult {
    let owner = QualityOwner::new(QualityDoctorConfig::detailed());
    let _doctor = owner.doctor();
    let identity = Arc::new(());
    let (inner, calls) = fake(Some(Err(OriginalError(identity.clone()))));
    let mut service = ConnectorLayer::new(owner.context(), 20)
        .layer(ConnectTimeoutLayer::new(Duration::from_secs(3)).layer(inner));
    ready(&mut service).await?;
    let error = service
        .call(())
        .await
        .expect_err("fake connector must fail");
    let original = error
        .downcast_ref::<OriginalError>()
        .ok_or("original source lost")?;
    assert!(Arc::ptr_eq(&identity, &original.0));
    assert_eq!(calls.dropped.load(Ordering::SeqCst), 1);

    let (inner, calls) = fake(None);
    let mut service = ConnectorLayer::new(owner.context(), 21)
        .layer(ConnectTimeoutLayer::new(Duration::from_secs(3)).layer(inner));
    ready(&mut service).await?;
    let mut future = Box::pin(service.call(()));
    assert!(futures_util::poll!(future.as_mut()).is_pending());
    drop(future);
    assert_eq!(calls.dropped.load(Ordering::SeqCst), 1);
    tokio::time::advance(Duration::from_secs(4)).await;
    assert_eq!(calls.calls.load(Ordering::SeqCst), 1);
    assert_eq!(calls.dropped.load(Ordering::SeqCst), 1);
    Ok(())
}

// These cases use the production admission/middleware path with paused workers.
// No network worker, socket, wall-clock delay, or collector task is required.
#[cfg(feature = "http-client-middleware")]
mod middleware_stream {
    use super::*;
    use crate::api::http::{HttpByteStream, HttpRetryPolicy, HttpTimeouts, TimeoutPhase};
    use crate::api::quality::{
        QualityDoctor, QualityObservationState, QualityOperationKind, QualityReport,
        QualityReportRef,
    };
    use crate::module::http::http_client_inner::tests::{check, paused_worker};

    type ResultOf<T> = Result<T, Box<dyn Error + Send + Sync>>;
    const TOTAL: Duration = Duration::from_secs(5);

    #[derive(Clone, Copy)]
    enum Mode {
        Reject,
        Synthetic,
        Hold,
        #[cfg(feature = "http-client-auth")]
        HoldAfterPollingNext,
    }

    #[async_trait::async_trait]
    impl HttpMiddleware for Mode {
        async fn call(
            &self,
            call: HttpCall,
            next: Next,
        ) -> Result<HttpMiddlewareResponse, NetError> {
            match self {
                Self::Reject => Err(NetError::from(ErrorKind::InvalidInput)),
                Self::Synthetic => Ok(HttpMiddlewareResponse::Streaming(HttpStreamResponse::new(
                    http::StatusCode::ACCEPTED,
                    http::HeaderMap::new(),
                    call.request_id(),
                    0,
                    Box::pin(futures_util::stream::empty()),
                ))),
                Self::Hold => {
                    let result = std::future::pending().await;
                    drop(next);
                    result
                }
                #[cfg(feature = "http-client-auth")]
                Self::HoldAfterPollingNext => {
                    let mut opening = Box::pin(next.run(call));
                    if let Poll::Ready(result) = futures_util::poll!(opening.as_mut()) {
                        return result;
                    }
                    // Stop polling the pending transport-opening future. The
                    // outer middleware timer must select the timeout and drop
                    // this future; an inner budget cannot select first.
                    let result = std::future::pending().await;
                    drop(opening);
                    result
                }
            }
        }
    }

    struct Fixture {
        worker: Arc<HttpClientInner>,
        request_rx: mpsc::Receiver<SendCommand>,
        supervisor_rx: mpsc::Receiver<StreamSupervisor>,
        _owner: QualityOwner,
        doctor: QualityDoctor,
    }

    impl Fixture {
        fn new(mode: Mode, detailed: bool) -> ResultOf<Self> {
            let (mut worker, request_rx) = paused_worker()?;
            let (supervisor_tx, supervisor_rx) = mpsc::channel(1);
            worker.supervisor_tx = supervisor_tx;
            worker.config = Arc::new(
                HttpClientConfig::new("http://127.0.0.1:1")?
                    .with_environment_proxy(false)
                    .with_middleware(mode),
            );
            let owner = QualityOwner::new(if detailed {
                QualityDoctorConfig::detailed()
            } else {
                QualityDoctorConfig::basic()
            });
            let doctor = owner.doctor().ok_or("doctor absent")?;
            worker.quality = owner.context();
            worker.quality_client_id = 23;
            Ok(Self {
                worker: Arc::new(worker),
                request_rx,
                supervisor_rx,
                _owner: owner,
                doctor,
            })
        }

        fn report(&self) -> ResultOf<QualityReport> {
            let snapshot = self
                .doctor
                .snapshot(QualityQuery::default().with_kind(QualityOperationKind::HttpStream))?;
            check(
                snapshot.counters().ingress_dropped() == 0,
                "unexpected diagnostic loss",
            )?;
            check(
                snapshot.counters().active_capacity_skipped() == 0,
                "unexpected capacity loss",
            )?;
            check(snapshot.reports().len() == 1, "expected one stream report")?;
            snapshot
                .reports()
                .first()
                .cloned()
                .ok_or_else(|| "stream report absent".into())
        }

        fn no_leaks(&mut self) -> TestResult {
            check(
                self.worker.registry.is_empty()?,
                "stream registration leaked",
            )?;
            check(
                self.worker.in_flight.available_permits() == 128,
                "in-flight permit leaked",
            )?;
            check(
                self.worker.supervisor_tx.capacity() == 1,
                "supervisor reservation leaked",
            )?;
            check(
                self.request_rx.try_recv().is_err(),
                "unexpected buffered transport submission",
            )?;
            check(
                self.supervisor_rx.try_recv().is_err(),
                "unexpected supervisor publication",
            )
        }
    }

    fn request() -> ResultOf<HttpRequest> {
        Ok(HttpRequest::builder()
            .path("/")
            .timeouts(HttpTimeouts::new().with_total_timeout(TOTAL))
            .build()?)
    }

    fn error<T>(result: Result<T, NetError>) -> ResultOf<NetError> {
        result
            .err()
            .ok_or_else(|| "stream unexpectedly succeeded".into())
    }

    fn completed(report: &QualityReport, outcome: QualityOutcome) -> TestResult {
        check(
            report.state() == QualityObservationState::Completed,
            "stream report not completed",
        )?;
        check(
            report.outcome() == Some(outcome),
            "wrong selected stream outcome",
        )?;
        check(report.result_offset().is_some(), "result timestamp missing")?;
        check(
            report.finished_offset().is_some(),
            "finish timestamp missing",
        )?;
        check(!report.possibly_incomplete(), "complete evidence required")
    }

    fn matches_error(report: &QualityReport, failure: &NetError) -> TestResult {
        completed(report, QualityOutcome::from_error(failure))?;
        check(
            report.failure().map(|value| (value.kind(), value.stage()))
                == Some((failure.kind(), failure.context().stage)),
            "cleanup replaced the selected API error",
        )
    }

    #[tokio::test(start_paused = true)]
    async fn short_circuit_stream_errors_keep_the_actual_result_without_transport() -> TestResult {
        for detailed in [false, true] {
            for mode in [Mode::Reject, Mode::Synthetic] {
                let mut fixture = Fixture::new(mode, detailed)?;
                let mut reference = None;
                let failure = error(
                    fixture
                        .worker
                        .stream_request_with_middleware_observed(
                            request()?,
                            Instant::now(),
                            Some(&mut reference),
                        )
                        .await,
                )?;
                let (kind, stage) = match mode {
                    Mode::Reject => (ErrorKind::InvalidInput, None),
                    _ => (ErrorKind::RoutingModeMismatch, Some(ErrorStage::Dispatch)),
                };
                check(
                    (failure.kind(), failure.context().stage) == (kind, stage),
                    "API error changed",
                )?;
                let retry = failure.retry_report().ok_or("retry report absent")?;
                check(
                    retry.transport_sends == 0 && retry.attempts == 0,
                    "short circuit entered transport",
                )?;
                let report = fixture.report()?;
                check(
                    Some(report.reference()) == reference,
                    "stream reference lost",
                )?;
                check(failure.quality_ref() == reference, "error correlation lost")?;
                matches_error(&report, &failure)?;
                fixture.no_leaks()?;
            }
        }
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn total_deadline_before_next_is_timeout_instead_of_drop_cancellation() -> TestResult {
        let mut fixture = Fixture::new(Mode::Hold, true)?;
        let mut reference = None;
        let mut operation = Box::pin(fixture.worker.stream_request_with_middleware_observed(
            request()?,
            Instant::now(),
            Some(&mut reference),
        ));
        check(
            futures_util::poll!(operation.as_mut()).is_pending(),
            "middleware did not suspend",
        )?;
        check(
            fixture.report()?.outcome().is_none(),
            "unselected middleware acquired a result",
        )?;
        tokio::time::advance(TOTAL).await;
        let failure = error(operation.await)?;
        check(
            failure.kind() == ErrorKind::DeadlineExceeded,
            "total deadline kind changed",
        )?;
        check(
            failure.timeout_phase() == Some(TimeoutPhase::Total),
            "total deadline phase lost",
        )?;
        matches_error(&fixture.report()?, &failure)?;
        fixture.no_leaks()
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_before_next_still_selects_and_finishes_cancellation() -> TestResult {
        let mut fixture = Fixture::new(Mode::Hold, true)?;
        let mut operation = Box::pin(fixture.worker.stream_request_with_middleware_observed(
            request()?,
            Instant::now(),
            None,
        ));
        check(
            futures_util::poll!(operation.as_mut()).is_pending(),
            "middleware did not suspend",
        )?;
        fixture.worker.cancel_all()?;
        let failure = error(operation.await)?;
        check(
            failure.kind() == ErrorKind::Cancelled,
            "cancel_all did not cancel",
        )?;
        matches_error(&fixture.report()?, &failure)?;
        fixture.no_leaks()
    }

    #[tokio::test(start_paused = true)]
    async fn dropped_middleware_future_before_next_finishes_without_leaking_capacity() -> TestResult
    {
        let mut fixture = Fixture::new(Mode::Hold, true)?;
        let mut operation = Box::pin(fixture.worker.stream_request_with_middleware_observed(
            request()?,
            Instant::now(),
            None,
        ));
        check(
            futures_util::poll!(operation.as_mut()).is_pending(),
            "middleware did not suspend",
        )?;
        drop(operation);
        completed(&fixture.report()?, QualityOutcome::Cancelled)?;
        fixture.no_leaks()
    }

    #[cfg(feature = "http-client-auth")]
    struct AuthGate {
        entered: Arc<AtomicUsize>,
        dropped: Arc<AtomicUsize>,
    }

    #[cfg(feature = "http-client-auth")]
    struct AuthFutureDrop(Arc<AtomicUsize>);
    #[cfg(feature = "http-client-auth")]
    impl Drop for AuthFutureDrop {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[cfg(feature = "http-client-auth")]
    #[async_trait::async_trait]
    impl crate::api::http::auth::HttpAuth for AuthGate {
        async fn apply(
            &self,
            _: &mut crate::api::http::auth::AuthRequestContext<'_>,
        ) -> Result<(), NetError> {
            self.entered.fetch_add(1, Ordering::SeqCst);
            let guard = AuthFutureDrop(Arc::clone(&self.dropped));
            let result = std::future::pending().await;
            drop(guard);
            result
        }
        async fn on_unauthorized(
            &self,
            _: &crate::api::http::auth::AuthResponseContext<'_>,
        ) -> Result<crate::api::http::auth::AuthAction, NetError> {
            Ok(crate::api::http::auth::AuthAction::DoNotRetry)
        }
    }

    #[cfg(feature = "http-client-auth")]
    async fn entered_next(timeout: bool) -> TestResult {
        let mut fixture = Fixture::new(Mode::HoldAfterPollingNext, true)?;
        let entered = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicUsize::new(0));
        let worker = Arc::get_mut(&mut fixture.worker).ok_or("worker already shared")?;
        worker.config = Arc::new((*worker.config).clone().with_auth(AuthGate {
            entered: Arc::clone(&entered),
            dropped: Arc::clone(&dropped),
        }));
        let mut reference: Option<QualityReportRef> = None;
        let mut operation = Box::pin(fixture.worker.stream_request_with_middleware_observed(
            request()?,
            Instant::now(),
            Some(&mut reference),
        ));
        check(
            futures_util::poll!(operation.as_mut()).is_pending(),
            "opening did not suspend",
        )?;
        check(
            entered.load(Ordering::SeqCst) == 1,
            "Next did not reach the auth gate",
        )?;
        check(
            fixture.report()?.outcome().is_none(),
            "opening selected a result before termination",
        )?;
        if timeout {
            tokio::time::advance(TOTAL).await;
            let failure = error(operation.await)?;
            check(
                failure.kind() == ErrorKind::DeadlineExceeded,
                "opening total deadline changed",
            )?;
            check(
                failure.timeout_phase() == Some(TimeoutPhase::Total),
                "opening timeout phase lost",
            )?;
            matches_error(&fixture.report()?, &failure)?;
        } else {
            drop(operation);
            completed(&fixture.report()?, QualityOutcome::Cancelled)?;
        }
        check(
            dropped.load(Ordering::SeqCst) == 1,
            "original provider future not dropped exactly once",
        )?;
        fixture.no_leaks()
    }

    #[cfg(feature = "http-client-auth")]
    #[tokio::test(start_paused = true)]
    async fn total_deadline_after_next_started_preserves_the_timeout_result() -> TestResult {
        entered_next(true).await
    }

    #[cfg(feature = "http-client-auth")]
    #[tokio::test(start_paused = true)]
    async fn dropping_opening_future_after_next_started_still_completes_cancellation() -> TestResult
    {
        entered_next(false).await
    }

    #[tokio::test(start_paused = true)]
    async fn actual_body_terminal_results_survive_later_middleware_errors() -> TestResult {
        for terminal in 0..3 {
            let mut fixture = Fixture::new(Mode::Reject, true)?;
            let start = Instant::now();
            let (registered, registration) = fixture.worker.register_request(None)?;
            registration.commit();
            let control = registered.control;
            let probe = fixture
                .worker
                .begin_quality(true)
                .ok_or("stream probe missing")?;
            control
                .quality
                .set(probe)
                .map_err(|_| "quality probe already installed")?;
            let permit = Arc::new(fixture.worker.try_acquire_permit()?);
            let mut owner = StreamOwner::new(&HttpRetryPolicy::no_retry(), start);
            owner.lease = Some(StreamRegistrationLease {
                registry: fixture.worker.registry(),
                request_id: registered.id,
                control: Arc::clone(&control),
                permit: Some(Arc::clone(&permit)),
            });
            let middleware = MiddlewareLease::new(
                fixture.worker.registry(),
                Arc::clone(&control),
                registered.id,
                permit,
                None,
                start,
            )?;
            if let Some(lifecycle) = owner.lifecycle.as_mut() {
                lifecycle.control = Some(Arc::clone(&control));
                lifecycle.deferred = control.middleware_completion.get().cloned();
            }
            let body: HttpByteStream = match terminal {
                0 => Box::pin(futures_util::stream::empty()),
                1 => Box::pin(futures_util::stream::once(async {
                    Err(NetError::from(ErrorKind::Protocol).with_stage(ErrorStage::Receive))
                })),
                _ => Box::pin(futures_util::stream::pending()),
            };
            let (mut body, supervisor) = FusedHttpBody::new(
                body,
                Arc::clone(&control),
                StreamBudget::total(start, Some(TOTAL))?,
                owner,
                http::StatusCode::OK,
                false,
                registered.id,
                1,
                start,
                None,
            )?;
            if terminal == 0 {
                check(body.next().await.is_none(), "empty body did not reach EOF")?;
            } else if terminal == 1 {
                check(
                    body.next().await.is_some_and(|item| item.is_err()),
                    "body error disappeared",
                )?;
            }
            drop(body);
            drop(supervisor);
            let selected = fixture.report()?;
            check(
                selected.state() == QualityObservationState::InProgress,
                "middleware observation finished too early",
            )?;
            let expected = match terminal {
                0 => QualityOutcome::Success,
                1 => QualityOutcome::Failed,
                _ => QualityOutcome::Cancelled,
            };
            check(
                selected.outcome() == Some(expected),
                "body terminal result was suppressed",
            )?;
            middleware.finish(&Err(NetError::from(ErrorKind::InvalidInput)));
            let report = fixture.report()?;
            completed(&report, expected)?;
            check(
                report.result_offset() == selected.result_offset(),
                "middleware replaced body result timestamp",
            )?;
            check(
                report
                    .failure()
                    .map(|failure| (failure.kind(), failure.stage()))
                    == selected
                        .failure()
                        .map(|failure| (failure.kind(), failure.stage())),
                "middleware replaced body failure",
            )?;
            check(
                report.http_status() == selected.http_status(),
                "middleware replaced body status",
            )?;
            fixture.no_leaks()?;
        }
        Ok(())
    }
}

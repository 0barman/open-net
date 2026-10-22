//! Deterministic tests for partial admission resources and commit arbitration.
use super::*;
use crate::api::http::{HttpAdmissionPolicy, HttpRetryPolicy, HttpTimeouts};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}

fn failure<T>(result: Result<T, NetError>) -> TestResult<NetError> {
    match result {
        Err(error) => Ok(error),
        Ok(_) => Err(std::io::Error::other("admission unexpectedly succeeded").into()),
    }
}

struct Fixture {
    worker: HttpClientInner,
    request_rx: mpsc::Receiver<SendCommand>,
    _supervisor_rx: mpsc::Receiver<StreamSupervisor>,
    capacity: usize,
}

impl Fixture {
    fn new(capacity: usize) -> TestResult<Self> {
        let (mut worker, request_rx) = super::tests::paused_worker()?;
        let (supervisor_tx, supervisor_rx) = mpsc::channel(1);
        worker.supervisor_tx = supervisor_tx;
        worker.in_flight = Arc::new(Semaphore::new(capacity));
        Ok(Self {
            worker,
            request_rx,
            _supervisor_rx: supervisor_rx,
            capacity,
        })
    }

    fn block_request_lane(&self) -> TestResult<mpsc::OwnedPermit<SendCommand>> {
        self.worker
            .request_tx
            .clone()
            .try_reserve_owned()
            .map_err(|_| std::io::Error::other("could not reserve test request slot").into())
    }

    fn block_supervisor_lane(&self) -> TestResult<mpsc::OwnedPermit<StreamSupervisor>> {
        self.worker
            .supervisor_tx
            .clone()
            .try_reserve_owned()
            .map_err(|_| std::io::Error::other("could not reserve test supervisor slot").into())
    }

    fn no_leaks(&mut self) -> TestResult {
        check(
            self.worker.in_flight.available_permits() == self.capacity,
            "in-flight permit leaked",
        )?;
        check(self.worker.registry.is_empty()?, "registry entry leaked")?;
        check(
            self.worker.request_tx.capacity() == 1,
            "request slot leaked",
        )?;
        check(
            self.worker.supervisor_tx.capacity() == 1,
            "supervisor slot leaked",
        )?;
        check(
            self.request_rx.try_recv().is_err(),
            "admission unexpectedly published a request",
        )
    }
}

fn request(policy: HttpAdmissionPolicy, timeouts: HttpTimeouts) -> TestResult<HttpRequest> {
    Ok(HttpRequest::builder()
        .path("/")
        .admission_policy(policy)
        .timeouts(timeouts)
        .build()?)
}

fn timeout_error(error: &NetError, phase: TimeoutPhase) -> TestResult {
    check(
        error.context().http_timeout_phase == Some(phase),
        "wrong timeout phase",
    )?;
    check(
        error.context().stage == Some(ErrorStage::Admission),
        "wrong timeout stage",
    )?;
    check(
        error.context().http_attempt == Some(0),
        "admission timeout counted an attempt",
    )?;
    check(
        error.context().http_request_id.is_some(),
        "admission error lost request ID",
    )?;
    check(
        error.kind()
            == if phase == TimeoutPhase::Total {
                ErrorKind::DeadlineExceeded
            } else {
                ErrorKind::TimedOut
            },
        "wrong timeout kind",
    )
}

#[tokio::test(start_paused = true)]
async fn available_inflight_but_full_request_slot_obeys_wait_and_fail_fast() -> TestResult {
    let mut fixture = Fixture::new(2)?;
    let occupied = fixture.block_request_lane()?;
    let waiting_request = request(HttpAdmissionPolicy::Wait, HttpTimeouts::new())?;
    let mut waiting = Box::pin(fixture.worker.admit_buffered_future(&waiting_request));
    check(
        futures_util::poll!(waiting.as_mut()).is_pending(),
        "Wait bypassed occupied request slot",
    )?;
    check(
        fixture.worker.in_flight.available_permits() == 1,
        "Wait did not retain its first-stage permit",
    )?;
    let rejected = request(HttpAdmissionPolicy::FailFast, HttpTimeouts::new())?;
    let error = failure(fixture.worker.admit_buffered_future(&rejected).await)?;
    check(
        error.kind() == ErrorKind::QueueFull,
        "FailFast did not reject occupied slot",
    )?;
    check(
        error.context().http_attempt == Some(0),
        "slot rejection counted a transport attempt",
    )?;
    check(
        fixture.worker.in_flight.available_permits() == 1,
        "FailFast leaked partial permit",
    )?;
    drop(occupied);
    let (accepted, _) = waiting.await?;
    check(
        fixture.worker.in_flight.available_permits() == 1,
        "Wait acquired its in-flight permit twice",
    )?;
    drop(accepted);
    fixture.no_leaks()
}

#[tokio::test(start_paused = true)]
async fn semaphore_and_queue_slot_share_one_absolute_admission_deadline() -> TestResult {
    let mut fixture = Fixture::new(1)?;
    let permit = Arc::clone(&fixture.worker.in_flight)
        .acquire_owned()
        .await?;
    let occupied = fixture.block_request_lane()?;
    let request = request(
        HttpAdmissionPolicy::Wait,
        HttpTimeouts::new()
            .with_admission_timeout(Duration::from_millis(100))
            .without_total_timeout(),
    )?;
    let start = Instant::now();
    let mut waiting = Box::pin(fixture.worker.admit_buffered_future(&request));
    check(
        futures_util::poll!(waiting.as_mut()).is_pending(),
        "semaphore wait was skipped",
    )?;
    tokio::time::advance(Duration::from_millis(60)).await;
    drop(permit);
    check(
        futures_util::poll!(waiting.as_mut()).is_pending(),
        "queue slot wait was skipped",
    )?;
    check(
        fixture.worker.in_flight.available_permits() == 0,
        "second stage did not own permit",
    )?;
    tokio::time::advance(Duration::from_millis(40)).await;
    let error = failure(waiting.await)?;
    timeout_error(&error, TimeoutPhase::Admission)?;
    check(
        Instant::now().duration_since(start) == Duration::from_millis(100),
        "second stage restarted its admission budget",
    )?;
    check(
        fixture.worker.in_flight.available_permits() == 1,
        "timeout leaked partially acquired permit",
    )?;
    drop(occupied);
    fixture.no_leaks()
}

#[tokio::test(start_paused = true)]
async fn queue_slot_timeout_uses_total_admission_priority_and_independent_disabling() -> TestResult
{
    for (admission, total, expected) in [
        (Some(20), Some(40), TimeoutPhase::Admission),
        (Some(40), Some(20), TimeoutPhase::Total),
        (Some(20), Some(20), TimeoutPhase::Total),
        (None, Some(20), TimeoutPhase::Total),
    ] {
        let mut fixture = Fixture::new(1)?;
        let occupied = fixture.block_request_lane()?;
        let mut timeouts = HttpTimeouts::new()
            .without_admission_timeout()
            .without_total_timeout();
        if let Some(ms) = admission {
            timeouts = timeouts.with_admission_timeout(Duration::from_millis(ms));
        }
        if let Some(ms) = total {
            timeouts = timeouts.with_total_timeout(Duration::from_millis(ms));
        }
        let request = request(HttpAdmissionPolicy::Wait, timeouts)?;
        let mut waiting = Box::pin(fixture.worker.admit_buffered_future(&request));
        check(
            futures_util::poll!(waiting.as_mut()).is_pending(),
            "request did not wait for slot",
        )?;
        tokio::time::advance(Duration::from_millis(20)).await;
        timeout_error(&failure(waiting.await)?, expected)?;
        drop(occupied);
        fixture.no_leaks()?;
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn slot_wait_drop_cancel_and_shutdown_release_partial_resources() -> TestResult {
    for action in ["drop", "cancel", "shutdown", "receiver-close"] {
        let mut fixture = Fixture::new(1)?;
        let occupied = fixture.block_request_lane()?;
        let request = request(
            HttpAdmissionPolicy::Wait,
            HttpTimeouts::new()
                .without_admission_timeout()
                .without_total_timeout(),
        )?;
        let mut waiting = Box::pin(fixture.worker.admit_buffered_future(&request));
        check(
            futures_util::poll!(waiting.as_mut()).is_pending(),
            "request failed to wait for slot",
        )?;
        check(
            fixture.worker.in_flight.available_permits() == 0,
            "waiter failed to retain partial permit",
        )?;
        match action {
            "drop" => drop(waiting),
            "cancel" => {
                fixture.worker.cancel_all()?;
                let error = failure(waiting.await)?;
                check(
                    error.kind() == ErrorKind::Cancelled,
                    "cancel_all missed slot waiter",
                )?;
            }
            "shutdown" => {
                fixture.worker.request_shutdown();
                let error = failure(waiting.await)?;
                check(
                    error.kind() == ErrorKind::QueueClosed,
                    "shutdown missed slot waiter",
                )?;
            }
            _ => {
                fixture.request_rx.close();
                let error = failure(waiting.await)?;
                check(
                    error.kind() == ErrorKind::QueueClosed,
                    "receiver close missed slot waiter",
                )?;
            }
        }
        drop(occupied);
        fixture.no_leaks()?;
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn queue_waiters_preserve_fifo_when_middle_reservation_is_dropped() -> TestResult {
    let mut fixture = Fixture::new(3)?;
    let occupied = fixture.block_request_lane()?;
    let request = request(HttpAdmissionPolicy::Wait, HttpTimeouts::new())?;
    let mut first = Box::pin(fixture.worker.admit_buffered_future(&request));
    let mut middle = Box::pin(fixture.worker.admit_buffered_future(&request));
    let mut last = Box::pin(fixture.worker.admit_buffered_future(&request));
    check(
        futures_util::poll!(first.as_mut()).is_pending(),
        "first slot waiter did not wait",
    )?;
    check(
        futures_util::poll!(middle.as_mut()).is_pending(),
        "middle slot waiter did not wait",
    )?;
    check(
        futures_util::poll!(last.as_mut()).is_pending(),
        "last slot waiter did not wait",
    )?;
    check(
        fixture.worker.in_flight.available_permits() == 0,
        "slot waiters did not own all partial permits",
    )?;
    drop(middle);
    check(
        fixture.worker.in_flight.available_permits() == 1,
        "middle cancellation leaked partial permit",
    )?;
    drop(occupied);
    check(
        futures_util::poll!(last.as_mut()).is_pending(),
        "later slot waiter overtook first",
    )?;
    let (first_admission, _) = first.await?;
    check(
        futures_util::poll!(last.as_mut()).is_pending(),
        "occupied slot admitted another waiter",
    )?;
    drop(first_admission);
    let (last_admission, _) = last.await?;
    drop(last_admission);
    fixture.no_leaks()
}

#[tokio::test(start_paused = true)]
async fn visible_cancel_or_expired_budget_wins_over_ready_slot_at_commit() -> TestResult {
    for cancel in [false, true] {
        let mut fixture = Fixture::new(1)?;
        let occupied = fixture.block_request_lane()?;
        let request = request(
            HttpAdmissionPolicy::Wait,
            HttpTimeouts::new().with_admission_timeout(Duration::from_millis(10)),
        )?;
        let mut waiting = Box::pin(fixture.worker.admit_buffered_future(&request));
        check(
            futures_util::poll!(waiting.as_mut()).is_pending(),
            "slot wait missing",
        )?;
        if cancel {
            fixture.worker.cancel_all()?;
        }
        tokio::time::advance(Duration::from_millis(10)).await;
        drop(occupied);
        let error = failure(waiting.await)?;
        if cancel {
            check(
                error.kind() == ErrorKind::Cancelled,
                "visible cancellation lost to deadline or ready capacity",
            )?;
        } else {
            timeout_error(&error, TimeoutPhase::Admission)?;
        }
        fixture.no_leaks()?;
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn supervisor_slot_is_part_of_stream_admission_and_uses_the_same_deadline() -> TestResult {
    let mut fixture = Fixture::new(1)?;
    let occupied = fixture.block_supervisor_lane()?;
    let held_permit = Arc::clone(&fixture.worker.in_flight)
        .acquire_owned()
        .await?;
    let options = HttpRequestOptions::new().with_timeouts(
        HttpTimeouts::new()
            .with_admission_timeout(Duration::from_millis(100))
            .without_total_timeout(),
    )?;
    let resolved = fixture.worker.resolve_timeouts(&options)?;
    let start = Instant::now();
    let mut owner = StreamOwner::new(&HttpRetryPolicy::no_retry(), start);
    let mut waiting = Box::pin(fixture.worker.admit_stream(
        HttpAdmissionPolicy::Wait,
        resolved,
        start,
        &mut owner,
    ));
    check(
        futures_util::poll!(waiting.as_mut()).is_pending(),
        "stream did not wait for permit",
    )?;
    tokio::time::advance(Duration::from_millis(60)).await;
    drop(held_permit);
    check(
        futures_util::poll!(waiting.as_mut()).is_pending(),
        "stream did not wait for supervisor slot",
    )?;
    check(
        fixture.worker.in_flight.available_permits() == 0,
        "stream failed to retain first-stage permit",
    )?;
    tokio::time::advance(Duration::from_millis(40)).await;
    let error = failure(waiting.await)?;
    timeout_error(&error, TimeoutPhase::Admission)?;
    check(
        Instant::now().duration_since(start) == Duration::from_millis(100),
        "stream supervisor stage restarted admission budget",
    )?;
    check(
        owner.lease.is_none() && owner.supervisor_slot.is_none(),
        "failed stream accepted ownership",
    )?;
    drop(occupied);
    drop(owner);
    fixture.no_leaks()
}

#[tokio::test(start_paused = true)]
async fn stream_supervisor_fail_fast_wait_drop_cancel_and_shutdown_reclaim_resources() -> TestResult
{
    for action in ["fail-fast", "drop", "cancel", "shutdown", "accept"] {
        let mut fixture = Fixture::new(1)?;
        let occupied = fixture.block_supervisor_lane()?;
        let options = HttpRequestOptions::new().with_timeouts(
            HttpTimeouts::new()
                .without_admission_timeout()
                .without_total_timeout(),
        )?;
        let resolved = fixture.worker.resolve_timeouts(&options)?;
        let start = Instant::now();
        let mut owner = StreamOwner::new(&HttpRetryPolicy::no_retry(), start);
        let policy = if action == "fail-fast" {
            HttpAdmissionPolicy::FailFast
        } else {
            HttpAdmissionPolicy::Wait
        };
        let mut waiting = Box::pin(
            fixture
                .worker
                .admit_stream(policy, resolved, start, &mut owner),
        );
        if action == "fail-fast" {
            let error = failure(waiting.await)?;
            check(
                error.kind() == ErrorKind::QueueFull,
                "FailFast stream ignored supervisor slot",
            )?;
            drop(occupied);
        } else {
            check(
                futures_util::poll!(waiting.as_mut()).is_pending(),
                "Wait stream ignored supervisor slot",
            )?;
            check(
                fixture.worker.in_flight.available_permits() == 0,
                "stream waiter lost its partial permit",
            )?;
            match action {
                "drop" => {
                    drop(waiting);
                    drop(occupied);
                }
                "cancel" => {
                    fixture.worker.cancel_all()?;
                    check(
                        failure(waiting.await)?.kind() == ErrorKind::Cancelled,
                        "stream slot waiter missed cancellation",
                    )?;
                    drop(occupied);
                }
                "shutdown" => {
                    fixture.worker.request_shutdown();
                    check(
                        failure(waiting.await)?.kind() == ErrorKind::QueueClosed,
                        "stream slot waiter missed shutdown",
                    )?;
                    drop(occupied);
                }
                _ => {
                    drop(occupied);
                    waiting.await?;
                    check(
                        owner.lease.is_some() && owner.supervisor_slot.is_some(),
                        "accepted stream did not own its reserved resources",
                    )?;
                    check(
                        fixture.worker.request_tx.capacity() == 1,
                        "stream consumed buffered lane capacity",
                    )?;
                    check(
                        fixture.worker.in_flight.available_permits() == 0,
                        "accepted stream released capacity before owner ended",
                    )?;
                }
            }
        }
        drop(owner);
        fixture.no_leaks()?;
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn disabled_admission_and_total_can_wait_past_default_until_cancelled() -> TestResult {
    let mut fixture = Fixture::new(1)?;
    let occupied = fixture.block_request_lane()?;
    let request = request(
        HttpAdmissionPolicy::Wait,
        HttpTimeouts::new()
            .without_admission_timeout()
            .without_total_timeout(),
    )?;
    let mut waiting = Box::pin(fixture.worker.admit_buffered_future(&request));
    check(
        futures_util::poll!(waiting.as_mut()).is_pending(),
        "disabled-budget waiter did not wait",
    )?;
    tokio::time::advance(Duration::from_secs(3600)).await;
    check(
        futures_util::poll!(waiting.as_mut()).is_pending(),
        "disabled admission retained a hidden default deadline",
    )?;
    fixture.worker.cancel_all()?;
    check(
        failure(waiting.await)?.kind() == ErrorKind::Cancelled,
        "unbounded waiter failed to cancel",
    )?;
    drop(occupied);
    fixture.no_leaks()
}

#[tokio::test(start_paused = true)]
async fn default_admission_deadline_is_thirty_seconds_and_independent_of_attempt() -> TestResult {
    let mut fixture = Fixture::new(1)?;
    fixture.worker.config = Arc::new(
        (*fixture.worker.config)
            .clone()
            .with_timeout(Duration::from_millis(1))?
            .with_timeouts(HttpTimeouts::new().with_headers_timeout(Duration::from_millis(1)))?,
    );
    let occupied = fixture.block_request_lane()?;
    let request = request(HttpAdmissionPolicy::Wait, HttpTimeouts::new())?;
    let mut waiting = Box::pin(fixture.worker.admit_buffered_future(&request));
    check(
        futures_util::poll!(waiting.as_mut()).is_pending(),
        "default admission failed to wait",
    )?;
    tokio::time::advance(Duration::from_secs(29)).await;
    check(
        futures_util::poll!(waiting.as_mut()).is_pending(),
        "attempt or headers budget shortened admission",
    )?;
    tokio::time::advance(Duration::from_secs(1)).await;
    timeout_error(&failure(waiting.await)?, TimeoutPhase::Admission)?;
    drop(occupied);
    fixture.no_leaks()
}

struct DrainWakeProbe {
    in_flight: Arc<Semaphore>,
    wakes: std::sync::atomic::AtomicUsize,
    fewest_available: std::sync::atomic::AtomicUsize,
}

impl DrainWakeProbe {
    fn new(worker: &HttpClientInner) -> Arc<Self> {
        Arc::new(Self {
            in_flight: Arc::clone(&worker.in_flight),
            wakes: std::sync::atomic::AtomicUsize::new(0),
            fewest_available: std::sync::atomic::AtomicUsize::new(usize::MAX),
        })
    }

    fn saw_released_capacity(&self) -> TestResult {
        check(
            self.wakes.load(Ordering::SeqCst) > 0,
            "drain was not notified of registry cleanup",
        )?;
        check(
            self.fewest_available.load(Ordering::SeqCst) == 1,
            "drain was notified before the operation returned its in-flight permit",
        )
    }
}

impl futures_util::task::ArcWake for DrainWakeProbe {
    fn wake_by_ref(probe: &Arc<Self>) {
        // Notify invokes this synchronously. Observe the exact release ordering,
        // instead of relying on a task winning a scheduler race after wakeup.
        probe
            .fewest_available
            .fetch_min(probe.in_flight.available_permits(), Ordering::SeqCst);
        probe.wakes.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test(start_paused = true)]
async fn unpublished_admission_releases_capacity_before_waking_drain() -> TestResult {
    let mut fixture = Fixture::new(1)?;
    let request = request(HttpAdmissionPolicy::Wait, HttpTimeouts::new())?;
    let (admission, _) = fixture.worker.admit_buffered_future(&request).await?;
    let probe = DrainWakeProbe::new(&fixture.worker);
    let waker = futures_util::task::waker(Arc::clone(&probe));
    let mut context = std::task::Context::from_waker(&waker);
    let mut draining = Box::pin(fixture.worker.drain());
    check(
        std::future::Future::poll(draining.as_mut(), &mut context).is_pending(),
        "drain did not wait for unpublished admission",
    )?;
    drop(admission);
    probe.saw_released_capacity()?;
    draining.await?;
    fixture.no_leaks()
}

#[tokio::test(start_paused = true)]
async fn completion_lease_releases_capacity_before_waking_drain() -> TestResult {
    let mut fixture = Fixture::new(1)?;
    let _handle = fixture
        .worker
        .submit_request(super::tests::NoopRequest, HttpRequestOptions::new())
        .map_err(EnqueueError::into_error)?;
    let SendCommand::Job(mut job) = fixture
        .request_rx
        .try_recv()
        .map_err(|_| std::io::Error::other("accepted job missing"))?;
    let completion = job
        .completion
        .take()
        .ok_or_else(|| std::io::Error::other("completion lease missing"))?;
    drop(job);
    let probe = DrainWakeProbe::new(&fixture.worker);
    let waker = futures_util::task::waker(Arc::clone(&probe));
    let mut context = std::task::Context::from_waker(&waker);
    let mut draining = Box::pin(fixture.worker.drain());
    check(
        std::future::Future::poll(draining.as_mut(), &mut context).is_pending(),
        "drain did not wait for completion lease",
    )?;
    drop(completion);
    probe.saw_released_capacity()?;
    draining.await?;
    fixture.no_leaks()
}

#[tokio::test(start_paused = true)]
async fn stream_owner_releases_capacity_before_waking_drain() -> TestResult {
    let mut fixture = Fixture::new(1)?;
    let resolved = fixture
        .worker
        .resolve_timeouts(&HttpRequestOptions::new())?;
    let start = Instant::now();
    let mut owner = StreamOwner::new(&HttpRetryPolicy::no_retry(), start);
    fixture
        .worker
        .admit_stream(HttpAdmissionPolicy::Wait, resolved, start, &mut owner)
        .await?;
    let probe = DrainWakeProbe::new(&fixture.worker);
    let waker = futures_util::task::waker(Arc::clone(&probe));
    let mut context = std::task::Context::from_waker(&waker);
    let mut draining = Box::pin(fixture.worker.drain());
    check(
        std::future::Future::poll(draining.as_mut(), &mut context).is_pending(),
        "drain did not wait for stream owner",
    )?;
    drop(owner);
    probe.saw_released_capacity()?;
    draining.await?;
    fixture.no_leaks()
}

#[tokio::test(start_paused = true)]
async fn finished_callback_releases_capacity_before_waking_drain() -> TestResult {
    let mut fixture = Fixture::new(1)?;
    let handle = fixture
        .worker
        .submit_request(super::tests::NoopRequest, HttpRequestOptions::new())
        .map_err(EnqueueError::into_error)?;
    let SendCommand::Job(job) = fixture
        .request_rx
        .try_recv()
        .map_err(|_| std::io::Error::other("accepted callback job missing"))?;
    let callback = CallbackJob {
        request: job.request,
        result: Err(NetError::from(ErrorKind::Cancelled)),
        request_id: job.request_id,
        control: job.control,
        registry: job.registry,
        _permit: job.permit,
        completion: job.completion,
    };
    let probe = DrainWakeProbe::new(&fixture.worker);
    let waker = futures_util::task::waker(Arc::clone(&probe));
    let mut context = std::task::Context::from_waker(&waker);
    let mut draining = Box::pin(fixture.worker.drain());
    check(
        std::future::Future::poll(draining.as_mut(), &mut context).is_pending(),
        "drain did not wait for accepted callback",
    )?;
    run_callback(callback).await;
    probe.saw_released_capacity()?;
    draining.await?;
    handle.finished().await.callback?;
    fixture.no_leaks()
}

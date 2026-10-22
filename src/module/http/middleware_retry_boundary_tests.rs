//! The internal middleware result must not depend on the user callback lane.
use super::tests::{check, paused_worker};
use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

type TestResult = Result<(), Box<dyn StdError + Send + Sync>>;

struct PassThrough;

#[async_trait::async_trait]
impl HttpMiddleware for PassThrough {
    async fn call(&self, call: HttpCall, next: Next) -> Result<HttpMiddlewareResponse, NetError> {
        next.run(call).await
    }
}

async fn respond_once(listener: tokio::net::TcpListener) -> TestResult {
    let (mut socket, _) = tokio::time::timeout(Duration::from_secs(3), listener.accept()).await??;
    let mut request = Vec::new();
    while !request.windows(4).any(|part| part == b"\r\n\r\n") {
        let mut buffer = [0_u8; 1024];
        let count =
            tokio::time::timeout(Duration::from_secs(3), socket.read(&mut buffer)).await??;
        if count == 0 || request.len() >= 64 * 1024 {
            return Err(std::io::Error::other("incomplete request headers").into());
        }
        request.extend_from_slice(&buffer[..count]);
    }
    socket
        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
        .await?;
    socket.shutdown().await?;
    Ok(())
}

async fn stalled_callback_lane(cancel: bool) -> TestResult {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let peer = tokio::spawn(respond_once(listener));
    let (mut worker, mut requests) = paused_worker()?;
    let (observed, mut events) = mpsc::unbounded_channel();
    let policy = crate::api::http::HttpRetryPolicy::builder()
        .observer(move |event: &RetryEvent| {
            let _ = observed.send(event.clone());
        })
        .build()?;
    worker.config = Arc::new(
        HttpClientConfig::new(format!("http://{address}"))?
            .with_default_retry_policy(policy)
            .with_middleware(PassThrough),
    );
    let worker = Arc::new(worker);
    let request = HttpRequest::builder()
        .path("/")
        .timeouts(
            crate::api::http::HttpTimeouts::new().with_total_timeout(Duration::from_millis(250)),
        )
        .build()?;
    let inner = Arc::clone(&worker);
    let operation =
        tokio::spawn(async move { inner.request_buffered(request, Instant::now()).await });
    let Some(SendCommand::Job(job)) = requests.recv().await else {
        return Err(std::io::Error::other("middleware did not submit a job").into());
    };
    let control = Arc::clone(&job.control);
    if cancel {
        control.cancel();
    }
    let (responses, mut callbacks) = mpsc::channel(1);
    process_job(
        job,
        worker.client.clone(),
        Arc::clone(&worker.config),
        responses,
        1,
    )
    .await;
    // Deliberately leave the callback lane unserviced past the total deadline.
    // A buffered middleware bridge has no application callback to queue here.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let outcome = tokio::time::timeout(Duration::from_secs(2), operation).await??;
    check(
        callbacks.is_empty(),
        "internal middleware result remained on the stalled callback lane",
    )?;
    check(
        worker.registry.is_empty()? && worker.in_flight.available_permits() == 128,
        "middleware terminal result retained registration or capacity",
    )?;
    // Check before cleanup so running queued callbacks cannot hide retained resources.
    while let Ok(callback) = callbacks.try_recv() {
        run_callback(callback).await;
    }
    if cancel {
        peer.abort();
        let _ = peer.await;
    } else {
        peer.await??;
    }
    let report = control
        .retry_report()
        .ok_or_else(|| std::io::Error::other("middleware terminal report missing"))?;
    let completed: Vec<_> = std::iter::from_fn(|| events.try_recv().ok())
        .filter_map(|event| match event {
            RetryEvent::Completed(report) => Some(report),
            _ => None,
        })
        .collect();
    check(
        completed.len() == 1 && completed.first() == Some(&report),
        "middleware did not publish exactly one matching terminal report",
    )?;
    if cancel {
        let error = outcome
            .err()
            .ok_or_else(|| std::io::Error::other("cancelled middleware returned success"))?;
        check(
            error.kind() == ErrorKind::Cancelled
                && error.retry_report() == Some(&report)
                && report.transport_sends == 0
                && report.stop_reason == Some(RetryStopReason::Cancelled),
            "middleware bridge changed cancellation or sent cancelled work",
        )
    } else {
        let response = outcome?;
        check(
            response.status == http::StatusCode::OK
                && response.body.as_ref() == b"ok"
                && response.retry_report() == Some(&report)
                && report.elapsed < Duration::from_millis(250)
                && report.stop_reason == Some(RetryStopReason::Completed),
            "callback queue changed successful middleware result or elapsed time",
        )
    }
}

#[tokio::test]
async fn buffered_middleware_result_does_not_wait_for_callback_lane() -> TestResult {
    stalled_callback_lane(false).await
}

#[tokio::test]
async fn buffered_middleware_cancellation_releases_resources_with_callback_lane_stalled(
) -> TestResult {
    stalled_callback_lane(true).await
}

struct HoldAfterResponse(Arc<Notify>);

#[async_trait::async_trait]
impl HttpMiddleware for HoldAfterResponse {
    async fn call(&self, call: HttpCall, next: Next) -> Result<HttpMiddlewareResponse, NetError> {
        let response = next.run(call).await?;
        self.0.notify_one();
        std::future::pending::<()>().await;
        Ok(response)
    }
}

async fn middleware_work_remains_bounded(cancel: bool) -> TestResult {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let peer = tokio::spawn(respond_once(listener));
    let entered = Arc::new(Notify::new());
    let (mut worker, mut requests) = paused_worker()?;
    worker.config = Arc::new(
        HttpClientConfig::new(format!("http://{address}"))?
            .with_middleware(HoldAfterResponse(Arc::clone(&entered))),
    );
    let worker = Arc::new(worker);
    let request = HttpRequest::builder()
        .path("/")
        .timeouts(
            crate::api::http::HttpTimeouts::new().with_total_timeout(Duration::from_millis(250)),
        )
        .build()?;
    let inner = Arc::clone(&worker);
    let operation =
        tokio::spawn(async move { inner.request_buffered(request, Instant::now()).await });
    let Some(SendCommand::Job(job)) = requests.recv().await else {
        return Err(std::io::Error::other("middleware did not submit a job").into());
    };
    let control = Arc::clone(&job.control);
    let (responses, mut callbacks) = mpsc::channel(1);
    process_job(
        job,
        worker.client.clone(),
        Arc::clone(&worker.config),
        responses,
        1,
    )
    .await;
    tokio::time::timeout(Duration::from_secs(1), entered.notified()).await?;
    check(
        worker.in_flight.available_permits() == 127 && !worker.registry.is_empty()?,
        "internal result delivery retired the still-running middleware",
    )?;
    if cancel {
        control.cancel();
    }
    let outcome = tokio::time::timeout(Duration::from_secs(2), operation).await??;
    check(
        callbacks.is_empty(),
        "internal middleware result remained on the stalled callback lane",
    )?;
    check(
        worker.registry.is_empty()? && worker.in_flight.available_permits() == 128,
        "middleware termination did not return registration and capacity",
    )?;
    while let Ok(callback) = callbacks.try_recv() {
        run_callback(callback).await;
    }
    peer.await??;
    let error = outcome
        .err()
        .ok_or_else(|| std::io::Error::other("unfinished middleware unexpectedly succeeded"))?;
    let expected = if cancel {
        ErrorKind::Cancelled
    } else {
        ErrorKind::DeadlineExceeded
    };
    let phase = if cancel {
        None
    } else {
        Some(TimeoutPhase::Total)
    };
    check(
        error.kind() == expected && error.timeout_phase() == phase,
        "middleware lost its own cancellation or total budget",
    )
}

#[tokio::test]
async fn middleware_post_processing_remains_total_bounded() -> TestResult {
    middleware_work_remains_bounded(false).await
}

#[tokio::test]
async fn middleware_post_processing_remains_cancellable() -> TestResult {
    middleware_work_remains_bounded(true).await
}

#![cfg(feature = "http-client")]
use open_net::api::error::{ErrorKind, ErrorStage};
use open_net::api::http::{
    HttpClient, HttpClientConfig, HttpRequest, HttpRequestMethod, RetryEvent, RetryPolicy,
};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
fn check(ok: bool, message: &str) -> TestResult {
    if ok {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}
struct Peer(tokio::task::JoinHandle<TestResult>);
impl Drop for Peer {
    fn drop(&mut self) {
        self.0.abort();
    }
}
async fn peer() -> TestResult<(String, oneshot::Sender<()>, Peer)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    let (tx, rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await?;
        let mut request = Vec::new();
        let mut bytes = [0_u8; 512];
        while !request.windows(4).any(|chunk| chunk == b"\r\n\r\n") {
            let n = socket.read(&mut bytes).await?;
            if n == 0 {
                return Err(std::io::Error::other("request EOF").into());
            }
            request.extend_from_slice(&bytes[..n]);
        }
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\nConnection: close\r\n\r\n")
            .await?;
        let _ = rx.await;
        let _ = socket.write_all(b"x").await;
        Ok(())
    });
    Ok((url, tx, Peer(task)))
}
fn request(policy: RetryPolicy) -> TestResult<HttpRequest> {
    Ok(HttpRequest::builder()
        .path("/")
        .retry_policy(policy)
        .build()?)
}

#[tokio::test]
async fn stream_total_deadline_remains_active_after_headers() -> TestResult {
    let (url, release, _peer) = peer().await?;
    let mut config = HttpClientConfig::new(url)?;
    config.request_queue_capacity = 1;
    let client = HttpClient::new(config, "stream-total-budget")?;
    let mut response = client
        .stream(request(
            RetryPolicy::builder()
                .with_total_deadline(Duration::from_millis(60))
                .build()?,
        )?)
        .await?;
    let result = tokio::time::timeout(Duration::from_secs(2), response.next_chunk()).await?;
    check(
        matches!(result, Some(Err(ref e)) if e.kind() == ErrorKind::DeadlineExceeded && e.context().stage == Some(ErrorStage::Receive)),
        "body must observe total deadline",
    )?;
    tokio::time::timeout(Duration::from_secs(1), client.drain()).await??;
    check(
        response.next_chunk().await.is_none(),
        "terminal error must fuse stream",
    )?;
    let _ = release.send(());
    client.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn stream_slow_consumer_cannot_read_ready_chunk_after_attempt_deadline() -> TestResult {
    let (url, release, _peer) = peer().await?;
    let client = HttpClient::new(HttpClientConfig::new(url)?, "stream-consumer-budget")?;
    let mut response = client
        .stream(request(
            RetryPolicy::builder()
                .with_attempt_timeout(Duration::from_millis(60))
                .build()?,
        )?)
        .await?;
    let _ = release.send(());
    tokio::time::sleep(Duration::from_millis(100)).await;
    let item = response.next_chunk().await;
    check(
        matches!(item, Some(Err(ref e)) if e.kind() == ErrorKind::TimedOut),
        "expired ready data was delivered",
    )?;
    tokio::time::timeout(Duration::from_secs(1), client.drain()).await??;
    client.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn stream_default_deadline_covers_admission_wait() -> TestResult {
    let (url, release, _peer) = peer().await?;
    let mut config = HttpClientConfig::new(url)?;
    config.request_queue_capacity = 1;
    config.default_retry_policy = RetryPolicy::builder()
        .with_total_deadline(Duration::from_millis(60))
        .build()?;
    let client = HttpClient::new(config, "stream-admission-budget")?;
    let first = client.stream(request(RetryPolicy::no_retry())?).await?;
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        client.stream(HttpRequest::builder().path("/").build()?),
    )
    .await?;
    check(
        matches!(result, Err(ref e) if e.kind() == ErrorKind::DeadlineExceeded && e.context().stage == Some(ErrorStage::Queue)),
        "admission ignored default total deadline",
    )?;
    drop(first);
    let _ = release.send(());
    client.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn stream_observer_completes_only_after_body_eof() -> TestResult {
    let (url, release, _peer) = peer().await?;
    let client = HttpClient::new(HttpClientConfig::new(url)?, "stream-observer-eof")?;
    let events = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&events);
    let policy = RetryPolicy::builder()
        .with_observer(move |event: &RetryEvent| {
            if let Ok(mut values) = recorded.lock() {
                values.push(event.clone());
            }
        })
        .build()?;
    let mut response = client.stream(request(policy)?).await?;
    {
        let events = events
            .lock()
            .map_err(|_| std::io::Error::other("events poisoned"))?;
        check(
            events.len() == 1
                && matches!(
                    events.first(),
                    Some(RetryEvent::AttemptStarted { attempt: 1 })
                ),
            "headers must start observer without completing",
        )?;
    }
    let _ = release.send(());
    while let Some(chunk) = response.next_chunk().await {
        let _ = chunk?;
    }
    let events = events
        .lock()
        .map_err(|_| std::io::Error::other("events poisoned"))?
        .clone();
    check(
        events.len() == 3
            && matches!(events.last(), Some(RetryEvent::Completed(report)) if report.attempts == 1 && report.final_reason.is_none()),
        "EOF must complete observer exactly once",
    )?;
    client.shutdown().await?;
    Ok(())
}

async fn read_headers(socket: &mut tokio::net::TcpStream) -> TestResult {
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 512];
    while !bytes.windows(4).any(|value| value == b"\r\n\r\n") {
        let count = socket.read(&mut buffer).await?;
        if count == 0 {
            return Err(std::io::Error::other("request EOF").into());
        }
        bytes.extend_from_slice(&buffer[..count]);
    }
    Ok(())
}

#[tokio::test]
async fn old_request_future_drop_cannot_cancel_same_id_new_callback() -> TestResult {
    use async_trait::async_trait;
    use open_net::api::http::{
        HttpRequestId, HttpRequestOptions, HttpRequestTrait, HttpResponseResult,
    };
    struct Callback(oneshot::Sender<HttpResponseResult>);
    #[async_trait]
    impl HttpRequestTrait for Callback {
        fn get_path(&self) -> String {
            "/".to_owned()
        }
        fn get_method(&self) -> HttpRequestMethod {
            HttpRequestMethod::GET
        }
        async fn deal_with_response(self: Box<Self>, result: HttpResponseResult) {
            let _ = self.0.send(result);
        }
    }
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let client = HttpClient::new(
        HttpClientConfig::new(format!("http://{}", listener.local_addr()?))?,
        "guard-public-aba",
    )?;
    let (arrived_tx, arrived_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let _peer = Peer(tokio::spawn(async move {
        let (mut first, _) = listener.accept().await?;
        read_headers(&mut first).await?;
        first
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\nConnection: close\r\n\r\nx")
            .await?;
        drop(first);
        let (mut second, _) = listener.accept().await?;
        read_headers(&mut second).await?;
        let _ = arrived_tx.send(());
        let _ = release_rx.await;
        let _ = second
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\nConnection: close\r\n\r\ny")
            .await;
        Ok(())
    }));
    let mut old = Box::pin(client.get("/"));
    check(
        futures_util::poll!(old.as_mut()).is_pending(),
        "first request did not enter pending",
    )?;
    tokio::time::timeout(Duration::from_secs(2), client.drain()).await??;
    let (result_tx, result_rx) = oneshot::channel();
    client.send_with_options(
        Callback(result_tx),
        HttpRequestOptions::default().with_request_id(HttpRequestId(1)),
    )?;
    tokio::time::timeout(Duration::from_secs(2), arrived_rx).await??;
    drop(old);
    let _ = release_tx.send(());
    let response = tokio::time::timeout(Duration::from_secs(2), result_rx).await???;
    check(
        response.body.as_ref() == b"y",
        "old request corrupted replacement",
    )?;
    client.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn stream_retry_observer_has_six_events_and_body_drop_is_failure() -> TestResult {
    use open_net::api::http::RetryOn;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let client = HttpClient::new(
        HttpClientConfig::new(format!("http://{}", listener.local_addr()?))?,
        "stream-observer-retry",
    )?;
    let (_release, release_rx) = oneshot::channel::<()>();
    let _peer = Peer(tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await?;
        read_headers(&mut socket).await?;
        socket
            .write_all(
                b"HTTP/1.1 503 Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            )
            .await?;
        drop(socket);
        let (mut socket, _) = listener.accept().await?;
        read_headers(&mut socket).await?;
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\nConnection: close\r\n\r\n")
            .await?;
        let _ = release_rx.await;
        Ok(())
    }));
    let events = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&events);
    let policy = RetryPolicy::builder()
        .max_attempts(2)
        .retry_on(RetryOn::standard())
        .observer(move |event: &RetryEvent| {
            if let Ok(mut events) = recorded.lock() {
                events.push(event.clone());
            }
        })
        .build()?;
    let response = client.stream(request(policy)?).await?;
    check(response.attempts == 2, "status retry did not execute")?;
    check(
        events
            .lock()
            .map_err(|_| std::io::Error::other("events poisoned"))?
            .len()
            == 4,
        "observer completed at headers",
    )?;
    drop(response);
    let values = events
        .lock()
        .map_err(|_| std::io::Error::other("events poisoned"))?
        .clone();
    check(
        matches!(values.as_slice(), [RetryEvent::AttemptStarted { attempt: 1 }, RetryEvent::RetryScheduled { attempt: 1, .. }, RetryEvent::AttemptFinished { attempt: 1, final_attempt: false }, RetryEvent::AttemptStarted { attempt: 2 }, RetryEvent::AttemptFinished { attempt: 2, final_attempt: true }, RetryEvent::Completed(report)] if report.attempts == 2 && report.retry_count == 1 && report.last_error.as_deref() == Some("Cancelled")),
        "stream observer sequence or drop outcome was incorrect",
    )?;
    client.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_consumes_on_thread_without_tokio_enter() -> TestResult {
    let (url, release, _peer) = peer().await?;
    let client = HttpClient::new(HttpClientConfig::new(url)?, "stream-cross-executor")?;
    let mut response = client
        .stream(request(
            RetryPolicy::builder()
                .attempt_timeout(Duration::from_secs(2))
                .build()?,
        )?)
        .await?;
    let _ = release.send(());
    let thread = std::thread::spawn(move || -> TestResult {
        futures::executor::block_on(async {
            let chunk = response
                .next_chunk()
                .await
                .ok_or_else(|| std::io::Error::other("missing chunk"))??;
            check(chunk.as_ref() == b"x", "wrong cross-executor body")?;
            check(
                response.next_chunk().await.is_none(),
                "cross-executor EOF missing",
            )
        })
    });
    tokio::task::spawn_blocking(move || {
        thread
            .join()
            .map_err(|_| std::io::Error::other("consumer thread failed"))
    })
    .await???;
    client.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn stream_attempt_observer_can_cancel_before_network_io() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let client = HttpClient::new(
        HttpClientConfig::new(format!("http://{}", listener.local_addr()?))?,
        "stream-observer-cancel",
    )?;
    let canceller = client.clone();
    let policy = RetryPolicy::builder()
        .observer(move |event: &RetryEvent| {
            if matches!(event, RetryEvent::AttemptStarted { .. }) {
                let _ = canceller.cancel_all();
            }
        })
        .build()?;
    let result = client.stream(request(policy)?).await;
    check(
        matches!(result, Err(ref error) if error.kind() == ErrorKind::Cancelled),
        "observer cancellation did not stop opening",
    )?;
    check(
        tokio::time::timeout(Duration::from_millis(30), listener.accept())
            .await
            .is_err(),
        "observer cancellation still started network I/O",
    )?;
    client.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn zero_retry_defaults_apply_to_buffered_timeout() -> TestResult {
    for total in [false, true] {
        let (url, release, _peer) = peer().await?;
        let policy = if total {
            RetryPolicy::builder()
                .total_deadline(Duration::from_millis(60))
                .build()?
        } else {
            RetryPolicy::builder()
                .attempt_timeout(Duration::from_millis(60))
                .build()?
        };
        let mut config = HttpClientConfig::new(url)?;
        config.default_retry_policy = policy;
        let client = HttpClient::new(config, format!("buffered-zero-retry-{total}"))?;
        let result = tokio::time::timeout(Duration::from_secs(2), client.get("/")).await?;
        // Buffered timeout kinds intentionally retain their pre-existing contract.
        check(
            matches!(result, Err(ref error) if error.kind() == ErrorKind::TimedOut),
            "zero retry default timeout did not apply",
        )?;
        let _ = release.send(());
        client.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn stream_total_deadline_before_headers_reports_deadline_exceeded() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let client = HttpClient::new(
        HttpClientConfig::new(format!("http://{}", listener.local_addr()?))?,
        "stream-head-deadline",
    )?;
    let (_release, release_rx) = oneshot::channel::<()>();
    let _peer = Peer(tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await?;
        read_headers(&mut socket).await?;
        let _ = release_rx.await;
        Ok(())
    }));
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        client.stream(request(
            RetryPolicy::builder()
                .total_deadline(Duration::from_millis(60))
                .build()?,
        )?),
    )
    .await?;
    check(
        matches!(result, Err(ref error) if error.kind() == ErrorKind::DeadlineExceeded),
        "header total deadline lost its error kind",
    )?;
    tokio::time::timeout(Duration::from_secs(1), client.drain()).await??;
    client.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn stream_config_timeout_includes_paused_consumption() -> TestResult {
    let (url, release, _peer) = peer().await?;
    let mut config = HttpClientConfig::new(url)?;
    config.timeout = Duration::from_millis(60);
    let client = HttpClient::new(config, "stream-config-timeout")?;
    let mut response = client.stream(request(RetryPolicy::no_retry())?).await?;
    let _ = release.send(());
    tokio::time::sleep(Duration::from_millis(100)).await;
    check(
        matches!(response.next_chunk().await, Some(Err(ref error)) if error.kind() == ErrorKind::TimedOut),
        "config timeout lost during paused consumption",
    )?;
    client.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn dropped_stream_opening_cleans_before_completed_observer() -> TestResult {
    use std::sync::atomic::{AtomicBool, Ordering};
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let mut config = HttpClientConfig::new(format!("http://{}", listener.local_addr()?))?;
    config.request_queue_capacity = 1;
    let client = HttpClient::new(config, "stream-opening-drop")?;
    let (arrived_tx, arrived_rx) = oneshot::channel();
    let (_release, release_rx) = oneshot::channel::<()>();
    let _peer = Peer(tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await?;
        read_headers(&mut socket).await?;
        let _ = arrived_tx.send(());
        let _ = release_rx.await;
        Ok(())
    }));
    let cleanup_seen = Arc::new(AtomicBool::new(false));
    let observed = Arc::clone(&cleanup_seen);
    let reentrant = client.clone();
    let policy = RetryPolicy::builder().observer(move |event: &RetryEvent| {
        if matches!(event, RetryEvent::Completed(report) if report.attempts == 1 && report.last_error.as_deref() == Some("Cancelled")) {
            // This is a nonblocking capacity check using the public submit API.
            if let Ok(request) = HttpRequest::builder().path("/").build() {
                observed.store(reentrant.send(request).is_ok(), Ordering::SeqCst);
            }
        }
    }).build()?;
    let opening_client = client.clone();
    let task = tokio::spawn(async move {
        opening_client
            .stream(request(policy)?)
            .await
            .map_err(Into::into) as TestResult<_>
    });
    tokio::time::timeout(Duration::from_secs(2), arrived_rx).await??;
    task.abort();
    let _ = task.await;
    check(
        cleanup_seen.load(Ordering::SeqCst),
        "opening drop notified observer before returning quota",
    )?;
    client.shutdown().await?;
    Ok(())
}

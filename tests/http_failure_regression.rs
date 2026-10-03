#![cfg(feature = "http-client")]

use http::StatusCode;
use open_net::api::http::{
    Backoff, HttpClientConfig, HttpProxyConfig, HttpRequest, RetryOn, RetryPolicy,
    TransportErrorKind,
};
use open_net::error::{ErrorKind, ErrorStage};
use open_net::OpenNet;
use std::error::Error;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;
const TIMEOUT: Duration = Duration::from_secs(5);

fn check(condition: bool, message: impl Into<String>) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(std::io::Error::other(message.into()).into())
    }
}

struct Peer {
    address: std::net::SocketAddr,
    count: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<TestResult>,
}

impl Drop for Peer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn peer(first: &'static [u8]) -> TestResult<Peer> {
    peer_with_following(
        first,
        b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
    )
    .await
}

async fn peer_with_following(first: &'static [u8], following: &'static [u8]) -> TestResult<Peer> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let count = Arc::new(AtomicUsize::new(0));
    let received = Arc::clone(&count);
    let task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await?;
            let mut input = Vec::new();
            loop {
                let mut chunk = [0_u8; 1024];
                let n = tokio::time::timeout(TIMEOUT, stream.read(&mut chunk)).await??;
                if n == 0 {
                    return Err(std::io::Error::other("request ended before headers").into());
                }
                input.extend_from_slice(&chunk[..n]);
                if input.windows(4).any(|part| part == b"\r\n\r\n") {
                    break;
                }
                check(input.len() < 65536, "oversized request")?;
            }
            let ordinal = received.fetch_add(1, Ordering::SeqCst);
            let output: &[u8] = if ordinal == 0 { first } else { following };
            stream.write_all(output).await?;
            stream.shutdown().await?;
        }
    });
    Ok(Peer {
        address,
        count,
        task,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_classification_does_not_depend_on_url_text() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    drop(listener);
    let net = OpenNet::new()?;
    let config =
        HttpClientConfig::new(format!("http://{address}"))?.with_timeout(Duration::from_secs(1))?;
    let client = net
        .create_http_client_with_config("typed-tcp", config)
        .await?;
    for path in [
        "/plain",
        "/tls",
        "/dns",
        "/proxy",
        "/TLS?dns=proxy",
        "/%74ls",
    ] {
        let result = tokio::time::timeout(TIMEOUT, client.get_bytes(path)).await?;
        let error = match result {
            Err(error) => error,
            Ok(_) => return Err(std::io::Error::other("closed endpoint succeeded").into()),
        };
        check(
            error.kind() == ErrorKind::Io && error.context().stage == Some(ErrorStage::Tcp),
            format!("{path}: expected Io/Tcp, got {error:?}"),
        )?;
        check(error.source().is_some(), "connection source was lost")?;
    }
    net.destroy_http_client("typed-tcp").await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn body_retry_flag_and_legacy_transport_matrix() -> TestResult {
    for (body, transport) in [(false, false), (true, false), (false, true), (true, true)] {
        let server =
            peer(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nx").await?;
        let net = OpenNet::new()?;
        let config = HttpClientConfig::new(format!("http://{}", server.address))?
            .with_timeout(Duration::from_secs(1))?;
        let client = net
            .create_http_client_with_config("typed-body", config)
            .await?;
        let reasons = if transport {
            vec![TransportErrorKind::BodyRead]
        } else {
            vec![]
        };
        let policy = RetryPolicy::builder()
            .max_attempts(2)
            .backoff(Backoff::None)
            .retry_on(RetryOn::none().with_body_read(body).with_transport(reasons))
            .build()?;
        let request = HttpRequest::builder()
            .path("/body")
            .retry_policy(policy.clone())
            .build()?;
        let result = tokio::time::timeout(TIMEOUT, client.request(request)).await?;
        if body || transport {
            let response = result?;
            check(
                response.body.as_ref() == b"ok" && response.attempts == 2,
                "retry did not return only final complete body",
            )?;
        } else {
            let error = match result {
                Err(error) => error,
                Ok(_) => return Err(std::io::Error::other("unexpected retry").into()),
            };
            check(error.kind() == ErrorKind::Io, "truncation was not I/O")?;
            check(
                !policy.should_retry_error(&error),
                "disabled query still retries",
            )?;
        }
        check(
            server.count.load(Ordering::SeqCst) == if body || transport { 2 } else { 1 },
            "wrong request count",
        )?;
        net.destroy_http_client("typed-body").await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connect_407_retains_status_stage_and_source() -> TestResult {
    let server = peer(b"HTTP/1.1 407 Proxy Authentication Required\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await?;
    let net = OpenNet::new()?;
    let config = HttpClientConfig::new("https://localhost:443")?
        .with_proxy(HttpProxyConfig::http(format!("http://{}", server.address))?)
        .with_timeout(Duration::from_secs(1))?;
    let client = net
        .create_http_client_with_config("typed-proxy", config)
        .await?;
    let error = match tokio::time::timeout(TIMEOUT, client.get_bytes("/plain")).await? {
        Err(error) => error,
        Ok(_) => return Err(std::io::Error::other("407 tunnel succeeded").into()),
    };
    check(
        error.kind() == ErrorKind::HttpStatus
            && error.context().stage == Some(ErrorStage::Proxy)
            && error.http_status() == Some(StatusCode::PROXY_AUTHENTICATION_REQUIRED),
        format!("incorrect CONNECT rejection: {error:?}"),
    )?;
    check(error.source().is_some(), "tunnel source missing")?;
    check(server.count.load(Ordering::SeqCst) == 1, "407 was retried")?;
    net.destroy_http_client("typed-proxy").await?;
    Ok(())
}

#[test]
fn legacy_transport_disable_clears_body_flag() -> TestResult {
    let policy = RetryPolicy::standard().with_transport_errors(false);
    check(
        !policy.retry_on().retries_body_read(),
        "legacy disable left body_read enabled",
    )?;
    let restored = policy.with_transport_errors(true);
    check(
        !restored.retry_on().retries_body_read(),
        "legacy enable unexpectedly changed dedicated flag",
    )?;
    check(
        restored.should_retry_reason(open_net::api::http::RetryReason::Transport(
            TransportErrorKind::BodyRead,
        )),
        "legacy enable did not restore BodyRead transport",
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_body_requires_explicit_protocol_selection() -> TestResult {
    for protocol in [false, true] {
        let server = peer(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\nZ\r\nbad\r\n0\r\n\r\n").await?;
        let net = OpenNet::new()?;
        let client = net
            .create_http_client_with_config(
                "typed-protocol",
                HttpClientConfig::new(format!("http://{}", server.address))?,
            )
            .await?;
        let transports = if protocol {
            vec![TransportErrorKind::Protocol]
        } else {
            vec![]
        };
        let policy = RetryPolicy::builder()
            .max_attempts(2)
            .backoff(Backoff::None)
            .retry_on(
                RetryOn::none()
                    .with_body_read(true)
                    .with_transport(transports),
            )
            .build()?;
        let result = tokio::time::timeout(
            TIMEOUT,
            client.request(
                HttpRequest::builder()
                    .path("/chunk")
                    .retry_policy(policy.clone())
                    .build()?,
            ),
        )
        .await?;
        if protocol {
            check(
                result?.body.as_ref() == b"ok",
                "explicit protocol retry did not recover",
            )?;
        } else {
            let error = match result {
                Err(error) => error,
                Ok(_) => {
                    return Err(std::io::Error::other("body flag retried invalid framing").into())
                }
            };
            check(
                error.kind() == ErrorKind::Protocol && !policy.should_retry_error(&error),
                "invalid framing classification/query mismatch",
            )?;
        }
        check(
            server.count.load(Ordering::SeqCst) == if protocol { 2 } else { 1 },
            "wrong protocol retry count",
        )?;
        net.destroy_http_client("typed-protocol").await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn body_failure_observer_reports_only_final_outcome() -> TestResult {
    use open_net::api::http::{RetryEvent, RetryReason};
    const TRUNCATED: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nx";
    const COMPLETE: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok";
    for success in [true, false] {
        let server =
            peer_with_following(TRUNCATED, if success { COMPLETE } else { TRUNCATED }).await?;
        let net = OpenNet::new()?;
        let client = net
            .create_http_client_with_config(
                "typed-observer",
                HttpClientConfig::new(format!("http://{}", server.address))?,
            )
            .await?;
        let (tx, rx) = std::sync::mpsc::channel();
        let policy = RetryPolicy::builder()
            .max_attempts(2)
            .backoff(Backoff::None)
            .retry_on(RetryOn::none().with_body_read(true))
            .observer(move |event: &RetryEvent| {
                if let Err(error) = tx.send(event.clone()) {
                    eprintln!("observer receiver unavailable: {error}");
                }
            })
            .build()?;
        let result = tokio::time::timeout(
            TIMEOUT,
            client.request(
                HttpRequest::builder()
                    .path("/observer")
                    .retry_policy(policy)
                    .build()?,
            ),
        )
        .await?;
        check(result.is_ok() == success, "unexpected final body result")?;
        let events: Vec<_> = rx.try_iter().collect();
        let completed: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                RetryEvent::Completed(report) => Some(report),
                _ => None,
            })
            .collect();
        check(completed.len() == 1, "duplicate or missing Completed")?;
        let report = completed
            .first()
            .ok_or_else(|| std::io::Error::other("missing report"))?;
        check(
            report.attempts == 2 && report.retry_count == 1,
            "observer retry counts changed",
        )?;
        check(
            report.final_reason
                == if success {
                    None
                } else {
                    Some(RetryReason::BodyRead)
                },
            "final reason does not match final result",
        )?;
        check(events.len() == 6, "retry event sequence length changed")?;
        net.destroy_http_client("typed-observer").await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn response_header_eof_does_not_use_body_only_retry() -> TestResult {
    let server = peer(b"").await?;
    let net = OpenNet::new()?;
    let client = net
        .create_http_client_with_config(
            "typed-header-eof",
            HttpClientConfig::new(format!("http://{}", server.address))?,
        )
        .await?;
    let policy = RetryPolicy::builder()
        .max_attempts(2)
        .backoff(Backoff::None)
        .retry_on(RetryOn::none().with_body_read(true))
        .build()?;
    let result = tokio::time::timeout(
        TIMEOUT,
        client.request(
            HttpRequest::builder()
                .path("/head")
                .retry_policy(policy.clone())
                .build()?,
        ),
    )
    .await?;
    let error = match result {
        Err(error) => error,
        Ok(_) => {
            return Err(std::io::Error::other("header EOF was retried by body-only policy").into())
        }
    };
    check(
        !policy.should_retry_error(&error),
        "header EOF query used body-only flag",
    )?;
    check(
        server.count.load(Ordering::SeqCst) == 1,
        "body-only policy replayed opening failure",
    )?;
    net.destroy_http_client("typed-header-eof").await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn body_retry_preserves_method_safety() -> TestResult {
    use http::Method;
    use open_net::api::http::RequestSafety;
    for (method, safety, expected) in [
        (Method::GET, RequestSafety::Automatic, 2),
        (Method::PUT, RequestSafety::Automatic, 2),
        (Method::POST, RequestSafety::Automatic, 1),
        (Method::POST, RequestSafety::IdempotentByContract, 2),
        (Method::GET, RequestSafety::Never, 1),
    ] {
        let server =
            peer(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nx").await?;
        let net = OpenNet::new()?;
        let client = net
            .create_http_client_with_config(
                "typed-safety",
                HttpClientConfig::new(format!("http://{}", server.address))?,
            )
            .await?;
        let policy = RetryPolicy::builder()
            .max_attempts(2)
            .backoff(Backoff::None)
            .safety(safety)
            .retry_on(RetryOn::none().with_body_read(true))
            .build()?;
        let result = tokio::time::timeout(
            TIMEOUT,
            client.request(
                HttpRequest::builder()
                    .method(method)
                    .path("/safety")
                    .retry_policy(policy)
                    .build()?,
            ),
        )
        .await?;
        check(
            result.is_ok() == (expected == 2),
            "body retry bypassed method safety",
        )?;
        check(
            server.count.load(Ordering::SeqCst) == expected,
            "method safety request count mismatch",
        )?;
        net.destroy_http_client("typed-safety").await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_and_deadline_during_body_backoff_replace_previous_reason() -> TestResult {
    use open_net::api::http::{RetryEvent, RetryReason};
    for cancel in [false, true] {
        let server =
            peer(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nx").await?;
        let net = OpenNet::new()?;
        let client = net
            .create_http_client_with_config(
                "typed-final-reason",
                HttpClientConfig::new(format!("http://{}", server.address))?,
            )
            .await?;
        let observed_client = client.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        let policy = RetryPolicy::builder()
            .max_attempts(2)
            .backoff(Backoff::Constant(Duration::from_secs(1)))
            .total_deadline(Duration::from_millis(100))
            .retry_on(RetryOn::none().with_body_read(true))
            .observer(move |event: &RetryEvent| {
                if cancel && matches!(event, RetryEvent::RetryScheduled { .. }) {
                    if let Err(error) = observed_client.cancel_all() {
                        eprintln!("observer cancellation failed: {error}");
                    }
                }
                if let RetryEvent::Completed(report) = event {
                    if let Err(error) = tx.send(report.clone()) {
                        eprintln!("observer receiver unavailable: {error}");
                    }
                }
            })
            .build()?;
        let result = tokio::time::timeout(
            TIMEOUT,
            client.request(
                HttpRequest::builder()
                    .path("/backoff")
                    .retry_policy(policy)
                    .build()?,
            ),
        )
        .await?;
        let error = match result {
            Err(error) => error,
            Ok(_) => return Err(std::io::Error::other("backoff unexpectedly recovered").into()),
        };
        check(
            error.kind()
                == if cancel {
                    ErrorKind::Cancelled
                } else {
                    ErrorKind::DeadlineExceeded
                },
            "backoff final error was overwritten",
        )?;
        let reports: Vec<_> = rx.try_iter().collect();
        check(
            reports.len() == 1,
            "backoff has duplicate or missing Completed",
        )?;
        let report = reports
            .first()
            .ok_or_else(|| std::io::Error::other("missing backoff report"))?;
        check(
            report.attempts == 1 && report.retry_count == 1,
            "backoff counters changed",
        )?;
        check(
            report.final_reason == Some(RetryReason::Application),
            "previous body failure overwrote terminal reason",
        )?;
        check(
            server.count.load(Ordering::SeqCst) == 1,
            "backoff termination allowed another request",
        )?;
        net.destroy_http_client("typed-final-reason").await?;
    }
    Ok(())
}

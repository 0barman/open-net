#![cfg(feature = "http-client")]

#[path = "support/reader_upload.rs"]
mod fixture;

use bytes::Bytes;
use fixture::{
    check, digest, receive, Loopback, PauseAfterHeaders, TestResult, MIB, TARGET, WATCHDOG,
};
use http::{header, StatusCode};
use open_net::api::error::{ErrorKind, ErrorStage};
use open_net::api::http::{
    HttpClient, HttpRequest, HttpRequestBuilder, HttpRetryPolicy, HttpStatusPolicy, HttpTimeouts,
    RedirectPolicy, RetryOn, TimeoutPhase,
};
use open_net::api::retry::{Backoff, RetrySchedule};
use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::oneshot;
use tokio::time::timeout;

fn put() -> Result<HttpRequestBuilder, open_net::NetError> {
    HttpRequest::builder()
        .method("PUT")
        .path(TARGET)
        .header("content-type", "image/png")
}

#[derive(Default)]
struct Progress {
    bytes: AtomicU64,
    received: Arc<AtomicU64>,
    peak_read_ahead: AtomicU64,
    max_read_buffer: AtomicUsize,
    polls: AtomicUsize,
}

struct Meter<R> {
    inner: R,
    progress: Arc<Progress>,
}

impl<R> Meter<R> {
    fn new(inner: R) -> (Self, Arc<Progress>) {
        let progress = Arc::new(Progress::default());
        (
            Self {
                inner,
                progress: Arc::clone(&progress),
            },
            progress,
        )
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for Meter<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.progress.polls.fetch_add(1, Ordering::Relaxed);
        self.progress
            .max_read_buffer
            .fetch_max(buf.remaining(), Ordering::Relaxed);
        let before = buf.filled().len();
        let result = Pin::new(&mut self.inner).poll_read(cx, buf);
        if matches!(result, Poll::Ready(Ok(()))) {
            let count = buf.filled().len().saturating_sub(before);
            let count = count as u64;
            let total = self.progress.bytes.fetch_add(count, Ordering::Relaxed) + count;
            let received = self.progress.received.load(Ordering::Acquire);
            self.progress
                .peak_read_ahead
                .fetch_max(total.saturating_sub(received), Ordering::Relaxed);
        }
        result
    }
}

#[tokio::test]
async fn signed_put_keeps_content_type_target_body_and_status() -> TestResult {
    const BYTES: &[u8] = b"image\x00binary\ncontent";
    for (source, length, status) in [
        (BYTES, BYTES.len(), StatusCode::CREATED),
        (BYTES, BYTES.len(), StatusCode::FORBIDDEN),
        (BYTES, 3, StatusCode::OK),
        (BYTES, 0, StatusCode::OK),
    ] {
        let peer = Loopback::bind()?;
        let client = HttpClient::new(peer.config(false)?, "reader-wire")?;
        let length64 = u64::try_from(length)?;
        let (reader, progress) = Meter::new(source);
        let builder = put()?;
        let (response, request) = timeout(WATCHDOG, async {
            tokio::join!(
                client.upload(builder, reader, length64),
                receive(&peer, status, false, None)
            )
        })
        .await?;
        let (response, request) = (response?, request?);
        check(
            response.status == status && response.body.as_ref() == b"ok",
            "buffered status or response body was lost",
        )?;
        check(
            response.check_status(HttpStatusPolicy::Success2xx).is_ok() == status.is_success(),
            "HTTP status policy ignored the storage response",
        )?;
        check(
            request.start_line == format!("PUT {TARGET} HTTP/1.1"),
            "signed request target or method changed",
        )?;
        check(
            request
                .headers
                .get(header::CONTENT_TYPE)
                .is_some_and(|h| h == "image/png"),
            "wrong Content-Type",
        )?;
        check(request.length == length64, "wrong Content-Length")?;
        let mut expected = digest()?;
        expected.update(source.get(..length).ok_or("invalid expected body range")?);
        check(
            request.digest == expected.finish().as_ref(),
            "wire body digest differs",
        )?;
        check(
            progress.bytes.load(Ordering::Relaxed) == length64,
            "reader over-read or skipped bytes",
        )?;
        if length == 0 {
            check(
                progress.polls.load(Ordering::Relaxed) == 0,
                "zero-length upload polled the reader",
            )?;
        }
        peer.no_more_connections()?;
        client.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn explicit_builder_bodies_are_rejected_before_reading_or_connecting() -> TestResult {
    let peer = Loopback::bind()?;
    let client = HttpClient::new(peer.config(false)?, "reader-conflict")?;
    let builders = vec![
        put()?.body(Bytes::new()),
        put()?.body(Bytes::from_static(b"buffered")),
    ];
    #[cfg(feature = "http-client-codecs")]
    let builders = {
        use open_net::api::http::Multipart;
        let mut builders = builders;
        let raw = || HttpRequest::builder().method("PUT").path("/conflict");
        builders.push(raw().json(&serde_json::json!({}))?);
        builders.push(raw().form(&serde_json::json!({"field": "value"}))?);
        builders.push(raw().multipart(Multipart::new())?);
        builders
    };
    for builder in builders {
        let (reader, progress) = Meter::new(&b"x"[..]);
        let error = client
            .upload(builder, reader, 1)
            .await
            .err()
            .ok_or("accepted conflicting upload body")?;
        check(
            error.kind() == ErrorKind::InvalidInput
                && error.context().stage == Some(ErrorStage::RequestBuild),
            "body conflict returned the wrong error",
        )?;
        check(
            progress.polls.load(Ordering::Relaxed) == 0,
            "reader polled for conflicting upload",
        )?;
    }
    peer.no_more_connections()?;
    client.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn sdk_managed_length_and_framing_headers_are_rejected_before_sending() -> TestResult {
    let peer = Loopback::bind()?;
    let client = HttpClient::new(peer.config(false)?, "reader-headers")?;
    for (name, value) in [("content-length", "999"), ("transfer-encoding", "chunked")] {
        let builder = put()?.header(name, value)?;
        let (reader, progress) = Meter::new(&b"x"[..]);
        check(
            client.upload(builder, reader, 1).await.is_err(),
            "caller overrode SDK-managed framing",
        )?;
        check(
            progress.polls.load(Ordering::Relaxed) == 0,
            "reader polled for invalid framing",
        )?;
    }
    peer.no_more_connections()?;
    client.shutdown().await?;
    Ok(())
}

// A caller-owned reader can be Send without implementing Sync or Unpin.
struct ReadFailure {
    _not_sync: std::cell::Cell<()>,
    _pinned: std::marker::PhantomPinned,
}
impl AsyncRead for ReadFailure {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Ready(Err(io::ErrorKind::PermissionDenied.into()))
    }
}

#[tokio::test]
async fn early_eof_and_read_errors_preserve_io_kind_and_stage() -> TestResult {
    verify_failure(&b"1234567"[..], io::ErrorKind::UnexpectedEof).await?;
    verify_failure(
        ReadFailure {
            _not_sync: std::cell::Cell::new(()),
            _pinned: std::marker::PhantomPinned,
        },
        io::ErrorKind::PermissionDenied,
    )
    .await
}

async fn verify_failure(
    reader: impl AsyncRead + Send + 'static,
    expected: io::ErrorKind,
) -> TestResult {
    let peer = Loopback::bind()?;
    let client = HttpClient::new(peer.config(false)?, "reader-io")?;
    let error = timeout(WATCHDOG, client.upload(put()?, reader, 10))
        .await?
        .err()
        .ok_or("incomplete reader upload succeeded")?;
    check(
        error.kind() == ErrorKind::Io
            && error.context().stage == Some(ErrorStage::Write)
            && error.io_kind() == Some(expected),
        &format!("unexpected reader error for {expected:?}: {error:?}"),
    )?;
    client.shutdown().await?;
    Ok(())
}

fn retry(status: StatusCode) -> TestResult<HttpRetryPolicy> {
    Ok(HttpRetryPolicy::builder()
        .schedule(
            RetrySchedule::builder()
                .max_attempts(3)
                .backoff(Backoff::None)
                .build()?,
        )
        .retry_on(RetryOn::none().with_statuses([status]))
        .build()?)
}

#[tokio::test]
async fn single_use_reader_stops_body_preserving_redirects_and_status_retries() -> TestResult {
    for status in [
        StatusCode::MOVED_PERMANENTLY,
        StatusCode::FOUND,
        StatusCode::TEMPORARY_REDIRECT,
        StatusCode::PERMANENT_REDIRECT,
        StatusCode::SERVICE_UNAVAILABLE,
    ] {
        let peer = Loopback::bind()?;
        let config = peer
            .config(false)?
            .with_redirect_policy(RedirectPolicy::same_origin())
            .with_default_retry_policy(retry(status)?);
        let client = HttpClient::new(config, "reader-single-use")?;
        let request = put()?;
        let (response, wire) = timeout(WATCHDOG, async {
            tokio::join!(
                client.upload(request, &b"x"[..], 1),
                receive(&peer, status, false, None)
            )
        })
        .await?;
        check(
            response?.status == status && wire?.length == 1,
            "original PUT status was lost",
        )?;
        peer.no_more_connections()?;
        client.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn body_dropping_redirects_follow_with_get_and_do_not_retry_the_original_upload() -> TestResult
{
    for (method, status) in [
        ("POST", StatusCode::MOVED_PERMANENTLY),
        ("POST", StatusCode::FOUND),
        ("PUT", StatusCode::SEE_OTHER),
    ] {
        let peer = Loopback::bind()?;
        let retryable = StatusCode::SERVICE_UNAVAILABLE;
        let config = peer
            .config(false)?
            .with_redirect_policy(RedirectPolicy::same_origin())
            .with_default_retry_policy(retry(retryable)?);
        let client = HttpClient::new(config, "reader-redirect-get")?;
        let builder = put()?.method(method);
        let source = &b"single-use body"[..];
        let length = u64::try_from(source.len())?;
        let (reader, progress) = Meter::new(source);
        let service = async {
            let initial = receive(&peer, status, false, None).await?;
            let next = receive(&peer, retryable, false, None).await?;
            TestResult::Ok((initial, next))
        };
        let (response, wire) = timeout(WATCHDOG, async {
            tokio::join!(client.upload(builder, reader, length), service)
        })
        .await?;
        let (initial, next) = wire?;
        check(
            response?.status == retryable,
            "did not return the final GET response",
        )?;
        check(
            initial.start_line == format!("{method} {TARGET} HTTP/1.1") && initial.length == length,
            "initial upload request was changed",
        )?;
        let mut hash = digest()?;
        hash.update(source);
        check(
            initial.digest == hash.finish().as_ref()
                && progress.bytes.load(Ordering::Relaxed) == length,
            "original upload body was lost",
        )?;
        check(
            next.start_line == "GET /replay HTTP/1.1" && next.length == 0,
            "redirect replayed the reader instead of following with GET",
        )?;
        check(
            !next.headers.contains_key(header::CONTENT_LENGTH)
                && !next.headers.contains_key(header::CONTENT_TYPE),
            "body metadata leaked onto redirected GET",
        )?;
        peer.no_more_connections()?;
        client.shutdown().await?;
    }
    Ok(())
}

#[cfg(feature = "http-client-http2")]
#[tokio::test]
async fn h2_refused_stream_does_not_replay_a_reader_at_either_retry_layer() -> TestResult {
    use open_net::api::http::HttpVersionPolicy;
    use open_net::api::retry::RetryDecision;

    for length in [0, 4] {
        let peer = Loopback::bind()?;
        let policy = HttpRetryPolicy::builder()
            .schedule(
                RetrySchedule::builder()
                    .max_attempts(3)
                    .backoff(Backoff::None)
                    .build()?,
            )
            .classifier(|_| RetryDecision::Retry)
            .build()?;
        let config = peer
            .config(true)?
            .with_http_version_policy(HttpVersionPolicy::Http2Only)
            .with_default_retry_policy(policy);
        let client = HttpClient::new(config, "reader-refused")?;
        let (done, mut stop) = oneshot::channel();
        let serve = async {
            let (socket, _) = peer.listener.accept().await?;
            let tls = fixture::tls_acceptor(b"h2")?.accept(socket).await?;
            let mut conn = h2::server::handshake(tls).await?;
            let mut count = 0_usize;
            loop {
                tokio::select! {
                    inbound = conn.accept() => {
                        let Some(inbound) = inbound else { break };
                        let (request, mut reply) = inbound?;
                        check(request.method() == http::Method::PUT, "unexpected method on H2 fixture")?;
                        count += 1;
                        // Refused streams are eligible for reqwest's built-in retries on cloneable bodies.
                        reply.send_reset(h2::Reason::REFUSED_STREAM);
                    }
                    _ = &mut stop => break,
                }
            }
            TestResult::Ok(count)
        };
        let request = put()?;
        let upload = async {
            let result = client.upload(request, &b"once"[..], length).await;
            let _ = done.send(());
            result
        };
        let (result, requests) = timeout(WATCHDOG, async { tokio::join!(upload, serve) }).await?;
        let error = result.err().ok_or("reset HTTP/2 upload succeeded")?;
        check(
            error
                .retry_report()
                .is_some_and(|report| report.attempts == 1),
            "refused upload consumed multiple SDK attempts",
        )?;
        check(
            requests? == 1,
            "transport replayed the reader in another H2 stream",
        )?;
        peer.no_more_connections()?;
        client.shutdown().await?;
    }
    Ok(())
}

struct PendingReader {
    polled: Option<oneshot::Sender<()>>,
    dropped: Option<oneshot::Sender<()>>,
}

impl PendingReader {
    fn observe() -> (Self, oneshot::Receiver<()>, oneshot::Receiver<()>) {
        let (started, ready) = oneshot::channel();
        let (on_drop, dropped) = oneshot::channel();
        (
            Self {
                polled: Some(started),
                dropped: Some(on_drop),
            },
            ready,
            dropped,
        )
    }
}

impl AsyncRead for PendingReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if let Some(polled) = self.polled.take() {
            let _ = polled.send(());
        }
        Poll::Pending
    }
}

impl Drop for PendingReader {
    fn drop(&mut self) {
        if let Some(signal) = self.dropped.take() {
            let _ = signal.send(());
        }
    }
}

#[tokio::test]
async fn cancelling_or_dropping_an_active_upload_releases_its_pending_reader() -> TestResult {
    let peer = Loopback::bind()?;
    let client = HttpClient::new(peer.config(false)?, "reader-cancel")?;
    for cancel_explicitly in [false, true] {
        let (reader, ready, dropped) = PendingReader::observe();
        let mut upload = Box::pin(client.upload(put()?, reader, 10));
        timeout(WATCHDOG, async {
            tokio::select! {
                result = &mut upload => Err(io::Error::other(format!("upload finished while its reader was pending: {result:?}"))),
                signal = ready => signal.map_err(io::Error::other),
            }
        }).await??;
        if cancel_explicitly {
            client.cancel_all()?;
            check(
                timeout(WATCHDOG, upload).await?.is_err_and(|error| {
                    error.kind() == ErrorKind::Cancelled
                        && error.context().http_request_id.is_some()
                }),
                "active upload lost its cancellation or request ID",
            )?;
        } else {
            drop(upload);
        }
        timeout(WATCHDOG, dropped).await??;
        timeout(WATCHDOG, client.drain()).await??;
    }
    client.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn upload_headers_budget_also_limits_waiting_for_source_bytes() -> TestResult {
    let peer = Loopback::bind()?;
    let client = HttpClient::new(peer.config(false)?, "reader-timeout")?;
    let (reader, ready, dropped) = PendingReader::observe();
    let builder =
        put()?.timeouts(HttpTimeouts::new().with_headers_timeout(Duration::from_millis(500)));
    let error = timeout(WATCHDOG, client.upload(builder, reader, 10))
        .await?
        .err()
        .ok_or("a blocked reader completed successfully")?;
    timeout(WATCHDOG, ready).await??;
    check(
        error.kind() == ErrorKind::TimedOut && error.timeout_phase() == Some(TimeoutPhase::Headers),
        &format!("unexpected timeout for a blocked reader: {error:?}"),
    )?;
    timeout(WATCHDOG, dropped).await??;
    client.shutdown().await?;
    Ok(())
}

#[cfg(feature = "http-client-auth")]
#[tokio::test]
async fn credentials_apply_once_and_a_401_cannot_refresh_a_single_use_upload() -> TestResult {
    use open_net::api::http::{
        AuthAction, AuthRequestContext, AuthResponseContext, AuthResult, HttpAuth,
    };
    struct Auth(Arc<AtomicUsize>);
    #[open_net::async_trait]
    impl HttpAuth for Auth {
        async fn apply(&self, ctx: &mut AuthRequestContext<'_>) -> AuthResult<()> {
            ctx.set_header(
                header::AUTHORIZATION,
                http::HeaderValue::from_static("Bearer fixture"),
            )
        }
        async fn on_unauthorized(&self, _: &AuthResponseContext<'_>) -> AuthResult<AuthAction> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(AuthAction::RetryWithFreshCredentials)
        }
    }

    let peer = Loopback::bind()?;
    let challenges = Arc::new(AtomicUsize::new(0));
    let config = peer
        .config(false)?
        .with_default_retry_policy(retry(StatusCode::UNAUTHORIZED)?)
        .with_auth(Auth(Arc::clone(&challenges)));
    let client = HttpClient::new(config, "reader-auth")?;
    let request = put()?;
    let (response, wire) = timeout(WATCHDOG, async {
        tokio::join!(
            client.upload(request, &b"x"[..], 1),
            receive(&peer, StatusCode::UNAUTHORIZED, false, None)
        )
    })
    .await?;
    check(
        response?.status == StatusCode::UNAUTHORIZED,
        "401 was not returned",
    )?;
    check(
        wire?
            .headers
            .get(header::AUTHORIZATION)
            .is_some_and(|h| h == "Bearer fixture"),
        "auth headers were not applied",
    )?;
    check(
        challenges.load(Ordering::Relaxed) == 0,
        "credentials refreshed for consumed body",
    )?;
    peer.no_more_connections()?;
    client.shutdown().await?;
    Ok(())
}

#[cfg(feature = "http-client-middleware")]
#[tokio::test]
async fn upload_middleware_can_forward_headers_or_return_without_polling_the_reader() -> TestResult
{
    use open_net::api::http::{
        HttpCall, HttpMiddleware, HttpMiddlewareResponse, HttpResponse, Next,
    };

    struct Route;
    #[open_net::async_trait]
    impl HttpMiddleware for Route {
        async fn call(
            &self,
            mut call: HttpCall,
            next: Next,
        ) -> Result<HttpMiddlewareResponse, open_net::NetError> {
            if call.request().path == "/local-reply" {
                return Ok(HttpMiddlewareResponse::Buffered(HttpResponse::synthetic(
                    call.request_id(),
                    StatusCode::ACCEPTED,
                    http::HeaderMap::new(),
                    Bytes::from_static(b"local"),
                )));
            }
            call.request_mut()
                .headers
                .insert("x-upload-route", http::HeaderValue::from_static("storage"));
            next.run(call).await
        }
    }

    let peer = Loopback::bind()?;
    let client = HttpClient::new(peer.config(false)?.with_middleware(Route), "reader-forward")?;
    let data = &b"original\x00upload bytes"[..];
    let length = u64::try_from(data.len())?;
    let request = put()?;
    let (response, observed) = timeout(WATCHDOG, async {
        tokio::join!(
            client.upload(request, data, length),
            receive(&peer, StatusCode::CREATED, false, None)
        )
    })
    .await?;
    check(
        response?.status == StatusCode::CREATED,
        "middleware did not return the peer response",
    )?;
    let wire = observed?;
    check(
        wire.start_line == format!("PUT {TARGET} HTTP/1.1"),
        "middleware dispatch changed signed target",
    )?;
    check(
        wire.headers
            .get("x-upload-route")
            .is_some_and(|value| value == "storage"),
        "middleware header did not reach the peer",
    )?;
    let mut expected = digest()?;
    expected.update(data);
    check(
        wire.length == length && wire.digest == expected.finish().as_ref(),
        "middleware dispatch lost the reader body",
    )?;

    let (reader, progress) = Meter::new(data);
    let local = HttpRequest::builder().method("PUT").path("/local-reply");
    let response = timeout(WATCHDOG, client.upload(local, reader, length)).await??;
    check(
        response.status == StatusCode::ACCEPTED && response.body.as_ref() == b"local",
        "middleware synthetic response was lost",
    )?;
    check(
        progress.polls.load(Ordering::Relaxed) == 0,
        "short-circuit polled the reader",
    )?;
    peer.no_more_connections()?;
    client.shutdown().await?;
    Ok(())
}

#[cfg(feature = "http-client-middleware")]
#[tokio::test]
async fn middleware_cannot_replace_reader_with_a_buffered_body() -> TestResult {
    use open_net::api::http::{HttpCall, HttpMiddleware, HttpMiddlewareResponse, Next};
    struct Replace;
    #[open_net::async_trait]
    impl HttpMiddleware for Replace {
        async fn call(
            &self,
            mut call: HttpCall,
            next: Next,
        ) -> Result<HttpMiddlewareResponse, open_net::NetError> {
            call.request_mut().body = Bytes::from_static(b"replacement");
            next.run(call).await
        }
    }
    let peer = Loopback::bind()?;
    let client = HttpClient::new(
        peer.config(false)?.with_middleware(Replace),
        "reader-middleware",
    )?;
    let (reader, progress) = Meter::new(&b"x"[..]);
    let error = client
        .upload(put()?, reader, 1)
        .await
        .err()
        .ok_or("middleware replaced the reader body")?;
    check(
        error.kind() == ErrorKind::InvalidInput
            && error.context().stage == Some(ErrorStage::RequestBuild),
        "wrong middleware conflict error",
    )?;
    check(
        progress.polls.load(Ordering::Relaxed) == 0,
        "conflicting middleware consumed the reader",
    )?;
    peer.no_more_connections()?;
    client.shutdown().await?;
    Ok(())
}

// Deliberately use real, differently sized files and compare streaming SHA-256
// hashes. Neither the client fixture nor the peer ever collects the file.
struct FileFixture(PathBuf);
impl Drop for FileFixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn create_file(mib: u64) -> TestResult<(FileFixture, std::fs::File, Vec<u8>)> {
    use std::io::{Seek, Write};
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "open-net-upload-{}-{stamp}-{mib}",
        std::process::id()
    ));
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&path)?;
    let guard = FileFixture(path);
    let mut hash = digest()?;
    let mut block = vec![0_u8; 64 * 1024];
    for (index, byte) in block.iter_mut().enumerate() {
        *byte = (index % 251) as u8;
    }
    let blocks = mib * MIB / u64::try_from(block.len())?;
    for index in 0..blocks {
        block
            .get_mut(..8)
            .ok_or("invalid fixture block")?
            .copy_from_slice(&index.to_le_bytes());
        file.write_all(&block)?;
        hash.update(&block);
    }
    file.rewind()?;
    Ok((guard, file, hash.finish().as_ref().to_vec()))
}

#[tokio::test]
#[ignore = "pre-release test transfers 550 MiB over loopback TLS; run with --ignored"]
async fn large_https_puts_preserve_digest_with_bounded_reader_demand() -> TestResult {
    let budget = Duration::from_secs(120);
    let mut max_reads = Vec::new();
    for mib in [50, 500] {
        let (_file_guard, file, expected) =
            tokio::task::spawn_blocking(move || create_file(mib)).await??;
        let (reader, progress) = Meter::new(tokio::fs::File::from_std(file));
        let peer = Loopback::bind()?;
        let client = HttpClient::new(
            peer.config(true)?.with_timeout(budget)?,
            "reader-large-https",
        )?;
        let (ready, headers_read) = oneshot::channel();
        let (resume, resume_reading) = oneshot::channel();
        let pause = PauseAfterHeaders {
            ready,
            resume: resume_reading,
            received: Arc::clone(&progress.received),
        };
        let request = put()?;
        let length = mib * MIB;
        let (response, wire, observed) = timeout(budget, async {
            tokio::join!(
                client.upload(request, reader, length),
                receive(&peer, StatusCode::CREATED, true, Some(pause)),
                async {
                    headers_read.await?;
                    // Leave the TLS peer paused while the socket's send buffer fills.
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    let prefetch = progress.bytes.load(Ordering::Relaxed);
                    resume
                        .send(())
                        .map_err(|_| "TLS peer dropped before resuming")?;
                    check(
                        prefetch < 32 * MIB,
                        "reader ran ahead of a paused receiver by >=32 MiB",
                    )
                }
            )
        })
        .await?;
        observed?;
        check(response?.status == StatusCode::CREATED, "large PUT failed")?;
        let wire = wire?;
        check(
            wire.length == length && wire.digest == expected,
            "large upload length or SHA-256 differs",
        )?;
        check(
            wire.start_line == format!("PUT {TARGET} HTTP/1.1"),
            "signed HTTPS target changed",
        )?;
        check(
            wire.headers
                .get(header::CONTENT_TYPE)
                .is_some_and(|h| h == "image/png"),
            "signed HTTPS content type changed",
        )?;
        check(
            progress.bytes.load(Ordering::Relaxed) == length,
            "upload progress did not reach file length",
        )?;
        // Source minus peer consumption includes the SDK, TLS and kernel buffers.
        // It gives a conservative wire-level bound without inferring process RSS.
        let peak = progress.peak_read_ahead.load(Ordering::Relaxed);
        let max_read = progress.max_read_buffer.load(Ordering::Relaxed);
        check(
            peak < 32 * MIB,
            "source read ahead of the peer by >=32 MiB during upload",
        )?;
        eprintln!(
            "{mib} MiB PUT: max reader buffer {max_read} bytes, peak read-ahead {peak} bytes"
        );
        max_reads.push(max_read);
        peer.no_more_connections()?;
        client.shutdown().await?;
    }
    let [small, large] = max_reads.as_slice() else {
        return Err("missing read-buffer observations".into());
    };
    check(
        small == large && *large > 0 && *large < MIB as usize,
        "reader buffer demand grew with the file",
    )
}

#[tokio::test]
async fn reader_upload_h1_releases_source_before_the_end_of_an_early_rejection() -> TestResult {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let peer = Loopback::bind()?;
    let client = HttpClient::new(peer.config(false)?, "early-h1-upload")?;
    let (reader, mut writer) = tokio::io::duplex(32);
    writer.write_all(b"x").await?;
    let request = put()?;
    let server = async {
        let (mut socket, _) = peer.listener.accept().await?;
        fixture::read_head(&mut socket).await?;
        check(
            socket.read_u8().await? == b'x',
            "wrong initial request body",
        )?;
        socket
            .write_all(
                b"HTTP/1.1 403 Forbidden\r\nContent-Length: 6\r\nConnection: close\r\n\r\nden",
            )
            .await?;
        // Deliver the rest only after the SDK releases the stalled reader.
        // Closing the HTTP/1 connection at the early head loses these bytes.
        check(
            writer.read(&mut [0]).await? == 0,
            "upload source stayed open",
        )?;
        socket.write_all(b"ied").await?;
        socket.shutdown().await?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    let (served, upload) = timeout(WATCHDOG, async {
        tokio::join!(server, client.upload(request, reader, 512))
    })
    .await?;
    served?;
    let response = upload?;
    check(
        response.status == StatusCode::FORBIDDEN && response.body.as_ref() == b"denied",
        "lost the early HTTP/1 rejection or its response body",
    )?;
    client.shutdown().await?;
    Ok(())
}

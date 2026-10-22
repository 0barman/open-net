#![cfg(feature = "http-client")]

use open_net::api::retry::{RetryEvent, RetrySetting};

use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use http::HeaderMap;
use open_net::api::error::{ErrorKind, ErrorStage, NetError};
use open_net::api::http::{
    HttpAdmissionPolicy, HttpClient, HttpClientConfig, HttpRequest, HttpRequestHandle,
    HttpRequestMethod, HttpRequestTrait, HttpResponse, HttpResponseResult, HttpRetryPolicy,
    HttpStreamResponse, HttpTimeouts, TimeoutPhase,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::task::{JoinHandle, JoinSet};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
const WATCHDOG: Duration = Duration::from_secs(5);

fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}

struct ObservedRequest {
    request_line: String,
    reply: oneshot::Sender<Reply>,
}

enum Reply {
    Complete,
    Streaming(oneshot::Receiver<()>),
}

impl ObservedRequest {
    fn respond(self) -> TestResult {
        self.reply
            .send(Reply::Complete)
            .map_err(|_| std::io::Error::other("peer connection stopped before response"))?;
        Ok(())
    }

    fn start_stream(self) -> TestResult<oneshot::Sender<()>> {
        let (release, body) = oneshot::channel();
        self.reply
            .send(Reply::Streaming(body))
            .map_err(|_| std::io::Error::other("peer connection stopped before stream"))?;
        Ok(release)
    }
}

struct Peer {
    url: String,
    requests: mpsc::UnboundedReceiver<ObservedRequest>,
    task: JoinHandle<()>,
}

impl Peer {
    async fn new() -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let (sender, requests) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let mut workers = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let Ok((stream, _)) = accepted else { break };
                        let sender = sender.clone();
                        workers.spawn(async move { serve(stream, sender).await });
                    }
                    Some(result) = workers.join_next(), if !workers.is_empty() => {
                        if let Err(error) = result {
                            eprintln!("admission peer worker failed: {error}");
                        }
                    }
                }
            }
        });
        Ok(Self {
            url,
            requests,
            task,
        })
    }

    fn config(&self, capacity: usize) -> TestResult<HttpClientConfig> {
        Ok(HttpClientConfig::new(&self.url)?
            .with_request_queue_capacity(capacity)?
            .with_environment_proxy(false))
    }

    fn client(&self, capacity: usize) -> TestResult<HttpClient> {
        Ok(HttpClient::new(self.config(capacity)?, "admission-tests")?)
    }

    async fn next(&mut self) -> TestResult<ObservedRequest> {
        tokio::time::timeout(WATCHDOG, self.requests.recv())
            .await?
            .ok_or_else(|| std::io::Error::other("peer stopped before request").into())
    }

    fn no_request(&mut self) -> TestResult {
        check(
            self.requests.try_recv().is_err(),
            "unexpected network request",
        )
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve(mut stream: TcpStream, sender: mpsc::UnboundedSender<ObservedRequest>) {
    let mut bytes = Vec::new();
    let mut chunk = [0; 4096];
    loop {
        if bytes.windows(4).any(|value| value == b"\r\n\r\n") {
            break;
        }
        match stream.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(length) => bytes.extend_from_slice(&chunk[..length]),
        }
    }
    let request_line = match String::from_utf8_lossy(&bytes).lines().next() {
        Some(line) => line.to_owned(),
        None => return,
    };
    let (reply, response) = oneshot::channel();
    if sender
        .send(ObservedRequest {
            request_line,
            reply,
        })
        .is_err()
    {
        return;
    }
    let Ok(response) = response.await else { return };
    let sent = match response {
        Reply::Complete => {
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                .await
        }
        Reply::Streaming(body) => {
            if stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n")
                .await
                .is_err()
            {
                return;
            }
            if body.await.is_err() {
                return;
            }
            stream.write_all(b"ok").await
        }
    };
    if let Err(error) = sent {
        eprintln!("admission peer response failed: {error}");
    }
}

struct CallbackRequest {
    path: &'static str,
    calls: Arc<AtomicUsize>,
}

#[open_net::async_trait]
impl HttpRequestTrait for CallbackRequest {
    fn get_path(&self) -> String {
        self.path.to_owned()
    }
    fn get_method(&self) -> HttpRequestMethod {
        HttpRequestMethod::GET
    }
    async fn deal_with_response(self: Box<Self>, _result: HttpResponseResult) {
        self.calls.fetch_add(1, Ordering::SeqCst);
    }
}

async fn occupy(
    peer: &mut Peer,
    client: &HttpClient,
) -> TestResult<(HttpRequestHandle, ObservedRequest)> {
    let handle = client
        .send(CallbackRequest {
            path: "/occupant",
            calls: Arc::new(AtomicUsize::new(0)),
        })
        .map_err(|error| error.into_error())?;
    let observed = peer.next().await?;
    check(
        observed.request_line == "GET /occupant HTTP/1.1",
        "wrong occupant request",
    )?;
    Ok((handle, observed))
}

async fn finish_occupant(handle: &HttpRequestHandle, observed: ObservedRequest) -> TestResult {
    observed.respond()?;
    let completion = tokio::time::timeout(WATCHDOG, handle.finished()).await?;
    completion.response?;
    completion.callback?;
    Ok(())
}

async fn respond_buffered(
    peer: &mut Peer,
    future: impl Future<Output = Result<HttpResponse, NetError>>,
    request_line: &str,
) -> TestResult<HttpResponse> {
    let (response, ()) = tokio::try_join!(
        async {
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                tokio::time::timeout(WATCHDOG, future).await??,
            )
        },
        async {
            let observed = peer.next().await?;
            check(
                observed.request_line == request_line,
                "wrong buffered request",
            )?;
            observed.respond()
        },
    )?;
    Ok(response)
}

async fn open_stream(
    peer: &mut Peer,
    future: impl Future<Output = Result<HttpStreamResponse, NetError>>,
    request_line: &str,
) -> TestResult<(HttpStreamResponse, oneshot::Sender<()>)> {
    tokio::try_join!(
        async {
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                tokio::time::timeout(WATCHDOG, future).await??,
            )
        },
        async {
            let observed = peer.next().await?;
            check(
                observed.request_line == request_line,
                "wrong streaming request",
            )?;
            observed.start_stream()
        },
    )
}

fn failure<T>(result: Result<T, NetError>) -> TestResult<NetError> {
    match result {
        Err(error) => Ok(error),
        Ok(_) => Err(std::io::Error::other("request unexpectedly succeeded").into()),
    }
}

async fn empty_client(client: &HttpClient, peer: &mut Peer) -> TestResult {
    tokio::time::timeout(WATCHDOG, client.drain()).await??;
    peer.no_request()
}

#[tokio::test]
async fn default_buffered_waits_until_capacity_is_released() -> TestResult {
    let mut peer = Peer::new().await?;
    let client = peer.client(1)?;
    let mut first = Box::pin(client.get("/first"));
    check(
        futures_util::poll!(first.as_mut()).is_pending(),
        "first request did not start",
    )?;
    let observed = peer.next().await?;
    check(
        observed.request_line == "GET /first HTTP/1.1",
        "wrong first request",
    )?;

    let mut second = Box::pin(client.get("/second"));
    check(
        futures_util::poll!(second.as_mut()).is_pending(),
        "default buffered request rejected instead of waiting for admission",
    )?;
    peer.no_request()?;
    observed.respond()?;
    tokio::time::timeout(WATCHDOG, first).await??;
    let (response, ()) = tokio::try_join!(
        async {
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                tokio::time::timeout(WATCHDOG, second).await??,
            )
        },
        async {
            let observed = peer.next().await?;
            check(
                observed.request_line == "GET /second HTTP/1.1",
                "wrong queued request",
            )?;
            observed.respond()
        },
    )?;
    check(response.body.as_ref() == b"ok", "wrong response body")?;
    client.shutdown_graceful().await?;
    peer.no_request()?;
    Ok(())
}

#[tokio::test]
async fn fail_fast_client_and_request_override_reject_before_transport() -> TestResult {
    for (client_policy, request_policy) in [
        (HttpAdmissionPolicy::FailFast, None),
        (
            HttpAdmissionPolicy::Wait,
            Some(HttpAdmissionPolicy::FailFast),
        ),
    ] {
        let mut peer = Peer::new().await?;
        let client = HttpClient::new(
            peer.config(1)?.with_admission_policy(client_policy),
            "fail-fast",
        )?;
        let (active, observed) = occupy(&mut peer, &client).await?;
        let attempts = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&attempts);
        let policy = HttpRetryPolicy::builder()
            .observer(move |event: &RetryEvent| {
                if matches!(event, RetryEvent::AttemptStarted { .. }) {
                    seen.fetch_add(1, Ordering::SeqCst);
                }
            })
            .build()?;
        for streaming in [false, true] {
            let mut builder = HttpRequest::builder()
                .path("/rejected")
                .retry_setting(RetrySetting::Policy(policy.clone()));
            if let Some(policy) = request_policy {
                builder = builder.admission_policy(policy);
            }
            let request = builder.build()?;
            let result = if streaming {
                let mut future = Box::pin(client.stream(request));
                match futures_util::poll!(future.as_mut()) {
                    std::task::Poll::Ready(value) => failure(value)?,
                    std::task::Poll::Pending => {
                        return Err(std::io::Error::other("FailFast stream waited").into())
                    }
                }
            } else {
                let mut future = Box::pin(client.request(request));
                match futures_util::poll!(future.as_mut()) {
                    std::task::Poll::Ready(value) => failure(value)?,
                    std::task::Poll::Pending => {
                        return Err(std::io::Error::other("FailFast buffered request waited").into())
                    }
                }
            };
            check(
                result.kind() == ErrorKind::QueueFull,
                "FailFast did not return QueueFull",
            )?;
            check(
                result.context().http_attempt == Some(0),
                "rejected admission reported a transport attempt",
            )?;
            check(
                attempts.load(Ordering::SeqCst) == 0,
                "admission rejection invoked transport observer",
            )?;
            peer.no_request()?;
        }
        finish_occupant(&active, observed).await?;
        empty_client(&client, &mut peer).await?;
        client.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn request_wait_overrides_fail_fast_client_for_buffered_and_stream() -> TestResult {
    for streaming in [false, true] {
        let mut peer = Peer::new().await?;
        let client = HttpClient::new(
            peer.config(1)?
                .with_admission_policy(HttpAdmissionPolicy::FailFast),
            "wait-override",
        )?;
        let (active, observed) = occupy(&mut peer, &client).await?;
        let request = HttpRequest::builder()
            .path("/wait")
            .admission_policy(HttpAdmissionPolicy::Wait)
            .build()?;
        if streaming {
            let mut future = Box::pin(client.stream(request));
            check(
                futures_util::poll!(future.as_mut()).is_pending(),
                "stream override did not wait",
            )?;
            peer.no_request()?;
            finish_occupant(&active, observed).await?;
            let (response, release) = open_stream(&mut peer, future, "GET /wait HTTP/1.1").await?;
            drop(response);
            drop(release);
        } else {
            let mut future = Box::pin(client.request(request));
            check(
                futures_util::poll!(future.as_mut()).is_pending(),
                "buffered override did not wait",
            )?;
            peer.no_request()?;
            finish_occupant(&active, observed).await?;
            respond_buffered(&mut peer, future, "GET /wait HTTP/1.1").await?;
        }
        empty_client(&client, &mut peer).await?;
        client.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn buffered_shortcuts_inherit_client_admission_policy() -> TestResult {
    for policy in [HttpAdmissionPolicy::Wait, HttpAdmissionPolicy::FailFast] {
        let mut peer = Peer::new().await?;
        let client = HttpClient::new(
            peer.config(1)?.with_admission_policy(policy),
            "shortcut-policy",
        )?;
        for method in 0..6 {
            let (active, observed) = occupy(&mut peer, &client).await?;
            let mut future: std::pin::Pin<
                Box<dyn Future<Output = Result<HttpResponse, NetError>>>,
            > = match method {
                0 => Box::pin(client.get("/shortcut")),
                1 => Box::pin(client.get_with_headers("/shortcut", HeaderMap::new())),
                2 => Box::pin(client.get_bytes("/shortcut")),
                3 => Box::pin(client.get_bytes_with_headers("/shortcut", HeaderMap::new())),
                4 => Box::pin(client.post("/shortcut", b"body")),
                _ => Box::pin(client.post_with_headers("/shortcut", HeaderMap::new(), b"body")),
            };
            if policy == HttpAdmissionPolicy::Wait {
                check(
                    futures_util::poll!(future.as_mut()).is_pending(),
                    "shortcut failed to inherit Wait",
                )?;
                peer.no_request()?;
                finish_occupant(&active, observed).await?;
                let line = if method < 4 {
                    "GET /shortcut HTTP/1.1"
                } else {
                    "POST /shortcut HTTP/1.1"
                };
                respond_buffered(&mut peer, future, line).await?;
            } else {
                let result = match futures_util::poll!(future.as_mut()) {
                    std::task::Poll::Ready(result) => result,
                    std::task::Poll::Pending => {
                        return Err(
                            std::io::Error::other("shortcut failed to inherit FailFast").into()
                        )
                    }
                };
                check(
                    failure(result)?.kind() == ErrorKind::QueueFull,
                    "shortcut wrong rejection",
                )?;
                finish_occupant(&active, observed).await?;
            }
            empty_client(&client, &mut peer).await?;
        }
        client.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn sse_shortcut_inherits_client_admission_policy() -> TestResult {
    for policy in [HttpAdmissionPolicy::Wait, HttpAdmissionPolicy::FailFast] {
        let mut peer = Peer::new().await?;
        let client = HttpClient::new(peer.config(1)?.with_admission_policy(policy), "sse-policy")?;
        let (active, observed) = occupy(&mut peer, &client).await?;
        let mut future = Box::pin(client.sse("/events", HeaderMap::new()));
        if policy == HttpAdmissionPolicy::Wait {
            check(
                futures_util::poll!(future.as_mut()).is_pending(),
                "SSE failed to inherit Wait",
            )?;
            peer.no_request()?;
            finish_occupant(&active, observed).await?;
            let (response, release) =
                open_stream(&mut peer, future, "GET /events HTTP/1.1").await?;
            drop(response);
            drop(release);
        } else {
            let result = match futures_util::poll!(future.as_mut()) {
                std::task::Poll::Ready(result) => result,
                std::task::Poll::Pending => {
                    return Err(std::io::Error::other("SSE failed to inherit FailFast").into())
                }
            };
            check(
                failure(result)?.kind() == ErrorKind::QueueFull,
                "SSE wrong rejection",
            )?;
            finish_occupant(&active, observed).await?;
        }
        empty_client(&client, &mut peer).await?;
        client.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn admission_and_total_deadlines_choose_earliest_with_total_tie_priority() -> TestResult {
    let short = Duration::from_millis(25);
    let long = Duration::from_millis(100);
    for (admission, total, expected) in [
        (Some(short), None, TimeoutPhase::Admission),
        (Some(short), Some(long), TimeoutPhase::Admission),
        (Some(long), Some(short), TimeoutPhase::Total),
        (Some(short), Some(short), TimeoutPhase::Total),
        (None, Some(short), TimeoutPhase::Total),
    ] {
        for streaming in [false, true] {
            let mut peer = Peer::new().await?;
            let client = peer.client(1)?;
            let (active, observed) = occupy(&mut peer, &client).await?;
            let mut timeouts = HttpTimeouts::new()
                .without_admission_timeout()
                .without_total_timeout();
            if let Some(duration) = admission {
                timeouts = timeouts.with_admission_timeout(duration);
            }
            if let Some(duration) = total {
                timeouts = timeouts.with_total_timeout(duration);
            }
            let request = HttpRequest::builder()
                .path("/expires")
                .timeouts(timeouts)
                .build()?;
            let error = if streaming {
                failure(tokio::time::timeout(WATCHDOG, client.stream(request)).await?)?
            } else {
                failure(tokio::time::timeout(WATCHDOG, client.request(request)).await?)?
            };
            let kind = if expected == TimeoutPhase::Total {
                ErrorKind::DeadlineExceeded
            } else {
                ErrorKind::TimedOut
            };
            check(error.kind() == kind, "wrong timeout error kind")?;
            check(
                error.context().http_timeout_phase == Some(expected),
                "wrong winning timeout phase",
            )?;
            check(
                error.context().stage == Some(ErrorStage::Admission),
                "timeout lost admission stage",
            )?;
            check(
                error.context().http_attempt == Some(0),
                "admission timeout counted transport attempt",
            )?;
            peer.no_request()?;
            finish_occupant(&active, observed).await?;
            respond_buffered(
                &mut peer,
                client.get("/after-timeout"),
                "GET /after-timeout HTTP/1.1",
            )
            .await?;
            empty_client(&client, &mut peer).await?;
            client.shutdown().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn dropping_waiter_releases_its_registration_and_does_not_block_successor() -> TestResult {
    let mut peer = Peer::new().await?;
    let client = peer.client(1)?;
    let unpolled = client.get("/unpolled");
    tokio::time::timeout(WATCHDOG, client.drain()).await??;
    drop(unpolled);
    peer.no_request()?;
    let (active, observed) = occupy(&mut peer, &client).await?;
    let request = HttpRequest::builder()
        .path("/dropped")
        .timeouts(
            HttpTimeouts::new()
                .without_admission_timeout()
                .without_total_timeout(),
        )
        .build()?;
    let mut waiting = Box::pin(client.request(request));
    check(
        futures_util::poll!(waiting.as_mut()).is_pending(),
        "waiter not registered",
    )?;
    let mut survivor = Box::pin(client.get("/survivor"));
    check(
        futures_util::poll!(survivor.as_mut()).is_pending(),
        "successor not waiting",
    )?;
    drop(waiting);
    peer.no_request()?;
    finish_occupant(&active, observed).await?;
    respond_buffered(&mut peer, survivor, "GET /survivor HTTP/1.1").await?;
    empty_client(&client, &mut peer).await?;
    client.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn cancel_all_cancels_waiters_but_not_unpolled_future() -> TestResult {
    for streaming in [false, true] {
        let mut peer = Peer::new().await?;
        let client = peer.client(1)?;
        let (active, observed) = occupy(&mut peer, &client).await?;
        let request = HttpRequest::builder()
            .path("/cancelled")
            .timeouts(
                HttpTimeouts::new()
                    .without_admission_timeout()
                    .without_total_timeout(),
            )
            .build()?;
        let unpolled = client.get("/unpolled");
        let error = if streaming {
            let mut waiting = Box::pin(client.stream(request));
            check(
                futures_util::poll!(waiting.as_mut()).is_pending(),
                "stream did not register",
            )?;
            client.cancel_all()?;
            failure(tokio::time::timeout(WATCHDOG, waiting).await?)?
        } else {
            let mut waiting = Box::pin(client.request(request));
            check(
                futures_util::poll!(waiting.as_mut()).is_pending(),
                "buffered waiter did not register",
            )?;
            client.cancel_all()?;
            failure(tokio::time::timeout(WATCHDOG, waiting).await?)?
        };
        check(
            error.kind() == ErrorKind::Cancelled,
            "cancel_all missed waiter",
        )?;
        check(
            error.context().http_attempt == Some(0),
            "cancelled admission counted attempt",
        )?;
        let completion = tokio::time::timeout(WATCHDOG, active.finished()).await?;
        check(
            failure(completion.response)?.kind() == ErrorKind::Cancelled,
            "active callback request not cancelled",
        )?;
        completion.callback?;
        drop(observed);
        empty_client(&client, &mut peer).await?;
        respond_buffered(&mut peer, unpolled, "GET /unpolled HTTP/1.1").await?;
        empty_client(&client, &mut peer).await?;
        client.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn request_shutdown_rejects_waiters_while_accepted_request_finishes() -> TestResult {
    let mut peer = Peer::new().await?;
    let client = peer.client(1)?;
    let mut active = Box::pin(client.get("/accepted"));
    check(
        futures_util::poll!(active.as_mut()).is_pending(),
        "active request not started",
    )?;
    let observed = peer.next().await?;
    let mut waiting = Box::pin(client.get("/waiting"));
    check(
        futures_util::poll!(waiting.as_mut()).is_pending(),
        "second request not waiting",
    )?;
    client.request_shutdown();
    let error = failure(tokio::time::timeout(WATCHDOG, waiting).await?)?;
    check(
        matches!(error.kind(), ErrorKind::QueueClosed | ErrorKind::Closed),
        "shutdown did not reject waiter",
    )?;
    check(
        error.context().http_attempt == Some(0),
        "closed waiter counted transport attempt",
    )?;
    observed.respond()?;
    let response = tokio::time::timeout(WATCHDOG, active).await??;
    check(
        response.body.as_ref() == b"ok",
        "accepted request failed after admission close",
    )?;
    for policy in [HttpAdmissionPolicy::Wait, HttpAdmissionPolicy::FailFast] {
        let request = HttpRequest::builder()
            .path("/closed")
            .admission_policy(policy)
            .build()?;
        let error = failure(tokio::time::timeout(WATCHDOG, client.request(request)).await?)?;
        check(
            matches!(error.kind(), ErrorKind::QueueClosed | ErrorKind::Closed),
            "closed client admitted request",
        )?;
    }
    empty_client(&client, &mut peer).await?;
    tokio::time::timeout(WATCHDOG, client.shutdown()).await??;
    Ok(())
}

#[tokio::test]
async fn semaphore_waiters_preserve_order_after_middle_waiter_is_dropped() -> TestResult {
    let mut peer = Peer::new().await?;
    let client = peer.client(1)?;
    let (active, observed) = occupy(&mut peer, &client).await?;
    let mut first = Box::pin(client.get("/first-waiter"));
    let mut middle = Box::pin(client.get("/middle-waiter"));
    let mut last = Box::pin(client.get("/last-waiter"));
    check(
        futures_util::poll!(first.as_mut()).is_pending(),
        "first waiter not waiting",
    )?;
    check(
        futures_util::poll!(middle.as_mut()).is_pending(),
        "middle waiter not waiting",
    )?;
    check(
        futures_util::poll!(last.as_mut()).is_pending(),
        "last waiter not waiting",
    )?;
    drop(middle);
    finish_occupant(&active, observed).await?;
    // Poll the later waiter first: the permit remains reserved for the earlier one.
    check(
        futures_util::poll!(last.as_mut()).is_pending(),
        "later waiter overtook earlier semaphore waiter",
    )?;
    peer.no_request()?;
    respond_buffered(&mut peer, first, "GET /first-waiter HTTP/1.1").await?;
    respond_buffered(&mut peer, last, "GET /last-waiter HTTP/1.1").await?;
    empty_client(&client, &mut peer).await?;
    client.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn stream_holds_capacity_until_body_eof_drop_or_timeout() -> TestResult {
    for release_mode in ["eof", "drop", "timeout"] {
        let mut peer = Peer::new().await?;
        let client = peer.client(1)?;
        let timeouts = if release_mode == "timeout" {
            HttpTimeouts::new().with_idle_read_timeout(Duration::from_millis(40))
        } else {
            HttpTimeouts::new()
                .without_total_timeout()
                .without_attempt_timeout()
        };
        let request = HttpRequest::builder()
            .path("/stream")
            .timeouts(timeouts)
            .build()?;
        let (mut stream, release) =
            open_stream(&mut peer, client.stream(request), "GET /stream HTTP/1.1").await?;
        let mut waiting = Box::pin(client.get("/after-stream"));
        check(
            futures_util::poll!(waiting.as_mut()).is_pending(),
            "headers released stream capacity prematurely",
        )?;
        peer.no_request()?;
        match release_mode {
            "eof" => {
                release
                    .send(())
                    .map_err(|_| std::io::Error::other("stream connection already closed"))?;
                while let Some(chunk) = tokio::time::timeout(WATCHDOG, stream.next_chunk()).await? {
                    chunk?;
                }
            }
            "drop" => {
                drop(stream);
                drop(release);
            }
            _ => {
                // Deliberately do not poll the stream body. Its independent supervisor
                // must release capacity and wake the buffered waiter on timeout.
                respond_buffered(&mut peer, waiting, "GET /after-stream HTTP/1.1").await?;
                check(
                    matches!(stream.next_chunk().await, Some(Err(ref error)) if error.kind() == ErrorKind::TimedOut),
                    "idle stream supervisor failed to terminate body",
                )?;
                check(
                    stream.next_chunk().await.is_none(),
                    "stream timeout was not terminal",
                )?;
                drop(release);
                empty_client(&client, &mut peer).await?;
                client.shutdown().await?;
                continue;
            }
        }
        respond_buffered(&mut peer, waiting, "GET /after-stream HTTP/1.1").await?;
        empty_client(&client, &mut peer).await?;
        client.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn explicit_stream_cancellation_releases_capacity_for_buffered_waiter() -> TestResult {
    let mut peer = Peer::new().await?;
    let client = peer.client(1)?;
    let request = HttpRequest::builder().path("/stream").build()?;
    let (mut response, release) =
        open_stream(&mut peer, client.stream(request), "GET /stream HTTP/1.1").await?;
    let mut waiting = Box::pin(client.get("/after-cancel"));
    check(
        futures_util::poll!(waiting.as_mut()).is_pending(),
        "stream capacity not held",
    )?;
    client.cancel(response.request_id)?;
    respond_buffered(&mut peer, waiting, "GET /after-cancel HTTP/1.1").await?;
    check(
        matches!(response.next_chunk().await, Some(Err(ref error)) if error.kind() == ErrorKind::Cancelled),
        "stream cancellation result missing",
    )?;
    drop(release);
    empty_client(&client, &mut peer).await?;
    client.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn callback_send_and_send_wait_keep_their_contract_independent_of_client_policy() -> TestResult
{
    for policy in [HttpAdmissionPolicy::Wait, HttpAdmissionPolicy::FailFast] {
        let mut peer = Peer::new().await?;
        let client = HttpClient::new(
            peer.config(1)?.with_admission_policy(policy),
            "callback-policy",
        )?;
        let (active, observed) = occupy(&mut peer, &client).await?;
        let calls = Arc::new(AtomicUsize::new(0));
        let request: Box<dyn HttpRequestTrait> = Box::new(CallbackRequest {
            path: "/callback",
            calls: Arc::clone(&calls),
        });
        let error = match client.send(request) {
            Err(error) => error,
            Ok(_) => {
                return Err(std::io::Error::other(
                    "send must reject at capacity regardless of client policy",
                )
                .into())
            }
        };
        check(
            error.error().kind() == ErrorKind::QueueFull,
            "send wrong rejection",
        )?;
        check(
            calls.load(Ordering::SeqCst) == 0,
            "rejected send invoked callback",
        )?;
        let mut waiting = Box::pin(client.send_wait(error.into_inner()));
        check(
            futures_util::poll!(waiting.as_mut()).is_pending(),
            "send_wait failed to wait regardless of client policy",
        )?;
        peer.no_request()?;
        finish_occupant(&active, observed).await?;
        let handle = tokio::time::timeout(WATCHDOG, waiting)
            .await?
            .map_err(|error| error.into_error())?;
        let observed = peer.next().await?;
        check(
            observed.request_line == "GET /callback HTTP/1.1",
            "rejected object not preserved",
        )?;
        finish_occupant(&handle, observed).await?;
        check(
            calls.load(Ordering::SeqCst) == 1,
            "accepted callback did not run exactly once",
        )?;
        empty_client(&client, &mut peer).await?;
        client.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn drain_counts_a_reserved_waiter_even_after_the_previous_request_finishes() -> TestResult {
    let mut peer = Peer::new().await?;
    let client = peer.client(1)?;
    let unpolled = client.get("/never-polled");
    let (active, observed) = occupy(&mut peer, &client).await?;
    let mut waiting = Box::pin(client.get("/reserved"));
    check(
        futures_util::poll!(waiting.as_mut()).is_pending(),
        "waiter was not registered",
    )?;
    finish_occupant(&active, observed).await?;
    // Capacity is available, but the caller has not polled its waiting Future
    // again. Its reservation must remain visible to drain until it is dropped.
    let mut drain = Box::pin(client.drain());
    check(
        futures_util::poll!(drain.as_mut()).is_pending(),
        "drain ignored a reserved waiter",
    )?;
    peer.no_request()?;
    drop(waiting);
    tokio::time::timeout(WATCHDOG, drain).await??;
    peer.no_request()?;
    // A constructed but never-polled Future has no registration at all.
    drop(unpolled);
    client.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn full_shutdown_wakes_unbounded_buffered_and_stream_waiters() -> TestResult {
    let mut peer = Peer::new().await?;
    let client = peer.client(1)?;
    let (active, observed) = occupy(&mut peer, &client).await?;
    let unbounded = HttpTimeouts::new()
        .without_admission_timeout()
        .without_total_timeout();
    let mut buffered = Box::pin(
        client.request(
            HttpRequest::builder()
                .path("/buffered")
                .timeouts(unbounded.clone())
                .build()?,
        ),
    );
    let mut stream = Box::pin(
        client.stream(
            HttpRequest::builder()
                .path("/stream")
                .timeouts(unbounded)
                .build()?,
        ),
    );
    check(
        futures_util::poll!(buffered.as_mut()).is_pending(),
        "buffered not waiting",
    )?;
    check(
        futures_util::poll!(stream.as_mut()).is_pending(),
        "stream not waiting",
    )?;
    let (buffered_result, stream_result, shutdown) = tokio::time::timeout(WATCHDOG, async {
        tokio::join!(buffered, stream, client.shutdown())
    })
    .await?;
    shutdown?;
    for error in [failure(buffered_result)?, failure(stream_result)?] {
        check(
            matches!(
                error.kind(),
                ErrorKind::QueueClosed | ErrorKind::Closed | ErrorKind::Cancelled
            ),
            "shutdown failed to end admission",
        )?;
        check(
            error.context().http_attempt == Some(0),
            "shutdown waiter counted transport attempt",
        )?;
    }
    let completion = tokio::time::timeout(WATCHDOG, active.finished()).await?;
    check(
        failure(completion.response)?.kind() == ErrorKind::Cancelled,
        "full shutdown did not cancel active request",
    )?;
    completion.callback?;
    drop(observed);
    empty_client(&client, &mut peer).await?;
    Ok(())
}

#[path = "support/crypto_provider.rs"]
mod crypto_provider;

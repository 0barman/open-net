#![cfg(feature = "http-client")]

use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::{HeaderMap, HeaderValue, StatusCode};
use open_net::api::error::{ErrorKind, ErrorStage};
use open_net::api::http::{
    HttpCancelOutcome, HttpClient, HttpClientConfig, HttpCompletion, HttpRequestHandle,
    HttpRequestId, HttpRequestMethod, HttpRequestOptions, HttpRequestTrait, HttpResponseResult,
    HttpTimeouts, RetryPolicy, TimeoutPhase,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::task::{JoinHandle, JoinSet};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
const WATCHDOG: Duration = Duration::from_secs(5);

struct ObservedRequest {
    headers: String,
    body: Vec<u8>,
    reply: oneshot::Sender<String>,
}

impl ObservedRequest {
    fn respond(self, status: u16) {
        let _ = self.reply.send(format!(
            "HTTP/1.1 {status} Response\r\nContent-Length: 2\r\nX-Observed: yes\r\nConnection: close\r\n\r\nok"
        ));
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
        let (requests_tx, requests) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let mut workers = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let Ok((stream, _)) = accepted else { break };
                        let sender = requests_tx.clone();
                        workers.spawn(async move { serve(stream, sender).await });
                    }
                    Some(_) = workers.join_next(), if !workers.is_empty() => {}
                }
            }
        });
        Ok(Self {
            url,
            requests,
            task,
        })
    }

    async fn next(&mut self) -> TestResult<ObservedRequest> {
        tokio::time::timeout(WATCHDOG, self.requests.recv())
            .await?
            .ok_or_else(|| std::io::Error::other("peer stopped before request").into())
    }

    fn client(&self, capacity: usize) -> TestResult<HttpClient> {
        let config = HttpClientConfig::new(&self.url)?
            .with_request_queue_capacity(capacity)?
            .with_environment_proxy(false);
        Ok(HttpClient::new(config, "http-ownership-tests")?)
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
    let header_end = loop {
        if let Some(position) = bytes.windows(4).position(|value| value == b"\r\n\r\n") {
            break position + 4;
        }
        match stream.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(length) => bytes.extend_from_slice(&chunk[..length]),
        }
    };
    let headers = String::from_utf8_lossy(&bytes[..header_end]).into_owned();
    let body_len = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(0);
    while bytes.len() < header_end + body_len {
        match stream.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(length) => bytes.extend_from_slice(&chunk[..length]),
        }
    }
    let (reply, response) = oneshot::channel();
    let request = ObservedRequest {
        headers,
        body: bytes[header_end..header_end + body_len].to_vec(),
        reply,
    };
    if sender.send(request).is_err() {
        return;
    }
    if let Ok(response) = response.await {
        let _ = stream.write_all(response.as_bytes()).await;
    }
}

#[derive(Default)]
struct Counts {
    callbacks: AtomicUsize,
    drops: AtomicUsize,
    paths: AtomicUsize,
    hooks: AtomicUsize,
}

// Cell and the FnOnce callback deliberately make this request Send, but not Sync,
// Clone or Debug. Its heap token proves rejection returns the original value.
struct Probe<C = ()> {
    identity: Box<Cell<usize>>,
    counts: Arc<Counts>,
    context: Arc<C>,
    path: String,
    method: HttpRequestMethod,
    body: Bytes,
    explicit_body: bool,
    policy: RetryPolicy,
    callback: Option<Box<dyn FnOnce(&Arc<C>, HttpResponseResult) + Send>>,
    release_callback: Option<oneshot::Receiver<()>>,
    #[cfg(feature = "http-client-cookies")]
    cookie_context: open_net::api::http::CookieRequestContext,
}

impl Probe {
    fn new(counts: &Arc<Counts>) -> Self {
        Self::with_context(counts, Arc::new(()))
    }
}

impl<C> Probe<C> {
    fn with_context(counts: &Arc<Counts>, context: Arc<C>) -> Self {
        Self {
            identity: Box::new(Cell::new(37)),
            counts: Arc::clone(counts),
            context,
            path: "/owned".to_owned(),
            method: HttpRequestMethod::POST,
            body: Bytes::from_static(b"\0\xffraw bytes"),
            explicit_body: true,
            policy: RetryPolicy::no_retry(),
            callback: None,
            release_callback: None,
            #[cfg(feature = "http-client-cookies")]
            cookie_context: open_net::api::http::CookieRequestContext::same_site(),
        }
    }

    fn identity(&self) -> *const Cell<usize> {
        &*self.identity
    }
}

impl<C> Drop for Probe<C> {
    fn drop(&mut self) {
        self.counts.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[open_net::async_trait]
impl<C: Send + Sync + 'static> HttpRequestTrait for Probe<C> {
    fn get_path(&self) -> String {
        self.counts.paths.fetch_add(1, Ordering::SeqCst);
        self.path.clone()
    }

    fn get_method(&self) -> HttpRequestMethod {
        self.method.clone()
    }
    fn get_req_data(&self) -> String {
        "must not replace raw bytes".to_owned()
    }
    fn get_req_body(&self) -> Bytes {
        self.body.clone()
    }
    fn has_req_body(&self) -> bool {
        self.explicit_body
    }
    fn retry_policy(&self) -> RetryPolicy {
        self.policy.clone()
    }
    fn headers(&self) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("x-request", HeaderValue::from_static("owned"));
        headers
    }

    #[cfg(feature = "http-client-cookies")]
    fn cookie_context(&self) -> open_net::api::http::CookieRequestContext {
        self.cookie_context.clone()
    }

    fn on_response_headers(&self, headers: &HeaderMap) {
        assert_eq!(
            headers.get("x-observed"),
            Some(&HeaderValue::from_static("yes"))
        );
        self.counts.hooks.fetch_add(1, Ordering::SeqCst);
    }

    async fn deal_with_response(mut self: Box<Self>, result: HttpResponseResult) {
        self.counts.callbacks.fetch_add(1, Ordering::SeqCst);
        if let Some(callback) = self.callback.take() {
            callback(&self.context, result);
        }
        if let Some(release) = self.release_callback.take() {
            let _ = release.await;
        }
    }
}

async fn finished(handle: &HttpRequestHandle) -> TestResult<HttpCompletion> {
    Ok(tokio::time::timeout(WATCHDOG, handle.finished()).await?)
}

#[tokio::test]
async fn invalid_extension_methods_finish_once_without_dispatch_and_release_capacity() -> TestResult
{
    let mut peer = Peer::new().await?;
    let client = peer.client(1)?;
    // Reusing the single permit and ID verifies both are released even when no
    // transport attempt starts. Invalid input must never become a different method.
    let request_id = HttpRequestId(981);
    for token in ["", "BAD METHOD", "GET\r\nsecret", "GÉT", "GET/", "GET\0"] {
        let counts = Arc::new(Counts::default());
        let mut request = Probe::new(&counts);
        request.method = HttpRequestMethod::Extension(token.to_owned());
        let (callback_tx, callback_rx) = oneshot::channel();
        request.callback = Some(Box::new(move |_, result| {
            let _ = callback_tx.send(result);
        }));
        let handle = client
            .send_with_options(
                request,
                HttpRequestOptions::new().with_request_id(request_id),
            )
            .map_err(|error| error.into_error())?;
        let completion = finished(&handle).await?;
        completion.callback?;
        let callback_result = tokio::time::timeout(WATCHDOG, callback_rx).await??;
        for error in [
            completion
                .response
                .expect_err("completion rejects invalid method"),
            callback_result.expect_err("callback receives invalid method error"),
        ] {
            assert_eq!(error.kind(), ErrorKind::InvalidInput);
            assert_eq!(error.context().stage, Some(ErrorStage::RequestBuild));
            assert_eq!(error.context().http_request_id, Some(request_id));
            assert_eq!(error.context().http_attempt, Some(0));
            let detail = error.config_error().expect("method error details");
            assert_eq!(detail.field(), "method");
            assert_eq!(detail.reason(), "invalid HTTP method token");
            assert!(!format!("{error} {error:?}").contains("secret"));
        }
        assert_eq!(counts.callbacks.load(Ordering::SeqCst), 1);
        assert_eq!(counts.drops.load(Ordering::SeqCst), 1);
        assert_eq!(counts.paths.load(Ordering::SeqCst), 0);
        assert_eq!(counts.hooks.load(Ordering::SeqCst), 0);
        assert!(peer.requests.try_recv().is_err());
    }

    // The same worker must still transmit valid extension methods verbatim.
    let counts = Arc::new(Counts::default());
    let mut request = Probe::new(&counts);
    request.method = HttpRequestMethod::Extension("CuStOm".to_owned());
    let handle = client
        .send_with_options(
            request,
            HttpRequestOptions::new().with_request_id(request_id),
        )
        .map_err(|error| error.into_error())?;
    let observed = peer.next().await?;
    assert!(observed.headers.starts_with("CuStOm /owned HTTP/1.1\r\n"));
    observed.respond(200);
    let completion = finished(&handle).await?;
    assert_eq!(completion.response?.status, StatusCode::OK);
    completion.callback?;
    assert_eq!(counts.callbacks.load(Ordering::SeqCst), 1);
    assert_eq!(counts.drops.load(Ordering::SeqCst), 1);
    client.shutdown_graceful().await?;
    assert!(peer.requests.try_recv().is_err());
    Ok(())
}

fn assert_returned<C>(request: &Probe<C>, identity: *const Cell<usize>, counts: &Counts) {
    assert_eq!(request.identity(), identity);
    assert_eq!(request.identity.get(), 37);
    assert_eq!(counts.callbacks.load(Ordering::SeqCst), 0);
    assert_eq!(counts.drops.load(Ordering::SeqCst), 0);
    assert_eq!(counts.paths.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn mixed_request_owned_contexts_and_nested_boxes_preserve_request_metadata() -> TestResult {
    struct Context {
        responses: AtomicUsize,
    }
    struct OtherContext {
        label: &'static str,
    }
    let mut peer = Peer::new().await?;
    let client = peer.client(1)?;
    let context = Arc::new(Context {
        responses: AtomicUsize::new(0),
    });
    let counts = Arc::new(Counts::default());
    let mut request = Probe::with_context(&counts, Arc::clone(&context));
    let expected_context = Arc::clone(&context);
    let (called_tx, called_rx) = oneshot::channel();
    request.callback = Some(Box::new(move |context, result| {
        assert!(Arc::ptr_eq(context, &expected_context));
        assert_eq!(result.expect("response").body(), &Bytes::from_static(b"ok"));
        context.responses.fetch_add(1, Ordering::SeqCst);
        let _ = called_tx.send(());
    }));
    let other_context = Arc::new(OtherContext { label: "other" });
    let weak_other_context = Arc::downgrade(&other_context);
    let other_counts = Arc::new(Counts::default());
    let mut other = Probe::with_context(&other_counts, other_context);
    other.callback = Some(Box::new(move |context, result| {
        assert_eq!(context.label, "other");
        assert!(result.is_ok());
    }));
    // Application request generics are erased by one non-generic SDK trait.
    // A single client and heterogeneous queue can carry unrelated contexts.
    let requests: Vec<Box<dyn HttpRequestTrait>> = vec![Box::new(request), Box::new(other)];
    for request in requests {
        let handle = client
            .send(Box::new(request))
            .map_err(|error| error.into_error())?;
        let observed = peer.next().await?;
        assert!(observed.headers.starts_with("POST /owned HTTP/1.1\r\n"));
        assert!(observed
            .headers
            .to_ascii_lowercase()
            .contains("x-request: owned\r\n"));
        assert_eq!(observed.body, b"\0\xffraw bytes");
        observed.respond(200);
        let completion = finished(&handle).await?;
        assert_eq!(completion.request_id, handle.id());
        assert_eq!(completion.response?.status, StatusCode::OK);
        completion.callback?;
    }
    tokio::time::timeout(WATCHDOG, called_rx).await??;
    assert_eq!(context.responses.load(Ordering::SeqCst), 1);
    assert_eq!(counts.callbacks.load(Ordering::SeqCst), 1);
    assert_eq!(counts.hooks.load(Ordering::SeqCst), 1);
    assert_eq!(counts.drops.load(Ordering::SeqCst), 1);
    assert_eq!(other_counts.callbacks.load(Ordering::SeqCst), 1);
    assert!(
        weak_other_context.upgrade().is_none(),
        "finished must release the request's context"
    );
    client.shutdown_graceful().await?;
    Ok(())
}

#[tokio::test]
async fn queue_full_returns_same_send_only_request_and_callback_can_be_used_by_caller() -> TestResult
{
    struct Context {
        label: &'static str,
    }
    let mut peer = Peer::new().await?;
    let client = peer.client(1)?;
    let active = client
        .send(Probe::<()>::new(&Arc::new(Counts::default())))
        .map_err(|error| error.into_error())?;
    let observed = peer.next().await?;
    let counts = Arc::new(Counts::default());
    let context = Arc::new(Context { label: "rejected" });
    let weak_context = Arc::downgrade(&context);
    let mut request = Probe::with_context(&counts, context);
    let (sender, receiver) = oneshot::channel();
    request.callback = Some(Box::new(move |context, result| {
        assert_eq!(context.label, "rejected");
        let _ = sender.send(result);
    }));
    let identity = request.identity();
    let rejected = client
        .send(request)
        .expect_err("capacity must reject immediately");
    assert_eq!(rejected.error().kind(), ErrorKind::QueueFull);
    assert_returned(rejected.value(), identity, &counts);
    assert!(Arc::ptr_eq(
        &rejected.value().context,
        &weak_context
            .upgrade()
            .expect("rejected request retains its context")
    ));
    let (request, error) = rejected.into_parts();
    Box::new(request).deal_with_response(Err(error)).await;
    assert!(weak_context.upgrade().is_none());
    assert_eq!(
        receiver
            .await?
            .expect_err("caller delivered rejection")
            .kind(),
        ErrorKind::QueueFull
    );
    observed.respond(200);
    finished(&active).await?.callback?;
    client.shutdown_graceful().await?;
    assert!(peer.requests.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn closed_duplicate_and_invalid_options_return_original_values_without_dispatch() -> TestResult
{
    let mut peer = Peer::new().await?;
    let client = peer.client(2)?;
    let id = HttpRequestId(912);
    let active = client
        .send_with_options(
            Probe::<()>::new(&Arc::new(Counts::default())),
            HttpRequestOptions::new().with_request_id(id),
        )
        .map_err(|error| error.into_error())?;
    let observed = peer.next().await?;
    let counts = Arc::new(Counts::default());
    let request = Probe::new(&counts);
    let identity = request.identity();
    let rejected = client
        .send_wait_with_options(request, HttpRequestOptions::new().with_request_id(id))
        .await
        .expect_err("active ID must be rejected");
    assert_eq!(rejected.error().kind(), ErrorKind::DuplicateRequestId);
    assert_returned(rejected.value(), identity, &counts);
    drop(rejected);
    assert_eq!(counts.drops.load(Ordering::SeqCst), 1);

    // RetryPolicy's builder validates nonzero durations; admission must also reject
    // a deadline that cannot fit the monotonic clock before taking request ownership.
    let policy = RetryPolicy::builder()
        .total_deadline(Duration::MAX)
        .build()?;
    let counts = Arc::new(Counts::default());
    let request = Probe::new(&counts);
    let identity = request.identity();
    let rejected = client
        .send_with_options(request, HttpRequestOptions::new().with_retry_policy(policy))
        .expect_err("overflowing timeout must be rejected");
    assert!(matches!(
        rejected.error().kind(),
        ErrorKind::InvalidConfig | ErrorKind::InvalidInput
    ));
    assert_returned(rejected.value(), identity, &counts);
    drop(rejected);

    observed.respond(200);
    finished(&active).await?.callback?;
    client.shutdown_graceful().await?;
    let counts = Arc::new(Counts::default());
    let request = Probe::new(&counts);
    let identity = request.identity();
    let rejected = client
        .send_wait(request)
        .await
        .expect_err("closed client must reject");
    assert!(matches!(
        rejected.error().kind(),
        ErrorKind::QueueClosed | ErrorKind::Closed
    ));
    assert_returned(rejected.value(), identity, &counts);
    assert!(peer.requests.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn admission_and_total_timeouts_return_request_and_release_reserved_id() -> TestResult {
    for phase in [TimeoutPhase::Admission, TimeoutPhase::Total] {
        let mut peer = Peer::new().await?;
        let client = peer.client(1)?;
        let active = client
            .send(Probe::<()>::new(&Arc::new(Counts::default())))
            .map_err(|error| error.into_error())?;
        let observed = peer.next().await?;
        let timeouts = match phase {
            TimeoutPhase::Admission => {
                HttpTimeouts::new().with_admission_timeout(Duration::from_millis(20))
            }
            _ => HttpTimeouts::new().with_total_timeout(Duration::from_millis(20)),
        };
        let counts = Arc::new(Counts::default());
        let request = Probe::new(&counts);
        let identity = request.identity();
        let options = HttpRequestOptions::new()
            .with_request_id(HttpRequestId(600))
            .with_timeouts(timeouts)?;
        let rejected =
            tokio::time::timeout(WATCHDOG, client.send_wait_with_options(request, options))
                .await?
                .expect_err("saturated admission must time out");
        assert_returned(rejected.value(), identity, &counts);
        assert_eq!(rejected.error().context().http_timeout_phase, Some(phase));
        assert_eq!(
            rejected.error().context().stage,
            Some(ErrorStage::Admission)
        );
        assert_eq!(rejected.error().context().http_attempt, Some(0));
        observed.respond(200);
        finished(&active).await?.callback?;
        let handle = client
            .send_with_options(
                rejected.into_inner(),
                HttpRequestOptions::new().with_request_id(HttpRequestId(600)),
            )
            .map_err(|error| error.into_error())?;
        peer.next().await?.respond(200);
        finished(&handle).await?.callback?;
        assert_eq!(counts.callbacks.load(Ordering::SeqCst), 1);
        client.shutdown_graceful().await?;
    }
    Ok(())
}

#[tokio::test]
async fn dropped_unpolled_and_waiting_submissions_release_requests_without_callbacks() -> TestResult
{
    let mut peer = Peer::new().await?;
    let client = peer.client(1)?;
    let counts = Arc::new(Counts::default());
    let unpolled = client.send_wait(Probe::<()>::new(&counts));
    drop(unpolled);
    assert_eq!(counts.drops.load(Ordering::SeqCst), 1);
    assert_eq!(counts.callbacks.load(Ordering::SeqCst), 0);
    assert_eq!(counts.paths.load(Ordering::SeqCst), 0);

    let active = client
        .send(Probe::<()>::new(&Arc::new(Counts::default())))
        .map_err(|error| error.into_error())?;
    let observed = peer.next().await?;
    let counts = Arc::new(Counts::default());
    let options = HttpRequestOptions::new().with_request_id(HttpRequestId(77));
    let mut waiting =
        Box::pin(client.send_wait_with_options(Probe::<()>::new(&counts), options.clone()));
    assert!(futures::poll!(waiting.as_mut()).is_pending());
    drop(waiting);
    assert_eq!(counts.drops.load(Ordering::SeqCst), 1);
    assert_eq!(counts.callbacks.load(Ordering::SeqCst), 0);
    assert_eq!(counts.paths.load(Ordering::SeqCst), 0);
    observed.respond(200);
    finished(&active).await?.callback?;
    let next = client
        .send_with_options(Probe::<()>::new(&Arc::new(Counts::default())), options)
        .map_err(|error| error.into_error())?;
    peer.next().await?.respond(200);
    finished(&next).await?.callback?;
    client.shutdown_graceful().await?;
    Ok(())
}

#[tokio::test]
async fn cancel_all_returns_registered_waiter_but_does_not_cancel_unpolled_submission() -> TestResult
{
    let mut peer = Peer::new().await?;
    let client = peer.client(1)?;
    let active = client
        .send(Probe::<()>::new(&Arc::new(Counts::default())))
        .map_err(|error| error.into_error())?;
    let observed = peer.next().await?;
    let counts = Arc::new(Counts::default());
    let request = Probe::new(&counts);
    let identity = request.identity();
    let options = HttpRequestOptions::new().with_request_id(HttpRequestId(76));
    let mut waiting = Box::pin(client.send_wait_with_options(request, options.clone()));
    assert!(futures::poll!(waiting.as_mut()).is_pending());
    let unpolled =
        client.send_wait_with_options(Probe::<()>::new(&Arc::new(Counts::default())), options);
    client.cancel_all()?;
    let rejected = tokio::time::timeout(WATCHDOG, waiting)
        .await?
        .expect_err("waiting reservation must be cancelled");
    assert_eq!(rejected.error().kind(), ErrorKind::Cancelled);
    assert_returned(rejected.value(), identity, &counts);
    assert_eq!(
        finished(&active)
            .await?
            .response
            .expect_err("active request cancelled")
            .kind(),
        ErrorKind::Cancelled
    );
    drop(observed);
    let next = unpolled.await.map_err(|error| error.into_error())?;
    peer.next().await?.respond(200);
    finished(&next).await?.response?;
    client.shutdown_graceful().await?;
    Ok(())
}

#[tokio::test]
async fn completion_waits_for_callback_and_remains_observable_after_id_reuse() -> TestResult {
    let mut peer = Peer::new().await?;
    let client = peer.client(1)?;
    let counts = Arc::new(Counts::default());
    let mut request = Probe::new(&counts);
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    request.callback = Some(Box::new(move |_, result| {
        assert!(result.is_ok());
        let _ = entered_tx.send(());
    }));
    request.release_callback = Some(release_rx);
    let options = HttpRequestOptions::new().with_request_id(HttpRequestId(55));
    let old = client
        .send_wait_with_options(request, options.clone())
        .await
        .map_err(|error| error.into_error())?;
    peer.next().await?.respond(200);
    tokio::time::timeout(WATCHDOG, entered_rx).await??;
    assert_eq!(old.cancel(), HttpCancelOutcome::TooLate);
    let mut pending_completion = Box::pin(old.finished());
    assert!(futures::poll!(pending_completion.as_mut()).is_pending());
    drop(pending_completion);
    let _ = release_tx.send(());
    let first = finished(&old).await?;
    assert_eq!(first.response?.attempts, 1);
    first.callback?;
    let new = client
        .send_with_options(Probe::<()>::new(&Arc::new(Counts::default())), options)
        .map_err(|error| error.into_error())?;
    let observed = peer.next().await?;
    assert_eq!(old.id(), new.id());
    assert_eq!(old.cancel(), HttpCancelOutcome::TooLate);
    assert_eq!(finished(&old).await?.response?.status, StatusCode::OK);
    observed.respond(201);
    assert_eq!(finished(&new).await?.response?.status, StatusCode::CREATED);
    client.shutdown_graceful().await?;
    Ok(())
}

#[tokio::test]
async fn dropping_handle_does_not_cancel_and_acceptance_error_runs_callback_once() -> TestResult {
    let mut peer = Peer::new().await?;
    let client = peer.client(1)?;
    let counts = Arc::new(Counts::default());
    let handle = client
        .send(Probe::<()>::new(&counts))
        .map_err(|error| error.into_error())?;
    let observed = peer.next().await?;
    drop(handle);
    observed.respond(200);
    tokio::time::timeout(WATCHDOG, client.drain()).await??;
    assert_eq!(counts.callbacks.load(Ordering::SeqCst), 1);
    let counts = Arc::new(Counts::default());
    let mut invalid = Probe::new(&counts);
    invalid.path = "https://different.invalid/private".to_owned();
    let handle = client.send(invalid).map_err(|error| error.into_error())?;
    let completion = finished(&handle).await?;
    assert!(completion.response.is_err());
    completion.callback?;
    assert_eq!(counts.callbacks.load(Ordering::SeqCst), 1);
    assert_eq!(counts.drops.load(Ordering::SeqCst), 1);
    client.shutdown_graceful().await?;
    assert!(peer.requests.try_recv().is_err());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_racing_commit_has_exactly_one_owner() -> TestResult {
    for _ in 0..24 {
        let mut peer = Peer::new().await?;
        let client = peer.client(1)?;
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let closing_client = client.clone();
        let closing_barrier = Arc::clone(&barrier);
        let closing = tokio::spawn(async move {
            closing_barrier.wait().await;
            closing_client.request_shutdown();
        });
        let counts = Arc::new(Counts::default());
        let request = Probe::<()>::new(&counts);
        let identity = request.identity();
        barrier.wait().await;
        match client.send_wait(request).await {
            Ok(handle) => {
                // Closing admission preserves an already accepted request.
                // Let it finish instead of waiting for an unsent response.
                peer.next().await?.respond(200);
                let completion = finished(&handle).await?;
                completion.callback?;
                assert_eq!(completion.response?.status, StatusCode::OK);
                assert_eq!(counts.callbacks.load(Ordering::SeqCst), 1);
                assert_eq!(counts.drops.load(Ordering::SeqCst), 1);
            }
            Err(rejected) => {
                assert!(matches!(
                    rejected.error().kind(),
                    ErrorKind::QueueClosed | ErrorKind::Closed
                ));
                assert_returned(rejected.value(), identity, &counts);
                drop(rejected);
                assert_eq!(counts.drops.load(Ordering::SeqCst), 1);
            }
        }
        closing.await?;
        tokio::time::timeout(WATCHDOG, client.shutdown_graceful()).await??;
    }
    Ok(())
}

#[tokio::test]
async fn boxed_explicit_empty_body_and_retry_policy_are_forwarded() -> TestResult {
    let mut peer = Peer::new().await?;
    let client = peer.client(1)?;
    let counts = Arc::new(Counts::default());
    let mut request = Probe::<()>::new(&counts);
    request.body = Bytes::new();
    // GET does not otherwise synthesize Content-Length: 0, so this verifies
    // has_req_body forwarding instead of relying on POST's default framing.
    request.method = HttpRequestMethod::GET;
    request.policy = RetryPolicy::new(1).with_non_idempotent(true);
    let request: Box<dyn HttpRequestTrait> = Box::new(request);
    let handle = client
        .send_wait(Box::new(request))
        .await
        .map_err(|error| error.into_error())?;
    let first = peer.next().await?;
    assert!(first.body.is_empty());
    assert!(first
        .headers
        .to_ascii_lowercase()
        .contains("content-length: 0\r\n"));
    first.respond(503);
    let second = peer.next().await?;
    assert!(second.body.is_empty());
    assert!(second
        .headers
        .to_ascii_lowercase()
        .contains("content-length: 0\r\n"));
    second.respond(200);
    assert_eq!(finished(&handle).await?.response?.attempts, 2);
    assert_eq!(counts.callbacks.load(Ordering::SeqCst), 1);

    // Supplying default options explicitly means inherit the client policy;
    // it must not reapply a request's retry override.
    let counts = Arc::new(Counts::default());
    let mut request = Probe::<()>::new(&counts);
    request.policy = RetryPolicy::new(1).with_non_idempotent(true);
    let handle = client
        .send_with_options(Box::new(request), HttpRequestOptions::new())
        .map_err(|error| error.into_error())?;
    peer.next().await?.respond(503);
    let response = finished(&handle).await?.response?;
    assert_eq!(response.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.attempts, 1);
    assert_eq!(counts.callbacks.load(Ordering::SeqCst), 1);
    client.shutdown_graceful().await?;
    assert!(peer.requests.try_recv().is_err());
    Ok(())
}

#[cfg(feature = "http-client-cookies")]
#[tokio::test]
async fn nested_box_preserves_cookie_context() -> TestResult {
    use open_net::api::http::{CookieJar, CookieRequestContext, CookieStore};
    let mut peer = Peer::new().await?;
    let jar = CookieJar::new();
    let mut cookie_headers = HeaderMap::new();
    cookie_headers.insert(
        "set-cookie",
        HeaderValue::from_static("strict=private; SameSite=Strict; Path=/"),
    );
    jar.set_response_cookies(&url::Url::parse(&peer.url)?, &cookie_headers)?;
    let client = HttpClient::new(
        HttpClientConfig::new(&peer.url)?
            .with_cookie_store(jar)
            .with_environment_proxy(false),
        "boxed-cookie-context",
    )?;
    let mut request = Probe::<()>::new(&Arc::new(Counts::default()));
    request.cookie_context =
        CookieRequestContext::initiated_by(url::Url::parse("https://unrelated.test")?, false);
    let request: Box<dyn HttpRequestTrait> = Box::new(request);
    let handle = client
        .send(Box::new(request))
        .map_err(|error| error.into_error())?;
    let cross_site = peer.next().await?;
    assert!(!cross_site.headers.to_ascii_lowercase().contains("cookie:"));
    cross_site.respond(200);
    finished(&handle).await?.response?;

    let handle = client
        .send(Box::new(Probe::<()>::new(&Arc::new(Counts::default()))))
        .map_err(|error| error.into_error())?;
    let same_site = peer.next().await?;
    assert!(same_site
        .headers
        .to_ascii_lowercase()
        .contains("cookie: strict=private\r\n"));
    same_site.respond(200);
    finished(&handle).await?.response?;
    client.shutdown_graceful().await?;
    Ok(())
}

#[tokio::test]
async fn handle_survives_client_drop_without_retaining_context_or_workers() -> TestResult {
    struct Context;
    let mut peer = Peer::new().await?;
    let context = Arc::new(Context);
    let weak_context = Arc::downgrade(&context);
    let client = peer.client(1)?;
    let counts = Arc::new(Counts::default());
    let handle = client
        .send(Probe::with_context(&counts, context))
        .map_err(|error| error.into_error())?;
    let observed = peer.next().await?;
    drop(client);
    let completion = finished(&handle).await?;
    assert_eq!(
        completion
            .response
            .expect_err("client drop cancels operation")
            .kind(),
        ErrorKind::Cancelled
    );
    completion.callback?;
    assert_eq!(counts.callbacks.load(Ordering::SeqCst), 1);
    assert_eq!(counts.drops.load(Ordering::SeqCst), 1);
    assert!(
        weak_context.upgrade().is_none(),
        "handle must not retain callback context"
    );
    assert_eq!(handle.cancel(), HttpCancelOutcome::TooLate);
    assert_eq!(
        finished(&handle)
            .await?
            .response
            .expect_err("completion retained")
            .kind(),
        ErrorKind::Cancelled
    );
    drop(observed);
    Ok(())
}

#[tokio::test]
async fn fast_completion_is_replayable_for_multiple_waiters() -> TestResult {
    let mut peer = Peer::new().await?;
    let client = peer.client(1)?;
    let counts = Arc::new(Counts::default());
    let mut request = Probe::new(&counts);
    let (callback_tx, callback_rx) = oneshot::channel();
    request.callback = Some(Box::new(move |_, result| {
        assert!(result.is_ok());
        let _ = callback_tx.send(());
    }));
    let handle = client
        .send_wait(request)
        .await
        .map_err(|error| error.into_error())?;
    peer.next().await?.respond(200);
    tokio::time::timeout(WATCHDOG, callback_rx).await??;
    tokio::time::timeout(WATCHDOG, client.drain()).await??;
    // No waiter existed at completion publication. Late subscribers must still
    // observe completion, including concurrent clones and subsequent waits.
    let clone = handle.clone();
    let (first, second) = tokio::join!(finished(&handle), finished(&clone));
    for completion in [first?, second?, finished(&handle).await?] {
        assert_eq!(completion.request_id, handle.id());
        assert_eq!(completion.response?.status, StatusCode::OK);
        completion.callback?;
    }
    assert_eq!(counts.callbacks.load(Ordering::SeqCst), 1);
    client.shutdown_graceful().await?;
    Ok(())
}

#[path = "support/crypto_provider.rs"]
mod crypto_provider;

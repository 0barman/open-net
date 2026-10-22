use crate::api::error::{EnqueueError, ErrorKind, NetError};
use crate::api::http::http_config::HttpClientConfig;
use crate::api::http::http_request::HttpRequest;
use crate::api::http::http_request_trait::HttpRequestTrait;
use crate::api::http::http_response::HttpResponse;
use crate::api::http::retry::HttpRequestOptions;
use crate::api::http::{HttpRequestHandle, HttpRequestId, HttpStreamResponse};
use crate::module::http::http_client_inner::HttpClientInner;
use crate::net_status::{NetworkSnapshot, NetworkStatusContext};
use crate::subscription::{CallbackContext, StateReceiver, Subscription};
use crate::LogType;
use bytes::Bytes;
use http::{HeaderMap, Method};
use std::sync::Arc;
use tokio::time::Instant;

/// A cloneable handle to shared HTTP workers.
///
/// Clones share connections, bounded capacity, request identifiers, and shutdown
/// state. Custom request objects own their application context and callback state.
/// A request is accepted when committed to the request lane. Accepted requests
/// receive one terminal callback, including when cancellation or shutdown begins.
/// Dropping the last owner of the worker initiates cancellation and shutdown;
/// retaining a request's completion handle does not keep the client alive.
#[derive(Clone)]
pub struct HttpClient {
    pub(crate) inner: Arc<HttpClientInner>,
}

impl HttpClient {
    /// Create standalone HTTP worker lanes.
    ///
    /// The thread name must be non-empty and contain no NUL. Application state
    /// can be owned by a custom request or captured by its callback.
    pub fn new(config: HttpClientConfig, thread_name: impl Into<String>) -> Result<Self, NetError> {
        let thread_name = thread_name.into();
        crate::log_s!(LogType::HTTP; "new", "config", format!("{:?}", config));
        if thread_name.trim().is_empty() || thread_name.contains('\0') {
            return Err(NetError::from(ErrorKind::InvalidInput));
        }
        Ok(Self {
            inner: HttpClientInner::new(thread_name, config)?,
        })
    }

    /// Create standalone workers observing the supplied engine's network state.
    /// The context remains bound to that engine's network monitoring service.
    pub fn new_with_network_status(
        config: HttpClientConfig,
        thread_name: impl Into<String>,
        context: NetworkStatusContext,
    ) -> Result<Self, NetError> {
        let thread_name = thread_name.into();
        crate::log_s!(LogType::HTTP; "new_with_network_status", "config", format!("{:?}", config));
        if thread_name.trim().is_empty() || thread_name.contains('\0') {
            return Err(NetError::from(ErrorKind::InvalidInput));
        }
        Ok(Self {
            inner: HttpClientInner::new_with_network_status(thread_name, config, Some(context))?,
        })
    }

    /// Read the shared engine's latest cached network state without starting or retrying monitoring.
    /// This client's terminal `Closed` view remains readable after shutdown. Standalone clients
    /// created without a network context return `InvalidConfig`.
    pub fn network_snapshot(&self) -> Result<NetworkSnapshot, NetError> {
        self.inner.network_snapshot()
    }

    /// Observe cached state and future changes, starting shared monitoring in the background.
    /// Initialization failures are delivered as `MonitorState::Failed`; a new registration can
    /// request a retry. All active observations on this client share one monitoring lease.
    /// Dropping the last receiver or callback releases that lease, and a later subscription
    /// can acquire it again. Network monitoring never pauses or rejects HTTP requests.
    pub fn subscribe_network_status(&self) -> Result<StateReceiver<NetworkSnapshot>, NetError> {
        self.inner.subscribe_network_status()
    }

    /// Deliver network observations on the shared callback pool.
    /// Keep the returned handle alive while observing changes. HTTP shutdown ends observation
    /// without waiting for a running network callback, so a network callback may call shutdown.
    /// Use `Subscription::close` externally to wait for that callback to retire.
    pub fn on_network_status_change<F>(&self, callback: F) -> Result<Subscription, NetError>
    where
        F: Fn(CallbackContext, Result<NetworkSnapshot, NetError>) + Send + Sync + 'static,
    {
        self.subscribe_network_status()?.into_callback(callback)
    }

    /// Attempt admission immediately, returning a handle or the rejected request.
    ///
    /// `Ok` transfers the request to the SDK; it does not mean that an HTTP
    /// response or callback is complete. Call [`HttpRequestHandle::finished`]
    /// to wait for the callback and resource cleanup. `Err` retains the original
    /// request in [`EnqueueError`] and never schedules its callback.
    /// Requests and their `FnOnce` state need only be `Send`, not `Sync` or `Clone`.
    /// Retry selection explicitly inherits, disables, or replaces the client policy.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use open_net::{async_trait, NetError};
    /// use open_net::api::http::{
    ///     HttpClient, HttpRequestMethod, HttpRequestTrait, HttpResponseResult,
    /// };
    /// struct Health;
    /// #[async_trait]
    /// impl HttpRequestTrait for Health {
    ///     fn get_path(&self) -> String { "/health".into() }
    ///     fn get_method(&self) -> HttpRequestMethod { HttpRequestMethod::GET }
    ///     async fn deal_with_response(self: Box<Self>, result: HttpResponseResult) {
    ///         println!("{result:?}");
    ///     }
    /// }
    /// # async fn example(client: &HttpClient) -> Result<(), NetError> {
    /// match client.send(Box::new(Health) as Box<dyn HttpRequestTrait>) {
    ///     Ok(handle) => { handle.finished().await.callback?; }
    ///     Err(rejected) => {
    ///         let (request, error) = rejected.into_parts();
    ///         request.deal_with_response(Err(error)).await;
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn send<R>(&self, request: R) -> Result<HttpRequestHandle, EnqueueError<R>>
    where
        R: HttpRequestTrait,
    {
        let start = Instant::now();
        crate::log_s!(LogType::HTTP; "send", "request_type", std::any::type_name::<R>());
        let options = match Self::request_options(&request, start) {
            Ok(options) => options,
            Err(error) => return Err(EnqueueError::new(request, error)),
        };
        self.inner.submit_request_at(request, options, start)
    }

    /// Attempt admission with explicit execution options, retaining input on failure.
    ///
    /// Options are authoritative: an inherited retry setting uses the client
    /// default, not the request's trait policy. This method does not wait for capacity.
    pub fn send_with_options<R>(
        &self,
        request: R,
        options: HttpRequestOptions,
    ) -> Result<HttpRequestHandle, EnqueueError<R>>
    where
        R: HttpRequestTrait,
    {
        let start = Instant::now();
        crate::log_s!(LogType::HTTP; "send_with_options", "request_type|options", std::any::type_name::<R>(), format!("{:?}", options));
        self.inner.submit_request_at(request, options, start)
    }

    /// Wait for admission capacity, then return a handle for the accepted request.
    ///
    /// This waits for admission, not the response or callback. Dropping the future
    /// before acceptance drops its owned request and releases its reservation;
    /// it cannot return that request or run its callback. Configure an admission
    /// timeout via [`Self::send_wait_with_options`] to receive ownership on timeout.
    /// Admission defaults to 30 seconds; [`super::HttpTimeouts`] can override or disable it.
    /// Do not await this method from a callback holding the last permit on these
    /// same workers; use [`Self::send`] or independent workers instead.
    pub async fn send_wait<R>(&self, request: R) -> Result<HttpRequestHandle, EnqueueError<R>>
    where
        R: HttpRequestTrait,
    {
        let start = Instant::now();
        crate::log_s!(LogType::HTTP; "send_wait", "request_type", std::any::type_name::<R>());
        let options = match Self::request_options(&request, start) {
            Ok(options) => options,
            Err(error) => return Err(EnqueueError::new(request, error)),
        };
        self.inner
            .submit_request_wait_at(request, options, start)
            .await
    }

    /// Wait for bounded admission with explicit retry and timeout options.
    ///
    /// Admission covers both in-flight capacity and a queue slot, using one
    /// deadline from the first poll. Every normal error returns the original
    /// request; cancellation by dropping this future has no return value.
    /// Admission defaults to 30 seconds independently of the attempt timeout.
    pub async fn send_wait_with_options<R>(
        &self,
        request: R,
        options: HttpRequestOptions,
    ) -> Result<HttpRequestHandle, EnqueueError<R>>
    where
        R: HttpRequestTrait,
    {
        let start = Instant::now();
        crate::log_s!(LogType::HTTP; "send_wait_with_options", "request_type|options", std::any::type_name::<R>(), format!("{:?}", options));
        self.inner
            .submit_request_wait_at(request, options, start)
            .await
    }

    fn request_options<R: HttpRequestTrait>(
        request: &R,
        start: Instant,
    ) -> Result<HttpRequestOptions, NetError> {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| request.retry_setting())) {
            Ok(setting) => Ok(HttpRequestOptions::new().with_retry_setting(setting)),
            Err(payload) => {
                if let Err(second) =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(payload)))
                {
                    std::mem::forget(second);
                }
                let mut report = crate::api::retry::RetryReport::new();
                report.elapsed = start.elapsed();
                report.stop_reason = Some(crate::api::retry::RetryStopReason::PolicyFailed);
                Err(NetError::from(ErrorKind::ProviderFailed)
                    .with_stage(crate::api::error::ErrorStage::Provider)
                    .with_http_attempt(0)
                    .with_retry_report(report))
            }
        }
    }

    /// Send a concrete request and wait for its buffered response.
    ///
    /// Admission uses the request override or client [`super::HttpAdmissionPolicy`]
    /// (waiting by default), with a default 30 second Admission budget.
    /// Dropping the waiting future requests cancellation. HTTP status failures are still
    /// `Ok(HttpResponse)` until classified with [`HttpResponse::check_status`].
    pub async fn request(&self, request: HttpRequest) -> Result<HttpResponse, NetError> {
        let start = Instant::now();
        crate::log_s!(LogType::HTTP; "request", "method|body_bytes", request.method.as_str(), request.body.len());
        self.inner.request_buffered(request, start).await
    }

    /// Issue a `GET` request and wait for its fully buffered response.
    ///
    /// `path` must satisfy the client's same-origin relative-path rules. The
    /// request uses the configured default retry policy.
    pub async fn get(&self, path: impl Into<String>) -> Result<HttpResponse, NetError> {
        let start = Instant::now();
        self.inner
            .request_buffered(HttpRequest::builder().path(path).build()?, start)
            .await
    }

    /// Issue a `GET` request with headers that override matching common headers.
    pub async fn get_with_headers(
        &self,
        path: impl Into<String>,
        headers: HeaderMap,
    ) -> Result<HttpResponse, NetError> {
        let start = Instant::now();
        self.inner
            .request_buffered(
                HttpRequest::builder().path(path).headers(headers).build()?,
                start,
            )
            .await
    }

    /// Issue a `POST` request with a copied request body.
    ///
    /// The body is retained as an owned byte buffer so it can be replayed only
    /// when the selected retry policy explicitly permits non-idempotent retries.
    pub async fn post(
        &self,
        path: impl Into<String>,
        body: impl AsRef<[u8]>,
    ) -> Result<HttpResponse, NetError> {
        let start = Instant::now();
        crate::log_s!(LogType::HTTP; "post");
        self.inner
            .request_buffered(
                HttpRequest::builder()
                    .method(Method::POST)
                    .path(path)
                    .body(Bytes::copy_from_slice(body.as_ref()))
                    .build()?,
                start,
            )
            .await
    }

    /// Issue a `POST` request with caller-supplied headers and an owned body.
    pub async fn post_with_headers(
        &self,
        path: impl Into<String>,
        headers: HeaderMap,
        body: impl AsRef<[u8]>,
    ) -> Result<HttpResponse, NetError> {
        let start = Instant::now();
        crate::log_s!(LogType::HTTP; "post_with_headers");
        self.inner
            .request_buffered(
                HttpRequest::builder()
                    .method(Method::POST)
                    .path(path)
                    .headers(headers)
                    .body(Bytes::copy_from_slice(body.as_ref()))
                    .build()?,
                start,
            )
            .await
    }

    /// Compatibility alias for [`Self::get`].
    pub async fn get_bytes(&self, path: impl Into<String>) -> Result<HttpResponse, NetError> {
        self.get(path).await
    }

    /// Compatibility alias for [`Self::get_with_headers`].
    pub async fn get_bytes_with_headers(
        &self,
        path: impl Into<String>,
        headers: HeaderMap,
    ) -> Result<HttpResponse, NetError> {
        self.get_with_headers(path, headers).await
    }

    /// Start a streaming request without buffering the response body.
    ///
    /// Admission uses the request override or client [`super::HttpAdmissionPolicy`]
    /// (waiting by default), with a default 30 second Admission budget.
    /// The returned stream yields owned byte chunks until EOF or a transport
    /// error. Once returned, the response is never replayed, even before its first
    /// body poll. Total deadlines include admission; attempt timeouts cover both
    /// headers and body. Paused consumption continues to consume these budgets.
    /// Consume to a terminal item or drop the stream to return admission capacity.
    pub async fn stream(&self, request: HttpRequest) -> Result<HttpStreamResponse, NetError> {
        self.stream_at(request, Instant::now()).await
    }
    async fn stream_at(
        &self,
        request: HttpRequest,
        start: Instant,
    ) -> Result<HttpStreamResponse, NetError> {
        #[cfg(feature = "http-client-middleware")]
        if self.inner.has_middleware() {
            return self
                .inner
                .stream_request_with_middleware(request, start)
                .await;
        }
        self.inner.stream_request(request, start).await
    }

    /// Start a streaming `GET` request intended for server-sent events.
    ///
    /// The method does not parse SSE frames; callers consume and decode chunks
    /// from [`HttpStreamResponse::next_chunk`].
    pub async fn sse(
        &self,
        path: impl Into<String>,
        headers: HeaderMap,
    ) -> Result<HttpStreamResponse, NetError> {
        let start = Instant::now();
        crate::log_s!(LogType::HTTP; "sse");
        self.stream_at(
            HttpRequest::builder().path(path).headers(headers).build()?,
            start,
        )
        .await
    }

    /// Cancel the operation currently registered under an ID, including a stream.
    ///
    /// For trait submissions, prefer [`HttpRequestHandle::cancel`], which remains
    /// bound to that specific operation even after its ID is reused. This lookup
    /// can target a newer operation if the caller retains an old, reused ID.
    pub fn cancel(&self, request_id: HttpRequestId) -> Result<(), NetError> {
        crate::log_s!(LogType::HTTP; "cancel", "request_id", request_id.0);
        self.inner.cancel(request_id)
    }

    /// Cancel accepted requests and currently registered admission waiters.
    /// Running callbacks are allowed to finish; future submissions are unaffected.
    pub fn cancel_all(&self) -> Result<(), NetError> {
        crate::log_s!(LogType::HTTP; "cancel_all");
        self.inner.cancel_all()
    }

    /// Wait until registered requests and admission reservations have retired.
    ///
    /// `drain` does not close admission; callers may enqueue new requests after
    /// it completes unless shutdown has separately been requested.
    pub async fn drain(&self) -> Result<(), NetError> {
        crate::log_s!(LogType::HTTP; "drain");
        self.inner.drain().await
    }

    /// Close admission, cancel registered requests, and join the worker lanes.
    ///
    /// Like [`Self::shutdown`], this works on an executor without a Tokio runtime.
    /// Cancelling the waiting future does not cancel accepted cleanup work.
    pub async fn shutdown_graceful(&self) -> Result<(), NetError> {
        crate::log_s!(LogType::HTTP; "shutdown_graceful");
        self.inner.request_shutdown();
        self.inner.cancel_all()?;
        self.inner.shutdown_and_join().await
    }

    /// Close admission and wait for the shared shutdown completion.
    ///
    /// Concurrent callers observe the same completion result. Cancelling one waiter does not
    /// cancel shutdown or make another waiter return before callbacks finish.
    /// Cleanup runs on an SDK-owned thread, so the waiting executor does not need Tokio and
    /// its runtime may stop after its waiter is cancelled. If the OS cannot start cleanup,
    /// this returns `RuntimeUnavailable` without losing worker ownership; a later call may retry.
    /// Do not await shutdown from a request callback on these same workers: shutdown waits
    /// for that callback to return. Request shutdown there and await completion externally.
    pub async fn shutdown(&self) -> Result<(), NetError> {
        crate::log_s!(LogType::HTTP; "shutdown");
        self.inner.request_shutdown();
        self.inner.cancel_all()?;
        self.inner.shutdown_and_join().await
    }

    /// Close admission synchronously without cancelling accepted requests.
    ///
    /// Requests accepted before this call, including active stream and SSE bodies, remain
    /// readable until their normal terminal state. Use [`Self::shutdown`] or [`Self::cancel_all`]
    /// when accepted requests must be cancelled.
    ///
    /// The operation is idempotent and does not wait for queue capacity.
    pub fn request_shutdown(&self) {
        crate::log_s!(LogType::HTTP; "request_shutdown");
        self.inner.request_shutdown();
    }
}

#[cfg(test)]
#[path = "http_network_status_tests.rs"]
mod network_status_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static TEST_CLIENT_ID: AtomicUsize = AtomicUsize::new(0);

    #[test]
    fn standalone_constructor_rejects_invalid_thread_names() -> Result<(), String> {
        let config = HttpClientConfig::new("http://127.0.0.1:1")
            .map_err(|error| format!("config failed: {error:?}"))?;
        for name in ["", "   ", "http\0worker"] {
            match HttpClient::new(config.clone(), name) {
                Ok(client) => {
                    let _ = client.request_shutdown();
                    return Err(format!("invalid thread name was accepted: {name:?}"));
                }
                Err(error) if error.kind() == ErrorKind::InvalidInput => {}
                Err(error) => {
                    return Err(format!(
                        "invalid thread name returned unexpected error: {error:?}"
                    ));
                }
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn standalone_constructor_starts_and_shuts_down_workers() -> Result<(), String> {
        let config = HttpClientConfig::new("http://127.0.0.1:1")
            .map_err(|error| format!("config failed: {error:?}"))?;
        let id = TEST_CLIENT_ID.fetch_add(1, Ordering::Relaxed);
        let client = HttpClient::new(config, format!("http-standalone-test-{id}"))
            .map_err(|error| format!("constructor failed: {error:?}"))?;
        client
            .shutdown()
            .await
            .map_err(|error| format!("shutdown failed: {error:?}"))
    }
}

#[cfg(test)]
mod submission_budget_tests {
    use super::*;
    use crate::api::http::{
        HttpRequestMethod, HttpResponseResult, HttpRetryPolicy, HttpTimeouts, TimeoutPhase,
    };
    use crate::api::retry::{RetrySetting, RetryStopReason};
    use crate::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::time::Duration;
    type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
    fn check(value: bool, message: &str) -> TestResult {
        if value {
            Ok(())
        } else {
            Err(std::io::Error::other(message).into())
        }
    }
    struct SlowRequest {
        before_admission: bool,
        callbacks: Arc<AtomicUsize>,
        sender: mpsc::Sender<HttpResponseResult>,
    }
    #[async_trait]
    impl HttpRequestTrait for SlowRequest {
        fn get_path(&self) -> String {
            "/".to_owned()
        }
        fn get_method(&self) -> HttpRequestMethod {
            HttpRequestMethod::GET
        }
        fn retry_setting(&self) -> RetrySetting<HttpRetryPolicy> {
            if self.before_admission {
                std::thread::sleep(Duration::from_millis(60));
            }
            RetrySetting::Inherit
        }
        fn get_req_body(&self) -> Bytes {
            if !self.before_admission {
                std::thread::sleep(Duration::from_millis(60));
            }
            Bytes::new()
        }
        async fn deal_with_response(self: Box<Self>, result: HttpResponseResult) {
            self.callbacks.fetch_add(1, Ordering::AcqRel);
            let _ = self.sender.send(result);
        }
    }
    #[tokio::test]
    async fn synchronous_sdk_preparation_consumes_the_submission_total() -> TestResult {
        for wait in [false, true] {
            for before_admission in [false, true] {
                let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
                listener.set_nonblocking(true)?;
                let config = HttpClientConfig::new(format!("http://{}", listener.local_addr()?))?
                    .with_timeouts(
                    HttpTimeouts::new().with_total_timeout(Duration::from_millis(20)),
                )?;
                let client = HttpClient::new(config, "http-entry-budget-test")?;
                let callbacks = Arc::new(AtomicUsize::new(0));
                let (sender, receiver) = mpsc::channel();
                let request = SlowRequest {
                    before_admission,
                    callbacks: Arc::clone(&callbacks),
                    sender,
                };
                let admitted = if wait {
                    client.send_wait(request).await
                } else {
                    client.send(request)
                };
                let error = if before_admission {
                    let rejected = match admitted {
                        Ok(_) => {
                            return Err(
                                std::io::Error::other("expired preparation was admitted").into()
                            )
                        }
                        Err(rejected) => rejected,
                    };
                    check(
                        callbacks.load(Ordering::Acquire) == 0,
                        "rejected request callback ran inside SDK",
                    )?;
                    let (request, error) = rejected.into_parts();
                    Box::new(request)
                        .deal_with_response(Err(error.clone()))
                        .await;
                    error
                } else {
                    let handle = admitted?;
                    let completion = handle.finished().await;
                    match completion.response {
                        Ok(_) => {
                            return Err(std::io::Error::other(
                                "expired accepted preparation succeeded",
                            )
                            .into())
                        }
                        Err(error) => error,
                    }
                };
                check(
                    error.timeout_phase() == Some(TimeoutPhase::Total),
                    "preparation did not consume total budget",
                )?;
                let report = error
                    .retry_report()
                    .ok_or_else(|| std::io::Error::other("missing preparation retry report"))?;
                check(
                    report.transport_sends == 0
                        && report.attempts == u32::from(!before_admission)
                        && report.stop_reason == Some(RetryStopReason::TimedOut),
                    "preparation report has incorrect sends/attempts/stop",
                )?;
                check(
                    callbacks.load(Ordering::Acquire) == 1,
                    "request callback ownership was violated",
                )?;
                let delivered = receiver.recv_timeout(Duration::from_secs(1))?;
                check(
                    delivered.is_err(),
                    "callback did not receive terminal error",
                )?;
                check(
                    matches!(listener.accept(),Err(error) if error.kind()==std::io::ErrorKind::WouldBlock),
                    "expired preparation reached the transport",
                )?;
                client.shutdown().await?;
            }
        }
        Ok(())
    }
}

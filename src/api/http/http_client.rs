use crate::api::error::{ErrorKind, ErrorStage, NetError};
use crate::api::http::http_config::HttpClientConfig;
use crate::api::http::http_request::HttpRequest;
use crate::api::http::http_request_trait::HttpRequestTrait;
use crate::api::http::http_response::{HttpRequestId, HttpResponse};
use crate::api::http::retry::HttpRequestOptions;
use crate::api::http::HttpStreamResponse;
use crate::module::http::http_client_inner::{HttpClientInner, RegisteredRequest, SendJob};
use crate::module::http::request_wait_guard::RequestWaitGuard;
use crate::module::http::result_request::ResultRequest;
use crate::net_status::{NetworkSnapshot, NetworkStatusContext};
use crate::subscription::{CallbackContext, StateReceiver, Subscription};
use crate::LogType;
use bytes::Bytes;
use http::{HeaderMap, Method};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::oneshot;

/// A cloneable handle to one isolated HTTP worker group.
///
/// Each client owns bounded request, response, and callback lanes configured by
/// [`HttpClientConfig`]. Clones share the same queues and shutdown state. A
/// request is considered accepted once it has entered the request lane; from
/// that point the worker guarantees one terminal callback or result, including
/// when shutdown begins.
#[derive(Clone)]
pub struct HttpClient {
    pub(crate) inner: Arc<HttpClientInner>,
}

impl HttpClient {
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
    /// Create a standalone HTTP client and start its worker lanes synchronously.
    ///
    /// This constructor is useful for integrations that own their own client lifecycle and do
    /// not need the named [`crate::OpenNet`] registry. `thread_name` is used as the prefix for
    /// the send, receive, and callback worker threads and must be non-empty and free of NUL
    /// characters. Call [`Self::shutdown`] when the client is no longer needed.
    pub fn new(config: HttpClientConfig, thread_name: impl Into<String>) -> Result<Self, NetError> {
        let thread_name = thread_name.into();
        crate::log_s!(LogType::HTTP; "new", "thread_name|config", thread_name, format!("{:?}", config));
        if thread_name.trim().is_empty() || thread_name.contains('\0') {
            return Err(NetError::from(ErrorKind::InvalidInput));
        }
        Ok(Self {
            inner: HttpClientInner::new(thread_name, config)?,
        })
    }

    /// Create standalone request workers that observe the supplied engine's network state.
    /// The context remains bound to that engine. Its shutdown ends observation while these
    /// independent HTTP request workers remain usable.
    pub fn new_with_network_status(
        config: HttpClientConfig,
        thread_name: impl Into<String>,
        context: NetworkStatusContext,
    ) -> Result<Self, NetError> {
        let thread_name = thread_name.into();
        crate::log_s!(LogType::HTTP; "new_with_network_status", "thread_name|config", thread_name, format!("{:?}", config));
        if thread_name.trim().is_empty() || thread_name.contains('\0') {
            return Err(NetError::from(ErrorKind::InvalidInput));
        }
        Ok(Self {
            inner: HttpClientInner::new_with_network_status(thread_name, config, Some(context))?,
        })
    }

    /// Enqueue a request without waiting for network I/O or user callback completion.
    ///
    /// A successful return is the admission point: the request owns one terminal callback, even
    /// if the client is closed immediately afterward. An error means admission failed and no
    /// callback is scheduled. The request's retry policy is used when it enables retries;
    /// otherwise the client default applies.
    /// # Examples
    ///
    /// ```no_run
    /// pub struct TestRequest<CB>
    /// where
    ///     CB: FnOnce(Result<String, i32>) + Send + 'static,
    /// {
    ///     cb: CB,
    /// }
    ///
    /// impl<CB> TestRequest<CB>
    /// where
    ///     CB: FnOnce(Result<String, i32>) + Send + 'static,
    /// {
    ///     pub fn new(_addr: String, cb: CB) -> Self {
    ///         Self { cb }
    ///     }
    /// }
    ///
    /// #[open_net::async_trait]
    /// impl<CB> HttpRequestTrait for TestRequest<CB>
    /// where
    ///     CB: FnOnce(Result<String, i32>) + Send + 'static,
    /// {
    ///     fn get_path(&self) -> String {
    ///         String::from("/api/v1/test")
    ///     }
    ///
    ///     fn get_method(&self) -> HttpRequestMethod {
    ///         HttpRequestMethod::POST
    ///     }
    ///
    ///     async fn deal_with_response(self: Box<Self>, result: HttpResponseResult) {
    ///         let outcome = match result {
    ///             Ok(response) => {
    ///                 let status = i32::from(response.status.as_u16());
    ///                 match response.json::<Value>() {
    ///                     Ok(body) => {
    ///                         // todo
    ///                         Ok(String::new())
    ///                     }
    ///                     Err(error) => {
    ///                         eprintln!("failed to parse HTTP response JSON: {error}");
    ///                         Err(-2)
    ///                     }
    ///                 }
    ///             }
    ///             Err(error) => {
    ///                 eprintln!("HTTP request failed before receiving a response: {error}");
    ///                 Err(-3)
    ///             }
    ///         };
    ///
    ///         (self.cb)(outcome);
    ///     }
    /// }
    ///
    /// let net = OpenNet::new_with_config(OpenNetConfig::default().with_runtime_worker_threads(2))?;
    /// let mut headers: HeaderMap = HeaderMap::new();
    /// headers.insert("Content-Type", "application/json".parse()?);
    /// let mut config = HttpClientConfig::new(BASE_URL)?
    ///     .with_timeout(Duration::from_millis(5000))?
    ///     .with_common_headers(headers)?;
    /// let current_thread = thread::current();
    /// // A-thread_name: main, thread_id: ThreadId(1)
    /// println!(
    ///     "A-thread_name: {}, thread_id: {:?}",
    ///     current_thread.name().unwrap_or("unnamed"),
    ///     current_thread.id()
    /// );
    /// let client = net
    ///     .create_http_client_with_config("test_http_thread", config)
    ///     .await?;
    /// let req = TestRequest::new(String::new(), |ret| {
    ///     let current_thread = thread::current();
    ///     // B-thread_name: test_http_thread-callback, thread_id: ThreadId(17)
    ///     println!(
    ///         "B-thread_name: {}, thread_id: {:?}",
    ///         current_thread.name().unwrap_or("unnamed"),
    ///         current_thread.id()
    ///     );
    /// });
    /// let ret: Result<HttpRequestId, NetError> = client.send(req);
    /// println!("send_ret: ret: {:?}", ret);
    /// ```
    pub fn send<R>(&self, request: R) -> Result<HttpRequestId, NetError>
    where
        R: HttpRequestTrait,
    {
        crate::log_s!(LogType::HTTP; "send", "request_type", std::any::type_name::<R>());
        let retry_policy = request.retry_policy();
        let options = if retry_policy.max_retries() > 0 {
            HttpRequestOptions::default().with_retry_policy(retry_policy)
        } else {
            HttpRequestOptions::default()
        };
        self.send_with_options(request, options)
    }

    /// Enqueue a request with an explicit retry and replay policy.
    ///
    /// Use `HttpRequestOptions::with_retry_policy(RetryPolicy::no_retry())` to override a
    /// client default that enables retries.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// pub struct TestRequest<CB>
    /// where
    ///     CB: FnOnce(Result<String, i32>) + Send + 'static,
    /// {
    ///     cb: CB,
    /// }
    ///
    /// impl<CB> TestRequest<CB>
    /// where
    ///     CB: FnOnce(Result<String, i32>) + Send + 'static,
    /// {
    ///     pub fn new(_addr: String, cb: CB) -> Self {
    ///         Self { cb }
    ///     }
    /// }
    ///
    /// #[open_net::async_trait]
    /// impl<CB> HttpRequestTrait for TestRequest<CB>
    /// where
    ///     CB: FnOnce(Result<String, i32>) + Send + 'static,
    /// {
    ///     fn get_path(&self) -> String {
    ///         String::from("/api/v1/test")
    ///     }
    ///
    ///     fn get_method(&self) -> HttpRequestMethod {
    ///         HttpRequestMethod::POST
    ///     }
    ///
    ///     async fn deal_with_response(self: Box<Self>, result: HttpResponseResult) {
    ///         let outcome = match result {
    ///             Ok(response) => {
    ///                 let status = i32::from(response.status.as_u16());
    ///                 match response.json::<Value>() {
    ///                     Ok(body) => {
    ///                         // todo
    ///                         Ok(String::new())
    ///                     }
    ///                     Err(error) => {
    ///                         eprintln!("failed to parse HTTP response JSON: {error}");
    ///                         Err(-2)
    ///                     }
    ///                 }
    ///             }
    ///             Err(error) => {
    ///                 eprintln!("HTTP request failed before receiving a response: {error}");
    ///                 Err(-3)
    ///             }
    ///         };
    ///
    ///         (self.cb)(outcome);
    ///     }
    /// }
    ///
    /// let net = OpenNet::new_with_config(OpenNetConfig::default().with_runtime_worker_threads(2))?;
    /// let mut headers: HeaderMap = HeaderMap::new();
    /// headers.insert("Content-Type", "application/json".parse()?);
    /// let mut config = HttpClientConfig::new(BASE_URL)?
    ///     .with_timeout(Duration::from_millis(5000))?
    ///     .with_common_headers(headers)?;
    /// let current_thread = thread::current();
    /// // A-thread_name: main, thread_id: ThreadId(1)
    /// println!(
    ///     "A-thread_name: {}, thread_id: {:?}",
    ///     current_thread.name().unwrap_or("unnamed"),
    ///     current_thread.id()
    /// );
    /// let client = net
    ///     .create_http_client_with_config("test_http_thread", config)
    ///     .await?;
    /// let req = TestRequest::new(String::new(), |ret| {
    ///     let current_thread = thread::current();
    ///     // B-thread_name: test_http_thread-callback, thread_id: ThreadId(17)
    ///     println!(
    ///         "B-thread_name: {}, thread_id: {:?}",
    ///         current_thread.name().unwrap_or("unnamed"),
    ///         current_thread.id()
    ///     );
    /// });
    /// let options = HttpRequestOptions::new().with_retry_policy(RetryPolicy::no_retry());
    /// let ret: Result<HttpRequestId, NetError> = client.send_with_options(req, options);
    /// println!("send_with_options_ret: ret: {:?}", ret);
    /// ```
    pub fn send_with_options<R>(
        &self,
        request: R,
        options: HttpRequestOptions,
    ) -> Result<HttpRequestId, NetError>
    where
        R: HttpRequestTrait,
    {
        crate::log_s!(LogType::HTTP; "send_with_options", "request_type|options", std::any::type_name::<R>(), format!("{:?}", options));
        self.send_registered(request, options)
            .map(|registered| registered.id)
    }

    fn send_registered<R>(
        &self,
        request: R,
        mut options: HttpRequestOptions,
    ) -> Result<RegisteredRequest, NetError>
    where
        R: HttpRequestTrait,
    {
        crate::log_s!(LogType::HTTP; "send_registered", "request_type|options", std::any::type_name::<R>(), format!("{:?}", options));
        let operation_start = Instant::now();
        let permit = self.inner.try_acquire_permit()?;
        let (registered, registration) = self.inner.register_request(options.request_id)?;
        let request_id = registered.id;
        options.request_id = Some(request_id);
        let control = Arc::clone(&registered.control);
        let job = SendJob {
            request: Box::new(request),
            options,
            request_id,
            control,
            registry: self.inner.registry(),
            permit,
            operation_start,
        };
        match self.inner.submit(job) {
            Ok(()) => {
                registration.commit();
                Ok(registered)
            }
            Err(error) => Err(error),
        }
    }

    /// Enqueue a request while waiting for bounded queue capacity.
    pub async fn send_wait<R>(&self, request: R) -> Result<HttpRequestId, NetError>
    where
        R: HttpRequestTrait,
    {
        crate::log_s!(LogType::HTTP; "send_wait", "request_type", std::any::type_name::<R>());
        let retry_policy = request.retry_policy();
        let options = if retry_policy.max_retries() > 0 {
            HttpRequestOptions::default().with_retry_policy(retry_policy)
        } else {
            HttpRequestOptions::default()
        };
        self.send_wait_with_options(request, options).await
    }

    /// Enqueue a request with bounded backpressure instead of failing on a full queue.
    ///
    /// Dropping this future before it returns cancels admission and releases its request ID. Once
    /// it returns `Ok`, the request follows the same terminal-callback contract as [`Self::send`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// pub struct TestRequest<CB>
    /// where
    ///     CB: FnOnce(Result<String, i32>) + Send + 'static,
    /// {
    ///     cb: CB,
    /// }
    ///
    /// impl<CB> TestRequest<CB>
    /// where
    ///     CB: FnOnce(Result<String, i32>) + Send + 'static,
    /// {
    ///     pub fn new(_addr: String, cb: CB) -> Self {
    ///         Self { cb }
    ///     }
    /// }
    ///
    /// #[open_net::async_trait]
    /// impl<CB> HttpRequestTrait for TestRequest<CB>
    /// where
    ///     CB: FnOnce(Result<String, i32>) + Send + 'static,
    /// {
    ///     fn get_path(&self) -> String {
    ///         String::from("/api/v1/test")
    ///     }
    ///
    ///     fn get_method(&self) -> HttpRequestMethod {
    ///         HttpRequestMethod::POST
    ///     }
    ///
    ///     async fn deal_with_response(self: Box<Self>, result: HttpResponseResult) {
    ///         let outcome = match result {
    ///             Ok(response) => {
    ///                 let status = i32::from(response.status.as_u16());
    ///                 match response.json::<Value>() {
    ///                     Ok(body) => {
    ///                         // todo
    ///                         Ok(String::new())
    ///                     }
    ///                     Err(error) => {
    ///                         eprintln!("failed to parse HTTP response JSON: {error}");
    ///                         Err(-2)
    ///                     }
    ///                 }
    ///             }
    ///             Err(error) => {
    ///                 eprintln!("HTTP request failed before receiving a response: {error}");
    ///                 Err(-3)
    ///             }
    ///         };
    ///
    ///         (self.cb)(outcome);
    ///     }
    /// }
    ///
    /// let net = OpenNet::new_with_config(OpenNetConfig::default().with_runtime_worker_threads(2))?;
    /// let mut headers: HeaderMap = HeaderMap::new();
    /// headers.insert("Content-Type", "application/json".parse()?);
    /// let mut config = HttpClientConfig::new(BASE_URL)?
    ///     .with_timeout(Duration::from_millis(5000))?
    ///     .with_common_headers(headers)?;
    /// let current_thread = thread::current();
    /// println!(
    ///     "A-thread_name: {}, thread_id: {:?}",
    ///     current_thread.name().unwrap_or("unnamed"),
    ///     current_thread.id()
    /// );
    /// let client = net
    ///     .create_http_client_with_config("test_http_thread", config)
    ///     .await?;
    /// let req = TestRequest::new(String::new(), |ret| {
    ///     let current_thread = thread::current();
    ///     println!(
    ///         "B-thread_name: {}, thread_id: {:?}",
    ///         current_thread.name().unwrap_or("unnamed"),
    ///         current_thread.id()
    ///     );
    ///     println!("send_wait_with_options_callback_ret: ret: {:?}", ret);
    /// });
    /// let options = HttpRequestOptions::new().with_retry_policy(RetryPolicy::no_retry());
    /// let ret: Result<HttpRequestId, NetError> = client.send_wait_with_options(req, options).await;
    /// ```
    pub async fn send_wait_with_options<R>(
        &self,
        request: R,
        mut options: HttpRequestOptions,
    ) -> Result<HttpRequestId, NetError>
    where
        R: HttpRequestTrait,
    {
        crate::log_s!(LogType::HTTP; "send_wait_with_options", "request_type|options", std::any::type_name::<R>(), format!("{:?}", options));
        let operation_start = Instant::now();
        let permit = self.inner.acquire_permit().await?;
        let (registered, registration) = self.inner.register_request(options.request_id)?;
        let request_id = registered.id;
        options.request_id = Some(request_id);
        let control = Arc::clone(&registered.control);
        let job = SendJob {
            request: Box::new(request),
            options,
            request_id,
            control,
            registry: self.inner.registry(),
            permit,
            operation_start,
        };
        match self.inner.submit_wait(job).await {
            Ok(()) => {
                registration.commit();
                Ok(request_id)
            }
            Err(error) => Err(error),
        }
    }

    /// Send a request and await its final buffered response.
    ///
    /// The request's callback is bridged to this future. A transport or
    /// cancellation failure is returned as `Err`; an HTTP status such as 404 is
    /// still returned as `Ok(HttpResponse)` and can be classified with
    /// [`HttpResponse::check_status`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// let net = OpenNet::new_with_config(OpenNetConfig::default().with_runtime_worker_threads(2))?;
    /// let mut headers: HeaderMap = HeaderMap::new();
    /// headers.insert("Content-Type", "application/json".parse()?);
    /// let mut config = HttpClientConfig::new(BASE_URL)?
    ///     .with_timeout(Duration::from_millis(5000))?
    ///     .with_common_headers(headers)?;
    /// let client = net
    ///     .create_http_client_with_config("test_http_thread", config)
    ///     .await?;
    /// let request = HttpRequest::builder()
    ///     .method(HttpRequestMethod::POST)
    ///     .path("/api/v1/test")
    ///     .body(Bytes::from_static(br#"{}"#))
    ///     .build()?;
    ///
    /// let ret = client.request(request).await;
    /// match ret {
    ///     Ok(r) => {
    ///         println!("Got response: {:?}", r);
    ///     }
    ///     Err(e) => {
    ///         println!("Got error: {:?}", e);
    ///     }
    /// }
    /// ```
    pub async fn request(&self, request: HttpRequest) -> Result<HttpResponse, NetError> {
        crate::log_s!(LogType::HTTP; "request", "method|body_bytes", request.method.as_str(), request.body.len());
        let (sender, receiver) = oneshot::channel();
        let options = HttpRequestOptions::new().with_retry_setting(request.retry_setting());
        let registered = self.send_registered(ResultRequest::new(request, sender), options)?;
        let wait_guard = RequestWaitGuard::new(Arc::clone(&self.inner), registered.control);
        let result = match receiver.await {
            Ok(result) => result,
            Err(_) => Err(NetError::from(crate::api::error::ErrorKind::Internal)
                .with_stage(ErrorStage::Dispatch)),
        };
        wait_guard.disarm();
        result
    }

    /// Issue a `GET` request and wait for its fully buffered response.
    ///
    /// `path` must satisfy the client's same-origin relative-path rules. The
    /// request uses the configured default retry policy.
    pub async fn get(&self, path: impl Into<String>) -> Result<HttpResponse, NetError> {
        self.request(HttpRequest::builder().path(path).build()?)
            .await
    }

    /// Issue a `GET` request with headers that override matching common headers.
    pub async fn get_with_headers(
        &self,
        path: impl Into<String>,
        headers: HeaderMap,
    ) -> Result<HttpResponse, NetError> {
        self.request(HttpRequest::builder().path(path).headers(headers).build()?)
            .await
    }

    /// Issue a `POST` request with a copied request body.
    ///
    /// The body is retained as an owned byte buffer so it can be replayed only
    /// when the selected retry policy explicitly permits non-idempotent retries.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// let net = OpenNet::new_with_config(OpenNetConfig::default().with_runtime_worker_threads(2))?;
    /// let mut headers: HeaderMap = HeaderMap::new();
    /// headers.insert("Content-Type", "application/json".parse()?);
    /// let mut config = HttpClientConfig::new(BASE_URL)?
    ///     .with_timeout(Duration::from_millis(5000))?
    ///     .with_common_headers(headers)?;
    /// let client = net
    ///     .create_http_client_with_config("test_http_thread", config)
    ///     .await?;
    ///
    /// let ret = client.post("/api/v1/test", br#"{}"#).await;
    /// match ret {
    ///     Ok(r) => {
    ///         println!("Got response: {:?}", r);
    ///     }
    ///     Err(e) => {
    ///         println!("Got error: {:?}", e);
    ///     }
    /// }
    /// ```
    pub async fn post(
        &self,
        path: impl Into<String>,
        body: impl AsRef<[u8]>,
    ) -> Result<HttpResponse, NetError> {
        let path = path.into();
        crate::log_s!(LogType::HTTP; "post", "path", path);
        self.request(
            HttpRequest::builder()
                .method(Method::POST)
                .path(path)
                .body(Bytes::copy_from_slice(body.as_ref()))
                .build()?,
        )
        .await
    }

    /// Issue a `POST` request with caller-supplied headers and an owned body.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// let net = OpenNet::new_with_config(OpenNetConfig::default().with_runtime_worker_threads(2))?;
    /// let mut headers: HeaderMap = HeaderMap::new();
    /// headers.insert("Content-Type", "application/json".parse()?);
    /// let mut config = HttpClientConfig::new(BASE_URL)?
    ///     .with_timeout(Duration::from_millis(5000))?
    ///     .with_common_headers(headers)?;
    /// let client = net
    ///     .create_http_client_with_config("test_http_thread", config)
    ///     .await?;
    ///
    /// let mut request_headers: HeaderMap = HeaderMap::new();
    /// request_headers.insert("test-header", "test5".parse()?);
    /// let ret = client
    ///     .post_with_headers("/api/v1/test", request_headers, br#"{}"#)
    ///     .await;
    /// match ret {
    ///     Ok(r) => {
    ///         println!("Got response: {:?}", r);
    ///     }
    ///     Err(e) => {
    ///         println!("Got error: {:?}", e);
    ///     }
    /// }
    /// ```
    pub async fn post_with_headers(
        &self,
        path: impl Into<String>,
        headers: HeaderMap,
        body: impl AsRef<[u8]>,
    ) -> Result<HttpResponse, NetError> {
        let path = path.into();
        crate::log_s!(LogType::HTTP; "post_with_headers", "path", path);
        self.request(
            HttpRequest::builder()
                .method(Method::POST)
                .path(path)
                .headers(headers)
                .body(Bytes::copy_from_slice(body.as_ref()))
                .build()?,
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
    /// The returned stream yields owned byte chunks until EOF or a transport
    /// error. Once returned, the response is never replayed, even before its first
    /// body poll. Total deadlines include admission; attempt timeouts cover both
    /// headers and body. Paused consumption continues to consume these budgets.
    /// Consume to a terminal item or drop the stream to return admission capacity.
    pub async fn stream(&self, request: HttpRequest) -> Result<HttpStreamResponse, NetError> {
        self.inner.stream_request(request).await
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
        let path = path.into();
        crate::log_s!(LogType::HTTP; "sse", "path", path);
        self.stream(HttpRequest::builder().path(path).headers(headers).build()?)
            .await
    }

    /// Cancel one accepted request by identifier.
    ///
    /// Cancellation is best effort with respect to a callback that is already
    /// running. The worker still retires the request's internal registration so
    /// a late response cannot be delivered twice.
    pub fn cancel(&self, request_id: HttpRequestId) -> Result<(), NetError> {
        crate::log_s!(LogType::HTTP; "cancel", "request_id", request_id.0);
        self.inner.cancel(request_id)
    }

    /// Cancel every accepted request currently owned by this client.
    pub fn cancel_all(&self) -> Result<(), NetError> {
        crate::log_s!(LogType::HTTP; "cancel_all");
        self.inner.cancel_all()
    }

    /// Wait until requests already admitted to the workers reach a terminal state.
    ///
    /// `drain` does not close admission; callers may enqueue new requests after
    /// it completes unless shutdown has separately been requested.
    pub async fn drain(&self) -> Result<(), NetError> {
        crate::log_s!(LogType::HTTP; "drain");
        self.inner.drain().await
    }

    /// Cancel accepted requests, then wait until callbacks and all worker lanes have completed.
    pub async fn shutdown_graceful(&self) -> Result<(), NetError> {
        crate::log_s!(LogType::HTTP; "shutdown_graceful");
        self.inner.cancel_all()?;
        self.inner.shutdown_and_join().await
    }

    /// Close admission and wait for the shared shutdown completion.
    ///
    /// Concurrent callers observe the same completion result. Cancelling one waiter does not
    /// cancel shutdown or make another waiter return before callbacks finish.
    pub async fn shutdown(&self) -> Result<(), NetError> {
        crate::log_s!(LogType::HTTP; "shutdown");
        self.inner.cancel_all()?;
        self.inner.shutdown_and_join().await
    }

    /// Publish shutdown synchronously. It is idempotent and does not wait for queue capacity.
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

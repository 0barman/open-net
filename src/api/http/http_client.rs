use crate::api::error::ErrorStage;
use crate::api::error::NetError;
use crate::api::http::http_config::HttpClientConfig;
use crate::api::http::http_request::HttpRequest;
use crate::api::http::http_request_trait::HttpRequestTrait;
use crate::api::http::http_response::{HttpRequestId, HttpResponse};
use crate::api::http::retry::HttpRequestOptions;
use crate::api::http::HttpStreamResponse;
use crate::module::http::http_client_inner::{HttpClientInner, SendJob};
use crate::module::http::result_request::ResultRequest;
use bytes::Bytes;
use http::{HeaderMap, Method};
use std::sync::Arc;
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
    pub(crate) fn new(config: HttpClientConfig, thread_name: String) -> Result<Self, NetError> {
        Ok(Self {
            inner: HttpClientInner::new(thread_name, config)?,
        })
    }

    /// Enqueue a request without waiting for network I/O or user callback completion.
    ///
    /// A successful return is the admission point: the request owns one terminal callback, even
    /// if the client is closed immediately afterward. An error means admission failed and no
    /// callback is scheduled. The request's retry policy is used when it enables retries;
    /// otherwise the client default applies.
    pub fn send<R>(&self, request: R) -> Result<HttpRequestId, NetError>
    where
        R: HttpRequestTrait,
    {
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
    pub fn send_with_options<R>(
        &self,
        request: R,
        mut options: HttpRequestOptions,
    ) -> Result<HttpRequestId, NetError>
    where
        R: HttpRequestTrait,
    {
        let request_id = match options.request_id {
            Some(request_id) => request_id,
            None => self.inner.allocate_request_id(),
        };
        options.request_id = Some(request_id);
        let (control, registration) = self.inner.register_request(request_id)?;
        let job = SendJob {
            request: Box::new(request),
            options,
            request_id,
            control,
            registry: self.inner.registry(),
        };
        match self.inner.submit(job) {
            Ok(()) => {
                registration.commit();
                Ok(request_id)
            }
            Err(error) => Err(error),
        }
    }

    /// Enqueue a request while waiting for bounded queue capacity.
    pub async fn send_wait<R>(&self, request: R) -> Result<HttpRequestId, NetError>
    where
        R: HttpRequestTrait,
    {
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
    pub async fn send_wait_with_options<R>(
        &self,
        request: R,
        mut options: HttpRequestOptions,
    ) -> Result<HttpRequestId, NetError>
    where
        R: HttpRequestTrait,
    {
        let request_id = match options.request_id {
            Some(request_id) => request_id,
            None => self.inner.allocate_request_id(),
        };
        options.request_id = Some(request_id);
        let (control, registration) = self.inner.register_request(request_id)?;
        let job = SendJob {
            request: Box::new(request),
            options,
            request_id,
            control,
            registry: self.inner.registry(),
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
    pub async fn request(&self, request: HttpRequest) -> Result<HttpResponse, NetError> {
        let (sender, receiver) = oneshot::channel();
        let options = HttpRequestOptions::new().with_retry_policy(request.retry_policy.clone());
        self.send_with_options(ResultRequest::new(request, sender), options)?;
        match receiver.await {
            Ok(result) => result,
            Err(_) => Err(NetError::from(crate::api::error::ErrorKind::Internal)
                .with_stage(ErrorStage::Dispatch)),
        }
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
    pub async fn post(
        &self,
        path: impl Into<String>,
        body: impl AsRef<[u8]>,
    ) -> Result<HttpResponse, NetError> {
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
    pub async fn post_with_headers(
        &self,
        path: impl Into<String>,
        headers: HeaderMap,
        body: impl AsRef<[u8]>,
    ) -> Result<HttpResponse, NetError> {
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
    /// error. Retry policies do not replay a partially consumed response body.
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
        self.stream(HttpRequest::builder().path(path).headers(headers).build()?)
            .await
    }

    /// Cancel one accepted request by identifier.
    ///
    /// Cancellation is best effort with respect to a callback that is already
    /// running. The worker still retires the request's internal registration so
    /// a late response cannot be delivered twice.
    pub fn cancel(&self, request_id: HttpRequestId) -> Result<(), NetError> {
        self.inner.cancel(request_id)
    }

    /// Cancel every accepted request currently owned by this client.
    pub fn cancel_all(&self) -> Result<(), NetError> {
        self.inner.cancel_all()
    }

    /// Wait until requests already admitted to the workers reach a terminal state.
    ///
    /// `drain` does not close admission; callers may enqueue new requests after
    /// it completes unless shutdown has separately been requested.
    pub async fn drain(&self) -> Result<(), NetError> {
        self.inner.drain().await
    }

    /// Cancel accepted requests, then wait until callbacks and all worker lanes have completed.
    pub async fn shutdown_graceful(&self) -> Result<(), NetError> {
        self.inner.cancel_all()?;
        self.inner.shutdown_and_join().await
    }

    /// Close admission and wait for the shared shutdown completion.
    ///
    /// Concurrent callers observe the same completion result. Cancelling one waiter does not
    /// cancel shutdown or make another waiter return before callbacks finish.
    pub async fn shutdown(&self) -> Result<(), NetError> {
        self.inner.shutdown_and_join().await
    }

    /// Publish shutdown synchronously. It is idempotent and does not wait for queue capacity.
    pub fn request_shutdown(&self) {
        self.inner.request_shutdown();
    }
}

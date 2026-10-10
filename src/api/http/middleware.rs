#![cfg(feature = "http-client-middleware")]

//! Composable middleware for concrete HTTP request and stream APIs.

use crate::api::error::{ErrorKind, ErrorStage, NetError};
use crate::api::http::{HttpRequest, HttpRequestId, HttpResponse, HttpStreamResponse};
use async_trait::async_trait;
use http::Method;
use std::future::Future;
use std::pin::Pin;

/// Selects the response shape expected by one middleware call.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpResponseMode {
    /// The response body is collected before the call completes.
    Buffered,
    /// The response headers are returned while the body remains incremental.
    Streaming,
}

/// Sanitized metadata that is safe to use in middleware diagnostics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HttpCallMetadata {
    /// Identifier assigned after request admission.
    pub request_id: HttpRequestId,
    /// HTTP method of the request.
    pub method: Method,
    /// Request path without query or fragment data.
    pub path: String,
    /// Expected response mode.
    pub mode: HttpResponseMode,
    /// Number of request body bytes currently held by the request.
    pub body_bytes: usize,
}

/// A request admitted by the client and handed to one middleware layer.
pub struct HttpCall {
    request: HttpRequest,
    request_id: HttpRequestId,
    mode: HttpResponseMode,
}

impl HttpCall {
    pub(crate) fn new(
        request: HttpRequest,
        request_id: HttpRequestId,
        mode: HttpResponseMode,
    ) -> Self {
        Self {
            request,
            request_id,
            mode,
        }
    }

    /// Borrow the concrete request being processed.
    pub fn request(&self) -> &HttpRequest {
        &self.request
    }

    /// Mutably borrow the concrete request before passing it to [`Next::run`].
    pub fn request_mut(&mut self) -> &mut HttpRequest {
        &mut self.request
    }

    /// Return metadata with query, fragment, headers, and body contents removed.
    pub fn metadata(&self) -> HttpCallMetadata {
        let path = self
            .request
            .path
            .split(['?', '#'])
            .next()
            .map_or_else(String::new, ToOwned::to_owned);
        HttpCallMetadata {
            request_id: self.request_id,
            method: self.request.method.clone(),
            path,
            mode: self.mode,
            body_bytes: self.request.body.len(),
        }
    }

    /// Return the request identifier assigned by admission.
    pub fn request_id(&self) -> HttpRequestId {
        self.request_id
    }

    /// Return the response mode selected by the client entry point.
    pub fn mode(&self) -> HttpResponseMode {
        self.mode
    }

    pub(crate) fn into_parts(self) -> (HttpRequest, HttpRequestId, HttpResponseMode) {
        (self.request, self.request_id, self.mode)
    }
}

/// Result returned by the middleware chain.
pub enum HttpMiddlewareResponse {
    /// A fully buffered response.
    Buffered(HttpResponse),
    /// A response whose body is still consumed incrementally.
    Streaming(HttpStreamResponse),
}

impl HttpMiddlewareResponse {
    /// Return the response mode carried by this value.
    pub fn mode(&self) -> HttpResponseMode {
        match self {
            Self::Buffered(_) => HttpResponseMode::Buffered,
            Self::Streaming(_) => HttpResponseMode::Streaming,
        }
    }
}

pub(crate) type MiddlewareFuture =
    Pin<Box<dyn Future<Output = Result<HttpMiddlewareResponse, NetError>> + Send + 'static>>;

pub(crate) type MiddlewareRunner = Box<dyn FnOnce(HttpCall) -> MiddlewareFuture + Send + 'static>;

/// The one-shot continuation supplied to a middleware layer.
pub struct Next {
    runner: Option<MiddlewareRunner>,
}

impl Next {
    pub(crate) fn from_runner(runner: MiddlewareRunner) -> Self {
        Self {
            runner: Some(runner),
        }
    }

    /// Continue the request exactly once.
    pub async fn run(mut self, call: HttpCall) -> Result<HttpMiddlewareResponse, NetError> {
        match self.runner.take() {
            Some(runner) => runner(call).await,
            None => Err(NetError::from(ErrorKind::Internal).with_stage(ErrorStage::Dispatch)),
        }
    }
}

/// A client middleware that can modify, short-circuit, observe, or recover a concrete call.
#[async_trait]
pub trait HttpMiddleware: Send + Sync + 'static {
    /// Process one logical request and optionally continue through the remaining chain.
    async fn call(&self, call: HttpCall, next: Next) -> Result<HttpMiddlewareResponse, NetError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use http::{HeaderMap, HeaderValue, StatusCode};

    type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

    fn check(condition: bool, message: &str) -> TestResult {
        if condition {
            Ok(())
        } else {
            Err(std::io::Error::other(message).into())
        }
    }

    fn request() -> Result<HttpRequest, NetError> {
        HttpRequest::builder()
            .method(Method::POST)
            .path("/items?token=redacted#fragment")
            .body(Bytes::from_static(b"payload"))
            .build()
    }

    #[test]
    fn metadata_contains_only_safe_request_facts() -> TestResult {
        let call = HttpCall::new(request()?, HttpRequestId(7), HttpResponseMode::Buffered);
        let metadata = call.metadata();
        check(
            metadata.request_id == HttpRequestId(7),
            "request id mismatch",
        )?;
        check(metadata.method == Method::POST, "method mismatch")?;
        check(metadata.path == "/items", "path was not sanitized")?;
        check(metadata.mode == HttpResponseMode::Buffered, "mode mismatch")?;
        check(metadata.body_bytes == 7, "body size mismatch")
    }

    #[tokio::test]
    async fn next_runs_the_terminal_once_and_preserves_response_mode() -> TestResult {
        let request_id = HttpRequestId(11);
        let call = HttpCall::new(request()?, request_id, HttpResponseMode::Buffered);
        let next = Next::from_runner(Box::new(move |call| {
            let request_id = call.request_id();
            Box::pin(async move {
                Ok(HttpMiddlewareResponse::Buffered(HttpResponse::synthetic(
                    request_id,
                    StatusCode::NO_CONTENT,
                    HeaderMap::new(),
                    Bytes::new(),
                )))
            })
        }));
        let response = next.run(call).await?;
        check(
            response.mode() == HttpResponseMode::Buffered,
            "next changed the response mode",
        )?;
        match response {
            HttpMiddlewareResponse::Buffered(response) => {
                check(response.request_id() == request_id, "response id mismatch")?;
                check(
                    response.status == StatusCode::NO_CONTENT,
                    "response status mismatch",
                )
            }
            HttpMiddlewareResponse::Streaming(_) => {
                Err(std::io::Error::other("next returned a streaming response").into())
            }
        }
    }

    #[test]
    fn request_mutation_remains_available_before_next() -> TestResult {
        let mut call = HttpCall::new(request()?, HttpRequestId(13), HttpResponseMode::Streaming);
        call.request_mut()
            .headers
            .insert("x-middleware", HeaderValue::from_static("enabled"));
        check(
            call.request().headers.get("x-middleware")
                == Some(&HeaderValue::from_static("enabled")),
            "request mutation was not retained",
        )
    }
}

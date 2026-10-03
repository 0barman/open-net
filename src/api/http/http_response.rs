//! HTTP response and request identifier contracts.

use crate::api::error::NetError;
use bytes::Bytes;
use http::{HeaderMap, StatusCode};
use serde::de::DeserializeOwned;
use std::fmt;
use std::str::Utf8Error;

/// Monotonic identifier assigned to a request by an HTTP client.
///
/// Automatically assigned identifiers increase without wrapping during a client
/// lifetime. Custom identifiers must be unique among in-flight requests and may
/// be reused after cleanup. Identifiers support cancellation and correlation;
/// they are not server-visible and are not persisted across client restarts.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct HttpRequestId(pub u64);

/// A fully buffered HTTP response delivered to a request callback.
///
/// The body is retained as owned bytes and is intentionally omitted from
/// `Debug` output. Use [`Self::text`] or [`Self::json`] when the payload format
/// is known.
#[derive(Clone)]
pub struct HttpResponse {
    /// Response status code returned by the peer.
    pub status: StatusCode,
    /// Response headers, including values supplied by the peer.
    pub headers: HeaderMap,
    /// Fully buffered response body.
    pub body: Bytes,
    /// Number of transport attempts used to obtain this response.
    pub attempts: u32,
    /// Identifier assigned to the originating request.
    pub request_id: HttpRequestId,
}

impl HttpResponse {
    /// Return the UTF-8 `Content-Type` header value, if present and valid.
    pub fn content_type(&self) -> Option<&str> {
        self.headers.get(http::header::CONTENT_TYPE)?.to_str().ok()
    }

    /// Check the response status against a caller-selected acceptance policy.
    ///
    /// A rejected status is returned as a typed [`NetError`]; the response body
    /// remains available to callers that need to inspect it before propagating
    /// the error.
    pub fn check_status(&self, policy: crate::api::http::HttpStatusPolicy) -> Result<(), NetError> {
        if policy.accepts(self.status) {
            Ok(())
        } else {
            Err(NetError::from_http_status(self.status))
        }
    }
    /// Construct a buffered response from its transport metadata and body.
    pub fn new(
        status: StatusCode,
        headers: HeaderMap,
        body: Bytes,
        attempts: u32,
        request_id: HttpRequestId,
    ) -> Self {
        Self {
            status,
            headers,
            body,
            attempts,
            request_id,
        }
    }

    /// Report whether the status code is in the 2xx success range.
    pub fn is_success(&self) -> bool {
        self.status.is_success()
    }

    /// Interpret the body as UTF-8 without allocating.
    pub fn text(&self) -> Result<&str, Utf8Error> {
        std::str::from_utf8(self.body.as_ref())
    }

    /// Deserialize the body as JSON using `serde_json`.
    pub fn json<T: DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_slice(self.body.as_ref())
    }

    /// Return the response status code.
    pub fn status(&self) -> StatusCode {
        self.status
    }

    /// Borrow the response headers.
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    /// Borrow the buffered response body.
    pub fn body(&self) -> &Bytes {
        &self.body
    }

    /// Return the number of transport attempts used for this response.
    pub fn attempts(&self) -> u32 {
        self.attempts
    }

    /// Return the originating request identifier.
    pub fn request_id(&self) -> HttpRequestId {
        self.request_id
    }
}

impl fmt::Debug for HttpResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpResponse")
            .field("status", &self.status)
            .field("header_count", &self.headers.len())
            .field("body_bytes", &self.body.len())
            .field("attempts", &self.attempts)
            .field("request_id", &self.request_id)
            .finish()
    }
}

/// Result delivered to an HTTP request callback after all attempts complete.
///
/// `Ok` contains a fully buffered response. `Err` contains a transport,
/// configuration, cancellation, or retry-exhaustion error; an HTTP status that
/// is merely unsuccessful is still represented by `Ok` until the caller applies
/// [`HttpResponse::check_status`].
pub type HttpResponseResult = Result<HttpResponse, NetError>;

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;

    type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

    fn check(condition: bool, message: &str) -> TestResult {
        if condition {
            Ok(())
        } else {
            Err(std::io::Error::other(message).into())
        }
    }

    #[test]
    fn response_exposes_status_body_and_json_without_leaking_body_in_debug() -> TestResult {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("application/json"));
        let response = HttpResponse::new(
            StatusCode::OK,
            headers,
            Bytes::from_static(br#"{"value":7}"#),
            2,
            HttpRequestId(9),
        );
        check(response.is_success(), "2xx response must be successful")?;
        check(response.attempts() == 2, "attempt count not retained")?;
        check(
            response.request_id() == HttpRequestId(9),
            "request id not retained",
        )?;
        check(response.text()? == r#"{"value":7}"#, "body text mismatch")?;
        let decoded: serde_json::Value = response.json()?;
        check(decoded["value"] == 7, "JSON body was not decoded")?;
        check(
            !format!("{response:?}").contains("value"),
            "response Debug leaked body",
        )
    }

    #[test]
    fn response_result_keeps_http_failures_distinct_from_transport_errors() -> TestResult {
        let result: HttpResponseResult =
            Err(NetError::from(crate::api::error::ErrorKind::TimedOut));
        let error = match result {
            Ok(_) => return Err(std::io::Error::other("transport result was successful").into()),
            Err(error) => error,
        };
        check(
            error.kind() == crate::api::error::ErrorKind::TimedOut,
            "wrong error kind",
        )
    }
}

use crate::api::http::http_request_method::HttpRequestMethod;
use crate::api::http::http_response::HttpResponseResult;
use crate::api::http::retry::RetryPolicy;
use async_trait::async_trait;
use bytes::Bytes;
use http::HeaderMap;

/// Request object consumed by an HTTP client.
///
/// Implementations own callback state and are moved to the callback lane after the final
/// attempt.  `get_req_data` must return the same body for every attempt when retries are enabled.
#[async_trait]
pub trait HttpRequestTrait: Send + 'static {
    /// Return the request target relative to the client's configured base URL.
    ///
    /// The value is copied before it enters the worker. Absolute or cross-origin
    /// targets are rejected by the client configuration's path validation.
    fn get_path(&self) -> String;

    /// Return the HTTP method for this request.
    fn get_method(&self) -> HttpRequestMethod;

    /// Return a textual request body for legacy request implementations.
    ///
    /// The default is an empty body. Implementations that need arbitrary bytes
    /// should override [`Self::get_req_body`] instead; if both are overridden,
    /// the byte-oriented method is authoritative.
    fn get_req_data(&self) -> String {
        String::new()
    }

    /// Return the owned bytes sent as the request body.
    ///
    /// This method is called once for each attempt. A retry policy may call it
    /// again, so a retryable request must return equivalent replayable data.
    fn get_req_body(&self) -> Bytes {
        Bytes::from(self.get_req_data())
    }

    /// Report whether this request has a body even when its byte representation is empty.
    ///
    /// The default is `false` and does not call a body provider. Requests with
    /// non-empty bytes are recognized from [`Self::get_req_body`]; override
    /// this method only for an explicit empty body that still needs a body
    /// framing header.
    fn has_req_body(&self) -> bool {
        false
    }

    /// Return request headers to merge over the client's common headers.
    ///
    /// Hop-by-hop headers are rejected during configuration/dispatch. The
    /// default returns an empty map.
    fn headers(&self) -> HeaderMap {
        HeaderMap::new()
    }

    /// Return the retry policy requested by this request.
    ///
    /// A policy with no retries allows the client default to remain in effect
    /// when using [`crate::api::http::HttpClient::send`]; explicit options can always override it.
    fn retry_policy(&self) -> RetryPolicy {
        RetryPolicy::no_retry()
    }

    /// Observe response headers on the callback lane.  The default implementation is a no-op.
    /// The callback may inspect the headers but cannot mutate worker state or
    /// replace the eventual response.
    fn on_response_headers(&self, _headers: &HeaderMap) {}

    /// Consume this request with the final result.
    ///
    /// The callback is invoked exactly once after the final attempt, whether
    /// the operation succeeded, exhausted its retry budget, was cancelled, or
    /// failed before a response was available. It runs on the callback lane and
    /// receives ownership of the request object and response/error result.
    async fn deal_with_response(self: Box<Self>, result: HttpResponseResult);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::http::http_response::HttpResponse;
    use http::StatusCode;

    type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

    fn check(condition: bool, message: &str) -> TestResult {
        if condition {
            Ok(())
        } else {
            Err(std::io::Error::other(message).into())
        }
    }

    struct EmptyRequest;

    #[async_trait]
    impl HttpRequestTrait for EmptyRequest {
        fn get_path(&self) -> String {
            "/health".to_owned()
        }

        fn get_method(&self) -> HttpRequestMethod {
            HttpRequestMethod::GET
        }

        async fn deal_with_response(self: Box<Self>, result: HttpResponseResult) {
            let _ = result;
        }
    }

    #[test]
    fn defaults_keep_request_contract_replay_safe_and_header_free() -> TestResult {
        let request = EmptyRequest;
        check(request.get_path() == "/health", "path contract changed")?;
        check(
            request.get_method() == HttpRequestMethod::GET,
            "method contract changed",
        )?;
        check(
            request.get_req_data().is_empty(),
            "default body must be empty",
        )?;
        check(
            request.headers().is_empty(),
            "default headers must be empty",
        )?;
        check(
            request.retry_policy().max_retries() == 0,
            "default requests must not retry",
        )
    }

    #[tokio::test]
    async fn callback_receives_owned_result_contract() -> TestResult {
        let response = HttpResponse::new(
            StatusCode::NO_CONTENT,
            HeaderMap::new(),
            bytes::Bytes::new(),
            1,
            crate::api::http::http_response::HttpRequestId(1),
        );
        Box::new(EmptyRequest)
            .deal_with_response(Ok(response))
            .await;
        Ok(())
    }
}

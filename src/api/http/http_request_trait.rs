#[cfg(feature = "http-client-cookies")]
use crate::api::http::cookies::CookieRequestContext;
use crate::api::http::http_request_method::HttpRequestMethod;
use crate::api::http::http_response::HttpResponseResult;
use crate::api::http::retry::HttpRetryPolicy;
use crate::api::retry::RetrySetting;
use async_trait::async_trait;
use bytes::Bytes;
use http::HeaderMap;

/// Request object consumed by an HTTP client.
///
/// Implementations own callback state and are moved to the callback lane after the final
/// attempt. `get_req_data` must return the same body for every attempt when retries are enabled.
///
/// Requests may own application context as fields or capture it in their callbacks.
/// The SDK only requires `Send + 'static`; request implementations may own
/// `FnOnce + Send` callbacks that are not `Sync`, `Clone`, or `Debug`.
#[async_trait]
pub trait HttpRequestTrait: Send + 'static {
    /// Return the request target relative to the client's configured base URL.
    ///
    /// The worker reads this value for each attempt. Absolute or cross-origin
    /// targets fail validation after admission and are delivered through the
    /// terminal callback; they are not returned as an admission rejection.
    fn get_path(&self) -> String;

    /// Return the HTTP method for this request.
    ///
    /// Invalid extension tokens fail validation after admission, before transport
    /// starts. The terminal callback receives `InvalidInput` at `RequestBuild`.
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

    /// Return SameSite context for this logical request.
    #[cfg(feature = "http-client-cookies")]
    fn cookie_context(&self) -> CookieRequestContext {
        CookieRequestContext::default()
    }

    /// Select the client policy, explicitly disable retries, or provide a complete override.
    fn retry_setting(&self) -> RetrySetting<HttpRetryPolicy> {
        RetrySetting::Inherit
    }

    /// Observe response headers on the callback lane.  The default implementation is a no-op.
    /// The callback may inspect the headers but cannot mutate worker state or
    /// replace the eventual response. Only fully buffered `Ok` responses invoke
    /// this hook. Body failures expose received headers through
    /// [`crate::api::error::NetError::http_response_head`] in the terminal callback.
    fn on_response_headers(&self, _headers: &HeaderMap) {}

    /// Consume this request with the final result.
    ///
    /// Once submission succeeds, the callback is invoked exactly once after the
    /// final attempt, whether the operation succeeded, exhausted its retry budget, was cancelled, or
    /// failed before a response was available. It runs on the callback lane and
    /// receives ownership of the request object and response/error result.
    /// Rejected submissions return the original request in `EnqueueError` and
    /// do not invoke this callback. Abnormal worker termination and callback
    /// panics are reported by the request handle's completion state.
    async fn deal_with_response(self: Box<Self>, result: HttpResponseResult);
}

// Forward all providers, including byte bodies and feature-gated metadata. The
// consuming callback removes exactly one box layer, so nested boxes work too.
#[async_trait]
impl<T> HttpRequestTrait for Box<T>
where
    T: HttpRequestTrait + ?Sized,
{
    fn get_path(&self) -> String {
        (**self).get_path()
    }
    fn get_method(&self) -> HttpRequestMethod {
        (**self).get_method()
    }
    fn get_req_data(&self) -> String {
        (**self).get_req_data()
    }
    fn get_req_body(&self) -> Bytes {
        (**self).get_req_body()
    }
    fn has_req_body(&self) -> bool {
        (**self).has_req_body()
    }
    fn headers(&self) -> HeaderMap {
        (**self).headers()
    }
    #[cfg(feature = "http-client-cookies")]
    fn cookie_context(&self) -> CookieRequestContext {
        (**self).cookie_context()
    }
    fn retry_setting(&self) -> RetrySetting<HttpRetryPolicy> {
        (**self).retry_setting()
    }
    fn on_response_headers(&self, headers: &HeaderMap) {
        (**self).on_response_headers(headers);
    }
    async fn deal_with_response(self: Box<Self>, result: HttpResponseResult) {
        (*self).deal_with_response(result).await;
    }
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
            matches!(request.retry_setting(), RetrySetting::Inherit),
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

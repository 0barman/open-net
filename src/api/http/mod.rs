//! Optional HTTP client contracts and value types.
//!
//! The module is available only with the `http-client` feature. It exposes
//! bounded client configuration, request builders, retry policy, buffered and
//! streaming responses, and the callback trait used by custom request types.
//! Network worker implementation details remain private to the crate.

/// HTTP client handle and request execution API.
pub mod http_client;
pub mod http_config;
/// HTTP request values and builders.
pub mod http_request;
pub mod http_request_method;
/// Trait implemented by custom HTTP request types.
pub mod http_request_trait;
pub mod http_response;
/// HTTP status policy configuration.
pub mod http_status;
/// Streaming HTTP response values.
pub mod http_stream;
pub mod retry;

pub use http_client::HttpClient;
pub use http_config::{HttpClientConfig, HttpProxyConfig, HttpRootCertificateMode, HttpTlsConfig};
pub use http_request::{HttpRequest, HttpRequestBuilder};
pub use http_request_method::HttpRequestMethod;
pub use http_request_trait::HttpRequestTrait;
pub use http_response::{HttpRequestId, HttpResponse, HttpResponseResult};
pub use http_status::HttpStatusPolicy;
pub use http_stream::{HttpByteStream, HttpStreamResponse};
pub use retry::{
    ApplicationPredicate, Backoff, HttpRequestOptions, RequestSafety, RetryAfterMode, RetryContext,
    RetryEvent, RetryObserver, RetryObserverHandle, RetryOn, RetryPolicy, RetryPolicyBuilder,
    RetryPolicyError, RetryReason, RetryReport, RetrySetting, TransportErrorKind,
};

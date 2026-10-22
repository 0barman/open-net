//! Optional HTTP client contracts and value types.
//!
//! The module is available only with the `http-client` feature. It exposes
//! bounded client configuration, request builders, retry policy, buffered and
//! streaming responses, and the callback trait used by custom request types.
//! Network worker implementation details remain private to the crate.

/// Optional asynchronous HTTP authentication providers.
#[cfg(feature = "http-client-auth")]
pub mod auth;
/// Optional SDK-owned Cookie stores and bounded persistence snapshots.
#[cfg(feature = "http-client-cookies")]
pub mod cookies;
#[cfg(feature = "http-client-http2")]
pub mod http2;
/// Capacity waiting and immediate rejection policies for response Futures.
pub mod http_admission;
/// HTTP client handle and request execution API.
pub mod http_client;
pub mod http_config;
/// HTTP request values and builders.
pub mod http_request;
/// Accepted request cancellation and callback completion handles.
pub mod http_request_handle;
pub mod http_request_method;
/// Trait implemented by custom HTTP request types.
pub mod http_request_trait;
pub mod http_response;
/// HTTP status policy configuration.
pub mod http_status;
/// Streaming HTTP response values.
pub mod http_stream;
pub mod http_timeouts;
/// Lazy fixed-configuration clients with SDK-owned initialization and shutdown.
pub mod managed_http_client;
/// Composable middleware for concrete HTTP request and stream APIs.
#[cfg(feature = "http-client-middleware")]
pub mod middleware;
/// Buffered multipart request construction (requires `http-client-codecs`).
#[cfg(feature = "http-client-codecs")]
pub mod multipart;
/// Redirect target authorization and transport security requirements.
pub mod redirect;
#[cfg(feature = "http-client-codecs")]
mod request_codecs;
pub mod retry;

#[cfg(feature = "http-client-auth")]
pub use auth::{
    AccessToken, AuthAction, AuthRequestContext, AuthResponseContext, AuthResult, BasicAuth,
    BearerAuth, BearerTokenSource, HttpAuth, RefreshReason,
};
#[cfg(feature = "http-client-cookies")]
pub use cookies::{
    CookieJar, CookieJarSnapshot, CookieLimits, CookieRequestContext, CookieStore,
    CookieStoreResult,
};
pub use http::{HeaderMap, HeaderValue, Method};
#[cfg(feature = "http-client-http2")]
pub use http2::{
    Http2Config, Http2FlowControlSnapshot, HttpProtocol, HttpResponseInfo, HttpVersionPolicy,
};
pub use http_admission::HttpAdmissionPolicy;
pub use http_client::HttpClient;
pub use http_config::{HttpClientConfig, HttpProxyConfig, HttpRootCertificateMode, HttpTlsConfig};
pub use http_request::{HttpRequest, HttpRequestBuilder};
pub use http_request_handle::{
    HttpCancelOutcome, HttpCompletion, HttpRequestHandle, HttpResponseSummary,
};
pub use http_request_method::HttpRequestMethod;
pub use http_request_trait::HttpRequestTrait;
pub use http_response::{HttpRequestId, HttpResponse, HttpResponseHead, HttpResponseResult};
pub use http_status::HttpStatusPolicy;
pub use http_stream::{HttpByteStream, HttpStreamResponse};
pub use http_timeouts::{HttpTimeouts, TimeoutPhase};
pub use managed_http_client::ManagedHttpClient;
#[cfg(feature = "http-client-middleware")]
pub use middleware::{
    HttpCall, HttpCallMetadata, HttpMiddleware, HttpMiddlewareResponse, HttpResponseMode, Next,
};
#[cfg(feature = "http-client-codecs")]
pub use multipart::Multipart;
pub use redirect::{HttpRedirectError, HttpTransportSecurity, RedirectPolicy};
pub use retry::{
    HttpRequestOptions, HttpRetryContext, HttpRetryPolicy, HttpRetryPolicyBuilder, RequestSafety,
    RetryAfterMode, RetryOn, RetryReason, TransportErrorKind,
};
#[cfg(feature = "http-client-cookies")]
pub use url::Url;

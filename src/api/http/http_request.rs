use crate::api::error::NetError;
use crate::api::http::http_request_trait::HttpRequestTrait;
use crate::api::http::http_response::HttpResponseResult;
use crate::api::http::retry::RetryPolicy;
use async_trait::async_trait;
use bytes::Bytes;
use http::{HeaderMap, HeaderName, HeaderValue, Method};

#[derive(Clone, Debug)]
/// A concrete HTTP request accepted by [`crate::api::http::HttpClient`].
///
/// Requests own their method, relative path, headers, body, and retry policy.
/// Use [`HttpRequest::builder`] to validate the path and construct one without
/// relying on unchecked field combinations.
pub struct HttpRequest {
    /// HTTP method used for the request.
    pub method: Method,
    /// Relative path resolved against [`crate::api::http::HttpClientConfig::base_url`].
    pub path: String,
    /// Request headers merged over the client's common headers.
    pub headers: HeaderMap,
    /// Owned request body bytes.
    pub body: Bytes,
    /// Retry and replay rules for this request.
    pub retry_policy: RetryPolicy,
}

/// Fluent builder for a validated [`HttpRequest`].
///
/// The builder starts with `GET`, an empty relative path, no headers, an empty
/// body, and [`RetryPolicy::no_retry`]. `build` rejects an empty path; the
/// client performs same-origin validation when the request is submitted.
pub struct HttpRequestBuilder {
    method: Method,
    path: String,
    headers: HeaderMap,
    body: Bytes,
    retry_policy: RetryPolicy,
}

impl HttpRequest {
    /// Start constructing a request with safe defaults.
    pub fn builder() -> HttpRequestBuilder {
        HttpRequestBuilder {
            method: Method::GET,
            path: String::new(),
            headers: HeaderMap::new(),
            body: Bytes::new(),
            retry_policy: RetryPolicy::no_retry(),
        }
    }
}

impl HttpRequestBuilder {
    /// Set the HTTP method.
    pub fn method(mut self, method: Method) -> Self {
        self.method = method;
        self
    }
    /// Set the relative request path.
    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.path = path.into();
        self
    }
    /// Replace all request-specific headers.
    pub fn headers(mut self, headers: HeaderMap) -> Self {
        self.headers = headers;
        self
    }
    /// Set the owned request body.
    pub fn body(mut self, body: Bytes) -> Self {
        self.body = body;
        self
    }
    /// Set the retry policy carried by the request.
    pub fn retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = policy;
        self
    }

    /// Insert or replace one request header.
    ///
    /// Header names and values are validated by the `http` crate. This method
    /// returns an input error without consuming the partially built request
    /// when conversion fails.
    pub fn header(
        mut self,
        name: impl TryInto<HeaderName>,
        value: impl TryInto<HeaderValue>,
    ) -> Result<Self, NetError> {
        let name = name
            .try_into()
            .map_err(|_| NetError::input("header", "invalid name"))?;
        let value = value
            .try_into()
            .map_err(|_| NetError::input("header", "invalid value"))?;
        self.headers.insert(name, value);
        Ok(self)
    }

    /// Validate the path and produce an owned request value.
    ///
    /// Only the non-empty-path invariant is checked here; the client later
    /// rejects absolute, cross-origin, or otherwise unsafe paths.
    pub fn build(self) -> Result<HttpRequest, NetError> {
        if self.path.is_empty() {
            return Err(NetError::input("path", "path must not be empty"));
        }
        Ok(HttpRequest {
            method: self.method,
            path: self.path,
            headers: self.headers,
            body: self.body,
            retry_policy: self.retry_policy,
        })
    }
}

#[async_trait]
impl HttpRequestTrait for HttpRequest {
    fn get_path(&self) -> String {
        self.path.clone()
    }

    fn get_method(&self) -> Method {
        self.method.clone()
    }

    fn get_req_body(&self) -> Bytes {
        self.body.clone()
    }

    fn headers(&self) -> HeaderMap {
        self.headers.clone()
    }

    fn retry_policy(&self) -> RetryPolicy {
        self.retry_policy.clone()
    }

    async fn deal_with_response(self: Box<Self>, _result: HttpResponseResult) {}
}

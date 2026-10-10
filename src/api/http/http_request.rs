use crate::api::error::NetError;
#[cfg(feature = "http-client-cookies")]
use crate::api::http::cookies::CookieRequestContext;
#[cfg(feature = "http-client-codecs")]
use crate::api::http::multipart::Multipart;
#[cfg(feature = "http-client-codecs")]
use crate::api::http::request_codecs::{
    append_query, encode_form, encode_json, encode_query, ensure_body_slot,
    set_default_content_type, validate_multipart_header, BodyEncoding,
};
use crate::api::http::retry::{RetryPolicy, RetrySetting};
use crate::api::http::{HttpAdmissionPolicy, HttpTimeouts};
use bytes::Bytes;
use http::{HeaderMap, HeaderName, HeaderValue, Method};

#[derive(Clone)]
#[cfg_attr(not(feature = "http-client-cookies"), derive(Debug))]
/// A concrete HTTP request accepted by [`crate::api::http::HttpClient`].
///
/// Requests own their method, relative path, headers, body, and retry policy.
/// Use [`HttpRequest::builder`] to validate the path and construct one without
/// relying on unchecked field combinations.
///
/// Concrete requests return their responses through the Future API:
///
/// ```no_run
/// use open_net::api::error::NetError;
/// use open_net::api::http::{HttpClient, HttpRequest};
///
/// async fn fetch(client: &HttpClient) -> Result<(), NetError> {
///     let request = HttpRequest::builder().path("/status").build()?;
///     let response = client.request(request).await?;
///     let _ = response;
///     Ok(())
/// }
/// ```
///
/// A concrete request intentionally does not implement
/// [`crate::api::http::HttpRequestTrait`]. Callback entry points require a custom
/// request type that owns its response handler:
///
/// ```compile_fail
/// use open_net::api::error::NetError;
/// use open_net::api::http::{HttpClient, HttpRequest};
///
/// fn submit(client: &HttpClient) -> Result<(), NetError> {
///     let request = HttpRequest::builder().path("/status").build()?;
///     let _ = client.send(request);
///     Ok(())
/// }
/// ```
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
    /// SameSite context used when the configured Cookie store is queried.
    #[cfg(feature = "http-client-cookies")]
    pub(crate) cookie_context: CookieRequestContext,
    /// Whether this request inherits the client policy or carries an explicit override.
    pub(crate) retry_setting: RetrySetting,
    pub(crate) timeouts: HttpTimeouts,
    admission_policy: Option<HttpAdmissionPolicy>,
}

#[cfg(feature = "http-client-cookies")]
impl std::fmt::Debug for HttpRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HttpRequest")
            .field("method", &self.method)
            .field("path_bytes", &self.path.len())
            .field("header_count", &self.headers.len())
            .field("body_bytes", &self.body.len())
            .field("retry_policy", &self.retry_policy)
            .field("timeouts", &self.timeouts)
            .field("admission_policy", &self.admission_policy)
            .finish_non_exhaustive()
    }
}

/// Fluent builder for a validated [`HttpRequest`].
///
/// The builder starts with `GET`, an empty relative path, no headers, an empty
/// body, and inherited retry and admission settings. `build` rejects an empty path; the
/// client performs same-origin validation when the request is submitted.
pub struct HttpRequestBuilder {
    method: Method,
    path: String,
    headers: HeaderMap,
    body: Bytes,
    retry_policy: RetryPolicy,
    retry_setting: RetrySetting,
    timeouts: HttpTimeouts,
    admission_policy: Option<HttpAdmissionPolicy>,
    #[cfg(feature = "http-client-cookies")]
    cookie_context: CookieRequestContext,
    #[cfg(feature = "http-client-codecs")]
    body_configured: bool,
    #[cfg(feature = "http-client-codecs")]
    body_encoding: Option<BodyEncoding>,
    #[cfg(feature = "http-client-codecs")]
    body_conflict: bool,
    #[cfg(feature = "http-client-codecs")]
    multipart_content_type: Option<HeaderValue>,
    #[cfg(feature = "http-client-codecs")]
    query_parts: Vec<String>,
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
            retry_setting: RetrySetting::Inherit,
            timeouts: HttpTimeouts::new(),
            admission_policy: None,
            #[cfg(feature = "http-client-cookies")]
            cookie_context: CookieRequestContext::default(),
            #[cfg(feature = "http-client-codecs")]
            body_configured: false,
            #[cfg(feature = "http-client-codecs")]
            body_encoding: None,
            #[cfg(feature = "http-client-codecs")]
            body_conflict: false,
            #[cfg(feature = "http-client-codecs")]
            multipart_content_type: None,
            #[cfg(feature = "http-client-codecs")]
            query_parts: Vec::new(),
        }
    }

    /// Return the explicit overrides installed when this request was built.
    pub fn timeouts(&self) -> &HttpTimeouts {
        &self.timeouts
    }

    /// Return this request's capacity policy override, or `None` to inherit the client.
    pub fn admission_policy_override(&self) -> Option<HttpAdmissionPolicy> {
        self.admission_policy
    }

    pub(crate) fn retry_setting(&self) -> RetrySetting {
        match &self.retry_setting {
            RetrySetting::Inherit if self.retry_policy == RetryPolicy::no_retry() => {
                RetrySetting::Inherit
            }
            RetrySetting::Inherit | RetrySetting::Policy(_) => {
                RetrySetting::Policy(self.retry_policy.clone())
            }
            RetrySetting::None => RetrySetting::None,
        }
    }
}

impl HttpRequestBuilder {
    /// Override the client's capacity policy for this response-returning request.
    ///
    /// The policy and timeout budgets are resolved when the response Future is
    /// first polled, before middleware runs. Middleware cannot alter the admission
    /// policy of an operation that has already been accepted.
    pub fn admission_policy(mut self, policy: HttpAdmissionPolicy) -> Self {
        self.admission_policy = Some(policy);
        self
    }

    /// Replace all timeout overrides for this request; `build` validates durations.
    pub fn timeouts(mut self, timeouts: HttpTimeouts) -> Self {
        self.timeouts = timeouts;
        self
    }

    /// Set the HTTP method.
    pub fn method(mut self, method: impl Into<Method>) -> Self {
        self.method = method.into();
        self
    }
    /// Set the relative request path.
    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.path = path.into();
        #[cfg(feature = "http-client-codecs")]
        for query in &self.query_parts {
            append_query(&mut self.path, query);
        }
        self
    }
    /// Replace all request-specific headers.
    pub fn headers(mut self, headers: HeaderMap) -> Self {
        self.headers = headers;
        self
    }
    /// Set the owned request body.
    pub fn body(mut self, body: Bytes) -> Self {
        #[cfg(feature = "http-client-codecs")]
        {
            if self
                .body_encoding
                .is_some_and(|encoding| encoding != BodyEncoding::Raw)
            {
                self.body_conflict = true;
            }
            self.body_encoding = Some(BodyEncoding::Raw);
            self.body_configured = true;
            self.multipart_content_type = None;
        }
        self.body = body;
        self
    }

    #[cfg(feature = "http-client-codecs")]
    /// Append a serde value to the request target query string.
    pub fn query<T: serde::Serialize>(mut self, value: &T) -> Result<Self, NetError> {
        let encoded = encode_query(value)?;
        append_query(&mut self.path, &encoded);
        if !encoded.is_empty() {
            self.query_parts.push(encoded);
        }
        Ok(self)
    }

    #[cfg(feature = "http-client-codecs")]
    /// Serialize a value as a JSON request body.
    pub fn json<T: serde::Serialize>(mut self, value: &T) -> Result<Self, NetError> {
        ensure_body_slot(self.body_encoding, BodyEncoding::Json)?;
        if self.body_configured {
            return Err(crate::api::error::NetError::input(
                "body",
                "a request body is already configured",
            ));
        }
        self.body = Bytes::from(encode_json(value)?);
        set_default_content_type(&mut self.headers, "application/json")?;
        self.body_configured = true;
        self.body_encoding = Some(BodyEncoding::Json);
        Ok(self)
    }

    #[cfg(feature = "http-client-codecs")]
    /// Serialize a value as an `application/x-www-form-urlencoded` body.
    pub fn form<T: serde::Serialize>(mut self, value: &T) -> Result<Self, NetError> {
        ensure_body_slot(self.body_encoding, BodyEncoding::Form)?;
        if self.body_configured {
            return Err(crate::api::error::NetError::input(
                "body",
                "a request body is already configured",
            ));
        }
        self.body = Bytes::from(encode_form(value)?);
        set_default_content_type(&mut self.headers, "application/x-www-form-urlencoded")?;
        self.body_configured = true;
        self.body_encoding = Some(BodyEncoding::Form);
        Ok(self)
    }

    #[cfg(feature = "http-client-codecs")]
    /// Encode a buffered multipart form body with an SDK-owned boundary.
    pub fn multipart(mut self, value: Multipart) -> Result<Self, NetError> {
        ensure_body_slot(self.body_encoding, BodyEncoding::Multipart)?;
        if self.body_configured || self.headers.contains_key(http::header::CONTENT_TYPE) {
            return Err(crate::api::error::NetError::input(
                "body",
                "multipart requires an unused body and SDK-owned content-type",
            ));
        }
        let (body, content_type) = value.encode()?;
        validate_multipart_header(&self.headers, &content_type)?;
        self.body = body;
        self.headers
            .insert(http::header::CONTENT_TYPE, content_type.clone());
        self.body_configured = true;
        self.body_encoding = Some(BodyEncoding::Multipart);
        self.multipart_content_type = Some(content_type);
        Ok(self)
    }
    /// Set the retry policy carried by the request.
    pub fn retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = policy;
        self.retry_setting = RetrySetting::Policy(self.retry_policy.clone());
        self
    }

    /// Select whether the request inherits, disables, or replaces the client policy.
    pub fn retry_setting(mut self, setting: RetrySetting) -> Self {
        self.retry_setting = setting.clone();
        self.retry_policy = match setting {
            RetrySetting::Policy(policy) => policy,
            RetrySetting::Inherit | RetrySetting::None => RetryPolicy::no_retry(),
        };
        self
    }

    /// Set the SameSite context for this logical request.
    #[cfg(feature = "http-client-cookies")]
    pub fn cookie_context(mut self, context: CookieRequestContext) -> Self {
        self.cookie_context = context;
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
        #[cfg(feature = "http-client-codecs")]
        if name == http::header::CONTENT_TYPE {
            if let Some(expected) = &self.multipart_content_type {
                if &value != expected {
                    return Err(NetError::input(
                        "content-type",
                        "multipart content-type is managed by the SDK",
                    ));
                }
            }
        }
        self.headers.insert(name, value);
        Ok(self)
    }

    /// Validate the path and produce an owned request value.
    ///
    /// Timeout durations and the nonempty path are checked here; the client later
    /// rejects absolute, cross-origin, or otherwise unsafe paths.
    pub fn build(self) -> Result<HttpRequest, NetError> {
        self.timeouts.validate_request()?;
        if self.path.is_empty() {
            return Err(NetError::input("path", "path must not be empty"));
        }
        #[cfg(feature = "http-client-codecs")]
        {
            if self.body_conflict {
                return Err(NetError::input(
                    "body",
                    "raw body and a typed body codec cannot be combined",
                ));
            }
            if let Some(expected) = &self.multipart_content_type {
                if self.headers.get(http::header::CONTENT_TYPE) != Some(expected) {
                    return Err(NetError::input(
                        "content-type",
                        "multipart content-type is managed by the SDK",
                    ));
                }
            }
        }
        Ok(HttpRequest {
            method: self.method,
            path: self.path,
            headers: self.headers,
            body: self.body,
            retry_policy: self.retry_policy,
            retry_setting: self.retry_setting,
            timeouts: self.timeouts,
            admission_policy: self.admission_policy,
            #[cfg(feature = "http-client-cookies")]
            cookie_context: self.cookie_context,
        })
    }
}

#[cfg(test)]
mod admission_tests {
    use super::*;
    use crate::api::http::HttpAdmissionPolicy;

    type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

    fn check(condition: bool, message: &str) -> TestResult {
        if condition {
            Ok(())
        } else {
            Err(std::io::Error::other(message).into())
        }
    }

    #[test]
    fn request_admission_defaults_to_client_inheritance() -> TestResult {
        let request = HttpRequest::builder().path("/").build()?;
        check(
            request.admission_policy_override().is_none(),
            "new requests must inherit their client's admission policy",
        )
    }

    #[test]
    fn request_admission_override_survives_build_clone_and_debug() -> TestResult {
        for policy in [HttpAdmissionPolicy::Wait, HttpAdmissionPolicy::FailFast] {
            let request = HttpRequest::builder()
                .path("/")
                .admission_policy(policy)
                .build()?;
            let cloned = request.clone();
            check(
                request.admission_policy_override() == Some(policy)
                    && cloned.admission_policy_override() == Some(policy),
                "build and clone must retain explicit admission policy overrides",
            )?;
            let debug = format!("{cloned:?}");
            check(
                debug.contains("admission_policy") && debug.contains(&format!("{policy:?}")),
                "request debug must include its non-sensitive admission configuration",
            )?;
        }
        Ok(())
    }

    #[test]
    fn last_request_admission_override_wins() -> TestResult {
        let request = HttpRequest::builder()
            .path("/")
            .admission_policy(HttpAdmissionPolicy::FailFast)
            .admission_policy(HttpAdmissionPolicy::Wait)
            .build()?;
        check(
            request.admission_policy_override() == Some(HttpAdmissionPolicy::Wait),
            "the last explicit request admission policy must win",
        )
    }
}

#[cfg(all(test, feature = "http-client-codecs"))]
mod codec_builder_tests {
    use super::*;
    use bytes::Bytes;
    use serde::Serialize;

    #[derive(Serialize)]
    struct QueryPart {
        key: &'static str,
    }

    #[derive(Serialize)]
    struct FormValue {
        value: &'static str,
    }

    #[test]
    fn repeated_query_values_survive_path_reset() {
        let request = HttpRequest::builder()
            .path("/first?fixed=1")
            .query(&QueryPart { key: "one" })
            .unwrap()
            .query(&QueryPart { key: "two" })
            .unwrap()
            .path("/second?reset=1")
            .build()
            .unwrap();

        assert_eq!(
            request.path, "/second?reset=1&key=one&key=two",
            "path() must preserve previously appended query parts"
        );
    }

    #[test]
    fn form_keeps_a_caller_supplied_content_type() {
        let request = HttpRequest::builder()
            .path("/form")
            .header("content-type", "application/vnd.example.form")
            .unwrap()
            .form(&FormValue { value: "demo" })
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(
            request.headers.get(http::header::CONTENT_TYPE).unwrap(),
            "application/vnd.example.form"
        );
    }

    #[test]
    fn reverse_raw_and_typed_body_combinations_are_rejected_at_build() {
        let json_then_raw = HttpRequest::builder()
            .path("/body")
            .json(&serde_json::json!({"ok": true}))
            .unwrap()
            .body(Bytes::from_static(b"raw"))
            .build();
        assert!(json_then_raw.is_err());

        let raw_then_json = HttpRequest::builder()
            .path("/body")
            .body(Bytes::from_static(b"raw"))
            .json(&serde_json::json!({"ok": true}));
        assert!(raw_then_json.is_err());

        let multipart_then_header = HttpRequest::builder()
            .path("/multipart")
            .multipart(
                crate::api::http::Multipart::new()
                    .text("field", "value")
                    .unwrap(),
            )
            .unwrap()
            .header("content-type", "multipart/form-data; boundary=user")
            .and_then(|builder| builder.build());
        assert!(multipart_then_header.is_err());
    }
}

//! Configuration contracts for the optional HTTP client.
//!
//! This module deliberately contains validation and immutable request defaults only.  Runtime
//! creation and transport setup live in the HTTP worker module.

use crate::api::error::{ErrorStage, NetError};
use crate::api::http::retry::RetryPolicy;
use http::{HeaderMap, HeaderName};
use rustls::pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::sign::CertifiedKey;
use std::fmt;
use std::time::Duration;

const MAX_TLS_PEM_BYTES: usize = 1024 * 1024;
const MAX_TLS_CERTIFICATES: usize = 128;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Controls whether the HTTP client's built-in WebPKI roots remain trusted
/// when custom PEM roots are configured.
pub enum HttpRootCertificateMode {
    /// Add custom roots to the built-in WebPKI root set.
    Append,
    /// Disable the built-in root set and trust only the configured custom roots.
    Replace,
}

/// TLS settings used by the HTTP client.  Certificate material is retained only in owned,
/// bounded buffers so a reqwest client can build its rustls connector once during startup.
#[derive(Clone)]
pub struct HttpTlsConfig {
    root_certificates: Vec<Vec<u8>>,
    root_mode: HttpRootCertificateMode,
    client_identity: Option<Vec<u8>>,
}

impl Default for HttpTlsConfig {
    fn default() -> Self {
        Self {
            root_certificates: Vec::new(),
            root_mode: HttpRootCertificateMode::Append,
            client_identity: None,
        }
    }
}

impl HttpTlsConfig {
    /// Configure one PEM bundle as the custom trust-root set.
    ///
    /// The bundle must contain one or more valid `CERTIFICATE` PEM blocks and
    /// remain within the library's bounded certificate limits. When
    /// `replace_builtin_roots` is `false`, the roots are appended to reqwest's
    /// built-in WebPKI roots; when it is `true`, only this custom set is used.
    pub fn with_root_certificates(
        mut self,
        pem: impl AsRef<[u8]>,
        replace_builtin_roots: bool,
    ) -> Result<Self, NetError> {
        let bytes = pem.as_ref();
        validate_root_certificates(bytes)?;
        self.root_certificates = vec![bytes.to_vec()];
        self.root_mode = if replace_builtin_roots {
            HttpRootCertificateMode::Replace
        } else {
            HttpRootCertificateMode::Append
        };
        Ok(self)
    }

    /// Configure custom roots with an explicit append-or-replace mode.
    pub fn with_root_certificates_mode(
        self,
        pem: impl AsRef<[u8]>,
        mode: HttpRootCertificateMode,
    ) -> Result<Self, NetError> {
        let replace = mode == HttpRootCertificateMode::Replace;
        self.with_root_certificates(pem, replace)
    }

    /// Add a PEM root bundle to the existing custom trust-root set while
    /// retaining roots already configured.
    ///
    /// Existing roots are retained. This method validates the new bundle before
    /// storing an owned copy and fails if the bounded certificate limit is
    /// exceeded.
    pub fn with_additional_root_certificate(
        mut self,
        pem: impl AsRef<[u8]>,
    ) -> Result<Self, NetError> {
        if self.root_certificates.len() >= MAX_TLS_CERTIFICATES {
            return Err(invalid_config("tls.root_certificates"));
        }
        let bytes = pem.as_ref();
        validate_root_certificates(bytes)?;
        self.root_certificates.push(bytes.to_vec());
        Ok(self)
    }

    /// Configure a client certificate chain and PKCS#8 private key from PEM.
    ///
    /// The certificate chain and key are parsed and checked for a matching key
    /// before they are retained. Private material is never exposed by the
    /// public API or by the type's `Debug` implementation.
    pub fn with_client_identity_pem(
        mut self,
        certificates: impl AsRef<[u8]>,
        private_key: impl AsRef<[u8]>,
    ) -> Result<Self, NetError> {
        let certificates = certificates.as_ref();
        let private_key = private_key.as_ref();
        if certificates.is_empty()
            || private_key.is_empty()
            || certificates.len() > MAX_TLS_PEM_BYTES
            || private_key.len() > MAX_TLS_PEM_BYTES
        {
            return Err(invalid_config("tls.client_identity"));
        }
        let chain = parse_certificates(certificates, "tls.client_identity.certificates")?;
        let keys = strict_pem_blocks(
            private_key,
            "PRIVATE KEY",
            "tls.client_identity.private_key",
        )?;
        if keys.len() != 1 {
            return Err(invalid_config("tls.client_identity.private_key"));
        }
        let key = PrivatePkcs8KeyDer::from_pem_slice(keys[0])
            .map_err(|_| invalid_config("tls.client_identity.private_key"))?;
        let certified = CertifiedKey::from_der(
            chain,
            PrivateKeyDer::Pkcs8(key),
            &rustls::crypto::aws_lc_rs::default_provider(),
        )
        .map_err(|_| invalid_config("tls.client_identity"))?;
        certified
            .keys_match()
            .map_err(|_| invalid_config("tls.client_identity"))?;
        let mut combined = Vec::with_capacity(certificates.len() + private_key.len());
        combined.extend_from_slice(certificates);
        if !certificates.ends_with(b"\n") {
            combined.push(b'\n');
        }
        combined.extend_from_slice(private_key);
        let identity = reqwest::Identity::from_pem(&combined)
            .map_err(|_| invalid_config("tls.client_identity"))?;
        let _ = identity;
        self.client_identity = Some(combined);
        Ok(self)
    }

    pub(crate) fn configure_builder(
        &self,
        mut builder: reqwest::ClientBuilder,
    ) -> Result<reqwest::ClientBuilder, NetError> {
        if self.root_mode == HttpRootCertificateMode::Replace {
            builder = builder.tls_built_in_root_certs(false);
        }
        for pem in &self.root_certificates {
            let certificate = reqwest::Certificate::from_pem(pem)
                .map_err(|_| invalid_config("tls.root_certificates"))?;
            builder = builder.add_root_certificate(certificate);
        }
        if let Some(identity) = &self.client_identity {
            let identity = reqwest::Identity::from_pem(identity)
                .map_err(|_| invalid_config("tls.client_identity"))?;
            builder = builder.identity(identity);
        }
        Ok(builder)
    }

    /// Return the number of custom trust roots currently configured.
    pub fn custom_root_count(&self) -> usize {
        self.root_certificates.len()
    }

    /// Report whether built-in WebPKI roots will be disabled at client creation.
    pub fn replaces_builtin_roots(&self) -> bool {
        self.root_mode == HttpRootCertificateMode::Replace
    }
}

#[derive(Clone)]
/// HTTP CONNECT proxy settings.
///
/// Proxy URLs contain no embedded credentials. Use [`Self::with_basic_auth`]
/// to attach bounded credentials after parsing the endpoint.
pub struct HttpProxyConfig {
    url: String,
    username: Option<String>,
    password: Option<String>,
}

impl fmt::Debug for HttpProxyConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpProxyConfig")
            .field("configured", &true)
            .field("credentials_configured", &self.username.is_some())
            .finish()
    }
}

impl HttpProxyConfig {
    /// Parse an unauthenticated HTTP proxy endpoint.
    ///
    /// Only an `http` URL with an authority and no userinfo, path, query, or
    /// fragment is accepted. SOCKS endpoints and environment proxy settings
    /// are configured through separate mechanisms.
    pub fn http(url: impl AsRef<str>) -> Result<Self, NetError> {
        let parsed = reqwest::Url::parse(url.as_ref()).map_err(|_| invalid_config("proxy.url"))?;
        if parsed.scheme() != "http"
            || parsed.host_str().is_none()
            || parsed.port_or_known_default().is_none()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.path() != "/"
            || parsed.query().is_some()
            || parsed.fragment().is_some()
        {
            return Err(invalid_config("proxy.url"));
        }
        Ok(Self {
            url: parsed.to_string(),
            username: None,
            password: None,
        })
    }

    /// Attach HTTP Basic authentication to this proxy.
    ///
    /// Credentials are stored for the worker's private use and are omitted from
    /// `Debug` output. Empty usernames and values beyond the configured bounds
    /// are rejected.
    pub fn with_basic_auth(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Result<Self, NetError> {
        let username = username.into();
        let password = password.into();
        if username.is_empty() || username.len() > 1024 || password.len() > 4096 {
            return Err(invalid_config("proxy.credentials"));
        }
        self.username = Some(username);
        self.password = Some(password);
        Ok(self)
    }

    pub(crate) fn url(&self) -> &str {
        &self.url
    }

    pub(crate) fn credentials(&self) -> Option<(&str, &str)> {
        match (&self.username, &self.password) {
            (Some(username), Some(password)) => Some((username.as_str(), password.as_str())),
            _ => None,
        }
    }
}

impl fmt::Debug for HttpTlsConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpTlsConfig")
            .field("custom_root_count", &self.root_certificates.len())
            .field("root_mode", &self.root_mode)
            .field(
                "client_identity_configured",
                &self.client_identity.is_some(),
            )
            .finish()
    }
}

/// Configuration used when an HTTP client is created.
///
/// The fields are public so an integration can inspect an already validated snapshot.  Use
/// [`HttpClientConfig::new`] and the `with_*` methods to construct values from untrusted input.
#[derive(Clone)]
pub struct HttpClientConfig {
    /// Base URL used to resolve request paths.
    pub base_url: reqwest::Url,
    /// Headers copied into every request before request-specific headers are merged.
    pub common_headers: HeaderMap,
    /// Timeout for each network attempt, including response headers and body.
    ///
    /// This starts when reqwest executes the attempt, excludes SDK admission
    /// waiting, and resets for a retry. Paused stream consumption still uses time.
    pub timeout: Duration,
    /// Optional TCP keepalive interval applied to pooled connections.
    pub tcp_keepalive: Option<Duration>,
    /// Maximum response body size accepted by the receive lane.
    pub max_response_bytes: usize,
    /// Capacity of the admission queue and in-flight operation quota.
    ///
    /// One permit is held from admission until the final callback completes, or
    /// a stream observes its terminal item/EOF or is dropped. Retries retain
    /// capacity between attempts; an unpolled retained stream does not release it.
    pub request_queue_capacity: usize,
    /// Capacity of the response queue owned by the receive lane.
    pub response_queue_capacity: usize,
    /// Capacity of the callback queue owned by the callback lane.
    pub callback_queue_capacity: usize,
    /// Policy applied when a request does not provide its own policy.
    pub default_retry_policy: RetryPolicy,
    /// TLS trust and client identity settings.
    pub tls: HttpTlsConfig,
    /// Optional explicit HTTP CONNECT proxy configuration.
    pub proxy: Option<HttpProxyConfig>,
    /// Whether reqwest may read HTTP(S) proxy settings from the environment.
    pub environment_proxy: bool,
}

impl HttpClientConfig {
    /// Construct a validated configuration from an absolute HTTP or HTTPS URL.
    pub fn new(base_url: impl AsRef<str>) -> Result<Self, NetError> {
        let value = base_url.as_ref();
        let parsed = reqwest::Url::parse(value).map_err(|_| invalid_config("base_url"))?;
        let scheme = parsed.scheme();
        if scheme != "http" && scheme != "https" {
            return Err(invalid_config("base_url"));
        }
        if parsed.host_str().is_none()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.fragment().is_some()
        {
            return Err(invalid_config("base_url"));
        }
        Ok(Self {
            base_url: parsed,
            common_headers: HeaderMap::new(),
            timeout: Duration::from_secs(30),
            // reqwest 0.12 defaults to a 15 second TCP keepalive.
            tcp_keepalive: Some(Duration::from_secs(15)),
            max_response_bytes: 8 * 1024 * 1024,
            request_queue_capacity: 128,
            response_queue_capacity: 128,
            callback_queue_capacity: 128,
            default_retry_policy: RetryPolicy::no_retry(),
            tls: HttpTlsConfig::default(),
            proxy: None,
            environment_proxy: false,
        })
    }

    /// Add or replace the public headers copied to each request.
    pub fn with_common_headers(mut self, headers: HeaderMap) -> Result<Self, NetError> {
        validate_headers(&headers)?;
        self.common_headers = headers;
        Ok(self)
    }

    /// Replace the request timeout.  A zero timeout is rejected because it would make every
    /// request fail before the transport can produce a useful response.
    pub fn with_timeout(mut self, timeout: Duration) -> Result<Self, NetError> {
        if timeout.is_zero() {
            return Err(invalid_config("timeout"));
        }
        self.timeout = timeout;
        Ok(self)
    }

    /// Set the TCP keepalive interval. A zero interval disables keepalive.
    pub fn with_tcp_keepalive(mut self, keepalive: Duration) -> Self {
        self.tcp_keepalive = if keepalive.is_zero() {
            None
        } else {
            Some(keepalive)
        };
        self
    }

    /// Disable TCP keepalive for pooled connections.
    pub fn without_tcp_keepalive(mut self) -> Self {
        self.tcp_keepalive = None;
        self
    }

    /// Set the maximum response body size in bytes.
    pub fn with_max_response_bytes(mut self, max_response_bytes: usize) -> Result<Self, NetError> {
        if max_response_bytes == 0 {
            return Err(invalid_config("max_response_bytes"));
        }
        self.max_response_bytes = max_response_bytes;
        Ok(self)
    }

    /// Set the bounded request queue capacity.
    pub fn with_request_queue_capacity(
        mut self,
        request_queue_capacity: usize,
    ) -> Result<Self, NetError> {
        if request_queue_capacity == 0 {
            return Err(invalid_config("request_queue_capacity"));
        }
        self.request_queue_capacity = request_queue_capacity;
        Ok(self)
    }

    /// Set the bounded response queue capacity.
    pub fn with_response_queue_capacity(
        mut self,
        response_queue_capacity: usize,
    ) -> Result<Self, NetError> {
        if response_queue_capacity == 0 {
            return Err(invalid_config("response_queue_capacity"));
        }
        self.response_queue_capacity = response_queue_capacity;
        Ok(self)
    }

    /// Set the bounded callback queue capacity.
    pub fn with_callback_queue_capacity(
        mut self,
        callback_queue_capacity: usize,
    ) -> Result<Self, NetError> {
        if callback_queue_capacity == 0 {
            return Err(invalid_config("callback_queue_capacity"));
        }
        self.callback_queue_capacity = callback_queue_capacity;
        Ok(self)
    }

    /// Set the default retry policy.
    pub fn with_default_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.default_retry_policy = policy;
        self
    }

    /// Set TLS trust and client identity settings.
    pub fn with_tls(mut self, tls: HttpTlsConfig) -> Self {
        self.tls = tls;
        self
    }

    /// Use an explicit HTTP CONNECT proxy for requests made by this client.
    pub fn with_proxy(mut self, proxy: HttpProxyConfig) -> Self {
        self.proxy = Some(proxy);
        self
    }

    /// Explicitly opt into proxy variables from the process environment.
    pub fn with_environment_proxy(mut self, enabled: bool) -> Self {
        self.environment_proxy = enabled;
        self
    }

    /// Validate a configuration assembled with public fields.
    pub fn validate(&self) -> Result<(), NetError> {
        let scheme = self.base_url.scheme();
        if (scheme != "http" && scheme != "https") || self.base_url.host_str().is_none() {
            return Err(invalid_config("base_url"));
        }
        if !self.base_url.username().is_empty()
            || self.base_url.password().is_some()
            || self.base_url.fragment().is_some()
        {
            return Err(invalid_config("base_url"));
        }
        if self.timeout.is_zero() {
            return Err(invalid_config("timeout"));
        }
        if self
            .tcp_keepalive
            .is_some_and(|duration| duration.is_zero())
        {
            return Err(invalid_config("tcp_keepalive"));
        }
        if self.max_response_bytes == 0 {
            return Err(invalid_config("max_response_bytes"));
        }
        if self.request_queue_capacity == 0 {
            return Err(invalid_config("request_queue_capacity"));
        }
        if self.response_queue_capacity == 0 {
            return Err(invalid_config("response_queue_capacity"));
        }
        if self.callback_queue_capacity == 0 {
            return Err(invalid_config("callback_queue_capacity"));
        }
        validate_headers(&self.common_headers)
    }

    /// Merge request headers over the client-wide headers.
    pub fn merge_headers(&self, request_headers: &HeaderMap) -> Result<HeaderMap, NetError> {
        self.validate()?;
        validate_headers(request_headers)?;
        let mut merged = self.common_headers.clone();
        for (name, value) in request_headers {
            merged.insert(name.clone(), value.clone());
        }
        Ok(merged)
    }
}

impl fmt::Debug for HttpClientConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let header_names: Vec<&str> = self.common_headers.keys().map(HeaderName::as_str).collect();
        formatter
            .debug_struct("HttpClientConfig")
            // Keep query strings, credentials, and fragments out of logs.  A URL may contain
            // a signed token even though request headers are otherwise redacted.
            .field("base_scheme", &self.base_url.scheme())
            .field("base_has_host", &self.base_url.host_str().is_some())
            .field("common_header_names", &header_names)
            .field("timeout", &self.timeout)
            .field("tcp_keepalive", &self.tcp_keepalive)
            .field("max_response_bytes", &self.max_response_bytes)
            .field("request_queue_capacity", &self.request_queue_capacity)
            .field("response_queue_capacity", &self.response_queue_capacity)
            .field("callback_queue_capacity", &self.callback_queue_capacity)
            .field("default_retry_policy", &self.default_retry_policy)
            .field("tls", &self.tls)
            .field("proxy_configured", &self.proxy.is_some())
            .field("environment_proxy", &self.environment_proxy)
            .finish()
    }
}

impl HttpClientConfig {
    pub(crate) fn resolve_request_path(&self, path: &str) -> Result<reqwest::Url, NetError> {
        let path = path.trim();
        if path.is_empty()
            || path.starts_with("//")
            || path.starts_with("\\")
            || path.contains('\\')
            || path.contains('#')
            || path.chars().any(char::is_control)
        {
            return Err(invalid_request_path());
        }
        if let Ok(parsed) = reqwest::Url::parse(path) {
            if !parsed.scheme().is_empty() || parsed.host_str().is_some() {
                return Err(invalid_request_path());
            }
        }
        let joined = self
            .base_url
            .join(path)
            .map_err(|_| invalid_request_path())?;
        if joined.scheme() != self.base_url.scheme()
            || joined.host_str() != self.base_url.host_str()
            || joined.port_or_known_default() != self.base_url.port_or_known_default()
            || !joined.username().is_empty()
            || joined.password().is_some()
            || joined.fragment().is_some()
        {
            return Err(invalid_request_path());
        }
        Ok(joined)
    }
}

fn validate_headers(headers: &HeaderMap) -> Result<(), NetError> {
    for name in headers.keys() {
        if is_hop_by_hop(name) {
            return Err(invalid_config("headers"));
        }
    }
    Ok(())
}

fn validate_root_certificates(bytes: &[u8]) -> Result<(), NetError> {
    if bytes.is_empty() || bytes.len() > MAX_TLS_PEM_BYTES {
        return Err(invalid_config("tls.root_certificates"));
    }
    let blocks = strict_pem_blocks(bytes, "CERTIFICATE", "tls.root_certificates")?;
    if blocks.len() > MAX_TLS_CERTIFICATES {
        return Err(invalid_config("tls.root_certificates"));
    }
    let mut roots = rustls::RootCertStore::empty();
    for block in blocks {
        let certificate = CertificateDer::from_pem_slice(block)
            .map_err(|_| invalid_config("tls.root_certificates"))?;
        roots
            .add(certificate)
            .map_err(|_| invalid_config("tls.root_certificates"))?;
    }
    if roots.is_empty() {
        return Err(invalid_config("tls.root_certificates"));
    }
    Ok(())
}

fn parse_certificates<'a>(
    bytes: &'a [u8],
    field: &'static str,
) -> Result<Vec<CertificateDer<'static>>, NetError> {
    let blocks = strict_pem_blocks(bytes, "CERTIFICATE", field)?;
    if blocks.len() > MAX_TLS_CERTIFICATES {
        return Err(invalid_config(field));
    }
    let mut output = Vec::new();
    output
        .try_reserve(blocks.len())
        .map_err(|_| invalid_config(field))?;
    for block in blocks {
        let certificate =
            CertificateDer::from_pem_slice(block).map_err(|_| invalid_config(field))?;
        output.push(certificate);
    }
    Ok(output)
}

fn strict_pem_blocks<'a>(
    bytes: &'a [u8],
    label: &str,
    field: &'static str,
) -> Result<Vec<&'a [u8]>, NetError> {
    if bytes.is_empty() || bytes.len() > MAX_TLS_PEM_BYTES {
        return Err(invalid_config(field));
    }
    let text = std::str::from_utf8(bytes)
        .map_err(|_| invalid_config(field))?
        .trim();
    let begin = format!("-----BEGIN {label}-----");
    let end = format!("-----END {label}-----");
    let mut remaining = text;
    let mut blocks = Vec::new();
    while !remaining.is_empty() {
        if blocks.len() >= MAX_TLS_CERTIFICATES || !remaining.starts_with(&begin) {
            return Err(invalid_config(field));
        }
        let end_index = remaining
            .find(&end)
            .and_then(|index| index.checked_add(end.len()))
            .ok_or_else(|| invalid_config(field))?;
        let block = remaining
            .get(..end_index)
            .ok_or_else(|| invalid_config(field))?;
        if block
            .get(begin.len()..)
            .is_none_or(|body| body.contains("-----BEGIN "))
        {
            return Err(invalid_config(field));
        }
        blocks.try_reserve(1).map_err(|_| invalid_config(field))?;
        blocks.push(block.as_bytes());
        remaining = remaining
            .get(end_index..)
            .ok_or_else(|| invalid_config(field))?
            .trim();
    }
    if blocks.is_empty() {
        return Err(invalid_config(field));
    }
    Ok(blocks)
}

fn is_hop_by_hop(name: &HeaderName) -> bool {
    matches!(
        name.as_str().to_ascii_lowercase().as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
            | "host"
            | "content-length"
    )
}

fn invalid_config(field: &'static str) -> NetError {
    NetError::config(field, "invalid HTTP client configuration")
        .with_stage(ErrorStage::Configuration)
}

fn invalid_request_path() -> NetError {
    NetError::input("request.path", "must be a safe relative path")
        .with_stage(ErrorStage::RequestBuild)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::error::ErrorKind;
    use http::{HeaderValue, Method};

    type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

    fn check(condition: bool, message: &str) -> TestResult {
        if condition {
            Ok(())
        } else {
            Err(std::io::Error::other(message).into())
        }
    }

    #[test]
    fn validates_url_and_limits() -> TestResult {
        let invalid = HttpClientConfig::new("ftp://example.com");
        check(invalid.is_err(), "unsupported URL scheme must fail")?;
        let config = HttpClientConfig::new("https://example.com")?
            .with_timeout(Duration::from_secs(2))?
            .with_max_response_bytes(1024)?
            .with_request_queue_capacity(2)?
            .with_response_queue_capacity(2)?;
        check(
            config.timeout == Duration::from_secs(2),
            "timeout not stored",
        )?;
        check(config.max_response_bytes == 1024, "body limit not stored")?;
        check(
            config.request_queue_capacity == 2,
            "request capacity not stored",
        )?;
        check(
            config.response_queue_capacity == 2,
            "response capacity not stored",
        )?;
        Ok(())
    }

    #[test]
    fn resolves_only_same_origin_request_paths() -> TestResult {
        let config = HttpClientConfig::new("https://example.com/api/")?;
        let relative = config.resolve_request_path("v1/items?limit=2")?;
        check(
            relative.as_str() == "https://example.com/api/v1/items?limit=2",
            "relative request path was resolved incorrectly",
        )?;
        for path in [
            "https://other.example/v1/items",
            "//other.example/v1/items",
            "/v1/items#fragment",
        ] {
            check(
                config.resolve_request_path(path).is_err(),
                "cross-origin or fragmented path was accepted",
            )?;
        }
        check(
            HttpClientConfig::new("https://user:secret@example.com/api").is_err(),
            "base URL credentials must be rejected",
        )
    }

    #[test]
    fn keeps_transport_defaults_explicit_and_validates_overrides() -> TestResult {
        let config = HttpClientConfig::new("https://example.com")?
            .with_tcp_keepalive(Duration::from_secs(45))
            .with_environment_proxy(true);
        check(
            config.tcp_keepalive == Some(Duration::from_secs(45)),
            "keepalive override was not retained",
        )?;
        check(
            config.environment_proxy,
            "environment proxy opt-in was not retained",
        )?;
        let disabled = config.clone().without_tcp_keepalive();
        check(
            disabled.tcp_keepalive.is_none(),
            "keepalive was not disabled",
        )?;
        check(
            config.with_timeout(Duration::ZERO).is_err(),
            "zero timeout must be rejected",
        )
    }

    #[test]
    fn merges_request_headers_and_rejects_hop_by_hop() -> TestResult {
        let mut common = HeaderMap::new();
        common.insert("x-company", HeaderValue::from_static("common"));
        common.insert("x-stable", HeaderValue::from_static("stable"));
        let config = HttpClientConfig::new("https://example.com")?.with_common_headers(common)?;
        let mut request = HeaderMap::new();
        request.insert("x-company", HeaderValue::from_static("request"));
        let merged = config.merge_headers(&request)?;
        let company = merged
            .get("x-company")
            .map(HeaderValue::to_str)
            .transpose()?;
        check(
            company == Some("request"),
            "request header must override common",
        )?;
        check(
            merged.contains_key("x-stable"),
            "common header must be preserved",
        )?;

        let mut forbidden = HeaderMap::new();
        forbidden.insert("host", HeaderValue::from_static("secret.example"));
        let error = HttpClientConfig::new("https://example.com")?.with_common_headers(forbidden);
        check(error.is_err(), "host must be rejected")?;
        Ok(())
    }

    #[test]
    fn config_debug_does_not_include_header_values() -> TestResult {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", HeaderValue::from_static("Bearer secret"));
        let config = HttpClientConfig::new("https://example.com/api?access_token=url-secret")?
            .with_common_headers(headers)?;
        let debug = format!("{config:?}");
        check(
            !debug.contains("Bearer secret"),
            "header value leaked through Debug",
        )?;
        check(
            debug.contains("authorization"),
            "header name missing from Debug",
        )?;
        check(
            !debug.contains("url-secret"),
            "URL query leaked through Debug",
        )?;
        let _ = Method::GET;
        check(
            ErrorKind::InvalidConfig != ErrorKind::InvalidInput,
            "error kinds must remain distinct",
        )
    }

    #[test]
    fn rejects_invalid_tls_material_before_worker_start() -> TestResult {
        let root_result =
            HttpTlsConfig::default().with_root_certificates(b"not-a-certificate", false);
        check(root_result.is_err(), "invalid root certificate must fail")?;
        let identity_result =
            HttpTlsConfig::default().with_client_identity_pem(b"not-a-certificate", b"not-a-key");
        check(
            identity_result.is_err(),
            "invalid client identity must fail",
        )
    }

    #[test]
    fn supports_replacing_and_appending_custom_roots() -> TestResult {
        let ca = include_bytes!("../../../tests/fixtures/network/ca.pem");
        let other_ca = include_bytes!("../../../tests/fixtures/network/other-ca.pem");
        let config = HttpTlsConfig::default()
            .with_root_certificates(ca, true)?
            .with_additional_root_certificate(other_ca)?;
        check(
            config.custom_root_count() == 2,
            "both custom roots must be retained",
        )?;
        check(
            config.replaces_builtin_roots(),
            "replace builtin root setting was lost",
        )
    }

    #[test]
    fn rejects_malformed_or_mismatched_identity_material() -> TestResult {
        let certificates = include_bytes!("../../../tests/fixtures/network/client.pem");
        let private_key = include_bytes!("../../../tests/fixtures/network/client-key.pem");
        let valid = HttpTlsConfig::default().with_client_identity_pem(certificates, private_key)?;
        check(
            format!("{valid:?}").contains("client_identity_configured: true"),
            "valid client identity was not retained",
        )?;
        let wrong_key = include_bytes!("../../../tests/fixtures/network/other-client-key.pem");
        let result = HttpTlsConfig::default().with_client_identity_pem(certificates, wrong_key);
        check(result.is_err(), "mismatched certificate and key must fail")?;
        let malformed = b"-----BEGIN CERTIFICATE-----\n-----END PRIVATE KEY-----";
        let result = HttpTlsConfig::default().with_root_certificates(malformed, false);
        check(result.is_err(), "mismatched PEM labels must fail")
    }

    #[test]
    fn validates_explicit_http_proxy_without_credentials_in_url() -> TestResult {
        let proxy =
            HttpProxyConfig::http("http://127.0.0.1:8080")?.with_basic_auth("user", "pw")?;
        check(
            proxy.url() == "http://127.0.0.1:8080/",
            "proxy URL normalization failed",
        )?;
        check(
            proxy.credentials() == Some(("user", "pw")),
            "proxy credentials missing",
        )?;
        let embedded = HttpProxyConfig::http("http://user:pw@127.0.0.1:8080");
        check(embedded.is_err(), "embedded proxy credentials must fail")
    }
}

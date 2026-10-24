//! Configuration contracts for the optional HTTP client.
//!
//! This module deliberately contains validation and immutable request defaults only.  Runtime
//! creation and transport setup live in the HTTP worker module.

use crate::api::error::{ErrorStage, NetError};
#[cfg(feature = "http-client-auth")]
use crate::api::http::auth::HttpAuth;
#[cfg(feature = "http-client-cookies")]
use crate::api::http::cookies::{CookieLimits, CookieStore};
#[cfg(feature = "http-client-http2")]
use crate::api::http::http2::{Http2Config, HttpVersionPolicy};
use crate::api::http::http_timeouts::{validate_duration, HttpTimeouts};
use crate::api::http::redirect::{HttpTransportSecurity, RedirectPolicy};
use crate::api::http::retry::HttpRetryPolicy;
use crate::api::http::HttpAdmissionPolicy;
#[cfg(feature = "http-client-middleware")]
use crate::api::http::HttpMiddleware;
use http::{HeaderMap, HeaderName};
use rustls::pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::sign::{CertifiedKey, SingleCertAndKey};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

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
/// bounded buffers so the worker can build its rustls connector once during startup.
#[derive(Clone)]
pub struct HttpTlsConfig {
    provider: Option<Arc<rustls::crypto::CryptoProvider>>,
    root_certificates: Vec<Vec<u8>>,
    root_mode: HttpRootCertificateMode,
    client_identity: Option<HttpClientIdentity>,
}

// Keep the key material rather than a provider-specific signing key: the
// client's final provider must load the key used for its actual TLS handshakes.
struct HttpClientIdentity {
    certificates: Vec<CertificateDer<'static>>,
    private_key: PrivatePkcs8KeyDer<'static>,
}

impl Clone for HttpClientIdentity {
    fn clone(&self) -> Self {
        Self {
            certificates: self.certificates.clone(),
            private_key: self.private_key.clone_key(),
        }
    }
}

impl Default for HttpTlsConfig {
    fn default() -> Self {
        Self {
            provider: None,
            root_certificates: Vec::new(),
            root_mode: HttpRootCertificateMode::Append,
            client_identity: None,
        }
    }
}

impl HttpTlsConfig {
    /// Select the rustls cryptographic provider for this HTTP client.
    ///
    /// An explicit provider takes precedence over the process-wide rustls
    /// provider. This does not install a process-wide provider. Without either
    /// provider, client creation returns a configuration error. Configure this
    /// before creating the client; its order relative to certificate and
    /// private-key setters is immaterial.
    ///
    /// ```
    /// use open_net::api::http::HttpTlsConfig;
    /// use open_net::tls::CryptoProvider;
    /// use std::sync::Arc;
    ///
    /// fn configure(provider: Arc<CryptoProvider>) -> HttpTlsConfig {
    ///     let tls = HttpTlsConfig::default().with_crypto_provider(Arc::clone(&provider));
    ///     assert!(tls.crypto_provider().is_some_and(|selected| Arc::ptr_eq(selected, &provider)));
    ///     tls
    /// }
    /// ```
    pub fn with_crypto_provider(mut self, provider: Arc<rustls::crypto::CryptoProvider>) -> Self {
        self.provider = Some(provider);
        self
    }

    /// Return the explicitly configured cryptographic provider, if any.
    pub fn crypto_provider(&self) -> Option<&Arc<rustls::crypto::CryptoProvider>> {
        self.provider.as_ref()
    }

    /// Configure one PEM bundle as the custom trust-root set.
    ///
    /// The bundle must contain one or more valid `CERTIFICATE` PEM blocks and
    /// remain within the library's bounded certificate limits. When
    /// `replace_builtin_roots` is `false`, the roots are appended to the SDK's
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
    /// This validates bounded PEM formatting and retains the decoded material.
    /// When the client is created, its final cryptographic provider loads the
    /// key and verifies that it matches the certificate. This allows provider
    /// selection before or after this method. Private material is never exposed
    /// by the public API or by the type's `Debug` implementation.
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
        self.client_identity = Some(HttpClientIdentity {
            certificates: chain,
            private_key: key,
        });
        Ok(self)
    }

    pub(crate) fn compile(
        &self,
        alpn_protocols: Vec<Vec<u8>>,
    ) -> Result<rustls::ClientConfig, NetError> {
        let provider = crate::module::tls::select_provider(self.provider.as_ref())?;
        let builder = rustls::ClientConfig::builder_with_provider(Arc::clone(&provider))
            .with_safe_default_protocol_versions()
            .map_err(|_| invalid_config("tls.provider"))?
            .with_root_certificates(self.compile_roots()?);
        let mut tls = if let Some(identity) = &self.client_identity {
            let certified = CertifiedKey::from_der(
                identity.certificates.clone(),
                PrivateKeyDer::Pkcs8(identity.private_key.clone_key()),
                &provider,
            )
            .map_err(|_| invalid_config("tls.client_identity"))?;
            certified
                .keys_match()
                .map_err(|_| invalid_config("tls.client_identity"))?;
            builder.with_client_cert_resolver(Arc::new(SingleCertAndKey::from(certified)))
        } else {
            builder.with_no_client_auth()
        };
        // reqwest's preconfigured path does not apply its own SNI or ALPN
        // options. Set them here so Cargo feature unification cannot expand
        // the protocols advertised by the SDK.
        tls.enable_sni = true;
        tls.alpn_protocols = alpn_protocols;
        Ok(tls)
    }

    fn compile_roots(&self) -> Result<rustls::RootCertStore, NetError> {
        let mut roots =
            crate::module::tls::root_store(self.root_mode == HttpRootCertificateMode::Append);
        for pem in &self.root_certificates {
            for certificate in parse_certificates(pem, "tls.root_certificates")? {
                roots
                    .add(certificate)
                    .map_err(|_| invalid_config("tls.root_certificates"))?;
            }
        }
        if roots.is_empty() {
            return Err(invalid_config("tls.root_certificates"));
        }
        Ok(roots)
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
            .field("crypto_provider_configured", &self.provider.is_some())
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
    #[cfg(feature = "quality-doctor")]
    standalone_quality: Option<crate::api::quality::QualityDoctorConfig>,
    /// Base URL used to resolve request paths.
    pub base_url: reqwest::Url,
    /// Headers copied into every request before request-specific headers are merged.
    pub common_headers: HeaderMap,
    /// Legacy timeout for each attempt, including preparation, response headers, and body.
    ///
    /// This starts at SDK request preparation, excludes admission waiting, and
    /// resets for a retry. Explicit [`HttpTimeouts`] attempt settings replace it.
    /// Paused stream consumption still uses time.
    pub timeout: Duration,
    connect_timeout: Option<Duration>,
    timeouts: HttpTimeouts,
    admission_policy: HttpAdmissionPolicy,
    redirect_policy: RedirectPolicy,
    transport_security: HttpTransportSecurity,
    /// Optional TCP keepalive interval applied to pooled connections.
    pub tcp_keepalive: Option<Duration>,
    /// Maximum response body size accepted by the receive lane. Defaults to 1024 MiB.
    pub max_response_bytes: usize,
    /// Capacity of the admission queue and in-flight operation quota.
    ///
    /// One permit is held from admission until the final callback completes, or
    /// a stream terminates or is dropped. Retries retain capacity between attempts;
    /// timeout, cancellation, and shutdown reclaim even an unpolled retained stream.
    /// The value must be within `1..=tokio::sync::Semaphore::MAX_PERMITS`; values outside
    /// this range return a structured configuration error.
    pub request_queue_capacity: usize,
    /// Capacity of the response queue owned by the receive lane.
    /// The value must be within `1..=tokio::sync::Semaphore::MAX_PERMITS`; values outside
    /// this range return a structured configuration error.
    pub response_queue_capacity: usize,
    /// Capacity of the callback queue owned by the callback lane.
    /// The value must be within `1..=tokio::sync::Semaphore::MAX_PERMITS`; values outside
    /// this range return a structured configuration error.
    pub callback_queue_capacity: usize,
    /// Policy applied when a request does not provide its own policy.
    pub default_retry_policy: HttpRetryPolicy,
    /// TLS trust and client identity settings.
    pub tls: HttpTlsConfig,
    /// Optional explicit HTTP CONNECT proxy configuration.
    pub proxy: Option<HttpProxyConfig>,
    /// Whether reqwest may read HTTP(S) proxy settings from the environment.
    pub environment_proxy: bool,
    /// Optional SDK-owned Cookie store. Cookie handling remains disabled when absent.
    #[cfg(feature = "http-client-cookies")]
    pub(crate) cookie_store: Option<Arc<dyn CookieStore>>,
    /// Bounded Cookie processing limits used by the HTTP worker.
    #[cfg(feature = "http-client-cookies")]
    pub(crate) cookie_limits: CookieLimits,
    /// Optional asynchronous authentication provider applied to each request attempt.
    #[cfg(feature = "http-client-auth")]
    auth: Option<Arc<dyn HttpAuth>>,
    /// HTTP protocol version policy and HTTP/2 tuning.
    #[cfg(feature = "http-client-http2")]
    http_version_policy: HttpVersionPolicy,
    #[cfg(feature = "http-client-http2")]
    http2: Http2Config,
    /// Optional middleware applied to concrete request and stream APIs.
    #[cfg(feature = "http-client-middleware")]
    middlewares: Vec<Arc<dyn HttpMiddleware>>,
}

impl HttpClientConfig {
    /// Sets passive diagnostics for an independently constructed HTTP client.
    ///
    /// Use with [`super::HttpClient::new`] or
    /// [`super::HttpClient::new_with_network_status`]. The supplied configuration
    /// determines the standalone collector's mode, protocol filter, bounded
    /// storage, retention, sampling, and finding thresholds. Omitting this setting
    /// uses `QualityMode::Off`; HTTP observations also require HTTP to be included
    /// in the configured protocol selection.
    ///
    /// This builder stores the value without validation. [`Self::validate`] and
    /// client construction validate it, returning `InvalidConfig` with field
    /// `http.quality_doctor` on failure. Use the diagnostic configuration's own
    /// `validate()` method to inspect its specific invalid field.
    ///
    /// Collection stays dormant until [`super::HttpClient::quality_doctor`] is
    /// called; activate before submitting work because earlier observations are
    /// not reconstructed. Client clones share this standalone diagnostic owner.
    /// Passing a network-status context does not make its diagnostics part of the
    /// context's engine collector.
    ///
    /// Engine-managed clients inherit the engine's diagnostic configuration and
    /// reject this setting, even when the supplied mode is Off. Configure those
    /// clients through [`crate::OpenNetConfig::with_quality_doctor`] instead.
    #[cfg(feature = "quality-doctor")]
    pub fn with_standalone_quality_doctor(
        mut self,
        config: crate::api::quality::QualityDoctorConfig,
    ) -> Self {
        self.standalone_quality = Some(config);
        self
    }

    #[cfg(feature = "quality-doctor")]
    pub(crate) fn has_standalone_quality_doctor(&self) -> bool {
        self.standalone_quality.is_some()
    }

    #[cfg(feature = "quality-doctor")]
    pub(crate) fn standalone_quality_config(&self) -> crate::api::quality::QualityDoctorConfig {
        self.standalone_quality.clone().unwrap_or_default()
    }

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
            #[cfg(feature = "quality-doctor")]
            standalone_quality: None,
            base_url: parsed,
            common_headers: HeaderMap::new(),
            timeout: Duration::from_secs(30),
            connect_timeout: None,
            timeouts: HttpTimeouts::new(),
            admission_policy: HttpAdmissionPolicy::default(),
            redirect_policy: RedirectPolicy::default(),
            transport_security: HttpTransportSecurity::default(),
            // reqwest defaults to a 15 second TCP keepalive.
            tcp_keepalive: Some(Duration::from_secs(15)),
            max_response_bytes: 1024 * 1024 * 1024,
            request_queue_capacity: 128,
            response_queue_capacity: 128,
            callback_queue_capacity: 128,
            default_retry_policy: HttpRetryPolicy::no_retry(),
            tls: HttpTlsConfig::default(),
            proxy: None,
            environment_proxy: false,
            #[cfg(feature = "http-client-cookies")]
            cookie_store: None,
            #[cfg(feature = "http-client-cookies")]
            cookie_limits: CookieLimits::default(),
            #[cfg(feature = "http-client-auth")]
            auth: None,
            #[cfg(feature = "http-client-http2")]
            http_version_policy: HttpVersionPolicy::Auto,
            #[cfg(feature = "http-client-http2")]
            http2: Http2Config::default(),
            #[cfg(feature = "http-client-middleware")]
            middlewares: Vec::new(),
        })
    }

    /// Replace the redirect authorization policy.
    ///
    /// The default follows at most 10 redirects within the original request's
    /// origin. Cross-origin targets require an explicit origin allowlist. This
    /// applies to buffered, callback, streaming, and SSE requests, regardless of
    /// HTTP version or installed Auth/Cookie providers.
    pub fn with_redirect_policy(mut self, policy: RedirectPolicy) -> Self {
        self.redirect_policy = policy;
        self
    }

    /// Return the redirect authorization policy.
    pub fn redirect_policy(&self) -> &RedirectPolicy {
        &self.redirect_policy
    }

    /// Select transport security independently of HTTP protocol versions.
    ///
    /// The default permits an explicitly configured HTTP base URL but rejects
    /// HTTPS-to-HTTP redirects. `HttpsOnly` also rejects an HTTP base URL here
    /// and during [`Self::validate`]. HTTP/2-only clients always require HTTPS.
    pub fn with_transport_security(
        mut self,
        security: HttpTransportSecurity,
    ) -> Result<Self, NetError> {
        if security == HttpTransportSecurity::HttpsOnly && self.base_url.scheme() != "https" {
            return Err(invalid_config("http.transport_security"));
        }
        self.transport_security = security;
        Ok(self)
    }

    /// Return the configured transport requirement.
    ///
    /// HTTP/2-only mode independently adds its HTTPS requirement.
    pub fn transport_security(&self) -> HttpTransportSecurity {
        self.transport_security
    }

    /// Append a middleware to the concrete request and stream pipeline.
    #[cfg(feature = "http-client-middleware")]
    pub fn with_middleware<M>(mut self, middleware: M) -> Self
    where
        M: HttpMiddleware + 'static,
    {
        self.middlewares.push(Arc::new(middleware));
        self
    }

    /// Append a shared middleware to the concrete request and stream pipeline.
    #[cfg(feature = "http-client-middleware")]
    pub fn with_middleware_arc(mut self, middleware: Arc<dyn HttpMiddleware>) -> Self {
        self.middlewares.push(middleware);
        self
    }

    /// Install an asynchronous authentication provider.
    #[cfg(feature = "http-client-auth")]
    pub fn with_auth<A>(mut self, auth: A) -> Self
    where
        A: HttpAuth + 'static,
    {
        self.auth = Some(Arc::new(auth));
        self
    }

    /// Install a shared authentication provider.
    #[cfg(feature = "http-client-auth")]
    pub fn with_auth_arc(mut self, auth: Arc<dyn HttpAuth>) -> Self {
        self.auth = Some(auth);
        self
    }

    /// Select the HTTP protocol versions used by this client.
    #[cfg(feature = "http-client-http2")]
    pub fn with_http_version_policy(mut self, policy: HttpVersionPolicy) -> Self {
        self.http_version_policy = policy;
        self
    }

    /// Configure HTTP/2 transport tuning after validating its bounds.
    #[cfg(feature = "http-client-http2")]
    pub fn with_http2(mut self, http2: Http2Config) -> Result<Self, NetError> {
        http2.validate()?;
        self.http2 = http2;
        Ok(self)
    }

    #[cfg(feature = "http-client-auth")]
    pub(crate) fn auth(&self) -> Option<Arc<dyn HttpAuth>> {
        self.auth.as_ref().map(Arc::clone)
    }

    #[cfg(feature = "http-client-middleware")]
    pub(crate) fn middleware_chain(&self) -> std::sync::Arc<[Arc<dyn HttpMiddleware>]> {
        std::sync::Arc::from(self.middlewares.clone())
    }

    /// Add or replace the public headers copied to each request.
    pub fn with_common_headers(mut self, headers: HeaderMap) -> Result<Self, NetError> {
        validate_headers(&headers)?;
        self.common_headers = headers;
        Ok(self)
    }

    /// Replace the legacy attempt timeout. Explicit [`HttpTimeouts`] attempt
    /// settings take precedence regardless of builder call order.
    pub fn with_timeout(mut self, timeout: Duration) -> Result<Self, NetError> {
        validate_duration(timeout, "http.timeout", false)?;
        self.timeout = timeout;
        Ok(self)
    }

    /// Limit establishment of each new connection, including DNS, proxy, and TLS.
    /// Reused pooled connections do not consume this dedicated budget.
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Result<Self, NetError> {
        validate_duration(timeout, "http.connect_timeout", false)?;
        self.connect_timeout = Some(timeout);
        Ok(self)
    }

    /// Disable only the dedicated connection budget.
    pub fn without_connect_timeout(mut self) -> Self {
        self.connect_timeout = None;
        self
    }

    /// Return the dedicated connection establishment budget.
    pub fn connect_timeout(&self) -> Option<Duration> {
        self.connect_timeout
    }

    /// Select the default capacity policy for response-returning Future APIs.
    ///
    /// The default is [`HttpAdmissionPolicy::Wait`]. Request builders may override
    /// this value. Callback `send` and `send_wait` keep their respective immediate
    /// and waiting admission behavior regardless of this setting.
    pub fn with_admission_policy(mut self, policy: HttpAdmissionPolicy) -> Self {
        self.admission_policy = policy;
        self
    }

    /// Return the default capacity policy for response-returning Future APIs.
    pub fn admission_policy(&self) -> HttpAdmissionPolicy {
        self.admission_policy
    }

    /// Replace this client's complete layer of phase timeout defaults.
    /// Inherited Admission still falls back to 30 seconds after replacement.
    pub fn with_timeouts(mut self, timeouts: HttpTimeouts) -> Result<Self, NetError> {
        timeouts.validate()?;
        self.timeouts = timeouts;
        Ok(self)
    }

    /// Return the explicit client overrides, before request and legacy resolution.
    pub fn timeouts(&self) -> &HttpTimeouts {
        &self.timeouts
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
    ///
    /// The value must be within `1..=tokio::sync::Semaphore::MAX_PERMITS`; values outside
    /// this range return a structured configuration error.
    pub fn with_request_queue_capacity(
        mut self,
        request_queue_capacity: usize,
    ) -> Result<Self, NetError> {
        validate_queue_capacity(request_queue_capacity, "request_queue_capacity")?;
        self.request_queue_capacity = request_queue_capacity;
        Ok(self)
    }

    /// Set the bounded response queue capacity.
    ///
    /// The value must be within `1..=tokio::sync::Semaphore::MAX_PERMITS`; values outside
    /// this range return a structured configuration error.
    pub fn with_response_queue_capacity(
        mut self,
        response_queue_capacity: usize,
    ) -> Result<Self, NetError> {
        validate_queue_capacity(response_queue_capacity, "response_queue_capacity")?;
        self.response_queue_capacity = response_queue_capacity;
        Ok(self)
    }

    /// Set the bounded callback queue capacity.
    ///
    /// The value must be within `1..=tokio::sync::Semaphore::MAX_PERMITS`; values outside
    /// this range return a structured configuration error.
    pub fn with_callback_queue_capacity(
        mut self,
        callback_queue_capacity: usize,
    ) -> Result<Self, NetError> {
        validate_queue_capacity(callback_queue_capacity, "callback_queue_capacity")?;
        self.callback_queue_capacity = callback_queue_capacity;
        Ok(self)
    }

    /// Set the default retry policy.
    pub fn with_default_retry_policy(mut self, policy: HttpRetryPolicy) -> Self {
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

    /// Install a Cookie store. `CookieJar::new()` creates independent state;
    /// cloning an existing jar or this configuration explicitly shares it.
    #[cfg(feature = "http-client-cookies")]
    pub fn with_cookie_store<S>(mut self, store: S) -> Self
    where
        S: CookieStore + 'static,
    {
        self.cookie_store = Some(Arc::new(store));
        self
    }

    /// Install a Cookie store whose sharing is explicit through `Arc`.
    #[cfg(feature = "http-client-cookies")]
    pub fn with_cookie_store_arc(mut self, store: Arc<dyn CookieStore>) -> Self {
        self.cookie_store = Some(store);
        self
    }

    /// Configure bounded Cookie header and response processing limits.
    /// Jar capacity is configured separately through `CookieJar::with_limits`.
    #[cfg(feature = "http-client-cookies")]
    pub fn with_cookie_limits(mut self, limits: CookieLimits) -> Result<Self, NetError> {
        limits.validate()?;
        self.cookie_limits = limits;
        Ok(self)
    }

    #[cfg(feature = "http-client-cookies")]
    pub(crate) fn cookie_store(&self) -> Option<Arc<dyn CookieStore>> {
        self.cookie_store.as_ref().map(Arc::clone)
    }

    #[cfg(feature = "http-client-cookies")]
    pub(crate) fn cookie_limits(&self) -> CookieLimits {
        self.cookie_limits
    }

    /// Validate a configuration assembled with public fields.
    ///
    /// TLS provider availability and client certificate/key compatibility are
    /// checked once when the HTTP client is created.
    pub fn validate(&self) -> Result<(), NetError> {
        #[cfg(feature = "quality-doctor")]
        if let Some(config) = &self.standalone_quality {
            config
                .validate()
                .map_err(|_| invalid_config("http.quality_doctor"))?;
        }
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
        if self.transport_security == HttpTransportSecurity::HttpsOnly && scheme != "https" {
            return Err(invalid_config("http.transport_security"));
        }
        validate_duration(self.timeout, "http.timeout", false)?;
        if let Some(timeout) = self.connect_timeout {
            validate_duration(timeout, "http.connect_timeout", false)?;
        }
        self.timeouts.validate()?;
        if self
            .tcp_keepalive
            .is_some_and(|duration| duration.is_zero())
        {
            return Err(invalid_config("tcp_keepalive"));
        }
        if self.max_response_bytes == 0 {
            return Err(invalid_config("max_response_bytes"));
        }
        validate_queue_capacity(self.request_queue_capacity, "request_queue_capacity")?;
        validate_queue_capacity(self.response_queue_capacity, "response_queue_capacity")?;
        validate_queue_capacity(self.callback_queue_capacity, "callback_queue_capacity")?;
        #[cfg(feature = "http-client-cookies")]
        self.cookie_limits.validate()?;
        #[cfg(feature = "http-client-http2")]
        {
            self.http2.validate()?;
            if self.http_version_policy == HttpVersionPolicy::Http2Only
                && self.base_url.scheme() != "https"
            {
                return Err(invalid_config("http_version_policy"));
            }
        }
        validate_headers(&self.common_headers)
    }

    /// Merge request headers over the client-wide headers.
    ///
    /// Each request header name replaces all common values for that name.
    /// Every request value is retained in its original per-name order, including
    /// repeated or empty values. Unrelated common headers are preserved, and
    /// neither input is modified.
    pub fn merge_headers(&self, request_headers: &HeaderMap) -> Result<HeaderMap, NetError> {
        self.validate()?;
        validate_headers(request_headers)?;
        let mut merged = self.common_headers.clone();
        for name in request_headers.keys() {
            merged.remove(name);
            for value in request_headers.get_all(name) {
                merged.append(name.clone(), value.clone());
            }
        }
        Ok(merged)
    }
}

impl fmt::Debug for HttpClientConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let header_names: Vec<&str> = self.common_headers.keys().map(HeaderName::as_str).collect();
        let mut debug = formatter.debug_struct("HttpClientConfig");
        debug
            // Keep query strings, credentials, and fragments out of logs.  A URL may contain
            // a signed token even though request headers are otherwise redacted.
            .field("base_scheme", &self.base_url.scheme())
            .field("base_has_host", &self.base_url.host_str().is_some())
            .field("common_header_names", &header_names)
            .field("timeout", &self.timeout)
            .field("connect_timeout", &self.connect_timeout)
            .field("timeouts", &self.timeouts)
            .field("admission_policy", &self.admission_policy)
            .field("redirect_policy", &self.redirect_policy)
            .field("transport_security", &self.transport_security)
            .field("tcp_keepalive", &self.tcp_keepalive)
            .field("max_response_bytes", &self.max_response_bytes)
            .field("request_queue_capacity", &self.request_queue_capacity)
            .field("response_queue_capacity", &self.response_queue_capacity)
            .field("callback_queue_capacity", &self.callback_queue_capacity)
            .field("default_retry_policy", &self.default_retry_policy)
            .field("tls", &self.tls)
            .field("proxy_configured", &self.proxy.is_some())
            .field("environment_proxy", &self.environment_proxy);
        #[cfg(feature = "http-client-cookies")]
        debug
            .field("cookie_store_configured", &self.cookie_store.is_some())
            .field("cookie_limits", &self.cookie_limits);
        #[cfg(feature = "http-client-auth")]
        debug.field("auth_configured", &self.auth.is_some());
        #[cfg(feature = "http-client-http2")]
        debug
            .field("http_version_policy", &self.http_version_policy)
            .field("http2", &self.http2);
        #[cfg(feature = "http-client-middleware")]
        debug.field("middleware_count", &self.middlewares.len());
        debug.finish()
    }
}

impl HttpClientConfig {
    pub(crate) fn tls_alpn_protocols(&self) -> Vec<Vec<u8>> {
        #[cfg(feature = "http-client-http2")]
        match self.http_version_policy {
            HttpVersionPolicy::Auto => return vec![b"h2".to_vec(), b"http/1.1".to_vec()],
            HttpVersionPolicy::Http2Only => return vec![b"h2".to_vec()],
            HttpVersionPolicy::Http1Only => {}
        }
        vec![b"http/1.1".to_vec()]
    }

    #[cfg(feature = "http-client-http2")]
    pub(crate) fn http_version_policy(&self) -> HttpVersionPolicy {
        self.http_version_policy
    }

    #[cfg(feature = "http-client-http2")]
    pub(crate) fn http2(&self) -> &Http2Config {
        &self.http2
    }

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
            | "proxy-connection"
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

fn validate_queue_capacity(capacity: usize, field: &'static str) -> Result<(), NetError> {
    if capacity == 0 || capacity > Semaphore::MAX_PERMITS {
        return Err(invalid_config(field));
    }
    Ok(())
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
    fn redirect_defaults_are_independent_of_http_version_and_private_in_debug() -> TestResult {
        let config = HttpClientConfig::new("http://api.example")?;
        check(
            config.transport_security() == HttpTransportSecurity::PreventDowngrade
                && config.redirect_policy().is_enabled()
                && config.redirect_policy().max_redirects() == 10,
            "HTTP clients must default to bounded same-origin redirects and no downgrade",
        )?;
        config.validate()?;
        let configured = config
            .with_redirect_policy(
                RedirectPolicy::allow_origins(["https://private-customer.example"])?
                    .with_max_redirects(3)?,
            )
            .clone();
        check(
            configured.redirect_policy().max_redirects() == 3,
            "redirect policy must survive clone",
        )?;
        let debug = format!("{configured:?}");
        check(
            debug.contains("allowed_origin_count: 1")
                && debug.contains("transport_security: PreventDowngrade")
                && !debug.contains("private-customer.example"),
            "configuration Debug must report policy without disclosing destination origins",
        )?;
        check(
            !configured
                .with_redirect_policy(RedirectPolicy::none())
                .redirect_policy()
                .is_enabled(),
            "the last redirect policy must win",
        )
    }

    #[test]
    fn https_only_validates_initial_url_and_public_field_mutations() -> TestResult {
        let error = HttpClientConfig::new("http://api.example")?
            .with_transport_security(HttpTransportSecurity::HttpsOnly)
            .expect_err("HTTPS-only must reject an HTTP base URL");
        check(
            error.kind() == ErrorKind::InvalidConfig
                && error.config_error().map(|detail| detail.field())
                    == Some("http.transport_security"),
            "HTTPS-only must produce a structured configuration error",
        )?;
        let mut configured = HttpClientConfig::new("https://api.example")?
            .with_transport_security(HttpTransportSecurity::HttpsOnly)?;
        configured.validate()?;
        check(
            configured.transport_security() == HttpTransportSecurity::HttpsOnly,
            "configured transport security must be observable",
        )?;
        configured.base_url = reqwest::Url::parse("http://api.example")?;
        check(
            configured.validate().is_err(),
            "public base URL mutation must not bypass HTTPS-only",
        )?;
        configured
            .with_transport_security(HttpTransportSecurity::PreventDowngrade)?
            .validate()?;
        Ok(())
    }

    #[test]
    fn admission_policy_defaults_to_wait_and_preserves_explicit_configuration() -> TestResult {
        use crate::api::http::HttpAdmissionPolicy;

        let default = HttpClientConfig::new("https://example.com")?;
        check(
            default.admission_policy() == HttpAdmissionPolicy::Wait
                && HttpAdmissionPolicy::default() == HttpAdmissionPolicy::Wait,
            "client admission policy must default to Wait",
        )?;
        let configured = default.with_admission_policy(HttpAdmissionPolicy::FailFast);
        let cloned = configured.clone();
        check(
            configured.admission_policy() == HttpAdmissionPolicy::FailFast
                && cloned.admission_policy() == HttpAdmissionPolicy::FailFast,
            "explicit admission policy must survive clone",
        )?;
        let debug = format!("{cloned:?}");
        check(
            debug.contains("admission_policy: FailFast"),
            "client debug must include the admission policy",
        )?;
        check(
            configured
                .with_admission_policy(HttpAdmissionPolicy::Wait)
                .admission_policy()
                == HttpAdmissionPolicy::Wait,
            "the last client admission policy must win",
        )
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
        let ca = include_bytes!("../../test_fixtures/network/ca.pem");
        let other_ca = include_bytes!("../../test_fixtures/network/other-ca.pem");
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
    fn compiled_roots_retain_every_certificate_in_each_bundle() -> TestResult {
        let ca = include_bytes!("../../test_fixtures/network/ca.pem");
        let other_ca = include_bytes!("../../test_fixtures/network/other-ca.pem");
        let bundle = [ca.as_slice(), other_ca.as_slice()].concat();
        let replaced = HttpTlsConfig::default().with_root_certificates(&bundle, true)?;
        let replaced_roots = replaced.compile_roots()?;
        check(
            replaced_roots.len() == 2,
            "PEM bundle lost a root certificate",
        )?;
        let appended = HttpTlsConfig::default().with_root_certificates(&bundle, false)?;
        let appended_roots = appended.compile_roots()?;
        check(
            appended_roots.len() == webpki_roots::TLS_SERVER_ROOTS.len() + 2,
            "Append did not retain both built-in and custom roots",
        )?;
        check(
            replaced_roots
                .roots
                .iter()
                .all(|root| appended_roots.roots.contains(root)),
            "Append lost a custom trust anchor",
        )?;
        let additional = replaced.with_additional_root_certificate(ca)?;
        check(
            additional.compile_roots()?.len() == 3,
            "adding another bundle lost the existing trust anchors",
        )
    }

    #[test]
    fn compiled_http_tls_uses_selected_provider_and_enables_sni() -> TestResult {
        let provider = Arc::new(rustls_openssl::default_provider());
        let alpn = vec![b"http/1.1".to_vec()];
        let configured = HttpTlsConfig::default().with_crypto_provider(Arc::clone(&provider));
        check(
            configured
                .crypto_provider()
                .is_some_and(|configured| Arc::ptr_eq(configured, &provider)),
            "explicit HTTP TLS provider was not retained",
        )?;
        let debug = format!("{configured:?}");
        check(
            debug.contains("crypto_provider_configured: true") && !debug.contains("cipher_suites"),
            "HTTP TLS Debug must report only whether a provider is configured",
        )?;
        let tls = configured.compile(alpn.clone())?;
        check(
            Arc::ptr_eq(&provider, tls.crypto_provider()),
            "compiled HTTP TLS replaced the selected provider",
        )?;
        check(tls.enable_sni, "HTTP TLS must keep SNI enabled")?;
        check(
            tls.alpn_protocols == alpn,
            "HTTP TLS changed the requested ALPN",
        )?;
        check(
            !tls.enable_early_data,
            "HTTP TLS unexpectedly enabled early data",
        )?;
        // Passing ClientConfig itself is essential: reqwest rejects an Arc or
        // a different rustls version through its type-erased TLS boundary.
        reqwest::Client::builder()
            .tls_backend_preconfigured(tls)
            .build()?;
        Ok(())
    }

    #[test]
    fn compiled_http_identity_is_loaded_by_the_selected_provider() -> TestResult {
        #[derive(Debug)]
        struct RejectSigningKeys;
        impl rustls::crypto::KeyProvider for RejectSigningKeys {
            fn load_private_key(
                &self,
                _key: PrivateKeyDer<'static>,
            ) -> Result<Arc<dyn rustls::sign::SigningKey>, rustls::Error> {
                Err(rustls::Error::General("test provider rejected key".into()))
            }
        }
        static REJECT_KEYS: RejectSigningKeys = RejectSigningKeys;
        let certificates = include_bytes!("../../test_fixtures/network/client.pem");
        let key = include_bytes!("../../test_fixtures/network/client-key.pem");
        let configured = HttpTlsConfig::default().with_client_identity_pem(certificates, key)?;
        let clone = configured
            .clone()
            .with_crypto_provider(Arc::new(rustls_openssl::default_provider()));
        check(
            clone
                .compile(vec![b"http/1.1".to_vec()])?
                .client_auth_cert_resolver
                .has_certs(),
            "cloning the configuration lost the client identity",
        )?;
        let mut custom = rustls_openssl::default_provider();
        custom.key_provider = &REJECT_KEYS;
        let provider = Arc::new(custom);
        let result = configured
            .with_crypto_provider(Arc::clone(&provider))
            .compile(vec![b"http/1.1".to_vec()]);
        check(
            result.is_err(),
            "client identity bypassed the selected provider's key loader",
        )?;
        // Setting the provider first must not make the PEM setter load keys.
        // Both setter orders defer key loading to client creation.
        let configured = HttpTlsConfig::default()
            .with_crypto_provider(provider)
            .with_client_identity_pem(certificates, key)?;
        check(
            configured.compile(vec![b"http/1.1".to_vec()]).is_err(),
            "identity setter order changed provider selection",
        )
    }

    #[test]
    fn http_alpn_matches_sdk_feature_and_version_policy() -> TestResult {
        let config = HttpClientConfig::new("https://example.com")?;
        #[cfg(not(feature = "http-client-http2"))]
        check(
            config.tls_alpn_protocols() == vec![b"http/1.1".to_vec()],
            "base HTTP must only advertise HTTP/1.1",
        )?;
        #[cfg(feature = "http-client-http2")]
        {
            check(
                config.tls_alpn_protocols() == vec![b"h2".to_vec(), b"http/1.1".to_vec()],
                "Auto must advertise h2 followed by HTTP/1.1",
            )?;
            check(
                config
                    .clone()
                    .with_http_version_policy(HttpVersionPolicy::Http1Only)
                    .tls_alpn_protocols()
                    == vec![b"http/1.1".to_vec()],
                "Http1Only advertised another protocol",
            )?;
            check(
                config
                    .with_http_version_policy(HttpVersionPolicy::Http2Only)
                    .tls_alpn_protocols()
                    == vec![b"h2".to_vec()],
                "Http2Only advertised another protocol",
            )?;
        }
        Ok(())
    }

    #[test]
    fn rejects_malformed_pem_and_defers_key_matching_to_client_creation() -> TestResult {
        let certificates = include_bytes!("../../test_fixtures/network/client.pem");
        let private_key = include_bytes!("../../test_fixtures/network/client-key.pem");
        let valid = HttpTlsConfig::default().with_client_identity_pem(certificates, private_key)?;
        check(
            format!("{valid:?}").contains("client_identity_configured: true"),
            "valid client identity was not retained",
        )?;
        let wrong_key = include_bytes!("../../test_fixtures/network/other-client-key.pem");
        let configured = HttpTlsConfig::default()
            .with_client_identity_pem(certificates, wrong_key)?
            .with_crypto_provider(Arc::new(rustls_openssl::default_provider()));
        let result = configured.compile(vec![b"http/1.1".to_vec()]);
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

//! Immutable network policy used as an engine default or overridden per client.
//!
//! The current release applies this policy to WebSocket clients; it does not create HTTP executors.
//! Credentials and certificates are supplied in memory. A client-specific
//! policy replaces engine defaults, including TLS settings, and remains
//! immutable for that client's lifetime.

use crate::api::tls::CryptoProvider;
use crate::error::NetError;
use base64::Engine;
use rustls::client::Resumption;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::sign::{CertifiedKey, SingleCertAndKey};
use std::fmt;
use std::sync::Arc;
use tokio_tungstenite::tungstenite::http::Uri;

const MAX_PEM_BYTES: usize = 1024 * 1024;
const MAX_CERTIFICATES: usize = 128;

/// Proxy, TLS, and network-status policy; defaults to direct connections and WebPKI roots.
///
/// Use [`crate::OpenNet::new_with_network_config`] for the engine default or
/// [`crate::OpenNet::create_ws_client_with_network_config`] for a client override.
/// Passing `NetworkConfig::default()` explicitly selects direct connections and
/// the built-in trust roots; clone an existing value when changing one setting.
#[derive(Clone, Debug, Default)]
pub struct NetworkConfig {
    pub(crate) proxy: ProxyConfig,
    pub(crate) tls: TlsConfig,
    pub(crate) network_status_policy: NetworkStatusPolicy,
}

impl NetworkConfig {
    /// Replaces the proxy policy.
    pub fn with_proxy(mut self, proxy: ProxyConfig) -> Self {
        self.proxy = proxy;
        self
    }

    /// Replaces the target WSS TLS policy. HTTPS proxy TLS is configured
    /// independently on [`ProxyConfig::https_connect`].
    pub fn with_tls(mut self, tls: TlsConfig) -> Self {
        self.tls = tls;
        self
    }

    /// Selects how connections respond to network-monitor state changes.
    /// The default is [`NetworkStatusPolicy::Ignore`].
    pub fn with_network_status_policy(mut self, policy: NetworkStatusPolicy) -> Self {
        self.network_status_policy = policy;
        self
    }

    /// Returns the current proxy policy by reference without exposing credentials.
    pub fn proxy(&self) -> &ProxyConfig {
        &self.proxy
    }

    /// Returns the target WSS TLS policy by reference.
    pub fn tls(&self) -> &TlsConfig {
        &self.tls
    }

    /// Returns the policy used when monitor reachability changes.
    pub fn network_status_policy(&self) -> NetworkStatusPolicy {
        self.network_status_policy
    }

    /// Validates this configuration without creating connections, runtimes, or
    /// background tasks. Trust roots and client identity are checked here;
    /// policy interactions with reconnect budgets are checked by the connector.
    pub fn validate(&self) -> Result<(), NetError> {
        self.proxy.validate()?;
        self.tls.validate_for(TlsRole::Target)
    }
}

/// Controls whether local reachability participates in WebSocket connection control.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum NetworkStatusPolicy {
    /// Preserve existing behavior and rely on heartbeats and transport errors.
    #[default]
    Ignore,
    /// Invalidate active connections and pause attempts while the monitor reports
    /// unavailable, retaining eligible connection intent for later recovery.
    /// Recovery remains bounded by the reconnect policy's elapsed-time budget;
    /// connections using this policy must configure
    /// `ReconnectPolicy::max_elapsed` as `Some(...)`.
    PauseOnUnavailable,
}

/// Explicit proxy routing. Defaults to direct connections and never reads proxy environment variables.
#[derive(Clone, Default)]
pub struct ProxyConfig {
    pub(crate) endpoint: Option<ProxyEndpoint>,
}

#[derive(Clone)]
pub(crate) struct ProxyEndpoint {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) auth: Option<ProxyBasicAuth>,
    pub(crate) transport: ProxyTransport,
    pub(crate) auth_transport_policy: ProxyAuthTransportPolicy,
    normalized_url: String,
}

#[derive(Clone)]
pub(crate) enum ProxyTransport {
    Http,
    Https(TlsConfig),
}

/// Controls whether proxy credentials may be sent without TLS to the proxy.
///
/// Target `wss` encryption starts after CONNECT and does not protect HTTP proxy
/// authentication. Localhost and private-network proxies receive no exemption.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum ProxyAuthTransportPolicy {
    /// Reject authentication unless the connection to the proxy uses TLS.
    #[default]
    RequireTls,
    /// Explicitly permit Basic credentials on a plaintext HTTP connection.
    AllowPlaintext,
}

impl ProxyConfig {
    /// Selects direct connections without consulting proxy environment variables.
    pub fn direct() -> Self {
        Self::default()
    }

    /// Routes `ws` and `wss` targets through an HTTP CONNECT tunnel.
    ///
    /// Accepts only `http://host[:port]`; credentials must be supplied separately.
    /// Authentication is rejected before any network activity. Use
    /// [`Self::https_connect`] to protect credentials, or explicitly opt in with
    /// [`Self::http_connect_with_auth_policy`] and
    /// [`ProxyAuthTransportPolicy::AllowPlaintext`].
    pub fn http_connect(url: &str, auth: Option<ProxyBasicAuth>) -> Result<Self, NetError> {
        Self::http_connect_with_auth_policy(url, auth, ProxyAuthTransportPolicy::RequireTls)
    }

    /// Routes targets through an HTTP CONNECT tunnel using the explicit
    /// authentication transport policy.
    ///
    /// `AllowPlaintext` permits credentials to be sent without encryption,
    /// including when the target uses `wss`. This is an explicit compatibility
    /// exception; prefer [`Self::https_connect`].
    pub fn http_connect_with_auth_policy(
        url: &str,
        auth: Option<ProxyBasicAuth>,
        policy: ProxyAuthTransportPolicy,
    ) -> Result<Self, NetError> {
        Self::connect(url, auth, ProxyTransport::Http, policy)
    }

    /// Establishes TLS to an HTTPS proxy before sending HTTP/1.1 CONNECT.
    ///
    /// Accepts only `https://host[:port]`, with port 443 by default. Proxy
    /// certificates are validated against the URL host using `proxy_tls`.
    /// Proxy roots, client identity, and explicit provider are independent of
    /// [`NetworkConfig::with_tls`]. Failure never falls back to plaintext.
    /// For `ws` targets, only the client-to-proxy connection is encrypted.
    pub fn https_connect(
        url: &str,
        auth: Option<ProxyBasicAuth>,
        proxy_tls: TlsConfig,
    ) -> Result<Self, NetError> {
        Self::connect(
            url,
            auth,
            ProxyTransport::Https(proxy_tls),
            ProxyAuthTransportPolicy::RequireTls,
        )
    }

    fn connect(
        url: &str,
        auth: Option<ProxyBasicAuth>,
        transport: ProxyTransport,
        auth_transport_policy: ProxyAuthTransportPolicy,
    ) -> Result<Self, NetError> {
        let (scheme, default_port) = match &transport {
            ProxyTransport::Http => ("http", 80),
            ProxyTransport::Https(_) => ("https", 443),
        };
        let invalid_url = || {
            NetError::config(
                "proxy.url",
                "requires a CONNECT URL with the constructor's scheme, a nonzero port, and no credentials, path, query, or fragment",
            )
        };
        if url.len() > 2048 || url.contains('#') {
            return Err(invalid_url());
        }
        let uri: Uri = url.parse().map_err(|_| invalid_url())?;
        let authority = uri.authority().ok_or_else(invalid_url)?;
        if uri.scheme_str() != Some(scheme)
            || authority.as_str().contains('@')
            || uri
                .path_and_query()
                .is_some_and(|path| path.as_str() != "/")
        {
            return Err(invalid_url());
        }
        let host = authority.host();
        if normalize_host(host).is_empty() {
            return Err(invalid_url());
        }
        if host.starts_with('[') && normalize_host(host).parse::<std::net::Ipv6Addr>().is_err() {
            return Err(invalid_url());
        }
        // Authority::port() is also None for malformed numeric ports; distinguish
        // an omitted port from an explicitly invalid one before applying a default.
        let suffix = authority
            .as_str()
            .strip_prefix(host)
            .ok_or_else(invalid_url)?;
        let port = match suffix {
            "" => default_port,
            value => {
                let digits = value.strip_prefix(':').ok_or_else(invalid_url)?;
                if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
                    return Err(invalid_url());
                }
                digits.parse::<u16>().map_err(|_| invalid_url())?
            }
        };
        if port == 0 {
            return Err(invalid_url());
        }
        let config = Self {
            endpoint: Some(ProxyEndpoint {
                host: normalize_host(host).to_owned(),
                port,
                auth,
                transport,
                auth_transport_policy,
                normalized_url: format!("{scheme}://{}:{port}", host.to_ascii_lowercase()),
            }),
        };
        config.validate_auth_transport()?;
        if let Some(tls) = config.proxy_tls() {
            tls.validate_material_for(TlsRole::Proxy)?;
        }
        Ok(config)
    }

    pub(crate) fn validate(&self) -> Result<(), NetError> {
        self.validate_auth_transport()?;
        if let Some(tls) = self.proxy_tls() {
            tls.validate_for(TlsRole::Proxy)?;
        }
        Ok(())
    }

    /// Recheck the credential policy without loading certificates or private keys.
    /// Safe to call immediately before each connection attempt.
    pub(crate) fn validate_auth_transport(&self) -> Result<(), NetError> {
        let Some(proxy) = &self.endpoint else {
            return Ok(());
        };
        if proxy.host.is_empty() || proxy.port == 0 {
            return Err(NetError::config(
                "proxy.url",
                "requires a host and nonzero port",
            ));
        }
        match &proxy.transport {
            ProxyTransport::Http
                if proxy.auth.is_some()
                    && proxy.auth_transport_policy == ProxyAuthTransportPolicy::RequireTls =>
            {
                Err(NetError::config(
                    "proxy.auth_transport_policy",
                    "Basic authentication requires HTTPS; plaintext HTTP requires explicit AllowPlaintext",
                ))
            }
            ProxyTransport::Http | ProxyTransport::Https(_) => Ok(()),
        }
    }

    /// Returns `true` when connections bypass a proxy.
    pub fn is_direct(&self) -> bool {
        self.endpoint.is_none()
    }

    /// Returns the credential-free normalized HTTP or HTTPS proxy URL, or `None`
    /// for direct mode.
    pub fn endpoint(&self) -> Option<&str> {
        self.endpoint
            .as_ref()
            .map(|proxy| proxy.normalized_url.as_str())
    }

    /// Returns whether proxy credentials are configured.
    pub fn is_authenticated(&self) -> bool {
        self.endpoint
            .as_ref()
            .is_some_and(|proxy| proxy.auth.is_some())
    }

    /// Returns the independent TLS configuration for an HTTPS proxy.
    /// Direct connections and plaintext HTTP proxies return `None`.
    pub fn proxy_tls(&self) -> Option<&TlsConfig> {
        match &self.endpoint.as_ref()?.transport {
            ProxyTransport::Http => None,
            ProxyTransport::Https(tls) => Some(tls),
        }
    }

    /// Returns the configured proxy authentication transport policy, or `None`
    /// for direct connections. HTTPS proxies always require TLS.
    pub fn auth_transport_policy(&self) -> Option<ProxyAuthTransportPolicy> {
        self.endpoint
            .as_ref()
            .map(|proxy| proxy.auth_transport_policy)
    }
}

impl fmt::Debug for ProxyConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProxyConfig")
            .field(
                "mode",
                &match self.endpoint.as_ref().map(|endpoint| &endpoint.transport) {
                    None => "Direct",
                    Some(ProxyTransport::Http) => "HttpConnect",
                    Some(ProxyTransport::Https(_)) => "HttpsConnect",
                },
            )
            .field("auth_transport_policy", &self.auth_transport_policy())
            .field("proxy_tls", &self.proxy_tls())
            .field(
                "authenticated",
                &self
                    .endpoint
                    .as_ref()
                    .is_some_and(|proxy| proxy.auth.is_some()),
            )
            .finish()
    }
}

/// ASCII Basic proxy credentials; `Debug` output always redacts the secret.
#[derive(Clone)]
pub struct ProxyBasicAuth {
    pub(crate) encoded: Arc<str>,
}

impl ProxyBasicAuth {
    /// Rejects non-ASCII/control characters and colons in usernames. Passwords
    /// may contain ordinary colons; each component is limited to 4096 bytes.
    pub fn new(username: &str, password: &str) -> Result<Self, NetError> {
        if username.len() > 4096
            || username.contains(':')
            || username
                .bytes()
                .any(|byte| !byte.is_ascii() || byte.is_ascii_control())
        {
            return Err(NetError::config(
                "proxy.username",
                "must be at most 4096 ASCII bytes without control characters or a colon",
            ));
        }
        if password.len() > 4096
            || password
                .bytes()
                .any(|byte| !byte.is_ascii() || byte.is_ascii_control())
        {
            return Err(NetError::config(
                "proxy.password",
                "must be at most 4096 ASCII bytes without control characters",
            ));
        }
        let credentials = format!("{username}:{password}");
        Ok(Self {
            encoded: base64::engine::general_purpose::STANDARD
                .encode(credentials)
                .into(),
        })
    }
}

impl fmt::Debug for ProxyBasicAuth {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProxyBasicAuth([redacted])")
    }
}

/// Defines how supplied CA certificates combine with built-in WebPKI roots.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum RootCertificateMode {
    #[default]
    /// Append custom roots to the built-in WebPKI roots.
    Append,
    /// Replace built-in roots with the custom set.
    Replace,
}

/// Strict server-certificate validation with optional client authentication.
#[derive(Clone, Default)]
pub struct TlsConfig {
    pub(crate) provider: Option<Arc<CryptoProvider>>,
    pub(crate) roots: Vec<CertificateDer<'static>>,
    pub(crate) root_mode: RootCertificateMode,
    pub(crate) identity: Option<ClientIdentity>,
}

#[derive(Clone, Copy)]
pub(crate) enum TlsRole {
    Target,
    Proxy,
}

impl TlsRole {
    fn field(self, name: &str) -> String {
        match self {
            Self::Target => format!("tls.{name}"),
            Self::Proxy => format!("proxy.tls.{name}"),
        }
    }
}

impl fmt::Debug for TlsConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TlsConfig")
            .field("crypto_provider_configured", &self.provider.is_some())
            .field("custom_certificate_count", &self.roots.len())
            .field("root_mode", &self.root_mode)
            .field("client_identity_configured", &self.identity.is_some())
            .finish()
    }
}

impl TlsConfig {
    /// Selects this TLS layer's rustls cryptography without changing global state.
    ///
    /// Takes precedence over the application's process default.
    /// The provider also loads and validates this layer's private key when the
    /// network configuration is validated or compiled. Proxy and target TLS do
    /// not inherit each other's explicit provider or client identity.
    pub fn with_crypto_provider(mut self, provider: Arc<CryptoProvider>) -> Self {
        self.provider = Some(provider);
        self
    }

    /// Returns the explicitly selected provider, if any, without resolving defaults.
    pub fn crypto_provider(&self) -> Option<&Arc<CryptoProvider>> {
        self.provider.as_ref()
    }

    /// Replaces the custom CA bundle and selects its trust mode. Accepts one to
    /// 128 PEM certificates with a total size of at most 1 MiB; malformed,
    /// mismatched, or trailing non-whitespace blocks are rejected.
    pub fn with_root_certificates(
        mut self,
        pem: impl AsRef<[u8]>,
        mode: RootCertificateMode,
    ) -> Result<Self, NetError> {
        self.roots = parse_certificates(pem.as_ref(), "tls.root_certificates")?;
        self.root_mode = mode;
        Ok(self)
    }

    /// Configures a client certificate identity.
    pub fn with_client_identity(mut self, identity: ClientIdentity) -> Self {
        self.identity = Some(identity);
        self
    }

    /// Returns how custom roots are combined with built-in roots.
    pub fn root_mode(&self) -> RootCertificateMode {
        self.root_mode
    }

    /// Returns the number of custom roots, excluding built-in WebPKI roots.
    pub fn custom_root_count(&self) -> usize {
        self.roots.len()
    }

    /// Returns the configured client identity, if any.
    pub fn client_identity(&self) -> Option<&ClientIdentity> {
        self.identity.as_ref()
    }

    pub(crate) fn validate_for(&self, role: TlsRole) -> Result<(), NetError> {
        self.validate_material_for(role)?;
        if let Some(identity) = &self.identity {
            let provider = self.provider_for(role)?;
            identity.compile_for(&provider, role)?;
        }
        Ok(())
    }

    fn validate_material_for(&self, role: TlsRole) -> Result<(), NetError> {
        if self.roots.len() > MAX_CERTIFICATES
            || (self.root_mode == RootCertificateMode::Replace && self.roots.is_empty())
        {
            return Err(NetError::config(
                role.field("root_certificates"),
                "replacement roots must be nonempty and bounded",
            ));
        }
        let mut roots = rustls::RootCertStore::empty();
        for certificate in &self.roots {
            roots.add(certificate.clone()).map_err(|_| {
                NetError::config(role.field("root_certificates"), "invalid root certificate")
            })?;
        }
        Ok(())
    }

    fn provider_for(&self, role: TlsRole) -> Result<Arc<CryptoProvider>, NetError> {
        crate::module::tls::select_provider(self.provider.as_ref()).map_err(|_| {
            NetError::config(
                role.field("provider"),
                "configure a rustls CryptoProvider or install an application process default",
            )
        })
    }

    pub(crate) fn compile_for(&self, role: TlsRole) -> Result<Arc<rustls::ClientConfig>, NetError> {
        self.validate_material_for(role)?;
        let provider = self.provider_for(role)?;
        let identity = self
            .identity
            .as_ref()
            .map(|identity| identity.compile_for(&provider, role))
            .transpose()?;
        let mut roots =
            crate::module::tls::root_store(self.root_mode == RootCertificateMode::Append);
        for root in &self.roots {
            roots.add(root.clone()).map_err(|_| {
                NetError::config(role.field("root_certificates"), "invalid root certificate")
            })?;
        }
        let builder = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|_| {
                NetError::config(
                    role.field("provider"),
                    "provider does not support the required TLS protocol versions",
                )
            })?
            .with_root_certificates(roots);
        let mut tls = match identity {
            Some(identity) => {
                builder.with_client_cert_resolver(Arc::new(SingleCertAndKey::from(identity)))
            }
            None => builder.with_no_client_auth(),
        };
        // CONNECT and WebSocket Upgrade use HTTP/1.1. Keep target ALPN behavior
        // unchanged and prevent the proxy from selecting unsupported HTTP/2.
        tls.alpn_protocols = match role {
            TlsRole::Target => Vec::new(),
            TlsRole::Proxy => vec![b"http/1.1".to_vec()],
        };
        tls.resumption = Resumption::disabled();
        tls.enable_early_data = false;
        Ok(Arc::new(tls))
    }
}

/// Parsed certificate material, validated cryptographically by the selected provider.
pub struct ClientIdentity {
    certificates: Vec<CertificateDer<'static>>,
    private_key: PrivatePkcs8KeyDer<'static>,
}

impl Clone for ClientIdentity {
    fn clone(&self) -> Self {
        Self {
            certificates: self.certificates.clone(),
            private_key: self.private_key.clone_key(),
        }
    }
}

impl ClientIdentity {
    /// Loads a PEM certificate chain and exactly one unencrypted PKCS#8 key.
    /// Each input is limited to 1 MiB and the chain to 128 certificates.
    /// This parses the PEM material without selecting cryptography. Key loading
    /// and certificate/key matching happen during [`NetworkConfig::validate`]
    /// and client construction, using the client's final provider.
    pub fn from_pem(
        certificates: impl AsRef<[u8]>,
        private_key: impl AsRef<[u8]>,
    ) -> Result<Self, NetError> {
        let certificates =
            parse_certificates(certificates.as_ref(), "tls.client_identity.certificates")?;
        let blocks = strict_pem_blocks(
            private_key.as_ref(),
            "PRIVATE KEY",
            "tls.client_identity.private_key",
        )?;
        if blocks.len() != 1 {
            return Err(NetError::config(
                "tls.client_identity.private_key",
                "requires one valid PKCS#8 private key matching the certificate",
            ));
        }
        let block = blocks.first().ok_or(NetError::config(
            "tls.client_identity.private_key",
            "requires one valid PKCS#8 private key matching the certificate",
        ))?;
        let key = PrivatePkcs8KeyDer::from_pem_slice(block).map_err(|_| {
            NetError::config(
                "tls.client_identity.private_key",
                "requires one valid PKCS#8 private key matching the certificate",
            )
        })?;
        Ok(Self {
            certificates,
            private_key: key,
        })
    }

    fn compile_for(
        &self,
        provider: &CryptoProvider,
        role: TlsRole,
    ) -> Result<Arc<CertifiedKey>, NetError> {
        let certified_key = CertifiedKey::from_der(
            self.certificates.clone(),
            PrivateKeyDer::Pkcs8(self.private_key.clone_key()),
            provider,
        )
        .map_err(|_| {
            NetError::config(
                role.field("client_identity.private_key"),
                "requires one valid PKCS#8 private key matching the certificate",
            )
        })?;
        certified_key.keys_match().map_err(|_| {
            NetError::config(
                role.field("client_identity.private_key"),
                "requires one valid PKCS#8 private key matching the certificate",
            )
        })?;
        Ok(Arc::new(certified_key))
    }

    /// Returns the number of certificates in the parsed client chain.
    pub fn certificate_count(&self) -> usize {
        self.certificates.len()
    }
}

impl fmt::Debug for ClientIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClientIdentity")
            .field("certificate_count", &self.certificates.len())
            .field("private_key", &"[redacted]")
            .finish()
    }
}

pub(crate) fn normalize_host(host: &str) -> &str {
    host.strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host)
}

fn parse_certificates(pem: &[u8], field: &str) -> Result<Vec<CertificateDer<'static>>, NetError> {
    let blocks = strict_pem_blocks(pem, "CERTIFICATE", field)?;
    let mut certificates = Vec::new();
    certificates
        .try_reserve(blocks.len())
        .map_err(|_| NetError::config(field, "requires a nonempty bounded PEM bundle containing only valid blocks of the requested type"))?;
    let mut validation = rustls::RootCertStore::empty();
    for block in blocks {
        let cert = CertificateDer::from_pem_slice(block).map_err(|_| NetError::config(field, "requires a nonempty bounded PEM bundle containing only valid blocks of the requested type"))?;
        validation
            .add(cert.clone())
            .map_err(|_| NetError::config(field, "requires a nonempty bounded PEM bundle containing only valid blocks of the requested type"))?;
        certificates.push(cert);
    }
    Ok(certificates)
}

fn strict_pem_blocks<'a>(
    pem: &'a [u8],
    label: &str,
    field: &str,
) -> Result<Vec<&'a [u8]>, NetError> {
    if pem.is_empty() || pem.len() > MAX_PEM_BYTES {
        return Err(NetError::config(field, "requires a nonempty bounded PEM bundle containing only valid blocks of the requested type"));
    }
    let mut remaining = std::str::from_utf8(pem)
        .map_err(|_| NetError::config(field, "requires a nonempty bounded PEM bundle containing only valid blocks of the requested type"))?
        .trim();
    let begin = format!("-----BEGIN {label}-----");
    let end = format!("-----END {label}-----");
    let mut blocks = Vec::new();
    while !remaining.is_empty() {
        if blocks.len() == MAX_CERTIFICATES || !remaining.starts_with(&begin) {
            return Err(NetError::config(field, "requires a nonempty bounded PEM bundle containing only valid blocks of the requested type"));
        }
        let boundary = remaining
            .find(&end)
            .and_then(|index| index.checked_add(end.len()))
            .ok_or(NetError::config(field, "requires a nonempty bounded PEM bundle containing only valid blocks of the requested type"))?;
        let block = remaining.get(..boundary).ok_or(NetError::config(field, "requires a nonempty bounded PEM bundle containing only valid blocks of the requested type"))?;
        if block
            .get(begin.len()..)
            .ok_or(NetError::config(field, "requires a nonempty bounded PEM bundle containing only valid blocks of the requested type"))?
            .contains("-----BEGIN ")
        {
            return Err(NetError::config(field, "requires a nonempty bounded PEM bundle containing only valid blocks of the requested type"));
        }
        blocks.try_reserve(1).map_err(|_| NetError::config(field, "requires a nonempty bounded PEM bundle containing only valid blocks of the requested type"))?;
        blocks.push(block.as_bytes());
        remaining = remaining
            .get(boundary..)
            .ok_or(NetError::config(field, "requires a nonempty bounded PEM bundle containing only valid blocks of the requested type"))?
            .trim();
    }
    if blocks.is_empty() {
        return Err(NetError::config(field, "requires a nonempty bounded PEM bundle containing only valid blocks of the requested type"));
    }
    Ok(blocks)
}

#[cfg(test)]
mod tests {
    use super::*;

    type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

    #[test]
    fn network_validation_rechecks_plaintext_authentication_policy() -> TestResult {
        let mut proxy = ProxyConfig::http_connect("http://localhost:8080", None)?;
        let endpoint = proxy.endpoint.as_mut().ok_or("proxy endpoint missing")?;
        // Model an internal caller bypassing the checked public constructor.
        endpoint.auth = Some(ProxyBasicAuth::new("test-user", "test-password")?);
        let invalid = NetworkConfig::default().with_proxy(proxy);
        let error = invalid
            .validate()
            .err()
            .ok_or("network validation accepted plaintext authentication")?;
        assert_eq!(
            error.config_error().map(|detail| detail.field()),
            Some("proxy.auth_transport_policy")
        );
        Ok(())
    }

    #[test]
    fn tls_validation_and_compilation_report_their_own_role() -> TestResult {
        let tls = TlsConfig {
            root_mode: RootCertificateMode::Replace,
            ..TlsConfig::default()
        };
        for (role, field) in [
            (TlsRole::Target, "tls.root_certificates"),
            (TlsRole::Proxy, "proxy.tls.root_certificates"),
        ] {
            for result in [tls.validate_for(role), tls.compile_for(role).map(|_| ())] {
                let error = result.err().ok_or("empty replacement roots accepted")?;
                assert_eq!(
                    error.config_error().map(|detail| detail.field()),
                    Some(field)
                );
            }
        }
        let mut provider = (*crate::module::tls::local_provider()).clone();
        provider.cipher_suites.clear();
        let tls = TlsConfig::default().with_crypto_provider(Arc::new(provider));
        for (role, field) in [
            (TlsRole::Target, "tls.provider"),
            (TlsRole::Proxy, "proxy.tls.provider"),
        ] {
            let error = tls
                .compile_for(role)
                .err()
                .ok_or("provider without cipher suites accepted")?;
            assert_eq!(
                error.config_error().map(|detail| detail.field()),
                Some(field)
            );
        }
        Ok(())
    }

    #[test]
    fn compiled_tls_layers_keep_independent_providers_and_protocols() -> TestResult {
        let proxy_provider = crate::module::tls::local_provider();
        let target_provider = crate::module::tls::local_provider();
        let proxy = TlsConfig::default()
            .with_crypto_provider(Arc::clone(&proxy_provider))
            .compile_for(TlsRole::Proxy)?;
        let target = TlsConfig::default()
            .with_crypto_provider(Arc::clone(&target_provider))
            .compile_for(TlsRole::Target)?;
        assert!(Arc::ptr_eq(proxy.crypto_provider(), &proxy_provider));
        assert!(Arc::ptr_eq(target.crypto_provider(), &target_provider));
        assert_eq!(proxy.alpn_protocols, vec![b"http/1.1".to_vec()]);
        assert!(target.alpn_protocols.is_empty());
        assert!(!proxy.enable_early_data && !target.enable_early_data);
        assert!(proxy.enable_sni && target.enable_sni);
        Ok(())
    }

    #[test]
    fn proxy_policy_checks_do_not_reload_keys_and_compilation_loads_once() -> TestResult {
        use std::sync::atomic::{AtomicUsize, Ordering};

        static KEY_LOADS: AtomicUsize = AtomicUsize::new(0);
        #[derive(Debug)]
        struct CountKeys;
        impl rustls::crypto::KeyProvider for CountKeys {
            fn load_private_key(
                &self,
                key: PrivateKeyDer<'static>,
            ) -> Result<Arc<dyn rustls::sign::SigningKey>, rustls::Error> {
                KEY_LOADS.fetch_add(1, Ordering::SeqCst);
                rustls_openssl::default_provider()
                    .key_provider
                    .load_private_key(key)
            }
        }
        static COUNT_KEYS: CountKeys = CountKeys;
        let mut provider = rustls_openssl::default_provider();
        provider.key_provider = &COUNT_KEYS;
        let proxy = ProxyConfig::https_connect(
            "https://localhost",
            Some(ProxyBasicAuth::new("test-user", "test-password")?),
            TlsConfig::default()
                .with_crypto_provider(Arc::new(provider))
                .with_client_identity(ClientIdentity::from_pem(
                    include_bytes!("../test_fixtures/network/client.pem"),
                    include_bytes!("../test_fixtures/network/client-key.pem"),
                )?),
        )?;
        proxy.validate_auth_transport()?;
        proxy.validate_auth_transport()?;
        assert_eq!(KEY_LOADS.load(Ordering::SeqCst), 0);
        proxy
            .proxy_tls()
            .ok_or("proxy TLS configuration missing")?
            .compile_for(TlsRole::Proxy)?;
        assert_eq!(KEY_LOADS.load(Ordering::SeqCst), 1);
        Ok(())
    }
}

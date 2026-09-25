//! Immutable network policy used as an engine default or overridden per client.
//!
//! The current release applies this policy to WebSocket clients; it does not create HTTP executors.
//! Credentials and certificates are supplied in memory. A client-specific
//! policy replaces engine defaults, including TLS settings, and remains
//! immutable for that client's lifetime.

use crate::error::NetError;
use base64::Engine;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::sign::CertifiedKey;
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

    /// Replaces the TLS policy.
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

    /// Returns the current TLS policy by reference.
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
        if self
            .proxy
            .endpoint
            .as_ref()
            .is_some_and(|proxy| proxy.host.is_empty() || proxy.port == 0)
        {
            return Err(NetError::config(
                "proxy.url",
                "requires a host and nonzero port",
            ));
        }
        if self.tls.roots.len() > MAX_CERTIFICATES
            || (self.tls.root_mode == RootCertificateMode::Replace && self.tls.roots.is_empty())
        {
            return Err(NetError::config(
                "tls.root_certificates",
                "replacement roots must be nonempty and bounded",
            ));
        }
        let mut roots = rustls::RootCertStore::empty();
        for certificate in &self.tls.roots {
            roots.add(certificate.clone()).map_err(|_| {
                NetError::config("tls.root_certificates", "invalid root certificate")
            })?;
        }
        if let Some(identity) = &self.tls.identity {
            identity.certified_key.keys_match().map_err(|_| {
                NetError::config(
                    "tls.client_identity.private_key",
                    "private key does not match certificate",
                )
            })?;
        }
        Ok(())
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
    normalized_url: String,
}

impl ProxyConfig {
    /// Selects direct connections without consulting proxy environment variables.
    pub fn direct() -> Self {
        Self::default()
    }

    /// Routes `ws` and `wss` targets through an HTTP CONNECT tunnel.
    ///
    /// Accepts only `http://host[:port]`; credentials must be supplied separately.
    /// A plain HTTP proxy does not encrypt Basic credentials, even for `wss` targets.
    pub fn http_connect(url: &str, auth: Option<ProxyBasicAuth>) -> Result<Self, NetError> {
        if url.len() > 2048 || url.contains('#') {
            return Err(NetError::config("proxy.url", "requires a valid HTTP CONNECT URL with a nonzero port and no credentials, path, query, or fragment"));
        }
        let uri: Uri = url.parse().map_err(|_| NetError::config("proxy.url", "requires a valid HTTP CONNECT URL with a nonzero port and no credentials, path, query, or fragment"))?;
        let authority = uri.authority().ok_or(NetError::config("proxy.url", "requires a valid HTTP CONNECT URL with a nonzero port and no credentials, path, query, or fragment"))?;
        if uri.scheme_str() != Some("http")
            || authority.as_str().contains('@')
            || uri
                .path_and_query()
                .is_some_and(|path| path.as_str() != "/")
        {
            return Err(NetError::config("proxy.url", "requires a valid HTTP CONNECT URL with a nonzero port and no credentials, path, query, or fragment"));
        }
        let host = authority.host();
        if normalize_host(host).is_empty() {
            return Err(NetError::config("proxy.url", "requires a valid HTTP CONNECT URL with a nonzero port and no credentials, path, query, or fragment"));
        }
        if host.starts_with('[') && normalize_host(host).parse::<std::net::Ipv6Addr>().is_err() {
            return Err(NetError::config("proxy.url", "requires a valid HTTP CONNECT URL with a nonzero port and no credentials, path, query, or fragment"));
        }
        // Authority::port() is also None for malformed numeric ports; distinguish
        // an omitted port from an explicitly invalid one before applying port 80.
        let suffix = authority
            .as_str()
            .strip_prefix(host)
            .ok_or(NetError::config("proxy.url", "requires a valid HTTP CONNECT URL with a nonzero port and no credentials, path, query, or fragment"))?;
        let port = match suffix {
            "" => 80,
            value => {
                let digits = value.strip_prefix(':').ok_or(NetError::config("proxy.url", "requires a valid HTTP CONNECT URL with a nonzero port and no credentials, path, query, or fragment"))?;
                if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
                    return Err(NetError::config("proxy.url", "requires a valid HTTP CONNECT URL with a nonzero port and no credentials, path, query, or fragment"));
                }
                digits.parse::<u16>().map_err(|_| NetError::config("proxy.url", "requires a valid HTTP CONNECT URL with a nonzero port and no credentials, path, query, or fragment"))?
            }
        };
        if port == 0 {
            return Err(NetError::config("proxy.url", "requires a valid HTTP CONNECT URL with a nonzero port and no credentials, path, query, or fragment"));
        }
        Ok(Self {
            endpoint: Some(ProxyEndpoint {
                host: normalize_host(host).to_owned(),
                port,
                auth,
                normalized_url: format!("http://{}:{port}", host.to_ascii_lowercase()),
            }),
        })
    }

    /// Returns `true` when connections bypass a proxy.
    pub fn is_direct(&self) -> bool {
        self.endpoint.is_none()
    }

    /// Returns the credential-free normalized HTTP proxy URL, or `None` for direct mode.
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
}

impl fmt::Debug for ProxyConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProxyConfig")
            .field(
                "mode",
                &if self.endpoint.is_some() {
                    "HttpConnect"
                } else {
                    "Direct"
                },
            )
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
    pub(crate) roots: Vec<CertificateDer<'static>>,
    pub(crate) root_mode: RootCertificateMode,
    pub(crate) identity: Option<ClientIdentity>,
}

impl fmt::Debug for TlsConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TlsConfig")
            .field("custom_certificate_count", &self.roots.len())
            .field("root_mode", &self.root_mode)
            .field("client_identity_configured", &self.identity.is_some())
            .finish()
    }
}

impl TlsConfig {
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
}

/// A validated client certificate chain and its matching PKCS#8 private key.
#[derive(Clone)]
pub struct ClientIdentity {
    pub(crate) certified_key: Arc<CertifiedKey>,
}

impl ClientIdentity {
    /// Loads a PEM certificate chain and exactly one unencrypted PKCS#8 key.
    /// Each input is limited to 1 MiB and the chain to 128 certificates.
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
        let certified_key = CertifiedKey::from_der(
            certificates,
            PrivateKeyDer::Pkcs8(key),
            &rustls::crypto::aws_lc_rs::default_provider(),
        )
        .map_err(|_| {
            NetError::config(
                "tls.client_identity.private_key",
                "requires one valid PKCS#8 private key matching the certificate",
            )
        })?;
        certified_key.keys_match().map_err(|_| {
            NetError::config(
                "tls.client_identity.private_key",
                "requires one valid PKCS#8 private key matching the certificate",
            )
        })?;
        Ok(Self {
            certified_key: Arc::new(certified_key),
        })
    }

    /// Returns the number of certificates in the validated client chain.
    /// Returns the number of certificates in the validated chain.
    pub fn certificate_count(&self) -> usize {
        self.certified_key.cert.len()
    }
}

impl fmt::Debug for ClientIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClientIdentity")
            .field("certificate_count", &self.certified_key.cert.len())
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

//! 不可变的网络策略，可作为引擎默认配置，也可由单个客户端独立覆盖。
//!
//! 当前版本供 WebSocket 客户端使用，不创建 HTTP 请求执行器。
//! 认证凭据和证书通过内存传入。客户端显式指定策略时，会完整替换引擎默认配置，
//! 包括 TLS 设置；该策略在客户端整个生命周期内保持不变，也不影响其他客户端。

use crate::api::net_error::NetError;
use base64::Engine;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::sign::CertifiedKey;
use std::fmt;
use std::sync::Arc;
use tokio_tungstenite::tungstenite::http::Uri;

const MAX_PEM_BYTES: usize = 1024 * 1024;
const MAX_CERTIFICATES: usize = 128;

/// 代理、TLS 与网络状态策略；默认使用直连和 WebPKI 根证书信任。
///
/// 使用 [`crate::OpenNet::new_with_network_config`] 设置引擎默认策略，
/// 或使用 [`crate::OpenNet::create_ws_client_with_network_config`] 为客户端指定独立策略。
/// 向后者传入 `NetworkConfig::default()` 会显式替换引擎的全部默认配置。
/// 如果只需修改一个设置，应先克隆原有配置。
#[derive(Clone, Debug, Default)]
pub struct NetworkConfig {
    pub(crate) proxy: ProxyConfig,
    pub(crate) tls: TlsConfig,
    pub(crate) network_status_policy: NetworkStatusPolicy,
}

impl NetworkConfig {
    pub fn with_proxy(mut self, proxy: ProxyConfig) -> Self {
        self.proxy = proxy;
        self
    }

    pub fn with_tls(mut self, tls: TlsConfig) -> Self {
        self.tls = tls;
        self
    }

    /// 选择客户端连接如何响应内部网络监控的状态变化。
    ///
    /// 默认为 [`NetworkStatusPolicy::Ignore`]。与代理和 TLS 策略相同，
    /// 此设置包含在引擎默认配置或客户端完整覆盖配置的快照中。
    pub fn with_network_status_policy(mut self, policy: NetworkStatusPolicy) -> Self {
        self.network_status_policy = policy;
        self
    }
}

/// 本地网络可达状态是否参与 WebSocket 连接控制。
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum NetworkStatusPolicy {
    /// 保持现有行为，包括无法访问互联网时的回环连接和局域网连接。
    /// 继续通过心跳和传输错误检测失效连接。
    #[default]
    Ignore,
    /// 确认网络不可用时，使当前连接失效并暂停建连尝试，同时保留仍满足条件的连接意图。
    /// 恢复连接仍受现有重连策略和有限的累计时长预算约束。
    ///
    /// 此全局网络可达信号不保证具体目标可达；仅应为需要随该信号调整连接的目标启用。
    /// 使用此策略的连接必须将 `ReconnectPolicy::max_elapsed` 设置为 `Some`。
    PauseOnUnavailable,
}

/// 显式配置的代理路由。默认不使用代理，也不读取环境变量中的代理配置。
#[derive(Clone, Default)]
pub struct ProxyConfig {
    pub(crate) endpoint: Option<ProxyEndpoint>,
}

#[derive(Clone)]
pub(crate) struct ProxyEndpoint {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) auth: Option<ProxyBasicAuth>,
}

impl ProxyConfig {
    /// 对 `ws` 和 `wss` 目标均使用 HTTP CONNECT 隧道。
    ///
    /// 仅支持 `http://host[:port]`。认证凭据必须单独传入，不能包含在 URL 中。
    /// 即使隧道目标使用 `wss`，普通 HTTP 代理也不会加密 Basic 认证凭据。
    pub fn http_connect(url: &str, auth: Option<ProxyBasicAuth>) -> Result<Self, NetError> {
        if url.len() > 2048 || url.contains('#') {
            return Err(NetError::ConfigError);
        }
        let uri: Uri = url.parse().map_err(|_| NetError::ConfigError)?;
        let authority = uri.authority().ok_or(NetError::ConfigError)?;
        if uri.scheme_str() != Some("http")
            || authority.as_str().contains('@')
            || uri
                .path_and_query()
                .is_some_and(|path| path.as_str() != "/")
        {
            return Err(NetError::ConfigError);
        }
        let host = authority.host();
        if normalize_host(host).is_empty() {
            return Err(NetError::ConfigError);
        }
        if host.starts_with('[') && normalize_host(host).parse::<std::net::Ipv6Addr>().is_err() {
            return Err(NetError::ConfigError);
        }
        // 数字端口无效时，http::Authority::port() 也会返回 None；
        // 使用默认端口前，需要区分未指定端口与指定了无效端口这两种情况。
        let suffix = authority
            .as_str()
            .strip_prefix(host)
            .ok_or(NetError::ConfigError)?;
        let port = match suffix {
            "" => 80,
            value => {
                let digits = value.strip_prefix(':').ok_or(NetError::ConfigError)?;
                if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
                    return Err(NetError::ConfigError);
                }
                digits.parse::<u16>().map_err(|_| NetError::ConfigError)?
            }
        };
        if port == 0 {
            return Err(NetError::ConfigError);
        }
        Ok(Self {
            endpoint: Some(ProxyEndpoint {
                host: normalize_host(host).to_owned(),
                port,
                auth,
            }),
        })
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

/// 使用 ASCII 编码的 Basic 代理认证凭据。`Debug` 输出始终隐藏凭据内容。
#[derive(Clone)]
pub struct ProxyBasicAuth {
    pub(crate) encoded: Arc<str>,
}

impl ProxyBasicAuth {
    /// 拒绝非 ASCII 字符、控制字符以及用户名中的冒号。
    /// 密码允许包含普通冒号。每项输入最多为 4096 字节。
    pub fn new(username: &str, password: &str) -> Result<Self, NetError> {
        if username.len() > 4096
            || password.len() > 4096
            || username.contains(':')
            || username
                .bytes()
                .chain(password.bytes())
                .any(|byte| !byte.is_ascii() || byte.is_ascii_control())
        {
            return Err(NetError::ConfigError);
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

/// 传入的 CA 证书集合如何影响内置 WebPKI 根证书。
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum RootCertificateMode {
    #[default]
    Append,
    Only,
}

/// 严格的服务器证书验证，以及可选的客户端身份认证。
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
    /// 替换已配置的自定义 CA 证书集合，并选择其信任模式。
    ///
    /// 接受 1 至 128 张 PEM 证书，总大小不超过 1 MiB。
    /// 无效或类型不匹配的 PEM 块，以及尾部的非空白内容，都会被拒绝，不会被静默跳过。
    pub fn with_root_certificates(
        mut self,
        pem: impl AsRef<[u8]>,
        mode: RootCertificateMode,
    ) -> Result<Self, NetError> {
        self.roots = parse_certificates(pem.as_ref())?;
        self.root_mode = mode;
        Ok(self)
    }

    pub fn with_client_identity(mut self, identity: ClientIdentity) -> Self {
        self.identity = Some(identity);
        self
    }
}

/// 已验证的客户端证书链及其匹配的 PKCS#8 私钥。
#[derive(Clone)]
pub struct ClientIdentity {
    pub(crate) certified_key: Arc<CertifiedKey>,
}

impl ClientIdentity {
    /// 加载 PEM 证书链，以及恰好一份未加密的 PKCS#8 PEM 私钥。
    /// 每项输入最多为 1 MiB；证书链最多包含 128 张证书。
    pub fn from_pem(
        certificates: impl AsRef<[u8]>,
        private_key: impl AsRef<[u8]>,
    ) -> Result<Self, NetError> {
        let certificates = parse_certificates(certificates.as_ref())?;
        let blocks = strict_pem_blocks(private_key.as_ref(), "PRIVATE KEY")?;
        if blocks.len() != 1 {
            return Err(NetError::ConfigError);
        }
        let block = blocks.first().ok_or(NetError::ConfigError)?;
        let key = PrivatePkcs8KeyDer::from_pem_slice(block).map_err(|_| NetError::ConfigError)?;
        let certified_key = CertifiedKey::from_der(
            certificates,
            PrivateKeyDer::Pkcs8(key),
            &rustls::crypto::aws_lc_rs::default_provider(),
        )
        .map_err(|_| NetError::ConfigError)?;
        certified_key
            .keys_match()
            .map_err(|_| NetError::ConfigError)?;
        Ok(Self {
            certified_key: Arc::new(certified_key),
        })
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

fn parse_certificates(pem: &[u8]) -> Result<Vec<CertificateDer<'static>>, NetError> {
    let blocks = strict_pem_blocks(pem, "CERTIFICATE")?;
    let mut certificates = Vec::new();
    certificates
        .try_reserve(blocks.len())
        .map_err(|_| NetError::ConfigError)?;
    let mut validation = rustls::RootCertStore::empty();
    for block in blocks {
        let cert = CertificateDer::from_pem_slice(block).map_err(|_| NetError::ConfigError)?;
        validation
            .add(cert.clone())
            .map_err(|_| NetError::ConfigError)?;
        certificates.push(cert);
    }
    Ok(certificates)
}

fn strict_pem_blocks<'a>(pem: &'a [u8], label: &str) -> Result<Vec<&'a [u8]>, NetError> {
    if pem.is_empty() || pem.len() > MAX_PEM_BYTES {
        return Err(NetError::ConfigError);
    }
    let mut remaining = std::str::from_utf8(pem)
        .map_err(|_| NetError::ConfigError)?
        .trim();
    let begin = format!("-----BEGIN {label}-----");
    let end = format!("-----END {label}-----");
    let mut blocks = Vec::new();
    while !remaining.is_empty() {
        if blocks.len() == MAX_CERTIFICATES || !remaining.starts_with(&begin) {
            return Err(NetError::ConfigError);
        }
        let boundary = remaining
            .find(&end)
            .and_then(|index| index.checked_add(end.len()))
            .ok_or(NetError::ConfigError)?;
        let block = remaining.get(..boundary).ok_or(NetError::ConfigError)?;
        if block
            .get(begin.len()..)
            .ok_or(NetError::ConfigError)?
            .contains("-----BEGIN ")
        {
            return Err(NetError::ConfigError);
        }
        blocks.try_reserve(1).map_err(|_| NetError::ConfigError)?;
        blocks.push(block.as_bytes());
        remaining = remaining
            .get(boundary..)
            .ok_or(NetError::ConfigError)?
            .trim();
    }
    if blocks.is_empty() {
        return Err(NetError::ConfigError);
    }
    Ok(blocks)
}

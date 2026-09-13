//! Native WebSocket transport with explicitly configured trust and routing.

use crate::api::net_error::NetError;
use crate::api::network_config::{normalize_host, NetworkConfig, NetworkStatusPolicy, ProxyConfig};
use crate::api::wsc::web_socket_connection_event::{
    WebSocketConnectStage, WebSocketConnectionFailure,
};
use rustls::client::Resumption;
use rustls::pki_types::ServerName;
use rustls::sign::SingleCertAndKey;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::time::{timeout_at, Instant};
use tokio_rustls::TlsConnector;
use tokio_tungstenite::tungstenite::handshake::client::Request;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::Error as WsError;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

mod proxy;
#[cfg(test)]
mod proxy_tests;
#[cfg(test)]
mod tests;

pub(crate) type NativeWebSocketStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Clone)]
pub(crate) struct CompiledNetworkConfig {
    proxy: ProxyConfig,
    tls: Arc<rustls::ClientConfig>,
    network_status_policy: NetworkStatusPolicy,
}

impl CompiledNetworkConfig {
    pub(crate) fn new(config: NetworkConfig) -> Result<Self, NetError> {
        let mut roots = rustls::RootCertStore::empty();
        if config.tls.root_mode == crate::api::network_config::RootCertificateMode::Append {
            roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        }
        for root in config.tls.roots {
            roots.add(root).map_err(|_| NetError::ConfigError)?;
        }
        if roots.is_empty() {
            return Err(NetError::ConfigError);
        }
        let builder = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|_| NetError::ConfigError)?
        .with_root_certificates(roots);
        let mut tls = match config.tls.identity {
            Some(identity) => builder.with_client_cert_resolver(Arc::new(SingleCertAndKey::from(
                identity.certified_key,
            ))),
            None => builder.with_no_client_auth(),
        };
        // Keep the original HTTP/1.1 upgrade behavior; do not offer h2/ALPN or
        // introduce cross-connection session resumption by sharing this config.
        tls.alpn_protocols.clear();
        tls.resumption = Resumption::disabled();
        tls.enable_early_data = false;
        Ok(Self {
            proxy: config.proxy,
            tls: Arc::new(tls),
            network_status_policy: config.network_status_policy,
        })
    }

    pub(crate) fn network_status_policy(&self) -> NetworkStatusPolicy {
        self.network_status_policy
    }

    /// Each stage uses the caller's original attempt deadline. Dropping the
    /// future also drops its owned TCP/TLS stream, including a partial tunnel.
    pub(crate) async fn dial(
        &self,
        mut request: Request,
        protocol_config: WebSocketConfig,
        tcp_nodelay: bool,
        deadline: Instant,
    ) -> Result<NativeWebSocketStream, WebSocketConnectionFailure> {
        let tls_required = match request.uri().scheme_str() {
            Some("ws") => false,
            Some("wss") => true,
            _ => {
                return Err(failure(
                    NetError::InvalidUrl,
                    WebSocketConnectStage::RequestBuild,
                    false,
                ));
            }
        };
        let host = request
            .uri()
            .host()
            .map(normalize_host)
            .ok_or_else(|| {
                failure(
                    NetError::InvalidUrl,
                    WebSocketConnectStage::RequestBuild,
                    false,
                )
            })?
            .to_owned();
        let port = request
            .uri()
            .port_u16()
            .unwrap_or(if tls_required { 443 } else { 80 });
        let (connect_host, connect_port) = match &self.proxy.endpoint {
            Some(proxy) => (proxy.host.as_str(), proxy.port),
            None => (host.as_str(), port),
        };
        let mut socket = connect_tcp(connect_host, connect_port, deadline).await?;
        if tcp_nodelay {
            socket
                .set_nodelay(true)
                .map_err(|_| failure(NetError::NetworkError, WebSocketConnectStage::Tcp, true))?;
        }
        if let Some(proxy) = &self.proxy.endpoint {
            ensure_deadline(deadline, WebSocketConnectStage::ProxyConnect)?;
            timeout_at(
                deadline,
                proxy::establish_tunnel(&mut socket, &host, port, proxy.auth.as_ref()),
            )
            .await
            .map_err(|_| timeout(WebSocketConnectStage::ProxyConnect))??;
            // Proxy credentials belong to the CONNECT exchange only, even if a
            // caller supplied a Proxy-Authorization header among Upgrade headers.
            request
                .headers_mut()
                .remove(tokio_tungstenite::tungstenite::http::header::PROXY_AUTHORIZATION);
        }
        let stream = if tls_required {
            ensure_deadline(deadline, WebSocketConnectStage::Tls)?;
            let name = ServerName::try_from(host)
                .map_err(|_| failure(NetError::InvalidUrl, WebSocketConnectStage::Tls, false))?;
            let tls = timeout_at(
                deadline,
                TlsConnector::from(Arc::clone(&self.tls)).connect(name, socket),
            )
            .await
            .map_err(|_| timeout(WebSocketConnectStage::Tls))?
            .map_err(|error| classify_tls_io(&error, WebSocketConnectStage::Tls))?;
            MaybeTlsStream::Rustls(tls)
        } else {
            MaybeTlsStream::Plain(socket)
        };
        ensure_deadline(deadline, WebSocketConnectStage::WebSocketUpgrade)?;
        let (stream, _) = timeout_at(
            deadline,
            tokio_tungstenite::client_async_with_config(request, stream, Some(protocol_config)),
        )
        .await
        .map_err(|_| timeout(WebSocketConnectStage::WebSocketUpgrade))?
        .map_err(classify_upgrade_error)?;
        Ok(stream)
    }
}

async fn connect_tcp(
    host: &str,
    port: u16,
    deadline: Instant,
) -> Result<TcpStream, WebSocketConnectionFailure> {
    resolve_and_connect(
        tokio::net::lookup_host((host, port)),
        TcpStream::connect,
        deadline,
    )
    .await
}

/// The same resolver/connector path is exercised with injected pending futures
/// in unit tests, so DNS and TCP timeout tests never depend on an external host.
async fn resolve_and_connect<R, A, C, F>(
    resolver: R,
    mut connector: C,
    deadline: Instant,
) -> Result<TcpStream, WebSocketConnectionFailure>
where
    R: std::future::Future<Output = std::io::Result<A>>,
    A: IntoIterator<Item = std::net::SocketAddr>,
    C: FnMut(std::net::SocketAddr) -> F,
    F: std::future::Future<Output = std::io::Result<TcpStream>>,
{
    ensure_deadline(deadline, WebSocketConnectStage::Dns)?;
    let addresses = timeout_at(deadline, resolver)
        .await
        .map_err(|_| timeout(WebSocketConnectStage::Dns))?
        .map_err(|_| failure(NetError::NetworkError, WebSocketConnectStage::Dns, true))?;
    let mut had_address = false;
    for address in addresses {
        had_address = true;
        ensure_deadline(deadline, WebSocketConnectStage::Tcp)?;
        match timeout_at(deadline, connector(address)).await {
            Ok(Ok(socket)) => return Ok(socket),
            Ok(Err(_)) => continue,
            Err(_) => return Err(timeout(WebSocketConnectStage::Tcp)),
        }
    }
    Err(failure(
        NetError::NetworkError,
        if had_address {
            WebSocketConnectStage::Tcp
        } else {
            WebSocketConnectStage::Dns
        },
        true,
    ))
}

fn classify_tls_io(
    error: &std::io::Error,
    stage: WebSocketConnectStage,
) -> WebSocketConnectionFailure {
    if error
        .get_ref()
        .is_some_and(|source| source.is::<rustls::Error>())
        || (stage == WebSocketConnectStage::Tls && error.kind() == std::io::ErrorKind::InvalidData)
    {
        // rustls protocol/certificate alerts can arrive when Upgrade first reads
        // a TLS 1.3 server's rejection of the client's certificate.
        failure(NetError::TlsConnectError, WebSocketConnectStage::Tls, false)
    } else {
        failure(NetError::NetworkError, stage, true)
    }
}

pub(crate) fn classify_upgrade_error(error: WsError) -> WebSocketConnectionFailure {
    let stage = WebSocketConnectStage::WebSocketUpgrade;
    match error {
        WsError::Http(response) => {
            let code = response.status().as_u16();
            WebSocketConnectionFailure::new(
                NetError::ConnectError,
                stage,
                Some(code),
                response.status().is_server_error() || code == 408 || code == 429,
            )
        }
        WsError::Io(error) => classify_tls_io(&error, stage),
        error => {
            let retryable = matches!(error, WsError::ConnectionClosed | WsError::AlreadyClosed);
            failure(NetError::from(error), stage, retryable)
        }
    }
}

fn timeout(stage: WebSocketConnectStage) -> WebSocketConnectionFailure {
    failure(NetError::TimeoutError, stage, true)
}

fn ensure_deadline(
    deadline: Instant,
    stage: WebSocketConnectStage,
) -> Result<(), WebSocketConnectionFailure> {
    if Instant::now() >= deadline {
        Err(timeout(stage))
    } else {
        Ok(())
    }
}

fn failure(
    error: NetError,
    stage: WebSocketConnectStage,
    retryable: bool,
) -> WebSocketConnectionFailure {
    WebSocketConnectionFailure::new(error, stage, None, retryable)
}

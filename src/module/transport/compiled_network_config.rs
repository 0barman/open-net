use crate::api::network_config::normalize_host;
use crate::module::transport::failure::ConnectStage;
#[cfg(test)]
use crate::module::transport::failure::ConnectionFailure;
use crate::module::transport::{
    classify_tls_io_with_diagnostics, classify_upgrade_error_with_diagnostics, connect_tcp,
    ensure_deadline, failure, proxy, timeout, NativeWebSocketStream, TransportFailure,
};
use crate::{NetError, NetworkConfig, NetworkStatusPolicy, ProxyConfig};
use rustls::client::Resumption;
use rustls::pki_types::ServerName;
use rustls::sign::SingleCertAndKey;
use std::sync::Arc;
use tokio::time::{timeout_at, Instant};
use tokio_rustls::TlsConnector;
use tokio_tungstenite::tungstenite::handshake::client::Request;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::MaybeTlsStream;

#[derive(Clone)]
pub(crate) struct CompiledNetworkConfig {
    original: Arc<NetworkConfig>,
    pub(crate) proxy: ProxyConfig,
    pub(crate) tls: Arc<rustls::ClientConfig>,
    pub(crate) network_status_policy: NetworkStatusPolicy,
}

impl CompiledNetworkConfig {
    pub(crate) fn new(config: NetworkConfig) -> Result<Self, NetError> {
        let original = Arc::new(config.clone());
        let mut roots = rustls::RootCertStore::empty();
        if config.tls.root_mode == crate::api::network_config::RootCertificateMode::Append {
            roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        }
        for root in config.tls.roots {
            roots
                .add(root)
                .map_err(|_| NetError::from(crate::error::ErrorKind::InvalidConfig))?;
        }
        if roots.is_empty() {
            return Err(NetError::from(crate::error::ErrorKind::InvalidConfig));
        }
        let builder = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|_| NetError::from(crate::error::ErrorKind::InvalidConfig))?
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
            original,
            proxy: config.proxy,
            tls: Arc::new(tls),
            network_status_policy: config.network_status_policy,
        })
    }

    pub(crate) fn original(&self) -> Arc<NetworkConfig> {
        self.original.clone()
    }

    pub(crate) fn network_status_policy(&self) -> NetworkStatusPolicy {
        self.network_status_policy
    }

    #[cfg(test)]
    pub(crate) async fn dial(
        &self,
        request: Request,
        protocol_config: WebSocketConfig,
        tcp_nodelay: bool,
        deadline: Instant,
    ) -> Result<NativeWebSocketStream, ConnectionFailure> {
        self.dial_with_diagnostics(request, protocol_config, tcp_nodelay, deadline, None)
            .await
            .map_err(|error| error.failure)
    }

    /// Each stage uses the caller's original attempt deadline. Dropping the
    /// future also drops its owned TCP/TLS stream, including a partial tunnel.
    pub(crate) async fn dial_with_diagnostics(
        &self,
        mut request: Request,
        protocol_config: WebSocketConfig,
        tcp_nodelay: bool,
        deadline: Instant,
        options: Option<&crate::ws::HandshakeDiagnosticOptions>,
    ) -> Result<NativeWebSocketStream, TransportFailure> {
        let fail = |failure| TransportFailure::with_options(failure, options);
        let tls_required = match request.uri().scheme_str() {
            Some("ws") => false,
            Some("wss") => true,
            _ => {
                return Err(fail(failure(
                    NetError::from(crate::error::ErrorKind::InvalidInput),
                    ConnectStage::RequestBuild,
                    false,
                )));
            }
        };
        let host = request
            .uri()
            .host()
            .map(normalize_host)
            .ok_or_else(|| {
                fail(failure(
                    NetError::from(crate::error::ErrorKind::InvalidInput),
                    ConnectStage::RequestBuild,
                    false,
                ))
            })?
            .to_owned();
        let port = request
            .uri()
            .port_u16()
            .map_or(if tls_required { 443 } else { 80 }, |port| port);
        let (connect_host, connect_port) = match &self.proxy.endpoint {
            Some(proxy) => (proxy.host.as_str(), proxy.port),
            None => (host.as_str(), port),
        };
        let mut socket = connect_tcp(connect_host, connect_port, deadline)
            .await
            .map_err(fail)?;
        if tcp_nodelay {
            socket
                .set_nodelay(true)
                .map_err(|error| TransportFailure::io(error, ConnectStage::Tcp, options))?;
        }
        if let Some(proxy) = &self.proxy.endpoint {
            ensure_deadline(deadline, ConnectStage::ProxyConnect).map_err(fail)?;
            timeout_at(
                deadline,
                proxy::establish_tunnel_with_diagnostics(
                    &mut socket,
                    &host,
                    port,
                    proxy.auth.as_ref(),
                    options,
                ),
            )
            .await
            .map_err(|_| fail(timeout(ConnectStage::ProxyConnect)))??;
            // Proxy credentials belong to the CONNECT exchange only, even if a
            // caller supplied a Proxy-Authorization header among Upgrade headers.
            request
                .headers_mut()
                .remove(tokio_tungstenite::tungstenite::http::header::PROXY_AUTHORIZATION);
        }
        let stream = if tls_required {
            ensure_deadline(deadline, ConnectStage::Tls).map_err(fail)?;
            let name = ServerName::try_from(host).map_err(|_| {
                fail(failure(
                    NetError::from(crate::error::ErrorKind::InvalidInput),
                    ConnectStage::Tls,
                    false,
                ))
            })?;
            let tls = timeout_at(
                deadline,
                TlsConnector::from(Arc::clone(&self.tls)).connect(name, socket),
            )
            .await
            .map_err(|_| fail(timeout(ConnectStage::Tls)))?
            .map_err(|error| classify_tls_io_with_diagnostics(error, ConnectStage::Tls, options))?;
            MaybeTlsStream::Rustls(tls)
        } else {
            MaybeTlsStream::Plain(socket)
        };
        ensure_deadline(deadline, ConnectStage::WebSocketUpgrade).map_err(fail)?;
        let (stream, _) = timeout_at(
            deadline,
            tokio_tungstenite::client_async_with_config(request, stream, Some(protocol_config)),
        )
        .await
        .map_err(|_| fail(timeout(ConnectStage::WebSocketUpgrade)))?
        .map_err(|error| classify_upgrade_error_with_diagnostics(error, options))?;
        Ok(stream)
    }
}

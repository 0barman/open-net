use super::stream::{tunnel_io, ProxyIo};
use crate::api::network_config::{normalize_host, ProxyEndpoint, ProxyTransport, TlsRole};
use crate::module::transport::failure::ConnectStage;
#[cfg(test)]
use crate::module::transport::failure::ConnectionFailure;
use crate::module::transport::{
    classify_tls_io_with_diagnostics, classify_upgrade_error_with_diagnostics, connect_tcp,
    ensure_deadline, failure, proxy, timeout, NativeWebSocketStream, TransportFailure,
};
use crate::{NetError, NetworkConfig, NetworkStatusPolicy, ProxyConfig};
use rustls::pki_types::ServerName;
use std::sync::Arc;
use tokio::time::{timeout_at, Instant};
use tokio_rustls::TlsConnector;
use tokio_tungstenite::tungstenite::handshake::client::Request;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::MaybeTlsStream;

#[derive(Clone)]
pub(crate) struct CompiledNetworkConfig {
    original: Arc<NetworkConfig>,
    pub(super) proxy: CompiledProxy,
    pub(super) target_tls: Arc<rustls::ClientConfig>,
    pub(crate) network_status_policy: NetworkStatusPolicy,
}

impl CompiledNetworkConfig {
    pub(crate) fn new(config: NetworkConfig) -> Result<Self, NetError> {
        config.proxy.validate_auth_transport()?;
        let target_tls = config.tls.compile_for(TlsRole::Target)?;
        let proxy = CompiledProxy::new(&config.proxy)?;
        Ok(Self {
            target_tls,
            proxy,
            network_status_policy: config.network_status_policy,
            original: Arc::new(config),
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
    #[cfg(test)]
    pub(crate) async fn dial_with_diagnostics(
        &self,
        request: Request,
        protocol_config: WebSocketConfig,
        tcp_nodelay: bool,
        deadline: Instant,
        options: Option<&crate::ws::HandshakeDiagnosticOptions>,
    ) -> Result<NativeWebSocketStream, TransportFailure> {
        self.dial_observed(request, protocol_config, tcp_nodelay, deadline, options,
            #[cfg(feature = "quality-doctor")]
            None,
        ).await
    }

    pub(crate) async fn dial_observed(
        &self,
        mut request: Request,
        protocol_config: WebSocketConfig,
        tcp_nodelay: bool,
        deadline: Instant,
        options: Option<&crate::ws::HandshakeDiagnosticOptions>,
        #[cfg(feature = "quality-doctor")] quality: Option<&crate::module::quality::QualityProbe>,
    ) -> Result<NativeWebSocketStream, TransportFailure> {
        let fail = |failure| TransportFailure::with_options(failure, options);
        // Recheck the immutable policy at the transport boundary before DNS/TCP.
        self.original
            .proxy
            .validate_auth_transport()
            .map_err(|error| fail(failure(error, ConnectStage::RequestBuild, false)))?;
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
        let (connect_host, connect_port) = match self.proxy.endpoint() {
            Some(proxy) => (proxy.host.as_str(), proxy.port),
            None => (host.as_str(), port),
        };
        let socket = connect_tcp(connect_host, connect_port, deadline,
            #[cfg(feature = "quality-doctor")]
            quality,
        )
            .await
            .map_err(fail)?;
        if tcp_nodelay {
            socket
                .set_nodelay(true)
                .map_err(|error| TransportFailure::io(error, ConnectStage::Tcp, options))?;
        }
        let io = match &self.proxy {
            CompiledProxy::HttpsConnect {
                tls, server_name, ..
            } => {
                ensure_deadline(deadline, ConnectStage::ProxyTls).map_err(fail)?;
                #[cfg(feature = "quality-doctor")]
                let _phase = quality.map(|probe| probe.phase(crate::api::quality::PhaseKind::ProxyTls));
                let stream = timeout_at(
                    deadline,
                    TlsConnector::from(Arc::clone(tls)).connect(server_name.clone(), socket),
                )
                .await
                .map_err(|_| fail(timeout(ConnectStage::ProxyTls)))?
                .map_err(|error| {
                    classify_tls_io_with_diagnostics(error, ConnectStage::ProxyTls, options)
                })?;
                ProxyIo::Tls(Box::new(stream))
            }
            CompiledProxy::Direct | CompiledProxy::HttpConnect(_) => ProxyIo::Plain(socket),
        };
        let mut socket = tunnel_io(io);
        if let Some(proxy) = self.proxy.endpoint() {
            ensure_deadline(deadline, ConnectStage::ProxyConnect).map_err(fail)?;
            #[cfg(feature = "quality-doctor")]
            let _phase = quality.map(|probe| probe.phase(crate::api::quality::PhaseKind::ProxyConnect));
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
            #[cfg(feature = "quality-doctor")]
            let _phase = quality.map(|probe| probe.phase(crate::api::quality::PhaseKind::Tls));
            let name = ServerName::try_from(host).map_err(|_| {
                fail(failure(
                    NetError::from(crate::error::ErrorKind::InvalidInput),
                    ConnectStage::Tls,
                    false,
                ))
            })?;
            let tls = timeout_at(
                deadline,
                TlsConnector::from(Arc::clone(&self.target_tls)).connect(name, socket),
            )
            .await
            .map_err(|_| fail(timeout(ConnectStage::Tls)))?
            .map_err(|error| classify_tls_io_with_diagnostics(error, ConnectStage::Tls, options))?;
            MaybeTlsStream::Rustls(tls)
        } else {
            MaybeTlsStream::Plain(socket)
        };
        ensure_deadline(deadline, ConnectStage::WebSocketUpgrade).map_err(fail)?;
        #[cfg(feature = "quality-doctor")]
        let _phase = quality.map(|probe| probe.phase(crate::api::quality::PhaseKind::WebSocketUpgrade));
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

/// Routing is compiled once per client, with an independent TLS session policy
/// for the proxy. No proxy configuration inherits the target's CA or identity.
#[derive(Clone)]
pub(super) enum CompiledProxy {
    Direct,
    HttpConnect(ProxyEndpoint),
    HttpsConnect {
        endpoint: ProxyEndpoint,
        tls: Arc<rustls::ClientConfig>,
        server_name: ServerName<'static>,
    },
}

impl CompiledProxy {
    fn new(config: &ProxyConfig) -> Result<Self, NetError> {
        config.validate_auth_transport()?;
        let Some(endpoint) = &config.endpoint else {
            return Ok(Self::Direct);
        };
        Ok(match &endpoint.transport {
            ProxyTransport::Http => Self::HttpConnect(endpoint.clone()),
            ProxyTransport::Https(tls) => Self::HttpsConnect {
                endpoint: endpoint.clone(),
                tls: tls.compile_for(TlsRole::Proxy)?,
                server_name: ServerName::try_from(endpoint.host.clone()).map_err(|_| {
                    NetError::config(
                        "proxy.url",
                        "requires a valid TLS server name or IP address",
                    )
                })?,
            },
        })
    }

    pub(super) fn endpoint(&self) -> Option<&ProxyEndpoint> {
        match self {
            Self::Direct => None,
            Self::HttpConnect(endpoint) | Self::HttpsConnect { endpoint, .. } => Some(endpoint),
        }
    }
}

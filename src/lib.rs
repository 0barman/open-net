// Keep shared implementation sources in libs/common while shipping one crate.
#[path = "../libs/common/src/mod.rs"]
pub(crate) mod common;

pub mod api;
pub(crate) mod inner;
pub(crate) mod module;

pub use crate::common::log::{LogInfo, LogLevel, LogListener, LogSubscription, LogType, Logger};
#[cfg(feature = "ws-client")]
pub use api::listener::WebSocketClientTaskCompleteListener;
pub use api::net_error::NetError;
pub use api::net_status_client::{
    IpStack, NetStatusClient, NetworkStatus, NetworkStatusListener, NetworkStatusListenerHandle,
};
#[cfg(feature = "ws-client")]
pub use api::network_config::{
    ClientIdentity, NetworkConfig, NetworkStatusPolicy, ProxyBasicAuth, ProxyConfig,
    RootCertificateMode, TlsConfig,
};
pub use api::open_net::OpenNet;
#[cfg(feature = "ws-client")]
pub use api::traits::ws::connection_status::ConnectionStatus;
#[cfg(feature = "ws-client")]
pub use api::traits::ws::ws_body::WsBody;
#[cfg(feature = "ws-client")]
pub use api::traits::ws::ws_request_config::{
    DisconnectedTaskPolicy, WSRequestConfig, WSRequestPriority,
};
#[cfg(feature = "ws-client")]
pub use api::traits::ws::ws_request_trait::WSRequestTrait;
#[cfg(feature = "ws-client")]
pub use api::web_socket_client::{
    ReconnectPolicy, TcpKeepaliveConfig, WebSocketClient, WebSocketClientConfig,
    WebSocketConnectOptions, WebSocketHeaderProvider,
};
#[cfg(feature = "ws-client")]
pub use api::wsc::{
    PendingRequestCompletion, PendingRequestInfo, PendingRequestStatus, PendingRequestView,
    QueuedRequestCompletion, WSCResponse, WebSocketMessage,
};
#[cfg(feature = "ws-client")]
pub use api::wsc::{
    PreparedRequest, RequestRegistration, RequestRegistrationToken, RequestScope,
    RequestTerminationOutcome, ResponseDeadlineOrigin, WebSocketRequestOptions,
};
#[cfg(feature = "ws-client")]
pub use api::wsc::{
    WebSocketConnectStage, WebSocketConnectionEvent, WebSocketConnectionEventKind,
    WebSocketConnectionEvents, WebSocketConnectionFailure, WebSocketContextConnectOptions,
    WebSocketHandshakeAttempt, WebSocketHandshakeProvider, WebSocketHandshakeSnapshot,
    WebSocketTerminationReason,
};
#[cfg(feature = "ws-client")]
pub use api::wsc::{
    WebSocketTaskDelivery, WebSocketTaskEndCause, WebSocketTaskEvent, WebSocketTaskEventOptions,
    WebSocketTaskPhase, WebSocketTaskSource, WebSocketTaskSuccess,
};

// Exported macros must resolve their helpers from downstream crates.
#[doc(hidden)]
pub use serde_json as __serde_json;

#[doc(hidden)]
pub mod __log {
    pub use crate::common::log::log_def::on_log;
    pub use crate::common::log::summary::error as error_summary;
}

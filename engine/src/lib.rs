pub mod api;
pub(crate) mod inner;
pub(crate) mod module;

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
pub use on_common::log::listener::LogListener;
pub use on_common::log::log_def::LogType;
pub use on_common::log::log_info::LogInfo;
pub use on_common::log::log_level::LogLevel;
pub use on_common::log::logger::{LogSubscription, Logger};

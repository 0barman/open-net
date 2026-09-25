// Keep shared implementation sources in libs/common while shipping one crate.
#[path = "../libs/common/src/mod.rs"]
pub(crate) mod common;

pub mod api;
pub(crate) mod inner;
pub(crate) mod module;

// Keep existing import paths as re-exports of the canonical API modules.
pub use api::{config, error, net_status, network, subscription};
#[cfg(feature = "ws-client")]
pub use api::{ws, WebSocketClient};
pub use api::{
    BoxError, Bytes, EnqueueError, HeaderMap, HeaderName, HeaderValue, IpStack, LogInfo, LogLevel,
    LogListener, LogSubscription, LogType, Logger, Metadata, MonitorState, NetError,
    NetStatusClient, NetworkSnapshot, NetworkStatus, OpenNet, OpenNetConfig, Result, StatusCode,
};
#[cfg(feature = "ws-client")]
pub(crate) use network::{NetworkConfig, NetworkStatusPolicy, ProxyConfig};

// Exported macros must resolve their helpers from downstream crates.
#[doc(hidden)]
pub use serde_json as __serde_json;

#[doc(hidden)]
pub mod __log {
    pub use crate::common::log::log_def::on_log;
    pub use crate::common::log::summary::error as error_summary;
}

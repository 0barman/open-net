// Keep shared implementation sources in libs/common while shipping one crate.
#[path = "../libs/common/src/mod.rs"]
pub(crate) mod common;

// Share validated retry logic across transports while publishing one crate.
#[path = "../libs/retry/src/mod.rs"]
pub(crate) mod retry_core;

pub mod api;

// Unit-test clients use an application-owned provider, just like downstream
// applications. This initializer and its dependency are never part of the SDK.
#[cfg(test)]
#[cfg(any(feature = "ws-client", feature = "http-client"))]
#[ctor::ctor]
fn install_unit_test_crypto_provider() {
    if rustls_openssl::default_provider()
        .install_default()
        .is_err()
    {
        eprintln!("unit-test TLS provider was already installed; retaining the existing provider");
    }
}
pub(crate) mod inner;
pub(crate) mod module;

// Keep existing import paths as re-exports of the canonical API modules.
#[cfg(any(feature = "ws-client", feature = "http-client"))]
pub use api::tls;
pub use api::{config, error, network, retry, subscription};
#[cfg(feature = "net-status")]
pub use api::{
    net_status, IpStack, MonitorState, NetStatusClient, NetworkEvent, NetworkEventKind,
    NetworkEventOptions, NetworkIpStackSource, NetworkNameObserver, NetworkNameOptions,
    NetworkNameSnapshot, NetworkNameSource, NetworkNameState, NetworkObservation,
    NetworkRefreshResult, NetworkSnapshot, NetworkStatus,
};
#[cfg(feature = "ws-client")]
pub use api::{ws, WebSocketClient};
pub use api::{
    BoxError, Bytes, EnqueueError, HeaderMap, HeaderName, HeaderValue, LogDropStats, LogInfo,
    LogLevel, LogListener, LogSubscription, LogSubscriptionOptions, LogType, Metadata, NetError,
    OpenNet, OpenNetConfig, OpenNetLogger, Result, SharedLogListener, StatusCode,
};
#[cfg(feature = "http-client")]
pub use async_trait::async_trait;
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

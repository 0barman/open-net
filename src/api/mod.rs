//! Public types and entry points for `open-net`.
//!
//! This module groups configuration, error handling, logging, network-status
//! monitoring, and transport clients. Most items are re-exported at the crate
//! root so applications can use stable, concise import paths.

pub mod config;
pub mod error;
pub mod log;
pub mod net_status;
pub mod network;
pub mod subscription;
#[cfg(feature = "ws-client")]
pub mod ws;

#[cfg(feature = "http-client")]
pub mod http;

#[cfg(feature = "ws-client")]
pub(crate) mod network_config;
pub(crate) mod open_net;
pub(crate) mod open_net_config;

pub use ::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
pub use bytes::Bytes;
/// String metadata associated with a request, event, or diagnostic record.
pub type Metadata = std::collections::BTreeMap<String, String>;

pub use config::OpenNetConfig;
pub use error::{BoxError, EnqueueError, NetError, Result};
pub use log::{LogInfo, LogLevel, LogListener, LogSubscription, LogType, Logger};
pub use net_status::{IpStack, MonitorState, NetStatusClient, NetworkSnapshot, NetworkStatus};
pub use open_net::OpenNet;
#[cfg(feature = "ws-client")]
pub use ws::WebSocketClient;

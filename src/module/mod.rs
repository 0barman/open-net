#[cfg(feature = "http-client")]
pub(crate) mod http;
#[cfg(feature = "net-status")]
pub(crate) mod net_status;
#[cfg(feature = "quality-doctor")]
pub(crate) mod quality;
#[cfg(any(feature = "http-client", feature = "ws-client"))]
pub(crate) mod tls;
#[cfg(feature = "ws-client")]
pub(crate) mod transport;
#[cfg(feature = "ws-client")]
pub(crate) mod ws_client;

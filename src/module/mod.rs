#[cfg(feature = "http-client")]
pub(crate) mod http;
pub(crate) mod net_status;
#[cfg(feature = "ws-client")]
pub(crate) mod transport;
#[cfg(feature = "ws-client")]
pub(crate) mod ws_client;

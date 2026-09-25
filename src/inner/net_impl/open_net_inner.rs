#[cfg(feature = "ws-client")]
use super::client_slot::ClientSlot;
#[cfg(feature = "http-client")]
use super::http_client_slot::HttpClientSlot;
use super::net_status_clients::NetStatusClientSlot;
use crate::common::CommonEngine;
#[cfg(feature = "ws-client")]
use crate::module::transport::compiled_network_config::CompiledNetworkConfig;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub(crate) struct OpenNetInner {
    #[cfg(feature = "ws-client")]
    pub(super) network: Arc<CompiledNetworkConfig>,
    #[allow(dead_code)]
    pub(crate) common_engine: Arc<CommonEngine>,
    pub(super) net_status_clients: Arc<Mutex<HashMap<String, NetStatusClientSlot>>>,
    #[cfg(feature = "ws-client")]
    pub(super) clients: Arc<Mutex<HashMap<String, ClientSlot>>>,
    #[cfg(feature = "http-client")]
    pub(super) http_clients: Arc<Mutex<HashMap<String, HttpClientSlot>>>,
}

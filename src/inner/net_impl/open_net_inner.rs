#[cfg(feature = "ws-client")]
use super::client_slot::ClientSlot;
use super::net_status_clients::NetStatusClientSlot;
use crate::common::CommonEngine;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub(crate) struct OpenNetInner {
    #[cfg(feature = "ws-client")]
    pub(super) network: Arc<crate::module::transport::CompiledNetworkConfig>,
    #[allow(dead_code)]
    pub(crate) common_engine: Arc<CommonEngine>,
    pub(super) net_status_clients: Arc<Mutex<HashMap<String, NetStatusClientSlot>>>,
    #[cfg(feature = "ws-client")]
    pub(super) clients: Arc<Mutex<HashMap<String, ClientSlot>>>,
}

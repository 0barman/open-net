use super::network_status_snapshot::NetworkStatusPublisher;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

// Private per-generation identity. Observable values live exclusively in NetworkStatusSource.
pub(crate) struct MonitorState {
    pub(crate) active: Arc<AtomicBool>,
    pub(super) observation: Option<NetworkStatusPublisher>,
}

impl Default for MonitorState {
    fn default() -> Self {
        Self {
            active: Arc::new(AtomicBool::new(true)),
            observation: None,
        }
    }
}

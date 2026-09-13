use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use tokio::sync::{oneshot, watch};

use super::monitor_state::MonitorState;

pub(crate) struct MonitorRuntime {
    pub(crate) stop_sender: Option<oneshot::Sender<()>>,
    pub(crate) initial_state: watch::Receiver<bool>,
    pub(crate) finished: watch::Receiver<bool>,
    pub(crate) state: Arc<Mutex<MonitorState>>,
    pub(crate) active: Arc<AtomicBool>,
}

/// Completion is also published if the runtime cancels the task before its
/// first poll or if the monitor unwinds, so shutdown cannot lose its waiter.
pub(crate) struct MonitorCompletion(pub(crate) watch::Sender<bool>);

impl Drop for MonitorCompletion {
    fn drop(&mut self) {
        self.0.send_replace(true);
    }
}

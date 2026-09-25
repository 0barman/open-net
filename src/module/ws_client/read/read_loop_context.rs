use crate::module::ws_client::heartbeat_state::HeartbeatState;
use crate::module::ws_client::io_event::IoEvent;
use crate::module::ws_client::session_runtime::SessionRuntime;
use crate::module::ws_client::write::control_message::ControlMessage;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// One physical reader with immutable session authorities installed at startup.
pub(crate) struct ReadLoopContext {
    pub(crate) runtime: Arc<SessionRuntime>,
    /// First decoded Close, recorded before awaiting the protocol reply.
    pub(crate) peer_close: Arc<crate::module::ws_client::io_diagnostics::PeerCloseObservation>,
    /// Automatic Pong and Close flushes use their independent bounded lane.
    pub(crate) control_tx: mpsc::Sender<ControlMessage>,
    pub(crate) io_event_tx: mpsc::Sender<IoEvent>,
    /// Checked physical identity shared with the writer and lifecycle events.
    pub(crate) generation: u64,
    pub(crate) cancel: CancellationToken,
    pub(crate) heartbeat: Arc<HeartbeatState>,
}

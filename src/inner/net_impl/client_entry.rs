use crate::common::retirement::ThreadHandle;
use crate::ws::WebSocketClient;

pub(super) struct ClientEntry {
    pub(super) client: WebSocketClient,
    pub(super) worker_thread: Option<ThreadHandle>,
}

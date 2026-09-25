use crate::ws::WebSocketClient;
use std::thread::JoinHandle;

pub(super) struct ClientEntry {
    pub(super) client: WebSocketClient,
    pub(super) worker_thread: Option<JoinHandle<()>>,
}

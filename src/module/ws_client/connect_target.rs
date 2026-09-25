use crate::module::ws_client::connection_session::ConnectionSession;
use crate::module::ws_client::session_runtime::SessionRuntime;
use crate::ws::ConnectOptions;
use std::sync::Arc;

/// 固定身份和配置的连接会话；自动重连复用它，终态后移除。
#[derive(Clone)]
pub(super) struct ConnectTarget {
    pub(super) options: ConnectOptions,
    pub(super) initial_connect_deadline: Option<tokio::time::Instant>,
    pub(super) session: Arc<ConnectionSession>,
    pub(super) runtime: Arc<SessionRuntime>,
}

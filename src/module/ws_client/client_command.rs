use crate::error::NetError;
use tokio::sync::oneshot;

/// 外部客户端句柄发送给唯一 WebSocket worker 的控制命令。
///
/// 命令经有界 MPSC 通道串行处理。Connect 回复会话准入结果；关闭完成由
/// Session 的终态和客户端 shutdown_complete 令牌通知，避免重复回复通道。
pub(super) enum ClientCommand {
    Connect {
        options: crate::ws::ConnectOptions,
        initial_connect_deadline: Option<tokio::time::Instant>,
        session: std::sync::Arc<crate::module::ws_client::connection_session::ConnectionSession>,
        runtime: std::sync::Arc<crate::module::ws_client::session_runtime::SessionRuntime>,
        reply: oneshot::Sender<Result<(), NetError>>,
    },
    /// Close only the session whose identity was captured by its owner.
    CloseSession {
        session: std::sync::Arc<crate::module::ws_client::connection_session::ConnectionSession>,
        frame: Option<crate::ws::CloseFrame>,
        deadline: tokio::time::Instant,
    },
    /// 永久关闭 worker 和业务写队列。
    Shutdown,
}

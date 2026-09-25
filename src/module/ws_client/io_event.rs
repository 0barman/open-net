use crate::error::NetError;
use crate::module::ws_client::ws_client_worker::connection_budget::{
    BudgetDeadline, HandshakeAdmission,
};
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

/// 当前平台上握手成功后得到的原生 TCP 或 TLS WebSocket 流。
type NativeWebSocketStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// 连接任务及读写任务发送给 WebSocket worker 的生命周期事件。
///
/// 每个事件都携带产生它的连接代次，worker 会忽略代次不匹配的过期事件。
/// 成功连接、连接失败及读写终止事件还会校验当前状态；
/// 这些规则使大部分被取消或较晚到达的旧任务不会污染新连接状态。
pub(crate) enum IoEvent {
    /// User code did not start because the OS refused a data callback thread.
    /// Session identity prevents delayed callbacks from affecting a newer owner.
    CallbackDispatchFailed { session_id: crate::ws::SessionId },
    ContextAttemptStarted {
        deadline: BudgetDeadline,
        generation: u64,
        attempt: crate::ws::HandshakeAttempt,
        reservation: crate::module::ws_client::connection_session::AttemptReservation,
        accepted: tokio::sync::oneshot::Sender<Result<(), NetError>>,
    },
    ContextPrepared {
        deadline: BudgetDeadline,
        generation: u64,
        attempt_id: u64,
        credential_version: Option<String>,
        accepted: tokio::sync::oneshot::Sender<Result<(), NetError>>,
    },
    ContextAttemptFailed {
        generation: u64,
        attempt_id: u64,
        failure: crate::module::transport::failure::ConnectionFailure,
        diagnostic: Option<crate::ws::HandshakeDiagnostic>,
        retry: crate::ws::RetryDecision,
        accepted: tokio::sync::oneshot::Sender<Result<(), NetError>>,
    },
    /// 一轮连接任务成功完成 WebSocket 握手。
    ConnectSucceeded {
        attempt_id: u64,
        /// The network loss epoch admitted before this handshake began.
        network_loss_epoch: Option<u64>,
        /// Shared deadline decision makes worker admission and timeout mutually exclusive.
        admission: std::sync::Arc<HandshakeAdmission>,
        accepted: tokio::sync::oneshot::Sender<Result<(), NetError>>,
        /// 启动该连接任务时分配的连接代次。
        generation: u64,
        /// 已建立且尚未拆分为读写两半的 WebSocket 流。
        stream: Box<NativeWebSocketStream>,
    },
    /// 一轮连接任务在重试终止后仍未建立连接。
    ConnectFailed {
        /// 启动该连接任务时分配的连接代次。
        generation: u64,
        /// 原始失败分类，保留发生阶段和 HTTP 状态。
        failure: crate::module::transport::failure::ConnectionFailure,
        /// 周期的停止原因，与 failure.error() 分开。例如 provider 返回
        /// RetryExhausted 仍属于 ConnectFailed，不能取得网络提示恢复权限。
        reason: crate::ws::TerminationReason,
    },
    /// WebSocket 读任务因对端关闭、读取错误或内部通道失败而结束。
    ReadEnded {
        /// 读任务所属的连接代次。
        generation: u64,
        /// 导致读循环结束并驱动断线处理的错误。
        error: NetError,
        kind: crate::ws::IoEndKind,
    },
    /// WebSocket 写任务因心跳超时、写入错误或队列异常而结束。
    WriteEnded {
        /// 写任务所属的连接代次。
        generation: u64,
        /// 导致写循环结束并驱动断线处理的错误。
        error: NetError,
        kind: crate::ws::IoEndKind,
    },
}

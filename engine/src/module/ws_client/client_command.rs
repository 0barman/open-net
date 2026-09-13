use crate::api::net_error::NetError;
use crate::api::web_socket_client::WebSocketConnectOptions;
use tokio::sync::oneshot;

/// 外部客户端句柄发送给唯一 WebSocket worker 的控制命令。
///
/// 命令经有界 MPSC 通道串行处理。需要等待操作完成的变体携带一次性回复通道；
/// 若调用方已经放弃等待，worker 发送回复失败不会影响自身状态机。
pub(super) enum ClientCommand {
    ConnectWithContext {
        options: crate::api::wsc::WebSocketContextConnectOptions,
        session: std::sync::Arc<crate::module::ws_client::connection_session::ConnectionSession>,
        reply: oneshot::Sender<Result<(), NetError>>,
    },
    /// 启动一轮初始连接流程。
    Connect {
        /// 用于构建 WebSocket 握手请求的目标 URL；公共入口会先去除首尾空白。
        url: String,
        /// 本轮连接的请求头、动态请求头提供器及重连策略。
        options: WebSocketConnectOptions,
        /// 返回握手成功或本轮连接流程最终错误的一次性通道。
        ///
        /// 主动断开或关闭时，也会通过该通道返回取消错误。
        reply: oneshot::Sender<Result<(), NetError>>,
    },
    /// 主动断开当前连接，但保留 worker 以便之后再次连接。
    Disconnect {
        /// 在连接尝试和活动 I/O 停止、队列及待请求清理、状态回到空闲后发送结果。
        reply: oneshot::Sender<Result<(), NetError>>,
    },
    /// 表示网络可能已经恢复，可唤起已启用自动重连的断开态客户端。
    ///
    /// 该命令只是一次触发信号，不保存持续的网络可用状态，也不保证一定建立连接。
    NetworkAvailable,
    /// 永久关闭 worker 和业务写队列。
    Shutdown {
        /// 可选的关闭完成回复通道。
        ///
        /// `Some` 供显式异步关闭等待清理结束；`None` 用于析构或取消令牌触发的
        /// 无等待关闭。回复发送失败不会撤销已经完成的关闭。
        reply: Option<oneshot::Sender<Result<(), NetError>>>,
    },
}

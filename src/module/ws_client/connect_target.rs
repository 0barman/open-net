use crate::api::web_socket_client::WebSocketConnectOptions;

/// 可由自动重连流程重复使用的连接目标快照。
///
/// 每次开始连接任务时都会克隆该值；静态请求头随快照复制，动态请求头提供器则由
/// 连接任务在每次握手尝试前重新调用。主动断开、永久关闭、context 会话终态或
/// legacy 不可重试失败会移除 worker 保存的目标；legacy 暂态耗尽可保留目标供提示恢复。
#[derive(Clone)]
pub(super) struct ConnectTarget {
    pub(super) context: Option<ContextConnectTarget>,
    /// 用于构建 WebSocket 握手请求的目标 URL。
    ///
    /// 正常由公共连接入口创建时已经去除首尾空白。
    pub(super) url: String,
    /// 请求头、动态请求头提供器和连接重试策略。
    pub(super) options: WebSocketConnectOptions,
}

#[derive(Clone)]
pub(super) struct ContextConnectTarget {
    pub(super) options: crate::api::wsc::WebSocketContextConnectOptions,
    pub(super) session:
        std::sync::Arc<crate::module::ws_client::connection_session::ConnectionSession>,
}

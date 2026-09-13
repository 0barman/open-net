use crate::api::net_error::NetError;
use crate::api::wsc::web_socket_connection_event::WebSocketConnectionEvent;
use crate::module::ws_client::connection_session::ConnectionSession;
use std::sync::Arc;

/// 有容量上限、按序交付的连接会话事件流的唯一消费者。
///
/// 收到 Established 或 ConnectionTerminated 后仍应继续接收，直到 SessionTerminated，
/// 以观察重连和会话的最终结果。重试标志描述的是一个周期内的握手失败；
/// 物理连接终止时该标志为 false，并不表示会话已经结束。丢弃本句柄只会请求取消
/// 其所属会话；Drop 不会阻塞。
pub struct WebSocketConnectionEvents {
    pub(crate) session: Arc<ConnectionSession>,
}

impl WebSocketConnectionEvents {
    /// 接收下一个不可变事件；最后一个事件被消费后返回 None。
    /// 取消本次接收的异步操作不会消费事件。
    pub async fn recv(&mut self) -> Result<Option<WebSocketConnectionEvent>, NetError> {
        self.session.recv().await
    }

    /// 请求取消会话，并等待工作任务完成会话清理。
    /// 已排队的事件仍可读取；取消操作无需等待这些事件被消费。
    pub async fn cancel(&self) -> Result<(), NetError> {
        self.session.request_cancel();
        self.session.wait_finished().await
    }
}

impl std::fmt::Debug for WebSocketConnectionEvents {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WebSocketConnectionEvents")
            .finish_non_exhaustive()
    }
}

impl Drop for WebSocketConnectionEvents {
    fn drop(&mut self) {
        self.session.request_cancel();
    }
}

use crate::api::net_error::NetError;
use std::sync::Arc;

/// 为一次握手尝试提供请求头及其不含秘密信息的关联上下文。
///
/// 回调在没有 Tokio 运行时上下文的专用操作系统线程上执行，必须最终返回：
/// 取消操作只会停止等待，无法终止任意用户代码。
pub type WebSocketHandshakeProvider = Arc<
    dyn Fn(WebSocketHandshakeAttempt) -> Result<WebSocketHandshakeSnapshot, NetError>
        + Send
        + Sync
        + 'static,
>;

/// 在握手信息提供器执行前获取的不可变传输标识。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WebSocketHandshakeAttempt {
    pub(crate) client_instance_id: u64,
    pub(crate) session_id: u64,
    pub(crate) cycle_id: u64,
    pub(crate) attempt_id: u64,
    pub(crate) session_context_id: u64,
}

impl WebSocketHandshakeAttempt {
    pub(crate) fn new(
        client_instance_id: u64,
        session_id: u64,
        cycle_id: u64,
        attempt_id: u64,
        session_context_id: u64,
    ) -> Self {
        Self {
            client_instance_id,
            session_id,
            cycle_id,
            attempt_id,
            session_context_id,
        }
    }

    pub fn client_instance_id(&self) -> u64 {
        self.client_instance_id
    }
    pub fn session_id(&self) -> u64 {
        self.session_id
    }
    pub fn cycle_id(&self) -> u64 {
        self.cycle_id
    }
    pub fn attempt_id(&self) -> u64 {
        self.attempt_id
    }
    pub fn session_context_id(&self) -> u64 {
        self.session_context_id
    }
}

/// 请求头及其凭据版本关联键；该键的内容由应用定义，不含秘密信息。
///
/// 本库不解释关联键的内容，也不会将请求头复制到生命周期事件中。
#[derive(Clone)]
pub struct WebSocketHandshakeSnapshot {
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) attempt_context_id: u64,
}

impl WebSocketHandshakeSnapshot {
    pub fn new(headers: Vec<(String, String)>, attempt_context_id: u64) -> Self {
        Self {
            headers,
            attempt_context_id,
        }
    }
}

impl std::fmt::Debug for WebSocketHandshakeSnapshot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WebSocketHandshakeSnapshot")
            .field("header_count", &self.headers.len())
            .field("attempt_context_id", &self.attempt_context_id)
            .finish()
    }
}

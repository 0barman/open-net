use crate::api::net_error::NetError;
use crate::api::wsc::reconnect_policy::ReconnectPolicy;
use crate::api::wsc::web_socket_handshake_context::WebSocketHandshakeProvider;
use crate::api::wsc::RequestScope;

pub(crate) const DEFAULT_EVENT_CAPACITY: usize = 32;
pub(crate) const MIN_EVENT_CAPACITY: usize = 3;
pub(crate) const MAX_EVENT_CAPACITY: usize = 4096;

/// 能可靠关联握手结果的连接会话配置。
///
/// URL 在整个会话中固定不变，包括自动重连。可以配置带有显式尝试上下文的静态请求头、
/// 请求头提供器，或同时配置两者；提供器返回的请求头会覆盖同名静态请求头，
/// 并使用提供器的上下文标识最终的握手尝试。
#[derive(Clone)]
pub struct WebSocketContextConnectOptions {
    pub(crate) url: String,
    pub(crate) session_context_id: u64,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) attempt_context_id: Option<u64>,
    pub(crate) header_provider: Option<WebSocketHandshakeProvider>,
    pub(crate) reconnect: ReconnectPolicy,
    pub(crate) event_capacity: usize,
    pub(crate) request_scope: Option<RequestScope>,
}

impl WebSocketContextConnectOptions {
    pub fn new(url: impl Into<String>, session_context_id: u64) -> Self {
        Self {
            url: url.into(),
            session_context_id,
            headers: Vec::new(),
            attempt_context_id: None,
            header_provider: None,
            reconnect: ReconnectPolicy::default(),
            event_capacity: DEFAULT_EVENT_CAPACITY,
            request_scope: None,
        }
    }

    pub fn with_headers(mut self, headers: Vec<(String, String)>, attempt_context_id: u64) -> Self {
        self.headers = headers;
        self.attempt_context_id = Some(attempt_context_id);
        self
    }

    pub fn with_header_provider(mut self, provider: WebSocketHandshakeProvider) -> Self {
        self.header_provider = Some(provider);
        self
    }

    pub fn with_reconnect(mut self, policy: ReconnectPolicy) -> Self {
        self.reconnect = policy;
        self
    }

    /// 将连接及其自动重连绑定到固定的业务取消域。
    ///
    /// 绑定后仅接受显式携带同一 scope 的发送；旧的无 scope 发送入口不会
    /// 自动取得该身份。scope 取消后不能将此连接重新认领为其他业务会话。
    pub fn with_request_scope(mut self, scope: RequestScope) -> Self {
        self.request_scope = Some(scope);
        self
    }

    /// 设置事件总容量，包含为终止事件预留的位置，取值范围为 3 至 4096。
    pub fn with_event_capacity(mut self, capacity: usize) -> Self {
        self.event_capacity = capacity;
        self
    }

    pub(crate) fn validate(&self) -> Result<(), NetError> {
        if self
            .request_scope
            .as_ref()
            .is_some_and(RequestScope::is_cancelled)
        {
            return Err(NetError::Cancelled);
        }
        if self.url.trim().is_empty() {
            return Err(NetError::ParameterEmpty);
        }
        if !(MIN_EVENT_CAPACITY..=MAX_EVENT_CAPACITY).contains(&self.event_capacity)
            || !self.reconnect.is_valid()
            || (self.header_provider.is_none() && self.attempt_context_id.is_none())
        {
            return Err(NetError::ConfigError);
        }
        Ok(())
    }
}

impl std::fmt::Debug for WebSocketContextConnectOptions {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WebSocketContextConnectOptions")
            .field("url", &"[redacted]")
            .field("session_context_id", &self.session_context_id)
            .field("header_count", &self.headers.len())
            .field("attempt_context_id", &self.attempt_context_id)
            .field("has_header_provider", &self.header_provider.is_some())
            .field("reconnect", &self.reconnect)
            .field("event_capacity", &self.event_capacity)
            .field("request_scope", &self.request_scope)
            .finish()
    }
}

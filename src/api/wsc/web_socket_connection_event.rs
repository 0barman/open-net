use crate::api::net_error::NetError;

/// 产生连接失败的阶段；HTTP 状态码归属于该阶段。
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WebSocketConnectStage {
    Provider,
    RequestBuild,
    Dns,
    Tcp,
    ProxyConnect,
    Tls,
    WebSocketUpgrade,
    /// WebSocket 连接建立后的读写失败。
    WebSocketIo,
    /// 事件准入或交付失败，或由工作任务决定的取消。
    /// 取消操作不会读取被中断任务当时所处的传输阶段。
    EventDelivery,
}

/// 稳定的网络错误，保留原始发生阶段及 HTTP 响应状态码（如有）。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WebSocketConnectionFailure {
    error: NetError,
    stage: WebSocketConnectStage,
    http_status: Option<u16>,
    retryable: bool,
}

impl WebSocketConnectionFailure {
    pub(crate) fn new(
        error: NetError,
        stage: WebSocketConnectStage,
        http_status: Option<u16>,
        retryable: bool,
    ) -> Self {
        Self {
            error,
            stage,
            http_status,
            retryable,
        }
    }

    pub fn error(&self) -> NetError {
        self.error
    }
    pub fn stage(&self) -> WebSocketConnectStage {
        self.stage
    }
    pub fn http_status(&self) -> Option<u16> {
        self.http_status
    }
    /// 对于 AttemptFailed 事件，表示该失败类别是否允许在当前周期内再次尝试握手，
    /// 不考虑剩余预算。工作任务结合策略作出的实际决定请查看该事件的 `will_retry()`。
    ///
    /// 对于其他事件种类，本值不描述会话恢复行为。尤其是 ConnectionTerminated 中
    /// 本值为 false 时，仍可能开始新的重连周期。应继续消费事件，直到 SessionTerminated。
    pub fn retryable(&self) -> bool {
        self.retryable
    }
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WebSocketConnectionEventKind {
    AttemptFailed,
    Established,
    ConnectionTerminated,
    SessionTerminated,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WebSocketTerminationReason {
    Cancelled,
    Disconnected,
    Shutdown,
    ConnectFailed,
    RetryExhausted,
    IoFailure,
    /// 显式启用的网络监控判定当前物理连接失效。
    /// 符合恢复条件的会话可以保持存续，等待网络恢复。
    NetworkUnavailable,
}

/// 一条不可变的生命周期事件；序号在本会话内连续递增。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WebSocketConnectionEvent {
    pub(crate) sequence: u64,
    pub(crate) client_instance_id: u64,
    pub(crate) session_id: u64,
    pub(crate) cycle_id: Option<u64>,
    pub(crate) attempt_id: Option<u64>,
    pub(crate) session_context_id: u64,
    pub(crate) attempt_context_id: Option<u64>,
    pub(crate) kind: WebSocketConnectionEventKind,
    pub(crate) failure: Option<WebSocketConnectionFailure>,
    pub(crate) termination_reason: Option<WebSocketTerminationReason>,
    pub(crate) will_retry: bool,
}

impl WebSocketConnectionEvent {
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
    pub fn client_instance_id(&self) -> u64 {
        self.client_instance_id
    }
    pub fn session_id(&self) -> u64 {
        self.session_id
    }
    pub fn cycle_id(&self) -> Option<u64> {
        self.cycle_id
    }
    pub fn attempt_id(&self) -> Option<u64> {
        self.attempt_id
    }
    pub fn session_context_id(&self) -> u64 {
        self.session_context_id
    }
    pub fn attempt_context_id(&self) -> Option<u64> {
        self.attempt_context_id
    }
    pub fn kind(&self) -> WebSocketConnectionEventKind {
        self.kind
    }
    pub fn failure(&self) -> Option<WebSocketConnectionFailure> {
        self.failure
    }
    pub fn termination_reason(&self) -> Option<WebSocketTerminationReason> {
        self.termination_reason
    }
    /// 对于 AttemptFailed，表示工作任务结合失败分类和重试预算后，
    /// 是否允许在当前周期内再次尝试握手。取消操作仍可能阻止该尝试启动。
    ///
    /// 对于其他事件种类，本值为 false，不得用于判断会话是否结束。
    /// ConnectionTerminated 后仍可能开始新周期；应继续消费事件，直到 SessionTerminated。
    pub fn will_retry(&self) -> bool {
        self.will_retry
    }
}

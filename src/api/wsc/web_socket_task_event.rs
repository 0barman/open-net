use crate::api::traits::ws::ws_body::WsBody;
use crate::api::traits::ws::ws_request_trait::WSRequestTrait;
use crate::NetError;
use std::sync::Arc;

/// 限制完成回调尚未返回的任务数量及其正文总字节数。
///
/// 这些限制独立于网络写队列。紧急任务的预留容量从总容量中划出；
/// 未设置预留时，普通任务与紧急任务共享全部容量；设置预留后，两类任务各自使用固定容量。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WebSocketTaskEventOptions {
    pub(crate) max_tasks: usize,
    pub(crate) max_payload_bytes: usize,
    pub(crate) urgent_tasks: usize,
    pub(crate) urgent_payload_bytes: usize,
}

impl WebSocketTaskEventOptions {
    /// 设置通知总容量；无效的容量配置会在注册监听器时被拒绝。
    pub fn new(max_tasks: usize, max_payload_bytes: usize) -> Self {
        Self {
            max_tasks,
            max_payload_bytes,
            urgent_tasks: 0,
            urgent_payload_bytes: 0,
        }
    }

    /// 从总容量中为紧急消息划出专用容量。任务数和字节数两个上限必须同时非零，
    /// 或同时为零以共享容量；划分后仍须为普通任务保留非零容量。
    pub fn with_urgent_reserve(mut self, tasks: usize, payload_bytes: usize) -> Self {
        self.urgent_tasks = tasks;
        self.urgent_payload_bytes = payload_bytes;
        self
    }
}

/// 调用发送接口时由应用提供的原始值；引擎不会重新构造该值。
#[derive(Clone)]
#[non_exhaustive]
pub enum WebSocketTaskSource {
    Request(Arc<dyn WSRequestTrait>),
    Body(WsBody),
}

/// 任务结束前所处的本地处理阶段。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum WebSocketTaskPhase {
    WaitingForCapacity,
    Queued,
    Writing,
    AwaitingResponse,
}

/// 传输层已知的投递情况，与本地任务结束的原因相互独立。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum WebSocketTaskDelivery {
    NotStarted,
    Unknown,
    Written,
    ResponseClaimed,
}

/// 导致本任务结束的首个本地原因。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum WebSocketTaskEndCause {
    Completed,
    Disconnect,
    Shutdown,
    SendCancelled,
    Failure,
    EngineDropped,
}

/// 本地处理成功完成，并不代表应用的业务操作成功。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum WebSocketTaskSuccess {
    Written,
    ResponseClaimed,
}

/// 已接收任务的一次终态通知，在专用操作系统线程上交付。
///
/// `Written` 确认写入端已完成写入；`ResponseClaimed` 确认响应已关联到请求，不表示业务成功。
/// 取消等待响应不会撤回服务器已经收到的请求。
/// 即使复用了业务请求 ID，任务 ID 仍可区分不同的尝试。
pub struct WebSocketTaskEvent {
    pub(crate) client_instance_id: u64,
    pub(crate) task_id: u64,
    pub(crate) request_id: Option<String>,
    pub(crate) source: WebSocketTaskSource,
    pub(crate) is_urgent: bool,
    pub(crate) connection_generation: Option<u64>,
    pub(crate) session_context_id: Option<u64>,
    pub(crate) phase: WebSocketTaskPhase,
    pub(crate) delivery: WebSocketTaskDelivery,
    pub(crate) cause: WebSocketTaskEndCause,
    pub(crate) result: Result<WebSocketTaskSuccess, NetError>,
}

impl WebSocketTaskEvent {
    pub fn client_instance_id(&self) -> u64 {
        self.client_instance_id
    }

    pub fn task_id(&self) -> u64 {
        self.task_id
    }

    pub fn request_id(&self) -> Option<&str> {
        self.request_id.as_deref()
    }

    pub fn source(&self) -> &WebSocketTaskSource {
        &self.source
    }

    pub fn is_urgent(&self) -> bool {
        self.is_urgent
    }

    pub fn connection_generation(&self) -> Option<u64> {
        self.connection_generation
    }

    pub fn session_context_id(&self) -> Option<u64> {
        self.session_context_id
    }

    pub fn phase(&self) -> WebSocketTaskPhase {
        self.phase
    }

    pub fn delivery(&self) -> WebSocketTaskDelivery {
        self.delivery
    }

    pub fn cause(&self) -> WebSocketTaskEndCause {
        self.cause
    }

    pub fn result(&self) -> Result<WebSocketTaskSuccess, NetError> {
        self.result
    }
}

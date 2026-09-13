pub use crate::api::wsc::pending_request_info::PendingRequestInfo;
pub use crate::api::wsc::pending_request_status::PendingRequestStatus;
pub use crate::api::wsc::pending_request_view::PendingRequestView;
use crate::common::log::log_def::LogType;

use crate::api::traits::ws::ws_request_trait::WSRequestTrait;
use crate::api::wsc::pending_request_view::PendingResponseGuard;
use crate::api::wsc::request_registration::RequestRegistration;
use std::sync::Arc;
use std::time::SystemTime;
use tokio_tungstenite::tungstenite::Message;

pub struct WSCResponse {
    message: Message,
    pending_requests: PendingRequestView,
    received_at: SystemTime,
    connection_generation: u64,
    _dispatch_guard: PendingResponseGuard,
}

impl WSCResponse {
    pub(crate) fn new(
        message: Message,
        pending_requests: PendingRequestView,
        connection_generation: u64,
    ) -> Self {
        crate::log_t!(LogType::WSC; "new", "message_bytes|pending_requests|connection_generation", message.len(), "PendingRequestView", connection_generation);
        let dispatch_guard = pending_requests.begin_response_dispatch(connection_generation);
        Self {
            message,
            pending_requests,
            received_at: SystemTime::now(),
            connection_generation,
            _dispatch_guard: dispatch_guard,
        }
    }

    pub fn message(&self) -> &Message {
        crate::log_t!(LogType::WSC; "message");
        &self.message
    }

    pub fn into_message(self) -> Message {
        crate::log_t!(LogType::WSC; "into_message");
        self.message
    }

    pub fn pending_requests(&self) -> PendingRequestView {
        crate::log_t!(LogType::WSC; "pending_requests");
        self.pending_requests.clone()
    }

    /// 获取关联请求的引用而不移除请求，查找范围限于本响应所属的连接代次。
    /// 请求绑定的业务 scope 已撤销时返回 `None`。
    pub fn get_request(&self, uuid: &str) -> Option<Arc<dyn WSRequestTrait>> {
        crate::log_t!(LogType::WSC; "get_request", "uuid", uuid);
        self.pending_requests
            .get_request(uuid, self.connection_generation)
    }

    /// 原子取出本响应所属连接代次的请求；请求绑定的业务 scope 已撤销时返回 `None`。
    pub fn take_request(&self, uuid: &str) -> Option<Arc<dyn WSRequestTrait>> {
        crate::log_t!(LogType::WSC; "take_request", "uuid", uuid);
        self.pending_requests
            .take_request(uuid, self.connection_generation)
    }

    /// Atomically claim the original registration only within this response's connection.
    ///
    /// The original pending table, registration token, physical generation and live request
    /// scope must all match. A missing, stale or foreign registration returns `Ok(None)`.
    /// Tokens are local authority: they cannot identify a late server ACK containing only a
    /// UUID that has been reused for a different logical operation.
    pub fn take_request_if_registered(
        &self,
        registration: &RequestRegistration,
    ) -> Result<Option<Arc<dyn WSRequestTrait>>, crate::NetError> {
        self.pending_requests
            .take_registered_request(registration, self.connection_generation)
    }

    pub fn received_at(&self) -> SystemTime {
        crate::log_t!(LogType::WSC; "received_at");
        self.received_at
    }

    pub fn connection_generation(&self) -> u64 {
        crate::log_t!(LogType::WSC; "connection_generation");
        self.connection_generation
    }
}

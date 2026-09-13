use crate::api::traits::ws::ws_request_config::WSRequestPriority;
use crate::api::wsc::pending_request_status::PendingRequestStatus;
use std::collections::HashMap;
use std::time::Instant;

#[derive(Clone, Debug)]
pub struct PendingRequestInfo {
    pub uuid: String,
    pub extension: HashMap<String, String>,
    pub priority: WSRequestPriority,
    pub status: PendingRequestStatus,
    pub queued_at: Instant,
    pub sent_at: Option<Instant>,
    pub send_attempt: u32,
    /// 当前请求实际开始写入的物理连接代次；尚未开始写入时为 `None`。
    pub connection_generation: Option<u64>,
}

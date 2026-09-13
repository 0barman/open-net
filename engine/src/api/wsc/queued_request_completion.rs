use crate::api::net_error::NetError;
use crate::api::wsc::pending_request_completion::PendingRequestCompletion;
use on_common::log::log_def::LogType;
use tokio::sync::oneshot;

/// 同步非阻塞入队后，可转交给异步执行器等待的请求回执。
///
/// `WebSocketClient::try_send_*_with_completion` 在本地入队成功后立即返回本类型，创建
/// 过程不要求调用线程存在 Tokio 运行时。调用方随后应把它交给已有运行时或通道，
/// 先调用 [`Self::wait_until_written`]；写入端确认写入或 Writing 阶段响应已被认领后得到
/// 普通 [`PendingRequestCompletion`]，再等待响应认领、响应超时或连接终止。
///
/// 丢弃回执不会取消已经入队的请求。它只会放弃观察结果，队列、待响应请求表及工作任务
/// 仍按正常生命周期清理资源。
#[must_use = "the receipt must be awaited or intentionally dropped"]
pub struct QueuedRequestCompletion {
    request_id: String,
    write_result: oneshot::Receiver<Result<(), NetError>>,
    pending_completion: PendingRequestCompletion,
}

impl QueuedRequestCompletion {
    pub(crate) fn new(
        write_result: oneshot::Receiver<Result<(), NetError>>,
        pending_completion: PendingRequestCompletion,
    ) -> Self {
        on_common::log_t!(LogType::WSC; "new", "write_result|pending_completion", "oneshot::Receiver", "PendingRequestCompletion");
        Self {
            request_id: pending_completion.request_id().to_string(),
            write_result,
            pending_completion,
        }
    }

    /// 返回回执对应的请求 UUID。
    pub fn request_id(&self) -> &str {
        on_common::log_t!(LogType::WSC; "request_id");
        self.request_id.as_str()
    }

    /// 等待队列中的请求完成 WebSocket 写入端的写入。
    ///
    /// 成功后返回响应阶段的唯一完成通知。成功可能来自写入端完成写入，也可能来自业务
    /// 响应在随后投递结果不明确的写缓冲刷新失败之前已被认领；后一种情况下连接仍会废弃。
    /// 失败表示既没有确认写入，也没有已认领响应；对应待响应请求已由写任务或生命周期流程
    /// 清理，不会遗留等待响应记录。
    pub async fn wait_until_written(self) -> Result<PendingRequestCompletion, NetError> {
        on_common::log_t!(LogType::WSC; "wait_until_written");
        match self.write_result.await {
            Ok(Ok(())) => {
                on_common::log_s!(LogType::WSC; "wait_until_written", "request_id|state", self.request_id, "write_or_response_confirmed");
                Ok(self.pending_completion)
            }
            Ok(Err(NetError::Cancelled)) => {
                on_common::log_s!(LogType::WSC; "wait_until_written", "request_id|state", self.request_id, "cancelled");
                Err(NetError::Cancelled)
            }
            Ok(Err(error)) => {
                on_common::log_e!(LogType::WSC; "wait_until_written", "request_id|error", self.request_id, format!("{:?}", error));
                Err(error)
            }
            Err(_) => {
                on_common::log_e!(LogType::WSC; "wait_until_written", "request_id|error", self.request_id, "EngineDropped");
                Err(NetError::EngineDropped)
            }
        }
    }
}

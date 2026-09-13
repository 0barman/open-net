use crate::api::net_error::NetError;
use crate::common::log::log_def::LogType;
use tokio::sync::oneshot;

/// 一次待响应请求的终态通知。
///
/// 此句柄会在写入端确认写入，或业务响应已在 Writing 阶段被监听器成功认领后返回。
/// `wait` 恰好观察以下首个后续终态之一：响应监听器成功认领请求、响应关联超时、连接
/// 代次终止、主动断开或关闭。既无确认写入也无已认领响应时的写入失败，以及句柄
/// 返回前发送异步操作被取消的情况，由 `send_with_completion` 本身承担。
/// 句柄不携带协议响应正文；正文仍由 `WSCResponse` 交给应用解析，成功认领由
/// `WSCResponse::take_request` 定义。
pub struct PendingRequestCompletion {
    request_id: String,
    receiver: oneshot::Receiver<Result<(), NetError>>,
}

impl PendingRequestCompletion {
    pub(in crate::api::wsc) fn new(
        request_id: String,
        receiver: oneshot::Receiver<Result<(), NetError>>,
    ) -> Self {
        crate::log_t!(LogType::WSC; "new", "request_id|receiver", request_id, "oneshot::Receiver");
        Self {
            request_id,
            receiver,
        }
    }

    /// 返回该完成通知对应的请求 UUID。
    pub fn request_id(&self) -> &str {
        crate::log_t!(LogType::WSC; "request_id");
        self.request_id.as_str()
    }

    /// 等待请求被响应认领或以错误结束。
    ///
    /// 返回 `Ok(())` 表示数据监听器已经通过匹配连接代次调用
    /// `WSCResponse::take_request` 认领请求；它不表示业务响应内容本身成功。
    pub async fn wait(self) -> Result<(), NetError> {
        crate::log_t!(LogType::WSC; "wait");
        let result = self.receiver.await.unwrap_or(Err(NetError::EngineDropped));
        match result {
            Ok(()) => {
                crate::log_s!(LogType::WSC; "wait", "request_id|state", self.request_id, "response_claimed")
            }
            Err(NetError::Cancelled) => {
                crate::log_s!(LogType::WSC; "wait", "request_id|state", self.request_id, "cancelled")
            }
            Err(error) => {
                crate::log_e!(LogType::WSC; "wait", "request_id|error", self.request_id, format!("{:?}", error))
            }
        }
        result
    }
}

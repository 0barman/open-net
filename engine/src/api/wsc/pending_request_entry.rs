use crate::api::traits::ws::ws_request_trait::WSRequestTrait;
use crate::api::wsc::pending_request_info::PendingRequestInfo;
use crate::api::wsc::request_registration::{RegistrationControl, RegistrationState};
use crate::api::wsc::request_scope::RequestScope;
use crate::api::wsc::{WebSocketTaskEndCause, WebSocketTaskSuccess};
use crate::module::ws_client::task_observer::TaskObservation;
use crate::module::ws_client::write::queued_request::DeferredWriteCompletion;
use on_common::log::log_def::LogType;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

pub(in crate::api::wsc) struct PendingRequestEntry {
    pub(in crate::api::wsc) request: Arc<dyn WSRequestTrait>,
    pub(in crate::api::wsc) info: PendingRequestInfo,
    pub(in crate::api::wsc) token: u64,
    pub(in crate::api::wsc) completion_tx: Option<oneshot::Sender<Result<(), crate::NetError>>>,
    /// 请求进入响应认领宽限时最先观察到的错误终态。
    pub(in crate::api::wsc) first_deferred_error: Option<crate::NetError>,
    /// 请求记录进入终态后，立即取消其响应超时任务。
    pub(in crate::api::wsc) response_timeout_cancel: CancellationToken,
    pub(in crate::api::wsc) observation: Option<Arc<TaskObservation>>,
    pub(in crate::api::wsc) registration_control: Option<RegistrationControl>,
    pub(in crate::api::wsc) scope: Option<RequestScope>,
    pub(in crate::api::wsc) registration_state: Arc<RegistrationState>,
    pub(in crate::api::wsc) registration_grace_deadline: Option<std::time::Instant>,
    pub(in crate::api::wsc) deferred_write: Option<DeferredWriteCompletion>,
}

impl PendingRequestEntry {
    pub(in crate::api::wsc) fn record_deferred_error(
        &mut self,
        error: crate::NetError,
    ) -> crate::NetError {
        if let Some(first) = self.first_deferred_error {
            return first;
        }
        self.first_deferred_error = Some(error);
        if let Some(observation) = &self.observation {
            let cause = match error {
                crate::NetError::Cancelled => WebSocketTaskEndCause::SendCancelled,
                crate::NetError::EngineDropped => WebSocketTaskEndCause::EngineDropped,
                _ => WebSocketTaskEndCause::Failure,
            };
            // 主动断开或关闭流程已在取消请求准入前记录终止原因。
            observation.set_cause(cause);
        }
        error
    }

    /// 以唯一终态完成通知，并返回请求所有权。
    pub(in crate::api::wsc) fn complete(
        mut self,
        result: Result<(), crate::NetError>,
    ) -> Arc<dyn WSRequestTrait> {
        on_common::log_t!(LogType::WSC; "complete", "result", format!("{:?}", result));
        let mut result = match result {
            Ok(()) => Ok(()),
            Err(error) => Err(self.first_deferred_error.unwrap_or(error)),
        };
        if let (Err(error), Some(control)) = (result, &self.registration_control) {
            result = Err(control.record_terminal_error(error));
        }
        match result {
            Ok(()) => {
                on_common::log_s!(LogType::WSC; "complete", "uuid|token|state", self.info.uuid, self.token, "response_claimed")
            }
            Err(crate::NetError::Cancelled) => {
                on_common::log_s!(LogType::WSC; "complete", "uuid|token|state", self.info.uuid, self.token, "cancelled")
            }
            Err(error) => {
                on_common::log_e!(LogType::WSC; "complete", "uuid|token|error", self.info.uuid, self.token, format!("{:?}", error))
            }
        }
        self.registration_state.finish(result);
        self.response_timeout_cancel.cancel();
        if let Some(write) = self.deferred_write.take() {
            write.complete(result);
        }
        if let Some(observation) = self.observation.take() {
            observation.finish(result.map(|()| WebSocketTaskSuccess::ResponseClaimed));
            if result.is_err() && self.registration_control.is_none() {
                if let Some(Err(selected)) = observation.selected_result() {
                    result = Err(selected);
                }
            }
        }
        if let Some(completion_tx) = self.completion_tx.take() {
            if completion_tx.send(result).is_err() {
                on_common::log_s!(LogType::WSC; "complete", "uuid|token|state", self.info.uuid, self.token, "completion_receiver_dropped");
            }
        }
        self.request
    }
}

use crate::module::ws_client::operation_control::OperationControl;
use crate::ws::{DisconnectedPolicy, Priority, RequestRegistration, SendOptions, TaskEndCause};
use crate::{NetError, Result};
use std::cmp::Ordering;
use std::sync::Arc;
use tokio::sync::OwnedSemaphorePermit;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

pub(crate) struct QueuedRequest {
    pub(crate) registration: Option<RequestRegistration>,
    pub(crate) message: Message,
    pub(crate) config: SendOptions,
    pub(crate) attempt: u32,
    pub(crate) ready_at: Option<std::time::Instant>,
    pub(crate) dispatch_cancel: CancellationToken,
    pub(crate) dispatch_phase: Arc<OperationControl>,
    pub(crate) admission_cancel: Option<CancellationToken>,
    pub(crate) sequence: u64,
    pub(crate) slot_permit: Option<OwnedSemaphorePermit>,
    pub(crate) byte_permit: Option<OwnedSemaphorePermit>,
    pub(crate) completed: bool,
    pub(crate) retirement: Option<crate::module::ws_client::native_pending::DeferredTermination>,
}
impl QueuedRequest {
    pub(crate) fn prepare_retirement(&mut self, error: &NetError) {
        if self.retirement.is_none() && !self.completed {
            let cause = match error.kind() {
                crate::error::ErrorKind::Cancelled => TaskEndCause::Cancelled,
                crate::error::ErrorKind::TimedOut => TaskEndCause::Expired,
                crate::error::ErrorKind::Closed | crate::error::ErrorKind::NotConnected => {
                    TaskEndCause::Disconnected
                }
                crate::error::ErrorKind::EngineDropped => TaskEndCause::Shutdown,
                _ => TaskEndCause::Failed,
            };
            self.retirement = Some(
                self.dispatch_phase
                    .prepare_termination(error.clone(), cause),
            );
        }
    }
    /// Called only after Q is released. Capacity returns before any notification.
    pub(crate) fn dispatch_retirement(&mut self) {
        if let Some(retirement) = self.retirement.take() {
            self.completed = true;
            drop((self.slot_permit.take(), self.byte_permit.take()));
            retirement.dispatch();
        }
    }
    pub(crate) fn complete(mut self, result: Result<()>) {
        let arg_result_kind = result.as_ref().err().map(NetError::kind);
        let arg_result_stage = result
            .as_ref()
            .err()
            .and_then(|error| error.context().stage);
        self.dispatch_retirement();
        if self.completed {
            return;
        }
        self.completed = true;
        drop((self.slot_permit.take(), self.byte_permit.take()));
        #[cfg(feature = "quality-doctor")]
        if let Some(probe) = self.dispatch_phase.quality() {
            probe.mark(crate::api::quality::PhaseKind::ResourcesReleased);
        }
        let result = match (result, self.registration.as_ref()) {
            (Ok(()), Some(registration)) => match registration.pending().upgrade() {
                Some(pending) => pending
                    .mark_written(registration, std::time::Instant::now())
                    .map(|_| ()),
                None if self.dispatch_phase.response_confirmed() => Ok(()),
                None => Err(NetError::from(crate::error::ErrorKind::EngineDropped)),
            },
            (Ok(()), None) => {
                let (_, publication) = self.dispatch_phase.select_written();
                publication.dispatch();
                Ok(())
            }
            (Err(error), _) => Err(error),
        };
        if let Err(error) = result {
            let cause = match error.kind() {
                crate::error::ErrorKind::Cancelled => TaskEndCause::Cancelled,
                crate::error::ErrorKind::TimedOut => TaskEndCause::Expired,
                crate::error::ErrorKind::Closed => TaskEndCause::Disconnected,
                crate::error::ErrorKind::EngineDropped => TaskEndCause::Shutdown,
                _ => TaskEndCause::Failed,
            };
            if let Err(error) = self.dispatch_phase.terminate(error, cause) {
                crate::log_e!(crate::common::log::log_def::LogType::WSC; "ws.queued_request.complete", "arg_result_kind|arg_result_stage|kind|stage", arg_result_kind.map(|kind| format!("{kind:?}")), arg_result_stage.map(|stage| format!("{stage:?}")), format!("{:?}", error.kind()), error.context().stage.map(|stage| format!("{stage:?}")));
            }
        }
    }
    pub(crate) fn release(mut self) {
        self.completed = true;
        drop((self.slot_permit.take(), self.byte_permit.take()));
    }
    pub(crate) fn terminal_error(&self, fallback: NetError) -> NetError {
        self.dispatch_phase
            .selected_error()
            .map_or(fallback, |error| error)
    }
    pub(crate) fn can_wait_for_reconnect(&self) -> bool {
        self.config.disconnected == DisconnectedPolicy::WaitForReconnect
            || (self.attempt > 0 && self.dispatch_phase.raw_replay_allowed())
    }
}
impl Drop for QueuedRequest {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        drop((self.slot_permit.take(), self.byte_permit.take()));
        if let Err(error) = self.dispatch_phase.terminate(
            NetError::from(crate::error::ErrorKind::EngineDropped),
            TaskEndCause::Shutdown,
        ) {
            crate::log_e!(crate::common::log::log_def::LogType::WSC; "dropped_operation", "error", format!("{error:?}"));
        }
    }
}
impl PartialEq for QueuedRequest {
    fn eq(&self, other: &Self) -> bool {
        self.sequence == other.sequence
    }
}
impl Eq for QueuedRequest {}
impl PartialOrd for QueuedRequest {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for QueuedRequest {
    fn cmp(&self, other: &Self) -> Ordering {
        let rank = |priority| match priority {
            Priority::Low => 0u8,
            Priority::Normal => 1,
            Priority::High => 2,
        };
        rank(self.config.priority)
            .cmp(&rank(other.config.priority))
            .then_with(|| other.sequence.cmp(&self.sequence))
    }
}

#[cfg(test)]
#[path = "message_control_tests.rs"]
mod message_control_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::module::ws_client::{
        test_support::{check, check_eq, TestResult},
        v2_test_support as fixture,
    };
    use crate::ws::{DeliveryEvidence, OperationPhase};
    #[test]
    fn retry_and_disconnect_policy_are_independent_and_bounded() -> TestResult {
        use crate::api::retry::{RetrySchedule, RetrySetting};
        use crate::ws::{WsReplaySafety, WsSendRetryOn, WsSendRetryPolicy};
        let policy = WsSendRetryPolicy::builder()
            .schedule(RetrySchedule::builder().max_attempts(2).build()?)
            .retry_on(WsSendRetryOn::none().with_write_errors(true))
            .safety(WsReplaySafety::IdempotentByContract)
            .build()?;
        let mut request = fixture::queued(
            1,
            SendOptions {
                retry_setting: RetrySetting::Policy(policy),
                ..Default::default()
            },
        )?;
        check!(!request.can_wait_for_reconnect())?;
        let control = &request.dispatch_phase;
        check!(control.mark_writing(crate::ws::ConnectionId::from_allocated(1)))?;
        check!(control.start_data_write(false)?)?;
        check!(control
            .raw_retry_after(&NetError::from(crate::error::ErrorKind::Io))?
            .is_some())?;
        check!(control.requeue())?;
        request.attempt = 1;
        check!(request.can_wait_for_reconnect())?;
        check!(control.mark_writing(crate::ws::ConnectionId::from_allocated(2)))?;
        check!(control.start_data_write(false)?)?;
        check!(control
            .raw_retry_after(&NetError::from(crate::error::ErrorKind::Io))?
            .is_none())?;
        check_eq!(control.retry_report().attempts, 2)?;
        check_eq!(control.retry_report().transport_sends, 2)?;
        Ok(())
    }
    #[test]
    fn classifier_decisions_respect_disable_budget_and_replay_safety() -> TestResult {
        use crate::api::retry::{RetryDecision, RetrySchedule, RetrySetting, RetryStopReason};
        use crate::ws::{WsReplaySafety, WsSendRetryOn, WsSendRetryPolicy};
        for (decision, safe, max_attempts, expected) in [
            (
                RetryDecision::Stop,
                true,
                2,
                RetryStopReason::ClassifierStopped,
            ),
            (
                RetryDecision::Retry,
                false,
                2,
                RetryStopReason::UnsafeToReplay,
            ),
            (
                RetryDecision::Retry,
                true,
                1,
                RetryStopReason::AttemptsExhausted,
            ),
        ] {
            let policy = WsSendRetryPolicy::builder()
                .schedule(
                    RetrySchedule::builder()
                        .max_attempts(max_attempts)
                        .build()?,
                )
                .retry_on(WsSendRetryOn::none().with_write_errors(true))
                .safety(if safe {
                    WsReplaySafety::IdempotentByContract
                } else {
                    WsReplaySafety::Never
                })
                .classifier(move |_| decision)
                .build()?;
            let request = fixture::queued(
                1,
                SendOptions {
                    retry_setting: RetrySetting::Policy(policy),
                    ..Default::default()
                },
            )?;
            let control = request.dispatch_phase.clone();
            check!(control.mark_writing(crate::ws::ConnectionId::from_allocated(1)))?;
            check!(control.start_data_write(false)?)?;
            let error = NetError::from(crate::error::ErrorKind::Io);
            check!(control.raw_retry_after(&error)?.is_none())?;
            request.complete(Err(error));
            check_eq!(control.retry_report().stop_reason, Some(expected))?;
        }
        let request = fixture::queued(
            2,
            SendOptions {
                retry_setting: RetrySetting::Disabled,
                ..Default::default()
            },
        )?;
        let control = request.dispatch_phase.clone();
        check!(control.mark_writing(crate::ws::ConnectionId::from_allocated(1)))?;
        check!(control.start_data_write(false)?)?;
        let error = NetError::from(crate::error::ErrorKind::Io);
        check!(control.raw_retry_after(&error)?.is_none())?;
        request.complete(Err(error));
        check_eq!(
            control.retry_report().stop_reason,
            Some(RetryStopReason::Disabled)
        )?;
        Ok(())
    }

    #[test]
    fn dropping_started_work_preserves_unknown_delivery_and_selects_terminal_once() -> TestResult {
        let request = fixture::queued(1, SendOptions::default())?;
        let control = request.dispatch_phase.clone();
        check!(control.mark_writing(crate::ws::ConnectionId::from_allocated(1)))?;
        check!(control.start_data_write(false)?)?;
        drop(request);
        check_eq!(control.snapshot()?.phase, OperationPhase::Finished)?;
        check_eq!(control.snapshot()?.delivery, DeliveryEvidence::Unknown)?;
        check_eq!(
            control
                .snapshot()?
                .result
                .and_then(|r| r.err())
                .map(|e| e.kind()),
            Some(crate::error::ErrorKind::DeliveryUnknown)
        )?;
        Ok(())
    }
}

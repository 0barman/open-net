use crate::module::ws_client::operation_control::OperationControl;
use crate::ws::{
    DisconnectedPolicy, Priority, RequestRegistration, SendOptions, SendRetryPolicy, TaskEndCause,
};
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
    pub(crate) dispatch_cancel: CancellationToken,
    pub(crate) dispatch_phase: Arc<OperationControl>,
    pub(crate) admission_cancel: Option<CancellationToken>,
    pub(crate) sequence: u64,
    pub(crate) slot_permit: Option<OwnedSemaphorePermit>,
    pub(crate) byte_permit: Option<OwnedSemaphorePermit>,
    pub(crate) completed: bool,
}
impl QueuedRequest {
    pub(crate) fn complete(mut self, result: Result<()>) {
        self.completed = true;
        drop((self.slot_permit.take(), self.byte_permit.take()));
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
                crate::log_e!(crate::common::log::log_def::LogType::WSC; "complete_operation", "error", format!("{error:?}"));
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
            || (self.attempt > 0 && matches!(self.config.retry, SendRetryPolicy::Idempotent { .. }))
    }
    pub(crate) fn can_retry(&self) -> bool {
        matches!(self.config.retry, SendRetryPolicy::Idempotent { max_retries } if self.attempt < max_retries)
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
        let mut request = fixture::queued(
            1,
            SendOptions {
                retry: SendRetryPolicy::Idempotent { max_retries: 2 },
                ..Default::default()
            },
        )?;
        check!(request.can_retry())?;
        check!(!request.can_wait_for_reconnect())?;
        request.attempt = 1;
        check!(request.can_wait_for_reconnect())?;
        request.attempt = 2;
        check!(!request.can_retry())?;
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

//! The single state and completion authority shared by writer and receipts.
use crate::common::log::log_def::LogType;
use crate::error::{ErrorKind, NetError};
use crate::module::ws_client::native_pending::NativePending;
use crate::module::ws_client::native_task_observer::TaskEventReservation;
use crate::module::ws_client::write::priority_write_queue::PriorityWriteQueue;
use crate::ws::cancellation::CancelHookGuard;
use crate::ws::{
    CancellationGroup, ClientId, ConnectionId, DeliveryEvidence, MessageLane, OperationId,
    OperationPhase, OperationSnapshot, RequestId, SendOptions, SessionId, TaskEndCause, TaskEvent,
    TaskSource, TaskSuccess, TerminationOutcome, WriteOutcome,
};
use crate::Result;
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::Instant;
use tokio_util::sync::CancellationToken;

struct Observation {
    source: TaskSource,
    reservation: TaskEventReservation,
}

#[derive(Clone)]
struct PendingIdentity {
    table: Weak<NativePending>,
    id: RequestId,
    token: u64,
}

struct OperationState {
    snapshot: OperationSnapshot,
    connection: Option<ConnectionId>,
    response_deadline: Option<Instant>,
    observation: Option<Observation>,
    queue: Weak<PriorityWriteQueue>,
    pending: Option<PendingIdentity>,
    retirement: Option<CancellationToken>,
    hook: Option<CancelHookGuard>,
    data_in_flight: bool,
}

pub(crate) struct OperationControl {
    client_id: ClientId,
    session_id: SessionId,
    id: OperationId,
    lane: MessageLane,
    tracked: bool,
    deadline: Option<Instant>,
    group: Option<CancellationGroup>,
    state: Mutex<OperationState>,
    cancelled: CancellationToken,
    written: CancellationToken,
}

/// This value owns every side effect selected while a caller may hold a table lock.
/// Dispatch it after releasing all caller locks, on successful and failed decisions.
#[must_use]
#[derive(Default)]
pub(crate) struct OperationPublication {
    task: Option<(TaskEventReservation, TaskEvent)>,
    cancelled: Option<CancellationToken>,
    written: Option<CancellationToken>,
    retirement: Option<CancellationToken>,
    queue: Option<(Weak<PriorityWriteQueue>, CancellationToken, NetError)>,
    hook: Option<CancelHookGuard>,
    retired_error: Option<NetError>,
}

impl OperationPublication {
    pub(crate) fn dispatch(self) {
        let Self {
            task,
            cancelled,
            written,
            retirement,
            queue,
            hook,
            retired_error,
        } = self;
        drop((hook, retired_error));
        if let Some(token) = retirement {
            token.cancel();
        }
        if let Some(token) = cancelled {
            token.cancel();
        }
        if let Some((queue, token, error)) = queue {
            if let Some(queue) = queue.upgrade() {
                queue.cancel_queued_with_error(&token, error);
            }
        }
        if let Some((reservation, event)) = task {
            if let Err(error) = reservation.publish(event) {
                crate::log_e!(LogType::WSC; "operation_task_delivery", "error", format!("{error:?}"));
            }
        }
        if let Some(token) = written {
            token.cancel();
        }
    }
}

impl OperationControl {
    pub(crate) fn new(
        client_id: ClientId,
        session_id: SessionId,
        id: OperationId,
        options: &SendOptions,
        tracked: bool,
        source: TaskSource,
        reservation: TaskEventReservation,
    ) -> Arc<Self> {
        Arc::new(Self {
            client_id,
            session_id,
            id,
            lane: options.lane,
            tracked,
            deadline: options.deadline,
            group: options.cancellation.clone(),
            state: Mutex::new(OperationState {
                snapshot: OperationSnapshot {
                    phase: OperationPhase::WaitingForCapacity,
                    delivery: DeliveryEvidence::NotStarted,
                    result: None,
                },
                connection: None,
                response_deadline: None,
                observation: Some(Observation {
                    source,
                    reservation,
                }),
                queue: Weak::new(),
                pending: None,
                retirement: None,
                hook: None,
                data_in_flight: false,
            }),
            cancelled: CancellationToken::new(),
            written: CancellationToken::new(),
        })
    }

    pub(crate) fn id(&self) -> OperationId {
        self.id
    }
    pub(crate) fn session_id(&self) -> SessionId {
        self.session_id
    }
    pub(crate) fn has_request_registration(&self) -> bool {
        self.lock().pending.is_some()
    }
    pub(crate) fn cancel_token(&self) -> CancellationToken {
        self.cancelled.clone()
    }
    pub(crate) fn cancel_domain(&self) -> Option<&CancellationGroup> {
        self.group.as_ref()
    }
    pub(crate) fn absolute_deadline(&self) -> Option<Instant> {
        self.deadline
    }
    pub(crate) fn deadline(&self) -> Option<Instant> {
        earliest(self.deadline, self.lock().response_deadline)
    }
    pub(crate) fn set_response_deadline(&self, deadline: Option<Instant>) {
        self.lock().response_deadline = deadline;
    }
    pub(crate) fn bind_request(
        &self,
        table: Weak<NativePending>,
        id: RequestId,
        token: u64,
    ) -> Result<()> {
        let mut state = self.state.lock().map_err(NetError::from_poison)?;
        self.check_active(&state)?;
        if state.pending.is_some() {
            return Err(NetError::from(ErrorKind::Internal));
        }
        state.pending = Some(PendingIdentity { table, id, token });
        Ok(())
    }
    pub(crate) fn bind_queue(&self, queue: Weak<PriorityWriteQueue>) {
        self.lock().queue = queue;
    }
    pub(crate) fn bind_cancellation(self: &Arc<Self>) -> Result<()> {
        if let Some(group) = &self.group {
            let weak = Arc::downgrade(self);
            let hook = group.inner.bind_cancel_hook(Arc::new(move || {
                if let Some(control) = weak.upgrade() {
                    if let Err(error) = control.cancel() {
                        crate::log_e!(LogType::WSC; "operation_cancel", "error", format!("{error:?}"));
                    }
                }
            }))?;
            let mut state = self.lock();
            let retired = if state.snapshot.result.is_none() {
                state.hook.replace(hook)
            } else {
                Some(hook)
            };
            drop(state);
            drop(retired);
        }
        Ok(())
    }
    pub(crate) fn snapshot(&self) -> Result<OperationSnapshot> {
        Ok(self
            .state
            .lock()
            .map_err(NetError::from_poison)?
            .snapshot
            .clone())
    }
    pub(crate) fn is_finished(&self) -> bool {
        self.lock().snapshot.result.is_some()
    }
    pub(crate) fn response_confirmed(&self) -> bool {
        self.lock().snapshot.delivery == DeliveryEvidence::ResponseConfirmed
    }
    pub(crate) fn selected_error(&self) -> Option<NetError> {
        self.lock()
            .snapshot
            .result
            .as_ref()
            .and_then(|result| result.as_ref().err().cloned())
    }
    pub(crate) fn prepare(&self) -> Result<()> {
        let mut state = self.state.lock().map_err(NetError::from_poison)?;
        self.check_active(&state)?;
        if state.snapshot.phase != OperationPhase::WaitingForCapacity {
            return Err(NetError::from(ErrorKind::Internal));
        }
        state.snapshot.phase = OperationPhase::Prepared;
        Ok(())
    }
    pub(crate) fn enqueue(&self) -> Result<()> {
        let mut state = self.state.lock().map_err(NetError::from_poison)?;
        self.check_active(&state)?;
        if !matches!(
            state.snapshot.phase,
            OperationPhase::Prepared | OperationPhase::WaitingForCapacity
        ) {
            return Err(NetError::from(ErrorKind::InvalidInput));
        }
        state.snapshot.phase = OperationPhase::Queued;
        Ok(())
    }
    pub(crate) fn mark_writing(&self, connection: ConnectionId) -> bool {
        let mut state = self.lock();
        if self.check_active(&state).is_err() || state.snapshot.phase != OperationPhase::Queued {
            return false;
        }
        state.snapshot.phase = OperationPhase::Writing;
        state.connection = Some(connection);
        true
    }
    pub(crate) fn start_data_write(&self, continuation: bool) -> Result<bool> {
        // Response expiry belongs to the pending table: an already dispatched
        // Manual response may still own its one bounded claim grace. Refresh it
        // before taking the cancellation gate or operation lock.
        let pending = {
            let state = self.lock();
            if state.snapshot.result.is_none()
                && state
                    .response_deadline
                    .is_some_and(|deadline| Instant::now() >= deadline)
            {
                state.pending.clone()
            } else {
                None
            }
        };
        if let Some(identity) = pending {
            let table = identity
                .table
                .upgrade()
                .ok_or_else(|| NetError::from(ErrorKind::EngineDropped))?;
            table.refresh_response_deadline(&identity.id, identity.token)?;
        }
        let domain = match self
            .group
            .as_ref()
            .map(|group| group.inner.lock_if_active())
            .transpose()
        {
            Ok(guard) => guard,
            Err(_) => return Ok(false),
        };
        let mut state = self.lock();
        // A successful early response settles the request, not the framing of
        // its current wire message. Finish continuation frames without reopening
        // the terminal result; writer and connection deadlines remain in force.
        let confirmed_continuation = continuation
            && state.snapshot.delivery == DeliveryEvidence::ResponseConfirmed
            && matches!(
                state.snapshot.result,
                Some(Ok(TaskSuccess::ResponseReceived))
            );
        let active = confirmed_continuation
            || (self.check_active_until(&state, self.deadline).is_ok()
                && state.snapshot.phase == OperationPhase::Writing);
        if active {
            state.data_in_flight = true;
            if state.snapshot.delivery == DeliveryEvidence::NotStarted {
                state.snapshot.delivery = DeliveryEvidence::Unknown;
            }
        }
        drop(state);
        drop(domain);
        Ok(active)
    }
    pub(crate) fn data_write_started(&self) -> bool {
        self.lock().data_in_flight
    }
    pub(crate) fn has_in_flight_data(&self) -> bool {
        let state = self.lock();
        state.data_in_flight
            && state.snapshot.delivery != DeliveryEvidence::ResponseConfirmed
            && state.snapshot.delivery != DeliveryEvidence::Written
    }
    pub(crate) fn bind_write_retirement(&self, retirement: CancellationToken) {
        let mut state = self.lock();
        let cancelled =
            state.data_in_flight && state.snapshot.result.as_ref().is_some_and(Result::is_err);
        let previous = state.retirement.replace(retirement.clone());
        drop(state);
        drop(previous);
        if cancelled {
            retirement.cancel();
        }
    }
    pub(crate) fn retire_write(&self) {
        let token = self.lock().retirement.clone();
        if let Some(token) = token {
            token.cancel();
        }
    }
    pub(crate) fn requeue(&self) -> bool {
        let mut state = self.lock();
        if self.check_active(&state).is_err() || state.snapshot.phase != OperationPhase::Writing {
            return false;
        }
        state.snapshot.phase = OperationPhase::Queued;
        state.data_in_flight = false;
        let retired = state.retirement.take();
        drop(state);
        drop(retired);
        true
    }
    pub(crate) fn select_written(&self) -> (bool, OperationPublication) {
        let _domain_guard = match self
            .group
            .as_ref()
            .map(|group| group.inner.lock_if_active())
            .transpose()
        {
            Ok(guard) => guard,
            Err(error) => return (false, self.select_failure(error, TaskEndCause::Cancelled).1),
        };
        let mut publication = OperationPublication::default();
        let mut state = self.lock();
        if state.snapshot.result.is_some() {
            return (
                state.snapshot.delivery == DeliveryEvidence::ResponseConfirmed,
                publication,
            );
        }
        if let Err(error) = self.check_active(&state) {
            drop(state);
            return (false, self.select_failure(error, TaskEndCause::Expired).1);
        }
        state.snapshot.delivery = DeliveryEvidence::Written;
        state.data_in_flight = false;
        publication.written = Some(self.written.clone());
        if self.tracked {
            state.snapshot.phase = OperationPhase::AwaitingResponse;
        } else {
            self.finish_locked(
                &mut state,
                Ok(TaskSuccess::Written),
                TaskEndCause::Completed,
                &mut publication,
            );
        }
        (true, publication)
    }
    pub(crate) fn select_response(&self) -> (bool, OperationPublication) {
        let _domain_guard = match self
            .group
            .as_ref()
            .map(|group| group.inner.lock_if_active())
            .transpose()
        {
            Ok(guard) => guard,
            Err(error) => return (false, self.select_failure(error, TaskEndCause::Cancelled).1),
        };
        let mut publication = OperationPublication::default();
        let mut state = self.lock();
        if state.snapshot.result.is_some() {
            return (false, publication);
        }
        if let Err(error) = self.check_active(&state) {
            drop(state);
            return (false, self.select_failure(error, TaskEndCause::Expired).1);
        }
        state.snapshot.delivery = DeliveryEvidence::ResponseConfirmed;
        state.data_in_flight = false;
        publication.written = Some(self.written.clone());
        self.finish_locked(
            &mut state,
            Ok(TaskSuccess::ResponseReceived),
            TaskEndCause::Completed,
            &mut publication,
        );
        (true, publication)
    }
    pub(crate) fn select_failure(
        &self,
        error: NetError,
        cause: TaskEndCause,
    ) -> (TerminationOutcome, OperationPublication) {
        let publication = OperationPublication {
            retired_error: Some(error.clone()),
            ..Default::default()
        };
        let mut state = self.lock();
        self.select_failure_locked(&mut state, error, cause, publication)
    }

    fn select_failure_locked(
        &self,
        state: &mut OperationState,
        error: NetError,
        cause: TaskEndCause,
        mut publication: OperationPublication,
    ) -> (TerminationOutcome, OperationPublication) {
        if state.snapshot.result.is_some() {
            return (TerminationOutcome::AlreadyFinished, publication);
        }
        let outcome = match state.snapshot.delivery {
            DeliveryEvidence::NotStarted => TerminationOutcome::TerminatedBeforeWrite,
            DeliveryEvidence::Unknown => TerminationOutcome::DeliveryUnknown,
            DeliveryEvidence::Written | DeliveryEvidence::ResponseConfirmed => {
                TerminationOutcome::TerminatedAfterWrite
            }
        };
        let mut context = error.context().clone();
        context.client_id = Some(self.client_id);
        context.session_id = Some(self.session_id);
        context.connection_id = state.connection.or(context.connection_id);
        if let Some(identity) = &state.pending {
            context.request_id = Some(identity.id.clone());
        }
        if context.stage.is_none() {
            context.stage = Some(match state.snapshot.phase {
                OperationPhase::WaitingForCapacity => crate::error::ErrorStage::Admission,
                OperationPhase::Prepared | OperationPhase::Queued => {
                    crate::error::ErrorStage::Queue
                }
                OperationPhase::Writing => crate::error::ErrorStage::Write,
                OperationPhase::AwaitingResponse => crate::error::ErrorStage::Response,
                OperationPhase::Finished => crate::error::ErrorStage::Dispatch,
            });
        }
        let error = error.with_context(context);
        let error = if state.snapshot.delivery == DeliveryEvidence::Unknown
            && error.kind() != ErrorKind::DeliveryUnknown
        {
            let context = error.context().clone();
            NetError::with_source(ErrorKind::DeliveryUnknown, error).with_context(context)
        } else {
            error
        };
        if state.data_in_flight {
            publication.retirement = state.retirement.clone();
        }
        publication.cancelled = Some(self.cancelled.clone());
        publication.queue = Some((state.queue.clone(), self.cancelled.clone(), error.clone()));
        publication.written = Some(self.written.clone());
        self.finish_locked(state, Err(error), cause, &mut publication);
        (outcome, publication)
    }
    fn finish_locked(
        &self,
        state: &mut OperationState,
        result: Result<TaskSuccess>,
        cause: TaskEndCause,
        publication: &mut OperationPublication,
    ) {
        let phase = state.snapshot.phase;
        state.snapshot.result = Some(result.clone());
        state.snapshot.phase = OperationPhase::Finished;
        if let Some(observation) = state.observation.take() {
            publication.task = Some((
                observation.reservation,
                TaskEvent {
                    client_id: self.client_id,
                    session_id: self.session_id,
                    operation_id: self.id,
                    connection_id: state.connection,
                    source: observation.source,
                    lane: self.lane,
                    phase,
                    delivery: state.snapshot.delivery,
                    cause,
                    result,
                },
            ));
        }
        publication.hook = state.hook.take();
    }
    pub(crate) fn cancel(&self) -> Result<TerminationOutcome> {
        self.terminate(
            NetError::from(ErrorKind::Cancelled),
            TaskEndCause::Cancelled,
        )
    }
    pub(crate) fn expire(&self) -> Result<TerminationOutcome> {
        self.terminate(NetError::from(ErrorKind::TimedOut), TaskEndCause::Expired)
    }
    pub(crate) fn terminate(
        &self,
        error: NetError,
        cause: TaskEndCause,
    ) -> Result<TerminationOutcome> {
        let mut state = self.lock();
        if let Some(identity) = state.pending.clone() {
            drop(state);
            if let Some(table) = identity.table.upgrade() {
                return table.terminate(&identity.id, identity.token, error, cause);
            }
            let (outcome, publication) = self.select_failure(error, cause);
            publication.dispatch();
            return Ok(outcome);
        }
        // Compete with request binding under the same lock: cancellation cannot
        // choose the message-only path and then leave a newly bound request live.
        let publication = OperationPublication {
            retired_error: Some(error.clone()),
            ..Default::default()
        };
        let (outcome, publication) =
            self.select_failure_locked(&mut state, error, cause, publication);
        drop(state);
        publication.dispatch();
        Ok(outcome)
    }
    pub(crate) async fn written(&self) -> Result<WriteOutcome> {
        self.written.cancelled().await;
        let state = self.state.lock().map_err(NetError::from_poison)?;
        match state.snapshot.delivery {
            DeliveryEvidence::ResponseConfirmed => Ok(WriteOutcome::ResponseConfirmed),
            DeliveryEvidence::Written => Ok(WriteOutcome::Written),
            _ => Err(state
                .snapshot
                .result
                .as_ref()
                .and_then(|result| result.as_ref().err())
                .cloned()
                .map_or_else(|| NetError::from(ErrorKind::Internal), |error| error)),
        }
    }
    fn check_active(&self, state: &OperationState) -> Result<()> {
        self.check_active_until(state, earliest(self.deadline, state.response_deadline))
    }
    fn check_active_until(&self, state: &OperationState, deadline: Option<Instant>) -> Result<()> {
        if let Some(result) = &state.snapshot.result {
            return Err(result
                .as_ref()
                .err()
                .cloned()
                .map_or_else(|| NetError::from(ErrorKind::Closed), |error| error));
        }
        if self
            .group
            .as_ref()
            .is_some_and(CancellationGroup::is_cancelled)
            || self.cancelled.is_cancelled()
        {
            return Err(NetError::from(ErrorKind::Cancelled));
        }
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Err(NetError::from(ErrorKind::TimedOut));
        }
        Ok(())
    }
    fn lock(&self) -> MutexGuard<'_, OperationState> {
        match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                crate::log_e!(LogType::WSC; "operation_state", "error", "lock_poisoned_recovered");
                poisoned.into_inner()
            }
        }
    }
}

pub(crate) fn earliest(left: Option<Instant>, right: Option<Instant>) -> Option<Instant> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (left, right) => left.or(right),
    }
}

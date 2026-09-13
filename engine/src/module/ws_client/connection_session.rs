use crate::api::net_error::NetError;
use crate::api::wsc::web_socket_connection_event::{
    WebSocketConnectStage, WebSocketConnectionEvent, WebSocketConnectionEventKind,
    WebSocketConnectionFailure, WebSocketTerminationReason,
};
use crate::api::wsc::web_socket_connection_events::WebSocketConnectionEvents;
use crate::api::wsc::web_socket_context_connect_options::{MAX_EVENT_CAPACITY, MIN_EVENT_CAPACITY};
use crate::api::wsc::web_socket_handshake_context::WebSocketHandshakeAttempt;
use crate::api::wsc::RequestScope;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

/// Two slots acquired together outside the worker loop. Dropping an unaccepted
/// reservation returns both slots, including when its acknowledgement is cancelled.
pub(crate) struct AttemptReservation {
    owner: Arc<Semaphore>,
    permit: OwnedSemaphorePermit,
}

#[derive(Clone, Copy)]
struct AttemptContext {
    attempt: WebSocketHandshakeAttempt,
    context_id: Option<u64>,
}

struct ActiveAttempt {
    context: AttemptContext,
    result_slot: OwnedSemaphorePermit,
    connection_slot: OwnedSemaphorePermit,
    success_decided: bool,
}

struct ActiveConnection {
    context: AttemptContext,
    terminal_slot: OwnedSemaphorePermit,
}

struct QueuedEvent {
    event: WebSocketConnectionEvent,
    _slot: OwnedSemaphorePermit,
}

struct SessionState {
    events: VecDeque<QueuedEvent>,
    session_terminal_slot: Option<OwnedSemaphorePermit>,
    attempt: Option<ActiveAttempt>,
    connection: Option<ActiveConnection>,
    last_attempt: Option<AttemptContext>,
    last_sequence: u64,
    terminated: bool,
    delivery_error: Option<NetError>,
}

struct EventDraft {
    context: Option<AttemptContext>,
    kind: WebSocketConnectionEventKind,
    failure: Option<WebSocketConnectionFailure>,
    reason: Option<WebSocketTerminationReason>,
    will_retry: bool,
}

/// A single session's bounded journal. Only the worker publishes facts; connection
/// tasks reserve capacity asynchronously and the sole public handle consumes it.
pub(crate) struct ConnectionSession {
    client_instance_id: u64,
    session_id: u64,
    session_context_id: u64,
    state: Mutex<SessionState>,
    slots: Arc<Semaphore>,
    changed: Notify,
    cancel_requested: CancellationToken,
    finished: CancellationToken,
}

impl ConnectionSession {
    /// Creates the journal and reserves its final event without requiring a runtime.
    #[cfg(test)]
    pub(crate) fn new(
        client_instance_id: u64,
        session_id: u64,
        session_context_id: u64,
        capacity: usize,
    ) -> Result<(Arc<Self>, WebSocketConnectionEvents), NetError> {
        Self::new_with_scope(
            client_instance_id,
            session_id,
            session_context_id,
            capacity,
            None,
        )
    }

    /// The connection cancellation is a child: cancelling this session does not
    /// revoke other connections that legitimately share the same business scope.
    pub(crate) fn new_with_scope(
        client_instance_id: u64,
        session_id: u64,
        session_context_id: u64,
        capacity: usize,
        scope: Option<&RequestScope>,
    ) -> Result<(Arc<Self>, WebSocketConnectionEvents), NetError> {
        if scope.is_some_and(RequestScope::is_cancelled) {
            return Err(NetError::Cancelled);
        }
        if !(MIN_EVENT_CAPACITY..=MAX_EVENT_CAPACITY).contains(&capacity) {
            return Err(NetError::ConfigError);
        }
        let mut events = VecDeque::new();
        events
            .try_reserve_exact(capacity)
            .map_err(|_| NetError::InternalError)?;
        let slots = Arc::new(Semaphore::new(capacity));
        let terminal_slot = Arc::clone(&slots)
            .try_acquire_owned()
            .map_err(|_| NetError::InternalError)?;
        let session = Arc::new(Self {
            client_instance_id,
            session_id,
            session_context_id,
            state: Mutex::new(SessionState {
                events,
                session_terminal_slot: Some(terminal_slot),
                attempt: None,
                connection: None,
                last_attempt: None,
                last_sequence: 0,
                terminated: false,
                delivery_error: None,
            }),
            slots,
            changed: Notify::new(),
            cancel_requested: match scope {
                Some(scope) => scope.cancel_token().child_token(),
                None => CancellationToken::new(),
            },
            finished: CancellationToken::new(),
        });
        let receiver = WebSocketConnectionEvents {
            session: Arc::clone(&session),
        };
        Ok((session, receiver))
    }

    pub(crate) fn handshake_attempt(
        &self,
        cycle_id: u64,
        attempt_id: u64,
    ) -> WebSocketHandshakeAttempt {
        WebSocketHandshakeAttempt::new(
            self.client_instance_id,
            self.session_id,
            cycle_id,
            attempt_id,
            self.session_context_id,
        )
    }

    pub(crate) fn cancel_token(&self) -> CancellationToken {
        self.cancel_requested.clone()
    }
    pub(crate) fn completion_token(&self) -> CancellationToken {
        self.finished.clone()
    }

    pub(crate) async fn reserve_attempt(&self) -> Result<AttemptReservation, NetError> {
        let permit = tokio::select! {
            biased;
            _ = self.cancel_requested.cancelled() => return Err(NetError::Cancelled),
            permit = Arc::clone(&self.slots).acquire_many_owned(2) => permit.map_err(|_| NetError::Cancelled)?,
        };
        if self.cancel_requested.is_cancelled() {
            return Err(NetError::Cancelled);
        }
        Ok(AttemptReservation {
            owner: Arc::clone(&self.slots),
            permit,
        })
    }

    pub(crate) fn begin_attempt(
        &self,
        attempt: WebSocketHandshakeAttempt,
        mut reservation: AttemptReservation,
    ) -> Result<(), NetError> {
        let mut state = self.lock_state()?;
        if state.terminated || self.cancel_requested.is_cancelled() {
            return Err(NetError::Cancelled);
        }
        if !Arc::ptr_eq(&reservation.owner, &self.slots)
            || attempt.client_instance_id() != self.client_instance_id
            || attempt.session_id() != self.session_id
            || attempt.session_context_id() != self.session_context_id
        {
            return Err(NetError::ConfigError);
        }
        if state.attempt.is_some() || state.connection.is_some() {
            return Err(NetError::ConnectionExists);
        }
        if state.last_attempt.is_some_and(|previous| {
            (attempt.cycle_id(), attempt.attempt_id())
                <= (previous.attempt.cycle_id(), previous.attempt.attempt_id())
        }) {
            return Err(NetError::Cancelled);
        }
        let result_slot = reservation.permit.split(1).ok_or(NetError::InternalError)?;
        let context = AttemptContext {
            attempt,
            context_id: None,
        };
        state.last_attempt = Some(context);
        state.attempt = Some(ActiveAttempt {
            context,
            result_slot,
            connection_slot: reservation.permit,
            success_decided: false,
        });
        Ok(())
    }

    pub(crate) fn set_attempt_context(
        &self,
        cycle_id: u64,
        attempt_id: u64,
        context_id: u64,
    ) -> Result<(), NetError> {
        let mut state = self.lock_state()?;
        Self::check_attempt(&state, cycle_id, attempt_id)?;
        let active = state.attempt.as_mut().ok_or(NetError::InternalError)?;
        if active.context.context_id.is_some() {
            return Err(NetError::ConfigError);
        }
        active.context.context_id = Some(context_id);
        state.last_attempt = Some(active.context);
        Ok(())
    }

    pub(crate) fn attempt_failed(
        &self,
        cycle_id: u64,
        attempt_id: u64,
        failure: WebSocketConnectionFailure,
        will_retry: bool,
    ) -> Result<(), NetError> {
        let mut state = self.lock_state()?;
        Self::check_attempt(&state, cycle_id, attempt_id)?;
        if state
            .attempt
            .as_ref()
            .is_some_and(|active| active.success_decided)
        {
            return Err(NetError::ConfigError);
        }
        let active = state.attempt.take().ok_or(NetError::InternalError)?;
        self.publish(
            &mut state,
            EventDraft {
                context: Some(active.context),
                kind: WebSocketConnectionEventKind::AttemptFailed,
                failure: Some(failure),
                reason: None,
                will_retry,
            },
            active.result_slot,
        )?;
        drop(active.connection_slot);
        Ok(())
    }

    #[cfg(test)]
    fn established(&self, cycle_id: u64, attempt_id: u64) -> Result<(), NetError> {
        self.prepare_established(cycle_id, attempt_id)?;
        self.commit_established(cycle_id, attempt_id)
    }

    /// Chooses handshake success before local I/O setup. Only the worker calls this;
    /// it must commit after setup, before processing another lifecycle command.
    pub(crate) fn prepare_established(
        &self,
        cycle_id: u64,
        attempt_id: u64,
    ) -> Result<(), NetError> {
        let mut state = self.lock_state()?;
        Self::check_attempt(&state, cycle_id, attempt_id)?;
        if self.cancel_requested.is_cancelled() {
            return Err(NetError::Cancelled);
        }
        if state
            .attempt
            .as_ref()
            .is_none_or(|active| active.context.context_id.is_none())
        {
            return Err(NetError::ConfigError);
        }
        let active = state.attempt.as_mut().ok_or(NetError::InternalError)?;
        if active.success_decided {
            return Err(NetError::ConfigError);
        }
        active.success_decided = true;
        Ok(())
    }

    /// Publishes the already selected success after the worker has made local I/O
    /// ready. A cancellation arriving after prepare cannot change this result;
    /// the worker processes it afterwards and emits ordered termination events.
    pub(crate) fn commit_established(
        &self,
        cycle_id: u64,
        attempt_id: u64,
    ) -> Result<(), NetError> {
        let mut state = self.lock_state()?;
        Self::check_attempt(&state, cycle_id, attempt_id)?;
        if state
            .attempt
            .as_ref()
            .is_none_or(|active| !active.success_decided)
        {
            return Err(NetError::ConfigError);
        }
        let active = state.attempt.take().ok_or(NetError::InternalError)?;
        self.publish(
            &mut state,
            EventDraft {
                context: Some(active.context),
                kind: WebSocketConnectionEventKind::Established,
                failure: None,
                reason: None,
                will_retry: false,
            },
            active.result_slot,
        )?;
        state.connection = Some(ActiveConnection {
            context: active.context,
            terminal_slot: active.connection_slot,
        });
        Ok(())
    }

    pub(crate) fn connection_terminated(
        &self,
        reason: WebSocketTerminationReason,
        failure: Option<WebSocketConnectionFailure>,
    ) -> Result<(), NetError> {
        let mut state = self.lock_state()?;
        if state.terminated {
            return Err(NetError::Cancelled);
        }
        let active = state.connection.take().ok_or(NetError::ConnectionClosed)?;
        self.publish(
            &mut state,
            EventDraft {
                context: Some(active.context),
                kind: WebSocketConnectionEventKind::ConnectionTerminated,
                failure,
                reason: Some(reason),
                will_retry: false,
            },
            active.terminal_slot,
        )
    }

    /// Called after the worker has stopped this session's I/O. Pending attempts and
    /// connections are finalized first, then the reserved session event is published.
    /// Repeated termination does not create another event or replace an earlier error.
    pub(crate) fn terminate(
        &self,
        reason: WebSocketTerminationReason,
        failure: Option<WebSocketConnectionFailure>,
    ) -> Result<(), NetError> {
        self.cancel_requested.cancel();
        let result = self.terminate_inner(reason, failure);
        self.slots.close();
        self.finished.cancel();
        self.changed.notify_one();
        result
    }

    fn terminate_inner(
        &self,
        reason: WebSocketTerminationReason,
        failure: Option<WebSocketConnectionFailure>,
    ) -> Result<(), NetError> {
        let mut state = self.lock_state()?;
        if state.terminated {
            return match state.delivery_error {
                Some(error) => Err(error),
                None => Ok(()),
            };
        }
        if let Some(active) = state.attempt.take() {
            let cancelled = match failure {
                Some(failure) => failure,
                None => WebSocketConnectionFailure::new(
                    NetError::Cancelled,
                    WebSocketConnectStage::EventDelivery,
                    None,
                    false,
                ),
            };
            self.publish(
                &mut state,
                EventDraft {
                    context: Some(active.context),
                    kind: WebSocketConnectionEventKind::AttemptFailed,
                    failure: Some(cancelled),
                    reason: Some(reason),
                    will_retry: false,
                },
                active.result_slot,
            )?;
            drop(active.connection_slot);
        }
        if let Some(active) = state.connection.take() {
            self.publish(
                &mut state,
                EventDraft {
                    context: Some(active.context),
                    kind: WebSocketConnectionEventKind::ConnectionTerminated,
                    failure,
                    reason: Some(reason),
                    will_retry: false,
                },
                active.terminal_slot,
            )?;
        }
        let slot = state
            .session_terminal_slot
            .take()
            .ok_or(NetError::InternalError)?;
        let context = state.last_attempt;
        self.publish(
            &mut state,
            EventDraft {
                context,
                kind: WebSocketConnectionEventKind::SessionTerminated,
                failure,
                reason: Some(reason),
                will_retry: false,
            },
            slot,
        )?;
        state.terminated = true;
        Ok(())
    }

    pub(crate) fn request_cancel(&self) {
        self.cancel_requested.cancel();
    }

    pub(crate) async fn wait_finished(&self) -> Result<(), NetError> {
        self.completion_token().cancelled().await;
        let state = self.lock_state()?;
        match state.delivery_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    pub(crate) async fn recv(&self) -> Result<Option<WebSocketConnectionEvent>, NetError> {
        loop {
            // There is exactly one receiver. notify_one retains a wakeup between the
            // empty check and this wait, so a publication cannot be missed.
            let notified = self.changed.notified();
            {
                let mut state = self.lock_state()?;
                if let Some(queued) = state.events.pop_front() {
                    let event = queued.event;
                    drop(queued);
                    return Ok(Some(event));
                }
                if let Some(error) = state.delivery_error {
                    return Err(error);
                }
                if state.terminated {
                    return Ok(None);
                }
            }
            notified.await;
        }
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, SessionState>, NetError> {
        self.state.lock().map_err(|_| {
            self.cancel_requested.cancel();
            NetError::InternalError
        })
    }

    fn check_attempt(state: &SessionState, cycle_id: u64, attempt_id: u64) -> Result<(), NetError> {
        if state.terminated {
            return Err(NetError::Cancelled);
        }
        if state.attempt.as_ref().is_some_and(|active| {
            active.context.attempt.cycle_id() == cycle_id
                && active.context.attempt.attempt_id() == attempt_id
        }) {
            Ok(())
        } else {
            Err(NetError::Cancelled)
        }
    }

    fn publish(
        &self,
        state: &mut SessionState,
        draft: EventDraft,
        slot: OwnedSemaphorePermit,
    ) -> Result<(), NetError> {
        let sequence = match state.last_sequence.checked_add(1) {
            Some(sequence) => sequence,
            None => {
                state.delivery_error = Some(NetError::InternalError);
                state.terminated = true;
                state.attempt.take();
                state.connection.take();
                state.session_terminal_slot.take();
                self.cancel_requested.cancel();
                self.slots.close();
                self.changed.notify_one();
                return Err(NetError::InternalError);
            }
        };
        state.last_sequence = sequence;
        let event = WebSocketConnectionEvent {
            sequence,
            client_instance_id: self.client_instance_id,
            session_id: self.session_id,
            cycle_id: draft.context.map(|context| context.attempt.cycle_id()),
            attempt_id: draft.context.map(|context| context.attempt.attempt_id()),
            session_context_id: self.session_context_id,
            attempt_context_id: draft.context.and_then(|context| context.context_id),
            kind: draft.kind,
            failure: draft.failure,
            termination_reason: draft.reason,
            will_retry: draft.will_retry,
        };
        state.events.push_back(QueuedEvent { event, _slot: slot });
        self.changed.notify_one();
        Ok(())
    }
}

#[cfg(test)]
#[path = "connection_session_tests.rs"]
mod tests;

use crate::error::{ErrorKind, ErrorStage, NetError};
use crate::module::transport::failure::ConnectionFailure;
use crate::module::ws_client::connection_history::{ConnectionHistory, HistoryPublication};
use crate::module::ws_client::io_diagnostics::ConnectionTerminationDetails;
use crate::module::ws_client::listener_store::ConnectionObservers;
use crate::subscription::{
    event_channel, EventOverflow, EventPublication, EventPublisher, EventQueueLimit, EventReceiver,
    EventResources, StatePublication, StatePublisher, StateReceiver, StateSource,
};
use crate::ws::{
    AttemptId, ClientId, ConnectionEnd, ConnectionEvent, ConnectionEventKind, ConnectionId,
    ConnectionInfo, ConnectionSnapshot, ConnectionState, CycleId, EventOptions, HandshakeAttempt,
    HandshakeDiagnostic, JournalOptions, RetryDecision, SessionEnd, SessionId, TerminationReason,
    MAX_CONNECTION_EVENT_BYTES,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::SystemTime;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

enum EventLease {
    Observed {
        slot: OwnedSemaphorePermit,
        bytes: OwnedSemaphorePermit,
    },
    Unobserved,
}

struct JournalBudget {
    publisher: EventPublisher<ConnectionEvent>,
    slots: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
}

/// Admission owns Started, its one outcome, and the possible Disconnected fact.
/// An unobserved reservation still belongs to one exact session. Option around
/// an individual lease means it was consumed, never that observation is disabled.
pub(crate) struct AttemptReservation {
    owner: Arc<()>,
    resources: EventLease,
}

#[derive(Clone)]
struct AttemptContext {
    attempt: HandshakeAttempt,
    credential_version: Option<String>,
    prepared: bool,
}

struct ActiveAttempt {
    context: AttemptContext,
    result_slot: Option<EventLease>,
    connection_slot: Option<EventLease>,
    success_decided: bool,
}

struct ActiveConnection {
    info: ConnectionInfo,
    terminal_slot: Option<EventLease>,
}

struct PendingEvent {
    event: ConnectionEvent,
    lease: EventLease,
}

struct SessionState {
    snapshot: Arc<ConnectionSnapshot>,
    cycle: Option<u64>,
    reconnecting: bool,
    closing: bool,
    state_failed: bool,
    session_terminal_slot: Option<EventLease>,
    attempt: Option<ActiveAttempt>,
    connection: Option<ActiveConnection>,
    last_attempt: Option<AttemptContext>,
    last_connection_end: Option<ConnectionEnd>,
    last_connection_info: Option<ConnectionInfo>,
    last_attempt_error: Option<NetError>,
    terminal_result: Option<Result<SessionEnd, NetError>>,
    last_sequence: u64,
    terminated: bool,
    delivery_error: Option<NetError>,
}

/// All permit returns and potentially last error sources leave the session lock
/// in this fixed-size retirement record. Terminal publication never allocates it.
#[derive(Default)]
struct Publication {
    journal: [Option<EventPublication<ConnectionEvent>>; 3],
    journal_finish: Option<EventPublication<ConnectionEvent>>,
    journal_failure: Option<EventPublication<ConnectionEvent>>,
    unobserved: [Option<PendingEvent>; 3],
    history: [Option<HistoryPublication>; 3],
    history_failure: Option<HistoryPublication>,
    snapshot: Option<StatePublication<ConnectionSnapshot>>,
    snapshot_failure: Option<StatePublication<ConnectionSnapshot>>,
    previous_snapshot: Option<Arc<ConnectionSnapshot>>,
    next_state: Option<ConnectionState>,
    next_error: Option<NetError>,
    pending: Option<PendingEvent>,
    attempt: Option<ActiveAttempt>,
    connection: Option<ActiveConnection>,
    terminal_slot: Option<EventLease>,
    previous_end: Option<ConnectionEnd>,
    previous_attempt_error: Option<NetError>,
    refunds: [Option<OwnedSemaphorePermit>; 3],
    close: bool,
    failed: bool,
}

/// The first close caller owns the frame and absolute cleanup budget.
pub(crate) struct SessionCloseRequest {
    pub(crate) frame: Option<crate::ws::CloseFrame>,
    pub(crate) deadline: tokio::time::Instant,
}

#[derive(Default)]
struct CloseRequestState {
    requested: bool,
    request: Option<SessionCloseRequest>,
}

/// A single session's bounded journal. Only the worker publishes facts; connection
/// tasks reserve capacity asynchronously and the sole public handle consumes it.
pub(crate) struct ConnectionSession {
    client_instance_id: u64,
    session_id: u64,
    metadata: Arc<crate::Metadata>,
    state: Mutex<SessionState>,
    history: ConnectionHistory,
    state_source: StateSource<ConnectionSnapshot>,
    state_publisher: StatePublisher<ConnectionSnapshot>,
    lifecycle_changed: Notify,
    #[cfg(test)]
    fixture_initial_cycle: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    before_snapshot: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    #[cfg(test)]
    before_finished: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    #[cfg(test)]
    failed_state_read: AtomicBool,
    identity: Arc<()>,
    journal: Option<JournalBudget>,
    cancel_requested: CancellationToken,
    cancel_selected: AtomicBool,
    close_requested: CancellationToken,
    close_request: Mutex<CloseRequestState>,
    finished: CancellationToken,
}

impl ConnectionSession {
    #[cfg(test)]
    pub(crate) fn journal_fixture(
        client_id: u64,
        session_id: u64,
        journal: JournalOptions,
    ) -> Result<(Arc<Self>, EventReceiver<ConnectionEvent>), NetError> {
        Self::journal_fixture_with_metadata(
            client_id,
            session_id,
            journal,
            Arc::new(crate::Metadata::new()),
        )
    }

    #[cfg(test)]
    fn journal_fixture_with_metadata(
        client_id: u64,
        session_id: u64,
        journal: JournalOptions,
        metadata: Arc<crate::Metadata>,
    ) -> Result<(Arc<Self>, EventReceiver<ConnectionEvent>), NetError> {
        let listeners = crate::module::ws_client::listener_store::ListenerStore::new(
            &crate::ws::WebSocketClientConfig::default(),
        )?;
        let (session, receiver) = Self::new_with_observers(
            client_id,
            session_id,
            Some(journal),
            metadata,
            EventOptions::default(),
            listeners.connection_observers(),
        )?;
        // Lifecycle-only tests may infer their initial cycle; worker tests establish it explicitly.
        session
            .fixture_initial_cycle
            .store(true, std::sync::atomic::Ordering::Relaxed);
        Ok((session, receiver.ok_or_else(Self::internal)?))
    }

    /// Create the lifecycle before publishing any networking work.
    pub(crate) fn new_with_observers(
        client_instance_id: u64,
        session_id: u64,
        journal: Option<JournalOptions>,
        metadata: Arc<crate::Metadata>,
        event_options: EventOptions,
        observers: ConnectionObservers,
    ) -> Result<(Arc<Self>, Option<EventReceiver<ConnectionEvent>>), NetError> {
        if let Some(options) = &journal {
            options.validate()?;
        }
        let snapshot = Arc::new(ConnectionSnapshot {
            revision: 0,
            session_id: SessionId::from_allocated(session_id),
            state: ConnectionState::Connecting,
            last_error: None,
        });
        let (state_publisher, state_source) = StateSource::new_arc_with_quota(
            Arc::clone(&snapshot),
            observers.state_executor,
            observers.state_quota,
        )?;
        let history = ConnectionHistory::new(
            event_options,
            Arc::clone(&observers.event_executor),
            Arc::clone(&observers.event_quota),
        )?;
        let (journal, receiver, terminal_slot) = if let Some(options) = journal {
            let permit = observers
                .event_quota
                .try_acquire_owned()
                .map_err(|_| NetError::from(ErrorKind::SubscriptionLimitReached))?;
            let (publisher, receiver) = event_channel(
                EventQueueLimit {
                    max_items: options.max_events,
                    max_bytes: options.max_bytes,
                },
                EventOverflow::Wait,
                observers.event_executor,
                Some(permit),
            )?;
            let slots = Arc::new(Semaphore::new(options.max_events));
            let bytes = Arc::new(Semaphore::new(options.max_bytes));
            let terminal_slot = EventLease::Observed {
                slot: Arc::clone(&slots)
                    .try_acquire_owned()
                    .map_err(|_| Self::internal())?,
                bytes: Arc::clone(&bytes)
                    .try_acquire_many_owned(MAX_CONNECTION_EVENT_BYTES as u32)
                    .map_err(|_| Self::internal())?,
            };
            (
                Some(JournalBudget {
                    publisher,
                    slots,
                    bytes,
                }),
                Some(receiver),
                terminal_slot,
            )
        } else {
            (None, None, EventLease::Unobserved)
        };
        let session = Arc::new(Self {
            client_instance_id,
            session_id,
            metadata,
            state: Mutex::new(SessionState {
                snapshot,
                cycle: None,
                reconnecting: false,
                closing: false,
                state_failed: false,
                session_terminal_slot: Some(terminal_slot),
                attempt: None,
                connection: None,
                last_attempt: None,
                last_connection_end: None,
                last_connection_info: None,
                last_attempt_error: None,
                terminal_result: None,
                last_sequence: 0,
                terminated: false,
                delivery_error: None,
            }),
            history,
            state_source,
            state_publisher,
            lifecycle_changed: Notify::new(),
            #[cfg(test)]
            fixture_initial_cycle: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            before_snapshot: Mutex::new(None),
            #[cfg(test)]
            before_finished: Mutex::new(None),
            #[cfg(test)]
            failed_state_read: AtomicBool::new(false),
            identity: Arc::new(()),
            journal,
            cancel_requested: CancellationToken::new(),
            cancel_selected: AtomicBool::new(false),
            close_requested: CancellationToken::new(),
            close_request: Mutex::new(CloseRequestState::default()),
            finished: CancellationToken::new(),
        });
        Ok((session, receiver))
    }

    pub(crate) fn snapshot(&self) -> Result<ConnectionSnapshot, NetError> {
        let state = self.lock_state()?;
        let result = self.state_source.current_result();
        drop(state);
        result
    }

    pub(crate) fn watch_state(&self) -> Result<StateReceiver<ConnectionSnapshot>, NetError> {
        let state = self.lock_state()?;
        let result = self.state_source.subscribe();
        drop(state);
        result
    }

    pub(crate) fn subscribe_events(&self) -> Result<EventReceiver<ConnectionEvent>, NetError> {
        self.history.subscribe()
    }

    pub(crate) async fn wait_connected(&self) -> Result<ConnectionInfo, NetError> {
        loop {
            let notified = self.lifecycle_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let state = self.lock_state()?;
                if let Some(result) = &state.terminal_result {
                    return match result {
                        Err(error) => Err(error.clone()),
                        Ok(_) => Err(self.annotate(
                            NetError::from(ErrorKind::Closed),
                            None,
                            None,
                            None,
                            None,
                        )),
                    };
                }
                if !state.closing {
                    if let Some(connection) = &state.connection {
                        return Ok(connection.info.clone());
                    }
                }
            }
            notified.await;
        }
    }

    pub(crate) async fn closed(&self) -> Result<SessionEnd, NetError> {
        loop {
            // Register before reading to observe a failed transition after the
            // waiter started, without ever confusing a chosen result with the
            // worker's completion publication.
            let changed = self.lifecycle_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            self.terminal_result_checked()?;
            if self.finished.is_cancelled() {
                // Re-read after observing completion: the earlier read may have
                // happened immediately before the worker chose its result.
                return self.terminal_result_checked()?.ok_or_else(Self::internal)?;
            }
            tokio::select! {
                _ = self.finished.cancelled() => {},
                _ = changed => {},
            }
        }
    }

    pub(crate) fn begin_cycle(&self, cycle: u64, reconnecting: bool) -> Result<(), NetError> {
        self.mutate(|state, publication| {
            if state.terminated
                || state.closing
                || state.connection.is_some()
                || state.attempt.is_some()
                || state.cycle.is_some_and(|current| current > cycle)
            {
                return Err(NetError::from(ErrorKind::Cancelled));
            }
            state.cycle = Some(cycle);
            state.reconnecting = reconnecting;
            publication.next_state = Some(Self::preparing_state(state, None)?);
            Ok(())
        })
    }

    pub(crate) fn waiting_for_network(&self, cycle: u64) -> Result<(), NetError> {
        self.mutate(|state, publication| {
            Self::check_progress(state, cycle)?;
            publication.next_state = Some(ConnectionState::WaitingForNetwork);
            Ok(())
        })
    }

    pub(crate) fn preparing_attempt(
        &self,
        cycle: u64,
        next_attempt_at: Option<std::time::Instant>,
    ) -> Result<(), NetError> {
        self.mutate(|state, publication| {
            Self::check_progress(state, cycle)?;
            if next_attempt_at.is_some() {
                state.reconnecting = true;
            }
            publication.next_state = Some(Self::preparing_state(state, next_attempt_at)?);
            Ok(())
        })
    }

    pub(crate) fn begin_closing(&self) -> Result<(), NetError> {
        self.mutate(|state, publication| {
            if !state.terminated && !state.closing {
                state.closing = true;
                publication.next_state = Some(ConnectionState::Closing);
            }
            Ok(())
        })
    }

    fn check_progress(state: &SessionState, cycle: u64) -> Result<(), NetError> {
        if state.terminated
            || state.closing
            || state.connection.is_some()
            || state.cycle != Some(cycle)
        {
            Err(NetError::from(ErrorKind::Cancelled))
        } else {
            Ok(())
        }
    }

    fn preparing_state(
        state: &SessionState,
        next_attempt_at: Option<std::time::Instant>,
    ) -> Result<ConnectionState, NetError> {
        if state.reconnecting {
            Ok(ConnectionState::Reconnecting {
                cycle_id: CycleId::from_allocated(state.cycle.ok_or_else(Self::internal)?),
                next_attempt_at,
            })
        } else {
            Ok(ConnectionState::Connecting)
        }
    }

    pub(crate) fn handshake_attempt(&self, cycle_id: u64, attempt_id: u64) -> HandshakeAttempt {
        HandshakeAttempt {
            client_id: ClientId::from_allocated(self.client_instance_id),
            session_id: SessionId::from_allocated(self.session_id),
            cycle_id: CycleId::from_allocated(cycle_id),
            attempt_id: AttemptId::from_allocated(attempt_id),
            metadata: Arc::clone(&self.metadata),
        }
    }

    pub(crate) fn cancel_token(&self) -> CancellationToken {
        self.cancel_requested.clone()
    }
    pub(crate) fn completion_token(&self) -> CancellationToken {
        self.finished.clone()
    }

    pub(crate) async fn reserve_attempt(&self) -> Result<AttemptReservation, NetError> {
        if self.cancel_requested.is_cancelled() {
            return Err(NetError::from(ErrorKind::Cancelled));
        }
        let Some(journal) = self
            .journal
            .as_ref()
            .filter(|journal| journal.publisher.is_active())
        else {
            return Ok(self.unobserved_reservation());
        };
        let slots = tokio::select! {
            biased;
            _ = self.cancel_requested.cancelled() => return Err(NetError::from(ErrorKind::Cancelled)),
            _ = journal.publisher.receiver_closed() => return Ok(self.unobserved_reservation()),
            permit = Arc::clone(&journal.slots).acquire_many_owned(3) =>
                permit.map_err(|_| NetError::from(ErrorKind::Cancelled))?,
        };
        let bytes = tokio::select! {
            biased;
            _ = self.cancel_requested.cancelled() => return Err(NetError::from(ErrorKind::Cancelled)),
            _ = journal.publisher.receiver_closed() => return Ok(self.unobserved_reservation()),
            permit = Arc::clone(&journal.bytes).acquire_many_owned((3 * MAX_CONNECTION_EVENT_BYTES) as u32) =>
                permit.map_err(|_| NetError::from(ErrorKind::Cancelled))?,
        };
        if self.cancel_requested.is_cancelled() {
            return Err(NetError::from(ErrorKind::Cancelled));
        }
        if !journal.publisher.is_active() {
            return Ok(self.unobserved_reservation());
        }
        Ok(AttemptReservation {
            owner: Arc::clone(&self.identity),
            resources: EventLease::Observed { slot: slots, bytes },
        })
    }

    fn unobserved_reservation(&self) -> AttemptReservation {
        AttemptReservation {
            owner: Arc::clone(&self.identity),
            resources: EventLease::Unobserved,
        }
    }

    pub(crate) fn begin_attempt(
        &self,
        attempt: HandshakeAttempt,
        mut reservation: AttemptReservation,
    ) -> Result<(), NetError> {
        // Split before taking state: an invalid reservation never releases permits under it.
        let mut started = Some(Self::split_lease(&mut reservation)?);
        let mut result_slot = Some(Self::split_lease(&mut reservation)?);
        let mut connection_slot = Some(Self::split_lease(&mut reservation)?);
        self.mutate(|state, publication| {
            if state.terminated || state.closing || self.cancel_requested.is_cancelled() {
                return Err(NetError::from(ErrorKind::Cancelled));
            }
            if !Arc::ptr_eq(&reservation.owner, &self.identity)
                || attempt.client_id.as_u64() != self.client_instance_id
                || attempt.session_id.as_u64() != self.session_id
                || !Arc::ptr_eq(&attempt.metadata, &self.metadata)
            {
                return Err(NetError::from(ErrorKind::InvalidConfig));
            }
            if state.attempt.is_some() || state.connection.is_some() {
                return Err(NetError::from(ErrorKind::SessionAlreadyExists));
            }
            if state.last_attempt.as_ref().is_some_and(|previous| {
                (attempt.cycle_id, attempt.attempt_id)
                    <= (previous.attempt.cycle_id, previous.attempt.attempt_id)
            }) {
                return Err(NetError::from(ErrorKind::Cancelled));
            }
            #[cfg(test)]
            if state.cycle.is_none()
                && self
                    .fixture_initial_cycle
                    .load(std::sync::atomic::Ordering::Relaxed)
            {
                state.cycle = Some(attempt.cycle_id.as_u64());
            }
            Self::check_progress(state, attempt.cycle_id.as_u64())?;
            publication.next_state = Some(Self::preparing_state(state, None)?);
            let context = AttemptContext {
                attempt: attempt.clone(),
                credential_version: None,
                prepared: false,
            };
            publication.previous_attempt_error = state.last_attempt_error.take();
            state.last_attempt = Some(context.clone());
            state.attempt = Some(ActiveAttempt {
                context,
                result_slot: result_slot.take(),
                connection_slot: connection_slot.take(),
                success_decided: false,
            });
            self.publish(
                state,
                ConnectionEventKind::AttemptStarted {
                    attempt: attempt.clone(),
                },
                started.take().ok_or_else(Self::internal)?,
                publication,
            )
        })
    }

    fn split_lease(reservation: &mut AttemptReservation) -> Result<EventLease, NetError> {
        match &mut reservation.resources {
            EventLease::Observed { slot, bytes } => Ok(EventLease::Observed {
                slot: slot.split(1).ok_or_else(Self::internal)?,
                bytes: bytes
                    .split(MAX_CONNECTION_EVENT_BYTES)
                    .ok_or_else(Self::internal)?,
            }),
            EventLease::Unobserved => Ok(EventLease::Unobserved),
        }
    }

    pub(crate) fn set_attempt_credential_version(
        &self,
        cycle_id: u64,
        attempt_id: u64,
        credential_version: Option<String>,
    ) -> Result<(), NetError> {
        let mut state = self.lock_state()?;
        Self::check_attempt(&state, cycle_id, attempt_id)?;
        let active = state.attempt.as_mut().ok_or_else(Self::internal)?;
        if active.context.prepared {
            return Err(NetError::from(ErrorKind::InvalidConfig));
        }
        active.context.credential_version = credential_version;
        active.context.prepared = true;
        state.last_attempt = Some(active.context.clone());
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn attempt_failed(
        &self,
        cycle_id: u64,
        attempt_id: u64,
        failure: ConnectionFailure,
        retry: RetryDecision,
    ) -> Result<(), NetError> {
        self.attempt_failed_with_diagnostic(cycle_id, attempt_id, failure, retry, None)
    }

    pub(crate) fn attempt_failed_with_diagnostic(
        &self,
        cycle_id: u64,
        attempt_id: u64,
        failure: ConnectionFailure,
        retry: RetryDecision,
        diagnostic: Option<HandshakeDiagnostic>,
    ) -> Result<(), NetError> {
        self.mutate(|state, publication| {
            Self::check_attempt(state, cycle_id, attempt_id)?;
            if state
                .attempt
                .as_ref()
                .is_some_and(|active| active.success_decided)
            {
                return Err(NetError::from(ErrorKind::InvalidConfig));
            }
            publication.attempt = state.attempt.take();
            let active = publication.attempt.as_mut().ok_or_else(Self::internal)?;
            let error = self.annotate(
                failure.error(),
                Some(&active.context.attempt),
                None,
                diagnostic.clone(),
                None,
            );
            let kind = ConnectionEventKind::AttemptFailed {
                attempt: active.context.attempt.clone(),
                credential_version: active.context.credential_version.clone(),
                error: error.clone(),
                retry: retry.clone(),
            };
            let lease = active.result_slot.take().ok_or_else(Self::internal)?;
            self.publish(state, kind, lease, publication)?;
            publication.previous_attempt_error = state.last_attempt_error.replace(error.clone());
            publication.next_error = Some(error);
            if matches!(retry, RetryDecision::Scheduled { .. }) {
                state.reconnecting = true;
                publication.next_state = Some(Self::preparing_state(state, None)?);
            }
            Ok(())
        })
    }

    #[cfg(test)]
    fn established(&self, cycle_id: u64, attempt_id: u64) -> Result<(), NetError> {
        self.prepare_established(cycle_id, attempt_id)?;
        self.commit_established(cycle_id, attempt_id)
    }

    pub(crate) fn prepare_established(
        &self,
        cycle_id: u64,
        attempt_id: u64,
    ) -> Result<(), NetError> {
        let mut state = self.lock_state()?;
        Self::check_attempt(&state, cycle_id, attempt_id)?;
        if self.cancel_requested.is_cancelled() {
            return Err(NetError::from(ErrorKind::Cancelled));
        }
        let active = state.attempt.as_mut().ok_or_else(Self::internal)?;
        if !active.context.prepared || active.success_decided {
            return Err(NetError::from(ErrorKind::InvalidConfig));
        }
        active.success_decided = true;
        Ok(())
    }

    pub(crate) fn commit_established(
        &self,
        cycle_id: u64,
        attempt_id: u64,
    ) -> Result<(), NetError> {
        self.mutate(|state, publication| {
            Self::check_attempt(state, cycle_id, attempt_id)?;
            if state
                .attempt
                .as_ref()
                .is_none_or(|active| !active.success_decided)
            {
                return Err(NetError::from(ErrorKind::InvalidConfig));
            }
            publication.attempt = state.attempt.take();
            let active = publication.attempt.as_mut().ok_or_else(Self::internal)?;
            let attempt = &active.context.attempt;
            let info = ConnectionInfo {
                client_id: attempt.client_id,
                session_id: attempt.session_id,
                connection_id: ConnectionId::from_allocated(attempt.cycle_id.as_u64()),
                cycle_id: attempt.cycle_id,
                attempt_id: attempt.attempt_id,
                connected_at: SystemTime::now(),
                credential_version: active.context.credential_version.clone(),
            };
            let lease = active.result_slot.take().ok_or_else(Self::internal)?;
            self.publish(
                state,
                ConnectionEventKind::Established {
                    connection: info.clone(),
                },
                lease,
                publication,
            )?;
            let active = publication.attempt.as_mut().ok_or_else(Self::internal)?;
            state.connection = Some(ActiveConnection {
                info: info.clone(),
                terminal_slot: active.connection_slot.take(),
            });
            publication.next_state = Some(ConnectionState::Connected(info));
            Ok(())
        })
    }

    #[cfg(test)]
    pub(crate) fn connection_terminated(
        &self,
        reason: TerminationReason,
        failure: Option<ConnectionFailure>,
    ) -> Result<(), NetError> {
        self.connection_terminated_with_details(
            reason,
            failure,
            ConnectionTerminationDetails::default(),
            None,
        )
    }

    pub(crate) fn connection_terminated_with_details(
        &self,
        reason: TerminationReason,
        failure: Option<ConnectionFailure>,
        details: ConnectionTerminationDetails,
        next_cycle: Option<u64>,
    ) -> Result<(), NetError> {
        self.mutate(|state, publication| {
            if state.terminated {
                return Err(NetError::from(ErrorKind::Cancelled));
            }
            self.disconnect(
                state,
                reason,
                failure.as_ref(),
                &details,
                next_cycle,
                publication,
            )
        })
    }

    fn disconnect(
        &self,
        state: &mut SessionState,
        reason: TerminationReason,
        failure: Option<&ConnectionFailure>,
        details: &ConnectionTerminationDetails,
        next_cycle: Option<u64>,
        publication: &mut Publication,
    ) -> Result<(), NetError> {
        if next_cycle.is_some_and(|next| {
            state
                .connection
                .as_ref()
                .is_some_and(|active| next <= active.info.cycle_id.as_u64())
        }) {
            return Err(NetError::from(ErrorKind::InvalidInput));
        }
        publication.connection = state.connection.take();
        let active = publication
            .connection
            .as_mut()
            .ok_or_else(|| NetError::from(ErrorKind::Closed))?;
        let error = failure.map(|failure| {
            self.annotate(
                failure.error(),
                None,
                Some(&active.info),
                None,
                Some(details),
            )
        });
        let end = ConnectionEnd {
            reason,
            error,
            peer_close: details.peer_close.clone(),
            io_end: details.io_end_kind,
        };
        let info = active.info.clone();
        let kind = ConnectionEventKind::Disconnected {
            connection: info.clone(),
            end: end.clone(),
        };
        let lease = active.terminal_slot.take().ok_or_else(Self::internal)?;
        self.publish(state, kind, lease, publication)?;
        publication.next_error = end.error.clone();
        publication.previous_end = state.last_connection_end.replace(end);
        state.last_connection_info = Some(info);
        if let Some(next_cycle) = next_cycle {
            state.cycle = Some(next_cycle);
            state.reconnecting = true;
            publication.next_state = Some(Self::preparing_state(state, None)?);
        } else {
            state.closing = true;
            publication.next_state = Some(ConnectionState::Closing);
        }
        Ok(())
    }

    pub(crate) fn terminate(
        &self,
        reason: TerminationReason,
        failure: Option<ConnectionFailure>,
    ) -> Result<(), NetError> {
        self.terminate_with_details(reason, failure, ConnectionTerminationDetails::default())
    }

    pub(crate) fn terminate_with_details(
        &self,
        reason: TerminationReason,
        failure: Option<ConnectionFailure>,
        details: ConnectionTerminationDetails,
    ) -> Result<(), NetError> {
        self.cancel_selected.store(true, Ordering::Release);
        self.cancel_requested.cancel();
        let result = self.mutate(|state, publication| {
            if state.terminated {
                return state.delivery_error.clone().map_or(Ok(()), Err);
            }
            if state.attempt.is_some() {
                publication.attempt = state.attempt.take();
                let active = publication.attempt.as_mut().ok_or_else(Self::internal)?;
                let error = match failure.as_ref() {
                    Some(failure) => failure.error(),
                    None => Self::reason_error(reason),
                };
                let error = self.annotate(
                    error,
                    Some(&active.context.attempt),
                    None,
                    None,
                    Some(&details),
                );
                let kind = ConnectionEventKind::AttemptFailed {
                    attempt: active.context.attempt.clone(),
                    credential_version: active.context.credential_version.clone(),
                    error,
                    retry: RetryDecision::Stop,
                };
                let lease = active.result_slot.take().ok_or_else(Self::internal)?;
                self.publish(state, kind, lease, publication)?;
            }
            if state.connection.is_some() {
                self.disconnect(state, reason, failure.as_ref(), &details, None, publication)?;
            }
            let terminal = match reason {
                TerminationReason::LocalClose
                | TerminationReason::ClientShutdown
                | TerminationReason::PeerClose => Ok(SessionEnd {
                    reason,
                    last_connection: state.last_connection_end.clone(),
                }),
                _ => {
                    let current_connection =
                        state.last_connection_info.as_ref().filter(|connection| {
                            state.last_attempt.as_ref().is_some_and(|last| {
                                last.attempt.cycle_id == connection.cycle_id
                                    && last.attempt.attempt_id == connection.attempt_id
                            })
                        });
                    let mut error = match failure.as_ref() {
                        Some(failure) => failure.error(),
                        None => Self::reason_error(reason),
                    };
                    // Only the same physical termination may contribute its error;
                    // a new attempt, cancellation, or engine shutdown has its own cause.
                    let observed = if current_connection.is_some() {
                        state
                            .last_connection_end
                            .as_ref()
                            .filter(|end| end.reason == reason)
                            .and_then(|end| end.error.as_ref())
                    } else if matches!(
                        reason,
                        TerminationReason::ConnectFailed | TerminationReason::RetryExhausted
                    ) {
                        state.last_attempt_error.as_ref()
                    } else {
                        None
                    };
                    if let Some(observed) = observed {
                        if failure.is_none() {
                            error = observed.clone();
                        } else {
                            // Keep an explicit terminal cause and its source. Only add
                            // observations from this exact attempt/physical connection.
                            let mut context = error.context().clone();
                            if context.diagnostic.is_none() {
                                context.diagnostic = observed.context().diagnostic.clone();
                            }
                            if context.peer_close.is_none() {
                                context.peer_close = observed.context().peer_close.clone();
                            }
                            if context.io_end.is_none() {
                                context.io_end = observed.context().io_end;
                            }
                            error = error.with_context(context);
                        }
                    }
                    if reason == TerminationReason::RetryExhausted
                        && error.kind() != ErrorKind::RetryExhausted
                    {
                        let context = error.context().clone();
                        error = NetError::with_source(ErrorKind::RetryExhausted, error)
                            .with_context(context);
                    }
                    Err(self.annotate(
                        error,
                        state.last_attempt.as_ref().map(|last| &last.attempt),
                        current_connection,
                        None,
                        Some(&details),
                    ))
                }
            };
            let lease = state
                .session_terminal_slot
                .take()
                .ok_or_else(Self::internal)?;
            self.publish(
                state,
                ConnectionEventKind::Closed {
                    result: terminal.clone(),
                },
                lease,
                publication,
            )?;
            if let Err(error) = &terminal {
                publication.next_error = Some(error.clone());
            }
            publication.next_state = Some(ConnectionState::Closed(terminal.clone()));
            state.terminal_result = Some(terminal);
            state.terminated = true;
            state.closing = true;
            publication.close = true;
            Ok(())
        });
        // A poisoned state is also a cleanup completion, never a hanging owner.
        self.close_journal_budget();
        self.finished.cancel();
        result
    }

    pub(crate) fn terminal_result_checked(
        &self,
    ) -> Result<Option<Result<SessionEnd, NetError>>, NetError> {
        Ok(self.lock_state()?.terminal_result.clone())
    }

    pub(crate) fn terminal_result(&self) -> Option<Result<SessionEnd, NetError>> {
        match self.terminal_result_checked() {
            Ok(result) => result,
            Err(error) => Some(Err(error)),
        }
    }

    pub(crate) fn request_cancel(&self) -> bool {
        if self
            .cancel_selected
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }
        self.cancel_requested.cancel();
        true
    }

    /// Store one immutable close request before waking the worker. A full command
    /// queue cannot lose this request or cause a later caller to restart its budget.
    pub(crate) fn request_close(
        &self,
        frame: Option<crate::ws::CloseFrame>,
        deadline: tokio::time::Instant,
    ) -> Result<bool, NetError> {
        let mut state = self.close_request.lock().map_err(NetError::from_poison)?;
        if state.requested {
            return Ok(false);
        }
        state.requested = true;
        state.request = Some(SessionCloseRequest { frame, deadline });
        drop(state);
        self.close_requested.cancel();
        Ok(true)
    }

    pub(crate) fn close_token(&self) -> CancellationToken {
        self.close_requested.clone()
    }

    pub(crate) fn take_close_request(&self) -> Result<Option<SessionCloseRequest>, NetError> {
        self.close_request
            .lock()
            .map(|mut state| state.request.take())
            .map_err(NetError::from_poison)
    }

    fn close_journal_budget(&self) {
        if let Some(journal) = &self.journal {
            journal.slots.close();
            journal.bytes.close();
        }
    }

    fn mutate(
        &self,
        operation: impl FnOnce(&mut SessionState, &mut Publication) -> Result<(), NetError>,
    ) -> Result<(), NetError> {
        let mut publication = Publication::default();
        let result = {
            let mut state = match self.lock_state() {
                Ok(state) => state,
                Err(error) => {
                    self.fail_observation_sources(&error);
                    return Err(error);
                }
            };
            let result = operation(&mut state, &mut publication);
            if publication.failed {
                if let Err(error) = &result {
                    state.delivery_error = Some(error.clone());
                    if state.terminal_result.is_none() {
                        state.terminal_result = Some(Err(error.clone()));
                    }
                }
                state.terminated = true;
                if publication.attempt.is_none() {
                    publication.attempt = state.attempt.take();
                }
                if publication.connection.is_none() {
                    publication.connection = state.connection.take();
                }
                publication.terminal_slot = state.session_terminal_slot.take();
                publication.close = true;
                state.closing = true;
                if let Some(result) = &state.terminal_result {
                    publication.next_state = Some(ConnectionState::Closed(result.clone()));
                    publication.next_error = result.as_ref().err().cloned();
                }
                if let Some(error) = &state.delivery_error {
                    if publication.history_failure.is_none() {
                        publication.history_failure =
                            Some(self.history.prepare_fail(error.clone()));
                    }
                }
            }
            #[cfg(test)]
            {
                let hook = self
                    .before_snapshot
                    .lock()
                    .map_err(NetError::from_poison)?
                    .take();
                if let Some(hook) = hook {
                    hook();
                }
            }
            self.prepare_snapshot(&mut state, &mut publication);
            if let Some(journal) = &self.journal {
                if publication.failed {
                    if let Some(error) = &state.delivery_error {
                        publication.journal_failure =
                            Some(journal.publisher.prepare_fail(error.clone()));
                    }
                } else if publication.close {
                    publication.journal_finish = Some(journal.publisher.prepare_finish());
                }
            }
            result
        };
        let close = publication.close;
        // Queue admission was checked under the session lock. Later callback
        // dispatch failure retires that observation and never cancels transport.
        // Dispatch every effect even when admission failed.
        for effect in &mut publication.journal {
            if let Some(effect) = effect.take() {
                if let Err(error) = effect.dispatch() {
                    if !self.detached_journal_error(&error) {
                        Self::log_observation_error("journal", &error);
                    }
                }
            }
        }
        for effect in [
            publication.journal_finish.take(),
            publication.journal_failure.take(),
        ]
        .into_iter()
        .flatten()
        {
            if let Err(error) = effect.dispatch() {
                if !self.detached_journal_error(&error) {
                    Self::log_observation_error("journal", &error);
                }
            }
        }
        for history in &mut publication.history {
            if let Some(effect) = history.take() {
                if let Err(error) = effect.dispatch() {
                    Self::log_observation_error("history", &error);
                }
            }
        }
        if let Some(effect) = publication.history_failure.take() {
            if let Err(error) = effect.dispatch() {
                Self::log_observation_error("history", &error);
            }
        }
        if let Some(effect) = publication.snapshot.take() {
            if let Err(error) = effect.dispatch() {
                Self::log_observation_error("state", &error);
            }
        }
        if let Some(effect) = publication.snapshot_failure.take() {
            if let Err(error) = effect.dispatch() {
                Self::log_observation_error("state", &error);
            }
        }
        drop(publication);
        if close {
            #[cfg(test)]
            {
                let hook = self
                    .before_finished
                    .lock()
                    .map_err(NetError::from_poison)?
                    .take();
                if let Some(hook) = hook {
                    hook();
                }
            }
            self.cancel_selected.store(true, Ordering::Release);
            self.cancel_requested.cancel();
            self.close_journal_budget();
            self.finished.cancel();
        }
        self.lifecycle_changed.notify_waiters();
        result
    }

    /// A failed transaction lock cannot leave native receivers or lifecycle
    /// waiters pending forever. No session lock is held on this cleanup path.
    fn fail_observation_sources(&self, error: &NetError) {
        if let Some(journal) = &self.journal {
            if let Err(failure) = journal.publisher.prepare_fail(error.clone()).dispatch() {
                Self::log_observation_error("journal", &failure);
            }
        }
        if let Err(failure) = self.history.prepare_fail(error.clone()).dispatch() {
            Self::log_observation_error("history", &failure);
        }
        if let Err(failure) = self
            .state_publisher
            .prepare_failure(error.clone())
            .dispatch()
        {
            Self::log_observation_error("state", &failure);
        }
        self.cancel_selected.store(true, Ordering::Release);
        self.cancel_requested.cancel();
        self.close_journal_budget();
        // Only worker termination or the owning client recovery coordinator may
        // declare completion after this failed state transition.
        self.lifecycle_changed.notify_waiters();
    }

    fn prepare_snapshot(&self, state: &mut SessionState, publication: &mut Publication) {
        if publication.next_state.is_none() && publication.next_error.is_none() {
            return;
        }
        let Some(revision) = state.snapshot.revision.checked_add(1) else {
            if !state.state_failed {
                publication.snapshot_failure = Some(
                    self.state_publisher
                        .prepare_failure(NetError::from(ErrorKind::ResourceExhausted)),
                );
                state.state_failed = true;
            }
            return;
        };
        let snapshot = Arc::new(ConnectionSnapshot {
            revision,
            session_id: state.snapshot.session_id,
            state: publication
                .next_state
                .take()
                .unwrap_or_else(|| state.snapshot.state.clone()),
            last_error: publication
                .next_error
                .take()
                .or_else(|| state.snapshot.last_error.clone()),
        });
        publication.previous_snapshot = Some(std::mem::replace(
            &mut state.snapshot,
            Arc::clone(&snapshot),
        ));
        if state.state_failed {
            return;
        }
        let effect = if state.terminated {
            self.state_publisher.prepare_finish_arc(snapshot)
        } else {
            self.state_publisher.prepare_arc(snapshot)
        };
        let failed = effect.result().err();
        publication.snapshot = Some(effect);
        if let Some(error) = failed {
            publication.snapshot_failure = Some(self.state_publisher.prepare_failure(error));
            state.state_failed = true;
        }
    }

    fn log_observation_error(source: &str, error: &NetError) {
        crate::log_e!(crate::common::log::log_def::LogType::WSC;
            "connection_observation", "source|kind", source, format!("{:?}", error.kind()));
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, SessionState>, NetError> {
        #[cfg(test)]
        if self.failed_state_read.load(Ordering::Acquire) {
            return Err(Self::internal());
        }
        self.state.lock().map_err(|_| Self::internal())
    }

    fn check_attempt(state: &SessionState, cycle_id: u64, attempt_id: u64) -> Result<(), NetError> {
        if !state.terminated
            && !state.closing
            && state.attempt.as_ref().is_some_and(|active| {
                active.context.attempt.cycle_id.as_u64() == cycle_id
                    && active.context.attempt.attempt_id.as_u64() == attempt_id
            })
        {
            Ok(())
        } else {
            Err(NetError::from(ErrorKind::Cancelled))
        }
    }

    fn publish(
        &self,
        state: &mut SessionState,
        kind: ConnectionEventKind,
        lease: EventLease,
        publication: &mut Publication,
    ) -> Result<(), NetError> {
        publication.pending = Some(PendingEvent {
            event: ConnectionEvent {
                sequence: 0,
                client_id: ClientId::from_allocated(self.client_instance_id),
                session_id: SessionId::from_allocated(self.session_id),
                occurred_at: SystemTime::now(),
                kind,
            },
            lease,
        });
        // On every failure the unpublished event and its leases remain owned by
        // Publication. Even an unobserved event can contain a user error source.
        publication.failed = true;
        let sequence = state.last_sequence.checked_add(1).ok_or_else(|| {
            NetError::from(ErrorKind::ResourceExhausted).with_stage(ErrorStage::Dispatch)
        })?;
        let pending = publication.pending.as_mut().ok_or_else(Self::internal)?;
        let size = pending.event.measured_size()?;
        if let EventLease::Observed { bytes, .. } = &mut pending.lease {
            let refund = bytes
                .num_permits()
                .checked_sub(size)
                .ok_or_else(Self::internal)?;
            let target = publication
                .refunds
                .iter_mut()
                .find(|slot| slot.is_none())
                .ok_or_else(Self::internal)?;
            *target = bytes.split(refund);
        }
        pending.event.sequence = sequence;
        state.last_sequence = sequence;
        let history_slot = publication
            .history
            .iter_mut()
            .find(|slot| slot.is_none())
            .ok_or_else(Self::internal)?;
        let effect = self.history.prepare_record(pending.event.clone());
        let observation_failure = effect.result().err();
        *history_slot = Some(effect);
        if let Some(error) = observation_failure {
            if publication.history_failure.is_none() {
                publication.history_failure = Some(self.history.prepare_fail(error));
            }
        }
        if let Some(journal) = &self.journal {
            let target = publication
                .journal
                .iter_mut()
                .find(|slot| slot.is_none())
                .ok_or_else(Self::internal)?;
            let pending = publication.pending.take().ok_or_else(Self::internal)?;
            let resources = match pending.lease {
                EventLease::Observed { slot, bytes } => EventResources {
                    items: Some(slot),
                    bytes: Some(bytes),
                    shared_bytes: None,
                },
                EventLease::Unobserved => EventResources::default(),
            };
            let effect = journal.publisher.prepare_try_publish_with_resources(
                pending.event,
                size,
                resources,
            );
            let error = effect.result().err();
            *target = Some(effect);
            if let Some(error) = error {
                if !self.detached_journal_error(&error) {
                    return Err(error);
                }
            }
        } else {
            let target = publication
                .unobserved
                .iter_mut()
                .find(|slot| slot.is_none())
                .ok_or_else(Self::internal)?;
            *target = publication.pending.take();
        }
        publication.failed = false;
        Ok(())
    }

    fn detached_journal_error(&self, error: &NetError) -> bool {
        error.kind() == ErrorKind::Closed
            && self
                .journal
                .as_ref()
                .is_some_and(|journal| !journal.publisher.is_active())
    }

    fn annotate(
        &self,
        error: NetError,
        attempt: Option<&HandshakeAttempt>,
        connection: Option<&ConnectionInfo>,
        diagnostic: Option<HandshakeDiagnostic>,
        details: Option<&ConnectionTerminationDetails>,
    ) -> NetError {
        let mut context = error.context().clone();
        context.client_id = Some(ClientId::from_allocated(self.client_instance_id));
        context.session_id = Some(SessionId::from_allocated(self.session_id));
        if let Some(attempt) = attempt {
            context.attempt_id = Some(attempt.attempt_id);
        }
        if let Some(connection) = connection {
            context.connection_id = Some(connection.connection_id);
            context.attempt_id = Some(connection.attempt_id);
        }
        if diagnostic.is_some() {
            context.diagnostic = diagnostic;
        }
        if let Some(details) = details {
            if details.peer_close.is_some() {
                context.peer_close = details.peer_close.clone();
            }
            if details.io_end_kind.is_some() {
                context.io_end = details.io_end_kind;
            }
        }
        error.with_context(context)
    }

    fn reason_error(reason: TerminationReason) -> NetError {
        NetError::from(match reason {
            TerminationReason::Cancelled
            | TerminationReason::LocalClose
            | TerminationReason::ClientShutdown
            | TerminationReason::PeerClose => ErrorKind::Cancelled,
            TerminationReason::EngineDropped => ErrorKind::EngineDropped,
            TerminationReason::RetryExhausted => ErrorKind::RetryExhausted,
            TerminationReason::ConnectFailed => ErrorKind::NotConnected,
            TerminationReason::IoFailure | TerminationReason::NetworkUnavailable => ErrorKind::Io,
        })
        .with_stage(ErrorStage::Close)
    }
    fn internal() -> NetError {
        NetError::from(ErrorKind::Internal).with_stage(ErrorStage::Dispatch)
    }

    #[cfg(test)]
    pub(crate) fn fail_state_read_for_test(&self) {
        self.failed_state_read.store(true, Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn before_finished_for_test(
        &self,
        hook: impl FnOnce() + Send + 'static,
    ) -> Result<(), NetError> {
        let previous = self
            .before_finished
            .lock()
            .map_err(NetError::from_poison)?
            .replace(Box::new(hook));
        drop(previous);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn can_lock_for_test(&self) -> bool {
        self.state.try_lock().is_ok()
    }

    #[cfg(test)]
    pub(crate) fn fail_history_allocation_for_test(&self) {
        self.history.fail_next_record_allocation_for_test();
    }

    #[cfg(test)]
    pub(crate) fn fail_state_publication_for_test(&self) {
        self.state_publisher.exhaust_revision_for_test();
    }

    #[cfg(test)]
    fn before_snapshot_for_test(
        &self,
        hook: impl FnOnce() + Send + 'static,
    ) -> Result<(), NetError> {
        let previous = self
            .before_snapshot
            .lock()
            .map_err(NetError::from_poison)?
            .replace(Box::new(hook));
        drop(previous);
        Ok(())
    }
}

#[cfg(test)]
#[path = "connection_session_budget_tests.rs"]
mod budget_tests;
#[cfg(test)]
#[path = "connection_session_diagnostic_tests.rs"]
mod diagnostic_tests;
#[cfg(test)]
#[path = "connection_session_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "connection_session_observation_tests.rs"]
mod observation_tests;

#[cfg(test)]
#[path = "connection_session_observation_regression_tests.rs"]
mod observation_regression_tests;

#[cfg(test)]
#[path = "connection_session_journal_tests.rs"]
mod journal_tests;

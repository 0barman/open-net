//! Native request correlation. The table lock owns terminal arbitration; publications
//! and resource retirement always happen after that lock has been released.
use super::operation_control::{OperationControl, OperationPublication};
use crate::common::log::log_def::LogType;
use crate::error::{ErrorKind, ErrorStage};
use crate::ws::request_control::RegistrationTiming;
use crate::ws::{
    ClientId, ConnectionId, IncomingMessage, Request, RequestHandle, RequestId, RequestLimits,
    RequestOptions, RequestRegistration, RequestSnapshot, ResolveOutcome, Response,
    ResponseTimeoutOrigin, SessionId, TaskEndCause, TerminationOutcome,
};
use crate::{NetError, Result};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant};
use tokio::sync::{oneshot, Notify, OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tokio_util::sync::CancellationToken;

pub(crate) struct NativePending {
    client_id: ClientId,
    session_id: SessionId,
    grace: Duration,
    max_dispatches: usize,
    slots: Arc<Semaphore>,
    admission_closed: CancellationToken,
    state: Mutex<PendingState>,
    changed: Arc<Notify>,
    #[cfg(test)]
    before_close_dispatch: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    #[cfg(test)]
    before_drop: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    #[cfg(test)]
    before_prepare: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    #[cfg(test)]
    fail_disconnected_allocation: std::sync::atomic::AtomicBool,
}

struct PendingState {
    entries: HashMap<RequestId, PendingEntry>,
    dispatches: HashMap<u64, DispatchRecord>,
    next_token: u64,
    next_dispatch: u64,
    active: Option<ConnectionId>,
    closed: bool,
}

struct PendingEntry {
    request: Arc<Request>,
    core: Arc<OperationControl>,
    registration: RequestRegistration,
    timing: Arc<RegistrationTiming>,
    connection_id: Option<ConnectionId>,
    bound_at: Option<Instant>,
    response_timeout: Duration,
    timeout_origin: ResponseTimeoutOrigin,
    absolute_deadline: Option<Instant>,
    grace_deadline: Option<Instant>,
    grace_used: bool,
    deferred_error: Option<NetError>,
    deferred_cause: Option<TaskEndCause>,
    response: oneshot::Sender<Result<Response>>,
    permit: OwnedSemaphorePermit,
    retirement: Option<(OperationPublication, NetError)>,
}

struct DispatchRecord {
    connection_id: ConnectionId,
    token_watermark: u64,
    received_at: Instant,
    expires_at: Instant,
}

pub(crate) struct Completion {
    entry: PendingEntry,
    publication: OperationPublication,
    response: Result<Response>,
}

/// Owns terminal side effects and every possibly final table/error owner until
/// the outer queue lock has been released. No Drop implementation dispatches it.
#[must_use]
#[derive(Default)]
pub(crate) struct DeferredTermination {
    pub(crate) outcome: Option<TerminationOutcome>,
    pub(crate) publication: Option<OperationPublication>,
    pub(crate) completion: Option<Completion>,
    pub(crate) pending: Option<Arc<NativePending>>,
    pub(crate) retired_error: Option<NetError>,
}
impl DeferredTermination {
    pub(crate) fn dispatch(self) {
        let Self {
            completion,
            publication,
            pending,
            retired_error,
            ..
        } = self;
        if let Some(completion) = completion {
            completion.dispatch();
        }
        if let Some(publication) = publication {
            publication.dispatch();
        }
        if let Some(pending) = &pending {
            pending.changed.notify_one();
        }
        drop((retired_error, pending));
    }
}

struct TimerRetirement {
    pending: Weak<NativePending>,
    shutdown: CancellationToken,
}

impl Drop for TimerRetirement {
    fn drop(&mut self) {
        if let Some(pending) = self.pending.upgrade() {
            let error = NetError::from(if self.shutdown.is_cancelled() {
                ErrorKind::Closed
            } else {
                ErrorKind::EngineDropped
            });
            if let Err(error) = pending.close(error, TaskEndCause::Shutdown) {
                crate::log_e!(LogType::WSC; "native_pending_timer_drop", "error", format!("{error:?}"));
            }
        }
    }
}

impl Completion {
    fn dispatch(self) {
        let Self {
            entry,
            publication,
            response,
        } = self;
        let PendingEntry {
            response: sender,
            permit,
            ..
        } = entry;
        drop(permit);
        publication.dispatch();
        if sender.send(response).is_err() {
            crate::log_s!(LogType::WSC; "native_response", "state", "observer_dropped");
        }
    }
}

impl NativePending {
    pub(crate) fn new(
        client_id: ClientId,
        session_id: SessionId,
        limits: RequestLimits,
        max_dispatches: usize,
        slots: Arc<Semaphore>,
    ) -> Result<Arc<Self>> {
        limits.validate()?;
        if limits.max_pending > Semaphore::MAX_PERMITS {
            return Err(NetError::config(
                "requests.max_pending",
                "exceeds semaphore capacity",
            ));
        }
        if max_dispatches == 0 {
            return Err(NetError::config(
                "dispatch.incoming.max_items",
                "must be greater than zero",
            ));
        }
        Ok(Arc::new(Self {
            client_id,
            session_id,
            grace: limits.manual_response_grace,
            max_dispatches,
            slots,
            admission_closed: CancellationToken::new(),
            state: Mutex::new(PendingState {
                entries: HashMap::new(),
                dispatches: HashMap::new(),
                next_token: 0,
                next_dispatch: 0,
                active: None,
                closed: false,
            }),
            changed: Arc::new(Notify::new()),
            #[cfg(test)]
            before_close_dispatch: Mutex::new(None),
            #[cfg(test)]
            before_drop: Mutex::new(None),
            #[cfg(test)]
            before_prepare: Mutex::new(None),
            #[cfg(test)]
            fail_disconnected_allocation: std::sync::atomic::AtomicBool::new(false),
        }))
    }

    fn lock(&self) -> Result<MutexGuard<'_, PendingState>> {
        self.state.lock().map_err(NetError::from_poison)
    }

    pub(crate) async fn reserve_slot(&self) -> Result<OwnedSemaphorePermit> {
        let permit = tokio::select! {
            biased;
            _ = self.admission_closed.cancelled() => {
                return Err(NetError::from(ErrorKind::Closed));
            },
            permit = self.slots.clone().acquire_owned() => {
                permit.map_err(|_| NetError::from(ErrorKind::Closed))?
            },
        };
        if self.admission_closed.is_cancelled() {
            return Err(NetError::from(ErrorKind::Closed));
        }
        Ok(permit)
    }

    pub(crate) fn try_reserve_slot(&self) -> Result<OwnedSemaphorePermit> {
        if self.admission_closed.is_cancelled() {
            return Err(NetError::from(ErrorKind::Closed));
        }
        let permit = self.slots.clone().try_acquire_owned().map_err(|error| {
            NetError::from(match error {
                TryAcquireError::Closed => ErrorKind::Closed,
                TryAcquireError::NoPermits => ErrorKind::PendingLimitReached,
            })
        })?;
        if self.admission_closed.is_cancelled() {
            return Err(NetError::from(ErrorKind::Closed));
        }
        Ok(permit)
    }

    pub(crate) fn register(
        self: &Arc<Self>,
        request: Arc<Request>,
        options: &RequestOptions,
        core: Arc<OperationControl>,
        permit: OwnedSemaphorePermit,
    ) -> Result<(RequestHandle, oneshot::Receiver<Result<Response>>)> {
        options.validate()?;
        crate::ws::validate_metadata(request.metadata())?;
        if core.session_id() != self.session_id {
            return Err(NetError::from(ErrorKind::Closed));
        }
        let (sender, receiver) = oneshot::channel();
        let mut state = self.lock()?;
        if state.closed {
            return Err(NetError::from(ErrorKind::Closed));
        }
        if state.entries.contains_key(request.id()) {
            return Err(NetError::from(ErrorKind::DuplicateRequestId));
        }
        if let Some(result) = core.snapshot()?.result {
            return Err(match result {
                Ok(_) => NetError::from(ErrorKind::Closed),
                Err(error) => error,
            });
        }
        state
            .entries
            .try_reserve(1)
            .map_err(|_| NetError::from(ErrorKind::ResourceExhausted))?;
        let token = state
            .next_token
            .checked_add(1)
            .ok_or_else(|| NetError::from(ErrorKind::ResourceExhausted))?;
        let registered_at = now();
        if earlier(options.registration_deadline, options.send.deadline)
            .is_some_and(|deadline| registered_at >= deadline)
        {
            return Err(timeout());
        }
        let deadline = match options.response_timeout_origin {
            ResponseTimeoutOrigin::Registered => {
                Some(add(registered_at, options.response_timeout)?)
            }
            ResponseTimeoutOrigin::Written => None,
        };
        let timing = Arc::new(RegistrationTiming::new(deadline));
        let registration = RequestRegistration::new(
            request.id().clone(),
            self.session_id,
            registered_at,
            token,
            Arc::downgrade(self),
            timing.clone(),
        );
        core.bind_request(Arc::downgrade(self), request.id().clone(), token)?;
        core.set_response_deadline(earlier(deadline, options.send.deadline));
        let handle = RequestHandle::new(core.clone(), registration.clone());
        state.next_token = token;
        state.entries.insert(
            request.id().clone(),
            PendingEntry {
                request,
                core,
                registration,
                timing,
                connection_id: None,
                bound_at: None,
                response_timeout: options.response_timeout,
                timeout_origin: options.response_timeout_origin,
                absolute_deadline: options.send.deadline,
                grace_deadline: None,
                grace_used: false,
                deferred_error: None,
                deferred_cause: None,
                response: sender,
                permit,
                retirement: None,
            },
        );
        drop(state);
        self.changed.notify_one();
        Ok((handle, receiver))
    }

    pub(crate) fn activate_connection(&self, connection: ConnectionId) -> Result<()> {
        let mut state = self.lock()?;
        if state.closed {
            return Err(NetError::from(ErrorKind::Closed));
        }
        state.active = Some(connection);
        Ok(())
    }

    pub(crate) fn bind_connection(
        &self,
        registration: &RequestRegistration,
        connection: ConnectionId,
    ) -> Result<bool> {
        if !registration.belongs_to(self) {
            return Err(NetError::from(ErrorKind::InvalidInput));
        }
        let mut state = self.lock()?;
        if state.active != Some(connection) {
            return Err(NetError::from(ErrorKind::NotConnected));
        }
        let Some(entry) = matching_entry(&mut state, registration) else {
            return Ok(false);
        };
        if entry.connection_id != Some(connection) {
            entry.bound_at = Some(now());
            entry.connection_id = Some(connection);
        }
        Ok(true)
    }

    pub(crate) fn mark_written(
        &self,
        registration: &RequestRegistration,
        written_at: Instant,
    ) -> Result<bool> {
        if !registration.belongs_to(self) {
            return Err(NetError::from(ErrorKind::InvalidInput));
        }
        let mut state = self.lock()?;
        let Some(entry) = matching_entry(&mut state, registration) else {
            return Ok(false);
        };
        if entry.timeout_origin == ResponseTimeoutOrigin::Written {
            entry
                .timing
                .establish(add(written_at, entry.response_timeout)?)?;
        }
        let response_expired = entry.deadline()?.is_some_and(|deadline| now() >= deadline);
        let grace = if response_expired {
            state
                .entries
                .get(registration.request_id())
                .and_then(|entry| {
                    grace_deadline(entry, &state.dispatches, now(), self.grace, false)
                })
        } else {
            None
        };
        let Some(entry) = matching_entry(&mut state, registration) else {
            return Ok(false);
        };
        if let Some(deadline) = grace {
            entry.grace_used = true;
            entry.grace_deadline = Some(deadline);
            entry.deferred_error = Some(timeout());
            entry.deferred_cause = Some(TaskEndCause::Expired);
        }
        let deadline = entry.deadline()?;
        entry.core.set_response_deadline(deadline);
        let (written, publication) = entry.core.select_written();
        let terminal_error = if !written {
            entry.core.selected_error()
        } else {
            None
        };
        let failed_entry = if terminal_error.is_some() {
            state.entries.remove(registration.request_id())
        } else {
            None
        };
        drop(state);
        if let (Some(entry), Some(error)) = (failed_entry, terminal_error) {
            Completion {
                entry,
                publication,
                response: Err(error),
            }
            .dispatch();
        } else {
            publication.dispatch();
        }
        self.changed.notify_one();
        Ok(written)
    }

    /// Apply a due response timeout synchronously before the writer admits a
    /// new frame. This uses the same one-shot Manual grace as the timer, without
    /// retaining the table lock while publishing completion or releasing permits.
    pub(crate) fn refresh_response_deadline(&self, id: &RequestId, token: u64) -> Result<()> {
        let mut state = self.lock()?;
        let at = now();
        let Some(entry) = state.entries.get(id) else {
            return Ok(());
        };
        if entry.registration.token() != token
            || entry.deadline()?.is_none_or(|deadline| at < deadline)
        {
            return Ok(());
        }
        let grace = grace_deadline(entry, &state.dispatches, at, self.grace, false);
        let Some(entry) = state.entries.get_mut(id) else {
            return Err(NetError::from(ErrorKind::Internal));
        };
        if let Some(deadline) = grace {
            entry.grace_used = true;
            entry.grace_deadline = Some(deadline);
            entry.deferred_error = Some(timeout());
            entry.deferred_cause = Some(TaskEndCause::Expired);
            entry
                .core
                .set_response_deadline(earlier(Some(deadline), entry.absolute_deadline));
            drop(state);
            self.changed.notify_one();
            return Ok(());
        }
        let error = entry
            .deferred_error
            .clone()
            .map_or_else(timeout, |error| error);
        let cause = entry
            .deferred_cause
            .map_or(TaskEndCause::Expired, |cause| cause);
        let (_, publication) = entry.core.select_failure(error.clone(), cause);
        let response = Err(selected_error(&entry.core, error));
        let entry = state.entries.remove(id);
        drop(state);
        if let Some(entry) = entry {
            Completion {
                entry,
                publication,
                response,
            }
            .dispatch();
        } else {
            publication.dispatch();
        }
        self.changed.notify_one();
        Ok(())
    }

    /// Return ownership to a reconnecting queue without resetting registration
    /// timing or discarding a response already accepted for Manual dispatch.
    pub(crate) fn requeue(&self, registration: &RequestRegistration) -> Result<bool> {
        if !registration.belongs_to(self) {
            return Err(NetError::from(ErrorKind::InvalidInput));
        }
        let mut state = self.lock()?;
        let at = now();
        let has_dispatch = state
            .entries
            .get(registration.request_id())
            .is_some_and(|entry| {
                entry.registration.token() == registration.token()
                    && grace_deadline(entry, &state.dispatches, at, self.grace, true).is_some()
            });
        if has_dispatch {
            return Ok(false);
        }
        let Some(entry) = matching_entry(&mut state, registration) else {
            return Ok(false);
        };
        if entry.deferred_error.is_some() || !entry.core.requeue() {
            return Ok(false);
        }
        entry.connection_id = None;
        entry.bound_at = None;
        Ok(true)
    }

    /// Q -> P -> C is permitted; this method only chooses a result and transfers
    /// resources. The caller must dispatch after releasing its own locks.
    pub(crate) fn prepare_termination(
        &self,
        id: &RequestId,
        token: u64,
        error: NetError,
        cause: TaskEndCause,
        original: Option<&OperationControl>,
    ) -> DeferredTermination {
        #[cfg(test)]
        {
            let hook = match self.before_prepare.lock() {
                Ok(mut hook) => hook.take(),
                Err(error) => error.into_inner().take(),
            };
            if let Some(hook) = hook {
                hook();
            }
        }
        let mut deferred = DeferredTermination {
            retired_error: Some(error.clone()),
            ..Default::default()
        };
        // Recover the private table to retire existing entries on a poisoned lock;
        // never invoke user callbacks while the outer queue may still be locked.
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                crate::log_e!(LogType::WSC; "pending_retirement", "error", "lock_poisoned_recovered");
                poisoned.into_inner()
            }
        };
        let Some(entry) = state
            .entries
            .get(id)
            .filter(|entry| entry.registration.token() == token)
        else {
            if let Some(original) = original {
                // A missing/mismatched registration is only a completed result
                // when the original control actually owns one. Never touch a
                // replacement entry with the same application ID.
                let (outcome, publication) = original.select_failure(
                    NetError::from(ErrorKind::EngineDropped),
                    TaskEndCause::Shutdown,
                );
                deferred.outcome = Some(outcome);
                deferred.publication = Some(publication);
            } else {
                deferred.outcome = Some(TerminationOutcome::AlreadyFinished);
            }
            return deferred;
        };
        let (outcome, publication) = entry.core.select_failure(error.clone(), cause);
        let response_error = selected_error(&entry.core, error);
        deferred.outcome = Some(outcome);
        match state.entries.remove(id) {
            Some(entry) => {
                deferred.completion = Some(Completion {
                    entry,
                    publication,
                    response: Err(response_error),
                })
            }
            None => deferred.publication = Some(publication),
        }
        deferred
    }

    pub(crate) fn terminate(
        &self,
        id: &RequestId,
        token: u64,
        error: NetError,
        cause: TaskEndCause,
    ) -> Result<TerminationOutcome> {
        let deferred = self.prepare_termination(id, token, error, cause, None);
        let outcome = deferred
            .outcome
            .ok_or_else(|| NetError::from(ErrorKind::Internal));
        deferred.dispatch();
        outcome
    }

    pub(crate) fn complete_response(
        &self,
        id: &RequestId,
        incoming: &IncomingMessage,
    ) -> Result<bool> {
        if !self.matches_origin(incoming) {
            return Ok(false);
        }
        let response = Response::from_incoming(id.clone(), incoming)?;
        let mut state = self.lock()?;
        if state.active != Some(incoming.connection_id()) {
            return Ok(false);
        }
        let Some(entry) = state.entries.get(id) else {
            return Ok(false);
        };
        if entry.connection_id != Some(incoming.connection_id()) {
            return Ok(false);
        }
        let expired = entry.deadline()?.is_some_and(|deadline| now() >= deadline);
        let (won, completion) = if expired {
            let error = timeout();
            let (_, publication) = entry
                .core
                .select_failure(error.clone(), TaskEndCause::Expired);
            let error = selected_error(&entry.core, error);
            (false, (publication, Err(error)))
        } else {
            let (won, publication) = entry.core.select_response();
            let result = if won {
                Ok(response)
            } else {
                Err(selected_error(
                    &entry.core,
                    NetError::from(ErrorKind::Closed),
                ))
            };
            (won, (publication, result))
        };
        let removed = state.entries.remove(id);
        drop(state);
        if let Some(entry) = removed {
            Completion {
                entry,
                publication: completion.0,
                response: completion.1,
            }
            .dispatch();
        } else {
            completion.0.dispatch();
        }
        self.changed.notify_one();
        Ok(won)
    }

    pub(crate) fn snapshot(&self) -> Result<Vec<RequestSnapshot>> {
        let state = self.lock()?;
        let mut snapshots = Vec::new();
        snapshots
            .try_reserve(state.entries.len())
            .map_err(|_| NetError::from(ErrorKind::ResourceExhausted))?;
        for entry in state.entries.values() {
            snapshots.push(RequestSnapshot {
                operation_id: entry.core.id(),
                request_id: entry.request.id().clone(),
                session_id: self.session_id,
                connection_id: entry.connection_id,
                registered_at: entry.registration.registered_at(),
                response_deadline: entry.timing.deadline()?,
                metadata: entry.request.metadata().clone(),
                operation: entry.core.snapshot()?,
            });
        }
        Ok(snapshots)
    }

    fn matches_origin(&self, incoming: &IncomingMessage) -> bool {
        incoming.client_id() == self.client_id && incoming.session_id() == self.session_id
    }

    pub(crate) fn register_dispatch(
        self: &Arc<Self>,
        incoming: &mut IncomingMessage,
    ) -> Result<()> {
        if incoming.message().is_none() {
            return Ok(());
        }
        if let Some(identity) = incoming.dispatch() {
            return if std::ptr::eq(identity.pending.as_ptr(), self.as_ref()) {
                Ok(())
            } else {
                Err(NetError::from(ErrorKind::InvalidInput))
            };
        }
        if !self.matches_origin(incoming) {
            return Err(NetError::from(ErrorKind::InvalidInput));
        }
        let received_at = now();
        let mut state = self.lock()?;
        if state.closed || state.active != Some(incoming.connection_id()) {
            return Err(NetError::from(ErrorKind::Closed));
        }
        state
            .dispatches
            .retain(|_, record| record.expires_at > received_at);
        let mut expires_at = None;
        for entry in state
            .entries
            .values()
            .filter(|entry| entry.connection_id == Some(incoming.connection_id()))
        {
            let base = match entry.timing.deadline()? {
                Some(deadline) => deadline,
                None => add(received_at, entry.response_timeout)?,
            };
            let extended = add(base, self.grace)?;
            let end = earlier(Some(extended), entry.absolute_deadline);
            if let Some(end) = end.filter(|end| *end > received_at) {
                expires_at = Some(expires_at.map_or(end, |prior: Instant| prior.max(end)));
            }
        }
        let Some(expires_at) = expires_at else {
            return Ok(());
        };
        if state.dispatches.len() >= self.max_dispatches {
            return Err(
                NetError::from(ErrorKind::CallbackOverflow).with_stage(ErrorStage::Dispatch)
            );
        }
        state
            .dispatches
            .try_reserve(1)
            .map_err(|_| NetError::from(ErrorKind::ResourceExhausted))?;
        let id = state
            .next_dispatch
            .checked_add(1)
            .ok_or_else(|| NetError::from(ErrorKind::ResourceExhausted))?;
        let watermark = state.next_token;
        state.dispatches.insert(
            id,
            DispatchRecord {
                connection_id: incoming.connection_id(),
                token_watermark: watermark,
                received_at,
                expires_at,
            },
        );
        state.next_dispatch = id;
        drop(state);
        incoming.bind_dispatch(Arc::downgrade(self), id);
        self.changed.notify_one();
        Ok(())
    }

    pub(crate) fn resolve(
        &self,
        registration: &RequestRegistration,
        incoming: &IncomingMessage,
    ) -> Result<ResolveOutcome> {
        if incoming.message().is_none() {
            return Err(NetError::input(
                "incoming",
                "control frames cannot complete a request",
            ));
        }
        if !registration.belongs_to(self) || !self.matches_origin(incoming) {
            return Ok(ResolveOutcome::ForeignOrigin);
        }
        let Some(identity) = incoming.dispatch() else {
            return Ok(ResolveOutcome::ForeignOrigin);
        };
        if !std::ptr::eq(identity.pending.as_ptr(), self) {
            return Ok(ResolveOutcome::ForeignOrigin);
        }
        let response = Response::from_incoming(registration.request_id().clone(), incoming)?;
        let mut state = self.lock()?;
        let Some(record) = state.dispatches.get(&identity.id) else {
            return Ok(ResolveOutcome::StaleOrFinished);
        };
        let at = now();
        if record.expires_at <= at {
            return Ok(ResolveOutcome::StaleOrFinished);
        }
        if record.connection_id != incoming.connection_id()
            || registration.token() > record.token_watermark
        {
            return Ok(ResolveOutcome::ForeignOrigin);
        }
        let Some(entry) = state.entries.get(registration.request_id()) else {
            return Ok(ResolveOutcome::StaleOrFinished);
        };
        if entry.registration.token() != registration.token() {
            return Ok(ResolveOutcome::StaleOrFinished);
        }
        if entry.connection_id != Some(incoming.connection_id()) {
            return Ok(ResolveOutcome::ForeignOrigin);
        }
        if entry
            .bound_at
            .is_none_or(|bound_at| bound_at > record.received_at)
        {
            return Ok(ResolveOutcome::ForeignOrigin);
        }
        if entry
            .absolute_deadline
            .is_some_and(|deadline| at >= deadline)
        {
            return Ok(ResolveOutcome::StaleOrFinished);
        }
        if state.active != entry.connection_id && entry.deferred_error.is_none() {
            return Ok(ResolveOutcome::StaleOrFinished);
        }
        let effective = entry.deadline()?;
        if effective.is_some_and(|deadline| at >= deadline) {
            // Only a message accepted before the original response deadline may
            // exercise its one fixed grace window, even if the timer was delayed.
            let eligible = entry
                .timing
                .deadline()?
                .is_some_and(|deadline| record.received_at <= deadline);
            let response_bound = entry
                .timing
                .deadline()?
                .and_then(|deadline| deadline.checked_add(self.grace));
            let bounded = earlier(
                earlier(Some(record.expires_at), response_bound),
                entry.absolute_deadline,
            );
            if !eligible || entry.grace_used || bounded.is_none_or(|deadline| at >= deadline) {
                return Ok(ResolveOutcome::StaleOrFinished);
            }
            entry.core.set_response_deadline(bounded);
        }
        let (won, publication) = entry.core.select_response();
        let result = if won {
            Ok(response)
        } else {
            Err(selected_error(
                &entry.core,
                NetError::from(ErrorKind::Closed),
            ))
        };
        let entry = state.entries.remove(registration.request_id());
        state.dispatches.remove(&identity.id);
        drop(state);
        if let Some(entry) = entry {
            Completion {
                entry,
                publication,
                response: result,
            }
            .dispatch();
        } else {
            publication.dispatch();
        }
        self.changed.notify_one();
        Ok(if won {
            ResolveOutcome::Resolved
        } else {
            ResolveOutcome::StaleOrFinished
        })
    }

    /// Revoke a Manual dispatch which no message receiver actually accepted.
    pub(crate) fn discard_dispatch(&self, incoming: &IncomingMessage) -> Result<()> {
        if !self.matches_origin(incoming) {
            return Err(NetError::from(ErrorKind::InvalidInput));
        }
        let Some(identity) = incoming.dispatch() else {
            return Ok(());
        };
        if !std::ptr::eq(identity.pending.as_ptr(), self) {
            return Err(NetError::from(ErrorKind::InvalidInput));
        }
        let mut state = self.lock()?;
        if state
            .dispatches
            .get(&identity.id)
            .is_some_and(|record| record.connection_id != incoming.connection_id())
        {
            return Err(NetError::from(ErrorKind::InvalidInput));
        }
        state.dispatches.remove(&identity.id);
        drop(state);
        self.changed.notify_one();
        Ok(())
    }

    pub(crate) fn fail_connection(&self, connection: ConnectionId, error: NetError) -> Result<()> {
        let mut completions = Vec::new();
        let mut state = self.lock()?;
        if state.active == Some(connection) {
            state.active = None;
        }
        let ids: Vec<_> = state
            .entries
            .iter()
            .filter(|(_, entry)| entry.connection_id == Some(connection))
            .map(|(id, _)| id.clone())
            .collect();
        completions
            .try_reserve(ids.len())
            .map_err(|_| NetError::from(ErrorKind::ResourceExhausted))?;
        for id in ids {
            let grace = state.entries.get(&id).and_then(|entry| {
                grace_deadline(entry, &state.dispatches, now(), self.grace, true)
            });
            let Some(entry) = state.entries.get_mut(&id) else {
                continue;
            };
            if entry
                .grace_deadline
                .is_some_and(|deadline| now() < deadline)
                && entry.deferred_error.is_some()
            {
                continue;
            }
            if !entry.grace_used {
                if let Some(deadline) = grace {
                    entry.grace_used = true;
                    entry.grace_deadline = Some(deadline);
                    entry.deferred_error = Some(error.clone());
                    entry.deferred_cause = Some(TaskEndCause::Disconnected);
                    entry
                        .core
                        .set_response_deadline(earlier(Some(deadline), entry.absolute_deadline));
                    continue;
                }
            }
            let selected = entry
                .deferred_error
                .clone()
                .map_or_else(|| error.clone(), |error| error);
            let cause = entry
                .deferred_cause
                .map_or(TaskEndCause::Disconnected, |cause| cause);
            let (_, publication) = entry.core.select_failure(selected.clone(), cause);
            let result = Err(selected_error(&entry.core, selected));
            if let Some(entry) = state.entries.remove(&id) {
                completions.push(Completion {
                    entry,
                    publication,
                    response: result,
                });
            } else {
                drop(state);
                publication.dispatch();
                for completion in completions {
                    completion.dispatch();
                }
                return Err(NetError::from(ErrorKind::Internal));
            }
        }
        drop(state);
        for completion in completions {
            completion.dispatch();
        }
        self.changed.notify_one();
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn set_before_drop_for_test(
        &self,
        hook: impl FnOnce() + Send + 'static,
    ) -> Result<()> {
        let previous = self
            .before_drop
            .lock()
            .map_err(NetError::from_poison)?
            .replace(Box::new(hook));
        drop(previous);
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn set_before_prepare_for_test(
        &self,
        hook: impl FnOnce() + Send + 'static,
    ) -> Result<()> {
        let previous = self
            .before_prepare
            .lock()
            .map_err(NetError::from_poison)?
            .replace(Box::new(hook));
        drop(previous);
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn fail_disconnected_allocation_for_test(&self) {
        self.fail_disconnected_allocation
            .store(true, std::sync::atomic::Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn set_before_close_dispatch_for_test(
        &self,
        hook: impl FnOnce() + Send + 'static,
    ) -> Result<()> {
        let previous = self
            .before_close_dispatch
            .lock()
            .map_err(NetError::from_poison)?
            .replace(Box::new(hook));
        drop(previous);
        Ok(())
    }
    #[cfg(test)]
    fn before_close_dispatch_for_test(&self) {
        let hook = match self.before_close_dispatch.lock() {
            Ok(mut hook) => hook.take(),
            Err(error) => error.into_inner().take(),
        };
        if let Some(hook) = hook {
            hook();
        }
    }

    pub(crate) fn close(&self, error: NetError, cause: TaskEndCause) -> Result<()> {
        let mut state = self.lock()?;
        state.closed = true;
        state.active = None;
        // Reuse each entry for its deferred publication: shutdown does not need a
        // new Vec allocation, and a missing entry always has a stable winner.
        for entry in state.entries.values_mut() {
            let selected = entry
                .deferred_error
                .clone()
                .map_or_else(|| error.clone(), |error| error);
            let selected_cause = entry.deferred_cause.map_or(cause, |cause| cause);
            let (_, publication) = entry.core.select_failure(selected.clone(), selected_cause);
            let response = selected_error(&entry.core, selected);
            entry.retirement = Some((publication, response));
        }
        let entries = std::mem::take(&mut state.entries);
        state.dispatches.clear();
        drop(state);
        #[cfg(test)]
        self.before_close_dispatch_for_test();
        self.admission_closed.cancel();
        for (_, mut entry) in entries {
            if let Some((publication, error)) = entry.retirement.take() {
                Completion {
                    entry,
                    publication,
                    response: Err(error),
                }
                .dispatch();
            }
        }
        self.changed.notify_one();
        Ok(())
    }

    /// Close admission immediately while preserving only already accepted Manual
    /// response work within its existing, non-renewable grace deadline.
    pub(crate) fn close_disconnected(&self, error: NetError) -> Result<()> {
        let mut completions = Vec::new();
        let mut ids = Vec::new();
        let mut state = self.lock()?;
        #[cfg(test)]
        if self
            .fail_disconnected_allocation
            .swap(false, std::sync::atomic::Ordering::AcqRel)
        {
            return Err(NetError::from(ErrorKind::ResourceExhausted));
        }
        completions
            .try_reserve(state.entries.len())
            .map_err(|_| NetError::from(ErrorKind::ResourceExhausted))?;
        ids.try_reserve(state.entries.len())
            .map_err(|_| NetError::from(ErrorKind::ResourceExhausted))?;
        let at = now();
        for (id, entry) in &state.entries {
            let retained = entry.deferred_error.is_some()
                && entry.grace_deadline.is_some_and(|deadline| deadline > at)
                && entry.absolute_deadline.is_none_or(|deadline| deadline > at);
            if !retained {
                ids.push(id.clone());
            }
        }
        state.closed = true;
        state.active = None;
        for id in ids {
            if let Some(entry) = state.entries.remove(&id) {
                let selected = entry
                    .deferred_error
                    .clone()
                    .map_or_else(|| error.clone(), |error| error);
                let cause = entry
                    .deferred_cause
                    .map_or(TaskEndCause::Disconnected, |cause| cause);
                let (_, publication) = entry.core.select_failure(selected.clone(), cause);
                let response = Err(selected_error(&entry.core, selected));
                completions.push(Completion {
                    entry,
                    publication,
                    response,
                });
            }
        }
        if state.entries.is_empty() {
            state.dispatches.clear();
        }
        drop(state);
        self.admission_closed.cancel();
        for completion in completions {
            completion.dispatch();
        }
        self.changed.notify_one();
        Ok(())
    }

    fn next_deadline(&self) -> Result<(bool, Option<Instant>)> {
        let mut state = self.lock()?;
        if state.closed && state.entries.is_empty() {
            state.dispatches.clear();
            return Ok((true, None));
        }
        let mut deadline = state
            .dispatches
            .values()
            .map(|record| record.expires_at)
            .min();
        for entry in state.entries.values() {
            deadline = earlier(deadline, entry.deadline()?);
        }
        Ok((state.closed && state.entries.is_empty(), deadline))
    }

    fn expire_due(&self, at: Instant) -> Result<()> {
        let mut completions = Vec::new();
        let mut state = self.lock()?;
        state.dispatches.retain(|_, record| record.expires_at > at);
        let mut ids = Vec::new();
        for (id, entry) in &state.entries {
            if entry.deadline()?.is_some_and(|deadline| at >= deadline) {
                ids.push(id.clone());
            }
        }
        completions
            .try_reserve(ids.len())
            .map_err(|_| NetError::from(ErrorKind::ResourceExhausted))?;
        for id in ids {
            let grace = state
                .entries
                .get(&id)
                .and_then(|entry| grace_deadline(entry, &state.dispatches, at, self.grace, false));
            let Some(entry) = state.entries.get_mut(&id) else {
                continue;
            };
            if !entry.grace_used {
                if let Some(deadline) = grace {
                    entry.grace_used = true;
                    entry.grace_deadline = Some(deadline);
                    entry.deferred_error = Some(timeout());
                    entry.deferred_cause = Some(TaskEndCause::Expired);
                    entry
                        .core
                        .set_response_deadline(earlier(Some(deadline), entry.absolute_deadline));
                    continue;
                }
            }
            let error = entry
                .deferred_error
                .clone()
                .map_or_else(timeout, |error| error);
            let cause = entry
                .deferred_cause
                .map_or(TaskEndCause::Expired, |cause| cause);
            let (_, publication) = entry.core.select_failure(error.clone(), cause);
            let result = Err(selected_error(&entry.core, error));
            if let Some(entry) = state.entries.remove(&id) {
                completions.push(Completion {
                    entry,
                    publication,
                    response: result,
                });
            } else {
                drop(state);
                publication.dispatch();
                for completion in completions {
                    completion.dispatch();
                }
                return Err(NetError::from(ErrorKind::Internal));
            }
        }
        drop(state);
        for completion in completions {
            completion.dispatch();
        }
        Ok(())
    }

    /// Spawn once on the existing worker runtime. The wait retains only a notifier.
    pub(crate) fn run_timers(
        pending: Weak<Self>,
        shutdown: CancellationToken,
    ) -> impl std::future::Future<Output = ()> + Send + 'static {
        // Construct outside the async body so even an unpolled task releases
        // registrations when its owning worker/runtime is destroyed.
        let retirement = TimerRetirement {
            pending: pending.clone(),
            shutdown: shutdown.clone(),
        };
        async move {
            let _retirement = retirement;
            loop {
                let Some(owner) = pending.upgrade() else {
                    return;
                };
                if shutdown.is_cancelled() {
                    if let Err(error) =
                        owner.close(NetError::from(ErrorKind::Closed), TaskEndCause::Shutdown)
                    {
                        crate::log_e!(LogType::WSC; "native_pending_shutdown", "error", format!("{error:?}"));
                    }
                    return;
                }
                let changed = owner.changed.clone();
                let next = owner.expire_due(now()).and_then(|()| owner.next_deadline());
                drop(owner);
                let (closed, deadline) = match next {
                    Ok(next) => next,
                    Err(error) => {
                        crate::log_e!(LogType::WSC; "native_pending_timer", "error", format!("{error:?}"));
                        return;
                    }
                };
                if closed {
                    return;
                }
                match deadline {
                    Some(deadline) => tokio::select! {
                        _ = shutdown.cancelled() => {},
                        _ = changed.notified() => {},
                        _ = tokio::time::sleep_until(deadline.into()) => {},
                    },
                    None => tokio::select! {
                        _ = shutdown.cancelled() => {},
                        _ = changed.notified() => {},
                    },
                }
            }
        }
    }
}

impl PendingEntry {
    fn deadline(&self) -> Result<Option<Instant>> {
        Ok(earlier(
            self.grace_deadline.or(self.timing.deadline()?),
            self.absolute_deadline,
        ))
    }
}

impl Drop for NativePending {
    fn drop(&mut self) {
        #[cfg(test)]
        {
            let hook = match self.before_drop.get_mut() {
                Ok(hook) => hook.take(),
                Err(error) => error.into_inner().take(),
            };
            if let Some(hook) = hook {
                hook();
            }
        }
        let state = match self.state.get_mut() {
            Ok(state) => state,
            Err(poisoned) => {
                crate::log_e!(LogType::WSC; "native_pending_drop", "error", "lock_poisoned_recovered");
                poisoned.into_inner()
            }
        };
        let entries = std::mem::take(&mut state.entries);
        self.admission_closed.cancel();
        self.changed.notify_one();
        for (_, entry) in entries {
            let error = NetError::from(ErrorKind::EngineDropped);
            let (_, publication) = entry
                .core
                .select_failure(error.clone(), TaskEndCause::Shutdown);
            let result = Err(selected_error(&entry.core, error));
            Completion {
                entry,
                publication,
                response: result,
            }
            .dispatch();
        }
    }
}

fn matching_entry<'a>(
    state: &'a mut PendingState,
    registration: &RequestRegistration,
) -> Option<&'a mut PendingEntry> {
    state
        .entries
        .get_mut(registration.request_id())
        .filter(|entry| entry.registration.token() == registration.token())
}

fn grace_deadline(
    entry: &PendingEntry,
    dispatches: &HashMap<u64, DispatchRecord>,
    at: Instant,
    grace: Duration,
    disconnected: bool,
) -> Option<Instant> {
    if entry.grace_used
        || entry
            .absolute_deadline
            .is_some_and(|deadline| at >= deadline)
    {
        return None;
    }
    let response_deadline = match entry.timing.deadline() {
        Ok(deadline) => deadline,
        Err(error) => {
            crate::log_e!(LogType::WSC; "native_pending_grace", "error", format!("{error:?}"));
            return None;
        }
    };
    let deadline = dispatches
        .values()
        .filter(|record| {
            entry.connection_id == Some(record.connection_id)
                && entry
                    .bound_at
                    .is_some_and(|bound_at| bound_at <= record.received_at)
                && entry.registration.token() <= record.token_watermark
                && record.expires_at > at
                && response_deadline.is_none_or(|deadline| record.received_at <= deadline)
        })
        .map(|record| record.expires_at)
        .max()?;
    let base = if disconnected {
        at
    } else {
        response_deadline.map_or(at, |deadline| deadline)
    };
    let bound = base.checked_add(grace)?;
    earlier(Some(deadline.min(bound)), entry.absolute_deadline).filter(|deadline| *deadline > at)
}

fn selected_error(core: &OperationControl, fallback: NetError) -> NetError {
    core.selected_error().map_or(fallback, |error| error)
}

fn earlier(left: Option<Instant>, right: Option<Instant>) -> Option<Instant> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (left, right) => left.or(right),
    }
}
fn now() -> Instant {
    tokio::time::Instant::now().into_std()
}
fn add(at: Instant, duration: Duration) -> Result<Instant> {
    at.checked_add(duration)
        .ok_or_else(|| NetError::config("response_timeout", "exceeds monotonic clock range"))
}
fn timeout() -> NetError {
    NetError::from(ErrorKind::TimedOut).with_stage(ErrorStage::Response)
}

//! Registration, resource reservation, and enqueue flow shared by message and request builders.
use super::{
    Message, MessageLane, Request, RequestHandle, RequestOptions, Response, SendOptions, TaskSource,
};
use crate::api::retry::RetrySetting;
use crate::error::ErrorKind;
use crate::module::ws_client::native_pending::PendingLease;
use crate::module::ws_client::native_task_observer::TaskEventReservation;
use crate::module::ws_client::timeout_budget::TimeoutBudget;
use crate::module::ws_client::write::{
    priority_write_queue::PriorityWriteQueue, queued_request::QueuedRequest,
};
use crate::module::ws_client::{
    operation_control::OperationControl, session_runtime::SessionRuntime,
};
use crate::{NetError, Result};
use std::future::Future;
use std::sync::{Arc, Mutex, Weak};
use std::time::Instant;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

pub(super) fn log_invariant(control: &OperationControl, reason: &'static str) {
    crate::log_e!(crate::LogType::WSC; "ws.request.log_invariant",
        "session_id|operation_id|reason", control.session_id().as_u64(), control.id().as_u64(), reason);
}

/// Internal credential for a prepared operation. It commits a reservation or
/// cancels the uncommitted operation when dropped.
pub(super) struct Admission {
    /// Shared control state for this message or request operation.
    pub(super) control: Arc<OperationControl>,
    /// Weak reference to the queue containing the reservation; it does not keep
    /// the queue alive.
    pub(super) queue: Weak<PriorityWriteQueue>,
    /// Registered request handle and response receiver; `None` for normal messages, before registration, or after handover.
    pub(super) request: Option<(RequestHandle, oneshot::Receiver<Result<Response>>)>,
    /// Whether the operation still needs to be canceled on discard; set to `false` after successful submission.
    armed: bool,
}
impl Admission {
    fn fail(&mut self, error: NetError) {
        let cause = match error.kind() {
            ErrorKind::Cancelled => super::TaskEndCause::Cancelled,
            ErrorKind::TimedOut => super::TaskEndCause::Expired,
            ErrorKind::Closed | ErrorKind::NotConnected => super::TaskEndCause::Disconnected,
            _ => super::TaskEndCause::Failed,
        };
        let arg_error_kind = error.kind();
        let arg_error_stage = error.context().stage;
        if let Err(failure) = self.control.terminate(error, cause) {
            crate::log_e!(crate::common::log::log_def::LogType::WSC;"ws.request.fail", "arg_error_kind|arg_error_stage|kind|stage", format!("{arg_error_kind:?}"), arg_error_stage.map(|stage| format!("{stage:?}")), format!("{:?}", failure.kind()), failure.context().stage.map(|stage| format!("{stage:?}")));
        }
    }
    pub(super) fn commit(&mut self) -> Result<()> {
        let result = self
            .queue
            .upgrade()
            .ok_or_else(|| NetError::from(ErrorKind::EngineDropped))
            .and_then(|queue| queue.commit_prepared(&self.control.cancel_token()));
        // Retirement removes the reservation. Preserve the winning terminal
        // cause (timeout, cancellation, shutdown) instead of replacing it with
        // the queue's generic missing-reservation error. The queue lock has
        // already been released before reading the operation state.
        result.map_err(|error| self.control.selected_error().unwrap_or(error))?;
        self.armed = false;
        #[cfg(feature = "quality-doctor")]
        if let Some(probe) = self.control.quality() {
            probe.mark(crate::api::quality::PhaseKind::Published);
        }
        Ok(())
    }
}
impl Drop for Admission {
    fn drop(&mut self) {
        if self.armed {
            if let Err(error) = self.control.cancel() {
                crate::log_e!(crate::common::log::log_def::LogType::WSC;"drop_prepared","error",format!("{error:?}"));
            }
        }
    }
}

/// Session state, send policy, cancellation token, and deadline borrowed by an
/// enqueue operation.
struct AdmissionContext<'a> {
    /// Shared runtime state for the owning session.
    runtime: &'a Arc<SessionRuntime>,
    /// The enqueue, disconnection, timeout and cancellation strategies for this send.
    options: &'a SendOptions,
    /// Admission-lifetime token bound to the current connection or whole session
    /// according to the disconnection policy.
    lease: CancellationToken,
    /// The earlier of the operation deadline or enqueue timeout; `None` when neither is set.
    deadline: Option<Instant>,
    budget: Option<TimeoutBudget>,
}
impl<'a> AdmissionContext<'a> {
    fn new(
        runtime: &'a Arc<SessionRuntime>,
        options: &'a SendOptions,
        started: Instant,
    ) -> Result<Self> {
        Self::new_with_phase(runtime, options, started, super::WsTimeoutPhase::Total, None)
    }
    fn new_with_phase(
        runtime: &'a Arc<SessionRuntime>,
        options: &'a SendOptions,
        started: Instant,
        phase: super::WsTimeoutPhase,
        connection_lease: Option<CancellationToken>,
    ) -> Result<Self> {
        options.validate()?;
        let timeout = options
            .enqueue_timeout
            .map(|duration| {
                started.checked_add(duration).ok_or_else(|| {
                    NetError::config("enqueue_timeout", "deadline is not representable")
                })
            })
            .transpose()?;
        let budget = TimeoutBudget::earlier(
            options.deadline.map(|at| TimeoutBudget::new(at, phase)),
            timeout.map(|at| TimeoutBudget::new(at, super::WsTimeoutPhase::Admission)),
        );
        let deadline = budget.map(|budget| budget.at);
        let context = Self {
            runtime,
            options,
            lease: match connection_lease {
                Some(lease) => lease,
                None => runtime.lease(options.disconnected)?,
            },
            deadline,
            budget,
        };
        context.check(deadline)?;
        Ok(context)
    }
    fn check(&self, deadline: Option<Instant>) -> Result<()> {
        if self.runtime.is_closed() {
            return Err(NetError::from(ErrorKind::Closed));
        }
        if self.lease.is_cancelled() {
            return Err(NetError::from(ErrorKind::NotConnected));
        }
        if self
            .options
            .cancellation
            .as_ref()
            .is_some_and(|group| group.is_cancelled())
        {
            return Err(NetError::from(ErrorKind::Cancelled));
        }
        if deadline.is_some_and(|at| Instant::now() >= at) {
            return Err(self.timeout_error(deadline));
        }
        Ok(())
    }
    fn timeout_error(&self, deadline: Option<Instant>) -> NetError {
        match self.budget.filter(|budget| Some(budget.at) == deadline) {
            Some(budget) => budget.timeout_error(),
            None => NetError::from(ErrorKind::TimedOut),
        }
    }
    async fn wait<T>(
        &self,
        future: impl Future<Output = Result<T>>,
        extra_budget: Option<TimeoutBudget>,
    ) -> Result<T> {
        let budget = TimeoutBudget::earlier(self.budget, extra_budget);
        let deadline = budget.map(|budget| budget.at);
        let annotate = |error: NetError| {
            if error.kind() == ErrorKind::TimedOut && error.ws_timeout_phase().is_none() {
                match budget {
                    Some(budget) => error.with_ws_timeout_phase(budget.phase),
                    None => error,
                }
            } else {
                error
            }
        };
        self.check(deadline).map_err(annotate)?;
        let cancelled = self.runtime.lifecycle.cancel_token();
        let ended = self.runtime.lifecycle.completion_token();
        let closed = self.runtime.closed_token();
        let group = async {
            match &self.options.cancellation {
                Some(group) => group.cancelled().await,
                None => std::future::pending().await,
            }
        };
        let result = tokio::select! {
            biased;
            _ = group => Err(NetError::from(ErrorKind::Cancelled)),
            _ = self.runtime.shutdown.cancelled() => Err(NetError::from(ErrorKind::Closed)),
            _ = cancelled.cancelled() => Err(NetError::from(ErrorKind::Closed)),
            _ = ended.cancelled() => Err(NetError::from(ErrorKind::Closed)),
            _ = closed.cancelled() => Err(NetError::from(ErrorKind::Closed)),
            _ = self.lease.cancelled() => Err(NetError::from(ErrorKind::NotConnected)),
            result = self.runtime.wait_deadline(deadline) => {
                result.and_then(|()| Err(annotate(self.timeout_error(deadline))))
            },
            result = future => result,
        }?;
        self.check(deadline).map_err(annotate)?;
        Ok(result)
    }
    fn queue(&self) -> Arc<PriorityWriteQueue> {
        match self.options.lane {
            MessageLane::Normal => self.runtime.queue.clone(),
            MessageLane::Urgent => self.runtime.urgent_queue.clone(),
        }
    }
}

pub(super) async fn prepare(
    runtime: &Arc<SessionRuntime>,
    message: &Message,
    options: &SendOptions,
    started: Instant,
    request: Option<(&Request, &RequestOptions)>,
    wait: bool,
) -> Result<Admission> {
    prepare_inner(
        runtime, message, options, started, request, wait, None, None,
    )
    .await
}

/// Short synchronous gate ordering registration against logical termination.
pub(super) struct AttemptRegistration<'a> {
    pub absolute_budget: Option<TimeoutBudget>,
    /// Keep every attempt bound to the logical operation's original generation.
    pub connection_lease: &'a CancellationToken,
    pub gate: &'a Mutex<()>,
    pub check: &'a (dyn Fn() -> Result<()> + Send + Sync),
    pub install: &'a (dyn Fn(&RequestHandle) -> Result<()> + Send + Sync),
}

pub(super) async fn prepare_operation_attempt(
    runtime: &Arc<SessionRuntime>,
    request: &Request,
    options: &RequestOptions,
    started: Instant,
    lease: Arc<PendingLease>,
    registration: AttemptRegistration<'_>,
) -> Result<Admission> {
    prepare_inner(
        runtime,
        request.message(),
        &options.send,
        started,
        Some((request, options)),
        true,
        Some(lease),
        Some(registration),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn prepare_inner(
    runtime: &Arc<SessionRuntime>,
    message: &Message,
    options: &SendOptions,
    started: Instant,
    request: Option<(&Request, &RequestOptions)>,
    wait: bool,
    pending_lease: Option<Arc<PendingLease>>,
    registration: Option<AttemptRegistration<'_>>,
) -> Result<Admission> {
    // The synchronous path is deliberately separate: try_* and commit never poll
    // a timer or assume that the caller owns a Tokio runtime.
    if !wait {
        return try_prepare(runtime, message, options, started, request);
    }
    #[cfg(feature = "quality-doctor")]
    let mut quality_scope = crate::module::ws_client::quality::QualityScope::new(runtime.quality_begin(if request.is_some() {
        crate::api::quality::QualityOperationKind::WsRequest
    } else {
        crate::api::quality::QualityOperationKind::WsMessage
    }));
    // Drop the phase interval before its observation owner on every exit path.
    #[cfg(feature = "quality-doctor")]
    let _quality_admission_owner;
    #[cfg(feature = "quality-doctor")]
    let _quality_admission = quality_scope.probe.as_ref().map(|probe| probe.phase(crate::api::quality::PhaseKind::Admission));
    if let Some((request, options)) = request {
        options.validate()?;
        super::validate_metadata(request.metadata())?;
    }
    let resolved = resolved_options(runtime, options, request.is_some())?;
    let options = &resolved;
    let deadline_phase = registration
        .as_ref()
        .and_then(|value| value.absolute_budget)
        .map_or(super::WsTimeoutPhase::Total, |budget| budget.phase);
    let context = AdmissionContext::new_with_phase(
        runtime,
        options,
        started,
        deadline_phase,
        registration.as_ref().map(|value| value.connection_lease.clone()),
    )?;
    let registration_deadline = request.and_then(|(_, options)| options.registration_deadline);
    // Every prerequisite to registration shares its cutoff. Once registered,
    // queue admission is governed only by the send and response budgets.
    let before_registration = minimum(context.deadline, registration_deadline);
    context.check(before_registration).map_err(|error| {
        if error.kind() == ErrorKind::TimedOut && error.ws_timeout_phase().is_none() {
            error.with_ws_timeout_phase(super::WsTimeoutPhase::Admission)
        } else {
            error
        }
    })?;
    let source = match request {
        Some((request, _)) => TaskSource::Request(Arc::new(request.clone())),
        None => TaskSource::Message(Arc::new(message.clone())),
    };
    let reservation = if pending_lease.is_some() {
        TaskEventReservation::unobserved(source.payload_bytes()?, options.lane)
    } else {
        context
            .wait(
                runtime.tasks.snapshot()?.reserve(
                    source.payload_bytes()?,
                    options.lane,
                    &context.lease,
                    before_registration,
                ),
                registration_deadline
                    .map(|at| TimeoutBudget::new(at, super::WsTimeoutPhase::Admission)),
            )
            .await?
    };
    let control = OperationControl::new_with_deadline_phase(
        runtime.client_id,
        runtime.id,
        runtime.allocate_operation()?,
        options,
        request.is_some(),
        source.clone(),
        reservation,
        deadline_phase,
    );
    control.set_retry_started(started);
    #[cfg(feature = "quality-doctor")]
    control.bind_quality(quality_scope.probe.take());
    #[cfg(feature = "quality-doctor")]
    { _quality_admission_owner = control.quality_scope_guard(); }
    let queue = context.queue();
    let mut admission = Admission {
        control: control.clone(),
        queue: Arc::downgrade(&queue),
        request: None,
        armed: true,
    };
    control.bind_queue(Arc::downgrade(&queue));
    control.bind_cancellation()?;
    runtime.register_operation(&control)?;
    let outcome = async {
        if let Some((_, request_options)) = request {
            let slot = match &pending_lease {
                Some(lease) => lease.clone(),
                None => {
                    context
                        .wait(
                            runtime.pending.reserve_slot(),
                            request_options
                                .registration_deadline
                                .map(|at| TimeoutBudget::new(at, super::WsTimeoutPhase::Admission)),
                        )
                        .await?
                }
            };
            let TaskSource::Request(request) = source else {
                log_invariant(&control, "request_source_missing");
                return Err(NetError::from(ErrorKind::Internal));
            };
            let registration_gate = registration
                .as_ref()
                .map(|registration| super::cancellation::lock_recover(registration.gate));
            if let Some(registration) = &registration {
                (registration.check)()?;
            }
            admission.request = Some(runtime.pending.register(
                request,
                request_options,
                control.clone(),
                slot,
            )?);
            if let (Some(registration), Some((handle, _))) = (&registration, &admission.request) {
                (registration.install)(handle)?;
            }
            drop(registration_gate);
        }
        let permits = context
            .wait(queue.reserve(message.len()), control.budget())
            .await?;
        context.check(minimum(context.deadline, control.deadline()))?;
        install(&context, message, &admission, permits)?;
        Ok(())
    }
    .await;
    if let Err(error) = outcome {
        let error = match control.selected_error() {
            Some(selected) => selected,
            None if error.kind() == ErrorKind::TimedOut && error.ws_timeout_phase().is_none() => {
                match TimeoutBudget::earlier(context.budget, control.budget()) {
                    Some(budget) => error.with_ws_timeout_phase(budget.phase),
                    None => error,
                }
            }
            None => error,
        };
        admission.fail(error.clone());
        #[cfg(feature = "quality-doctor")]
        let error = match control.quality_ref() {
            Some(reference) => error.with_quality_ref(reference),
            None => error,
        };
        return Err(error);
    }
    Ok(admission)
}

pub(super) fn try_prepare(
    runtime: &Arc<SessionRuntime>,
    message: &Message,
    options: &SendOptions,
    started: Instant,
    request: Option<(&Request, &RequestOptions)>,
) -> Result<Admission> {
    #[cfg(feature = "quality-doctor")]
    let mut quality_scope = crate::module::ws_client::quality::QualityScope::new(runtime.quality_begin(if request.is_some() {
        crate::api::quality::QualityOperationKind::WsRequest
    } else {
        crate::api::quality::QualityOperationKind::WsMessage
    }));
    // Declaration order keeps the phase interval alive until admission ends.
    #[cfg(feature = "quality-doctor")]
    let _quality_admission_owner;
    #[cfg(feature = "quality-doctor")]
    let _quality_admission = quality_scope.probe.as_ref().map(|probe| probe.phase(crate::api::quality::PhaseKind::Admission));
    if let Some((request, options)) = request {
        options.validate()?;
        super::validate_metadata(request.metadata())?;
    }
    let resolved = resolved_options(runtime, options, request.is_some())?;
    let options = &resolved;
    let context = AdmissionContext::new(runtime, options, started)?;
    context.check(minimum(
        context.deadline,
        request.and_then(|(_, options)| options.registration_deadline),
    ))?;
    let source = match request {
        Some((request, _)) => TaskSource::Request(Arc::new(request.clone())),
        None => TaskSource::Message(Arc::new(message.clone())),
    };
    let reservation = runtime
        .tasks
        .snapshot()?
        .try_reserve(source.payload_bytes()?, options.lane)?;
    let control = OperationControl::new(
        runtime.client_id,
        runtime.id,
        runtime.allocate_operation()?,
        options,
        request.is_some(),
        source.clone(),
        reservation,
    );
    control.set_retry_started(started);
    #[cfg(feature = "quality-doctor")]
    control.bind_quality(quality_scope.probe.take());
    #[cfg(feature = "quality-doctor")]
    { _quality_admission_owner = control.quality_scope_guard(); }
    let queue = context.queue();
    let mut admission = Admission {
        control: control.clone(),
        queue: Arc::downgrade(&queue),
        request: None,
        armed: true,
    };
    control.bind_queue(Arc::downgrade(&queue));
    control.bind_cancellation()?;
    runtime.register_operation(&control)?;
    let outcome = (|| {
        if let Some((_, request_options)) = request {
            context.check(minimum(
                context.deadline,
                request_options.registration_deadline,
            ))?;
            let slot = runtime.pending.try_reserve_slot()?;
            let TaskSource::Request(request) = source else {
                log_invariant(&control, "request_source_missing");
                return Err(NetError::from(ErrorKind::Internal));
            };
            admission.request = Some(runtime.pending.register(
                request,
                request_options,
                control.clone(),
                slot,
            )?);
        }
        let permits = queue.try_reserve(message.len())?;
        context.check(minimum(context.deadline, control.deadline()))?;
        install(&context, message, &admission, permits)?;
        Ok(())
    })();
    if let Err(error) = outcome {
        admission.fail(error.clone());
        #[cfg(feature = "quality-doctor")]
        let error = match control.quality_ref() {
            Some(reference) => error.with_quality_ref(reference),
            None => error,
        };
        return Err(error);
    }
    Ok(admission)
}
fn install(
    context: &AdmissionContext<'_>,
    message: &Message,
    admission: &Admission,
    permits: crate::module::ws_client::write::priority_write_queue::QueuePermits,
) -> Result<()> {
    let queue = admission
        .queue
        .upgrade()
        .ok_or_else(|| NetError::from(ErrorKind::EngineDropped))?;
    let wire = match message {
        Message::Text(text) => tokio_tungstenite::tungstenite::Message::Text(text.clone().into()),
        Message::Binary(bytes) => tokio_tungstenite::tungstenite::Message::Binary(bytes.clone()),
    };
    let registration = admission
        .request
        .as_ref()
        .map(|(handle, _)| handle.registration().clone());
    let queued = QueuedRequest {
        registration,
        message: wire,
        config: context.options.clone(),
        attempt: 0,
        ready_at: None,
        dispatch_cancel: admission.control.cancel_token(),
        dispatch_phase: admission.control.clone(),
        admission_cancel: Some(context.lease.clone()),
        sequence: PriorityWriteQueue::sequence()?,
        slot_permit: Some(permits.item),
        byte_permit: Some(permits.bytes),
        completed: false,
        retirement: None,
    };
    match queue.prepare(queued) {
        Ok(()) => {
            #[cfg(feature = "quality-doctor")]
            if let Some(probe) = admission.control.quality() {
                probe.bytes(message.len() as u64);
                probe.mark(crate::api::quality::PhaseKind::Prepared);
            }
            Ok(())
        }
        Err((request, error)) => {
            request.complete(Err(error.clone()));
            Err(error)
        }
    }
}
fn minimum(left: Option<Instant>, right: Option<Instant>) -> Option<Instant> {
    match (left, right) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}

fn resolved_options(
    runtime: &SessionRuntime,
    options: &SendOptions,
    tracked: bool,
) -> Result<SendOptions> {
    let mut resolved = options.clone();
    resolved.retry_setting = if tracked {
        if let RetrySetting::Policy(policy) = &options.retry_setting {
            if policy.schedule().max_attempts() > 1 {
                return Err(NetError::config(
                    "send.retry_setting",
                    "fixed Request is single-attempt; use request_with_factory for retries",
                ));
            }
        }
        RetrySetting::Disabled
    } else {
        match &options.retry_setting {
            RetrySetting::Inherit => {
                RetrySetting::Policy(runtime.config.default_send_retry_policy.clone())
            }
            setting => setting.clone(),
        }
    };
    Ok(resolved)
}

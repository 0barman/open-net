//! Registration, resource reservation, and enqueue flow shared by message and request builders.
use super::{
    Message, MessageLane, Request, RequestHandle, RequestOptions, Response, SendOptions, TaskSource,
};
use crate::error::ErrorKind;
use crate::module::ws_client::write::{
    priority_write_queue::PriorityWriteQueue, queued_request::QueuedRequest,
};
use crate::module::ws_client::{
    operation_control::OperationControl, session_runtime::SessionRuntime,
};
use crate::{NetError, Result};
use std::future::Future;
use std::sync::{Arc, Weak};
use std::time::Instant;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

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
        if let Err(failure) = self.control.terminate(error, cause) {
            crate::log_e!(crate::common::log::log_def::LogType::WSC;"admission_failure","error",format!("{failure:?}"));
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
}
impl<'a> AdmissionContext<'a> {
    fn new(
        runtime: &'a Arc<SessionRuntime>,
        options: &'a SendOptions,
        started: Instant,
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
        let deadline = minimum(options.deadline, timeout);
        let context = Self {
            runtime,
            options,
            lease: runtime.lease(options.disconnected)?,
            deadline,
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
            return Err(NetError::from(ErrorKind::TimedOut));
        }
        Ok(())
    }
    async fn wait<T>(
        &self,
        future: impl Future<Output = Result<T>>,
        extra_deadline: Option<Instant>,
    ) -> Result<T> {
        let deadline = minimum(self.deadline, extra_deadline);
        self.check(deadline)?;
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
                result.and_then(|()| Err(NetError::from(ErrorKind::TimedOut)))
            },
            result = future => result,
        }?;
        self.check(deadline)?;
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
    // The synchronous path is deliberately separate: try_* and commit never poll
    // a timer or assume that the caller owns a Tokio runtime.
    if !wait {
        return try_prepare(runtime, message, options, started, request);
    }
    if let Some((request, options)) = request {
        options.validate()?;
        super::validate_metadata(request.metadata())?;
    }
    let context = AdmissionContext::new(runtime, options, started)?;
    let registration_deadline = request.and_then(|(_, options)| options.registration_deadline);
    // Every prerequisite to registration shares its cutoff. Once registered,
    // queue admission is governed only by the send and response budgets.
    let before_registration = minimum(context.deadline, registration_deadline);
    context.check(before_registration)?;
    let source = match request {
        Some((request, _)) => TaskSource::Request(Arc::new(request.clone())),
        None => TaskSource::Message(Arc::new(message.clone())),
    };
    let snapshot = runtime.tasks.snapshot()?;
    let reservation = context
        .wait(
            snapshot.reserve(
                source.payload_bytes()?,
                options.lane,
                &context.lease,
                before_registration,
            ),
            registration_deadline,
        )
        .await?;
    let control = OperationControl::new(
        runtime.client_id,
        runtime.id,
        runtime.allocate_operation()?,
        options,
        request.is_some(),
        source.clone(),
        reservation,
    );
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
            let slot = context
                .wait(
                    runtime.pending.reserve_slot(),
                    request_options.registration_deadline,
                )
                .await?;
            let TaskSource::Request(request) = source else {
                return Err(NetError::from(ErrorKind::Internal));
            };
            admission.request = Some(runtime.pending.register(
                request,
                request_options,
                control.clone(),
                slot,
            )?);
        }
        let permits = context
            .wait(queue.reserve(message.len()), control.deadline())
            .await?;
        context.check(minimum(context.deadline, control.deadline()))?;
        install(&context, message, &admission, permits)?;
        Ok(())
    }
    .await;
    if let Err(error) = outcome {
        admission.fail(error.clone());
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
    if let Some((request, options)) = request {
        options.validate()?;
        super::validate_metadata(request.metadata())?;
    }
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
        dispatch_cancel: admission.control.cancel_token(),
        dispatch_phase: admission.control.clone(),
        admission_cancel: Some(context.lease.clone()),
        sequence: PriorityWriteQueue::sequence()?,
        slot_permit: Some(permits.item),
        byte_permit: Some(permits.bytes),
        completed: false,
    };
    match queue.prepare(queued) {
        Ok(()) => Ok(()),
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

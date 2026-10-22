//! Logical requests rebuild opaque protocol messages for every bounded attempt.
use super::cancellation::{lock_recover, CancelHookGuard};
use super::*;
use crate::api::retry::{RetryDecision, RetryEvent, RetryReport, RetrySetting, RetryStopReason};
use crate::error::{ErrorKind, ErrorStage};
use crate::module::ws_client::native_pending::PendingLease;
use crate::module::ws_client::timeout_budget::TimeoutBudget;
use crate::module::ws_client::{
    native_task_observer::TaskEventReservation, session_runtime::SessionRuntime,
};
use crate::retry_core::RetryState;
use crate::{EnqueueError, Metadata, NetError, Result};
use std::fmt;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

type AttemptFactory = dyn Fn(&RequestAttemptContext) -> Result<Message> + Send + Sync;

/// Immutable identity supplied to a synchronous message factory. The application
/// must encode the fresh wire ID and stable key in its own protocol.
#[derive(Clone)]
pub struct RequestAttemptContext {
    operation_id: OperationId,
    attempt: u32,
    request_id: RequestId,
    idempotency_key: Arc<str>,
}
impl RequestAttemptContext {
    pub fn operation_id(&self) -> OperationId {
        self.operation_id
    }
    pub fn attempt(&self) -> u32 {
        self.attempt
    }
    pub fn request_id(&self) -> &RequestId {
        &self.request_id
    }
    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }
}
impl fmt::Debug for RequestAttemptContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RequestAttemptContext")
            .field("operation_id", &self.operation_id)
            .field("attempt", &self.attempt)
            .field("request_id", &self.request_id)
            .field("idempotency_key_bytes", &self.idempotency_key.len())
            .finish()
    }
}

/// Limits for one logical request. Total time starts when enqueue/send is first
/// polled, and includes admission, construction, reconnection, backoff and all attempts.
#[derive(Clone)]
pub struct WsRequestOperationOptions {
    pub priority: Priority,
    pub admission_timeout: Option<Duration>,
    pub write_timeout: Duration,
    pub response_timeout: Duration,
    pub response_timeout_origin: ResponseTimeoutOrigin,
    pub attempt_timeout: Option<Duration>,
    pub total_timeout: Option<Duration>,
    pub deadline: Option<Instant>,
    pub disconnected: DisconnectedPolicy,
    pub cancellation: Option<CancellationGroup>,
    pub metadata: Metadata,
    pub retry_setting: RetrySetting<WsRequestRetryPolicy>,
}
impl fmt::Debug for WsRequestOperationOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WsRequestOperationOptions")
            .field("priority", &self.priority)
            .field("admission_timeout", &self.admission_timeout)
            .field("write_timeout", &self.write_timeout)
            .field("response_timeout", &self.response_timeout)
            .field("response_timeout_origin", &self.response_timeout_origin)
            .field("attempt_timeout", &self.attempt_timeout)
            .field("total_timeout", &self.total_timeout)
            .field("deadline", &self.deadline)
            .field("disconnected", &self.disconnected)
            .field("cancellation", &self.cancellation)
            .field("metadata_entries", &self.metadata.len())
            .field("retry_setting", &self.retry_setting)
            .finish()
    }
}
impl Default for WsRequestOperationOptions {
    fn default() -> Self {
        Self {
            priority: Priority::Normal,
            admission_timeout: None,
            write_timeout: Duration::from_secs(10),
            response_timeout: Duration::from_secs(30),
            response_timeout_origin: ResponseTimeoutOrigin::Written,
            attempt_timeout: None,
            total_timeout: None,
            deadline: None,
            disconnected: DisconnectedPolicy::Reject,
            cancellation: None,
            metadata: Metadata::new(),
            retry_setting: RetrySetting::Inherit,
        }
    }
}
impl WsRequestOperationOptions {
    pub fn validate(&self) -> Result<()> {
        for (name, duration) in [
            ("admission_timeout", self.admission_timeout),
            ("attempt_timeout", self.attempt_timeout),
            ("total_timeout", self.total_timeout),
            ("write_timeout", Some(self.write_timeout)),
            ("response_timeout", Some(self.response_timeout)),
        ] {
            if let Some(duration) = duration {
                super::config::deadline(duration, name, true)?;
            }
        }
        validate_metadata(&self.metadata)
    }
}

/// Builder retaining the protocol factory until its operation terminates.
#[must_use]
pub struct WsRequestOperationBuilder {
    client: RequestClient,
    key: Arc<str>,
    factory: Arc<AttemptFactory>,
    options: WsRequestOperationOptions,
    started: Option<Instant>,
}
impl fmt::Debug for WsRequestOperationBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WsRequestOperationBuilder")
            .field("client", &self.client)
            .field("key_bytes", &self.key.len())
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}
impl RequestClient {
    /// Creates a logical operation. The factory must encode both context IDs;
    /// response routing must extract the current attempt's wire request ID.
    pub fn request_with_factory<F>(
        &self,
        key: impl Into<String>,
        factory: F,
    ) -> Result<WsRequestOperationBuilder>
    where
        F: Fn(&RequestAttemptContext) -> Result<Message> + Send + Sync + 'static,
    {
        let key = key.into();
        if key.len() > 1024 || key.trim().is_empty() {
            return Err(NetError::input(
                "idempotency_key",
                "must contain a non-whitespace character and be at most 1024 UTF-8 bytes",
            ));
        }
        Ok(WsRequestOperationBuilder {
            client: self.clone(),
            key: Arc::from(key),
            factory: Arc::new(factory),
            options: WsRequestOperationOptions::default(),
            started: None,
        })
    }
}
impl WsRequestOperationBuilder {
    /// Replace the operation policy without merging fields or resetting submission time.
    pub fn retry_setting(mut self, setting: RetrySetting<WsRequestRetryPolicy>) -> Self {
        self.options.retry_setting = setting;
        self
    }
    pub fn options(mut self, options: WsRequestOperationOptions) -> Self {
        self.options = options;
        self.started = None;
        self
    }
    pub async fn send(self) -> Result<Response> {
        self.enqueue()
            .await
            .map_err(EnqueueError::into_error)?
            .response()
            .await
    }
    /// Reserve one pending-operation slot and one terminal event. Individual
    /// attempts share that slot, including backoff, so max_pending=1 can retry.
    pub async fn enqueue(
        mut self,
    ) -> std::result::Result<WsRequestOperationReceipt, EnqueueError<Self>> {
        let started = *self.started.get_or_insert_with(Instant::now);
        match self.admit(started).await {
            Ok(receipt) => Ok(receipt),
            Err(error) => {
                let error =
                    if error.kind() == ErrorKind::TimedOut && error.ws_timeout_phase().is_none() {
                        let total = earlier(
                            self.options.deadline,
                            add(started, self.options.total_timeout).ok().flatten(),
                        );
                        let admission = add(started, self.options.admission_timeout).ok().flatten();
                        error.with_ws_timeout_phase(
                            if total.is_some() && earlier(total, admission) == total {
                                WsTimeoutPhase::Total
                            } else {
                                WsTimeoutPhase::Admission
                            },
                        )
                    } else {
                        error
                    };
                let report = RetryReport {
                    elapsed: started.elapsed(),
                    stop_reason: Some(error_stop_reason(&error)),
                    ..RetryReport::default()
                };
                Err(EnqueueError::new(self, error.with_retry_report(report)))
            }
        }
    }
    async fn admit(&self, started: Instant) -> Result<WsRequestOperationReceipt> {
        self.options.validate()?;
        let runtime = &self.client.runtime;
        let deadline = earlier(
            self.options.deadline,
            add(started, self.options.total_timeout)?,
        );
        let admission_deadline = earlier(deadline, add(started, self.options.admission_timeout)?);
        let cancellation = CancellationToken::new();
        // Reject captures the current physical generation before any bounded wait.
        let admission_lease = runtime.lease(self.options.disconnected)?;
        let source = TaskSource::RequestOperation(Arc::new(self.options.metadata.clone()));
        let snapshot = runtime.tasks.snapshot()?;
        let reservation = admission_scope(
            &admission_lease,
            runtime,
            &self.options,
            &cancellation,
            admission_deadline,
            snapshot.reserve(
                source.payload_bytes()?,
                MessageLane::Normal,
                &cancellation,
                admission_deadline,
            ),
        )
        .await?;
        let lease = admission_scope(
            &admission_lease,
            runtime,
            &self.options,
            &cancellation,
            admission_deadline,
            runtime.pending.reserve_slot(),
        )
        .await?;
        check_scope(runtime, &self.options, &cancellation, admission_deadline)?;
        if admission_lease.is_cancelled() {
            return Err(NetError::from(ErrorKind::NotConnected));
        }
        let policy = match &self.options.retry_setting {
            RetrySetting::Inherit => runtime.config.default_request_retry_policy.clone(),
            RetrySetting::Disabled => WsRequestRetryPolicy::no_retry(),
            RetrySetting::Policy(policy) => policy.clone(),
        };
        let (sender, receiver) = oneshot::channel();
        let control = Arc::new(LogicalControl {
            id: runtime.allocate_operation()?,
            runtime: runtime.clone(),
            started,
            deadline,
            cancellation,
            connection_lease: admission_lease.clone(),
            policy: policy.clone(),
            options: self.options.clone(),
            source,
            registration_gate: Mutex::new(()),
            state: Mutex::new(LogicalState {
                retry: RetryState::new(policy.schedule().clone()),
                current: None,
                terminal: false,
                elapsed: None,
                delivery: DeliveryEvidence::NotStarted,
                phase: OperationPhase::WaitingForCapacity,
                connection: None,
                response: Some(sender),
                reservation: Some(reservation),
                lease: Some(lease),
                hook: None,
            }),
        });
        if let Some(group) = &self.options.cancellation {
            let weak = Arc::downgrade(&control);
            let hook = group.inner.bind_cancel_hook(Arc::new(move || {
                if let Some(control) = weak.upgrade() {
                    control.finish(
                        Err(NetError::from(ErrorKind::Cancelled)),
                        RetryStopReason::Cancelled,
                    );
                }
            }))?;
            lock_recover(&control.state).hook = Some(hook);
        }
        let handle = WsRequestOperationHandle {
            control: control.clone(),
        };
        check_scope(
            runtime,
            &self.options,
            &control.cancellation,
            admission_deadline,
        )?;
        if admission_lease.is_cancelled() {
            return Err(NetError::from(ErrorKind::NotConnected));
        }
        let guard = RunnerGuard(control.clone());
        let factory = self.factory.clone();
        let key = self.key.clone();
        runtime.spawn_request(async move {
            let _guard = guard;
            run(control, factory, key, admission_deadline).await;
        })?;
        Ok(WsRequestOperationReceipt {
            handle,
            response: receiver,
        })
    }
}

/// Cloneable control for the stable operation, independent of individual registrations.
#[derive(Clone)]
pub struct WsRequestOperationHandle {
    control: Arc<LogicalControl>,
}
impl WsRequestOperationHandle {
    pub fn id(&self) -> OperationId {
        self.control.id
    }
    /// Snapshot only: None before registration, during backoff, and after completion.
    pub fn current_registration(&self) -> Option<RequestRegistration> {
        lock_recover(&self.control.state)
            .current
            .as_ref()
            .map(|handle| handle.registration().clone())
    }
    pub fn retry_report(&self) -> RetryReport {
        self.control.report()
    }
    pub fn cancel(&self) -> Result<TerminationOutcome> {
        Ok(self.control.finish(
            Err(NetError::from(ErrorKind::Cancelled)),
            RetryStopReason::Cancelled,
        ))
    }
}
impl fmt::Debug for WsRequestOperationHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WsRequestOperationHandle")
            .field("id", &self.id())
            .field("retry_report", &self.retry_report())
            .finish()
    }
}
#[must_use]
#[derive(Debug)]
pub struct WsRequestOperationReceipt {
    handle: WsRequestOperationHandle,
    response: oneshot::Receiver<Result<Response>>,
}
impl WsRequestOperationReceipt {
    pub fn handle(&self) -> &WsRequestOperationHandle {
        &self.handle
    }
    /// Dropping the receipt/future stops waiting and does not cancel the operation.
    pub async fn response(self) -> Result<Response> {
        self.response.await.map_err(|_| {
            NetError::from(ErrorKind::EngineDropped).with_retry_report(self.handle.retry_report())
        })?
    }
}

struct LogicalControl {
    id: OperationId,
    runtime: Arc<SessionRuntime>,
    started: Instant,
    deadline: Option<Instant>,
    cancellation: CancellationToken,
    /// Reject retains the physical generation captured at submission. A
    /// reconnect cannot renew this authority between factory or retry phases.
    connection_lease: CancellationToken,
    policy: WsRequestRetryPolicy,
    options: WsRequestOperationOptions,
    source: TaskSource,
    registration_gate: Mutex<()>,
    state: Mutex<LogicalState>,
}
struct LogicalState {
    retry: RetryState,
    current: Option<RequestHandle>,
    terminal: bool,
    elapsed: Option<Duration>,
    delivery: DeliveryEvidence,
    phase: OperationPhase,
    connection: Option<ConnectionId>,
    response: Option<oneshot::Sender<Result<Response>>>,
    reservation: Option<TaskEventReservation>,
    lease: Option<Arc<PendingLease>>,
    hook: Option<CancelHookGuard>,
}
impl LogicalControl {
    fn report(&self) -> RetryReport {
        let state = lock_recover(&self.state);
        let mut report = state.retry.report(
            state
                .elapsed
                .map_or_else(|| self.started.elapsed(), |elapsed| elapsed),
        );
        if let Some(current) = &state.current {
            report.transport_sends = report
                .transport_sends
                .saturating_add(current.retry_report().transport_sends);
        }
        report
    }
    fn notify(&self, event: RetryEvent) {
        if let Some(observer) = self.policy.observer() {
            observer.notify(event);
        }
    }
    fn check(&self) -> Result<()> {
        self.check_result(false)
    }
    fn check_result(&self, selected: bool) -> Result<()> {
        check_result_scope(
            &self.runtime,
            &self.options,
            &self.cancellation,
            self.deadline,
            || selected,
        )
        .map_err(|error| {
            if error.kind() == ErrorKind::TimedOut {
                error.with_ws_timeout_phase(WsTimeoutPhase::Total)
            } else {
                error
            }
        })
    }
    /// Only unfinished work needs a live generation. Already selected outcomes
    /// retain the pending table's result, subject to cancellation and total time.
    fn check_connection(&self) -> Result<()> {
        self.check()?;
        if self.connection_lease.is_cancelled() {
            return Err(NetError::from(ErrorKind::NotConnected));
        }
        Ok(())
    }
    fn clear_current(&self) -> Option<RequestHandle> {
        let mut state = lock_recover(&self.state);
        let current = state.current.take();
        if let Some(current) = &current {
            // Retire before sampling so the writer cannot start between the
            // transport counter snapshot and removal of this registration.
            let _ = current.cancel();
            absorb(&mut state, current);
        }
        current
    }
    fn finish(&self, result: Result<Response>, reason: RetryStopReason) -> TerminationOutcome {
        let registration_gate = lock_recover(&self.registration_gate);
        let (current, sender, reservation, lease, hook, report, event, outcome) = {
            let mut state = lock_recover(&self.state);
            if state.terminal {
                return TerminationOutcome::AlreadyFinished;
            }
            let current = state.current.take();
            // Retire the internal single-attempt control before sampling sends;
            // this closes the race with a concurrent first transport write.
            if let Some(current) = &current {
                let _ = current.cancel();
                absorb(&mut state, current);
            }
            if let Ok(response) = &result {
                state.delivery = DeliveryEvidence::ResponseConfirmed;
                state.connection = Some(response.connection_id());
                state.phase = OperationPhase::AwaitingResponse;
            }
            state.terminal = true;
            state.retry.finish(reason);
            let elapsed = self.started.elapsed();
            state.elapsed = Some(elapsed);
            let report = state.retry.report(elapsed);
            let outcome = match state.delivery {
                DeliveryEvidence::NotStarted => TerminationOutcome::TerminatedBeforeWrite,
                DeliveryEvidence::Unknown => TerminationOutcome::DeliveryUnknown,
                _ => TerminationOutcome::TerminatedAfterWrite,
            };
            let event = TaskEvent {
                client_id: self.runtime.client_id,
                session_id: self.runtime.id,
                operation_id: self.id,
                connection_id: state.connection,
                source: self.source.clone(),
                lane: MessageLane::Normal,
                phase: state.phase,
                delivery: state.delivery,
                cause: end_cause(&result, reason),
                result: result
                    .as_ref()
                    .map(|_| TaskSuccess::ResponseReceived)
                    .map_err(|error| error.clone().with_retry_report(report.clone())),
            };
            (
                current,
                state.response.take(),
                state.reservation.take(),
                state.lease.take(),
                state.hook.take(),
                report,
                event,
                outcome,
            )
        };
        drop(registration_gate);
        self.cancellation.cancel();
        if let Some(current) = current {
            let _ = current.cancel();
        }
        if let Some(lease) = lease {
            lease.release();
        }
        drop(hook);
        let result = result
            .map(|response| response.with_retry_report(report.clone()))
            .map_err(|error| error.with_retry_report(report.clone()));
        if let Some(reservation) = reservation {
            if let Err(error) = reservation.publish(event) {
                crate::log_e!(crate::LogType::WSC; "ws.request_operation.publish", "error", format!("{error:?}"));
            }
        }
        self.notify(RetryEvent::Completed(report));
        if let Some(sender) = sender {
            let _ = sender.send(result);
        }
        outcome
    }
}
fn absorb(state: &mut LogicalState, current: &RequestHandle) {
    let sends = current.retry_report().transport_sends;
    for _ in 0..sends {
        let _ = state.retry.record_transport_send();
    }
    if let Ok(snapshot) = current.state() {
        if delivery_rank(snapshot.delivery) > delivery_rank(state.delivery) {
            state.delivery = snapshot.delivery;
        }
        if snapshot.phase != OperationPhase::Finished {
            state.phase = snapshot.phase;
        }
        if let Some(Err(error)) = snapshot.result {
            state.connection = error.context().connection_id.or(state.connection);
            state.phase = match error.context().stage {
                Some(ErrorStage::Write) => OperationPhase::Writing,
                Some(ErrorStage::Response) => OperationPhase::AwaitingResponse,
                Some(ErrorStage::Admission) => OperationPhase::WaitingForCapacity,
                _ => state.phase,
            };
        }
    }
}
fn delivery_rank(value: DeliveryEvidence) -> u8 {
    match value {
        DeliveryEvidence::NotStarted => 0,
        DeliveryEvidence::Unknown => 1,
        DeliveryEvidence::Written => 2,
        DeliveryEvidence::ResponseConfirmed => 3,
    }
}
struct RunnerGuard(Arc<LogicalControl>);
impl Drop for RunnerGuard {
    fn drop(&mut self) {
        self.0.finish(
            Err(NetError::from(ErrorKind::EngineDropped)),
            RetryStopReason::Shutdown,
        );
    }
}
async fn run(
    control: Arc<LogicalControl>,
    factory: Arc<AttemptFactory>,
    key: Arc<str>,
    admission_deadline: Option<Instant>,
) {
    // Keep the previous winner until the next attempt actually begins. Natural
    // retirement during backoff must not turn that winner into Closed.
    let mut previous_result = None;
    loop {
        if let Err(error) = control.check_connection() {
            if let Some((result, selected)) = previous_result.take() {
                finish_after_attempt_error(&control, result, selected, error);
            } else {
                finish_error(&control, error);
            }
            return;
        }
        drop(previous_result.take());
        let attempt = {
            let mut state = lock_recover(&control.state);
            if state.terminal {
                return;
            }
            match state.retry.begin_attempt() {
                Ok(attempt) => attempt,
                Err(_) => {
                    drop(state);
                    finish_error(&control, NetError::from(ErrorKind::InvalidPolicy));
                    return;
                }
            }
        };
        let attempt_started = Instant::now();
        control.notify(RetryEvent::AttemptStarted { attempt });
        let result = attempt_once(
            &control,
            &factory,
            key.clone(),
            attempt,
            attempt_started,
            if attempt == 1 {
                admission_deadline
            } else {
                None
            },
        )
        .await;
        let selected = lock_recover(&control.state)
            .current
            .as_ref()
            .is_some_and(RequestHandle::has_selected_result);
        if let Some(current) = control.clear_current() {
            let _ = current.cancel();
        }
        if lock_recover(&control.state).terminal {
            return;
        }
        if let Err(error) = control.check_result(selected) {
            control.notify(RetryEvent::AttemptFinished {
                attempt,
                final_attempt: true,
            });
            finish_error(&control, error);
            return;
        }
        let mut decision = classify(&control, &result);
        if let Err(error) = control.check_result(selected) {
            control.notify(RetryEvent::AttemptFinished {
                attempt,
                final_attempt: true,
            });
            finish_error(&control, error);
            return;
        }
        if selected && control.runtime.preserves_selected_result()
            && matches!(decision, Ok(RetryStep::Retry(_)))
        {
            decision = Ok(RetryStep::Stop(RetryStopReason::NotRetryable));
        }
        let delay = match decision {
            Ok(RetryStep::Retry(delay)) => delay,
            Ok(RetryStep::Stop(reason)) => {
                control.notify(RetryEvent::AttemptFinished {
                    attempt,
                    final_attempt: true,
                });
                if let Err(error) = control.check_result(selected) {
                    finish_error(&control, error);
                } else {
                    control.finish(result, reason);
                }
                return;
            }
            Err(error) => {
                control.notify(RetryEvent::AttemptFinished {
                    attempt,
                    final_attempt: true,
                });
                control.finish(Err(error), RetryStopReason::PolicyFailed);
                return;
            }
        };
        control.notify(RetryEvent::AttemptFinished {
            attempt,
            final_attempt: false,
        });
        if let Err(error) = control.check() {
            finish_after_attempt_error(&control, result, selected, error);
            return;
        }
        lock_recover(&control.state).phase = OperationPhase::Queued;
        control.notify(RetryEvent::RetryScheduled { attempt, delay });
        let ready = match Instant::now().checked_add(delay) {
            Some(ready) => ready,
            None => {
                finish_error(&control, NetError::from(ErrorKind::InvalidPolicy));
                return;
            }
        };
        let sleep = control.runtime.wait_deadline(Some(ready));
        if let Err(error) = admission_scope(
            &control.connection_lease,
            &control.runtime,
            &control.options,
            &control.cancellation,
            control.deadline,
            sleep,
        )
        .await
        {
            let error = if error.kind() == ErrorKind::TimedOut {
                error.with_ws_timeout_phase(WsTimeoutPhase::Total)
            } else {
                error
            };
            finish_after_attempt_error(&control, result, selected, error);
            return;
        }
        previous_result = Some((result, selected));
    }
}
async fn attempt_once(
    control: &Arc<LogicalControl>,
    factory: &Arc<AttemptFactory>,
    key: Arc<str>,
    attempt: u32,
    started: Instant,
    admission_deadline: Option<Instant>,
) -> Result<Response> {
    check_attempt_budget(control, started)?;
    control.check_connection()?;
    let attempt_budget = TimeoutBudget::earlier(
        control
            .deadline
            .map(|at| TimeoutBudget::new(at, WsTimeoutPhase::Total)),
        add(started, control.options.attempt_timeout)?
            .map(|at| TimeoutBudget::new(at, WsTimeoutPhase::Attempt)),
    );
    let attempt_deadline = attempt_budget.map(|budget| budget.at);
    let context = RequestAttemptContext {
        operation_id: control.id,
        attempt,
        request_id: RequestId::random()?,
        idempotency_key: key,
    };
    let built = super::retry::isolate_classifier(|| factory(&context));
    // A synchronous factory cannot be preempted, so arbitrate its elapsed time
    // before accepting either its success or its returned construction error.
    check_attempt_budget(control, started)?;
    control.check_connection()?;
    let message = built?.map_err(|error| error.with_stage(ErrorStage::RequestBuild))?;
    let request =
        Request::new(context.request_id, message).with_metadata(control.options.metadata.clone());
    let options = RequestOptions {
        send: SendOptions {
            priority: control.options.priority,
            write_timeout: control.options.write_timeout,
            deadline: attempt_deadline,
            disconnected: control.options.disconnected,
            cancellation: control.options.cancellation.clone(),
            retry_setting: RetrySetting::Disabled,
            metadata: control.options.metadata.clone(),
            ..SendOptions::default()
        },
        response_timeout: control.options.response_timeout,
        response_timeout_origin: control.options.response_timeout_origin,
        registration_deadline: admission_deadline,
    };
    let lease = lock_recover(&control.state)
        .lease
        .clone()
        .ok_or_else(|| NetError::from(ErrorKind::Cancelled))?;
    let registration_check = || {
        control.check_connection()?;
        if lock_recover(&control.state).terminal {
            return Err(NetError::from(ErrorKind::Cancelled));
        }
        Ok(())
    };
    let on_registered = |handle: &RequestHandle| {
        let mut state = lock_recover(&control.state);
        if state.terminal {
            return Err(NetError::from(ErrorKind::Cancelled));
        }
        state.current = Some(handle.clone());
        state.phase = OperationPhase::WaitingForCapacity;
        Ok(())
    };
    let mut admission = scope_attempt(
        &control.runtime,
        &control.options,
        &control.cancellation,
        control.deadline,
        TimeoutBudget::earlier(
            attempt_budget,
            admission_deadline.map(|at| TimeoutBudget::new(at, WsTimeoutPhase::Admission)),
        ),
        None,
        dispatch::prepare_operation_attempt(
            &control.runtime,
            &request,
            &options,
            control.started,
            lease,
            dispatch::AttemptRegistration {
                absolute_budget: attempt_budget,
                connection_lease: &control.connection_lease,
                gate: &control.registration_gate,
                check: &registration_check,
                install: &on_registered,
            },
        ),
    )
    .await
    .map_err(|error| {
        let error = preserve_timeout_phase(error);
        if error.context().stage.is_none() {
            error.with_stage(ErrorStage::Admission)
        } else {
            error
        }
    })?;
    let (handle, response) = admission
        .request
        .take()
        .ok_or_else(|| NetError::from(ErrorKind::Internal))?;
    {
        let mut state = lock_recover(&control.state);
        if state.terminal {
            return Err(NetError::from(ErrorKind::Cancelled));
        }
        state.current = Some(handle.clone());
        state.phase = OperationPhase::Queued;
    }
    admission
        .commit()
        .map_err(|error| preserve_timeout_phase(error))?;
    let result = scope_attempt(
        &control.runtime,
        &control.options,
        &control.cancellation,
        control.deadline,
        attempt_budget,
        Some(&handle),
        async {
            response
                .await
                .map_err(|_| NetError::from(ErrorKind::EngineDropped))?
        },
    )
    .await;
    result.map_err(|error| {
        let error = preserve_timeout_phase(error);
        if error.kind() == ErrorKind::TimedOut && error.context().stage.is_none() {
            error.with_stage(ErrorStage::Response)
        } else {
            error
        }
    })
}

fn check_attempt_budget(control: &LogicalControl, started: Instant) -> Result<()> {
    control.check()?;
    if add(started, control.options.attempt_timeout)?
        .is_some_and(|deadline| deadline <= Instant::now())
    {
        return Err(
            NetError::from(ErrorKind::TimedOut).with_ws_timeout_phase(WsTimeoutPhase::Attempt)
        );
    }
    Ok(())
}

fn preserve_timeout_phase(error: NetError) -> NetError {
    match super::retry::timeout_phase(&error) {
        Some(phase) => error.with_ws_timeout_phase(phase),
        None => error,
    }
}
enum RetryStep {
    Retry(Duration),
    Stop(RetryStopReason),
}
fn classify(control: &LogicalControl, result: &Result<Response>) -> Result<RetryStep> {
    if matches!(control.options.retry_setting, RetrySetting::Disabled) {
        return Ok(RetryStep::Stop(RetryStopReason::Disabled));
    }
    if let Err(error) = result {
        if super::retry::terminal_error(error) {
            let reason = if error.kind() == ErrorKind::CallbackPanicked {
                RetryStopReason::PolicyFailed
            } else {
                RetryStopReason::NotRetryable
            };
            return Ok(RetryStep::Stop(reason));
        }
    }
    let retry = lock_recover(&control.state).retry.context(
        control.started.elapsed(),
        control
            .deadline
            .map(|deadline| deadline.saturating_duration_since(Instant::now())),
    );
    let decision = control.policy.classify(&WsRequestRetryContext {
        outcome: result.as_ref(),
        retry: &retry,
    })?;
    let reason = if decision == RetryDecision::Stop {
        Some(RetryStopReason::ClassifierStopped)
    } else if decision == RetryDecision::Default
        && !result
            .as_ref()
            .err()
            .is_some_and(|error| control.policy.should_retry_error(error))
    {
        Some(if result.is_ok() {
            RetryStopReason::Completed
        } else {
            RetryStopReason::NotRetryable
        })
    } else if !lock_recover(&control.state).retry.can_retry() {
        Some(RetryStopReason::AttemptsExhausted)
    } else if !control.policy.allows_replay() {
        Some(RetryStopReason::UnsafeToReplay)
    } else {
        None
    };
    let mut state = lock_recover(&control.state);
    if let Some(reason) = reason {
        return Ok(RetryStep::Stop(reason));
    }
    state
        .retry
        .next_delay()
        .map(|delay| match delay {
            Some(delay) => RetryStep::Retry(delay),
            None => RetryStep::Stop(RetryStopReason::AttemptsExhausted),
        })
        .map_err(|_| NetError::from(ErrorKind::InvalidPolicy))
}
fn error_stop_reason(error: &NetError) -> RetryStopReason {
    match error.kind() {
        ErrorKind::Cancelled => RetryStopReason::Cancelled,
        ErrorKind::TimedOut => RetryStopReason::TimedOut,
        ErrorKind::Closed | ErrorKind::EngineDropped => RetryStopReason::Shutdown,
        ErrorKind::CallbackPanicked | ErrorKind::InvalidPolicy => RetryStopReason::PolicyFailed,
        _ => RetryStopReason::NotRetryable,
    }
}
fn finish_error(control: &LogicalControl, error: NetError) {
    let reason = error_stop_reason(&error);
    control.finish(Err(error), reason);
}

fn finish_after_attempt_error(
    control: &LogicalControl,
    result: Result<Response>,
    selected: bool,
    error: NetError,
) {
    if selected && control.runtime.preserves_selected_result() {
        match control.check_result(true) {
            Ok(()) => {
                // Every caller has already decided to retry. Natural retirement
                // prevents that retry even when the retained result is a response.
                control.finish(result, RetryStopReason::NotRetryable);
            }
            Err(error) => finish_error(control, error),
        }
    } else {
        finish_error(control, error);
    }
}
fn end_cause(result: &Result<Response>, reason: RetryStopReason) -> TaskEndCause {
    if result.is_ok() {
        return TaskEndCause::Completed;
    }
    match reason {
        RetryStopReason::Cancelled => TaskEndCause::Cancelled,
        RetryStopReason::TimedOut => TaskEndCause::Expired,
        RetryStopReason::Shutdown => TaskEndCause::Shutdown,
        _ => match result.as_ref().err().map(NetError::kind) {
            Some(ErrorKind::TimedOut) => TaskEndCause::Expired,
            Some(ErrorKind::Cancelled) => TaskEndCause::Cancelled,
            Some(ErrorKind::Closed | ErrorKind::NotConnected) => TaskEndCause::Disconnected,
            _ => TaskEndCause::Failed,
        },
    }
}
fn earlier(left: Option<Instant>, right: Option<Instant>) -> Option<Instant> {
    match (left, right) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}
fn add(start: Instant, duration: Option<Duration>) -> Result<Option<Instant>> {
    duration
        .map(|duration| {
            start
                .checked_add(duration)
                .ok_or_else(|| NetError::config("timeout", "deadline is not representable"))
        })
        .transpose()
}
fn check_scope(
    runtime: &SessionRuntime,
    options: &WsRequestOperationOptions,
    cancel: &CancellationToken,
    deadline: Option<Instant>,
) -> Result<()> {
    check_result_scope(runtime, options, cancel, deadline, || false)
}

fn check_result_scope(
    runtime: &SessionRuntime,
    options: &WsRequestOperationOptions,
    cancel: &CancellationToken,
    deadline: Option<Instant>,
    selected: impl FnOnce() -> bool,
) -> Result<()> {
    if cancel.is_cancelled()
        || options
            .cancellation
            .as_ref()
            .is_some_and(CancellationGroup::is_cancelled)
    {
        return Err(NetError::from(ErrorKind::Cancelled));
    }
    // Read the request authority only after observing closure. A false snapshot
    // taken earlier could miss a result selected just before natural retirement.
    if runtime.is_closed() && !(selected() && runtime.preserves_selected_result()) {
        return Err(NetError::from(ErrorKind::Closed));
    }
    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
        return Err(NetError::from(ErrorKind::TimedOut));
    }
    Ok(())
}
/// Keep unfinished admission and backoff bound to the original Reject connection.
async fn admission_scope<T>(
    lease: &CancellationToken,
    runtime: &SessionRuntime,
    options: &WsRequestOperationOptions,
    cancel: &CancellationToken,
    deadline: Option<Instant>,
    future: impl Future<Output = Result<T>>,
) -> Result<T> {
    if options.disconnected == DisconnectedPolicy::WaitForReconnect {
        return scope(runtime, options, cancel, deadline, future).await;
    }
    check_scope(runtime, options, cancel, deadline)?;
    if lease.is_cancelled() {
        return Err(NetError::from(ErrorKind::NotConnected));
    }
    let result = tokio::select! {
        biased;
        _ = lease.cancelled() => {
            check_scope(runtime, options, cancel, deadline)?;
            Err(NetError::from(ErrorKind::NotConnected))
        },
        result = scope(runtime, options, cancel, deadline, future) => result,
    }?;
    check_scope(runtime, options, cancel, deadline)?;
    if lease.is_cancelled() {
        return Err(NetError::from(ErrorKind::NotConnected));
    }
    Ok(result)
}

async fn scope<T>(
    runtime: &SessionRuntime,
    options: &WsRequestOperationOptions,
    cancel: &CancellationToken,
    deadline: Option<Instant>,
    future: impl Future<Output = Result<T>>,
) -> Result<T> {
    scope_with_request(runtime, options, cancel, deadline, None, future).await
}

/// A natural session end cannot replace a pending table winner. Its producer
/// may still be releasing resources before publishing to the response channel.
async fn result_closure(runtime: &SessionRuntime, request: Option<&RequestHandle>) {
    let closed = runtime.closed_token();
    let session_end = runtime.lifecycle.completion_token();
    tokio::select! {
        _ = closed.cancelled() => {},
        _ = session_end.cancelled() => {},
    }
    if request.is_some_and(RequestHandle::has_selected_result)
        && runtime.preserves_selected_result()
    {
        // Explicit retirement has its own signal in the surrounding scope.
        std::future::pending::<()>().await;
    }
}

async fn scope_with_request<T>(
    runtime: &SessionRuntime,
    options: &WsRequestOperationOptions,
    cancel: &CancellationToken,
    deadline: Option<Instant>,
    request: Option<&RequestHandle>,
    future: impl Future<Output = Result<T>>,
) -> Result<T> {
    check_result_scope(
        runtime,
        options,
        cancel,
        deadline,
        || request.is_some_and(RequestHandle::has_selected_result),
    )?;
    let forced_closed = runtime.forced_closed_token();
    let session_cancel = runtime.lifecycle.cancel_token();
    let group = async {
        match &options.cancellation {
            Some(group) => group.cancelled().await,
            None => std::future::pending().await,
        }
    };
    tokio::select! {
        biased;
        _ = cancel.cancelled() => Err(NetError::from(ErrorKind::Cancelled)),
        _ = group => Err(NetError::from(ErrorKind::Cancelled)),
        _ = runtime.shutdown.cancelled() => Err(NetError::from(ErrorKind::Closed)),
        _ = forced_closed.cancelled() => Err(NetError::from(ErrorKind::Closed)),
        _ = session_cancel.cancelled() => Err(NetError::from(ErrorKind::Closed)),
        _ = result_closure(runtime, request) => Err(NetError::from(ErrorKind::Closed)),
        result = runtime.wait_deadline(deadline) => result.and_then(|()| Err(NetError::from(ErrorKind::TimedOut))),
        result = future => result,
    }
}

/// An outcome already selected by the pending table or writer wins over a late
/// poll of the per-attempt timer. Cancellation and the logical total budget still
/// arbitrate before and after consuming that outcome.
async fn scope_attempt<T>(
    runtime: &SessionRuntime,
    options: &WsRequestOperationOptions,
    cancel: &CancellationToken,
    total_deadline: Option<Instant>,
    budget: Option<TimeoutBudget>,
    request: Option<&RequestHandle>,
    future: impl Future<Output = Result<T>>,
) -> Result<T> {
    let check = || {
        let selected = || request.is_some_and(RequestHandle::has_selected_result);
        #[cfg(test)]
        if let Some(request) = request {
            runtime.before_request_result_check_for_test(request)?;
        }
        check_result_scope(
            runtime,
            options,
            cancel,
            total_deadline,
            selected,
        ).map_err(|error| {
            if error.kind() == ErrorKind::TimedOut {
                error.with_ws_timeout_phase(WsTimeoutPhase::Total)
            } else {
                error
            }
        })
    };
    check()?;
    let forced_closed = runtime.forced_closed_token();
    let session_cancel = runtime.lifecycle.cancel_token();
    let group = async {
        match &options.cancellation {
            Some(group) => group.cancelled().await,
            None => std::future::pending().await,
        }
    };
    tokio::pin!(future);
    let result = tokio::select! {
        biased;
        _ = cancel.cancelled() => Err(NetError::from(ErrorKind::Cancelled)),
        _ = group => Err(NetError::from(ErrorKind::Cancelled)),
        _ = runtime.shutdown.cancelled() => Err(NetError::from(ErrorKind::Closed)),
        _ = forced_closed.cancelled() => Err(NetError::from(ErrorKind::Closed)),
        _ = session_cancel.cancelled() => Err(NetError::from(ErrorKind::Closed)),
        _ = result_closure(runtime, request) => Err(NetError::from(ErrorKind::Closed)),
        result = runtime.wait_deadline(total_deadline) => result.and_then(|()|
            Err(NetError::from(ErrorKind::TimedOut).with_ws_timeout_phase(WsTimeoutPhase::Total))),
        result = &mut future => result,
        result = runtime.wait_deadline(budget.map(|budget| budget.at)) => {
            result?;
            if let Some(request) = request {
                // The channel may still be pending after its outcome was selected.
                // Expiration competes with that selection under the same P/C
                // authority; a previously chosen result cannot be replaced here.
                request.expire_operation_attempt();
                scope_with_request(runtime, options, cancel, total_deadline, Some(request), &mut future).await
            } else {
                Err(budget.map_or_else(|| NetError::from(ErrorKind::TimedOut), TimeoutBudget::timeout_error))
            }
        },
    };
    check()?;
    result
}

#[cfg(test)]
#[path = "request_operation_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "request_operation_generation_tests.rs"]
mod generation_tests;

#[cfg(test)]
#[path = "request_operation_selection_tests.rs"]
mod selection_tests;

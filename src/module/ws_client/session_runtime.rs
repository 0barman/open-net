use super::connection_session::ConnectionSession;
use super::message_source::MessageSource;
use super::native_pending::NativePending;
use super::native_task_observer::NativeTaskObserver;
use super::operation_control::OperationControl;
use super::write::priority_write_queue::PriorityWriteQueue;
use crate::error::ErrorKind;
use crate::ws::{
    ClientId, ConnectionId, ConnectionState, DisconnectedPolicy, OperationId, ResponseRouting,
    SessionId, TaskEndCause, WebSocketClientConfig,
};
use crate::{NetError, Result};
use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Instant;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

#[cfg(test)]
type RequestResultCheckHook = Box<dyn FnOnce(&crate::ws::RequestHandle) -> Result<()> + Send>;

/// All authorities captured before the worker is allowed to begin networking.
/// Shared senders retain this identity; receipts retain only OperationControl.
pub(crate) struct SessionRuntime {
    pub(crate) config: Arc<WebSocketClientConfig>,
    pub(crate) client_id: ClientId,
    pub(crate) id: SessionId,
    pub(crate) lifecycle: Arc<ConnectionSession>,
    pub(crate) pending: Arc<NativePending>,
    pub(crate) messages: Arc<MessageSource>,
    pub(crate) tasks: Arc<NativeTaskObserver>,
    pub(crate) routing: ResponseRouting,
    pub(crate) queue: Arc<PriorityWriteQueue>,
    pub(crate) urgent_queue: Arc<PriorityWriteQueue>,
    pub(crate) shutdown: CancellationToken,
    worker_runtime: OnceLock<tokio::runtime::Handle>,
    closed: CancellationToken,
    /// Unlike a natural disconnect, explicit retirement also stops publication
    /// of results already selected by a request. This signal is never reset.
    forced_closed: CancellationToken,
    connection: Mutex<Option<(ConnectionId, CancellationToken)>>,
    next_operation: AtomicU64,
    operations: Mutex<HashMap<OperationId, Weak<OperationControl>>>,
    changed: Arc<Notify>,
    request_jobs: TaskTracker,
    request_gate: Mutex<bool>,
    #[cfg(test)]
    before_request_result_check: Mutex<Option<RequestResultCheckHook>>,
}
impl SessionRuntime {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        client_id: ClientId,
        id: SessionId,
        lifecycle: Arc<ConnectionSession>,
        pending: Arc<NativePending>,
        messages: Arc<MessageSource>,
        tasks: Arc<NativeTaskObserver>,
        routing: ResponseRouting,
        queue: Arc<PriorityWriteQueue>,
        urgent_queue: Arc<PriorityWriteQueue>,
        shutdown: CancellationToken,
        config: Arc<WebSocketClientConfig>,
    ) -> Arc<Self> {
        Arc::new(Self {
            config,
            client_id,
            id,
            lifecycle,
            pending,
            messages,
            tasks,
            routing,
            queue,
            urgent_queue,
            shutdown,
            worker_runtime: OnceLock::new(),
            closed: CancellationToken::new(),
            forced_closed: CancellationToken::new(),
            connection: Mutex::new(None),
            next_operation: AtomicU64::new(0),
            operations: Mutex::new(HashMap::new()),
            changed: Arc::new(Notify::new()),
            request_jobs: TaskTracker::new(),
            request_gate: Mutex::new(false),
            #[cfg(test)]
            before_request_result_check: Mutex::new(None),
        })
    }
    #[cfg(test)]
    pub(crate) fn set_before_request_result_check_for_test(
        &self,
        hook: impl FnOnce(&crate::ws::RequestHandle) -> Result<()> + Send + 'static,
    ) -> Result<()> {
        *self
            .before_request_result_check
            .lock()
            .map_err(NetError::from_poison)? = Some(Box::new(hook));
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn before_request_result_check_for_test(
        &self,
        request: &crate::ws::RequestHandle,
    ) -> Result<()> {
        let hook = self
            .before_request_result_check
            .lock()
            .map_err(NetError::from_poison)?
            .take();
        if let Some(hook) = hook {
            hook(request)?;
        }
        Ok(())
    }
    pub(crate) fn is_closed(&self) -> bool {
        self.closed.is_cancelled()
            || self.forced_closed.is_cancelled()
            || self.shutdown.is_cancelled()
            || self.lifecycle.cancel_token().is_cancelled()
            || self.lifecycle.completion_token().is_cancelled()
    }
    pub(crate) fn closed_token(&self) -> CancellationToken {
        self.closed.clone()
    }
    pub(crate) fn forced_closed_token(&self) -> CancellationToken {
        self.forced_closed.clone()
    }
    pub(crate) fn preserves_selected_result(&self) -> bool {
        self.closed.is_cancelled()
            && !self.forced_closed.is_cancelled()
            && !self.shutdown.is_cancelled()
            && !self.lifecycle.cancel_token().is_cancelled()
    }
    /// Spawns a future on the already-running worker runtime.
    ///
    /// Public API tasks use this helper so a caller cannot accidentally create
    /// an unrelated Tokio runtime for session-owned work.
    pub(crate) fn spawn<F>(&self, future: F) -> Result<tokio::task::JoinHandle<F::Output>>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let runtime = self.worker_runtime.get().ok_or_else(|| {
            NetError::from(ErrorKind::RuntimeUnavailable)
                .with_stage(crate::error::ErrorStage::Runtime)
        })?;
        Ok(runtime.spawn(future))
    }
    /// Admit an owned request task under the same gate used by session retirement.
    pub(crate) fn spawn_request<F>(&self, future: F) -> Result<()>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let retired = self.request_gate.lock().map_err(NetError::from_poison)?;
        if *retired || self.is_closed() {
            return Err(NetError::from(ErrorKind::Closed));
        }
        let runtime = self.worker_runtime.get().ok_or_else(|| {
            NetError::from(ErrorKind::RuntimeUnavailable)
                .with_stage(crate::error::ErrorStage::Runtime)
        })?;
        self.request_jobs.spawn_on(future, runtime);
        drop(retired);
        Ok(())
    }
    /// Called only after end() closes task admission and signals all waits.
    pub(crate) async fn wait_requests(&self) {
        self.request_jobs.wait().await;
    }
    /// Bind the already-running, timer-enabled client worker before exposing
    /// the session. Caller executors and callback threads need no Tokio context.
    pub(crate) fn bind_worker_runtime(&self) -> Result<()> {
        let runtime = tokio::runtime::Handle::try_current().map_err(|error| {
            NetError::with_source(ErrorKind::RuntimeUnavailable, error)
                .with_stage(crate::error::ErrorStage::Runtime)
        })?;
        self.worker_runtime.set(runtime).map_err(|_| {
            NetError::from(ErrorKind::Internal).with_stage(crate::error::ErrorStage::Runtime)
        })
    }
    /// Sleep only on the existing worker runtime. Dropping an admission wait
    /// aborts its timer, and worker destruction becomes an ordinary error.
    pub(crate) async fn wait_deadline(&self, deadline: Option<Instant>) -> Result<()> {
        let Some(deadline) = deadline else {
            return std::future::pending().await;
        };
        let runtime = self.worker_runtime.get().ok_or_else(|| {
            NetError::from(ErrorKind::RuntimeUnavailable)
                .with_stage(crate::error::ErrorStage::Runtime)
        })?;
        let mut timer = DeadlineTask(runtime.spawn(async move {
            tokio::time::sleep_until(deadline.into()).await;
        }));
        (&mut timer.0).await.map_err(|error| {
            NetError::with_source(ErrorKind::EngineDropped, error)
                .with_stage(crate::error::ErrorStage::Runtime)
        })
    }
    pub(crate) fn allocate_operation(&self) -> Result<OperationId> {
        OperationId::allocate(&self.next_operation)
    }
    pub(crate) fn lease(&self, policy: DisconnectedPolicy) -> Result<CancellationToken> {
        if self.is_closed() {
            return Err(NetError::from(ErrorKind::Closed));
        }
        if policy == DisconnectedPolicy::WaitForReconnect {
            return Ok(self.closed.clone());
        }
        let connection = self
            .connection
            .lock()
            .map_err(NetError::from_poison)?
            .clone();
        match (connection, self.lifecycle.snapshot()?.state) {
            (Some((id, token)), ConnectionState::Connected(info))
                if id == info.connection_id && !token.is_cancelled() =>
            {
                Ok(token)
            }
            _ => Err(NetError::from(ErrorKind::NotConnected)),
        }
    }
    pub(crate) fn current_connection_id(&self) -> Result<Option<ConnectionId>> {
        let connection = self
            .connection
            .lock()
            .map_err(NetError::from_poison)
            .map(|connection| connection.clone())?;
        match (connection, self.lifecycle.snapshot()?.state) {
            (Some((id, token)), ConnectionState::Connected(info))
                if id == info.connection_id && !token.is_cancelled() =>
            {
                Ok(Some(id))
            }
            _ => Ok(None),
        }
    }
    pub(crate) fn activate_connection(&self, id: ConnectionId) -> Result<()> {
        self.pending.activate_connection(id)?;
        let previous = self
            .connection
            .lock()
            .map_err(NetError::from_poison)?
            .replace((id, CancellationToken::new()));
        if let Some((_, token)) = previous {
            token.cancel();
        }
        Ok(())
    }
    pub(crate) fn connection_ended(&self, id: ConnectionId, error: NetError) -> Result<()> {
        let previous = {
            let mut connection = self.connection.lock().map_err(NetError::from_poison)?;
            if connection
                .as_ref()
                .is_some_and(|(current, _)| *current == id)
            {
                connection.take()
            } else {
                None
            }
        };
        if let Some((_, token)) = previous {
            token.cancel();
        }
        self.pending.fail_connection(id, error)
    }
    pub(crate) fn register_operation(&self, control: &Arc<OperationControl>) -> Result<()> {
        if self.is_closed() {
            return Err(NetError::from(ErrorKind::Closed));
        }
        let mut operations = self.operations.lock().map_err(NetError::from_poison)?;
        operations
            .try_reserve(1)
            .map_err(|_| NetError::from(ErrorKind::ResourceExhausted))?;
        operations.insert(control.id(), Arc::downgrade(control));
        drop(operations);
        self.changed.notify_one();
        // Final validation closes a race with end() draining the previous snapshot.
        if self.is_closed() {
            control.terminate(
                NetError::from(ErrorKind::Closed),
                TaskEndCause::Disconnected,
            )?;
            return Err(NetError::from(ErrorKind::Closed));
        }
        Ok(())
    }
    pub(crate) fn end(&self, error: NetError, cause: TaskEndCause) {
        // Publish the stronger boundary before closed wakes any request. A
        // later natural-disconnect notification cannot undo explicit shutdown.
        if cause != TaskEndCause::Disconnected {
            self.forced_closed.cancel();
        }
        let mut retired = match self.request_gate.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        *retired = true;
        self.request_jobs.close();
        drop(retired);
        let mut preserve_responses = cause == TaskEndCause::Disconnected
            && !self.forced_closed.is_cancelled()
            && !self.shutdown.is_cancelled()
            && !self.lifecycle.cancel_token().is_cancelled();
        self.closed.cancel();
        let connection = match self.connection.lock() {
            Ok(mut current) => current.take(),
            Err(poisoned) => {
                crate::log_e!(crate::common::log::log_def::LogType::WSC; "ws.session_runtime.end", "arg_error_kind|arg_error_stage|cause|error", format!("{:?}", error.kind()), error.context().stage.map(|stage| format!("{stage:?}")), format!("{cause:?}"), "connection_lock_poisoned_recovered");
                poisoned.into_inner().take()
            }
        };
        if let Some((_, token)) = connection {
            token.cancel();
        }
        let references = match self.operations.lock() {
            Ok(mut map) => std::mem::take(&mut *map),
            Err(poisoned) => {
                crate::log_e!(crate::common::log::log_def::LogType::WSC; "ws.session_runtime.end", "arg_error_kind|arg_error_stage|cause|error", format!("{:?}", error.kind()), error.context().stage.map(|stage| format!("{stage:?}")), format!("{cause:?}"), "operations_lock_poisoned_recovered");
                std::mem::take(&mut *poisoned.into_inner())
            }
        };
        let pending_result = if preserve_responses {
            self.pending.close_disconnected(error.clone())
        } else {
            self.pending.close(error.clone(), cause)
        };
        if let Err(failure) = pending_result {
            crate::log_e!(crate::common::log::log_def::LogType::WSC; "ws.session_runtime.end", "arg_error_kind|arg_error_stage|cause|event|kind|stage", format!("{:?}", error.kind()), error.context().stage.map(|stage| format!("{stage:?}")), format!("{cause:?}"), "pending_close_failed", format!("{:?}", failure.kind()), failure.context().stage.map(|stage| format!("{stage:?}")));
            // A failed grace snapshot must not leave registrations admitted to
            // a terminal session. Ordinary close retires the table without a
            // fallible allocation and the remaining controls are also selected.
            preserve_responses = false;
            if let Err(failure) = self.pending.close(error.clone(), cause) {
                crate::log_e!(crate::common::log::log_def::LogType::WSC; "ws.session_runtime.end", "arg_error_kind|arg_error_stage|cause|event|kind|stage", format!("{:?}", error.kind()), error.context().stage.map(|stage| format!("{stage:?}")), format!("{cause:?}"), "pending_force_close_failed", format!("{:?}", failure.kind()), failure.context().stage.map(|stage| format!("{stage:?}")));
            }
        }
        for reference in references.into_values() {
            if let Some(control) = reference.upgrade() {
                if preserve_responses && control.has_request_registration() {
                    continue;
                }
                if let Err(failure) = control.terminate(error.clone(), cause) {
                    crate::log_e!(crate::common::log::log_def::LogType::WSC; "ws.session_runtime.end", "arg_error_kind|arg_error_stage|cause|event|kind|stage", format!("{:?}", error.kind()), error.context().stage.map(|stage| format!("{stage:?}")), format!("{cause:?}"), "operation_close_failed", format!("{:?}", failure.kind()), failure.context().stage.map(|stage| format!("{stage:?}")));
                }
            }
        }
        self.tasks.close();
        self.messages.finish();
        self.changed.notify_waiters();
    }
    /// One maintenance future on the existing worker runtime; never retains a
    /// session or completed control while sleeping, and never starts a runtime.
    pub(crate) async fn run_timers(weak: Weak<Self>) {
        loop {
            let Some(runtime) = weak.upgrade() else {
                return;
            };
            let changed = runtime.changed.clone();
            let closed = runtime.closed.clone();
            let changed_wait = changed.notified();
            tokio::pin!(changed_wait);
            changed_wait.as_mut().enable();
            if runtime.is_closed() {
                return;
            }
            let references = match runtime.operations.lock() {
                Ok(map) => map
                    .iter()
                    .map(|(id, control)| (*id, control.clone()))
                    .collect::<Vec<_>>(),
                Err(_) => {
                    crate::log_e!(crate::common::log::log_def::LogType::WSC;"ws.session_runtime.run_timers", "arg_weak_present|error", true, "lock_poisoned");
                    return;
                }
            };
            let mut retired = Vec::new();
            let mut deadline: Option<Instant> = None;
            for (id, reference) in references {
                let Some(control) = reference.upgrade() else {
                    retired.push(id);
                    continue;
                };
                if control.is_finished() {
                    retired.push(id);
                    continue;
                }
                if let Some(at) = control.absolute_deadline() {
                    if Instant::now() >= at {
                        if let Err(error) = control.expire() {
                            crate::log_e!(crate::common::log::log_def::LogType::WSC;"ws.session_runtime.run_timers", "arg_weak_present|kind|stage", true, format!("{:?}", error.kind()), error.context().stage.map(|stage| format!("{stage:?}")));
                        }
                    } else {
                        deadline = Some(deadline.map_or(at, |current| current.min(at)));
                    }
                }
            }
            if let Ok(mut map) = runtime.operations.lock() {
                for id in retired {
                    map.remove(&id);
                }
            }
            drop(runtime);
            tokio::select! {
                _=closed.cancelled()=>return,
                _=&mut changed_wait=>{},
                _=async {match deadline{Some(at)=>tokio::time::sleep_until(at.into()).await,None=>std::future::pending().await}}=>{},
            }
        }
    }
}

struct DeadlineTask(tokio::task::JoinHandle<()>);

impl Drop for DeadlineTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

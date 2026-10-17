#![cfg(feature = "http-client")]

use crate::api::error::{EnqueueError, ErrorKind, ErrorStage, NetError};
use crate::api::http::HttpClient;
#[path = "admission.rs"]
mod admission;
#[path = "request_completion.rs"]
mod request_completion;
#[path = "worker_shutdown.rs"]
mod worker_shutdown;
use crate::module::http::http_failure::{
    map_body_error, map_reqwest_error, map_response_error, retry_reason_for_error,
};
use crate::module::http::network_observation::HttpNetworkObservation;
use crate::module::http::request_wait_guard::RequestWaitGuard;
use crate::module::http::result_request::ResultRequest;
use crate::module::http::retry_after::parse_retry_after;
use crate::net_status::{NetworkSnapshot, NetworkStatusContext};
use crate::subscription::StateReceiver;
use admission::Admission;
pub(crate) use request_completion::CompletionLease;
use worker_shutdown::{WorkerJoin, WorkerShutdown};
#[path = "http_stream_body.rs"]
mod stream_body;
#[cfg(feature = "http-client-auth")]
use crate::api::http::auth::{AuthAction, AuthRequestContext, AuthResponseContext};
#[cfg(feature = "http-client-cookies")]
use crate::api::http::cookies::CookieRequestContext;
use crate::api::http::http_timeouts::ResolvedTimeouts;
use crate::api::http::TimeoutPhase;
#[cfg(feature = "http-client-middleware")]
use crate::api::http::{HttpCall, HttpMiddleware, HttpMiddlewareResponse, HttpResponseMode, Next};
use crate::api::http::{
    HttpCancelOutcome, HttpClientConfig, HttpCompletion, HttpRequest, HttpRequestHandle,
    HttpRequestId, HttpRequestOptions, HttpRequestTrait, HttpResponse, HttpResponseHead,
    HttpResponseResult, HttpResponseSummary, HttpStreamResponse, RetryEvent, RetryObserverHandle,
    RetryReason, RetryReport,
};
#[cfg(feature = "http-client-http2")]
use crate::api::http::{HttpResponseInfo, HttpVersionPolicy};
use bytes::BytesMut;
use futures_util::{FutureExt, StreamExt};
use http::Method;
use reqwest::{Client, Response};
use std::collections::HashMap;
#[cfg(test)]
use std::error::Error as StdError;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use stream_body::{FusedHttpBody, StreamBudget, StreamOwner, StreamSupervisor};
use tokio::sync::oneshot;
use tokio::sync::{mpsc, Notify, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio::time::Instant;
#[cfg(feature = "http-client-cookies")]
#[path = "cookie_hooks.rs"]
mod cookie_hooks;
#[cfg(feature = "http-client-cookies")]
use cookie_hooks::{apply_cookie_header, update_response_cookies};
#[cfg(any(feature = "http-client-auth", feature = "http-client-cookies"))]
#[path = "response_hops.rs"]
mod response_hops;
#[cfg(any(feature = "http-client-auth", feature = "http-client-cookies"))]
use response_hops::{follow_response_hops, uses_worker_redirects, ResponseHops};

#[cfg(feature = "http-client-middleware")]
#[path = "middleware_lifecycle.rs"]
mod middleware_lifecycle;
#[cfg(feature = "http-client-middleware")]
use middleware_lifecycle::{MiddlewareCompletion, MiddlewareLease, TerminalObservation};

#[cfg(test)]
#[path = "admission_tests.rs"]
mod admission_tests;

#[cfg(test)]
#[path = "shutdown_admission_tests.rs"]
mod shutdown_admission_tests;

#[cfg(test)]
#[path = "cookie_worker_tests.rs"]
mod cookie_worker_tests;

pub(crate) fn create_http_client(
    thread_name: String,
    config: HttpClientConfig,
    context: NetworkStatusContext,
) -> Result<HttpClient, NetError> {
    let threads = context.common_engine()?.resource_threads();
    Ok(HttpClient {
        inner: HttpClientInner::new_with_thread_registry(
            thread_name,
            config,
            Some(context),
            Some(threads),
        )?,
    })
}

pub(crate) struct SendJob {
    pub(crate) request: Box<dyn HttpRequestTrait>,
    pub(crate) options: HttpRequestOptions,
    pub(crate) request_id: HttpRequestId,
    pub(crate) control: Arc<CancellationSignal>,
    pub(crate) registry: Arc<RequestRegistry>,
    pub(crate) permit: Arc<OwnedSemaphorePermit>,
    pub(crate) operation_start: Instant,
    pub(crate) completion: Option<CompletionLease>,
}

type ReceiveJob = CallbackJob;

struct CallbackJob {
    request: Box<dyn HttpRequestTrait>,
    result: HttpResponseResult,
    request_id: HttpRequestId,
    control: Arc<CancellationSignal>,
    registry: Arc<RequestRegistry>,
    _permit: Arc<OwnedSemaphorePermit>,
    completion: Option<CompletionLease>,
}

enum SendCommand {
    Job(SendJob),
}

pub(crate) struct HttpClientInner {
    request_tx: mpsc::Sender<SendCommand>,
    supervisor_tx: mpsc::Sender<StreamSupervisor>,
    shutdown_sent: Arc<AtomicBool>,
    admission_gate: Mutex<()>,
    shutdown_notify: Arc<Notify>,
    shutdown_state: Arc<WorkerShutdown>,
    client: Client,
    config: Arc<HttpClientConfig>,
    registry: Arc<RequestRegistry>,
    in_flight: Arc<Semaphore>,
    network_observation: Option<Arc<HttpNetworkObservation>>,
}

struct RetryObserverLifecycle {
    observer: Option<RetryObserverHandle>,
    operation_start: Instant,
    attempts: u32,
    retry_count: u32,
    current_attempt: Option<u32>,
    terminal_reason: Option<RetryReason>,
    last_retry_reason: Option<RetryReason>,
    completed: bool,
    #[cfg(feature = "http-client-middleware")]
    deferred: Option<Arc<MiddlewareCompletion>>,
}

impl RetryObserverLifecycle {
    fn new(observer: Option<RetryObserverHandle>, operation_start: Instant) -> Self {
        Self {
            observer,
            operation_start,
            attempts: 0,
            retry_count: 0,
            current_attempt: None,
            terminal_reason: None,
            last_retry_reason: None,
            completed: false,
            #[cfg(feature = "http-client-middleware")]
            deferred: None,
        }
    }

    fn attempt_started(&mut self, attempt: u32) {
        self.attempts = self.attempts.max(attempt);
        self.current_attempt = Some(attempt);
        if let Some(observer) = &self.observer {
            observer.notify(RetryEvent::AttemptStarted { attempt });
        }
    }

    fn note_reason(&mut self, reason: RetryReason) {
        self.terminal_reason = Some(reason);
    }

    fn retry_scheduled(&mut self, attempt: u32, delay: Duration, reason: RetryReason) {
        self.retry_count = self.retry_count.saturating_add(1);
        self.last_retry_reason = Some(reason);
        if let Some(observer) = &self.observer {
            observer.notify(RetryEvent::RetryScheduled { attempt, delay });
            observer.notify(RetryEvent::AttemptFinished {
                attempt,
                final_attempt: false,
            });
        }
        self.current_attempt = None;
    }

    fn complete(&mut self, result: &HttpResponseResult, exhausted: bool) {
        self.complete_summary(result.as_ref().map(|response| response.status), exhausted);
    }

    fn complete_summary(&mut self, result: Result<http::StatusCode, &NetError>, exhausted: bool) {
        if self.completed {
            return;
        }
        self.completed = true;
        self.emit_terminal(Some(result), exhausted);
    }

    fn emit_terminal(
        &mut self,
        result: Option<Result<http::StatusCode, &NetError>>,
        exhausted: bool,
    ) {
        let Some(observer) = &self.observer else {
            return;
        };
        let attempt = self.current_attempt.take();
        let final_reason = match result {
            Some(Ok(status)) if status.is_success() => None,
            Some(Ok(status)) => Some(RetryReason::HttpStatus(status)),
            Some(Err(error)) => self
                .terminal_reason
                .or_else(|| Some(retry_reason_for_error(error))),
            None => self.terminal_reason.or(self.last_retry_reason),
        };
        let report = RetryReport {
            attempts: self.attempts,
            retry_count: self.retry_count,
            elapsed: self.operation_start.elapsed(),
            final_reason,
            exhausted,
            last_error: result
                .and_then(Result::err)
                .map(|error| format!("{:?}", error.kind())),
        };
        #[cfg(feature = "http-client-middleware")]
        if let Some(deferred) = &self.deferred {
            deferred.queue(TerminalObservation {
                observer: observer.clone(),
                attempt,
                report,
            });
            return;
        }
        if let Some(attempt) = attempt {
            observer.notify(RetryEvent::AttemptFinished {
                attempt,
                final_attempt: true,
            });
        }
        observer.notify(RetryEvent::Completed(report));
    }
}

impl Drop for RetryObserverLifecycle {
    fn drop(&mut self) {
        if !self.completed {
            self.completed = true;
            self.emit_terminal(None, false);
        }
    }
}

pub(crate) struct RegistrationGuard {
    registry: Arc<RequestRegistry>,
    request_id: HttpRequestId,
    control: Arc<CancellationSignal>,
    committed: AtomicBool,
}

impl RegistrationGuard {
    pub(crate) fn commit(&self) {
        self.committed.store(true, Ordering::Release);
    }
}

impl Drop for RegistrationGuard {
    fn drop(&mut self) {
        if !self.committed.load(Ordering::Acquire) {
            self.registry.finish(self.request_id, &self.control);
        }
    }
}

pub(crate) struct CancellationSignal {
    phase: std::sync::atomic::AtomicU8,
    completion: std::sync::OnceLock<HttpCompletion>,
    completion_notify: Notify,
    attempt: std::sync::atomic::AtomicU32,
    streaming: AtomicBool,
    #[cfg(feature = "http-client-middleware")]
    middleware_active: AtomicBool,
    #[cfg(feature = "http-client-middleware")]
    transport_finished: AtomicBool,
    #[cfg(feature = "http-client-middleware")]
    middleware_completion: std::sync::OnceLock<Arc<MiddlewareCompletion>>,
    notify: Notify,
}

impl CancellationSignal {
    fn new() -> Self {
        Self {
            phase: std::sync::atomic::AtomicU8::new(0),
            completion: std::sync::OnceLock::new(),
            completion_notify: Notify::new(),
            attempt: std::sync::atomic::AtomicU32::new(0),
            streaming: AtomicBool::new(false),
            #[cfg(feature = "http-client-middleware")]
            middleware_active: AtomicBool::new(false),
            #[cfg(feature = "http-client-middleware")]
            transport_finished: AtomicBool::new(false),
            #[cfg(feature = "http-client-middleware")]
            middleware_completion: std::sync::OnceLock::new(),
            notify: Notify::new(),
        }
    }

    pub(crate) fn cancel(&self) {
        let _ = self.request_cancel();
    }

    pub(crate) fn request_cancel(&self) -> HttpCancelOutcome {
        const CANCELLED: u8 = 1;
        const CALLBACK_OR_COMPLETE: u8 = 2;
        let mut current = self.phase.load(Ordering::Acquire);
        loop {
            if current & CALLBACK_OR_COMPLETE != 0 {
                return HttpCancelOutcome::TooLate;
            }
            if current & CANCELLED != 0 {
                return HttpCancelOutcome::AlreadyRequested;
            }
            match self.phase.compare_exchange_weak(
                current,
                current | CANCELLED,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.notify.notify_waiters();
                    return HttpCancelOutcome::Requested;
                }
                Err(observed) => current = observed,
            }
        }
    }

    fn enter_callback(&self) -> bool {
        self.phase.fetch_or(2, Ordering::AcqRel) & 1 != 0
    }

    fn publish_completion(&self, completion: HttpCompletion) {
        self.phase.fetch_or(2, Ordering::AcqRel);
        if self.completion.set(completion).is_ok() {
            self.completion_notify.notify_waiters();
        }
    }

    pub(crate) async fn finished(&self) -> HttpCompletion {
        loop {
            // Register before checking the cached state to avoid lost wakeups.
            let notified = self.completion_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(completion) = self.completion.get() {
                return completion.clone();
            }
            notified.await;
        }
    }

    fn is_cancelled(&self) -> bool {
        self.phase.load(Ordering::Acquire) & 1 != 0
    }

    async fn wait(&self) {
        if self.is_cancelled() {
            return;
        }
        let notified = self.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if self.is_cancelled() {
            return;
        }
        notified.await;
    }
}

struct StreamRegistrationLease {
    registry: Arc<RequestRegistry>,
    request_id: HttpRequestId,
    control: Arc<CancellationSignal>,
    permit: Option<Arc<OwnedSemaphorePermit>>,
}

impl Drop for StreamRegistrationLease {
    fn drop(&mut self) {
        drop(self.permit.take());
        self.registry.finish(self.request_id, &self.control);
    }
}

pub(crate) struct RegisteredRequest {
    pub(crate) id: HttpRequestId,
    pub(crate) control: Arc<CancellationSignal>,
}

pub(crate) struct RequestAdmission {
    pub(crate) registered: RegisteredRequest,
    pub(crate) permit: Arc<OwnedSemaphorePermit>,
    pub(crate) registration: RegistrationGuard,
    pub(crate) operation_start: Instant,
}

impl RequestAdmission {
    pub(crate) fn id(&self) -> HttpRequestId {
        self.registered.id
    }

    pub(crate) fn into_job<R>(
        self,
        request: R,
        mut options: HttpRequestOptions,
    ) -> (RegisteredRequest, SendJob, RegistrationGuard)
    where
        R: HttpRequestTrait,
    {
        let Self {
            registered,
            registration,
            permit,
            operation_start,
        } = self;
        options.request_id = Some(registered.id);
        let control = Arc::clone(&registered.control);
        let returned = RegisteredRequest {
            id: registered.id,
            control: Arc::clone(&registered.control),
        };
        let job = SendJob {
            request: Box::new(request),
            options,
            request_id: registered.id,
            control,
            registry: Arc::clone(&registration.registry),
            permit,
            operation_start,
            completion: None,
        };
        (returned, job, registration)
    }
}

struct RegistryState {
    entries: HashMap<HttpRequestId, Arc<CancellationSignal>>,
    reserved: HashMap<HttpRequestId, Arc<CancellationSignal>>,
    next_id: Option<u64>,
}

pub(crate) struct RequestRegistry {
    state: Mutex<RegistryState>,
    changed: Notify,
}

impl RequestRegistry {
    fn new() -> Self {
        Self {
            state: Mutex::new(RegistryState {
                entries: HashMap::new(),
                reserved: HashMap::new(),
                next_id: Some(1),
            }),
            changed: Notify::new(),
        }
    }

    fn reserve(
        self: &Arc<Self>,
        requested: Option<HttpRequestId>,
    ) -> Result<(RegisteredRequest, RegistrationGuard), NetError> {
        let mut state = self.state.lock().map_err(NetError::from_poison)?;
        let request_id = match requested {
            Some(id) => {
                if state.entries.contains_key(&id) || state.reserved.contains_key(&id) {
                    return Err(
                        NetError::from(ErrorKind::DuplicateRequestId).with_stage(ErrorStage::Queue)
                    );
                }
                id
            }
            None => loop {
                let value = state.next_id.ok_or_else(|| {
                    NetError::from(ErrorKind::ResourceExhausted).with_stage(ErrorStage::Queue)
                })?;
                state.next_id = value.checked_add(1);
                let id = HttpRequestId(value);
                if !state.entries.contains_key(&id) && !state.reserved.contains_key(&id) {
                    break id;
                }
            },
        };
        state.reserved.try_reserve(1).map_err(|_| {
            NetError::from(ErrorKind::ResourceExhausted).with_stage(ErrorStage::Queue)
        })?;
        let signal = Arc::new(CancellationSignal::new());
        state.reserved.insert(request_id, Arc::clone(&signal));
        let guard = RegistrationGuard {
            registry: Arc::clone(self),
            request_id,
            control: Arc::clone(&signal),
            committed: AtomicBool::new(false),
        };
        Ok((
            RegisteredRequest {
                id: request_id,
                control: signal,
            },
            guard,
        ))
    }

    fn activate(
        &self,
        request_id: HttpRequestId,
        control: &Arc<CancellationSignal>,
    ) -> Result<(), NetError> {
        let mut state = self.state.lock().map_err(NetError::from_poison)?;
        if state
            .entries
            .get(&request_id)
            .is_some_and(|entry| Arc::ptr_eq(entry, control))
        {
            return Ok(());
        }
        if !state
            .reserved
            .get(&request_id)
            .is_some_and(|entry| Arc::ptr_eq(entry, control))
        {
            return Err(NetError::from(ErrorKind::Internal).with_stage(ErrorStage::Admission));
        }
        state.entries.try_reserve(1).map_err(|_| {
            NetError::from(ErrorKind::ResourceExhausted).with_stage(ErrorStage::Admission)
        })?;
        state.reserved.remove(&request_id);
        state.entries.insert(request_id, Arc::clone(control));
        Ok(())
    }

    #[cfg(test)]
    fn register(
        self: &Arc<Self>,
        requested: Option<HttpRequestId>,
    ) -> Result<(RegisteredRequest, RegistrationGuard), NetError> {
        let (registered, guard) = self.reserve(requested)?;
        self.activate(registered.id, &registered.control)?;
        Ok((registered, guard))
    }

    fn finish(&self, request_id: HttpRequestId, control: &Arc<CancellationSignal>) {
        #[cfg(feature = "http-client-middleware")]
        {
            control.transport_finished.store(true, Ordering::SeqCst);
            if control.middleware_active.load(Ordering::SeqCst) {
                return;
            }
        }
        let removed = match self.state.lock() {
            Ok(mut state) => {
                if state
                    .entries
                    .get(&request_id)
                    .is_some_and(|entry| Arc::ptr_eq(entry, control))
                {
                    state.entries.remove(&request_id)
                } else if state
                    .reserved
                    .get(&request_id)
                    .is_some_and(|entry| Arc::ptr_eq(entry, control))
                {
                    state.reserved.remove(&request_id)
                } else {
                    None
                }
            }
            Err(_) => {
                crate::log_e!(crate::LogType::HTTP; "request_registry", "error", "registry_poisoned");
                None
            }
        };
        if removed.is_some() {
            self.changed.notify_waiters();
        }
        drop(removed);
    }

    fn cancel(&self, request_id: HttpRequestId) -> Result<(), NetError> {
        let signal = self
            .state
            .lock()
            .map_err(NetError::from_poison)?
            .entries
            .get(&request_id)
            .cloned();
        if let Some(signal) = signal {
            signal.cancel();
        }
        Ok(())
    }

    fn cancel_all(&self) -> Result<(), NetError> {
        let state = self.state.lock().map_err(NetError::from_poison)?;
        let signals = state
            .entries
            .values()
            .chain(state.reserved.values())
            .cloned()
            .collect::<Vec<_>>();
        drop(state);
        for signal in signals {
            signal.cancel();
        }
        Ok(())
    }

    fn is_empty(&self) -> Result<bool, NetError> {
        let state = self.state.lock().map_err(NetError::from_poison)?;
        Ok(state.entries.is_empty() && state.reserved.is_empty())
    }

    async fn drain(&self) -> Result<(), NetError> {
        loop {
            if self.is_empty()? {
                return Ok(());
            }
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_empty()? {
                return Ok(());
            }
            notified.await;
        }
    }
}

impl HttpClientInner {
    pub(crate) fn new(
        thread_name: String,
        config: HttpClientConfig,
    ) -> Result<Arc<Self>, NetError> {
        Self::new_with_network_status(thread_name, config, None)
    }

    pub(crate) fn new_with_network_status(
        thread_name: String,
        config: HttpClientConfig,
        context: Option<NetworkStatusContext>,
    ) -> Result<Arc<Self>, NetError> {
        Self::new_with_thread_registry(thread_name, config, context, None)
    }

    fn new_with_thread_registry(
        thread_name: String,
        config: HttpClientConfig,
        context: Option<NetworkStatusContext>,
        threads: Option<Arc<crate::common::retirement::ThreadRegistry>>,
    ) -> Result<Arc<Self>, NetError> {
        config.validate()?;
        let client = build_client(&config)?;
        let network_observation = context.map(HttpNetworkObservation::new).transpose()?;
        let (request_tx, request_rx) = mpsc::channel(config.request_queue_capacity);
        let (supervisor_tx, supervisor_rx) = mpsc::channel(config.request_queue_capacity);
        let (response_tx, response_rx) = mpsc::channel(config.response_queue_capacity);
        let (callback_tx, callback_rx) = mpsc::channel(config.callback_queue_capacity);
        let config = Arc::new(config);
        let in_flight = Arc::new(Semaphore::new(config.request_queue_capacity));
        let shutdown_notify = Arc::new(Notify::new());

        let callback_name = format!("{thread_name}-callback");
        let receive_name = format!("{thread_name}-recv");
        let send_name = thread_name;

        let (callback_thread, callback_ready) =
            spawn_runtime(callback_name, threads.as_ref(), move || {
                Box::pin(async move { callback_loop(callback_rx).await })
            })?;
        if let Err(error) = wait_ready(callback_ready) {
            drop(callback_tx);
            let _ = callback_thread.join();
            return Err(error);
        }

        let callback_tx_for_receive = callback_tx.clone();
        let (receive_thread, receive_ready) =
            match spawn_runtime(receive_name, threads.as_ref(), move || {
                Box::pin(async move {
                    receive_loop(response_rx, callback_tx_for_receive).await;
                })
            }) {
                Ok(value) => value,
                Err(error) => {
                    drop(callback_tx);
                    let _ = callback_thread.join();
                    return Err(error);
                }
            };
        if let Err(error) = wait_ready(receive_ready) {
            drop(callback_tx);
            drop(response_tx);
            let _ = receive_thread.join();
            let _ = callback_thread.join();
            return Err(error);
        }

        let send_config = Arc::clone(&config);
        let send_client = client.clone();
        let send_shutdown_notify = Arc::clone(&shutdown_notify);
        let shutdown_sent = Arc::new(AtomicBool::new(false));
        let send_shutdown_sent = Arc::clone(&shutdown_sent);
        let (send_thread, send_ready) =
            match spawn_runtime(send_name, threads.as_ref(), move || {
                Box::pin(async move {
                    send_loop(
                        request_rx,
                        supervisor_rx,
                        response_tx,
                        send_client,
                        send_config,
                        send_shutdown_notify,
                        send_shutdown_sent,
                    )
                    .await;
                })
            }) {
                Ok(value) => value,
                Err(error) => {
                    drop(request_tx);
                    drop(callback_tx);
                    let _ = receive_thread.join();
                    let _ = callback_thread.join();
                    return Err(error);
                }
            };
        if let Err(error) = wait_ready(send_ready) {
            drop(request_tx);
            drop(callback_tx);
            let _ = send_thread.join();
            let _ = receive_thread.join();
            let _ = callback_thread.join();
            return Err(error);
        }

        Ok(Arc::new(Self {
            request_tx,
            supervisor_tx,
            shutdown_sent,
            admission_gate: Mutex::new(()),
            shutdown_notify,
            shutdown_state: Arc::new(WorkerShutdown::from_joins(
                vec![send_thread, receive_thread, callback_thread],
                threads,
            )),
            client,
            config,
            registry: Arc::new(RequestRegistry::new()),
            in_flight,
            network_observation,
        }))
    }

    pub(crate) fn network_snapshot(&self) -> Result<NetworkSnapshot, NetError> {
        self.network_observation()?.snapshot()
    }

    pub(crate) fn subscribe_network_status(
        &self,
    ) -> Result<StateReceiver<NetworkSnapshot>, NetError> {
        self.network_observation()?.subscribe()
    }

    fn network_observation(&self) -> Result<&Arc<HttpNetworkObservation>, NetError> {
        self.network_observation.as_ref().ok_or_else(|| {
            NetError::config(
                "http.network_status",
                "no network status context was configured",
            )
        })
    }

    pub(crate) fn try_acquire_permit(&self) -> Result<OwnedSemaphorePermit, NetError> {
        Arc::clone(&self.in_flight)
            .try_acquire_owned()
            .map_err(|error| {
                let kind = match error {
                    tokio::sync::TryAcquireError::NoPermits => ErrorKind::QueueFull,
                    tokio::sync::TryAcquireError::Closed => ErrorKind::QueueClosed,
                };
                NetError::from(kind).with_stage(ErrorStage::Queue)
            })
    }

    pub(crate) async fn acquire_permit(&self) -> Result<OwnedSemaphorePermit, NetError> {
        Arc::clone(&self.in_flight)
            .acquire_owned()
            .await
            .map_err(|_| NetError::from(ErrorKind::QueueClosed).with_stage(ErrorStage::Queue))
    }

    pub(crate) fn resolve_timeouts(
        &self,
        options: &HttpRequestOptions,
    ) -> Result<ResolvedTimeouts, NetError> {
        let policy = if options.use_default_retry_policy && options.retry_policy.max_retries() == 0
        {
            &self.config.default_retry_policy
        } else {
            &options.retry_policy
        };
        self.config
            .timeouts()
            .resolve(options.timeouts(), self.config.timeout, policy)
    }

    fn reserve_request(
        &self,
        request_id: Option<HttpRequestId>,
    ) -> Result<(RegisteredRequest, RegistrationGuard), NetError> {
        let _gate = self.admission_gate.lock().map_err(NetError::from_poison)?;
        if self.shutdown_sent.load(Ordering::Acquire) {
            return Err(NetError::from(ErrorKind::QueueClosed).with_stage(ErrorStage::Admission));
        }
        self.registry.reserve(request_id)
    }

    pub(crate) fn registry(&self) -> Arc<RequestRegistry> {
        Arc::clone(&self.registry)
    }

    pub(crate) async fn request_buffered(
        self: &Arc<Self>,
        request: HttpRequest,
    ) -> Result<HttpResponse, NetError> {
        #[cfg(feature = "http-client-middleware")]
        if self.has_middleware() {
            return self.request_with_middleware(request).await;
        }
        let (admission, resolved) = self.admit_buffered_future(&request).await?;
        let (sender, receiver) = oneshot::channel();
        let options = HttpRequestOptions::new()
            .with_retry_setting(request.retry_setting())
            .with_timeouts(resolved.as_overrides())?;
        let handle = self.publish_buffered(ResultRequest::new(request, sender), options, admission);
        let wait_guard = RequestWaitGuard::new(Arc::clone(self), handle.control());
        let result = receiver.await.map_err(|_| {
            NetError::from(ErrorKind::Internal)
                .with_stage(ErrorStage::Dispatch)
                .with_http_request_id(handle.id())
        })?;
        wait_guard.disarm();
        result
    }

    #[cfg(feature = "http-client-middleware")]
    pub(crate) fn middleware_chain(&self) -> Arc<[Arc<dyn HttpMiddleware>]> {
        self.config.middleware_chain()
    }

    #[cfg(feature = "http-client-middleware")]
    pub(crate) fn has_middleware(&self) -> bool {
        !self.config.middleware_chain().is_empty()
    }

    #[cfg(feature = "http-client-middleware")]
    pub(crate) async fn request_with_middleware(
        self: &Arc<Self>,
        request: HttpRequest,
    ) -> Result<HttpResponse, NetError> {
        let policy = request
            .retry_setting()
            .resolve(&self.config.default_retry_policy);
        let (admission, resolved) = self.admit_buffered_future(&request).await?;
        let total = StreamBudget::total(admission.request.operation_start, resolved.total)?;
        let request_id = admission.request.id();
        let control = Arc::clone(&admission.request.registered.control);
        let lease = MiddlewareLease::new(
            self.registry(),
            Arc::clone(&control),
            request_id,
            Arc::clone(&admission.request.permit),
            policy.observer(),
            admission.request.operation_start,
        )?;
        let call = HttpCall::new(request, request_id, HttpResponseMode::Buffered);
        let chain = self.middleware_chain();
        let inner = Arc::clone(self);
        let terminal: crate::api::http::middleware::MiddlewareRunner = Box::new(move |call| {
            let inner = Arc::clone(&inner);
            Box::pin(async move { inner.run_buffered_terminal(admission, call, resolved).await })
        });
        let runner = build_middleware_runner(chain, 0, terminal);
        let result = run_middleware_runner(runner, call, control, total, lease).await?;
        match result {
            HttpMiddlewareResponse::Buffered(response) => {
                if response.request_id() != request_id {
                    return Err(NetError::from(ErrorKind::Protocol)
                        .with_stage(ErrorStage::Response)
                        .with_http_request_id(request_id));
                }
                Ok(response)
            }
            other => Err(middleware_mode_error(
                request_id,
                HttpResponseMode::Buffered,
                other.mode(),
            )),
        }
    }

    #[cfg(feature = "http-client-middleware")]
    async fn run_buffered_terminal(
        self: Arc<Self>,
        admission: Admission<SendCommand>,
        call: HttpCall,
        resolved: ResolvedTimeouts,
    ) -> Result<HttpMiddlewareResponse, NetError> {
        let (request, request_id, mode) = call.into_parts();
        if mode != HttpResponseMode::Buffered {
            return Err(middleware_mode_error(
                request_id,
                HttpResponseMode::Buffered,
                mode,
            ));
        }
        let (sender, receiver) = oneshot::channel();
        let options = HttpRequestOptions::new()
            .with_retry_setting(request.retry_setting())
            .with_timeouts(resolved.as_overrides())?;
        let Admission {
            slot,
            request: admission,
        } = admission;
        check_budget(
            StreamBudget::total(admission.operation_start, resolved.total)?,
            &admission.registered.control,
            ErrorStage::Dispatch,
        )?;
        let (_, job, registration) =
            admission.into_job(ResultRequest::new(request, sender), options);
        let control = Arc::clone(&job.control);
        registration.commit();
        drop(slot.send(SendCommand::Job(job)));
        let wait_guard = RequestWaitGuard::new(Arc::clone(&self), control);
        let result = match receiver.await {
            Ok(result) => result,
            Err(_) => Err(NetError::from(ErrorKind::Internal)
                .with_stage(ErrorStage::Dispatch)
                .with_http_request_id(request_id)),
        };
        wait_guard.disarm();
        result.map(HttpMiddlewareResponse::Buffered)
    }

    #[cfg(test)]
    pub(crate) fn register_request(
        &self,
        request_id: Option<HttpRequestId>,
    ) -> Result<(RegisteredRequest, RegistrationGuard), NetError> {
        let _admission = self.admission_gate.lock().map_err(NetError::from_poison)?;
        if self.shutdown_sent.load(Ordering::Acquire) {
            return Err(NetError::from(ErrorKind::QueueClosed).with_stage(ErrorStage::Queue));
        }
        self.registry.register(request_id)
    }

    pub(crate) fn cancel(&self, request_id: HttpRequestId) -> Result<(), NetError> {
        self.registry.cancel(request_id)
    }

    pub(crate) fn cancel_all(&self) -> Result<(), NetError> {
        self.registry.cancel_all()
    }

    pub(crate) async fn drain(&self) -> Result<(), NetError> {
        self.registry.drain().await
    }

    #[cfg(test)]
    pub(crate) fn submit(&self, job: SendJob) -> Result<(), NetError> {
        let result = {
            let _admission = self.admission_gate.lock().map_err(NetError::from_poison)?;
            if self.shutdown_sent.load(Ordering::Acquire) {
                return Err(NetError::from(ErrorKind::QueueClosed).with_stage(ErrorStage::Queue));
            }
            self.request_tx.try_send(SendCommand::Job(job))
        };
        // A rejected job owns a user request; release it outside the admission gate.
        result.map_err(|error| {
            let kind = match &error {
                mpsc::error::TrySendError::Full(_) => ErrorKind::QueueFull,
                mpsc::error::TrySendError::Closed(_) => ErrorKind::QueueClosed,
            };
            NetError::from(kind).with_stage(ErrorStage::Queue)
        })
    }

    pub(crate) async fn stream_request(
        &self,
        request: HttpRequest,
    ) -> Result<HttpStreamResponse, NetError> {
        let operation_start = Instant::now();
        let policy = request
            .retry_setting()
            .resolve(&self.config.default_retry_policy);
        let resolved =
            self.config
                .timeouts()
                .resolve(request.timeouts(), self.config.timeout, &policy)?;
        let mut owner = StreamOwner::new(&policy, operation_start);
        let admission = self
            .admit_stream(
                self.future_admission_policy(&request),
                resolved,
                operation_start,
                &mut owner,
            )
            .await;
        let (request_id, control) = match admission {
            Ok(admission) => admission,
            Err(error) => {
                owner.complete(Err(&error), false);
                return Err(error);
            }
        };
        let result = self
            .open_stream_after_admission(
                request,
                &policy,
                resolved,
                operation_start,
                &mut owner,
                request_id,
                Arc::clone(&control),
            )
            .await
            .map_err(|error| {
                error
                    .with_http_request_id(request_id)
                    .with_http_attempt(control.attempt.load(Ordering::Acquire))
            });
        match result {
            Ok(response) => Ok(response),
            Err(error) => {
                owner.complete(Err(&error), false);
                Err(error)
            }
        }
    }

    #[cfg(feature = "http-client-middleware")]
    pub(crate) async fn stream_request_with_middleware(
        self: &Arc<Self>,
        request: HttpRequest,
    ) -> Result<HttpStreamResponse, NetError> {
        let operation_start = Instant::now();
        let policy = request
            .retry_setting()
            .resolve(&self.config.default_retry_policy);
        let resolved =
            self.config
                .timeouts()
                .resolve(request.timeouts(), self.config.timeout, &policy)?;
        let mut owner = StreamOwner::new(&policy, operation_start);
        let admission = self
            .admit_stream(
                self.future_admission_policy(&request),
                resolved,
                operation_start,
                &mut owner,
            )
            .await;
        let (request_id, control) = match admission {
            Ok(admission) => admission,
            Err(error) => {
                owner.complete(Err(&error), false);
                return Err(error);
            }
        };
        let total = StreamBudget::total(operation_start, resolved.total)?;
        let permit = owner
            .lease
            .as_ref()
            .and_then(|lease| lease.permit.as_ref())
            .map(Arc::clone)
            .ok_or_else(|| NetError::from(ErrorKind::Internal).with_stage(ErrorStage::Admission))?;
        let lease = MiddlewareLease::new(
            self.registry(),
            Arc::clone(&control),
            request_id,
            permit,
            policy.observer(),
            operation_start,
        )?;
        if let Some(lifecycle) = owner.lifecycle.as_mut() {
            lifecycle.deferred = control.middleware_completion.get().cloned();
        }
        let call = HttpCall::new(request, request_id, HttpResponseMode::Streaming);
        let chain = self.middleware_chain();
        let inner = Arc::clone(self);
        let middleware_control = Arc::clone(&control);
        let terminal_called = Arc::new(AtomicBool::new(false));
        let terminal_called_for_runner = Arc::clone(&terminal_called);
        let terminal: crate::api::http::middleware::MiddlewareRunner = Box::new(move |call| {
            let inner = Arc::clone(&inner);
            let mut owner = owner;
            let control = Arc::clone(&middleware_control);
            Box::pin(async move {
                let (request, request_id, mode) = call.into_parts();
                if mode != HttpResponseMode::Streaming {
                    return Err(middleware_mode_error(
                        request_id,
                        HttpResponseMode::Streaming,
                        mode,
                    ));
                }
                let policy = request
                    .retry_setting()
                    .resolve(&inner.config.default_retry_policy);
                let result = inner
                    .open_stream_after_admission(
                        request,
                        &policy,
                        resolved,
                        operation_start,
                        &mut owner,
                        request_id,
                        Arc::clone(&control),
                    )
                    .await
                    .map_err(|error| {
                        error
                            .with_http_request_id(request_id)
                            .with_http_attempt(control.attempt.load(Ordering::Acquire))
                    });
                terminal_called_for_runner.store(true, Ordering::Release);
                match result {
                    Ok(response) => Ok(HttpMiddlewareResponse::Streaming(response)),
                    Err(error) => {
                        owner.complete(Err(&error), false);
                        Err(error)
                    }
                }
            })
        });
        let runner = build_middleware_runner(chain, 0, terminal);
        let result = run_middleware_runner(runner, call, control, total, lease).await?;
        if !terminal_called.load(Ordering::Acquire) {
            return Err(middleware_mode_error(
                request_id,
                HttpResponseMode::Streaming,
                result.mode(),
            ));
        }
        match result {
            HttpMiddlewareResponse::Streaming(response) => Ok(response),
            other => Err(middleware_mode_error(
                request_id,
                HttpResponseMode::Streaming,
                other.mode(),
            )),
        }
    }

    async fn open_stream_after_admission(
        &self,
        request: HttpRequest,
        policy: &crate::api::http::RetryPolicy,
        resolved: ResolvedTimeouts,
        operation_start: Instant,
        owner: &mut StreamOwner,
        request_id: HttpRequestId,
        control: Arc<CancellationSignal>,
    ) -> Result<HttpStreamResponse, NetError> {
        let total = StreamBudget::total(operation_start, resolved.total)?;
        let method = request.method.clone();
        let retry_allowed = policy.max_retries() > 0 && policy.allows_method(&method, true);
        let mut attempt = 1_u32;
        let mut retry_attempt = 1_u32;
        #[cfg(feature = "http-client-auth")]
        let mut auth_retry_used = false;
        #[cfg(feature = "http-client-auth")]
        let mut auth_do_not_retry = false;
        loop {
            check_stream_start(total, &control)?;
            control.attempt.store(attempt, Ordering::Release);
            let budget =
                total.with_phase(Instant::now(), resolved.attempt, TimeoutPhase::Attempt)?;
            if let Some(lifecycle) = owner.lifecycle.as_mut() {
                lifecycle.attempt_started(attempt);
            }
            check_stream_start(total, &control)?;
            let build_future = build_spec_request(
                &request,
                &self.config,
                &self.client,
                &method,
                request_id,
                attempt,
                #[cfg(feature = "http-client-cookies")]
                &control,
            );
            let prepared = run_budget(budget, &control, ErrorStage::Provider, build_future).await;
            let headers_budget =
                budget.with_phase(Instant::now(), resolved.headers, TimeoutPhase::Headers)?;
            let (result, _auth_generation) = match prepared {
                Ok((built, generation)) => (
                    run_budget(headers_budget, &control, ErrorStage::Response, async {
                        self.client.execute(built).await.map_err(map_response_error)
                    })
                    .await,
                    generation,
                ),
                Err(error) => (Err(error), 0),
            };
            #[cfg(feature = "http-client-auth")]
            let mut response_auth_generation = _auth_generation;
            #[cfg(feature = "http-client-auth")]
            let mut response_auth_allowed = self.config.auth().is_some();
            #[cfg(any(feature = "http-client-auth", feature = "http-client-cookies"))]
            let result = match result {
                Ok(response) => {
                    let hops = ResponseHops {
                        request_headers: &request.headers,
                        body: &request.body,
                        body_present: !request.body.is_empty()
                            || matches!(method, Method::POST | Method::PUT | Method::PATCH),
                        replayable_body: true,
                        method: &method,
                        request_id,
                        attempt,
                        initial_generation: _auth_generation,
                        control: &control,
                        budget: headers_budget,
                        #[cfg(feature = "http-client-cookies")]
                        cookie_context: &request.cookie_context,
                    };
                    match follow_response_hops(response, hops, &self.config, &self.client).await {
                        Ok((response, _generation, _auth_allowed)) => {
                            #[cfg(feature = "http-client-auth")]
                            {
                                response_auth_generation = _generation;
                                response_auth_allowed = _auth_allowed;
                            }
                            Ok(response)
                        }
                        Err(error) => Err(error),
                    }
                }
                Err(error) => Err(error),
            };
            #[cfg(feature = "http-client-auth")]
            if let Ok(response) = &result {
                if response.status() == http::StatusCode::UNAUTHORIZED
                    && !auth_retry_used
                    && response_auth_allowed
                    && policy.allows_method(&method, true)
                {
                    if let Some(auth) = self.config.auth() {
                        let www_authenticate = response
                            .headers()
                            .get(http::header::WWW_AUTHENTICATE)
                            .cloned();
                        let context = AuthResponseContext::new(
                            response.status(),
                            www_authenticate.as_ref(),
                            request_id,
                            attempt,
                            response_auth_generation,
                        );
                        let decision = run_budget(budget, &control, ErrorStage::Provider, async {
                            Ok(AssertUnwindSafe(auth.on_unauthorized(&context))
                                .catch_unwind()
                                .await)
                        })
                        .await?;
                        let action = match decision {
                            Ok(result) => result.map_err(|_| {
                                NetError::from(ErrorKind::ProviderFailed)
                                    .with_stage(ErrorStage::Provider)
                            })?,
                            Err(_) => {
                                return Err(NetError::from(ErrorKind::ProviderFailed)
                                    .with_stage(ErrorStage::Provider));
                            }
                        };
                        if action == AuthAction::RetryWithFreshCredentials {
                            auth_retry_used = true;
                            drop(result);
                            if let Some(lifecycle) = owner.lifecycle.as_mut() {
                                lifecycle.retry_scheduled(
                                    attempt,
                                    Duration::ZERO,
                                    RetryReason::HttpStatus(http::StatusCode::UNAUTHORIZED),
                                );
                            }
                            attempt = attempt.saturating_add(1);
                            continue;
                        } else {
                            auth_do_not_retry = true;
                        }
                    }
                }
            }
            #[cfg(feature = "http-client-auth")]
            let auth_retry_status_blocked = (auth_retry_used || auth_do_not_retry)
                && result
                    .as_ref()
                    .is_ok_and(|response| response.status() == http::StatusCode::UNAUTHORIZED);
            #[cfg(not(feature = "http-client-auth"))]
            let auth_retry_status_blocked = false;
            let retry = match &result {
                Ok(response)
                    if retry_allowed
                        && policy.can_retry_attempt(retry_attempt)
                        && policy.should_retry_status(response.status())
                        && !auth_retry_status_blocked =>
                {
                    Some((
                        RetryReason::HttpStatus(response.status()),
                        policy.retry_delay(retry_attempt, parse_retry_after(response.headers())),
                    ))
                }
                Err(error)
                    if retry_allowed
                        && policy.can_retry_attempt(retry_attempt)
                        && policy.should_retry_error(error)
                        && !matches!(
                            error.timeout_phase(),
                            Some(TimeoutPhase::Admission | TimeoutPhase::Total)
                        ) =>
                {
                    Some((
                        retry_reason_for_error(error),
                        policy.retry_delay(retry_attempt, None),
                    ))
                }
                _ => None,
            };
            if let Some((reason, delay)) = retry {
                drop(result);
                if let Some(lifecycle) = owner.lifecycle.as_mut() {
                    lifecycle.retry_scheduled(attempt, delay, reason);
                }
                check_stream_start(total, &control)?;
                if !wait_for_retry(delay, total.deadline(), &control).await {
                    check_stream_start(total, &control)?;
                    return Err(NetError::from(ErrorKind::DeadlineExceeded)
                        .with_timeout_phase(TimeoutPhase::Total)
                        .with_stage(ErrorStage::Receive));
                }
                attempt = attempt.saturating_add(1);
                retry_attempt = retry_attempt.saturating_add(1);
                continue;
            }
            let response = match result {
                Ok(response) => response,
                Err(error) => {
                    owner.complete(Err(&error), policy.max_attempts() == retry_attempt);
                    return Err(error);
                }
            };
            let status = response.status();
            let headers = response.headers().clone();
            #[cfg(feature = "http-client-http2")]
            let response_info =
                HttpResponseInfo::from_response(response.version(), self.config.http2());
            let body = Box::pin(
                response
                    .bytes_stream()
                    .map(|chunk| chunk.map_err(map_body_error)),
            );
            let body_start = Instant::now();
            let active_budget =
                budget.with_phase(body_start, resolved.idle_read, TimeoutPhase::IdleRead)?;
            check_budget(active_budget, &control, ErrorStage::Receive)?;
            let body_owner = StreamOwner {
                lease: owner.lease.take(),
                supervisor_slot: owner.supervisor_slot.take(),
                lifecycle: owner.lifecycle.take(),
            };
            let (stream, supervisor) = FusedHttpBody::new(
                body,
                Arc::clone(&control),
                budget,
                body_owner,
                status,
                policy.max_attempts() == retry_attempt,
                request_id,
                attempt,
                body_start,
                resolved.idle_read,
            )?;
            check_budget(active_budget, &control, ErrorStage::Receive)?;
            stream.publish_supervisor(supervisor)?;
            let response =
                HttpStreamResponse::new(status, headers, request_id, attempt, Box::pin(stream));
            #[cfg(feature = "http-client-http2")]
            let response = response.with_info(response_info);
            return Ok(response);
        }
    }

    /// Read the permanent admission closure shared by every raw client clone.
    pub(crate) fn is_shutdown_requested(&self) -> bool {
        self.shutdown_sent.load(Ordering::Acquire)
    }

    /// Report actual worker retirement, independently of cleanup launch errors.
    pub(crate) fn workers_joined(&self) -> bool {
        self.shutdown_state.workers_joined()
    }

    /// Close admission under the same gate as queue publication, independently of capacity.
    pub(crate) fn request_shutdown(&self) {
        let _admission = match self.admission_gate.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                crate::log_e!(crate::LogType::HTTP; "request_shutdown", "error", "admission_gate_poisoned");
                poisoned.into_inner()
            }
        };
        if let Some(observation) = &self.network_observation {
            observation.request_close();
        }
        if self.shutdown_sent.swap(true, Ordering::AcqRel) {
            return;
        }
        drop(_admission);
        self.in_flight.close();
        self.shutdown_notify.notify_waiters();
        self.shutdown_notify.notify_one();
    }

    pub(crate) async fn shutdown_and_join(&self) -> Result<(), NetError> {
        self.request_shutdown();
        let workers = self.join_workers().await;
        let observations = match &self.network_observation {
            Some(observation) => observation.wait_cleanup().await,
            None => Ok(()),
        };
        workers.and(observations)
    }

    async fn join_workers(&self) -> Result<(), NetError> {
        self.shutdown_state.start()?;
        self.shutdown_state.wait().await
    }
}

#[cfg(feature = "http-client-middleware")]
fn build_middleware_runner(
    middlewares: Arc<[Arc<dyn HttpMiddleware>]>,
    index: usize,
    terminal: crate::api::http::middleware::MiddlewareRunner,
) -> crate::api::http::middleware::MiddlewareRunner {
    Box::new(move |call| {
        let Some(middleware) = middlewares.get(index).cloned() else {
            return terminal(call);
        };
        let next = Next::from_runner(build_middleware_runner(
            Arc::clone(&middlewares),
            index.saturating_add(1),
            terminal,
        ));
        Box::pin(async move { middleware.call(call, next).await })
    })
}

#[cfg(feature = "http-client-middleware")]
async fn run_middleware_runner(
    runner: crate::api::http::middleware::MiddlewareRunner,
    call: HttpCall,
    control: Arc<CancellationSignal>,
    total: StreamBudget,
    lease: MiddlewareLease,
) -> Result<HttpMiddlewareResponse, NetError> {
    let request_id = call.request_id();
    // Construct user middleware inside the caught and budgeted future too.
    let future = async {
        match AssertUnwindSafe(async { runner(call).await })
            .catch_unwind()
            .await
        {
            Ok(result) => result,
            Err(_) => {
                Err(NetError::from(ErrorKind::CallbackPanicked).with_stage(ErrorStage::Dispatch))
            }
        }
    };
    let result = run_budget(total, &control, ErrorStage::Dispatch, future).await;
    if result
        .as_ref()
        .err()
        .is_some_and(|error| error.timeout_phase().is_some())
    {
        control.cancel();
    }
    lease.finish(&result);
    result.map_err(|error| {
        error
            .with_http_request_id(request_id)
            .with_http_attempt(control.attempt.load(Ordering::Acquire))
    })
}

#[cfg(feature = "http-client-middleware")]
fn middleware_mode_error(
    request_id: HttpRequestId,
    _expected: HttpResponseMode,
    _actual: HttpResponseMode,
) -> NetError {
    NetError::from(ErrorKind::RoutingModeMismatch)
        .with_stage(ErrorStage::Dispatch)
        .with_http_request_id(request_id)
}

impl Drop for HttpClientInner {
    fn drop(&mut self) {
        self.request_shutdown();
        if let Err(error) = self.cancel_all() {
            crate::log_e!(
                crate::LogType::HTTP;
                "drop",
                "error",
                format!("request_cancellation_failed:{:?}", error.kind())
            );
        }
        if self.shutdown_state.start().is_err() {
            // Preserve the final-owner fallback: there is no caller left to
            // retry an OS thread-creation failure. Async shutdown never blocks
            // its caller here, and an accepted cleanup owns its own state.
            self.shutdown_state.join_workers();
        }
    }
}

fn build_client(config: &HttpClientConfig) -> Result<Client, NetError> {
    let default_headers = config.common_headers.clone();
    #[cfg(any(feature = "http-client-auth", feature = "http-client-cookies"))]
    let default_headers = if uses_worker_redirects(config) {
        http::HeaderMap::new()
    } else {
        default_headers
    };
    let builder = Client::builder()
        .tls_backend_preconfigured(config.tls.compile(config.tls_alpn_protocols())?)
        .tcp_keepalive(config.tcp_keepalive)
        .default_headers(default_headers);
    #[cfg(not(feature = "http-client-http2"))]
    let builder = builder.http1_only();
    #[cfg(feature = "http-client-http2")]
    let builder = {
        let builder = match config.http_version_policy() {
            HttpVersionPolicy::Auto => builder,
            HttpVersionPolicy::Http1Only => builder.http1_only(),
            HttpVersionPolicy::Http2Only => {
                // Initial URL validation does not cover transport-owned redirects.
                // Reject a cleartext target before opening a connection or replaying
                // a body, including when no auth/Cookie provider is installed.
                builder.http2_prior_knowledge().https_only(true)
            }
        };
        config.http2().configure_builder(builder)
    };
    let mut builder = builder;
    if let Some(timeout) = config.connect_timeout() {
        builder =
            builder.connector_layer(super::connect_timeout::ConnectTimeoutLayer::new(timeout));
    }
    if !config.environment_proxy {
        builder = builder.no_proxy();
    }
    if let Some(proxy) = &config.proxy {
        let mut proxy_builder = reqwest::Proxy::all(proxy.url()).map_err(|_| {
            NetError::from(ErrorKind::InvalidConfig).with_stage(ErrorStage::Configuration)
        })?;
        if let Some((username, password)) = proxy.credentials() {
            proxy_builder = proxy_builder.basic_auth(username, password);
        }
        builder = builder.proxy(proxy_builder);
    }
    #[cfg(any(feature = "http-client-auth", feature = "http-client-cookies"))]
    if uses_worker_redirects(config) {
        // SDK requests already merge common headers. Keeping another copy in
        // reqwest would reinsert credentials removed at a cross-origin hop.
        builder = builder.redirect(reqwest::redirect::Policy::none());
    }
    builder
        .build()
        .map_err(|error| map_reqwest_error(error, ErrorStage::Configuration))
}

type RuntimeReady = (WorkerJoin, std::sync::mpsc::Receiver<Result<(), NetError>>);

fn spawn_runtime<F>(
    name: String,
    threads: Option<&Arc<crate::common::retirement::ThreadRegistry>>,
    task: F,
) -> Result<RuntimeReady, NetError>
where
    F: FnOnce() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + 'static,
{
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let run = move || {
        let _scope = crate::common::retirement::ExecutionScope::enter();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build();
        match runtime {
            Ok(runtime) => {
                let _ = ready_tx.send(Ok(()));
                runtime.block_on(task());
            }
            Err(error) => {
                let _ = ready_tx.send(Err(NetError::with_source(
                    ErrorKind::RuntimeUnavailable,
                    error,
                )
                .with_stage(ErrorStage::Runtime)));
            }
        }
    };
    let thread = match threads {
        Some(threads) => threads.spawn_tracked(name, run).map(WorkerJoin::Tracked),
        None => thread::Builder::new()
            .name(name)
            .spawn(run)
            .map(WorkerJoin::Owned),
    }
    .map_err(|error| {
        NetError::with_source(ErrorKind::RuntimeUnavailable, error).with_stage(ErrorStage::Runtime)
    })?;
    Ok((thread, ready_rx))
}

fn wait_ready(ready: std::sync::mpsc::Receiver<Result<(), NetError>>) -> Result<(), NetError> {
    ready
        .recv_timeout(Duration::from_secs(5))
        .map_err(|_| NetError::from(ErrorKind::TimedOut).with_stage(ErrorStage::Runtime))?
}

async fn send_loop(
    mut request_rx: mpsc::Receiver<SendCommand>,
    mut supervisor_rx: mpsc::Receiver<StreamSupervisor>,
    response_tx: mpsc::Sender<ReceiveJob>,
    client: Client,
    config: Arc<HttpClientConfig>,
    shutdown_notify: Arc<Notify>,
    shutdown_sent: Arc<AtomicBool>,
) {
    let mut jobs = JoinSet::new();
    let mut accepting = true;
    while accepting {
        // Register before reading the durable flag: an admission waiter may
        // consume Notify's retained permit while this worker handles a job.
        let closing = shutdown_notify.notified();
        tokio::pin!(closing);
        closing.as_mut().enable();
        if shutdown_sent.load(Ordering::Acquire) {
            break;
        }
        tokio::select! {
            _ = closing => accepting = false,
            supervisor = supervisor_rx.recv() => {
                if let Some(supervisor) = supervisor { jobs.spawn(supervisor); }
            }
            command = request_rx.recv() => {
                match command {
                    Some(SendCommand::Job(job)) => {
                        let client = client.clone();
                        let config = Arc::clone(&config);
                        let response_tx = response_tx.clone();
                        jobs.spawn(async move {
                            process_job(job, client, config, response_tx, 1).await;
                        });
                    }
                    None => accepting = false,
                }
            }
            completed = jobs.join_next(), if !jobs.is_empty() => {
                if let Some(Err(_)) = completed {
                    crate::log_e!(crate::LogType::HTTP; "send_loop", "error", "request_task_failed");
                }
            }
        }
    }
    request_rx.close();
    supervisor_rx.close();
    // Outstanding owned slots can still publish after close. Drain both lanes
    // concurrently: either lane's producer may depend on work in the other.
    let mut requests_done = false;
    let mut supervisors_done = false;
    while !requests_done || !supervisors_done || !jobs.is_empty() {
        tokio::select! {
            supervisor = supervisor_rx.recv(), if !supervisors_done => match supervisor {
                Some(supervisor) => { jobs.spawn(supervisor); }
                None => supervisors_done = true,
            },
            command = request_rx.recv(), if !requests_done => match command {
                Some(SendCommand::Job(job)) => {
                    let client = client.clone();
                    let config = Arc::clone(&config);
                    let response_tx = response_tx.clone();
                    jobs.spawn(async move { process_job(job, client, config, response_tx, 1).await; });
                }
                None => requests_done = true,
            },
            completed = jobs.join_next(), if !jobs.is_empty() => {
                if let Some(Err(_)) = completed {
                    crate::log_e!(crate::LogType::HTTP; "send_loop", "error", "request_task_failed");
                }
            }
        }
    }
}

async fn process_job(
    mut job: SendJob,
    client: Client,
    config: Arc<HttpClientConfig>,
    response_tx: mpsc::Sender<ReceiveJob>,
    initial_attempt: u32,
) {
    // Keep ownership of the request outside the attempt future so a user implementation
    // unwinding while building a request still receives its terminal result.
    let result = match AssertUnwindSafe(run_request(&mut job, &client, &config, initial_attempt))
        .catch_unwind()
        .await
    {
        Ok(result) => result,
        Err(_) => {
            crate::log_e!(crate::LogType::HTTP; "process_job", "error", "request_task_panicked");
            Err(NetError::from(ErrorKind::Internal).with_stage(ErrorStage::Dispatch))
        }
    };
    let result = result.map_err(|error| {
        error
            .with_http_request_id(job.request_id)
            .with_http_attempt(job.control.attempt.load(Ordering::Acquire))
    });
    let response = ReceiveJob {
        request: job.request,
        result,
        request_id: job.request_id,
        control: job.control,
        registry: job.registry,
        _permit: job.permit,
        completion: job.completion,
    };
    if let Err(error) = response_tx.send(response).await {
        crate::log_e!(crate::LogType::HTTP; "request_response_queue", "error", "response_queue_closed");
        run_callback(error.0).await;
    }
}

async fn run_request(
    job: &mut SendJob,
    client: &Client,
    config: &HttpClientConfig,
    initial_attempt: u32,
) -> HttpResponseResult {
    let method: Method = job.request.get_method().try_into()?;
    #[cfg(feature = "http-client-cookies")]
    let cookie_context = job.request.cookie_context();
    let replayable = job.options.replayable_body;
    let mut policy = job.options.retry_policy.clone();
    if job.options.use_default_retry_policy && policy.max_retries() == 0 {
        policy = config.default_retry_policy.clone();
    }
    let retry_allowed = policy.max_retries() > 0 && policy.allows_method(&method, replayable);
    let operation_start = job.operation_start;
    let mut lifecycle = RetryObserverLifecycle::new(policy.observer(), operation_start);
    #[cfg(feature = "http-client-middleware")]
    {
        lifecycle.deferred = job.control.middleware_completion.get().cloned();
    }
    let resolved = config
        .timeouts()
        .resolve(job.options.timeouts(), config.timeout, &policy)?;
    let total = StreamBudget::total(operation_start, resolved.total)?;
    let operation_deadline = total.deadline();
    let mut attempt = initial_attempt.max(1);
    let mut retry_attempt = 1_u32;
    #[cfg(feature = "http-client-auth")]
    let mut auth_retry_used = false;
    #[cfg(feature = "http-client-auth")]
    let mut auth_do_not_retry = false;
    loop {
        if let Err(error) = check_budget(total, &job.control, ErrorStage::Dispatch) {
            return terminal_error(&mut lifecycle, error);
        }
        if attempt > 1 && !replayable {
            return terminal_error(
                &mut lifecycle,
                NetError::from(ErrorKind::BodyNotReplayable).with_stage(ErrorStage::RequestBuild),
            );
        }
        job.control.attempt.store(attempt, Ordering::Release);
        let budget = total.with_phase(Instant::now(), resolved.attempt, TimeoutPhase::Attempt)?;
        lifecycle.attempt_started(attempt);
        let request_path = job.request.get_path();
        let request_headers = job.request.headers();
        let request_body = job.request.get_req_body();
        let request_body_present = !request_body.is_empty() || job.request.has_req_body();
        let build = build_request(
            request_path,
            request_headers.clone(),
            request_body.clone(),
            request_body_present,
            config,
            client,
            &method,
            job.request_id,
            attempt,
            #[cfg(feature = "http-client-cookies")]
            &cookie_context,
            #[cfg(feature = "http-client-cookies")]
            &job.control,
        );
        let prepared = run_budget(budget, &job.control, ErrorStage::Provider, build).await;
        let headers_budget =
            budget.with_phase(Instant::now(), resolved.headers, TimeoutPhase::Headers)?;
        // Preparation and transport failures share the existing retry policy, while
        // every stage keeps the one absolute attempt deadline.
        let (response, _auth_generation) = match prepared {
            Ok((request, _has_body, generation)) => (
                run_budget(headers_budget, &job.control, ErrorStage::Response, async {
                    client.execute(request).await.map_err(map_response_error)
                })
                .await,
                generation,
            ),
            Err(error) => (Err(error), 0),
        };
        #[cfg(feature = "http-client-auth")]
        let mut response_auth_generation = _auth_generation;
        #[cfg(feature = "http-client-auth")]
        let mut response_auth_allowed = config.auth().is_some();
        #[cfg(any(feature = "http-client-auth", feature = "http-client-cookies"))]
        let response = match response {
            Ok(response) => {
                let hops = ResponseHops {
                    request_headers: &request_headers,
                    body: &request_body,
                    body_present: request_body_present
                        || matches!(method, Method::POST | Method::PUT | Method::PATCH),
                    replayable_body: replayable,
                    method: &method,
                    request_id: job.request_id,
                    attempt,
                    initial_generation: _auth_generation,
                    control: &job.control,
                    budget: headers_budget,
                    #[cfg(feature = "http-client-cookies")]
                    cookie_context: &cookie_context,
                };
                match follow_response_hops(response, hops, config, client).await {
                    Ok((response, _generation, _auth_allowed)) => {
                        #[cfg(feature = "http-client-auth")]
                        {
                            response_auth_generation = _generation;
                            response_auth_allowed = _auth_allowed;
                        }
                        Ok(response)
                    }
                    Err(error) => Err(error),
                }
            }
            Err(error) => Err(error),
        };
        let auth_retry_status_blocked = false;
        #[cfg(feature = "http-client-auth")]
        let mut auth_retry_status_blocked = auth_retry_status_blocked;
        let result = match response {
            Ok(response) => {
                #[cfg(feature = "http-client-auth")]
                if response.status() == http::StatusCode::UNAUTHORIZED
                    && !auth_retry_used
                    && response_auth_allowed
                {
                    if let Some(auth) = config.auth() {
                        let allowed = replayable && policy.allows_method(&method, replayable);
                        if allowed {
                            let www_authenticate = response
                                .headers()
                                .get(http::header::WWW_AUTHENTICATE)
                                .cloned();
                            let context = AuthResponseContext::new(
                                response.status(),
                                www_authenticate.as_ref(),
                                job.request_id,
                                attempt,
                                response_auth_generation,
                            );
                            let decision =
                                run_budget(budget, &job.control, ErrorStage::Provider, async {
                                    Ok(AssertUnwindSafe(auth.on_unauthorized(&context))
                                        .catch_unwind()
                                        .await)
                                })
                                .await?;
                            let action = match decision {
                                Ok(result) => result.map_err(|_| {
                                    NetError::from(ErrorKind::ProviderFailed)
                                        .with_stage(ErrorStage::Provider)
                                })?,
                                Err(_) => {
                                    return terminal_error(
                                        &mut lifecycle,
                                        NetError::from(ErrorKind::ProviderFailed)
                                            .with_stage(ErrorStage::Provider),
                                    )
                                }
                            };
                            if matches!(action, AuthAction::RetryWithFreshCredentials) {
                                auth_retry_used = true;
                                drop(response);
                                lifecycle.retry_scheduled(
                                    attempt,
                                    Duration::ZERO,
                                    RetryReason::HttpStatus(http::StatusCode::UNAUTHORIZED),
                                );
                                attempt = attempt.saturating_add(1);
                                continue;
                            } else {
                                auth_do_not_retry = true;
                            }
                        }
                    }
                }
                #[cfg(feature = "http-client-auth")]
                {
                    auth_retry_status_blocked = (auth_retry_used || auth_do_not_retry)
                        && response.status() == http::StatusCode::UNAUTHORIZED;
                }
                if retry_allowed
                    && policy.can_retry_attempt(retry_attempt)
                    && policy.should_retry_status(response.status())
                    && !auth_retry_status_blocked
                {
                    if !replayable {
                        lifecycle.note_reason(RetryReason::HttpStatus(response.status()));
                        return terminal_error(
                            &mut lifecycle,
                            NetError::from(ErrorKind::BodyNotReplayable)
                                .with_stage(ErrorStage::RequestBuild),
                        );
                    }
                    let status = response.status();
                    let retry_after = parse_retry_after(response.headers());
                    drop(response);
                    let delay = policy.retry_delay(retry_attempt, retry_after);
                    lifecycle.retry_scheduled(attempt, delay, RetryReason::HttpStatus(status));
                    if !wait_for_retry(delay, operation_deadline, &job.control).await {
                        if job.control.is_cancelled() {
                            return terminal_error(
                                &mut lifecycle,
                                NetError::from(ErrorKind::Cancelled)
                                    .with_stage(ErrorStage::Dispatch),
                            );
                        }
                        return terminal_error(
                            &mut lifecycle,
                            NetError::from(ErrorKind::DeadlineExceeded)
                                .with_timeout_phase(TimeoutPhase::Total)
                                .with_stage(ErrorStage::Receive),
                        );
                    }
                    attempt = attempt.saturating_add(1);
                    retry_attempt = retry_attempt.saturating_add(1);
                    continue;
                }
                read_response(
                    response,
                    attempt,
                    job.request_id,
                    config.max_response_bytes,
                    Arc::clone(&job.control),
                    #[cfg(feature = "http-client-http2")]
                    config.http2(),
                    budget,
                    resolved.idle_read,
                )
                .await
            }
            Err(error) => Err(error),
        };
        if let Ok(buffered) = &result {
            if retry_allowed
                && policy.can_retry_attempt(retry_attempt)
                && policy.should_retry_application(buffered)
                && !auth_retry_status_blocked
            {
                if !replayable {
                    lifecycle.note_reason(RetryReason::Application);
                    return terminal_error(
                        &mut lifecycle,
                        NetError::from(ErrorKind::BodyNotReplayable)
                            .with_stage(ErrorStage::RequestBuild),
                    );
                }
                let delay = policy.retry_delay(retry_attempt, None);
                lifecycle.retry_scheduled(attempt, delay, RetryReason::Application);
                if !wait_for_retry(delay, operation_deadline, &job.control).await {
                    if job.control.is_cancelled() {
                        return terminal_error(
                            &mut lifecycle,
                            NetError::from(ErrorKind::Cancelled).with_stage(ErrorStage::Receive),
                        );
                    }
                    return terminal_error(
                        &mut lifecycle,
                        NetError::from(ErrorKind::DeadlineExceeded)
                            .with_timeout_phase(TimeoutPhase::Total)
                            .with_stage(ErrorStage::Receive),
                    );
                }
                attempt = attempt.saturating_add(1);
                retry_attempt = retry_attempt.saturating_add(1);
                continue;
            }
        }
        if let Err(error) = &result {
            if retry_allowed
                && policy.can_retry_attempt(retry_attempt)
                && policy.should_retry_error(error)
                && !matches!(
                    error.timeout_phase(),
                    Some(TimeoutPhase::Admission | TimeoutPhase::Total)
                )
            {
                if !replayable {
                    lifecycle.note_reason(retry_reason_for_error(error));
                    return terminal_error(
                        &mut lifecycle,
                        NetError::from(ErrorKind::BodyNotReplayable)
                            .with_stage(ErrorStage::RequestBuild),
                    );
                }
                let delay = policy.retry_delay(retry_attempt, None);
                lifecycle.retry_scheduled(attempt, delay, retry_reason_for_error(error));
                if !wait_for_retry(delay, operation_deadline, &job.control).await {
                    if job.control.is_cancelled() {
                        return terminal_error(
                            &mut lifecycle,
                            NetError::from(ErrorKind::Cancelled).with_stage(ErrorStage::Receive),
                        );
                    }
                    return terminal_error(
                        &mut lifecycle,
                        NetError::from(ErrorKind::DeadlineExceeded)
                            .with_timeout_phase(TimeoutPhase::Total)
                            .with_stage(ErrorStage::Receive),
                    );
                }
                attempt = attempt.saturating_add(1);
                retry_attempt = retry_attempt.saturating_add(1);
                continue;
            }
        }
        let result = arbitrate_result(budget, &job.control, ErrorStage::Receive, result);
        lifecycle.complete(&result, policy.max_attempts() == retry_attempt);
        return result;
    }
}

fn terminal_error(lifecycle: &mut RetryObserverLifecycle, error: NetError) -> HttpResponseResult {
    let result = Err(error);
    lifecycle.complete(&result, false);
    result
}

async fn build_request(
    path: String,
    request_headers: http::HeaderMap,
    body: bytes::Bytes,
    body_present: bool,
    config: &HttpClientConfig,
    client: &Client,
    method: &Method,
    _request_id: HttpRequestId,
    _attempt: u32,
    #[cfg(feature = "http-client-cookies")] cookie_context: &CookieRequestContext,
    #[cfg(feature = "http-client-cookies")] control: &CancellationSignal,
) -> Result<(reqwest::Request, bool, u64), NetError> {
    let url = config.resolve_request_path(path.as_str())?;
    let body_is_empty = body.is_empty();
    let auth_generation = 0_u64;
    #[cfg(feature = "http-client-auth")]
    let mut auth_generation = auth_generation;
    let mut headers = config.merge_headers(&request_headers)?;
    #[cfg(feature = "http-client-auth")]
    if let Some(auth) = config.auth() {
        let origin = url.origin().ascii_serialization();
        let mut context = AuthRequestContext::new(
            method,
            &origin,
            path.as_str(),
            _request_id,
            _attempt,
            &mut headers,
        );
        let applied = AssertUnwindSafe(auth.apply(&mut context))
            .catch_unwind()
            .await;
        match applied {
            Ok(result) => result.map_err(|_| {
                NetError::from(ErrorKind::ProviderFailed).with_stage(ErrorStage::Provider)
            })?,
            Err(_) => {
                return Err(
                    NetError::from(ErrorKind::ProviderFailed).with_stage(ErrorStage::Provider)
                )
            }
        }
        auth_generation = context.credential_generation();
    }
    #[cfg(feature = "http-client-cookies")]
    {
        cookie_hooks::check_cancelled(control, _request_id)?;
        let applied = apply_cookie_header(
            &mut headers,
            &url,
            method,
            cookie_context,
            config,
            _request_id,
        );
        cookie_hooks::check_cancelled(control, _request_id)?;
        applied?;
    }
    let mut builder = client.request(method.clone(), url);
    builder = builder.headers(headers);
    if body_present || matches!(*method, Method::POST | Method::PUT | Method::PATCH) {
        builder = builder.body(body);
        if body_is_empty {
            builder = builder.header(http::header::CONTENT_LENGTH, "0");
        }
    }
    let built = builder
        .build()
        .map_err(|error| map_reqwest_error(error, ErrorStage::RequestBuild))?;
    Ok((built, body_present, auth_generation))
}

async fn build_spec_request(
    request: &HttpRequest,
    config: &HttpClientConfig,
    client: &Client,
    method: &Method,
    _request_id: HttpRequestId,
    _attempt: u32,
    #[cfg(feature = "http-client-cookies")] control: &CancellationSignal,
) -> Result<(reqwest::Request, u64), NetError> {
    let (built, _, generation) = build_request(
        request.path.clone(),
        request.headers.clone(),
        request.body.clone(),
        !request.body.is_empty(),
        config,
        client,
        method,
        _request_id,
        _attempt,
        #[cfg(feature = "http-client-cookies")]
        &request.cookie_context,
        #[cfg(feature = "http-client-cookies")]
        control,
    )
    .await?;
    Ok((built, generation))
}

fn admission_error(error: NetError, request_id: Option<HttpRequestId>) -> NetError {
    let error = error.with_stage(ErrorStage::Admission).with_http_attempt(0);
    match request_id {
        Some(id) => error.with_http_request_id(id),
        None => error,
    }
}

fn check_budget(
    budget: StreamBudget,
    control: &CancellationSignal,
    stage: ErrorStage,
) -> Result<(), NetError> {
    if control.is_cancelled() {
        return Err(NetError::from(ErrorKind::Cancelled).with_stage(stage));
    }
    match budget.expired(Instant::now(), stage) {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn check_stream_start(budget: StreamBudget, control: &CancellationSignal) -> Result<(), NetError> {
    check_budget(budget, control, ErrorStage::Response)
}

async fn run_budget<T>(
    budget: StreamBudget,
    control: &CancellationSignal,
    stage: ErrorStage,
    future: impl std::future::Future<Output = Result<T, NetError>>,
) -> Result<T, NetError> {
    check_budget(budget, control, stage)?;
    let mut timer = budget.timer();
    let result = tokio::select! {
        biased;
        _ = control.wait() => Err(NetError::from(ErrorKind::Cancelled).with_stage(stage)),
        result = future => result,
        _ = wait_budget(&mut timer) => match budget.expired(Instant::now(), stage) {
            Some(error) => Err(error),
            None => Err(NetError::from(ErrorKind::Internal).with_stage(stage)),
        },
    };
    arbitrate_result(budget, control, stage, result)
}

fn arbitrate_result<T>(
    budget: StreamBudget,
    control: &CancellationSignal,
    stage: ErrorStage,
    result: Result<T, NetError>,
) -> Result<T, NetError> {
    if control.is_cancelled() {
        return Err(NetError::from(ErrorKind::Cancelled).with_stage(stage));
    }
    let candidate = result.as_ref().err().and_then(|error| {
        crate::module::http::http_timeout::timeout_deadline(error).or_else(|| {
            crate::module::http::http_failure::connect_timeout_deadline(error)
                .map(|deadline| (deadline, TimeoutPhase::Connect))
        })
    });
    if candidate.is_some_and(|(deadline, phase)| budget.candidate_wins(deadline, phase)) {
        return result;
    }
    if let Err(error) = check_budget(budget, control, stage) {
        // A deadline can expire between reading the body and final arbitration.
        // Preserve this attempt's received head without changing the winning
        // error or attaching metadata to explicit cancellation.
        return Err(
            match result.as_ref().err().and_then(NetError::http_response_head) {
                Some(head) if error.kind() != ErrorKind::Cancelled => {
                    error.with_http_response_head(head.clone())
                }
                _ => error,
            },
        );
    }
    result
}

async fn wait_budget(timer: &mut Option<std::pin::Pin<Box<tokio::time::Sleep>>>) {
    match timer.as_mut() {
        Some(timer) => timer.as_mut().await,
        None => std::future::pending::<()>().await,
    }
}

async fn wait_for_retry(
    delay: Duration,
    deadline: Option<Instant>,
    control: &CancellationSignal,
) -> bool {
    let delay = match deadline {
        Some(limit) => match limit.checked_duration_since(Instant::now()) {
            Some(remaining) => remaining.min(delay),
            None => return false,
        },
        None => delay,
    };
    tokio::select! {
        _ = control.wait() => false,
        _ = tokio::time::sleep(delay) => !control.is_cancelled() && deadline.is_none_or(|limit| Instant::now() < limit),
    }
}

async fn receive_loop(
    mut response_rx: mpsc::Receiver<ReceiveJob>,
    callback_tx: mpsc::Sender<CallbackJob>,
) {
    while let Some(job) = response_rx.recv().await {
        if let Err(error) = callback_tx.send(job).await {
            crate::log_e!(crate::LogType::HTTP; "receive_loop", "error", "callback_queue_closed");
            run_callback(error.0).await;
        }
    }
}

async fn read_response(
    response: Response,
    attempts: u32,
    request_id: HttpRequestId,
    max_response_bytes: usize,
    control: Arc<CancellationSignal>,
    #[cfg(feature = "http-client-http2")] http2: &crate::api::http::Http2Config,
    budget: StreamBudget,
    idle_timeout: Option<Duration>,
) -> HttpResponseResult {
    let status = response.status();
    let headers = response.headers().clone();
    #[cfg(feature = "http-client-http2")]
    let response_version = response.version();
    let result = async {
        let mut stream = response.bytes_stream();
        let mut body = BytesMut::new();
        let mut body_chunks = 0_u64;
        let mut read_budget =
            budget.with_phase(Instant::now(), idle_timeout, TimeoutPhase::IdleRead)?;
        loop {
            let next = run_budget(read_budget, &control, ErrorStage::Receive, async {
                stream.next().await.transpose().map_err(map_body_error)
            })
            .await?;
            let Some(chunk) = next else {
                break;
            };
            if !chunk.is_empty() {
                read_budget =
                    budget.with_phase(Instant::now(), idle_timeout, TimeoutPhase::IdleRead)?;
            }
            if body.len().saturating_add(chunk.len()) > max_response_bytes {
                return Err(NetError::from(ErrorKind::ItemTooLarge).with_stage(ErrorStage::Receive));
            }
            body_chunks = body_chunks.saturating_add(1);
            body.extend_from_slice(&chunk);
        }
        Ok((body, body_chunks))
    }
    .await;
    let (body, _body_chunks) = match result {
        Ok(body) => body,
        Err(error) if error.kind() == ErrorKind::Cancelled => return Err(error),
        Err(error) => {
            return Err(error.with_http_response_head(HttpResponseHead { status, headers }));
        }
    };
    let response = HttpResponse::new(status, headers, body.freeze(), attempts, request_id);
    #[cfg(feature = "http-client-http2")]
    let response = {
        let mut info = HttpResponseInfo::from_response(response_version, http2);
        if let Some(mut flow) = info.flow_control {
            flow.body_bytes_delivered = response.body.len() as u64;
            flow.body_chunks_delivered = _body_chunks;
            info.flow_control = Some(flow);
        }
        response.with_info(info)
    };
    Ok(response)
}

async fn callback_loop(mut callback_rx: mpsc::Receiver<CallbackJob>) {
    let mut callbacks = JoinSet::new();
    loop {
        tokio::select! {
            job = callback_rx.recv() => match job {
                Some(job) => { callbacks.spawn(run_callback(job)); }
                None => break,
            },
            completed = callbacks.join_next(), if !callbacks.is_empty() => {
                if let Some(Err(_)) = completed {
                    crate::log_e!(crate::LogType::HTTP; "callback_loop", "error", "callback_task_failed");
                }
            }
        }
    }
    while let Some(callback) = callbacks.join_next().await {
        if callback.is_err() {
            crate::log_e!(crate::LogType::HTTP; "callback_loop", "error", "callback_task_failed");
        }
    }
}

async fn run_callback(job: CallbackJob) {
    // Declare the unique completion owner before operation resources so unwind
    // and future-drop paths release those resources before publishing completion.
    let mut completion = job.completion;
    let registration = RegistrationGuard {
        registry: job.registry,
        request_id: job.request_id,
        control: Arc::clone(&job.control),
        // The completion owner retires trait requests after its last permit.
        committed: AtomicBool::new(completion.is_some()),
    };
    let permit = job._permit;
    let cancelled = if completion.is_some() {
        job.control.enter_callback()
    } else {
        // Buffered middleware bridges and streams retain their existing
        // cancellation lifetime beyond this internal result callback.
        job.control.is_cancelled()
    };
    let result = if cancelled
        && job
            .result
            .as_ref()
            .err()
            .is_none_or(|error| error.timeout_phase().is_none())
    {
        Err(NetError::from(ErrorKind::Cancelled)
            .with_stage(ErrorStage::Dispatch)
            .with_http_request_id(job.request_id)
            .with_http_attempt(job.control.attempt.load(Ordering::Acquire)))
    } else {
        job.result
    };
    if let Some(completion) = completion.as_mut() {
        completion.delivered(
            result
                .as_ref()
                .map(|response| HttpResponseSummary {
                    status: response.status,
                    attempts: response.attempts(),
                })
                .map_err(Clone::clone),
        );
    }
    let mut callback_result = Ok(());
    let callback_error = || {
        NetError::from(ErrorKind::CallbackPanicked)
            .with_stage(ErrorStage::Dispatch)
            .with_http_request_id(job.request_id)
            .with_http_attempt(job.control.attempt.load(Ordering::Acquire))
    };
    let request = job.request;
    if let Ok(response) = &result {
        if std::panic::catch_unwind(AssertUnwindSafe(|| {
            request.on_response_headers(&response.headers);
        }))
        .is_err()
        {
            callback_result = Err(callback_error());
            crate::log_e!(crate::LogType::HTTP; "callback_loop", "error", "header_callback_panicked");
        }
    }
    // Catch both synchronous future construction and asynchronous execution.
    if AssertUnwindSafe(async move { request.deal_with_response(result).await })
        .catch_unwind()
        .await
        .is_err()
    {
        callback_result = Err(callback_error());
        crate::log_e!(crate::LogType::HTTP; "callback_loop", "error", "callback_panicked");
    }
    drop(permit);
    drop(registration);
    if let Some(completion) = completion.as_mut() {
        completion.callback_finished(callback_result);
    }
    drop(completion);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::http::HttpRequestMethod;

    #[tokio::test(start_paused = true)]
    async fn final_deadline_arbitration_retains_this_attempts_body_failure_head(
    ) -> Result<(), Box<dyn StdError + Send + Sync>> {
        let control = CancellationSignal::new();
        let budget = StreamBudget::total(Instant::now(), Some(Duration::from_secs(1)))?;
        let error = NetError::from(ErrorKind::Io)
            .with_stage(ErrorStage::Receive)
            .with_http_response_head(HttpResponseHead {
                status: http::StatusCode::UNAUTHORIZED,
                headers: http::HeaderMap::new(),
            });
        tokio::time::advance(Duration::from_secs(1)).await;
        let result: HttpResponseResult =
            arbitrate_result(budget, &control, ErrorStage::Receive, Err(error.clone()));
        let expired = result.err().ok_or("expired body became successful")?;
        check(
            expired.kind() == ErrorKind::DeadlineExceeded
                && expired.timeout_phase() == Some(TimeoutPhase::Total)
                && expired.http_response_head().map(|head| head.status)
                    == Some(http::StatusCode::UNAUTHORIZED),
            "final timeout arbitration lost received head or timeout context",
        )?;
        control.request_cancel();
        let result: HttpResponseResult =
            arbitrate_result(budget, &control, ErrorStage::Receive, Err(error));
        let cancelled = result.err().ok_or("cancelled body became successful")?;
        check(
            cancelled.kind() == ErrorKind::Cancelled && cancelled.http_response_head().is_none(),
            "cancellation was misrepresented as a buffered body failure",
        )
    }

    #[tokio::test]
    async fn cancellation_after_body_poll_does_not_become_a_response_read_failure(
    ) -> Result<(), Box<dyn StdError + Send + Sync>> {
        let (entered, body_polled) = oneshot::channel();
        let stream = futures_util::stream::once(async move {
            let _ = entered.send(());
            std::future::pending::<Result<bytes::Bytes, std::io::Error>>().await
        });
        let response = http::Response::builder()
            .status(http::StatusCode::UNAUTHORIZED)
            .body(reqwest::Body::wrap_stream(stream))?;
        let control = Arc::new(CancellationSignal::new());
        #[cfg(feature = "http-client-http2")]
        let config = HttpClientConfig::new("http://localhost")?;
        let reading = read_response(
            response.into(),
            1,
            HttpRequestId(1),
            1024,
            Arc::clone(&control),
            #[cfg(feature = "http-client-http2")]
            config.http2(),
            StreamBudget::default(),
            None,
        );
        let cancelling = async {
            body_polled.await?;
            control.request_cancel();
            Ok::<(), oneshot::error::RecvError>(())
        };
        let (result, cancelled) = tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(reading, cancelling)
        })
        .await?;
        cancelled?;
        let error = result.err().ok_or("cancelled body became successful")?;
        check(
            error.kind() == ErrorKind::Cancelled && error.http_response_head().is_none(),
            "cancellation after body polling was misrepresented as a body failure",
        )
    }

    struct CountingRequest(Arc<std::sync::atomic::AtomicUsize>);

    struct OwnedRequest {
        token: Box<u64>,
        calls: Arc<std::sync::atomic::AtomicUsize>,
        drops: Arc<std::sync::atomic::AtomicUsize>,
        _send_only: std::cell::Cell<()>,
    }

    impl Drop for OwnedRequest {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[async_trait::async_trait]
    impl HttpRequestTrait for OwnedRequest {
        fn get_path(&self) -> String {
            "/".to_owned()
        }
        fn get_method(&self) -> HttpRequestMethod {
            HttpRequestMethod::GET
        }
        async fn deal_with_response(self: Box<Self>, _result: HttpResponseResult) {
            self.calls.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn owned_request() -> OwnedRequest {
        OwnedRequest {
            token: Box::new(42),
            calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            drops: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            _send_only: std::cell::Cell::new(()),
        }
    }

    #[test]
    fn cancellation_and_callback_entry_have_one_atomic_boundary(
    ) -> Result<(), Box<dyn StdError + Send + Sync>> {
        for _ in 0..64 {
            let control = Arc::new(CancellationSignal::new());
            let barrier = Arc::new(std::sync::Barrier::new(2));
            let entered = thread::scope(|scope| {
                let cancel = scope.spawn(|| {
                    barrier.wait();
                    control.request_cancel()
                });
                barrier.wait();
                let entered_cancelled = control.enter_callback();
                (cancel.join(), entered_cancelled)
            });
            let outcome = entered
                .0
                .map_err(|_| std::io::Error::other("cancel thread panicked"))?;
            check(
                matches!(
                    (outcome, entered.1),
                    (HttpCancelOutcome::Requested, true) | (HttpCancelOutcome::TooLate, false)
                ),
                "cancel and callback entry observed inconsistent order",
            )?;
            check(
                control.request_cancel() == HttpCancelOutcome::TooLate,
                "callback entry did not freeze cancellation",
            )?;
        }
        Ok(())
    }

    struct BlockingCallback {
        started: tokio::sync::oneshot::Sender<()>,
    }

    struct CompletionProbe {
        header_panics: bool,
        handler_panics: bool,
        delivered: tokio::sync::oneshot::Sender<HttpResponseResult>,
    }

    #[async_trait::async_trait]
    impl HttpRequestTrait for CompletionProbe {
        fn get_path(&self) -> String {
            "/".to_owned()
        }

        fn get_method(&self) -> HttpRequestMethod {
            HttpRequestMethod::GET
        }

        fn on_response_headers(&self, _headers: &http::HeaderMap) {
            if self.header_panics {
                panic!("intentional header hook panic for completion verification");
            }
        }

        async fn deal_with_response(self: Box<Self>, result: HttpResponseResult) {
            let _ = self.delivered.send(result);
            if self.handler_panics {
                panic!("intentional handler panic for completion verification");
            }
        }
    }

    fn callback_job(job: SendJob, result: HttpResponseResult) -> CallbackJob {
        CallbackJob {
            request: job.request,
            result,
            request_id: job.request_id,
            control: job.control,
            registry: job.registry,
            _permit: job.permit,
            completion: job.completion,
        }
    }

    async fn verify_panicking_callback_completion(
        header_panics: bool,
        handler_panics: bool,
    ) -> Result<(), Box<dyn StdError + Send + Sync>> {
        let (worker, mut receiver) = paused_worker()?;
        let (delivered, delivered_rx) = tokio::sync::oneshot::channel();
        let handle = worker
            .submit_request(
                CompletionProbe {
                    header_panics,
                    handler_panics,
                    delivered,
                },
                HttpRequestOptions::new(),
            )
            .map_err(EnqueueError::into_error)?;
        let Some(SendCommand::Job(job)) = receiver.recv().await else {
            return Err(NetError::from(ErrorKind::Internal).into());
        };
        let response = HttpResponse::new(
            http::StatusCode::CREATED,
            http::HeaderMap::new(),
            bytes::Bytes::from_static(b"created"),
            2,
            handle.id(),
        );
        run_callback(callback_job(job, Ok(response))).await;
        let delivered = delivered_rx.await??;
        let completion = tokio::time::timeout(Duration::from_secs(2), handle.finished()).await?;
        let summary = completion.response?;
        check(
            delivered.status == http::StatusCode::CREATED
                && delivered.body == bytes::Bytes::from_static(b"created")
                && summary.status == delivered.status
                && summary.attempts == delivered.attempts(),
            "callback panic lost or changed the actual response summary",
        )?;
        check(
            completion.callback.as_ref().is_err_and(|error| {
                error.kind() == ErrorKind::CallbackPanicked
                    && error.context().stage == Some(ErrorStage::Dispatch)
            }),
            "callback panic was not reported in completion",
        )?;
        check(
            worker.registry.is_empty()? && worker.in_flight.available_permits() == 128,
            "callback panic completion preceded resource cleanup",
        )
    }

    #[tokio::test]
    async fn completion_reports_header_panic_and_still_delivers_response_to_handler(
    ) -> Result<(), Box<dyn StdError + Send + Sync>> {
        verify_panicking_callback_completion(true, false).await
    }

    #[tokio::test]
    async fn completion_reports_handler_panic_and_preserves_successful_response(
    ) -> Result<(), Box<dyn StdError + Send + Sync>> {
        verify_panicking_callback_completion(false, true).await
    }

    #[tokio::test]
    async fn completion_preserves_determined_timeout_when_callback_is_cancelled(
    ) -> Result<(), Box<dyn StdError + Send + Sync>> {
        let (worker, mut receiver) = paused_worker()?;
        let (delivered, delivered_rx) = tokio::sync::oneshot::channel();
        let handle = worker
            .submit_request(
                CompletionProbe {
                    header_panics: false,
                    handler_panics: false,
                    delivered,
                },
                HttpRequestOptions::new(),
            )
            .map_err(EnqueueError::into_error)?;
        let Some(SendCommand::Job(job)) = receiver.recv().await else {
            return Err(NetError::from(ErrorKind::Internal).into());
        };
        let expired = NetError::from(ErrorKind::DeadlineExceeded)
            .with_stage(ErrorStage::Receive)
            .with_timeout_phase(TimeoutPhase::Total)
            .with_http_request_id(handle.id())
            .with_http_attempt(2)
            .with_http_response_head(HttpResponseHead {
                status: http::StatusCode::UNAUTHORIZED,
                headers: http::HeaderMap::new(),
            });
        check(
            handle.cancel() == HttpCancelOutcome::Requested,
            "cancellation was not registered",
        )?;
        run_callback(callback_job(job, Err(expired))).await;
        let delivered = delivered_rx
            .await?
            .err()
            .ok_or_else(|| std::io::Error::other("handler did not receive timeout"))?;
        let completion = tokio::time::timeout(Duration::from_secs(2), handle.finished()).await?;
        let completed = completion
            .response
            .err()
            .ok_or_else(|| std::io::Error::other("completion did not preserve timeout"))?;
        check(
            delivered.kind() == ErrorKind::DeadlineExceeded
                && delivered.timeout_phase() == Some(TimeoutPhase::Total)
                && completed.kind() == delivered.kind()
                && completed.timeout_phase() == delivered.timeout_phase()
                && completed.context().stage == delivered.context().stage
                && completed.context().http_request_id == delivered.context().http_request_id
                && completed.context().http_attempt == delivered.context().http_attempt
                && completed.http_response_head().map(|head| head.status)
                    == Some(http::StatusCode::UNAUTHORIZED)
                && delivered.http_response_head().map(|head| head.status)
                    == Some(http::StatusCode::UNAUTHORIZED),
            "callback cancellation overwrote timeout or completion disagreed with handler",
        )?;
        completion.callback?;
        check(
            handle.cancel() == HttpCancelOutcome::TooLate
                && worker.registry.is_empty()?
                && worker.in_flight.available_permits() == 128,
            "timeout completion did not settle operation resources",
        )
    }

    #[async_trait::async_trait]
    impl HttpRequestTrait for BlockingCallback {
        fn get_path(&self) -> String {
            "/".to_owned()
        }
        fn get_method(&self) -> HttpRequestMethod {
            HttpRequestMethod::GET
        }
        async fn deal_with_response(self: Box<Self>, _result: HttpResponseResult) {
            let _ = self.started.send(());
            std::future::pending::<()>().await;
        }
    }

    #[tokio::test]
    async fn aborted_callback_keeps_delivered_summary_and_settles_after_cleanup(
    ) -> Result<(), Box<dyn StdError + Send + Sync>> {
        let (worker, mut receiver) = paused_worker()?;
        let (started, started_rx) = tokio::sync::oneshot::channel();
        let handle = worker
            .submit_request(BlockingCallback { started }, HttpRequestOptions::new())
            .map_err(EnqueueError::into_error)?;
        let Some(SendCommand::Job(job)) = receiver.recv().await else {
            return Err(NetError::from(ErrorKind::Internal).into());
        };
        let callback = tokio::spawn(run_callback(CallbackJob {
            request: job.request,
            result: Err(NetError::from(ErrorKind::Cancelled)),
            request_id: job.request_id,
            control: job.control,
            registry: job.registry,
            _permit: job.permit,
            completion: job.completion,
        }));
        tokio::time::timeout(Duration::from_secs(2), started_rx).await??;
        check(
            handle.cancel() == HttpCancelOutcome::TooLate,
            "running callback remained cancellable",
        )?;
        let mut waiting = Box::pin(handle.finished());
        check(
            futures_util::poll!(waiting.as_mut()).is_pending(),
            "callback completed before exit",
        )?;
        drop(waiting);
        callback.abort();
        check(callback.await.is_err(), "callback was not aborted")?;
        let completion = tokio::time::timeout(Duration::from_secs(2), handle.finished()).await?;
        check(
            completion
                .response
                .as_ref()
                .is_err_and(|error| error.kind() == ErrorKind::Cancelled),
            "lost delivered result on abort",
        )?;
        check(
            completion
                .callback
                .as_ref()
                .is_err_and(|error| error.kind() == ErrorKind::Internal),
            "abort incorrectly reported callback success",
        )?;
        check(
            worker.registry.is_empty()? && worker.in_flight.available_permits() == 128,
            "completion preceded abort cleanup",
        )
    }

    #[tokio::test]
    async fn queue_rejection_returns_same_send_only_request_and_releases_reservation(
    ) -> Result<(), Box<dyn StdError + Send + Sync>> {
        let (worker, receiver) = paused_worker()?;
        let first = worker
            .submit_request(NoopRequest, HttpRequestOptions::new())
            .map_err(EnqueueError::into_error)?;
        let request = owned_request();
        let address = (&*request.token) as *const u64;
        let calls = Arc::clone(&request.calls);
        let drops = Arc::clone(&request.drops);
        let options = HttpRequestOptions::new().with_request_id(HttpRequestId(900));
        let rejected = worker
            .submit_request(request, options)
            .err()
            .ok_or_else(|| NetError::from(ErrorKind::Internal))?;
        check(
            rejected.error().kind() == ErrorKind::QueueFull,
            "queue error changed",
        )?;
        check(
            std::ptr::eq(&*rejected.value().token, address),
            "request ownership changed",
        )?;
        check(
            calls.load(Ordering::SeqCst) == 0 && drops.load(Ordering::SeqCst) == 0,
            "unaccepted request was called or dropped",
        )?;
        let (_, registration) = worker.reserve_request(Some(HttpRequestId(900)))?;
        drop(registration);
        drop(rejected);
        check(
            drops.load(Ordering::SeqCst) == 1,
            "request did not drop once",
        )?;
        drop(receiver);
        let completion = first.finished().await;
        check(
            completion
                .callback
                .as_ref()
                .is_err_and(|error| error.kind() == ErrorKind::Internal),
            "abandoned accepted job lacked failed completion",
        )?;
        check(
            worker.registry.is_empty()? && worker.in_flight.available_permits() == 128,
            "completion preceded resource cleanup",
        )
    }

    #[tokio::test]
    async fn closed_queue_and_duplicate_id_return_requests_without_callbacks(
    ) -> Result<(), Box<dyn StdError + Send + Sync>> {
        let (worker, receiver) = paused_worker()?;
        let id = HttpRequestId(901);
        let first = worker
            .submit_request(NoopRequest, HttpRequestOptions::new().with_request_id(id))
            .map_err(EnqueueError::into_error)?;
        let request = owned_request();
        let calls = Arc::clone(&request.calls);
        let request = worker
            .submit_request(request, HttpRequestOptions::new().with_request_id(id))
            .err()
            .ok_or_else(|| NetError::from(ErrorKind::Internal))?;
        check(
            request.error().kind() == ErrorKind::DuplicateRequestId,
            "duplicate ID error changed",
        )?;
        drop(receiver);
        first.finished().await;
        let request = worker
            .submit_request(
                request.into_inner(),
                HttpRequestOptions::new().with_request_id(id),
            )
            .err()
            .ok_or_else(|| NetError::from(ErrorKind::Internal))?;
        check(
            request.error().kind() == ErrorKind::QueueClosed,
            "closed queue error changed",
        )?;
        check(
            calls.load(Ordering::SeqCst) == 0 && worker.registry.is_empty()?,
            "rejection leaked or called back",
        )?;
        drop(request);
        Ok(())
    }

    #[tokio::test]
    async fn waiting_reservation_cancel_all_returns_request_and_drop_releases_resources(
    ) -> Result<(), Box<dyn StdError + Send + Sync>> {
        let (worker, receiver) = paused_worker()?;
        let first = worker
            .submit_request(NoopRequest, HttpRequestOptions::new())
            .map_err(EnqueueError::into_error)?;
        let request = owned_request();
        let calls = Arc::clone(&request.calls);
        let drops = Arc::clone(&request.drops);
        let mut pending = Box::pin(worker.submit_request_wait(
            request,
            HttpRequestOptions::new().with_request_id(HttpRequestId(902)),
        ));
        check(
            futures_util::poll!(pending.as_mut()).is_pending(),
            "expected waiting admission",
        )?;
        worker.cancel_all()?;
        let rejected = pending
            .await
            .err()
            .ok_or_else(|| NetError::from(ErrorKind::Internal))?;
        check(
            rejected.error().kind() == ErrorKind::Cancelled,
            "cancel_all did not cancel reservation",
        )?;
        check(
            calls.load(Ordering::SeqCst) == 0 && drops.load(Ordering::SeqCst) == 0,
            "cancelled reservation consumed request",
        )?;
        let mut pending = Box::pin(worker.submit_request_wait(
            rejected.into_inner(),
            HttpRequestOptions::new().with_request_id(HttpRequestId(902)),
        ));
        check(
            futures_util::poll!(pending.as_mut()).is_pending(),
            "expected replacement reservation",
        )?;
        drop(pending);
        check(
            drops.load(Ordering::SeqCst) == 1,
            "dropped admission did not drop request once",
        )?;
        let (_, registration) = worker.reserve_request(Some(HttpRequestId(902)))?;
        drop(registration);
        drop(receiver);
        first.finished().await;
        check(
            worker.registry.is_empty()? && worker.in_flight.available_permits() == 128,
            "reservation resources leaked",
        )
    }

    #[tokio::test(start_paused = true)]
    async fn queue_wait_timeout_returns_request_with_attempt_zero(
    ) -> Result<(), Box<dyn StdError + Send + Sync>> {
        let (worker, receiver) = paused_worker()?;
        let first = worker
            .submit_request(NoopRequest, HttpRequestOptions::new())
            .map_err(EnqueueError::into_error)?;
        let options = HttpRequestOptions::new().with_timeouts(
            crate::api::http::HttpTimeouts::new().with_admission_timeout(Duration::from_secs(2)),
        )?;
        let request = owned_request();
        let calls = Arc::clone(&request.calls);
        let mut pending = Box::pin(worker.submit_request_wait(request, options));
        check(
            futures_util::poll!(pending.as_mut()).is_pending(),
            "queue wait was not pending",
        )?;
        tokio::time::advance(Duration::from_secs(2)).await;
        let rejected = pending
            .await
            .err()
            .ok_or_else(|| NetError::from(ErrorKind::Internal))?;
        check(
            rejected.error().timeout_phase() == Some(TimeoutPhase::Admission),
            "wrong timeout phase",
        )?;
        check(
            rejected.error().context().http_attempt == Some(0),
            "admission timeout attempted transport",
        )?;
        check(
            calls.load(Ordering::SeqCst) == 0,
            "timeout called handler before admission",
        )?;
        drop(receiver);
        first.finished().await;
        check(
            worker.registry.is_empty()? && worker.in_flight.available_permits() == 128,
            "timeout leaked admission resources",
        )
    }

    #[async_trait::async_trait]
    impl HttpRequestTrait for CountingRequest {
        fn get_path(&self) -> String {
            "/".to_owned()
        }
        fn get_method(&self) -> HttpRequestMethod {
            HttpRequestMethod::GET
        }
        async fn deal_with_response(self: Box<Self>, _result: HttpResponseResult) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn closed_callback_lane_still_terminalizes_accepted_request(
    ) -> Result<(), Box<dyn StdError + Send + Sync>> {
        let (worker, _receiver) = paused_worker()?;
        let (mut job, registration) = test_job(&worker, 804)?;
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        job.request = Box::new(CountingRequest(Arc::clone(&calls)));
        registration.commit();
        let (response_tx, response_rx) = mpsc::channel(1);
        let (callback_tx, callback_rx) = mpsc::channel(1);
        drop(callback_rx);
        response_tx
            .send(ReceiveJob {
                request: job.request,
                result: Err(NetError::from(ErrorKind::Cancelled)),
                request_id: job.request_id,
                control: job.control,
                registry: job.registry,
                _permit: job.permit,
                completion: job.completion,
            })
            .await
            .map_err(|_| NetError::from(ErrorKind::Internal))?;
        drop(response_tx);
        receive_loop(response_rx, callback_tx).await;
        check(
            calls.load(Ordering::SeqCst) == 1,
            "closed callback lane lost terminal callback",
        )?;
        check(
            worker.registry.is_empty()?,
            "closed callback lane leaked registration",
        )
    }

    #[tokio::test]
    async fn closed_response_lane_still_terminalizes_accepted_request(
    ) -> Result<(), Box<dyn StdError + Send + Sync>> {
        let (worker, _receiver) = paused_worker()?;
        let (mut job, registration) = test_job(&worker, 805)?;
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        job.request = Box::new(CountingRequest(Arc::clone(&calls)));
        job.control.cancel();
        registration.commit();
        let (response_tx, response_rx) = mpsc::channel(1);
        drop(response_rx);
        process_job(
            job,
            worker.client.clone(),
            Arc::clone(&worker.config),
            response_tx,
            1,
        )
        .await;
        check(
            calls.load(Ordering::SeqCst) == 1,
            "closed response lane lost terminal callback",
        )?;
        check(
            worker.registry.is_empty()?,
            "closed response lane leaked registration",
        )
    }

    pub(super) struct NoopRequest;

    #[async_trait::async_trait]
    impl HttpRequestTrait for NoopRequest {
        fn get_path(&self) -> String {
            "/".to_owned()
        }

        fn get_method(&self) -> HttpRequestMethod {
            HttpRequestMethod::GET
        }

        async fn deal_with_response(self: Box<Self>, _result: HttpResponseResult) {}
    }

    pub(super) fn paused_worker() -> Result<(HttpClientInner, mpsc::Receiver<SendCommand>), NetError>
    {
        let config = Arc::new(HttpClientConfig::new("http://127.0.0.1:1")?);
        let (request_tx, request_rx) = mpsc::channel(1);
        Ok((
            HttpClientInner {
                network_observation: None,
                request_tx,
                supervisor_tx: mpsc::channel(1).0,
                shutdown_sent: Arc::new(AtomicBool::new(false)),
                admission_gate: Mutex::new(()),
                shutdown_notify: Arc::new(Notify::new()),
                shutdown_state: Arc::new(WorkerShutdown::new(Vec::new())),
                client: build_client(&config)?,
                config,
                registry: Arc::new(RequestRegistry::new()),
                in_flight: Arc::new(Semaphore::new(128)),
            },
            request_rx,
        ))
    }

    pub(super) fn test_job(
        worker: &HttpClientInner,
        id: u64,
    ) -> Result<(SendJob, RegistrationGuard), NetError> {
        let request_id = HttpRequestId(id);
        let (registered, registration) = worker.register_request(Some(request_id))?;
        let control = registered.control;
        Ok((
            SendJob {
                request: Box::new(NoopRequest),
                options: HttpRequestOptions::default(),
                request_id,
                control,
                registry: worker.registry(),
                permit: Arc::new(worker.try_acquire_permit()?),
                operation_start: Instant::now(),
                completion: None,
            },
            registration,
        ))
    }

    #[tokio::test]
    async fn pending_admission_cannot_commit_after_shutdown(
    ) -> Result<(), Box<dyn StdError + Send + Sync>> {
        let (worker, mut receiver) = paused_worker()?;
        let first = worker
            .submit_request(
                NoopRequest,
                HttpRequestOptions::new().with_request_id(HttpRequestId(801)),
            )
            .map_err(EnqueueError::into_error)?;
        let mut admission = Box::pin(worker.submit_request_wait(
            NoopRequest,
            HttpRequestOptions::new().with_request_id(HttpRequestId(802)),
        ));
        check(
            futures_util::poll!(admission.as_mut()).is_pending(),
            "queue was not full",
        )?;
        worker.request_shutdown();
        receiver.close();
        let result = admission.await;
        check(
            matches!(result, Err(ref error) if error.error().kind() == ErrorKind::QueueClosed),
            "pending admission committed after shutdown",
        )?;
        drop(receiver);
        first.finished().await;
        check(worker.registry.is_empty()?, "shutdown leaked a reservation")
    }

    #[test]
    fn request_shutdown_keeps_accepted_stream_registration_alive(
    ) -> Result<(), Box<dyn StdError + Send + Sync>> {
        let (worker, _receiver) = paused_worker()?;
        let request_id = HttpRequestId(806);
        let (registered, registration) = worker.registry.register(Some(request_id))?;
        registered.control.streaming.store(true, Ordering::Release);
        registration.commit();

        worker.request_shutdown();

        check(
            !registered.control.is_cancelled(),
            "request_shutdown cancelled an accepted stream",
        )?;
        check(
            !worker.registry.is_empty()?,
            "request_shutdown removed an accepted stream registration",
        )?;

        worker.registry.cancel_all()?;
        check(
            registered.control.is_cancelled(),
            "cancel_all did not cancel the accepted stream",
        )?;
        worker.registry.finish(request_id, &registered.control);
        check(
            worker.registry.is_empty()?,
            "accepted stream registration was not released",
        )
    }

    #[test]
    fn stale_registration_drop_does_not_remove_reused_id(
    ) -> Result<(), Box<dyn StdError + Send + Sync>> {
        let registry = Arc::new(RequestRegistry::new());
        let id = HttpRequestId(803);
        let (stale_control, stale) = registry.register(Some(id))?;
        registry.finish(id, &stale_control.control);
        let (replacement_control, replacement) = registry.register(Some(id))?;
        replacement.commit();
        drop(stale);
        check(
            !registry.is_empty()?,
            "stale guard removed replacement registration",
        )?;
        registry.finish(id, &replacement_control.control);
        Ok(())
    }

    pub(super) fn check(
        condition: bool,
        message: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if condition {
            Ok(())
        } else {
            Err(std::io::Error::other(message).into())
        }
    }

    #[test]
    fn uncommitted_registration_is_released_when_guard_drops(
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let registry = Arc::new(RequestRegistry::new());
        let request_id = HttpRequestId(7001);
        let (_, guard) = registry.register(Some(request_id))?;
        drop(guard);
        let (replacement_control, replacement) = registry.register(Some(request_id))?;
        replacement.commit();
        registry.finish(request_id, &replacement_control.control);
        check(
            registry.is_empty()?,
            "uncommitted registration was not released",
        )
    }

    #[test]
    fn committed_registration_survives_guard_drop_until_terminal_finish(
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let registry = Arc::new(RequestRegistry::new());
        let request_id = HttpRequestId(7002);
        let (control, guard) = registry.register(Some(request_id))?;
        guard.commit();
        drop(guard);
        check(
            !registry.is_empty()?,
            "committed registration was released too early",
        )?;
        registry.finish(request_id, &control.control);
        check(
            registry.is_empty()?,
            "terminal finish did not release registration",
        )
    }
}

#[cfg(test)]
#[path = "http_regression_tests.rs"]
mod regression_tests;

#![cfg(feature = "http-client")]

use crate::api::error::{ErrorKind, ErrorStage, NetError};
use crate::api::http::HttpClient;
use crate::module::http::http_failure::{
    map_body_error, map_reqwest_error, retry_reason_for_error,
};
use crate::module::http::network_observation::HttpNetworkObservation;
use crate::net_status::{NetworkSnapshot, NetworkStatusContext};
use crate::subscription::StateReceiver;
#[path = "http_stream_body.rs"]
mod stream_body;
use crate::api::http::{
    HttpClientConfig, HttpRequest, HttpRequestId, HttpRequestOptions, HttpRequestTrait,
    HttpResponse, HttpResponseResult, HttpStreamResponse, RetryEvent, RetryObserverHandle,
    RetryReason, RetryReport,
};
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
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use stream_body::{FusedHttpBody, StreamBudget, StreamOwner};
use tokio::sync::{mpsc, Notify, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;

pub(crate) fn create_http_client(
    thread_name: String,
    config: HttpClientConfig,
    context: NetworkStatusContext,
) -> Result<HttpClient, NetError> {
    HttpClient::new_with_network_status(config, thread_name, context)
}

pub(crate) struct SendJob {
    pub(crate) request: Box<dyn HttpRequestTrait>,
    pub(crate) options: HttpRequestOptions,
    pub(crate) request_id: HttpRequestId,
    pub(crate) control: Arc<CancellationSignal>,
    pub(crate) registry: Arc<RequestRegistry>,
    pub(crate) permit: OwnedSemaphorePermit,
    pub(crate) operation_start: Instant,
}

type ReceiveJob = CallbackJob;

struct CallbackJob {
    request: Box<dyn HttpRequestTrait>,
    result: HttpResponseResult,
    request_id: HttpRequestId,
    control: Arc<CancellationSignal>,
    registry: Arc<RequestRegistry>,
    _permit: OwnedSemaphorePermit,
}

enum SendCommand {
    Job(SendJob),
}

pub(crate) struct HttpClientInner {
    request_tx: mpsc::Sender<SendCommand>,
    shutdown_sent: AtomicBool,
    admission_gate: Mutex<()>,
    shutdown_notify: Arc<Notify>,
    shutdown_state: Arc<ShutdownState>,
    client: Client,
    config: Arc<HttpClientConfig>,
    registry: Arc<RequestRegistry>,
    in_flight: Arc<Semaphore>,
    network_observation: Option<Arc<HttpNetworkObservation>>,
}

struct ShutdownState {
    joins: Mutex<Option<Vec<JoinHandle<()>>>>,
    result: Mutex<Option<Result<(), NetError>>>,
    notify: Notify,
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
        if let Some(attempt) = self.current_attempt.take() {
            observer.notify(RetryEvent::AttemptFinished {
                attempt,
                final_attempt: true,
            });
        }
        let final_reason = match result {
            Some(Ok(status)) if status.is_success() => None,
            Some(Ok(status)) => Some(RetryReason::HttpStatus(status)),
            Some(Err(error)) => self
                .terminal_reason
                .or_else(|| Some(retry_reason_for_error(error))),
            None => self.terminal_reason.or(self.last_retry_reason),
        };
        observer.notify(RetryEvent::Completed(RetryReport {
            attempts: self.attempts,
            retry_count: self.retry_count,
            elapsed: self.operation_start.elapsed(),
            final_reason,
            exhausted,
            last_error: result
                .and_then(Result::err)
                .map(|error| format!("{:?}", error.kind())),
        }));
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
    cancelled: AtomicBool,
    notify: Notify,
}

impl CancellationSignal {
    fn new() -> Self {
        Self {
            cancelled: AtomicBool::new(false),
            notify: Notify::new(),
        }
    }

    pub(crate) fn cancel(&self) {
        if !self.cancelled.swap(true, Ordering::AcqRel) {
            self.notify.notify_waiters();
        }
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    async fn wait(&self) {
        if self.is_cancelled() {
            return;
        }
        let notified = self.notify.notified();
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
    _permit: OwnedSemaphorePermit,
}

impl Drop for StreamRegistrationLease {
    fn drop(&mut self) {
        self.registry.finish(self.request_id, &self.control);
    }
}

pub(crate) struct RegisteredRequest {
    pub(crate) id: HttpRequestId,
    pub(crate) control: Arc<CancellationSignal>,
}

struct RegistryState {
    entries: HashMap<HttpRequestId, Arc<CancellationSignal>>,
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
                next_id: Some(1),
            }),
            changed: Notify::new(),
        }
    }

    fn register(
        self: &Arc<Self>,
        requested: Option<HttpRequestId>,
    ) -> Result<(RegisteredRequest, RegistrationGuard), NetError> {
        let mut state = self.state.lock().map_err(NetError::from_poison)?;
        let request_id = match requested {
            Some(id) => {
                if state.entries.contains_key(&id) {
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
                if !state.entries.contains_key(&id) {
                    break id;
                }
            },
        };
        state.entries.try_reserve(1).map_err(|_| {
            NetError::from(ErrorKind::ResourceExhausted).with_stage(ErrorStage::Queue)
        })?;
        let signal = Arc::new(CancellationSignal::new());
        state.entries.insert(request_id, Arc::clone(&signal));
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

    fn finish(&self, request_id: HttpRequestId, control: &Arc<CancellationSignal>) {
        let removed = match self.state.lock() {
            Ok(mut state) => {
                if state
                    .entries
                    .get(&request_id)
                    .is_some_and(|entry| Arc::ptr_eq(entry, control))
                {
                    state.entries.remove(&request_id)
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
        let signals = self
            .state
            .lock()
            .map_err(NetError::from_poison)?
            .entries
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for signal in signals {
            signal.cancel();
        }
        Ok(())
    }

    fn is_empty(&self) -> Result<bool, NetError> {
        Ok(self
            .state
            .lock()
            .map_err(NetError::from_poison)?
            .entries
            .is_empty())
    }

    async fn drain(&self) -> Result<(), NetError> {
        loop {
            if self.is_empty()? {
                return Ok(());
            }
            let notified = self.changed.notified();
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
        config.validate()?;
        let client = build_client(&config)?;
        let network_observation = context.map(HttpNetworkObservation::new).transpose()?;
        let (request_tx, request_rx) = mpsc::channel(config.request_queue_capacity);
        let (response_tx, response_rx) = mpsc::channel(config.response_queue_capacity);
        let (callback_tx, callback_rx) = mpsc::channel(config.callback_queue_capacity);
        let config = Arc::new(config);
        let in_flight = Arc::new(Semaphore::new(config.request_queue_capacity));
        let shutdown_notify = Arc::new(Notify::new());

        let callback_name = format!("{thread_name}-callback");
        let receive_name = format!("{thread_name}-recv");
        let send_name = thread_name;

        let (callback_thread, callback_ready) = spawn_runtime(callback_name, move || {
            Box::pin(async move { callback_loop(callback_rx).await })
        })?;
        if let Err(error) = wait_ready(callback_ready) {
            drop(callback_tx);
            let _ = callback_thread.join();
            return Err(error);
        }

        let callback_tx_for_receive = callback_tx.clone();
        let (receive_thread, receive_ready) = match spawn_runtime(receive_name, move || {
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
        let (send_thread, send_ready) = match spawn_runtime(send_name, move || {
            Box::pin(async move {
                send_loop(
                    request_rx,
                    response_tx,
                    send_client,
                    send_config,
                    send_shutdown_notify,
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
            shutdown_sent: AtomicBool::new(false),
            admission_gate: Mutex::new(()),
            shutdown_notify,
            shutdown_state: Arc::new(ShutdownState {
                joins: Mutex::new(Some(vec![send_thread, receive_thread, callback_thread])),
                result: Mutex::new(None),
                notify: Notify::new(),
            }),
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

    pub(crate) fn registry(&self) -> Arc<RequestRegistry> {
        Arc::clone(&self.registry)
    }

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

    pub(crate) async fn submit_wait(&self, job: SendJob) -> Result<(), NetError> {
        if self.shutdown_sent.load(Ordering::Acquire) {
            return Err(NetError::from(ErrorKind::QueueClosed).with_stage(ErrorStage::Queue));
        }
        let permit =
            self.request_tx.reserve().await.map_err(|_| {
                NetError::from(ErrorKind::QueueClosed).with_stage(ErrorStage::Queue)
            })?;
        // Never hold the lifecycle gate while waiting for queue capacity.
        let _admission = self.admission_gate.lock().map_err(NetError::from_poison)?;
        if self.shutdown_sent.load(Ordering::Acquire) {
            return Err(NetError::from(ErrorKind::QueueClosed).with_stage(ErrorStage::Queue));
        }
        permit.send(SendCommand::Job(job));
        Ok(())
    }

    pub(crate) async fn stream_request(
        &self,
        request: HttpRequest,
    ) -> Result<HttpStreamResponse, NetError> {
        let operation_start = Instant::now();
        let policy = request
            .retry_setting()
            .resolve(&self.config.default_retry_policy);
        let mut owner = StreamOwner::new(&policy, operation_start);
        let result = self
            .open_stream(request, &policy, operation_start, &mut owner)
            .await;
        match result {
            Ok(response) => Ok(response),
            Err(error) => {
                owner.complete(Err(&error), false);
                Err(error)
            }
        }
    }

    async fn open_stream(
        &self,
        request: HttpRequest,
        policy: &crate::api::http::RetryPolicy,
        operation_start: Instant,
        owner: &mut StreamOwner,
    ) -> Result<HttpStreamResponse, NetError> {
        let total = StreamBudget::total(operation_start, policy.total_deadline())?;
        let permit = match total.deadline() {
            Some(deadline) => tokio::time::timeout_at(deadline.into(), self.acquire_permit())
                .await
                .map_err(|_| {
                    NetError::from(ErrorKind::DeadlineExceeded).with_stage(ErrorStage::Queue)
                })??,
            None => self.acquire_permit().await?,
        };
        owner.permit = Some(permit);
        if let Some(error) = total.expired(Instant::now(), ErrorStage::Queue) {
            return Err(error);
        }
        let (registered, registration) = self.register_request(None)?;
        let request_id = registered.id;
        let control = registered.control;
        let permit = owner
            .permit
            .take()
            .ok_or_else(|| NetError::from(ErrorKind::Internal).with_stage(ErrorStage::Queue))?;
        owner.lease = Some(StreamRegistrationLease {
            registry: self.registry(),
            request_id,
            control: Arc::clone(&control),
            _permit: permit,
        });
        registration.commit();
        let method = request.method.clone();
        let retry_allowed = policy.max_retries() > 0 && policy.allows_method(&method, true);
        let mut attempt = 1_u32;
        loop {
            check_stream_start(total, &control)?;
            if let Some(lifecycle) = owner.lifecycle.as_mut() {
                lifecycle.attempt_started(attempt);
            }
            check_stream_start(total, &control)?;
            let built = build_spec_request(&request, &self.config, &self.client, &method)?;
            check_stream_start(total, &control)?;
            let budget = total.attempt(
                Instant::now(),
                policy.attempt_timeout(),
                self.config.timeout,
            )?;
            let mut timer = budget.timer();
            let result = tokio::select! {
                _ = control.wait() => Err(NetError::from(ErrorKind::Cancelled).with_stage(ErrorStage::Dispatch)),
                _ = wait_budget(&mut timer) => Err(budget.expired(Instant::now(), ErrorStage::Receive)
                    .map_or_else(|| NetError::from(ErrorKind::TimedOut).with_stage(ErrorStage::Receive), |error| error)),
                response = self.client.execute(built) => {
                    match budget.expired(Instant::now(), ErrorStage::Receive) {
                        Some(error) => Err(error),
                        None => response.map_err(|error| map_reqwest_error(error, ErrorStage::Receive)),
                    }
                }
            };
            let retry = match &result {
                Ok(response)
                    if retry_allowed
                        && policy.can_retry_attempt(attempt)
                        && policy.should_retry_status(response.status()) =>
                {
                    Some((
                        RetryReason::HttpStatus(response.status()),
                        policy.retry_delay(attempt, parse_retry_after(response.headers())),
                    ))
                }
                Err(error)
                    if retry_allowed
                        && policy.can_retry_attempt(attempt)
                        && policy.should_retry_error(error) =>
                {
                    Some((
                        retry_reason_for_error(error),
                        policy.retry_delay(attempt, None),
                    ))
                }
                _ => None,
            };
            if let Some((reason, delay)) = retry {
                drop(result);
                drop(timer);
                if let Some(lifecycle) = owner.lifecycle.as_mut() {
                    lifecycle.retry_scheduled(attempt, delay, reason);
                }
                check_stream_start(total, &control)?;
                if !wait_for_retry(delay, total.deadline(), &control).await {
                    check_stream_start(total, &control)?;
                    return Err(
                        NetError::from(ErrorKind::DeadlineExceeded).with_stage(ErrorStage::Receive)
                    );
                }
                attempt = attempt.saturating_add(1);
                continue;
            }
            let response = match result {
                Ok(response) => response,
                Err(error) => {
                    drop(timer);
                    owner.complete(Err(&error), policy.max_attempts() == attempt);
                    return Err(error);
                }
            };
            let status = response.status();
            let headers = response.headers().clone();
            let body = Box::pin(
                response
                    .bytes_stream()
                    .map(|chunk| chunk.map_err(map_body_error)),
            );
            let body_owner = StreamOwner {
                lease: owner.lease.take(),
                permit: owner.permit.take(),
                lifecycle: owner.lifecycle.take(),
            };
            let stream = FusedHttpBody::new(
                body,
                timer,
                control,
                budget,
                body_owner,
                status,
                policy.max_attempts() == attempt,
            );
            return Ok(HttpStreamResponse {
                status,
                headers,
                request_id,
                attempts: attempt,
                stream: Box::pin(stream),
            });
        }
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
        let joins = self.shutdown_state.joins.lock().map_or_else(
            |poisoned| poisoned.into_inner().take(),
            |mut joins| joins.take(),
        );
        if let Some(joins) = joins {
            let state = Arc::clone(&self.shutdown_state);
            let joined = tokio::task::spawn_blocking(move || {
                let mut first_error = None;
                for join in joins {
                    if join.join().is_err() && first_error.is_none() {
                        first_error =
                            Some(NetError::from(ErrorKind::Internal).with_stage(ErrorStage::Close));
                    }
                }
                let result = first_error.map_or(Ok(()), Err);
                publish_shutdown_result(&state, &result);
                result
            })
            .await;
            return match joined {
                Ok(result) => result,
                Err(_) => {
                    let result =
                        Err(NetError::from(ErrorKind::Internal).with_stage(ErrorStage::Close));
                    publish_shutdown_result(&self.shutdown_state, &result);
                    result
                }
            };
        }
        loop {
            if let Some(result) = self
                .shutdown_state
                .result
                .lock()
                .map_err(NetError::from_poison)?
                .clone()
            {
                return result;
            }
            let notified = self.shutdown_state.notify.notified();
            if let Some(result) = self
                .shutdown_state
                .result
                .lock()
                .map_err(NetError::from_poison)?
                .clone()
            {
                return result;
            }
            notified.await;
        }
    }
}

fn publish_shutdown_result(state: &ShutdownState, result: &Result<(), NetError>) {
    match state.result.lock() {
        Ok(mut stored) => *stored = Some(result.clone()),
        Err(poisoned) => *poisoned.into_inner() = Some(result.clone()),
    }
    state.notify.notify_waiters();
}

impl Drop for HttpClientInner {
    fn drop(&mut self) {
        if let Err(error) = self.cancel_all() {
            crate::log_e!(
                crate::LogType::HTTP;
                "drop",
                "error",
                format!("request_cancellation_failed:{:?}", error.kind())
            );
        }
        self.request_shutdown();
        let joins = match self.shutdown_state.joins.lock() {
            Ok(mut joins) => joins.take(),
            Err(poisoned) => {
                crate::log_e!(
                    crate::LogType::HTTP;
                    "drop",
                    "error",
                    "worker_join_registry_poisoned"
                );
                poisoned.into_inner().take()
            }
        };
        let Some(joins) = joins else {
            return;
        };
        let cleanup = Arc::new(Mutex::new(Some(joins)));
        let thread_cleanup = Arc::clone(&cleanup);
        let spawned = thread::Builder::new()
            .name("open-net-http-destroy-cleanup".to_owned())
            .spawn(move || {
                if let Some(joins) = thread_cleanup
                    .lock()
                    .ok()
                    .and_then(|mut pending| pending.take())
                {
                    join_worker_threads(joins);
                }
            });
        if spawned.is_err() {
            if let Some(joins) = cleanup.lock().ok().and_then(|mut pending| pending.take()) {
                join_worker_threads(joins);
            }
        }
    }
}

fn join_worker_threads(joins: Vec<JoinHandle<()>>) {
    for join in joins {
        if join.join().is_err() {
            crate::log_e!(
                crate::LogType::HTTP;
                "worker_cleanup",
                "error",
                "worker_thread_failed"
            );
        }
    }
}

fn build_client(config: &HttpClientConfig) -> Result<Client, NetError> {
    let builder = Client::builder()
        .use_rustls_tls()
        .timeout(config.timeout)
        .tcp_keepalive(config.tcp_keepalive)
        .default_headers(config.common_headers.clone());
    let mut builder = config.tls.configure_builder(builder)?;
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
    builder
        .build()
        .map_err(|error| map_reqwest_error(error, ErrorStage::Configuration))
}

type RuntimeReady = (
    JoinHandle<()>,
    std::sync::mpsc::Receiver<Result<(), NetError>>,
);

fn spawn_runtime<F>(name: String, task: F) -> Result<RuntimeReady, NetError>
where
    F: FnOnce() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + 'static,
{
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let thread = thread::Builder::new()
        .name(name)
        .spawn(move || {
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
        })
        .map_err(|error| {
            NetError::with_source(ErrorKind::RuntimeUnavailable, error)
                .with_stage(ErrorStage::Runtime)
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
    response_tx: mpsc::Sender<ReceiveJob>,
    client: Client,
    config: Arc<HttpClientConfig>,
    shutdown_notify: Arc<Notify>,
) {
    let mut jobs = JoinSet::new();
    let mut accepting = true;
    while accepting {
        tokio::select! {
            _ = shutdown_notify.notified() => accepting = false,
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
    while let Some(SendCommand::Job(job)) = request_rx.recv().await {
        let client = client.clone();
        let config = Arc::clone(&config);
        let response_tx = response_tx.clone();
        jobs.spawn(async move {
            process_job(job, client, config, response_tx, 1).await;
        });
    }
    while let Some(completed) = jobs.join_next().await {
        if completed.is_err() {
            crate::log_e!(crate::LogType::HTTP; "send_loop", "error", "request_task_failed");
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
    let response = ReceiveJob {
        request: job.request,
        result,
        request_id: job.request_id,
        control: job.control,
        registry: job.registry,
        _permit: job.permit,
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
    let method: Method = job.request.get_method().into();
    let replayable = job.options.replayable_body;
    let mut policy = job.options.retry_policy.clone();
    if job.options.use_default_retry_policy && policy.max_retries() == 0 {
        policy = config.default_retry_policy.clone();
    }
    let retry_allowed = policy.max_retries() > 0 && policy.allows_method(&method, replayable);
    let operation_start = job.operation_start;
    let mut lifecycle = RetryObserverLifecycle::new(policy.observer(), operation_start);
    let operation_deadline = match policy.total_deadline() {
        Some(duration) => match operation_start.checked_add(duration) {
            Some(deadline) => Some(deadline),
            None => {
                return terminal_error(
                    &mut lifecycle,
                    NetError::from(ErrorKind::InvalidPolicy).with_stage(ErrorStage::Configuration),
                )
            }
        },
        None => None,
    };
    let mut attempt = initial_attempt.max(1);
    loop {
        if job.control.is_cancelled() {
            return terminal_error(
                &mut lifecycle,
                NetError::from(ErrorKind::Cancelled).with_stage(ErrorStage::Dispatch),
            );
        }
        if operation_deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return terminal_error(
                &mut lifecycle,
                NetError::from(ErrorKind::DeadlineExceeded).with_stage(ErrorStage::Receive),
            );
        }
        if attempt > 1 && !replayable {
            return terminal_error(
                &mut lifecycle,
                NetError::from(ErrorKind::BodyNotReplayable).with_stage(ErrorStage::RequestBuild),
            );
        }
        lifecycle.attempt_started(attempt);
        let (request, _has_body) =
            match build_request(job.request.as_ref(), config, client, &method) {
                Ok(request) => request,
                Err(error) => return terminal_error(&mut lifecycle, error),
            };
        if job.control.is_cancelled() {
            return terminal_error(
                &mut lifecycle,
                NetError::from(ErrorKind::Cancelled).with_stage(ErrorStage::Dispatch),
            );
        }
        if operation_deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return terminal_error(
                &mut lifecycle,
                NetError::from(ErrorKind::DeadlineExceeded).with_stage(ErrorStage::Receive),
            );
        }
        let attempt_started = Instant::now();
        let attempt_timeout = remaining_timeout(&policy, operation_deadline);
        let response = tokio::select! {
            _ = job.control.wait() => Err(NetError::from(ErrorKind::Cancelled).with_stage(ErrorStage::Dispatch)),
            response = execute_with_timeout(client, request, attempt_timeout) => response,
        };
        let result = match response {
            Ok(response) => {
                if retry_allowed
                    && policy.can_retry_attempt(attempt)
                    && policy.should_retry_status(response.status())
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
                    let delay = policy.retry_delay(attempt, retry_after);
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
                                .with_stage(ErrorStage::Receive),
                        );
                    }
                    attempt = attempt.saturating_add(1);
                    continue;
                }
                let read_timeout = attempt_timeout
                    .map(|duration| duration.saturating_sub(attempt_started.elapsed()));
                read_response_with_timeout(
                    response,
                    attempt,
                    job.request_id,
                    config.max_response_bytes,
                    Arc::clone(&job.control),
                    read_timeout,
                )
                .await
            }
            Err(error) => Err(error),
        };
        if let Ok(buffered) = &result {
            if retry_allowed
                && policy.can_retry_attempt(attempt)
                && policy.should_retry_application(buffered)
            {
                if !replayable {
                    lifecycle.note_reason(RetryReason::Application);
                    return terminal_error(
                        &mut lifecycle,
                        NetError::from(ErrorKind::BodyNotReplayable)
                            .with_stage(ErrorStage::RequestBuild),
                    );
                }
                let delay = policy.retry_delay(attempt, None);
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
                        NetError::from(ErrorKind::DeadlineExceeded).with_stage(ErrorStage::Receive),
                    );
                }
                attempt = attempt.saturating_add(1);
                continue;
            }
        }
        if let Err(error) = &result {
            if retry_allowed
                && policy.can_retry_attempt(attempt)
                && policy.should_retry_error(error)
            {
                if !replayable {
                    lifecycle.note_reason(retry_reason_for_error(error));
                    return terminal_error(
                        &mut lifecycle,
                        NetError::from(ErrorKind::BodyNotReplayable)
                            .with_stage(ErrorStage::RequestBuild),
                    );
                }
                let delay = policy.retry_delay(attempt, None);
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
                        NetError::from(ErrorKind::DeadlineExceeded).with_stage(ErrorStage::Receive),
                    );
                }
                attempt = attempt.saturating_add(1);
                continue;
            }
        }
        lifecycle.complete(&result, policy.max_attempts() == attempt);
        return result;
    }
}

fn terminal_error(lifecycle: &mut RetryObserverLifecycle, error: NetError) -> HttpResponseResult {
    let result = Err(error);
    lifecycle.complete(&result, false);
    result
}

fn build_request(
    request: &dyn HttpRequestTrait,
    config: &HttpClientConfig,
    client: &Client,
    method: &Method,
) -> Result<(reqwest::Request, bool), NetError> {
    let path = request.get_path();
    let url = config.resolve_request_path(path.as_str())?;
    let headers = config.merge_headers(&request.headers())?;
    // Materialize the provider exactly once for this attempt.  The presence
    // hook is evaluated only after materialization and its default is
    // side-effect free, so a one-shot provider cannot be consumed twice.
    let body = request.get_req_body();
    let body_is_empty = body.is_empty();
    let body_present = !body_is_empty || request.has_req_body();
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
    Ok((built, body_present))
}

fn build_spec_request(
    request: &HttpRequest,
    config: &HttpClientConfig,
    client: &Client,
    method: &Method,
) -> Result<reqwest::Request, NetError> {
    let url = config.resolve_request_path(request.path.as_str())?;
    let headers = config.merge_headers(&request.headers)?;
    let mut builder = client.request(method.clone(), url).headers(headers);
    if !request.body.is_empty() || matches!(*method, Method::POST | Method::PUT | Method::PATCH) {
        builder = builder.body(request.body.clone());
        if request.body.is_empty() {
            builder = builder.header(http::header::CONTENT_LENGTH, "0");
        }
    }
    builder
        .build()
        .map_err(|error| map_reqwest_error(error, ErrorStage::RequestBuild))
}

fn check_stream_start(budget: StreamBudget, control: &CancellationSignal) -> Result<(), NetError> {
    if let Some(error) = budget.expired(Instant::now(), ErrorStage::Receive) {
        return Err(error);
    }
    if control.is_cancelled() {
        return Err(NetError::from(ErrorKind::Cancelled).with_stage(ErrorStage::Dispatch));
    }
    Ok(())
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

fn remaining_timeout(
    policy: &crate::api::http::RetryPolicy,
    deadline: Option<Instant>,
) -> Option<Duration> {
    let deadline_remaining = deadline.map(|limit| {
        limit
            .checked_duration_since(Instant::now())
            .map_or(Duration::ZERO, |remaining| remaining)
    });
    match (policy.attempt_timeout(), deadline_remaining) {
        (Some(attempt), Some(total)) => Some(attempt.min(total)),
        (Some(attempt), None) => Some(attempt),
        (None, Some(total)) => Some(total),
        (None, None) => None,
    }
}

async fn execute_with_timeout(
    client: &Client,
    request: reqwest::Request,
    timeout: Option<Duration>,
) -> Result<Response, NetError> {
    let future = client.execute(request);
    let result = match timeout {
        Some(duration) => tokio::time::timeout(duration, future)
            .await
            .map_err(|_| NetError::from(ErrorKind::TimedOut).with_stage(ErrorStage::Receive))?,
        None => future.await,
    };
    result.map_err(|error| map_reqwest_error(error, ErrorStage::Receive))
}

async fn read_response_with_timeout(
    response: Response,
    attempts: u32,
    request_id: HttpRequestId,
    max_response_bytes: usize,
    control: Arc<CancellationSignal>,
    timeout: Option<Duration>,
) -> HttpResponseResult {
    let future = read_response(response, attempts, request_id, max_response_bytes, control);
    match timeout {
        Some(duration) => tokio::time::timeout(duration, future)
            .await
            .map_err(|_| NetError::from(ErrorKind::TimedOut).with_stage(ErrorStage::Receive))?,
        None => future.await,
    }
}

fn parse_retry_after(headers: &http::HeaderMap) -> Option<Duration> {
    let value = headers.get(http::header::RETRY_AFTER)?.to_str().ok()?;
    let value = value.trim();
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    let date = chrono::DateTime::parse_from_rfc2822(value)
        .ok()?
        .with_timezone(&chrono::Utc);
    let now = chrono::Utc::now();
    if date <= now {
        return Some(Duration::ZERO);
    }
    (date - now).to_std().ok()
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
) -> HttpResponseResult {
    let status = response.status();
    let headers = response.headers().clone();
    let mut stream = response.bytes_stream();
    let mut body = BytesMut::new();
    while let Some(chunk) = tokio::select! {
        _ = control.wait() => {
            return Err(NetError::from(ErrorKind::Cancelled).with_stage(ErrorStage::Receive));
        }
        chunk = stream.next() => chunk,
    } {
        let chunk = chunk.map_err(map_body_error)?;
        if body.len().saturating_add(chunk.len()) > max_response_bytes {
            return Err(NetError::from(ErrorKind::ItemTooLarge).with_stage(ErrorStage::Receive));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(HttpResponse::new(
        status,
        headers,
        body.freeze(),
        attempts,
        request_id,
    ))
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
    // This guard also releases the registry if callback dispatch itself unwinds.
    let _completion = RegistrationGuard {
        registry: job.registry,
        request_id: job.request_id,
        control: Arc::clone(&job.control),
        committed: AtomicBool::new(false),
    };
    let result = if job.control.is_cancelled() {
        Err(NetError::from(ErrorKind::Cancelled).with_stage(ErrorStage::Dispatch))
    } else {
        job.result
    };
    let request = job.request;
    if let Ok(response) = &result {
        if std::panic::catch_unwind(AssertUnwindSafe(|| {
            request.on_response_headers(&response.headers);
        }))
        .is_err()
        {
            crate::log_e!(crate::LogType::HTTP; "callback_loop", "error", "header_callback_panicked");
        }
    }
    // Catch both synchronous future construction and asynchronous callback execution.
    if AssertUnwindSafe(async move { request.deal_with_response(result).await })
        .catch_unwind()
        .await
        .is_err()
    {
        crate::log_e!(crate::LogType::HTTP; "callback_loop", "error", "callback_panicked");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::http::HttpRequestMethod;

    struct CountingRequest(Arc<std::sync::atomic::AtomicUsize>);

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
                shutdown_sent: AtomicBool::new(false),
                admission_gate: Mutex::new(()),
                shutdown_notify: Arc::new(Notify::new()),
                shutdown_state: Arc::new(ShutdownState {
                    joins: Mutex::new(None),
                    result: Mutex::new(Some(Ok(()))),
                    notify: Notify::new(),
                }),
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
                permit: worker.try_acquire_permit()?,
                operation_start: Instant::now(),
            },
            registration,
        ))
    }

    #[tokio::test]
    async fn pending_admission_cannot_commit_after_shutdown(
    ) -> Result<(), Box<dyn StdError + Send + Sync>> {
        let (worker, mut receiver) = paused_worker()?;
        let (first, _first_registration) = test_job(&worker, 801)?;
        worker.submit(first)?;
        let (pending, _pending_registration) = test_job(&worker, 802)?;
        let mut admission = Box::pin(worker.submit_wait(pending));
        check(
            futures_util::poll!(admission.as_mut()).is_pending(),
            "queue was not full",
        )?;
        worker.request_shutdown();
        receiver.close();
        let result = admission.await;
        check(
            matches!(result, Err(ref error) if error.kind() == ErrorKind::QueueClosed),
            "pending admission committed after shutdown",
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

    #[test]
    fn retry_after_accepts_delta_seconds_and_http_date(
    ) -> Result<(), Box<dyn StdError + Send + Sync>> {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::header::RETRY_AFTER,
            http::HeaderValue::from_static("3"),
        );
        check(
            parse_retry_after(&headers) == Some(Duration::from_secs(3)),
            "delta-seconds Retry-After was not parsed",
        )?;

        let future = (chrono::Utc::now() + chrono::Duration::seconds(2)).to_rfc2822();
        let value = http::HeaderValue::from_str(future.as_str())
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        headers.insert(http::header::RETRY_AFTER, value);
        let delay = parse_retry_after(&headers)
            .ok_or_else(|| std::io::Error::other("HTTP-date Retry-After was not parsed"))?;
        check(
            delay <= Duration::from_secs(2) && delay >= Duration::from_millis(500),
            "HTTP-date Retry-After produced an unexpected delay",
        )
    }
}

#[cfg(test)]
#[path = "http_regression_tests.rs"]
mod regression_tests;

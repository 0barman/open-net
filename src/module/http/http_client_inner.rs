#![cfg(feature = "http-client")]

use crate::api::error::{ErrorKind, ErrorStage, NetError};
use crate::api::http::HttpClient;
use crate::api::http::{
    HttpClientConfig, HttpRequest, HttpRequestId, HttpRequestOptions, HttpRequestTrait,
    HttpResponse, HttpResponseResult, HttpStreamResponse,
};
use bytes::BytesMut;
use futures_util::{FutureExt, StreamExt};
use http::Method;
use reqwest::{Client, Response};
use std::collections::HashMap;
use std::error::Error as StdError;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinSet;

pub(crate) fn create_http_client(
    thread_name: String,
    config: HttpClientConfig,
) -> Result<HttpClient, NetError> {
    HttpClient::new(config, thread_name)
}

pub(crate) struct SendJob {
    pub(crate) request: Box<dyn HttpRequestTrait>,
    pub(crate) options: HttpRequestOptions,
    pub(crate) request_id: HttpRequestId,
    pub(crate) control: Arc<CancellationSignal>,
    pub(crate) registry: Arc<RequestRegistry>,
}

type ReceiveJob = CallbackJob;

struct CallbackJob {
    request: Box<dyn HttpRequestTrait>,
    result: HttpResponseResult,
    request_id: HttpRequestId,
    control: Arc<CancellationSignal>,
    registry: Arc<RequestRegistry>,
}

enum SendCommand {
    Job(SendJob),
}

pub(crate) struct HttpClientInner {
    request_tx: mpsc::Sender<SendCommand>,
    shutdown_sent: AtomicBool,
    admission_gate: Mutex<()>,
    shutdown_notify: Arc<Notify>,
    next_request_id: AtomicU64,
    shutdown_state: Arc<ShutdownState>,
    client: Client,
    config: Arc<HttpClientConfig>,
    registry: Arc<RequestRegistry>,
}

struct ShutdownState {
    joins: Mutex<Option<Vec<JoinHandle<()>>>>,
    result: Mutex<Option<Result<(), NetError>>>,
    notify: Notify,
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

    fn cancel(&self) {
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

pub(crate) struct RequestRegistry {
    entries: Mutex<HashMap<HttpRequestId, Arc<CancellationSignal>>>,
    changed: Notify,
}

impl RequestRegistry {
    fn new() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            changed: Notify::new(),
        }
    }

    fn register(
        self: &Arc<Self>,
        request_id: HttpRequestId,
    ) -> Result<(Arc<CancellationSignal>, RegistrationGuard), NetError> {
        let mut entries = self.entries.lock().map_err(NetError::from_poison)?;
        if entries.contains_key(&request_id) {
            return Err(NetError::from(ErrorKind::DuplicateRequestId).with_stage(ErrorStage::Queue));
        }
        let signal = Arc::new(CancellationSignal::new());
        entries.insert(request_id, Arc::clone(&signal));
        let guard = RegistrationGuard {
            registry: Arc::clone(self),
            request_id,
            control: Arc::clone(&signal),
            committed: AtomicBool::new(false),
        };
        Ok((signal, guard))
    }

    fn finish(&self, request_id: HttpRequestId, control: &Arc<CancellationSignal>) {
        match self.entries.lock() {
            Ok(mut entries) => {
                if entries
                    .get(&request_id)
                    .is_some_and(|entry| Arc::ptr_eq(entry, control))
                {
                    entries.remove(&request_id);
                    self.changed.notify_waiters();
                }
            }
            Err(_) => {
                crate::log_e!(crate::LogType::HTTP; "request_registry", "error", "registry_poisoned");
            }
        }
    }

    fn cancel(&self, request_id: HttpRequestId) -> Result<(), NetError> {
        let signal = self
            .entries
            .lock()
            .map_err(NetError::from_poison)?
            .get(&request_id)
            .cloned();
        if let Some(signal) = signal {
            signal.cancel();
        }
        Ok(())
    }

    fn cancel_all(&self) -> Result<(), NetError> {
        let signals = self
            .entries
            .lock()
            .map_err(NetError::from_poison)?
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
            .entries
            .lock()
            .map_err(NetError::from_poison)?
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
        config.validate()?;
        let client = build_client(&config)?;
        let (request_tx, request_rx) = mpsc::channel(config.request_queue_capacity);
        let (response_tx, response_rx) = mpsc::channel(config.response_queue_capacity);
        let (callback_tx, callback_rx) = mpsc::channel(config.callback_queue_capacity);
        let config = Arc::new(config);
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
            next_request_id: AtomicU64::new(1),
            shutdown_state: Arc::new(ShutdownState {
                joins: Mutex::new(Some(vec![send_thread, receive_thread, callback_thread])),
                result: Mutex::new(None),
                notify: Notify::new(),
            }),
            client,
            config,
            registry: Arc::new(RequestRegistry::new()),
        }))
    }

    pub(crate) fn allocate_request_id(&self) -> HttpRequestId {
        HttpRequestId(self.next_request_id.fetch_add(1, Ordering::Relaxed))
    }

    pub(crate) fn registry(&self) -> Arc<RequestRegistry> {
        Arc::clone(&self.registry)
    }

    pub(crate) fn register_request(
        &self,
        request_id: HttpRequestId,
    ) -> Result<(Arc<CancellationSignal>, RegistrationGuard), NetError> {
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
        let _admission = self.admission_gate.lock().map_err(NetError::from_poison)?;
        if self.shutdown_sent.load(Ordering::Acquire) {
            return Err(NetError::from(ErrorKind::QueueClosed).with_stage(ErrorStage::Queue));
        }
        self.request_tx
            .try_send(SendCommand::Job(job))
            .map_err(|error| {
                let kind = match error {
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
        let request_id = self.allocate_request_id();
        let method = request.method.clone();
        let built = build_spec_request(&request, &self.config, &self.client, &method)?;
        let response = self
            .client
            .execute(built)
            .await
            .map_err(|error| map_reqwest_error(error, ErrorStage::Receive))?;
        let status = response.status();
        let headers = response.headers().clone();
        let stream = response
            .bytes_stream()
            .map(|chunk| chunk.map_err(|error| map_reqwest_error(error, ErrorStage::Receive)));
        Ok(HttpStreamResponse {
            status,
            headers,
            request_id,
            attempts: 1,
            stream: Box::pin(stream),
        })
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
        if self.shutdown_sent.swap(true, Ordering::AcqRel) {
            return;
        }
        self.shutdown_notify.notify_one();
    }

    pub(crate) async fn shutdown_and_join(&self) -> Result<(), NetError> {
        self.request_shutdown();
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
    let method = job.request.get_method();
    let replayable = job.options.replayable_body;
    let mut policy = job.options.retry_policy.clone();
    if job.options.use_default_retry_policy
        && policy.max_retries() == 0
        && config.default_retry_policy.max_retries() > 0
    {
        policy = config.default_retry_policy.clone();
    }
    let retry_allowed = policy.allows_method(&method, replayable);
    let mut attempt = initial_attempt.max(1);
    loop {
        if job.control.is_cancelled() {
            return Err(NetError::from(ErrorKind::Cancelled).with_stage(ErrorStage::Dispatch));
        }
        let request = build_request(job.request.as_ref(), config, client, &method)?;
        let response = tokio::select! {
            _ = job.control.wait() => Err(NetError::from(ErrorKind::Cancelled).with_stage(ErrorStage::Dispatch)),
            response = client.execute(request) => response.map_err(|error| map_reqwest_error(error, ErrorStage::Receive)),
        };
        let result = match response {
            Ok(response) => {
                if retry_allowed
                    && policy.can_retry_attempt(attempt)
                    && policy.should_retry_status(response.status())
                {
                    drop(response);
                    if !wait_before_retry(&policy, attempt, &job.control).await {
                        return Err(
                            NetError::from(ErrorKind::Cancelled).with_stage(ErrorStage::Dispatch)
                        );
                    }
                    attempt = attempt.saturating_add(1);
                    continue;
                }
                read_response(
                    response,
                    attempt,
                    job.request_id,
                    config.max_response_bytes,
                    Arc::clone(&job.control),
                )
                .await
            }
            Err(error) => Err(error),
        };
        if let Err(error) = &result {
            if retry_allowed
                && policy.can_retry_attempt(attempt)
                && policy.should_retry_error(error)
            {
                if !wait_before_retry(&policy, attempt, &job.control).await {
                    return Err(
                        NetError::from(ErrorKind::Cancelled).with_stage(ErrorStage::Receive)
                    );
                }
                attempt = attempt.saturating_add(1);
                continue;
            }
        }
        return result;
    }
}

fn build_request(
    request: &dyn HttpRequestTrait,
    config: &HttpClientConfig,
    client: &Client,
    method: &Method,
) -> Result<reqwest::Request, NetError> {
    let path = request.get_path();
    let url = config.resolve_request_path(path.as_str())?;
    let headers = config.merge_headers(&request.headers())?;
    let body = request.get_req_body();
    let body_is_empty = body.is_empty();
    let mut builder = client.request(method.clone(), url);
    builder = builder.headers(headers);
    if request.has_req_body() || matches!(*method, Method::POST | Method::PUT | Method::PATCH) {
        builder = builder.body(body);
        if body_is_empty {
            builder = builder.header(http::header::CONTENT_LENGTH, "0");
        }
    }
    builder
        .build()
        .map_err(|error| map_reqwest_error(error, ErrorStage::RequestBuild))
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

async fn wait_before_retry(
    policy: &crate::api::http::RetryPolicy,
    attempt: u32,
    control: &CancellationSignal,
) -> bool {
    let exponent = attempt.saturating_sub(1).min(6);
    let multiplier = 1_u64 << exponent;
    let millis = 50_u64.saturating_mul(multiplier);
    let delay = Duration::from_millis(millis).min(policy.max_delay());
    tokio::select! {
        _ = control.wait() => false,
        _ = tokio::time::sleep(delay) => true,
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
        let chunk = chunk.map_err(|error| map_reqwest_error(error, ErrorStage::Receive))?;
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

fn map_reqwest_error(error: reqwest::Error, stage: ErrorStage) -> NetError {
    let detail = error.to_string().to_ascii_lowercase();
    let body_failure = classify_body_failure(&error);
    let is_dns = error.is_connect()
        && (detail.contains("dns")
            || detail.contains("resolve")
            || detail.contains("name or service")
            || detail.contains("getaddrinfo")
            || detail.contains("lookup address"));
    let is_tls = error.is_connect()
        && (detail.contains("tls")
            || detail.contains("certificate")
            || detail.contains("cert")
            || detail.contains("handshake"));
    let is_proxy = detail.contains("proxy");
    let (kind, mapped_stage) = if error.is_timeout() {
        (ErrorKind::TimedOut, stage)
    } else if is_dns {
        (ErrorKind::Dns, ErrorStage::Dns)
    } else if is_tls {
        (ErrorKind::Tls, ErrorStage::Tls)
    } else if body_failure == BodyFailure::Protocol {
        (ErrorKind::Protocol, ErrorStage::Receive)
    } else if body_failure == BodyFailure::Transport {
        (ErrorKind::Io, stage)
    } else if error.is_decode() {
        (ErrorKind::Protocol, ErrorStage::Receive)
    } else if error.is_connect() || error.is_body() {
        let mapped_stage = if is_proxy { ErrorStage::Proxy } else { stage };
        (ErrorKind::Io, mapped_stage)
    } else if error.is_request() {
        (ErrorKind::Io, stage)
    } else if error.is_builder() {
        (ErrorKind::InvalidInput, ErrorStage::RequestBuild)
    } else {
        (ErrorKind::Protocol, stage)
    };
    NetError::with_source(kind, error).with_stage(mapped_stage)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BodyFailure {
    Transport,
    Protocol,
    Other,
}

fn classify_body_failure(error: &reqwest::Error) -> BodyFailure {
    let mut source = StdError::source(error);
    for _ in 0..16 {
        let Some(current) = source else {
            return BodyFailure::Other;
        };
        if let Some(hyper_error) = current.downcast_ref::<hyper::Error>() {
            if hyper_error.is_incomplete_message() {
                return BodyFailure::Transport;
            }
            if hyper_error.is_parse() {
                return BodyFailure::Protocol;
            }
        }
        if let Some(io_error) = current.downcast_ref::<std::io::Error>() {
            if is_transport_io_kind(io_error.kind()) {
                return BodyFailure::Transport;
            }
        }
        source = current.source();
    }
    BodyFailure::Other
}

fn is_transport_io_kind(kind: std::io::ErrorKind) -> bool {
    matches!(
        kind,
        std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::NotConnected
            | std::io::ErrorKind::UnexpectedEof
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    struct CountingRequest(Arc<std::sync::atomic::AtomicUsize>);

    #[async_trait::async_trait]
    impl HttpRequestTrait for CountingRequest {
        fn get_path(&self) -> String {
            "/".to_owned()
        }
        fn get_method(&self) -> Method {
            Method::GET
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

    struct NoopRequest;

    #[async_trait::async_trait]
    impl HttpRequestTrait for NoopRequest {
        fn get_path(&self) -> String {
            "/".to_owned()
        }

        fn get_method(&self) -> Method {
            Method::GET
        }

        async fn deal_with_response(self: Box<Self>, _result: HttpResponseResult) {}
    }

    fn paused_worker() -> Result<(HttpClientInner, mpsc::Receiver<SendCommand>), NetError> {
        let config = Arc::new(HttpClientConfig::new("http://127.0.0.1:1")?);
        let (request_tx, request_rx) = mpsc::channel(1);
        Ok((
            HttpClientInner {
                request_tx,
                shutdown_sent: AtomicBool::new(false),
                admission_gate: Mutex::new(()),
                shutdown_notify: Arc::new(Notify::new()),
                next_request_id: AtomicU64::new(1),
                shutdown_state: Arc::new(ShutdownState {
                    joins: Mutex::new(None),
                    result: Mutex::new(Some(Ok(()))),
                    notify: Notify::new(),
                }),
                client: build_client(&config)?,
                config,
                registry: Arc::new(RequestRegistry::new()),
            },
            request_rx,
        ))
    }

    fn test_job(
        worker: &HttpClientInner,
        id: u64,
    ) -> Result<(SendJob, RegistrationGuard), NetError> {
        let request_id = HttpRequestId(id);
        let (control, registration) = worker.register_request(request_id)?;
        Ok((
            SendJob {
                request: Box::new(NoopRequest),
                options: HttpRequestOptions::default(),
                request_id,
                control,
                registry: worker.registry(),
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
        let (stale_control, stale) = registry.register(id)?;
        registry.finish(id, &stale_control);
        let (replacement_control, replacement) = registry.register(id)?;
        replacement.commit();
        drop(stale);
        check(
            !registry.is_empty()?,
            "stale guard removed replacement registration",
        )?;
        registry.finish(id, &replacement_control);
        Ok(())
    }

    fn check(
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
        let (_, guard) = registry.register(request_id)?;
        drop(guard);
        let (replacement_control, replacement) = registry.register(request_id)?;
        replacement.commit();
        registry.finish(request_id, &replacement_control);
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
        let (control, guard) = registry.register(request_id)?;
        guard.commit();
        drop(guard);
        check(
            !registry.is_empty()?,
            "committed registration was released too early",
        )?;
        registry.finish(request_id, &control);
        check(
            registry.is_empty()?,
            "terminal finish did not release registration",
        )
    }
}

use crate::api::open_net_config::OpenNetConfig;
use crate::common::log::log_def::LogType;
#[cfg(feature = "ws-client")]
use crate::common::retirement::ThreadHandle;
use crate::common::CommonEngine;
use crate::error::NetError;
#[cfg(feature = "ws-client")]
use crate::module::ws_client::ws_client_inner::WSClientInner;
#[cfg(feature = "ws-client")]
use crate::ws::{WebSocketClient, WebSocketClientConfig};
#[cfg(feature = "ws-client")]
use client_entry::ClientEntry;
#[cfg(feature = "ws-client")]
use client_slot::ClientSlot;
#[cfg(feature = "net-status")]
use std::collections::HashMap;
use std::sync::Arc;
#[cfg(feature = "net-status")]
use std::sync::Mutex;
#[cfg(feature = "ws-client")]
use std::time::Duration;

#[cfg(test)]
mod channel_config_tests;
#[cfg(feature = "ws-client")]
mod client_entry;
#[cfg(feature = "ws-client")]
mod client_slot;
mod engine_shutdown;
#[cfg(test)]
mod engine_shutdown_common_tests;
#[cfg(all(test, feature = "ws-client"))]
mod engine_shutdown_ws_tests;
#[cfg(feature = "http-client")]
mod http_client_slot;
#[cfg(feature = "http-client")]
pub(crate) mod managed_http_client;
#[cfg(feature = "net-status")]
mod net_status_clients;
mod open_net_inner;
#[cfg(test)]
mod runtime_config_tests;
#[cfg(all(test, feature = "ws-client"))]
mod tests;
#[cfg(feature = "ws-client")]
mod ws_cleanup;

#[cfg(feature = "http-client")]
use crate::api::http::http_client::HttpClient;
#[cfg(feature = "ws-client")]
use crate::module::transport::compiled_network_config::CompiledNetworkConfig;
pub(crate) use open_net_inner::OpenNetInner;

impl OpenNetInner {
    pub(crate) fn new_with_config(config: OpenNetConfig) -> Result<Self, NetError> {
        crate::log_t!(LogType::Engine; "engine.new_with_config",
            "config_runtime_worker_threads|config_async_queue_capacity|config_sync_queue_capacity",
            config.runtime_worker_threads, config.async_queue_capacity, config.sync_queue_capacity);
        let result: Result<Self, NetError> = (|| {
            config.validate()?;
            #[cfg(feature = "ws-client")]
            let network = Arc::new(CompiledNetworkConfig::new(config.network_config)?);
            let common_engine = Arc::new(CommonEngine::new_with_runtime_worker_threads(
                config.async_queue_capacity,
                config.sync_queue_capacity,
                config.runtime_worker_threads,
            )?);
            #[cfg(feature = "net-status")]
            let network_status =
                crate::module::net_status::inner::shared::SharedNetworkService::new(
                    common_engine.clone(),
                )?;
            let mut inner = Self {
                shutdown: None,
                common_engine,
                #[cfg(feature = "net-status")]
                network_status,
                #[cfg(feature = "net-status")]
                net_status_clients: Arc::new(Mutex::new(HashMap::new())),
                #[cfg(feature = "ws-client")]
                network,
                #[cfg(feature = "ws-client")]
                clients: Arc::new(Mutex::new(HashMap::new())),
                #[cfg(feature = "ws-client")]
                clients_changed: Arc::new(tokio::sync::Notify::new()),
                #[cfg(feature = "http-client")]
                http_clients: Arc::new(Mutex::new(HashMap::new())),
            };
            inner.shutdown = Some(engine_shutdown::EngineShutdown::new(&inner)?);
            Ok(inner)
        })();
        result.inspect_err(|error| {
            crate::log_e!(LogType::Engine; "new_with_config", "error|error_kind", format!("{error:?}"), format!("{:?}", error.kind()));
        })
    }

    #[cfg(feature = "ws-client")]
    pub(crate) async fn create_ws_client(
        &self,
        thread_name: String,
        config: WebSocketClientConfig,
        network_config: Option<crate::api::network_config::NetworkConfig>,
    ) -> Result<WebSocketClient, NetError> {
        crate::log_t!(LogType::WSC; "ws.create_ws_client",
            "config_close_timeout|config_command_capacity|config_heartbeat_enabled|network_config_present",
            config.close_timeout, config.queues.commands, config.heartbeat.is_some(), network_config.is_some());
        let result: Result<WebSocketClient, NetError> = async {
        self.clients
            .lock()
            .map_err(|_| NetError::from(crate::error::ErrorKind::Internal))
            .and_then(|mut clients| {
                self.ensure_open()?;
                if clients.contains_key(thread_name.as_str()) {
                    return Err(NetError::from(crate::error::ErrorKind::ClientAlreadyExists));
                }
                clients.insert(thread_name.clone(), ClientSlot::Creating);
                crate::log_s!(LogType::WSC; "create_ws_client", "status", "Creating");
                Ok(())
            })?;

        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let clients = Arc::clone(&self.clients);
        let clients_changed = Arc::clone(&self.clients_changed);
        let default_network = Arc::clone(&self.network);
        let network_status = self.network_status.context();
        let operation_engine = self.common_engine.clone();
        self.common_engine.post(async move {
            // Accepted creation owns its runtime through failure cleanup without
            // retaining the OpenNet owner or reopening its closed service.
            let _operation_engine = operation_engine;
            // Compile an explicit client override once, outside the registry lock.
            // None inherits the existing compiled default; Some(Default) is an
            // explicit direct/WebPKI policy and must not be treated as inheritance.
            let network = match network_config {
                Some(config) => CompiledNetworkConfig::new(config)
                    .map(Arc::new),
                None => Ok(default_network),
            };
            let created = network.and_then(|network| {
                create_client_worker(thread_name.as_str(), config, network, network_status.clone())
            });
            let result =
                match created {
                    Ok((client, worker_thread)) => {
                        let mut worker_thread = Some(worker_thread);
                        let stored = clients.lock().map_err(|_| NetError::from(crate::error::ErrorKind::Internal)).map(
                            |mut clients| {
                                if network_status.is_closed() {
                                    return Err(NetError::from(crate::error::ErrorKind::Closed));
                                }
                                clients.insert(
                                    thread_name.clone(),
                                    ClientSlot::Ready(ClientEntry {
                                        client: client.clone(),
                                        worker_thread: worker_thread.take(),
                                    }),
                                );
                                Ok(())
                            },
                        ).and_then(|result| result);
                        match stored {
                            Ok(()) => {
                                crate::log_s!(LogType::WSC; "create_ws_client", "status", "Ready");
                                clients_changed.notify_waiters();
                                Ok(client)
                            },
                            Err(error) => {
                                if let Ok(mut clients) = clients.lock() {
                                    if matches!(clients.get(thread_name.as_str()), Some(ClientSlot::Creating)) {
                                        clients.remove(thread_name.as_str());
                                    }
                                }
                                clients_changed.notify_waiters();
                                client.inner.request_shutdown();
                                // Preserve ownership after a rejected Ready commit.
                                // Joining outside the registry lock also covers a
                                // completed Ignore worker racing engine teardown.
                                if let Some(worker_thread) = worker_thread {
                                    match tokio::task::spawn_blocking(move || worker_thread.join()).await {
                                        Ok(Ok(())) => {}
                                        Ok(Err(_)) => {
                                            crate::log_e!(LogType::WSC; "ws.create_ws_client", "event|error", "cleanup_failed", "worker_thread_panicked");
                                        }
                                        Err(join_error) => {
                                            crate::log_e!(LogType::WSC; "ws.create_ws_client", "event|join_cancelled|join_panicked", "cleanup_failed", join_error.is_cancelled(), join_error.is_panic());
                                        }
                                    }
                                }
                                Err(error)
                            }
                        }
                    }
                    Err(error) => {
                        if let Ok(mut clients) = clients.lock() {
                            clients.remove(thread_name.as_str());
                        }
                        clients_changed.notify_waiters();
                        Err(error)
                    }
                };
            if let Err(error) = &result {
                crate::log_e!(LogType::WSC; "create_ws_client", "stage|error", "worker_create", format!("{error:?}"));
            }
            if result_tx.send(result).is_err() {
                crate::log_s!(LogType::WSC; "create_ws_client", "stage", "caller_stopped_waiting");
            }
        });
        result_rx
            .await
            .map_err(|error| self.ensure_open().err().unwrap_or_else(|| NetError::with_source(crate::error::ErrorKind::Internal, error)))?
    }.await;
        result.inspect_err(|error| {
            crate::log_e!(LogType::WSC; "create_ws_client", "error|error_kind", format!("{error:?}"), format!("{:?}", error.kind()));
        })
    }

    #[cfg(feature = "ws-client")]
    pub(crate) fn get_ws_client(&self, thread_name: &str) -> Result<WebSocketClient, NetError> {
        self.ensure_open()?;
        crate::log_t!(LogType::WSC; "get_ws_client");
        let result: Result<WebSocketClient, NetError> = (|| {
            let clients = self
                .clients
                .lock()
                .map_err(|_| NetError::from(crate::error::ErrorKind::Internal))?;
            match clients.get(thread_name) {
                Some(ClientSlot::Ready(entry)) => Ok(entry.client.clone()),
                Some(ClientSlot::Creating | ClientSlot::Closing) => {
                    Err(NetError::from(crate::error::ErrorKind::ConnectionClosing))
                }
                None => Err(NetError::from(crate::error::ErrorKind::ClientNotFound)),
            }
        })();
        result.inspect_err(|error| {
            crate::log_e!(LogType::WSC; "get_ws_client", "error|error_kind", format!("{error:?}"), format!("{:?}", error.kind()));
        })
    }

    /// Returns a ready named client or submits one creation for the name.
    ///
    /// The registry remains the source of truth for the operation. A caller
    /// that races an in-flight creation waits for the state transition rather
    /// than surfacing `ClientAlreadyExists` or `ConnectionClosing`.
    #[cfg(feature = "ws-client")]
    pub(crate) async fn get_or_create_ws_client(
        &self,
        thread_name: String,
        config: WebSocketClientConfig,
    ) -> Result<WebSocketClient, NetError> {
        loop {
            match self.get_ws_client(thread_name.as_str()) {
                Ok(client) => return Ok(client),
                Err(error)
                    if matches!(
                        error.kind(),
                        crate::error::ErrorKind::ClientNotFound
                            | crate::error::ErrorKind::ClientAlreadyExists
                    ) =>
                {
                    match self
                        .create_ws_client(thread_name.clone(), config.clone(), None)
                        .await
                    {
                        Ok(client) => return Ok(client),
                        Err(error)
                            if matches!(
                                error.kind(),
                                crate::error::ErrorKind::ClientAlreadyExists
                                    | crate::error::ErrorKind::ConnectionClosing
                            ) =>
                        {
                            continue
                        }
                        Err(error) => return Err(error),
                    }
                }
                Err(error) if error.kind() == crate::error::ErrorKind::ConnectionClosing => {
                    self.wait_for_ws_client_change(thread_name.as_str()).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Waits for a named client to leave the transient Creating or Closing state.
    #[cfg(feature = "ws-client")]
    async fn wait_for_ws_client_change(&self, thread_name: &str) {
        loop {
            // Register the waiter before inspecting the slot so a release
            // between the inspection and await cannot be missed.
            let notified = self.clients_changed.notified();
            match self.get_ws_client(thread_name) {
                Err(error) if error.kind() == crate::error::ErrorKind::ConnectionClosing => {
                    notified.await;
                }
                _ => return,
            }
        }
    }

    /// Idempotently destroys a named client, waiting for a concurrent lifecycle
    /// operation to release the name before retrying.
    #[cfg(feature = "ws-client")]
    pub(crate) async fn destroy_ws_client_if_exists(
        &self,
        thread_name: &str,
    ) -> Result<(), NetError> {
        loop {
            // Register before attempting destruction. A concurrent creator or
            // destroyer may finish between the call and the await below.
            let notified = self.clients_changed.notified();
            match self.destroy_ws_client(thread_name).await {
                Ok(()) => return Ok(()),
                Err(error) if error.kind() == crate::error::ErrorKind::ClientNotFound => {
                    return Ok(())
                }
                Err(error) if error.kind() == crate::error::ErrorKind::ConnectionClosing => {
                    notified.await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    #[cfg(feature = "ws-client")]
    pub(crate) async fn destroy_ws_client(&self, thread_name: &str) -> Result<(), NetError> {
        self.ensure_open()?;
        crate::log_t!(LogType::WSC; "destroy_ws_client");
        let result: Result<(), NetError> = async {
        let entry = {
            let mut clients = self.clients.lock().map_err(|_| NetError::from(crate::error::ErrorKind::Internal))?;
            match clients.remove(thread_name) {
                Some(ClientSlot::Ready(entry)) => {
                    clients.insert(thread_name.to_string(), ClientSlot::Closing);
                    crate::log_s!(LogType::WSC; "destroy_ws_client", "status", "Closing");
                    entry
                }
                Some(slot @ (ClientSlot::Creating | ClientSlot::Closing)) => {
                    clients.insert(thread_name.to_string(), slot);
                    return Err(NetError::from(crate::error::ErrorKind::ConnectionClosing));
                }
                None => return Err(NetError::from(crate::error::ErrorKind::ClientNotFound)),
            }
        };
        let mut guard = DestroyClientGuard::new(
            Arc::clone(&self.common_engine),
            Arc::clone(&self.clients),
            Arc::clone(&self.clients_changed),
            thread_name.to_string(),
            entry,
        );
        let shutdown_result = guard.client()?.shutdown().await;
        guard.join_and_release().await?;
        shutdown_result
    }.await;
        result.inspect_err(|error| {
            crate::log_e!(LogType::WSC; "destroy_ws_client", "error|error_kind", format!("{error:?}"), format!("{:?}", error.kind()));
        })
    }
}

impl OpenNetInner {
    #[cfg(feature = "http-client")]
    pub(crate) async fn create_http_client(
        &self,
        thread_name: String,
        config: crate::api::http::http_config::HttpClientConfig,
    ) -> Result<HttpClient, NetError> {
        crate::log_t!(LogType::HTTP; "create_http_client", "config", format!("{config:?}"));
        config.validate()?;
        self.http_clients
            .lock()
            .map_err(NetError::from_poison)
            .and_then(|mut clients| {
                self.ensure_open()?;
                if clients.contains_key(thread_name.as_str()) {
                    return Err(NetError::from(crate::error::ErrorKind::ClientAlreadyExists));
                }
                clients.insert(
                    thread_name.clone(),
                    crate::inner::net_impl::http_client_slot::HttpClientSlot::Creating,
                );
                Ok(())
            })?;

        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let clients = Arc::clone(&self.http_clients);
        let name = thread_name.clone();
        let network_status = self.network_status.context();
        self.common_engine.post(async move {
            let creation_context = network_status.clone();
            let created = match tokio::task::spawn_blocking(move || {
                crate::module::http::http_client_inner::create_http_client(name, config, creation_context)
            })
            .await
            {
                Ok(result) => result,
                Err(_) => Err(NetError::from(crate::error::ErrorKind::RuntimeUnavailable)),
            };
            let result = match created {
                Ok(client) => {
                    let stored = clients
                        .lock()
                        .map_err(NetError::from_poison)
                        .and_then(|mut slots| {
                            if network_status.is_closed() {
                                return Err(NetError::from(crate::error::ErrorKind::Closed));
                            }
                            slots.insert(
                                thread_name.clone(),
                                crate::inner::net_impl::http_client_slot::HttpClientSlot::Ready(
                                    client.clone(),
                                ),
                            );
                            Ok(client.clone())
                        });
                    match stored {
                        Ok(client) => {
                            crate::log_r!(LogType::HTTP; "create_http_client", "status", "Ready");
                            Ok(client)
                        }
                        Err(error) => {
                            if let Ok(mut slots) = clients.lock() {
                                slots.remove(thread_name.as_str());
                            }
                            client.request_shutdown();
                            let _ = client.shutdown().await;
                            Err(error)
                        }
                    }
                }
                Err(error) => {
                    if let Ok(mut slots) = clients.lock() {
                        slots.remove(thread_name.as_str());
                    }
                    crate::log_e!(LogType::HTTP; "create_http_client", "error|error_kind", format!("{error:?}"), format!("{:?}", error.kind()));
                    Err(error)
                }
            };
            if result_tx.send(result).is_err() {
                crate::log_s!(LogType::HTTP; "create_http_client", "stage", "caller_stopped_waiting");
            }
        });
        result_rx.await.map_err(|error| {
            self.ensure_open()
                .err()
                .unwrap_or_else(|| NetError::with_source(crate::error::ErrorKind::Internal, error))
        })?
    }

    #[cfg(feature = "http-client")]
    pub(crate) fn get_http_client(&self, thread_name: &str) -> Result<HttpClient, NetError> {
        self.ensure_open()?;
        let slots = self.http_clients.lock().map_err(NetError::from_poison)?;
        match slots.get(thread_name) {
            Some(crate::inner::net_impl::http_client_slot::HttpClientSlot::Ready(client)) => {
                Ok(client.clone())
            }
            Some(crate::inner::net_impl::http_client_slot::HttpClientSlot::Creating)
            | Some(crate::inner::net_impl::http_client_slot::HttpClientSlot::Closing(_)) => {
                Err(NetError::from(crate::error::ErrorKind::ConnectionClosing))
            }
            Some(crate::inner::net_impl::http_client_slot::HttpClientSlot::Managed(client)) => {
                let client = client.clone();
                drop(slots);
                client.get_registered()
            }
            None => Err(NetError::from(crate::error::ErrorKind::ClientNotFound)),
        }
    }

    #[cfg(feature = "http-client")]
    pub(crate) async fn destroy_http_client(&self, thread_name: &str) -> Result<(), NetError> {
        self.ensure_open()?;
        enum Target {
            Legacy(HttpClient),
            Managed(Arc<managed_http_client::ManagedHttpClientInner>),
        }
        let target = {
            let mut slots = self.http_clients.lock().map_err(NetError::from_poison)?;
            match slots.remove(thread_name) {
                Some(crate::inner::net_impl::http_client_slot::HttpClientSlot::Ready(client)) => {
                    let clone = client.clone();
                    slots.insert(
                        thread_name.to_string(),
                        crate::inner::net_impl::http_client_slot::HttpClientSlot::Closing(clone),
                    );
                    Target::Legacy(client)
                }
                Some(crate::inner::net_impl::http_client_slot::HttpClientSlot::Managed(client)) => {
                    slots.insert(
                        thread_name.to_string(),
                        crate::inner::net_impl::http_client_slot::HttpClientSlot::Managed(
                            client.clone(),
                        ),
                    );
                    Target::Managed(client)
                }
                Some(slot) => {
                    slots.insert(thread_name.to_string(), slot);
                    return Err(NetError::from(crate::error::ErrorKind::ConnectionClosing));
                }
                None => return Err(NetError::from(crate::error::ErrorKind::ClientNotFound)),
            }
        };
        let client = match target {
            Target::Legacy(client) => client,
            Target::Managed(client) => return client.shutdown().await,
        };
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let clients = Arc::clone(&self.http_clients);
        let name = thread_name.to_string();
        self.common_engine.post(async move {
            let result = client.shutdown().await;
            if let Ok(mut slots) = clients.lock() {
                if matches!(
                    slots.get(name.as_str()),
                    Some(crate::inner::net_impl::http_client_slot::HttpClientSlot::Closing(_))
                ) {
                    slots.remove(name.as_str());
                }
            }
            if result_tx.send(result).is_err() {
                crate::log_s!(LogType::HTTP; "http.destroy_http_client", "stage", "caller_stopped_waiting");
            }
        });
        result_rx.await.map_err(|error| {
            self.ensure_open()
                .err()
                .unwrap_or_else(|| NetError::with_source(crate::error::ErrorKind::Internal, error))
        })?
    }
}

/// Owns a registry `Closing` reservation until the worker thread has really exited.
///
/// If the async destroy future is dropped, `Drop` publishes shutdown synchronously and
/// hands the join/removal work to a detached cleanup thread. This prevents both orphaned
/// workers and reuse of the same name while the old instance is still alive.
#[cfg(feature = "ws-client")]
struct DestroyClientGuard {
    engine: Arc<CommonEngine>,
    clients: Arc<Mutex<HashMap<String, ClientSlot>>>,
    clients_changed: Arc<tokio::sync::Notify>,
    thread_name: String,
    entry: Option<ClientEntry>,
}

#[cfg(feature = "ws-client")]
impl DestroyClientGuard {
    fn new(
        engine: Arc<CommonEngine>,
        clients: Arc<Mutex<HashMap<String, ClientSlot>>>,
        clients_changed: Arc<tokio::sync::Notify>,
        thread_name: String,
        entry: ClientEntry,
    ) -> Self {
        crate::log_t!(LogType::WSC; "ws.destroy_client_guard.new",
            "engine|clients|clients_changed|entry_client_id|entry_worker_present",
            "provided", "provided", "provided", entry.client.id().as_u64(), entry.worker_thread.is_some());
        Self {
            engine,
            clients,
            clients_changed,
            thread_name,
            entry: Some(entry),
        }
    }

    fn client(&self) -> Result<&WebSocketClient, NetError> {
        crate::log_t!(LogType::WSC; "client");
        self.entry
            .as_ref()
            .map(|entry| &entry.client)
            .ok_or(NetError::from(crate::error::ErrorKind::Internal))
    }

    async fn join_and_release(&mut self) -> Result<(), NetError> {
        crate::log_t!(LogType::WSC; "join_and_release");
        let (cleanup, completed) = self
            .take_cleanup()
            .ok_or_else(|| NetError::from(crate::error::ErrorKind::Internal))?;
        // The task owns both the join and its completion, independently of this
        // waiter and the caller's runtime. A failed submission still runs the
        // fallback before we return its error and allow name reuse.
        let submitted = cleanup.submit(&self.engine.runtime_handle());
        let result = completed
            .await
            .map_err(|error| NetError::with_source(crate::error::ErrorKind::Internal, error))?;
        submitted.and(result)
    }

    fn take_cleanup(
        &mut self,
    ) -> Option<(
        ws_cleanup::CleanupTask,
        tokio::sync::oneshot::Receiver<Result<(), NetError>>,
    )> {
        let mut entry = self.entry.take()?;
        let worker = entry.worker_thread.take();
        // Release the extra client handle before reporting join completion.
        drop(entry);
        Some(ws_cleanup::CleanupTask::new(
            Arc::clone(&self.engine),
            Arc::clone(&self.clients),
            Arc::clone(&self.clients_changed),
            self.thread_name.clone(),
            worker,
        ))
    }
}

#[cfg(feature = "ws-client")]
impl Drop for DestroyClientGuard {
    fn drop(&mut self) {
        crate::log_t!(LogType::WSC; "drop");
        let Some(entry) = self.entry.as_ref() else {
            return;
        };
        entry.client.inner.request_shutdown();
        if let Some((cleanup, _completed)) = self.take_cleanup() {
            // Dropping an unsubmitted task invokes the same owned fallback as
            // a queued task discarded by its executor.
            drop(cleanup);
        }
    }
}

#[cfg(feature = "ws-client")]
fn release_closing_slot(
    clients: &Arc<Mutex<HashMap<String, ClientSlot>>>,
    clients_changed: &Arc<tokio::sync::Notify>,
    thread_name: &str,
) {
    crate::log_t!(LogType::WSC; "ws.release_closing_slot",
        "clients|clients_changed", "provided", "provided");
    {
        let mut clients = clients
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if matches!(clients.get(thread_name), Some(ClientSlot::Closing)) {
            clients.remove(thread_name);
            crate::log_s!(LogType::WSC; "release_closing_slot", "stage", "released");
        }
    }
    clients_changed.notify_waiters();
}

#[cfg(feature = "ws-client")]
fn create_client_worker(
    thread_name: &str,
    config: WebSocketClientConfig,
    network: Arc<CompiledNetworkConfig>,
    network_status: crate::net_status::NetworkStatusContext,
) -> Result<(WebSocketClient, ThreadHandle), NetError> {
    crate::log_t!(LogType::WSC; "ws.create_client_worker",
        "config_close_timeout|config_command_capacity|config_heartbeat_enabled|network_status_policy|network_status",
        config.close_timeout, config.queues.commands, config.heartbeat.is_some(),
        format!("{:?}", network.network_status_policy()), "provided");
    let result: Result<(WebSocketClient, ThreadHandle), NetError> = (|| {
        if thread_name.contains('\0') {
            return Err(NetError::from(crate::error::ErrorKind::InvalidConfig));
        }
        let network_observation = match network.network_status_policy() {
            crate::NetworkStatusPolicy::Ignore => None,
            crate::NetworkStatusPolicy::PauseOnUnavailable => {
                Some((network_status.acquire()?, network_status.subscribe_gate()))
            }
        };
        let threads = network_status.common_engine()?.resource_threads();
        let (inner, worker) = WSClientInner::new_with_network_and_threads(
            config,
            network,
            network_observation,
            threads.clone(),
        )?;
        let client = WebSocketClient::from_inner(inner);
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let worker_thread = threads.spawn_tracked(thread_name, move || worker.run(ready_tx))
        .map_err(|error| {
            crate::log_e!(LogType::WSC; "create_client_worker", "stage|error", "spawn_worker", crate::common::log::summary::error(&error));
            NetError::with_source(crate::error::ErrorKind::RuntimeUnavailable, error)
                .with_stage(crate::error::ErrorStage::Runtime)
        })?;
        match ready_rx.recv_timeout(Duration::from_secs(5)) {
            Ok(Ok(())) => Ok((client, worker_thread)),
            Ok(Err(error)) => {
                let _ = worker_thread.join();
                Err(error)
            }
            Err(error) => {
                client.inner.request_shutdown();
                let _ = worker_thread.join();
                Err(
                    NetError::with_source(crate::error::ErrorKind::TimedOut, error)
                        .with_stage(crate::error::ErrorStage::Runtime),
                )
            }
        }
    })();
    result.inspect_err(|error| {
        crate::log_e!(LogType::WSC; "create_client_worker", "error|error_kind", format!("{error:?}"), format!("{:?}", error.kind()));
    })
}

impl Drop for OpenNetInner {
    fn drop(&mut self) {
        if self.shutdown.is_some() {
            self.begin_shutdown(true);
            return;
        }
        #[cfg(feature = "net-status")]
        {
            self.network_status.request_close();
            self.stop_net_status_clients();
        }
        #[cfg(feature = "ws-client")]
        {
            crate::log_t!(LogType::WSC; "drop");
            let entries = match self.clients.lock() {
                Ok(mut clients) => clients.drain().map(|(_, slot)| slot).collect::<Vec<_>>(),
                Err(_) => {
                    crate::log_e!(LogType::WSC; "drop", "error", "ws_registry_lock_poisoned");
                    Vec::new()
                }
            };
            for slot in entries {
                if let ClientSlot::Ready(entry) = slot {
                    entry.client.inner.request_engine_drop();
                }
            }
        }
        #[cfg(feature = "http-client")]
        {
            crate::log_t!(LogType::HTTP; "drop");
            let clients = match self.http_clients.lock() {
                Ok(mut clients) => clients.drain().map(|(_, slot)| slot).collect::<Vec<_>>(),
                Err(_) => {
                    crate::log_e!(LogType::HTTP; "drop", "error", "http_registry_lock_poisoned");
                    Vec::new()
                }
            };
            for slot in clients {
                match slot {
                    http_client_slot::HttpClientSlot::Managed(client) => client.begin_shutdown(),
                    slot => {
                        if let Some(client) = slot.into_client() {
                            client.request_shutdown();
                        }
                    }
                }
            }
        }
    }
}

#[cfg(all(test, feature = "net-status"))]
mod shared_network_tests;

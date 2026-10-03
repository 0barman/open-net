use crate::api::open_net_config::OpenNetConfig;
use crate::common::log::log_def::LogType;
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
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
#[cfg(feature = "ws-client")]
use std::thread::JoinHandle;
#[cfg(feature = "ws-client")]
use std::time::Duration;

#[cfg(test)]
mod channel_config_tests;
#[cfg(feature = "ws-client")]
mod client_entry;
#[cfg(feature = "ws-client")]
mod client_slot;
#[cfg(feature = "http-client")]
mod http_client_slot;
mod net_status_clients;
mod open_net_inner;
#[cfg(test)]
mod runtime_config_tests;
#[cfg(all(test, feature = "ws-client"))]
mod tests;

#[cfg(feature = "http-client")]
use crate::api::http::http_client::HttpClient;
#[cfg(feature = "ws-client")]
use crate::module::transport::compiled_network_config::CompiledNetworkConfig;
pub(crate) use open_net_inner::OpenNetInner;

impl OpenNetInner {
    pub(crate) fn new_with_config(config: OpenNetConfig) -> Result<Self, NetError> {
        crate::log_t!(LogType::Engine; "new_with_config");
        let result: Result<Self, NetError> = (|| {
            config.validate()?;
            #[cfg(feature = "ws-client")]
            let network = Arc::new(CompiledNetworkConfig::new(config.network_config)?);
            let common_engine = Arc::new(CommonEngine::new_with_runtime_worker_threads(
                config.async_queue_capacity,
                config.sync_queue_capacity,
                config.runtime_worker_threads,
            )?);
            let network_status =
                crate::module::net_status::inner::shared::SharedNetworkService::new(
                    common_engine.clone(),
                )?;
            Ok(Self {
                common_engine,
                network_status,
                net_status_clients: Arc::new(Mutex::new(HashMap::new())),
                #[cfg(feature = "ws-client")]
                network,
                #[cfg(feature = "ws-client")]
                clients: Arc::new(Mutex::new(HashMap::new())),
                #[cfg(feature = "http-client")]
                http_clients: Arc::new(Mutex::new(HashMap::new())),
            })
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
        crate::log_t!(LogType::WSC; "create_ws_client", "thread_name|config", thread_name, format!("{:?}", config));
        let result: Result<WebSocketClient, NetError> = async {
        self.clients
            .lock()
            .map_err(|_| NetError::from(crate::error::ErrorKind::Internal))
            .and_then(|mut clients| {
                if clients.contains_key(thread_name.as_str()) {
                    return Err(NetError::from(crate::error::ErrorKind::ClientAlreadyExists));
                }
                clients.insert(thread_name.clone(), ClientSlot::Creating);
                crate::log_s!(LogType::WSC; "create_ws_client", "thread_name|status", thread_name, "Creating");
                Ok(())
            })?;

        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let clients = Arc::clone(&self.clients);
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
                                crate::log_s!(LogType::WSC; "create_ws_client", "thread_name|status", thread_name, "Ready");
                                Ok(client)
                            },
                            Err(error) => {
                                client.inner.request_shutdown();
                                // Preserve ownership after a rejected Ready commit.
                                // Joining outside the registry lock also covers a
                                // completed Ignore worker racing engine teardown.
                                if let Some(worker_thread) = worker_thread {
                                    match tokio::task::spawn_blocking(move || worker_thread.join()).await {
                                        Ok(Ok(())) => {}
                                        Ok(Err(_)) => {
                                            crate::log_e!(LogType::WSC; "create_ws_client_cleanup", "error", "worker_thread_panicked");
                                        }
                                        Err(join_error) => {
                                            crate::log_e!(LogType::WSC; "create_ws_client_cleanup", "error", crate::common::log::summary::error(&join_error));
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
                        Err(error)
                    }
                };
            if let Err(error) = &result {
                crate::log_e!(LogType::WSC; "create_ws_client", "thread_name|stage|error", thread_name, "worker_create", format!("{error:?}"));
            }
            if result_tx.send(result).is_err() {
                crate::log_s!(LogType::WSC; "create_ws_client", "stage", "caller_stopped_waiting");
            }
        });
        result_rx
            .await
            .map_err(|error| NetError::with_source(crate::error::ErrorKind::Internal, error))?
    }.await;
        result.inspect_err(|error| {
            crate::log_e!(LogType::WSC; "create_ws_client", "error|error_kind", format!("{error:?}"), format!("{:?}", error.kind()));
        })
    }

    #[cfg(feature = "ws-client")]
    pub(crate) fn get_ws_client(&self, thread_name: &str) -> Result<WebSocketClient, NetError> {
        crate::log_t!(LogType::WSC; "get_ws_client", "thread_name", thread_name);
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

    #[cfg(feature = "ws-client")]
    pub(crate) async fn destroy_ws_client(&self, thread_name: &str) -> Result<(), NetError> {
        crate::log_t!(LogType::WSC; "destroy_ws_client", "thread_name", thread_name);
        let result: Result<(), NetError> = async {
        let entry = {
            let mut clients = self.clients.lock().map_err(|_| NetError::from(crate::error::ErrorKind::Internal))?;
            match clients.remove(thread_name) {
                Some(ClientSlot::Ready(entry)) => {
                    clients.insert(thread_name.to_string(), ClientSlot::Closing);
                    crate::log_s!(LogType::WSC; "destroy_ws_client", "thread_name|status", thread_name, "Closing");
                    entry
                }
                Some(slot @ (ClientSlot::Creating | ClientSlot::Closing)) => {
                    clients.insert(thread_name.to_string(), slot);
                    return Err(NetError::from(crate::error::ErrorKind::ConnectionClosing));
                }
                None => return Err(NetError::from(crate::error::ErrorKind::ClientNotFound)),
            }
        };
        let mut guard =
            DestroyClientGuard::new(Arc::clone(&self.clients), thread_name.to_string(), entry);
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
        crate::log_t!(LogType::HTTP; "create_http_client", "thread_name|config", thread_name, format!("{config:?}"));
        config.validate()?;
        self.http_clients
            .lock()
            .map_err(NetError::from_poison)
            .and_then(|mut clients| {
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
                            crate::log_s!(LogType::HTTP; "create_http_client", "thread_name|status", thread_name, "Ready");
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
        result_rx
            .await
            .map_err(|error| NetError::with_source(crate::error::ErrorKind::Internal, error))?
    }

    #[cfg(feature = "http-client")]
    pub(crate) fn get_http_client(&self, thread_name: &str) -> Result<HttpClient, NetError> {
        let slots = self.http_clients.lock().map_err(NetError::from_poison)?;
        match slots.get(thread_name) {
            Some(crate::inner::net_impl::http_client_slot::HttpClientSlot::Ready(client)) => {
                Ok(client.clone())
            }
            Some(crate::inner::net_impl::http_client_slot::HttpClientSlot::Creating)
            | Some(crate::inner::net_impl::http_client_slot::HttpClientSlot::Closing(_)) => {
                Err(NetError::from(crate::error::ErrorKind::ConnectionClosing))
            }
            None => Err(NetError::from(crate::error::ErrorKind::ClientNotFound)),
        }
    }

    #[cfg(feature = "http-client")]
    pub(crate) async fn destroy_http_client(&self, thread_name: &str) -> Result<(), NetError> {
        let client = {
            let mut slots = self.http_clients.lock().map_err(NetError::from_poison)?;
            match slots.remove(thread_name) {
                Some(crate::inner::net_impl::http_client_slot::HttpClientSlot::Ready(client)) => {
                    let clone = client.clone();
                    slots.insert(
                        thread_name.to_string(),
                        crate::inner::net_impl::http_client_slot::HttpClientSlot::Closing(clone),
                    );
                    client
                }
                Some(slot) => {
                    slots.insert(thread_name.to_string(), slot);
                    return Err(NetError::from(crate::error::ErrorKind::ConnectionClosing));
                }
                None => return Err(NetError::from(crate::error::ErrorKind::ClientNotFound)),
            }
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
                crate::log_s!(LogType::HTTP; "destroy_http_client", "stage", "caller_stopped_waiting");
            }
        });
        result_rx
            .await
            .map_err(|error| NetError::with_source(crate::error::ErrorKind::Internal, error))?
    }
}

/// Owns a registry `Closing` reservation until the worker thread has really exited.
///
/// If the async destroy future is dropped, `Drop` publishes shutdown synchronously and
/// hands the join/removal work to a detached cleanup thread. This prevents both orphaned
/// workers and reuse of the same name while the old instance is still alive.
#[cfg(feature = "ws-client")]
struct DestroyClientGuard {
    clients: Arc<Mutex<HashMap<String, ClientSlot>>>,
    thread_name: String,
    entry: Option<ClientEntry>,
    cleanup_handed_off: bool,
}

#[cfg(feature = "ws-client")]
impl DestroyClientGuard {
    fn new(
        clients: Arc<Mutex<HashMap<String, ClientSlot>>>,
        thread_name: String,
        entry: ClientEntry,
    ) -> Self {
        crate::log_t!(LogType::WSC; "new", "thread_name", thread_name);
        Self {
            clients,
            thread_name,
            entry: Some(entry),
            cleanup_handed_off: false,
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
        let result: Result<(), NetError> = async {
            let worker_thread = self
                .entry
                .as_mut()
                .and_then(|entry| entry.worker_thread.take());
            let clients = Arc::clone(&self.clients);
            let thread_name = self.thread_name.clone();
            self.cleanup_handed_off = true;
            self.entry.take();
            match worker_thread {
                Some(worker_thread) => tokio::task::spawn_blocking(move || {
                    let result = worker_thread
                        .join()
                        .map_err(|_| NetError::from(crate::error::ErrorKind::Internal));
                    release_closing_slot(&clients, thread_name.as_str());
                    result
                })
                .await
                .map_err(|error| NetError::with_source(crate::error::ErrorKind::Internal, error))?,
                None => {
                    release_closing_slot(&clients, thread_name.as_str());
                    Ok(())
                }
            }
        }
        .await;
        result.inspect_err(|error| {
            crate::log_e!(LogType::WSC; "join_and_release", "error|error_kind", format!("{error:?}"), format!("{:?}", error.kind()));
        })
    }
}

#[cfg(feature = "ws-client")]
impl Drop for DestroyClientGuard {
    fn drop(&mut self) {
        crate::log_t!(LogType::WSC; "drop");
        if self.cleanup_handed_off {
            return;
        }
        let Some(mut entry) = self.entry.take() else {
            return;
        };
        entry.client.inner.request_shutdown();
        let clients = Arc::clone(&self.clients);
        let thread_name = self.thread_name.clone();
        if let Some(worker_thread) = entry.worker_thread.take() {
            let cleanup = Arc::new(Mutex::new(Some((
                worker_thread,
                Arc::clone(&clients),
                thread_name.clone(),
            ))));
            let thread_cleanup = Arc::clone(&cleanup);
            let spawned = std::thread::Builder::new()
                .name("open-net-ws-destroy-cleanup".to_string())
                .spawn(move || {
                    if let Some((worker_thread, clients, thread_name)) = thread_cleanup
                        .lock()
                        .ok()
                        .and_then(|mut cleanup| cleanup.take())
                    {
                        let _ = worker_thread.join();
                        release_closing_slot(&clients, thread_name.as_str());
                    }
                });
            if spawned.is_err() {
                // Thread creation failure is exceptional; synchronously finish cleanup so
                // the name reservation and worker ownership cannot be leaked.
                if let Some((worker_thread, clients, thread_name)) =
                    cleanup.lock().ok().and_then(|mut cleanup| cleanup.take())
                {
                    let _ = worker_thread.join();
                    release_closing_slot(&clients, thread_name.as_str());
                }
            }
        } else {
            release_closing_slot(&clients, thread_name.as_str());
        }
    }
}

#[cfg(feature = "ws-client")]
fn release_closing_slot(clients: &Arc<Mutex<HashMap<String, ClientSlot>>>, thread_name: &str) {
    crate::log_t!(LogType::WSC; "release_closing_slot", "thread_name", thread_name);
    if let Ok(mut clients) = clients.lock() {
        if matches!(clients.get(thread_name), Some(ClientSlot::Closing)) {
            clients.remove(thread_name);
            crate::log_s!(LogType::WSC; "release_closing_slot", "thread_name|stage", thread_name, "released");
        }
    }
}

#[cfg(feature = "ws-client")]
fn create_client_worker(
    thread_name: &str,
    config: WebSocketClientConfig,
    network: Arc<CompiledNetworkConfig>,
    network_status: crate::net_status::NetworkStatusContext,
) -> Result<(WebSocketClient, JoinHandle<()>), NetError> {
    crate::log_t!(LogType::WSC; "create_client_worker", "thread_name|config", thread_name, format!("{:?}", config));
    let result: Result<(WebSocketClient, JoinHandle<()>), NetError> = (|| {
        if thread_name.contains('\0') {
            return Err(NetError::from(crate::error::ErrorKind::InvalidConfig));
        }
        let network_observation = match network.network_status_policy() {
            crate::NetworkStatusPolicy::Ignore => None,
            crate::NetworkStatusPolicy::PauseOnUnavailable => {
                Some((network_status.acquire()?, network_status.subscribe_gate()))
            }
        };
        let (inner, worker) =
            WSClientInner::new_with_network(config, network, network_observation)?;
        let client = WebSocketClient::from_inner(inner);
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let worker_thread = std::thread::Builder::new()
        .name(thread_name.to_string())
        .spawn(move || worker.run(ready_tx))
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
        self.network_status.request_close();
        self.stop_net_status_clients();
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
                if let Some(client) = slot.into_client() {
                    client.request_shutdown();
                }
            }
        }
    }
}

#[cfg(test)]
mod shared_network_tests;

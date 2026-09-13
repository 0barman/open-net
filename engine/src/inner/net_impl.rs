use crate::api::net_error::NetError;
#[cfg(feature = "ws-client")]
use crate::api::web_socket_client::{WebSocketClient, WebSocketClientConfig};
#[cfg(feature = "ws-client")]
use crate::module::ws_client::ws_client_inner::WSClientInner;
#[cfg(feature = "ws-client")]
use client_entry::ClientEntry;
#[cfg(feature = "ws-client")]
use client_slot::ClientSlot;
use on_common::log::log_def::LogType;
use on_common::CommonEngine;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
#[cfg(feature = "ws-client")]
use std::thread::JoinHandle;
#[cfg(feature = "ws-client")]
use std::time::Duration;

#[cfg(feature = "ws-client")]
mod client_entry;
#[cfg(feature = "ws-client")]
mod client_slot;
mod net_status_clients;
mod open_net_inner;
#[cfg(all(test, feature = "ws-client"))]
mod tests;

pub(crate) use open_net_inner::OpenNetInner;

impl OpenNetInner {
    #[cfg(feature = "ws-client")]
    pub(crate) fn new_with_network_config(
        config: crate::api::network_config::NetworkConfig,
    ) -> Result<Self, NetError> {
        let network = Arc::new(crate::module::transport::CompiledNetworkConfig::new(
            config,
        )?);
        Ok(Self {
            network,
            common_engine: Arc::new(CommonEngine::new(1024, 1024)?),
            net_status_clients: Arc::new(Mutex::new(HashMap::new())),
            clients: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub(crate) fn new() -> Result<Self, NetError> {
        on_common::log_t!(LogType::Engine; "new");
        let result: Result<Self, NetError> = (|| {
            let common_engine = Arc::new(CommonEngine::new(1024, 1024)?);
            Ok(Self {
                common_engine,
                net_status_clients: Arc::new(Mutex::new(HashMap::new())),
                #[cfg(feature = "ws-client")]
                network: Arc::new(crate::module::transport::CompiledNetworkConfig::new(
                    crate::api::network_config::NetworkConfig::default(),
                )?),
                #[cfg(feature = "ws-client")]
                clients: Arc::new(Mutex::new(HashMap::new())),
            })
        })();
        result.inspect_err(|error| {
            on_common::log_e!(LogType::Engine; "new", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    #[cfg(feature = "ws-client")]
    pub(crate) async fn create_ws_client(
        &self,
        thread_name: String,
        config: WebSocketClientConfig,
        network_config: Option<crate::api::network_config::NetworkConfig>,
    ) -> Result<WebSocketClient, NetError> {
        on_common::log_t!(LogType::WSC; "create_ws_client", "thread_name|config", thread_name, format!("{:?}", config));
        let result: Result<WebSocketClient, NetError> = async {
        self.clients
            .lock()
            .map_err(|_| NetError::InternalError)
            .and_then(|mut clients| {
                if clients.contains_key(thread_name.as_str()) {
                    return Err(NetError::ClientAlreadyExists);
                }
                clients.insert(thread_name.clone(), ClientSlot::Creating);
                on_common::log_s!(LogType::WSC; "create_ws_client", "thread_name|status", thread_name, "Creating");
                Ok(())
            })?;

        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let clients = Arc::clone(&self.clients);
        let default_network = Arc::clone(&self.network);
        let common_engine = Arc::clone(&self.common_engine);
        self.common_engine.post(async move {
            // Compile an explicit client override once, outside the registry lock.
            // None inherits the existing compiled default; Some(Default) is an
            // explicit direct/WebPKI policy and must not be treated as inheritance.
            let network = match network_config {
                Some(config) => crate::module::transport::CompiledNetworkConfig::new(config)
                    .map(Arc::new),
                None => Ok(default_network),
            };
            let created = network.and_then(|network| {
                create_client_worker(thread_name.as_str(), config, network, common_engine)
            });
            let result =
                match created {
                    Ok((client, worker_thread)) => {
                        let stored = clients.lock().map_err(|_| NetError::InternalError).map(
                            |mut clients| {
                                clients.insert(
                                    thread_name.clone(),
                                    ClientSlot::Ready(ClientEntry {
                                        client: client.clone(),
                                        worker_thread: Some(worker_thread),
                                    }),
                                );
                            },
                        );
                        match stored {
                            Ok(()) => {
                                on_common::log_s!(LogType::WSC; "create_ws_client", "thread_name|status", thread_name, "Ready");
                                Ok(client)
                            },
                            Err(error) => {
                                client.inner.request_shutdown();
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
                on_common::log_e!(LogType::WSC; "create_ws_client", "thread_name|stage|error", thread_name, "worker_create", format!("{error:?}"));
            }
            if result_tx.send(result).is_err() {
                on_common::log_s!(LogType::WSC; "create_ws_client", "stage", "caller_stopped_waiting");
            }
        });
        result_rx
            .await
            .map_err(|_| NetError::TaskInterruptionError)?
    }.await;
        result.inspect_err(|error| {
            on_common::log_e!(LogType::WSC; "create_ws_client", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    #[cfg(feature = "ws-client")]
    pub(crate) fn get_ws_client(&self, thread_name: &str) -> Result<WebSocketClient, NetError> {
        on_common::log_t!(LogType::WSC; "get_ws_client", "thread_name", thread_name);
        let result: Result<WebSocketClient, NetError> = (|| {
            let clients = self.clients.lock().map_err(|_| NetError::InternalError)?;
            match clients.get(thread_name) {
                Some(ClientSlot::Ready(entry)) => Ok(entry.client.clone()),
                Some(ClientSlot::Creating | ClientSlot::Closing) => {
                    Err(NetError::ConnectionClosing)
                }
                None => Err(NetError::ClientNotFound),
            }
        })();
        result.inspect_err(|error| {
            on_common::log_e!(LogType::WSC; "get_ws_client", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    #[cfg(feature = "ws-client")]
    pub(crate) async fn destroy_ws_client(&self, thread_name: &str) -> Result<(), NetError> {
        on_common::log_t!(LogType::WSC; "destroy_ws_client", "thread_name", thread_name);
        let result: Result<(), NetError> = async {
        let entry = {
            let mut clients = self.clients.lock().map_err(|_| NetError::InternalError)?;
            match clients.remove(thread_name) {
                Some(ClientSlot::Ready(entry)) => {
                    clients.insert(thread_name.to_string(), ClientSlot::Closing);
                    on_common::log_s!(LogType::WSC; "destroy_ws_client", "thread_name|status", thread_name, "Closing");
                    entry
                }
                Some(slot @ (ClientSlot::Creating | ClientSlot::Closing)) => {
                    clients.insert(thread_name.to_string(), slot);
                    return Err(NetError::ConnectionClosing);
                }
                None => return Err(NetError::ClientNotFound),
            }
        };
        let mut guard =
            DestroyClientGuard::new(Arc::clone(&self.clients), thread_name.to_string(), entry);
        let shutdown_result = guard.client()?.shutdown().await;
        guard.join_and_release().await?;
        shutdown_result
    }.await;
        result.inspect_err(|error| {
            on_common::log_e!(LogType::WSC; "destroy_ws_client", "error|error_code", format!("{error:?}"), *error as i32);
        })
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
        on_common::log_t!(LogType::WSC; "new", "thread_name", thread_name);
        Self {
            clients,
            thread_name,
            entry: Some(entry),
            cleanup_handed_off: false,
        }
    }

    fn client(&self) -> Result<&WebSocketClient, NetError> {
        on_common::log_t!(LogType::WSC; "client");
        self.entry
            .as_ref()
            .map(|entry| &entry.client)
            .ok_or(NetError::InternalError)
    }

    async fn join_and_release(&mut self) -> Result<(), NetError> {
        on_common::log_t!(LogType::WSC; "join_and_release");
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
                        .map_err(|_| NetError::TaskInterruptionError);
                    release_closing_slot(&clients, thread_name.as_str());
                    result
                })
                .await
                .map_err(|_| NetError::TaskInterruptionError)?,
                None => {
                    release_closing_slot(&clients, thread_name.as_str());
                    Ok(())
                }
            }
        }
        .await;
        result.inspect_err(|error| {
            on_common::log_e!(LogType::WSC; "join_and_release", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }
}

#[cfg(feature = "ws-client")]
impl Drop for DestroyClientGuard {
    fn drop(&mut self) {
        on_common::log_t!(LogType::WSC; "drop");
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
    on_common::log_t!(LogType::WSC; "release_closing_slot", "thread_name", thread_name);
    if let Ok(mut clients) = clients.lock() {
        if matches!(clients.get(thread_name), Some(ClientSlot::Closing)) {
            clients.remove(thread_name);
            on_common::log_s!(LogType::WSC; "release_closing_slot", "thread_name|stage", thread_name, "released");
        }
    }
}

#[cfg(feature = "ws-client")]
fn create_client_worker(
    thread_name: &str,
    config: WebSocketClientConfig,
    network: Arc<crate::module::transport::CompiledNetworkConfig>,
    common_engine: Arc<CommonEngine>,
) -> Result<(WebSocketClient, JoinHandle<()>), NetError> {
    on_common::log_t!(LogType::WSC; "create_client_worker", "thread_name|config", thread_name, format!("{:?}", config));
    let result: Result<(WebSocketClient, JoinHandle<()>), NetError> = (|| {
        if thread_name.contains('\0') {
            return Err(NetError::ConfigError);
        }
        let net_status_client = match network.network_status_policy() {
            crate::NetworkStatusPolicy::Ignore => None,
            crate::NetworkStatusPolicy::PauseOnUnavailable => Some(Arc::new(
                crate::module::net_status::inner::inner_net_status_client::InnerNetStatusClient::new(common_engine),
            )),
        };
        let (inner, worker) = WSClientInner::new_with_network(config, network, net_status_client)?;
        let client = WebSocketClient::from_inner(inner);
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let worker_thread = std::thread::Builder::new()
        .name(thread_name.to_string())
        .spawn(move || worker.run(ready_tx))
        .map_err(|error| {
            on_common::log_e!(LogType::WSC; "create_client_worker", "stage|error", "spawn_worker", on_common::log::summary::error(&error));
            NetError::RuntimeError
        })?;
        match ready_rx.recv_timeout(Duration::from_secs(5)) {
            Ok(Ok(())) => Ok((client, worker_thread)),
            Ok(Err(error)) => {
                let _ = worker_thread.join();
                Err(error)
            }
            Err(_) => {
                client.inner.request_shutdown();
                let _ = worker_thread.join();
                Err(NetError::TimeoutError)
            }
        }
    })();
    result.inspect_err(|error| {
        on_common::log_e!(LogType::WSC; "create_client_worker", "error|error_code", format!("{error:?}"), *error as i32);
    })
}

impl Drop for OpenNetInner {
    fn drop(&mut self) {
        self.stop_net_status_clients();
        #[cfg(feature = "ws-client")]
        {
            on_common::log_t!(LogType::WSC; "drop");
            let entries = self
                .clients
                .lock()
                .map(|mut clients| clients.drain().map(|(_, slot)| slot).collect::<Vec<_>>())
                .unwrap_or_default();
            for slot in entries {
                if let ClientSlot::Ready(entry) = slot {
                    entry.client.inner.request_shutdown();
                }
            }
        }
    }
}

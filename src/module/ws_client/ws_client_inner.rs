use crate::error::{ErrorKind, NetError};
use crate::module::net_status::inner::network_status_snapshot::NetworkStatusSnapshot;
use crate::module::net_status::inner::shared::NetworkLease;
use crate::module::transport::compiled_network_config::CompiledNetworkConfig;
use crate::module::transport::failure::{ConnectStage, ConnectionFailure};
use crate::module::ws_client::callback_event::CallbackEvent;
use crate::module::ws_client::callback_executor::DataCallbackPool;
use crate::module::ws_client::client_command::ClientCommand;
use crate::module::ws_client::connection_session::ConnectionSession;
use crate::module::ws_client::connection_status::ConnectionStatus;
use crate::module::ws_client::data_subscription_executor::DataSubscriptionExecutor;
use crate::module::ws_client::listener_executor::ListenerExecutor;
use crate::module::ws_client::listener_store::ListenerStore;
use crate::module::ws_client::message_source::{MessageResources, MessageSource};
use crate::module::ws_client::native_pending::NativePending;
use crate::module::ws_client::native_task_observer::NativeTaskObserver;
use crate::module::ws_client::session_runtime::SessionRuntime;
use crate::module::ws_client::write::priority_write_queue::PriorityWriteQueue;
use crate::module::ws_client::ws_client_worker::WSClientWorker;
use crate::ws::{
    ClientId, ConnectOptions, JournalOptions, SessionId, TaskEndCause, TerminationReason,
    WebSocketClientConfig,
};
use crate::NetworkConfig;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};
use tokio::sync::{mpsc, oneshot, Notify, Semaphore};
use tokio_util::sync::CancellationToken;

static NEXT_CLIENT_INSTANCE_ID: AtomicU64 = AtomicU64::new(0);

#[path = "ws_client_inner/session_close.rs"]
mod session_close;

/// The first shutdown initiator is selected before publishing cancellation.
#[derive(Default)]
pub(super) struct ShutdownReason(AtomicU8);
impl ShutdownReason {
    pub(super) fn client_shutdown(&self) {
        let _ = self
            .0
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire);
    }
    fn engine_dropped(&self) {
        let _ = self
            .0
            .compare_exchange(0, 2, Ordering::AcqRel, Ordering::Acquire);
    }
    pub(super) fn selected(&self) -> TerminationReason {
        if self.0.load(Ordering::Acquire) == 2 {
            TerminationReason::EngineDropped
        } else {
            TerminationReason::ClientShutdown
        }
    }
}

/// Client-wide worker and resource ownership. Session admission retains only a
/// weak lifecycle reference and therefore never keeps a completed session alive.
pub(crate) struct WSClientInner {
    config: Arc<WebSocketClientConfig>,
    network_config: Arc<NetworkConfig>,
    close_timeout: std::time::Duration,
    network_lease: Option<NetworkLease>,
    instance_id: ClientId,
    next_session_id: AtomicU64,
    session_admission: Mutex<Weak<ConnectionSession>>,
    admission_failed: AtomicBool,
    task_observers: Arc<NativeTaskObserver>,
    message_resources: Arc<MessageResources>,
    pending_slots: Arc<Semaphore>,
    data_callback_tx: mpsc::Sender<CallbackEvent>,
    data_callback_pool: Arc<DataCallbackPool>,
    command_tx: mpsc::Sender<ClientCommand>,
    queue: Arc<PriorityWriteQueue>,
    urgent_queue: Arc<PriorityWriteQueue>,
    listeners: Arc<ListenerStore>,
    shutdown: CancellationToken,
    shutdown_reason: Arc<ShutdownReason>,
    shutdown_complete: CancellationToken,
    network_available: Arc<Notify>,
}

impl WSClientInner {
    pub(crate) fn new_with_network(
        config: WebSocketClientConfig,
        network: Arc<CompiledNetworkConfig>,
        network_observation: Option<(
            NetworkLease,
            tokio::sync::watch::Receiver<NetworkStatusSnapshot>,
        )>,
    ) -> Result<(Arc<Self>, WSClientWorker), NetError> {
        config.validate()?;
        let queue = PriorityWriteQueue::new(
            config.queues.normal.max_items,
            config.queues.normal.max_bytes,
        )?;
        let urgent_queue = PriorityWriteQueue::new(
            config.queues.urgent.max_items,
            config.queues.urgent.max_bytes,
        )?;
        let (command_tx, command_rx) = mpsc::channel(config.queues.commands);
        let (io_event_tx, io_event_rx) = mpsc::channel(config.queues.io_events);
        let (data_callback_tx, data_callback_rx) =
            mpsc::channel(config.dispatch.incoming.max_items);
        let data_callback_pool = Arc::new(DataCallbackPool::new(
            config.dispatch.message_callback_workers,
        ));
        let state = Arc::new(RwLock::new(ConnectionStatus::Idle));
        let listeners = Arc::new(ListenerStore::new(&config)?);
        let shutdown = CancellationToken::new();
        let shutdown_reason = Arc::new(ShutdownReason::default());
        let shutdown_complete = CancellationToken::new();
        let network_available = Arc::new(Notify::new());
        let instance_id = ClientId::allocate(&NEXT_CLIENT_INSTANCE_ID)?;
        let task_executor = ListenerExecutor::new(
            &format!("open-net-task-events-{instance_id}"),
            config.dispatch.task_callback_workers,
            config.dispatch.task_subscriptions,
        )?;
        let task_observers = NativeTaskObserver::new(task_executor, &config.dispatch)?;
        let message_resources = MessageResources::new(&config.dispatch)?;
        let (network_lease, network_status) = match network_observation {
            Some((lease, receiver)) => (Some(lease), Some(receiver)),
            None => (None, None),
        };
        // Historical shared losses predate this client's connections. Future
        // epochs are still checked by the gate, establishment and I/O paths.
        let network_loss_epoch = network_status.as_ref().map_or(0, |receiver| {
            ::tokio::sync::watch::Receiver::borrow(receiver).loss_epoch
        });
        let inner = Arc::new(Self {
            config: Arc::new(config.clone()),
            network_config: network.original(),
            close_timeout: config.close_timeout,
            network_lease: network_lease.clone(),
            instance_id,
            next_session_id: AtomicU64::new(0),
            session_admission: Mutex::new(Weak::new()),
            admission_failed: AtomicBool::new(false),
            task_observers: Arc::clone(&task_observers),
            message_resources,
            pending_slots: Arc::new(Semaphore::new(config.requests.max_pending)),
            data_callback_tx: data_callback_tx.clone(),
            data_callback_pool: Arc::clone(&data_callback_pool),
            command_tx,
            queue: Arc::clone(&queue),
            urgent_queue: Arc::clone(&urgent_queue),
            listeners: Arc::clone(&listeners),
            shutdown: shutdown.clone(),
            shutdown_reason: Arc::clone(&shutdown_reason),
            shutdown_complete: shutdown_complete.clone(),
            network_available: Arc::clone(&network_available),
        });
        let worker = WSClientWorker {
            network_lease,
            network_status,
            network_loss_epoch,
            task_observers,
            network,
            context_provider_slots: Arc::new(Semaphore::new(
                config.dispatch.blocking_handshake_jobs,
            )),
            config,
            command_rx,
            io_event_tx,
            io_event_rx,
            data_callback_tx,
            data_callback_rx: Some(data_callback_rx),
            data_callback_pool,
            queue,
            urgent_queue,
            state,
            listeners,
            shutdown,
            shutdown_reason,
            shutdown_complete,
            network_available,
            generation: 0,
            connect_target: None,
            connect_cancel: None,
            connect_handle: None,
            active_io: None,
        };
        Ok((inner, worker))
    }

    pub(crate) fn id(&self) -> ClientId {
        self.instance_id
    }
    pub(crate) fn config(&self) -> &WebSocketClientConfig {
        &self.config
    }
    pub(crate) fn network_config(&self) -> &NetworkConfig {
        &self.network_config
    }
    pub(crate) fn is_shutdown(&self) -> bool {
        self.shutdown.is_cancelled() || self.admission_failed.load(Ordering::Acquire)
    }

    pub(crate) async fn start_session(
        self: &Arc<Self>,
        options: ConnectOptions,
        journal: Option<JournalOptions>,
    ) -> Result<crate::ws::Session, NetError> {
        options.validate()?;
        if let Some(journal) = &journal {
            journal.validate()?;
        }
        if self.is_shutdown() {
            return Err(NetError::from(ErrorKind::Closed));
        }
        let initial_connect_deadline = options
            .connect_timeout
            .map(|duration| {
                tokio::time::Instant::now()
                    .checked_add(duration)
                    .ok_or_else(|| {
                        NetError::config(
                            "connect_timeout",
                            "cannot represent initial connection deadline",
                        )
                    })
            })
            .transpose()?;
        let previous;
        let (runtime, journal) = {
            let mut admission = self
                .session_admission
                .lock()
                .map_err(NetError::from_poison)?;
            // Coordinate with failed-session recovery under the same gate.
            if self.is_shutdown() {
                return Err(NetError::from(ErrorKind::Closed));
            }
            previous = admission.upgrade();
            if previous
                .as_ref()
                .is_some_and(|session| !session.completion_token().is_cancelled())
            {
                return Err(NetError::from(ErrorKind::SessionAlreadyExists));
            }
            let id = SessionId::allocate(&self.next_session_id)?;
            let (lifecycle, journal) = ConnectionSession::new_with_observers(
                self.instance_id.as_u64(),
                id.as_u64(),
                journal,
                Arc::new(options.metadata.clone()),
                options.event_history.clone(),
                self.listeners.connection_observers(),
            )?;
            let pending = NativePending::new(
                self.instance_id,
                id,
                self.config.requests.clone(),
                self.config.dispatch.incoming.max_items,
                Arc::clone(&self.pending_slots),
            )?;
            let executor = Arc::new(DataSubscriptionExecutor::for_session(
                self.data_callback_tx.clone(),
                id,
                Arc::clone(&self.data_callback_pool),
            ));
            let messages = MessageSource::new(
                Arc::clone(&self.message_resources),
                executor,
                options.initial_messages.clone(),
                matches!(options.routing, crate::ws::ResponseRouting::Manual),
            )?;
            let tasks = self.task_observers.for_session()?;
            let runtime = SessionRuntime::new(
                self.instance_id,
                id,
                Arc::clone(&lifecycle),
                pending,
                messages,
                tasks,
                options.routing.clone(),
                Arc::clone(&self.queue),
                Arc::clone(&self.urgent_queue),
                self.shutdown.clone(),
            );
            *admission = Arc::downgrade(&lifecycle);
            (runtime, journal)
        };
        drop(previous);
        let session = crate::ws::Session::new(
            Arc::clone(self),
            Arc::clone(&runtime),
            journal,
            options.metadata.clone(),
        );
        let (reply, received) = oneshot::channel();
        if self
            .command_tx
            .send(ClientCommand::Connect {
                options,
                initial_connect_deadline,
                session: Arc::clone(&runtime.lifecycle),
                runtime: Arc::clone(&runtime),
                reply,
            })
            .await
            .is_err()
        {
            let error = NetError::from(ErrorKind::EngineDropped);
            runtime.end(error.clone(), TaskEndCause::Failed);
            runtime.lifecycle.terminate(
                TerminationReason::EngineDropped,
                Some(ConnectionFailure::new(
                    error.clone(),
                    ConnectStage::EventDelivery,
                    None,
                    false,
                )),
            )?;
            return Err(error);
        }
        received
            .await
            .map_err(|error| NetError::with_source(ErrorKind::EngineDropped, error))??;
        Ok(session)
    }

    pub(crate) fn notify_network_available(&self) {
        self.network_available.notify_waiters();
    }
    pub(crate) fn request_engine_drop(&self) {
        self.shutdown_reason.engine_dropped();
        self.request_shutdown();
    }
    pub(crate) fn request_shutdown(&self) {
        self.shutdown_reason.client_shutdown();
        self.task_observers.close();
        self.pending_slots.close();
        self.shutdown.cancel();
        if let Some(lease) = &self.network_lease {
            lease.release();
        }
        let _ = self.command_tx.try_send(ClientCommand::Shutdown);
    }
    pub(crate) async fn shutdown(&self) -> Result<(), NetError> {
        if self.shutdown_complete.is_cancelled() {
            return Ok(());
        }
        self.request_shutdown();
        tokio::select! {
            biased;
            _ = self.shutdown_complete.cancelled() => Ok(()),
            _ = self.command_tx.closed() => {
                if self.shutdown_complete.is_cancelled() { Ok(()) }
                else { Err(NetError::from(ErrorKind::EngineDropped)) }
            }
        }
    }
}

impl Drop for WSClientInner {
    fn drop(&mut self) {
        self.request_engine_drop();
    }
}

#[cfg(test)]
#[path = "ws_client_inner/channel_config_tests.rs"]
mod channel_config_tests;

#[cfg(test)]
#[path = "ws_client_inner/context_tests.rs"]
mod context_tests;

#[cfg(test)]
#[path = "ws_client_inner/session_admission_tests.rs"]
mod session_admission_tests;

#[cfg(test)]
#[path = "ws_client_inner/network_admission_tests.rs"]
mod network_admission_tests;

#[cfg(test)]
#[path = "ws_client_inner/network_recovery_tests.rs"]
mod network_recovery_tests;

#[cfg(test)]
#[path = "ws_client_inner/task_observation_tests.rs"]
mod task_observation_tests;

#[cfg(test)]
#[path = "ws_client_inner/deadline_tests.rs"]
mod deadline_tests;

#[cfg(test)]
#[path = "ws_client_inner/registration_tests.rs"]
mod registration_tests;

#[cfg(test)]
#[path = "ws_client_inner/message_dispatch_tests.rs"]
mod message_dispatch_tests;

#[cfg(test)]
#[path = "ws_client_inner/cancel_domain_race_tests.rs"]
mod cancel_domain_race_tests;

#[cfg(test)]
#[path = "ws_client_inner/cancel_domain_admission_tests.rs"]
mod cancel_domain_admission_tests;

#[cfg(test)]
#[path = "ws_client_inner/configuration_contract_tests.rs"]
mod configuration_contract_tests;

#[cfg(test)]
#[path = "ws_client_inner/status_listener_tests.rs"]
mod status_listener_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::module::ws_client::test_support::{check_eq, TestResult};
    #[test]
    fn first_shutdown_initiator_owns_terminal_reason() -> TestResult {
        let engine_first = ShutdownReason::default();
        engine_first.engine_dropped();
        engine_first.client_shutdown();
        check_eq!(engine_first.selected(), TerminationReason::EngineDropped)?;
        let client_first = ShutdownReason::default();
        client_first.client_shutdown();
        client_first.engine_dropped();
        check_eq!(client_first.selected(), TerminationReason::ClientShutdown)?;
        Ok(())
    }
}

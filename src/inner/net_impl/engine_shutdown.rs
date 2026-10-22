//! Engine retirement is owned outside the runtime being retired.
use super::OpenNetInner;
use crate::common::CommonEngine;
use crate::error::{ErrorKind, ErrorStage, NetError};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{mpsc, Arc, Mutex, OnceLock};
use std::thread::JoinHandle;
use std::time::Duration;
use tokio::sync::watch;

type Outcome = Result<(), NetError>;

pub(super) struct EngineShutdown {
    // First initiator wins: explicit shutdown = 1, owner Drop = 2.
    cause: AtomicU8,
    signal: mpsc::Sender<()>,
    retirement: Mutex<Option<Retirement>>,
    completed: watch::Receiver<Option<Outcome>>,
    reaper: mpsc::Sender<Retirement>,
}

struct Retirement {
    worker: JoinHandle<Outcome>,
    completed: watch::Sender<Option<Outcome>>,
}

// This process-level service only joins finished cleanup coordinators. A blocked
// engine never prevents another engine from publishing its completion. It owns
// no engine runtime. Completion wakers execute in a marked SDK context so they
// cannot synchronously wait on this service to publish another completion.
fn reaper() -> Result<mpsc::Sender<Retirement>, NetError> {
    static REAPER: OnceLock<Result<mpsc::Sender<Retirement>, String>> = OnceLock::new();
    REAPER
        .get_or_init(|| {
            let (sender, receiver) = mpsc::channel::<Retirement>();
            std::thread::Builder::new()
                .name("open-net-retirement-reaper".into())
                .spawn(move || {
                    let _scope = crate::common::retirement::ExecutionScope::enter();
                    let mut pending = Vec::<Retirement>::new();
                    loop {
                        if pending.is_empty() {
                            match receiver.recv() {
                                Ok(job) => pending.push(job),
                                Err(_) => return,
                            }
                        } else if let Ok(job) = receiver.recv_timeout(Duration::from_millis(2)) {
                            pending.push(job);
                        }
                        let mut index = 0;
                        while index < pending.len() {
                            if !pending[index].worker.is_finished() {
                                index += 1;
                                continue;
                            }
                            let job = pending.swap_remove(index);
                            let outcome = match job.worker.join() {
                                Ok(outcome) => outcome,
                                Err(payload) => {
                                    // Unexpected coordinator failure: never execute a
                                    // user destructor on the shared reaper.
                                    std::mem::forget(payload);
                                    Err(NetError::from(ErrorKind::Internal)
                                        .with_stage(ErrorStage::Close))
                                }
                            };
                            // Published after the coordinator itself has exited.
                            if let Err(payload) =
                                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                    job.completed.send_replace(Some(outcome));
                                }))
                            {
                                // A hostile waiter must not kill completion for other engines.
                                std::mem::forget(payload);
                            }
                        }
                    }
                })
                .map(|_| sender)
                .map_err(|error| error.to_string())
        })
        .clone()
        .map_err(|_| NetError::from(ErrorKind::RuntimeUnavailable).with_stage(ErrorStage::Runtime))
}

fn discard_panic(payload: Box<dyn std::any::Any + Send>) {
    if let Err(secondary) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(payload)))
    {
        std::mem::forget(secondary);
    }
}

impl EngineShutdown {
    pub(super) fn new(inner: &OpenNetInner) -> Result<Self, NetError> {
        let reaper = reaper()?;
        let resources = Resources::new(inner);
        let (signal, requested) = mpsc::channel();
        let (completed, receiver) = watch::channel(None);
        let worker = std::thread::Builder::new()
            .name("open-net-engine-cleanup".into())
            .spawn(move || {
                let _scope = crate::common::retirement::ExecutionScope::enter();
                let _ = requested.recv();
                match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| resources.retire()))
                {
                    Ok(result) => result,
                    Err(payload) => {
                        discard_panic(payload);
                        Err(NetError::from(ErrorKind::Internal).with_stage(ErrorStage::Close))
                    }
                }
            })
            .map_err(|error| {
                NetError::with_source(ErrorKind::RuntimeUnavailable, error)
                    .with_stage(ErrorStage::Runtime)
            })?;
        Ok(Self {
            cause: AtomicU8::new(0),
            signal,
            retirement: Mutex::new(Some(Retirement { worker, completed })),
            completed: receiver,
            reaper,
        })
    }

    pub(super) fn mark(&self, dropped: bool) -> bool {
        let cause = if dropped { 2 } else { 1 };
        let selected =
            match self
                .cause
                .compare_exchange(0, cause, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => cause,
                Err(selected) => selected,
            };
        selected == 2
    }

    pub(super) fn is_closing(&self) -> bool {
        self.cause.load(Ordering::Acquire) != 0
    }

    pub(super) fn launch(&self) {
        let job = self
            .retirement
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if let Some(job) = job {
            let _ = self.signal.send(());
            // The receiver is process-owned and has no normal shutdown path.
            // A lost service must be observable, never treated as successful exit.
            if let Err(error) = self.reaper.send(job) {
                error
                    .0
                    .completed
                    .send_replace(Some(Err(NetError::from(ErrorKind::RuntimeUnavailable))));
            }
        }
    }

    pub(super) async fn wait(&self) -> Outcome {
        let mut receiver = self.completed.clone();
        loop {
            if let Some(result) = receiver.borrow_and_update().clone() {
                return result;
            }
            receiver
                .changed()
                .await
                .map_err(|_| NetError::from(ErrorKind::RuntimeUnavailable))?;
        }
    }
}

struct Resources {
    common: Arc<CommonEngine>,
    #[cfg(feature = "net-status")]
    network: Arc<crate::module::net_status::inner::shared::SharedNetworkService>,
    #[cfg(feature = "net-status")]
    monitors: Arc<
        Mutex<std::collections::HashMap<String, super::net_status_clients::NetStatusClientSlot>>,
    >,
    #[cfg(feature = "ws-client")]
    ws: Arc<Mutex<std::collections::HashMap<String, super::client_slot::ClientSlot>>>,
    #[cfg(feature = "ws-client")]
    ws_changed: Arc<tokio::sync::Notify>,
    #[cfg(feature = "http-client")]
    http: Arc<Mutex<std::collections::HashMap<String, super::http_client_slot::HttpClientSlot>>>,
}

impl Resources {
    fn new(inner: &OpenNetInner) -> Self {
        Self {
            common: inner.common_engine.clone(),
            #[cfg(feature = "net-status")]
            network: inner.network_status.clone(),
            #[cfg(feature = "net-status")]
            monitors: inner.net_status_clients.clone(),
            #[cfg(feature = "ws-client")]
            ws: inner.clients.clone(),
            #[cfg(feature = "ws-client")]
            ws_changed: inner.clients_changed.clone(),
            #[cfg(feature = "http-client")]
            http: inner.http_clients.clone(),
        }
    }

    fn retire(self) -> Outcome {
        let mut outcome = self.common.drain_management().map_err(NetError::from);
        #[cfg(feature = "ws-client")]
        {
            let entries: Vec<_> = self
                .ws
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .drain()
                .map(|(_, slot)| slot)
                .collect();
            self.ws_changed.notify_waiters();
            for slot in entries {
                if let super::client_slot::ClientSlot::Ready(entry) = slot {
                    entry.client.inner.request_shutdown();
                    if let Some(worker) = entry.worker_thread {
                        record(
                            &mut outcome,
                            worker.join().map_err(|_| {
                                NetError::from(ErrorKind::Internal).with_stage(ErrorStage::Close)
                            }),
                        );
                    }
                }
            }
        }
        #[cfg(feature = "http-client")]
        {
            let clients: Vec<_> = self
                .http
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .drain()
                .map(|(_, slot)| slot)
                .collect();
            for slot in clients {
                let result = self.common.runtime_handle().block_on(async move {
                    match slot {
                        super::http_client_slot::HttpClientSlot::Managed(client) => {
                            client.shutdown().await
                        }
                        slot => match slot.into_client() {
                            Some(client) => client.shutdown().await,
                            None => Ok(()),
                        },
                    }
                });
                record(&mut outcome, result);
            }
        }
        #[cfg(feature = "net-status")]
        {
            let monitors: Vec<_> = self
                .monitors
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .drain()
                .map(|(_, slot)| slot)
                .collect();
            for slot in monitors {
                let client = match slot {
                    super::net_status_clients::NetStatusClientSlot::Ready(client)
                    | super::net_status_clients::NetStatusClientSlot::Closing(client) => client,
                };
                record(
                    &mut outcome,
                    self.common.runtime_handle().block_on(client.destroy()),
                );
            }
            record(
                &mut outcome,
                self.common
                    .runtime_handle()
                    .block_on(self.network.shutdown()),
            );
        }
        record(
            &mut outcome,
            self.common.shutdown_blocking().map_err(NetError::from),
        );
        outcome
    }
}

fn record(result: &mut Outcome, next: Outcome) {
    if result.is_ok() {
        *result = next;
    }
}

impl OpenNetInner {
    pub(crate) fn ensure_open(&self) -> Outcome {
        if self
            .shutdown
            .as_ref()
            .is_some_and(EngineShutdown::is_closing)
        {
            Err(NetError::from(ErrorKind::Closed))
        } else {
            Ok(())
        }
    }

    pub(crate) fn begin_shutdown(&self, dropped: bool) {
        // A concurrent caller may launch cleanup while this caller is still
        // publishing terminal observations. Keep that publication in the barrier.
        let _publication = self.common_engine.retirement_work().register();
        let _scope = crate::common::retirement::ExecutionScope::enter();
        let Some(shutdown) = &self.shutdown else {
            return;
        };
        let dropped = shutdown.mark(dropped);
        // Snapshot under short registry locks; never wake user code while locked.
        #[cfg(feature = "net-status")]
        self.network_status.request_close();
        #[cfg(feature = "ws-client")]
        {
            let clients: Vec<_> = self
                .clients
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .values()
                .filter_map(|slot| match slot {
                    super::client_slot::ClientSlot::Ready(entry) => Some(entry.client.clone()),
                    _ => None,
                })
                .collect();
            for client in clients {
                if dropped {
                    client.inner.request_engine_drop();
                } else {
                    client.inner.request_shutdown();
                }
            }
            self.clients_changed.notify_waiters();
        }
        #[cfg(not(feature = "ws-client"))]
        let _ = dropped;
        #[cfg(feature = "http-client")]
        {
            let clients: Vec<_> = self
                .http_clients
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .values()
                .filter_map(|slot| match slot {
                    super::http_client_slot::HttpClientSlot::Ready(client)
                    | super::http_client_slot::HttpClientSlot::Closing(client) => {
                        Some(Ok(client.clone()))
                    }
                    super::http_client_slot::HttpClientSlot::Managed(client) => {
                        Some(Err(client.clone()))
                    }
                    _ => None,
                })
                .collect();
            for client in clients {
                match client {
                    Ok(client) => {
                        client.request_shutdown();
                        let _ = client.inner.cancel_all();
                    }
                    Err(client) => client.begin_shutdown(),
                }
            }
        }
        #[cfg(feature = "net-status")]
        {
            let clients: Vec<_> = self
                .net_status_clients
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .values()
                .map(|slot| match slot {
                    super::net_status_clients::NetStatusClientSlot::Ready(client)
                    | super::net_status_clients::NetStatusClientSlot::Closing(client) => {
                        client.clone()
                    }
                })
                .collect();
            for client in clients {
                client.request_destroy();
            }
        }
        shutdown.launch();
    }

    pub(crate) async fn shutdown(&self) -> Outcome {
        self.begin_shutdown(false);
        let wait = self
            .shutdown
            .as_ref()
            .ok_or_else(|| NetError::from(ErrorKind::Internal).with_stage(ErrorStage::Close))?
            .wait();
        tokio::pin!(wait);
        std::future::poll_fn(|cx| {
            if crate::common::retirement::ExecutionScope::is_current()
                || crate::api::subscription::CallbackScope::is_current()
                || tokio::runtime::Handle::try_current().is_ok_and(|current| current.id() == self.common_engine.runtime_handle().id())
            {
                return std::task::Poll::Ready(Err(NetError::input("open_net.shutdown", "await engine shutdown outside SDK callbacks and workers; use begin_shutdown inside callbacks").with_stage(ErrorStage::Close)));
            }
            std::future::Future::poll(wait.as_mut(), cx)
        }).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_shutdown_owner_returns_internal_close_error(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut inner = OpenNetInner::new_with_config(
            crate::OpenNetConfig::default().with_runtime_worker_threads(1),
        )?;
        let owner = inner.shutdown.take().ok_or("shutdown owner missing")?;
        let result = futures::executor::block_on(inner.shutdown());
        // Restore the owner before checking the injected failure so ordinary
        // cleanup still joins the runtime and all retirement work.
        inner.shutdown = Some(owner);
        futures::executor::block_on(inner.shutdown())?;
        let error = result.err().ok_or("missing shutdown owner was accepted")?;
        assert_eq!(error.kind(), ErrorKind::Internal);
        assert_eq!(error.context().stage, Some(ErrorStage::Close));
        Ok(())
    }
}

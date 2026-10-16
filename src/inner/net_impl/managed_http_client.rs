//! Fixed-configuration HTTP lifecycle ownership, independent of caller executors.
use super::http_client_slot::HttpClientSlot;
use super::OpenNetInner;
use crate::api::http::{HttpClient, HttpClientConfig, ManagedHttpClient};
use crate::common::CommonEngine;
use crate::error::{ErrorKind, ErrorStage, NetError};
use crate::net_status::NetworkStatusContext;
use std::collections::HashMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use tokio::sync::watch;

type Registry = Mutex<HashMap<String, HttpClientSlot>>;
type Outcome<T> = Result<T, NetError>;

fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn closed() -> NetError {
    NetError::from(ErrorKind::Closed)
}

fn runtime_failed() -> NetError {
    NetError::from(ErrorKind::RuntimeUnavailable).with_stage(ErrorStage::Runtime)
}

struct Completion<T> {
    result: watch::Sender<Option<Outcome<T>>>,
}

impl<T: Clone> Completion<T> {
    fn new() -> Self {
        Self {
            result: watch::channel(None).0,
        }
    }

    fn finish(&self, result: Outcome<T>) {
        self.result.send_if_modified(|stored| {
            if stored.is_some() {
                false
            } else {
                *stored = Some(result.clone());
                true
            }
        });
    }

    async fn wait(&self) -> Outcome<T> {
        let mut receiver = self.result.subscribe();
        loop {
            if let Some(result) = receiver.borrow_and_update().clone() {
                return result;
            }
            receiver.changed().await.map_err(|_| runtime_failed())?;
        }
    }
}

enum Phase {
    Idle,
    Creating(Arc<Completion<HttpClient>>),
    Ready(HttpClient),
    Closing(HttpClient),
    Closed,
}

struct State {
    closed: bool,
    phase: Phase,
}

pub(crate) struct ManagedHttpClientInner {
    name: String,
    config: HttpClientConfig,
    engine: Weak<CommonEngine>,
    registry: Weak<Registry>,
    context: NetworkStatusContext,
    state: Mutex<State>,
    finished: Completion<()>,
    #[cfg(test)]
    creation_hook: Mutex<Option<Arc<dyn Fn() -> Outcome<()> + Send + Sync>>>,
    #[cfg(test)]
    before_publish: Mutex<Option<Arc<dyn Fn(&HttpClient) + Send + Sync>>>,
    #[cfg(test)]
    cleanup_abort: Mutex<Option<tokio::task::AbortHandle>>,
}

impl OpenNetInner {
    pub(crate) fn managed_http(
        &self,
        name: String,
        config: HttpClientConfig,
    ) -> Outcome<ManagedHttpClient> {
        config.validate()?;
        let inner = Arc::new(ManagedHttpClientInner {
            name: name.clone(),
            config,
            engine: Arc::downgrade(&self.common_engine),
            registry: Arc::downgrade(&self.http_clients),
            context: self.network_status.context(),
            state: Mutex::new(State {
                closed: false,
                phase: Phase::Idle,
            }),
            finished: Completion::new(),
            #[cfg(test)]
            creation_hook: Mutex::new(None),
            #[cfg(test)]
            before_publish: Mutex::new(None),
            #[cfg(test)]
            cleanup_abort: Mutex::new(None),
        });
        let mut registry = self.http_clients.lock().map_err(NetError::from_poison)?;
        self.ensure_open()?;
        if registry.contains_key(&name) {
            return Err(NetError::from(ErrorKind::ClientAlreadyExists));
        }
        registry.insert(name, HttpClientSlot::Managed(inner.clone()));
        Ok(ManagedHttpClient { inner })
    }
}

impl ManagedHttpClientInner {
    pub(crate) async fn get(self: &Arc<Self>) -> Outcome<HttpClient> {
        if self.context.is_closed() {
            self.begin_shutdown();
            return Err(closed());
        }
        let (round, start) = {
            let mut state = locked(&self.state);
            if state.closed {
                return Err(closed());
            }
            match &state.phase {
                Phase::Ready(client) => {
                    return if client.inner.is_shutdown_requested() {
                        Err(closed())
                    } else {
                        Ok(client.clone())
                    };
                }
                Phase::Creating(round) => (round.clone(), None),
                Phase::Idle => {
                    let engine = self.engine.upgrade().ok_or_else(closed)?;
                    let round = Arc::new(Completion::new());
                    state.phase = Phase::Creating(round.clone());
                    let work = Creation {
                        owner: self.clone(),
                        round: round.clone(),
                        engine,
                        armed: true,
                    };
                    (round, Some(work))
                }
                Phase::Closing(_) | Phase::Closed => return Err(closed()),
            }
        };
        if let Some(work) = start {
            // The closure owns the guard before its first poll/run. Abandoning
            // this get future cannot strand a reservation or cancel creation.
            let runtime = work.engine.runtime_handle();
            runtime.spawn_blocking(move || work.run());
        }
        let client = round.wait().await?;
        // A successful round can have completed immediately before shutdown.
        // Never hand its cached result out as a newly available client afterward.
        let state = locked(&self.state);
        if state.closed || client.inner.is_shutdown_requested() || self.context.is_closed() {
            Err(closed())
        } else {
            Ok(client)
        }
    }

    pub(super) fn get_registered(&self) -> Outcome<HttpClient> {
        let state = locked(&self.state);
        if !state.closed && !self.context.is_closed() {
            if let Phase::Ready(client) = &state.phase {
                if !client.inner.is_shutdown_requested() {
                    return Ok(client.clone());
                }
            }
        }
        Err(NetError::from(ErrorKind::ConnectionClosing))
    }

    pub(crate) fn begin_shutdown(self: &Arc<Self>) {
        enum Action {
            Complete,
            Creating(Arc<Completion<HttpClient>>),
            Start(HttpClient),
            Signal(HttpClient),
            None,
        }
        let action = {
            let mut state = locked(&self.state);
            state.closed = true;
            match &state.phase {
                Phase::Idle => {
                    state.phase = Phase::Closed;
                    Action::Complete
                }
                Phase::Creating(round) => Action::Creating(round.clone()),
                Phase::Ready(client) => {
                    let client = client.clone();
                    state.phase = Phase::Closing(client.clone());
                    Action::Start(client)
                }
                Phase::Closing(client) => Action::Signal(client.clone()),
                Phase::Closed => Action::None,
            }
        };
        // Notify outside the lifecycle lock: waking callers may run user wakers.
        match action {
            Action::Complete => self.finish_shutdown(Ok(()), true),
            Action::Creating(round) => round.finish(Err(closed())),
            Action::Start(client) => self.start_cleanup(client, self.engine.upgrade()),
            Action::Signal(client) => signal_close(&client),
            Action::None => {}
        }
    }

    pub(crate) async fn shutdown(self: &Arc<Self>) -> Outcome<()> {
        self.begin_shutdown();
        self.finished.wait().await
    }

    fn complete_creation(
        self: &Arc<Self>,
        round: &Arc<Completion<HttpClient>>,
        engine: &Arc<CommonEngine>,
        result: Outcome<HttpClient>,
    ) {
        let (result, cleanup, finish) = {
            let mut state = locked(&self.state);
            if !matches!(&state.phase, Phase::Creating(current) if Arc::ptr_eq(current, round)) {
                return;
            }
            state.closed |= self.context.is_closed();
            match result {
                Ok(client) if state.closed => {
                    state.phase = Phase::Closing(client.clone());
                    (Err(closed()), Some(client), false)
                }
                Ok(client) => {
                    state.phase = Phase::Ready(client.clone());
                    (Ok(client), None, false)
                }
                Err(error) if state.closed => {
                    state.phase = Phase::Closed;
                    let _ = error;
                    (Err(closed()), None, true)
                }
                Err(error) => {
                    state.phase = Phase::Idle;
                    (Err(error), None, false)
                }
            }
        };
        if let Some(client) = cleanup {
            self.start_cleanup(client, Some(engine.clone()));
        }
        if finish {
            self.finish_shutdown(Ok(()), true);
        }
        round.finish(result);
    }

    fn start_cleanup(self: &Arc<Self>, client: HttpClient, engine: Option<Arc<CommonEngine>>) {
        // Keep the raw handle in Closing so every concurrent begin_shutdown
        // caller closes it before returning, even if this caller is preempted.
        signal_close(&client);
        let Some(engine) = engine else {
            self.finish_shutdown(Err(runtime_failed()), false);
            return;
        };
        let runtime = engine.runtime_handle();
        let guard = CleanupGuard(Some(CleanupWork {
            owner: self.clone(),
            client,
            engine,
        }));
        let task = runtime.spawn(guard.run());
        #[cfg(test)]
        {
            *locked(&self.cleanup_abort) = Some(task.abort_handle());
        }
        drop(task);
    }

    fn finish_shutdown(self: &Arc<Self>, result: Outcome<()>, retired: bool) {
        if retired {
            locked(&self.state).phase = Phase::Closed;
            if let Some(registry) = self.registry.upgrade() {
                let mut registry = locked(&registry);
                if matches!(registry.get(&self.name), Some(HttpClientSlot::Managed(current)) if Arc::ptr_eq(current, self))
                {
                    registry.remove(&self.name);
                }
            }
        }
        // A failure without confirmed worker retirement is observable, but does
        // not release the name for a potentially overlapping new client.
        self.finished.finish(result);
    }
}

fn signal_close(client: &HttpClient) {
    client.inner.request_shutdown();
    let _ = client.inner.cancel_all();
}

struct Creation {
    owner: Arc<ManagedHttpClientInner>,
    round: Arc<Completion<HttpClient>>,
    engine: Arc<CommonEngine>,
    armed: bool,
}

impl Creation {
    fn run(mut self) {
        let result = catch_unwind(AssertUnwindSafe(|| {
            #[cfg(test)]
            if let Some(hook) = locked(&self.owner.creation_hook).clone() {
                hook()?;
            }
            // Shutdown before this job began must not create otherwise unused workers.
            if locked(&self.owner.state).closed || self.owner.context.is_closed() {
                return Err(closed());
            }
            let client = crate::module::http::http_client_inner::create_http_client(
                self.owner.name.clone(),
                self.owner.config.clone(),
                self.owner.context.clone(),
            )?;
            #[cfg(test)]
            {
                let hook = locked(&self.owner.before_publish).clone();
                if let Some(hook) = hook {
                    hook(&client);
                }
            }
            Ok(client)
        }))
        .unwrap_or_else(|_| Err(runtime_failed()));
        self.owner
            .complete_creation(&self.round, &self.engine, result);
        self.armed = false;
    }
}

impl Drop for Creation {
    fn drop(&mut self) {
        if self.armed {
            self.owner
                .complete_creation(&self.round, &self.engine, Err(runtime_failed()));
        }
    }
}

struct CleanupWork {
    owner: Arc<ManagedHttpClientInner>,
    client: HttpClient,
    // An active operation retains runtime resources, never the public root.
    engine: Arc<CommonEngine>,
}

impl CleanupWork {
    async fn clean(&self) -> Outcome<()> {
        self.client.inner.request_shutdown();
        let cancellation = self.client.inner.cancel_all();
        let joined = self.client.inner.shutdown_and_join().await;
        cancellation.and(joined)
    }

    fn finish(self, result: Outcome<()>) {
        self.owner
            .finish_shutdown(result, self.client.inner.workers_joined());
    }

    fn fallback(self) {
        let _scope = crate::common::retirement::ExecutionScope::enter();
        // A cancelled SDK task must not lose the completion result or release
        // its name early. An ordinary thread can wait on the retained runtime
        // without occupying that runtime's bounded blocking pool itself.
        let threads = self.engine.resource_threads();
        let pending = Arc::new(Mutex::new(Some(self)));
        let transferred = pending.clone();
        let spawned = threads.spawn("open-net-managed-http-cleanup", move || {
            let Some(work) = locked(&transferred).take() else {
                return;
            };
            let result = catch_unwind(AssertUnwindSafe(|| {
                work.engine.runtime_handle().block_on(work.clean())
            }))
            .unwrap_or_else(|payload| {
                crate::common::retirement::discard_panic_payload(payload);
                Err(runtime_failed())
            });
            work.finish(result.and(Err(runtime_failed())));
        });
        if spawned.is_err() {
            if let Some(work) = locked(&pending).take() {
                signal_close(&work.client);
                work.owner.finish_shutdown(Err(runtime_failed()), false);
            }
        }
    }
}

struct CleanupGuard(Option<CleanupWork>);

impl CleanupGuard {
    async fn run(mut self) {
        let Some(work) = self.0.as_ref() else {
            return;
        };
        let result = work.clean().await;
        if let Some(work) = self.0.take() {
            work.finish(result);
        }
    }
}

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        if let Some(work) = self.0.take() {
            work.fallback();
        }
    }
}

#[cfg(test)]
#[path = "managed_http_client_tests.rs"]
mod tests;

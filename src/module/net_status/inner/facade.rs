//! Per-client network view. Only the engine service controls the detector.
use std::sync::{Arc, Mutex};

use crate::common::CommonEngine;
use crate::error::{ErrorKind, NetError};
use crate::net_status::{MonitorState, NetworkSnapshot};
use crate::subscription::common_executor::CommonCallbackExecutor;
use crate::subscription::{StatePublication, StatePublisher, StateReceiver, StateSource};
use tokio::sync::{oneshot, watch, Notify};

use super::network_view::NetworkView;
use super::shared::{CleanupTicket, NetworkLease, NetworkStatusContext};

struct StartCycle {
    result: Mutex<Option<Result<NetworkSnapshot, NetError>>>,
    changed: Arc<Notify>,
}
impl StartCycle {
    fn new() -> Self {
        Self {
            result: Mutex::new(None),
            changed: Arc::new(Notify::new()),
        }
    }
    fn result(&self) -> Result<Option<Result<NetworkSnapshot, NetError>>, NetError> {
        Ok(self.result.lock().map_err(NetError::from_poison)?.clone())
    }
    // Prepare the local outcome before dispatching arbitrary caller wakers.
    fn complete(&self, result: Result<NetworkSnapshot, NetError>) -> Result<Arc<Notify>, NetError> {
        let mut current = self.result.lock().map_err(NetError::from_poison)?;
        if current.is_none() {
            *current = Some(result);
        }
        Ok(self.changed.clone())
    }
    async fn wait(&self) -> Result<NetworkSnapshot, NetError> {
        loop {
            let notification = self.changed.notified();
            tokio::pin!(notification);
            notification.as_mut().enable();
            if let Some(result) = self.result()? {
                return result;
            }
            notification.await;
        }
    }
}
struct LocalState {
    view: NetworkView,
    lease: Option<NetworkLease>,
    tickets: Vec<CleanupTicket>,
    cycle: Arc<StartCycle>,
    failure: Option<NetError>,
}

pub(crate) struct NetworkObservationOwner {
    context: NetworkStatusContext,
    engine: Arc<CommonEngine>,
    local: Mutex<LocalState>,
    publisher: StatePublisher<NetworkSnapshot>,
    source: StateSource<NetworkSnapshot>,
    bridge_stop: Mutex<Option<oneshot::Sender<()>>>,
    bridge_finished: watch::Receiver<bool>,
    #[cfg(test)]
    bridge_pause: Mutex<Option<BridgePause>>,
}
#[cfg(test)]
struct BridgePause {
    entered: oneshot::Sender<()>,
    resume: oneshot::Receiver<()>,
    processed: oneshot::Sender<()>,
}
struct BridgeCompletion(watch::Sender<bool>);
impl Drop for BridgeCompletion {
    fn drop(&mut self) {
        self.0.send_replace(true);
    }
}
impl NetworkObservationOwner {
    pub(crate) fn new(context: NetworkStatusContext) -> Result<Arc<Self>, NetError> {
        Self::build(
            context,
            #[cfg(test)]
            None,
        )
    }
    fn build(
        context: NetworkStatusContext,
        #[cfg(test)] bridge_pause: Option<BridgePause>,
    ) -> Result<Arc<Self>, NetError> {
        if context.is_closed() {
            return Err(NetError::from(ErrorKind::Closed));
        }
        let engine = context.common_engine()?;
        let view = NetworkView::new();
        let (publisher, source) = StateSource::new(
            view.snapshot.clone(),
            Arc::new(CommonCallbackExecutor::new(&engine)),
            1024,
        )?;
        let (bridge_stop, mut bridge_stopped) = oneshot::channel();
        let (finished, bridge_finished) = watch::channel(false);
        let completion = BridgeCompletion(finished);
        let owner = Arc::new(Self {
            context,
            engine,
            publisher,
            source,
            bridge_stop: Mutex::new(Some(bridge_stop)),
            bridge_finished,
            #[cfg(test)]
            bridge_pause: Mutex::new(bridge_pause),
            local: Mutex::new(LocalState {
                view,
                lease: None,
                tickets: Vec::new(),
                cycle: Arc::new(StartCycle::new()),
                failure: None,
            }),
        });
        let mut facts = owner.context.subscribe_facts();
        let mut failures = owner.context.subscribe_failure();
        let weak = Arc::downgrade(&owner);
        owner.engine.runtime_handle().spawn(async move {
            let _completion = completion;
            loop {
                let snapshot = facts.borrow_and_update().clone();
                let terminal = matches!(snapshot.state, MonitorState::Closed);
                let Some(owner) = weak.upgrade() else {
                    break;
                };
                #[cfg(test)]
                let processed = {
                    let pause = match owner.bridge_pause.lock() {
                        Ok(mut pause) => pause.take(),
                        Err(_) => None,
                    };
                    drop(owner);
                    match pause {
                        Some(pause) => {
                            let _ = pause.entered.send(());
                            tokio::select! {
                                biased;
                                _ = &mut bridge_stopped => break,
                                _ = pause.resume => {},
                            }
                            Some(pause.processed)
                        }
                        None => None,
                    }
                };
                #[cfg(test)]
                let Some(owner) = weak.upgrade() else {
                    break;
                };
                let failure = failures.borrow_and_update().clone();
                if let Some(error) = failure {
                    owner.fail_source(error);
                    break;
                }
                if let Err(error) = owner.accept(snapshot, false) {
                    owner.fail_source(error);
                    break;
                }
                drop(owner);
                #[cfg(test)]
                if let Some(processed) = processed {
                    let _ = processed.send(());
                }
                if terminal {
                    break;
                }
                tokio::select! {
                    biased;
                    _ = &mut bridge_stopped => break,
                    changed = facts.changed() => if changed.is_err() { break; },
                    changed = failures.changed() => if changed.is_err() { break; },
                }
            }
        });
        Ok(owner)
    }
    // Returns deferred notifications; no user code runs on the lifecycle stack.
    fn prepare(&self, local: &LocalState) -> StatePublication<NetworkSnapshot> {
        let snapshot = Arc::new(local.view.snapshot.clone());
        if local.view.closed() {
            self.publisher.prepare_finish_arc(snapshot)
        } else {
            self.publisher.prepare_arc(snapshot)
        }
    }
    fn dispatch(
        &self,
        publication: Option<StatePublication<NetworkSnapshot>>,
        wake: Option<Arc<Notify>>,
    ) {
        if publication.is_none() && wake.is_none() {
            return;
        }
        let engine = self.engine.clone();
        self.engine.runtime_handle().spawn_blocking(move || {
            let _engine = engine;
            if let Some(wake) = wake {
                wake.notify_waiters();
            }
            if let Some(publication) = publication {
                if let Err(error) = publication.dispatch() {
                    log_error("network_view_publish", &error);
                }
            }
        });
    }
    fn accept(&self, snapshot: NetworkSnapshot, refresh: bool) -> Result<(), NetError> {
        if matches!(snapshot.state, MonitorState::Closed) {
            self.request_close();
            return Ok(());
        }
        let publication = {
            let mut local = self.local.lock().map_err(NetError::from_poison)?;
            let changed = if refresh {
                local.view.refresh(snapshot)?
            } else {
                local.view.import(snapshot, false)?
            };
            changed.then(|| self.prepare(&local))
        };
        self.dispatch(publication, None);
        Ok(())
    }
    pub(crate) fn activate(self: &Arc<Self>) -> Result<(), NetError> {
        self.activate_impl(true).map(|_| ())
    }
    fn activate_impl(
        self: &Arc<Self>,
        ensure_started: bool,
    ) -> Result<Option<(Arc<StartCycle>, NetworkLease, u64)>, NetError> {
        let mut publication = None;
        let result = {
            let mut local = self.local.lock().map_err(NetError::from_poison)?;
            if let Some(error) = &local.failure {
                return Err(error.clone());
            }
            if local.view.closed() || self.context.is_closed() {
                return Err(NetError::from(ErrorKind::Closed));
            }
            local.tickets.retain(|ticket| !ticket.is_finished());
            let new_lease = local.lease.is_none();
            if new_lease {
                let snapshot = self.context.snapshot()?;
                let lease = self.context.acquire()?;
                if let Err(error) = local.view.activate(snapshot) {
                    local.tickets.push(lease.release());
                    return Err(error);
                }
                local.cycle = Arc::new(StartCycle::new());
                local.lease = Some(lease);
                publication = Some(self.prepare(&local));
            }
            let result = match &local.lease {
                Some(lease) if ensure_started => lease.ensure_started(),
                Some(_) => Ok(()),
                None => Err(NetError::from(ErrorKind::Internal)),
            };
            if result.is_err() && new_lease {
                if let Some(lease) = local.lease.take() {
                    local.tickets.push(lease.release());
                }
                local.view.stop(false)?;
            }
            let captured = if !ensure_started && result.is_ok() {
                Some((
                    local.cycle.clone(),
                    local
                        .lease
                        .clone()
                        .ok_or_else(|| NetError::from(ErrorKind::Internal))?,
                    local.view.generation,
                ))
            } else {
                None
            };
            result.map(|()| captured)
        };
        self.dispatch(publication, None);
        result
    }
    pub(crate) async fn start(self: &Arc<Self>) -> Result<NetworkSnapshot, NetError> {
        // A single named start submits one initialization request. Submitting an
        // additional fire-and-forget Ensure could accidentally retry an immediate failure.
        let (cycle, lease, generation) = self
            .activate_impl(false)?
            .ok_or_else(|| NetError::from(ErrorKind::Internal))?;
        let result = tokio::select! {
            biased;
            stopped = cycle.wait() => return stopped,
            initialized = lease.wait_started() => initialized,
        };
        let (result, publication) = {
            let mut local = self.local.lock().map_err(NetError::from_poison)?;
            if let Some(outcome) = cycle.result()? {
                return outcome;
            }
            if local.view.closed() {
                return Err(NetError::from(ErrorKind::Closed));
            }
            if local.view.generation != generation || !local.view.active {
                return Err(NetError::from(ErrorKind::Internal));
            }
            match result {
                Ok(snapshot) => {
                    let changed = local.view.import(snapshot, false)?;
                    (
                        Ok(local.view.snapshot.clone()),
                        changed.then(|| self.prepare(&local)),
                    )
                }
                Err(error) => (Err(error), None),
            }
        };
        self.dispatch(publication, None);
        result
    }
    fn retire(&self, permanent: bool, source_error: Option<NetError>) -> Result<(), NetError> {
        let (publication, wake, result, closed) = {
            let mut local = match self.local.lock() {
                Ok(local) => local,
                Err(poisoned) => {
                    log_error(
                        "network_view_retire_poison_recovered",
                        &NetError::from(ErrorKind::Internal),
                    );
                    poisoned.into_inner()
                }
            };
            let was_closed = local.view.closed();
            let stopped = local.view.stop(permanent);
            // A local Closed commit is terminal for this view. A late source
            // error must not overwrite its readable terminal snapshot or a
            // previously recorded infrastructure error.
            let failure = if was_closed {
                None
            } else {
                source_error.or_else(|| stopped.as_ref().err().cloned())
            };
            let changed = match &stopped {
                Ok(changed) => *changed,
                Err(_) => true,
            };
            // Exhausted local versions are an infrastructure error, never a
            // reason to retain demand or leave a seemingly Running view behind.
            if let Some(error) = &failure {
                local.failure = Some(error.clone());
                local.view.active = false;
                local.view.snapshot.state = MonitorState::Closed;
                local.view.snapshot.reachability = None;
                local.view.snapshot.ip_stack = None;
                local.view.snapshot.network_name = None;
                local.view.snapshot.observed_at = None;
            }
            if let Some(lease) = local.lease.take() {
                local.tickets.push(lease.release());
            }
            let notify = !was_closed && (changed || failure.is_some());
            let wake = if notify {
                let outcome = if let Some(error) = &failure {
                    Err(error.clone())
                } else if permanent {
                    Err(NetError::from(ErrorKind::Closed))
                } else {
                    Ok(local.view.snapshot.clone())
                };
                Some(local.cycle.complete(outcome)?)
            } else {
                None
            };
            let publication = if notify {
                Some(match failure {
                    Some(error) => self.publisher.prepare_failure(error),
                    None => self.prepare(&local),
                })
            } else {
                None
            };
            (publication, wake, stopped.map(|_| ()), local.view.closed())
        };
        if closed {
            self.stop_bridge();
        }
        self.dispatch(publication, wake);
        result
    }
    fn fail_source(&self, error: NetError) {
        if let Err(error) = self.retire(true, Some(error)) {
            log_error("network_view_failure", &error);
        }
    }
    fn stop_bridge(&self) {
        let stop = match self.bridge_stop.lock() {
            Ok(mut stop) => stop.take(),
            Err(poisoned) => {
                log_error(
                    "network_view_bridge_poison_recovered",
                    &NetError::from(ErrorKind::Internal),
                );
                poisoned.into_inner().take()
            }
        };
        if let Some(stop) = stop {
            let _ = stop.send(());
        }
    }
    pub(crate) fn request_stop(&self) -> Result<(), NetError> {
        self.retire(false, None)
    }
    pub(crate) fn request_close(&self) {
        if let Err(error) = self.retire(true, None) {
            log_error("network_view_close", &error);
        }
        self.stop_bridge();
    }
    pub(crate) async fn wait_cleanup(&self) -> Result<(), NetError> {
        let (tickets, closed) = {
            let local = self.local.lock().map_err(NetError::from_poison)?;
            (local.tickets.clone(), local.view.closed())
        };
        let mut failure = None;
        for ticket in tickets {
            if let Err(error) = ticket.wait().await {
                if failure.is_none() {
                    failure = Some(error);
                }
            }
        }
        if closed {
            let mut finished = self.bridge_finished.clone();
            while !*finished.borrow_and_update() {
                if finished.changed().await.is_err() {
                    if failure.is_none() {
                        failure = Some(NetError::from(ErrorKind::RuntimeUnavailable));
                    }
                    break;
                }
            }
        }
        failure.map_or(Ok(()), Err)
    }
    pub(crate) fn snapshot(&self) -> Result<NetworkSnapshot, NetError> {
        if self.context.is_closed() {
            self.request_close();
        }
        let local = self.local.lock().map_err(NetError::from_poison)?;
        if let Some(error) = &local.failure {
            return Err(error.clone());
        }
        Ok(local.view.snapshot.clone())
    }
    pub(crate) fn snapshot_latest(&self) -> Result<NetworkSnapshot, NetError> {
        {
            let local = self.local.lock().map_err(NetError::from_poison)?;
            if local.view.closed() {
                if let Some(error) = &local.failure {
                    return Err(error.clone());
                }
                return Ok(local.view.snapshot.clone());
            }
        }
        match self
            .context
            .snapshot()
            .and_then(|snapshot| self.accept(snapshot, true))
        {
            Ok(()) => self.snapshot(),
            Err(error) => {
                self.fail_source(error.clone());
                Err(error)
            }
        }
    }
    /// Named monitors preserve their historical ability to observe a cached
    /// terminal snapshot. HTTP registration uses the stricter active-only entry.
    pub(crate) fn subscribe_terminal(&self) -> Result<StateReceiver<NetworkSnapshot>, NetError> {
        if self.context.is_closed() {
            self.request_close();
        }
        self.source.subscribe_committed()
    }
    pub(crate) fn subscribe(&self) -> Result<StateReceiver<NetworkSnapshot>, NetError> {
        let local = self.local.lock().map_err(NetError::from_poison)?;
        if let Some(error) = &local.failure {
            return Err(error.clone());
        }
        if local.view.closed() || self.context.is_closed() {
            return Err(NetError::from(ErrorKind::Closed));
        }
        self.source.subscribe_committed()
    }

    #[cfg(test)]
    pub(crate) fn close_source_for_test(&self) -> Result<(), NetError> {
        let mut snapshot = self.snapshot()?;
        snapshot.state = MonitorState::Closed;
        self.publisher.finish(snapshot)
    }
}
impl Drop for NetworkObservationOwner {
    fn drop(&mut self) {
        self.request_close();
    }
}
fn log_error(event: &str, error: &NetError) {
    crate::log_e!(crate::common::log::log_def::LogType::Engine; event, "error", crate::common::log::summary::error(error));
}

#[cfg(test)]
#[path = "facade_tests.rs"]
mod tests;

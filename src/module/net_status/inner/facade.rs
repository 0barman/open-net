//! Per-client network view. Only the engine service controls the detector.
use std::sync::{Arc, Mutex};

use crate::common::CommonEngine;
use crate::error::{ErrorKind, NetError};
use crate::net_status::{
    IpStack, MonitorState, NetworkEvent, NetworkEventOptions, NetworkObservation, NetworkSnapshot,
    NetworkStatus,
};
use crate::subscription::common_executor::CommonCallbackExecutor;
use crate::subscription::{
    CallbackContext, EventReceiver, StateReceiver, StateSource, Subscription,
};
use tokio::sync::{oneshot, watch, Notify};

use super::coordinated_view::ViewPublication;
use super::network_status_snapshot::{NetworkRefreshTarget, NetworkViewHandle};
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
    closed: bool,
    lease: Option<NetworkLease>,
    tickets: Vec<CleanupTicket>,
    cycle: Arc<StartCycle>,
    failure: Option<NetError>,
}

pub(crate) struct NetworkObservationOwner {
    context: NetworkStatusContext,
    engine: Arc<CommonEngine>,
    local: Mutex<LocalState>,
    view: NetworkViewHandle,
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
        // Register before exposing the view to a concurrent terminal commit.
        // Even an unpolled bridge owns this lease until its future is dropped.
        let bridge_work = engine.retirement_work().register();
        let view = context.observation_source()?.new_view(
            Arc::new(CommonCallbackExecutor::new(&engine)),
            Some(engine.clone()),
        )?;
        let source = view.source()?;
        let (bridge_stop, mut bridge_stopped) = oneshot::channel();
        let (finished, bridge_finished) = watch::channel(false);
        let completion = BridgeCompletion(finished);
        let owner = Arc::new(Self {
            context,
            engine,
            view,
            source,
            bridge_stop: Mutex::new(Some(bridge_stop)),
            bridge_finished,
            #[cfg(test)]
            bridge_pause: Mutex::new(bridge_pause),
            local: Mutex::new(LocalState {
                closed: false,
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
            let _work = bridge_work;
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
    fn dispatch(&self, publication: Option<ViewPublication>, wake: Option<Arc<Notify>>) {
        let publication = publication.filter(|publication| !publication.is_empty());
        if publication.is_none() && wake.is_none() {
            return;
        }
        let engine = self.engine.clone();
        let work = engine.retirement_work().register();
        self.engine.runtime_handle().spawn_blocking(move || {
            let _work = work;
            let _engine = engine;
            if let Some(wake) = wake {
                wake.notify_waiters();
            }
            if let Some(publication) = publication {
                publication.dispatch();
            }
        });
    }
    fn accept(&self, snapshot: NetworkSnapshot, refresh: bool) -> Result<(), NetError> {
        if matches!(snapshot.state, MonitorState::Closed) {
            self.request_close();
        } else if refresh {
            let publication = self.view.refresh_latest()?;
            self.dispatch(Some(publication), None);
        } else {
            self.view.current()?;
        }
        // Facts are already projected atomically by the source. This bridge is
        // only responsible for retiring leases/start waiters on source failure.
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
        let mut rollback = None;
        let result = (|| {
            let mut local = self.local.lock().map_err(NetError::from_poison)?;
            if let Some(error) = &local.failure {
                return Err(error.clone());
            }
            if local.closed || self.view.current()?.closed() || self.context.is_closed() {
                return Err(NetError::from(ErrorKind::Closed));
            }
            local.tickets.retain(|ticket| !ticket.is_finished());
            let new_lease = local.lease.is_none();
            if new_lease {
                let lease = self.context.acquire()?;
                match self.view.activate() {
                    Ok((_, prepared)) => publication = Some(prepared),
                    Err(error) => {
                        local.tickets.push(lease.release());
                        return Err(error);
                    }
                }
                local.cycle = Arc::new(StartCycle::new());
                local.lease = Some(lease);
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
                rollback = Some(self.view.retire(false, None)?);
            }
            let captured = if !ensure_started && result.is_ok() {
                Some((
                    local.cycle.clone(),
                    local
                        .lease
                        .clone()
                        .ok_or_else(|| NetError::from(ErrorKind::Internal))?,
                    self.view.current()?.generation,
                ))
            } else {
                None
            };
            result.map(|()| captured)
        })();
        self.dispatch(publication, None);
        self.dispatch(rollback, None);
        if let Err(error) = self.view.current() {
            self.fail_source(error);
        }
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
        let _local = self.local.lock().map_err(NetError::from_poison)?;
        if let Some(outcome) = cycle.result()? {
            return outcome;
        }
        let view = self.view.current()?;
        if view.closed() {
            return Err(NetError::from(ErrorKind::Closed));
        }
        if view.generation != generation || !view.active {
            return Err(NetError::from(ErrorKind::Internal));
        }
        match result {
            Ok(_) => Ok(view.snapshot),
            Err(error) => Err(error),
        }
    }
    fn retire(&self, permanent: bool, source_error: Option<NetError>) -> Result<(), NetError> {
        // The local bridge can be stopped before the deferred start-waiter
        // wake is submitted below. Keep its successor work registered across
        // that handoff, including when the source already closed this view.
        let _work = self.engine.retirement_work().register();
        let (publication, wake, result, closed) = {
            let mut local = match self.local.lock() {
                Ok(local) => local,
                Err(poisoned) => poisoned.into_inner(),
            };
            let was_closed = local.closed;
            let publication = self.view.retire(permanent, source_error)?;
            let current = self.view.current();
            let closed = current.as_ref().map_or(true, |view| view.closed());
            if let Err(error) = &current {
                local.failure.get_or_insert_with(|| error.clone());
            }
            if let Some(lease) = local.lease.take() {
                local.tickets.push(lease.release());
            }
            let wake = if !was_closed {
                let outcome = match &current {
                    Err(error) => Err(error.clone()),
                    Ok(view) if view.closed() => Err(NetError::from(ErrorKind::Closed)),
                    Ok(view) => Ok(view.snapshot.clone()),
                };
                Some(local.cycle.complete(outcome)?)
            } else {
                None
            };
            local.closed = closed;
            (Some(publication), wake, current.map(|_| ()), closed)
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
            (local.tickets.clone(), local.closed)
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
    /// Read-only queries do not acquire a monitoring lease or require Running.
    /// Release these short locks before any OS call; admitted queries may finish
    /// after close and are deliberately outside the monitor cleanup barrier.
    pub(crate) fn admit_lan_query(&self) -> Result<(), NetError> {
        let local = self.local.lock().map_err(NetError::from_poison)?;
        if local.closed || self.context.is_closed() {
            return Err(ErrorKind::Closed.into());
        }
        Ok(())
    }

    pub(crate) fn snapshot(&self) -> Result<NetworkSnapshot, NetError> {
        if self.context.is_closed() {
            self.request_close();
        }
        Ok(self.view.current()?.snapshot)
    }
    pub(crate) fn snapshot_latest(&self) -> Result<NetworkSnapshot, NetError> {
        if self.context.is_closed() {
            self.request_close();
        }
        let publication = match self.view.refresh_latest() {
            Ok(publication) => publication,
            Err(error) => {
                self.fail_source(error.clone());
                return Err(error);
            }
        };
        self.dispatch(Some(publication), None);
        let snapshot = self.snapshot();
        if let Err(error) = &snapshot {
            self.fail_source(error.clone());
        }
        snapshot
    }
    pub(super) fn context(&self) -> &NetworkStatusContext {
        &self.context
    }
    pub(crate) fn subscribe_events(
        &self,
        options: NetworkEventOptions,
    ) -> Result<EventReceiver<NetworkEvent>, NetError> {
        if self.context.is_closed() {
            self.request_close();
        }
        self.view.subscribe_events(options)
    }
    pub(crate) fn on_recoverable_event<F>(
        self: &Arc<Self>,
        options: NetworkEventOptions,
        callback: F,
    ) -> Result<Subscription, NetError>
    where
        F: Fn(CallbackContext, Result<NetworkObservation, NetError>) + Send + Sync + 'static,
    {
        // Admission follows registration: if engine closure has already
        // reached its work barrier, the closed context rejects this observer.
        let retirement = self.engine.retirement_work().register();
        if self.context.is_closed() {
            return Err(ErrorKind::Closed.into());
        }
        let receiver = self.subscribe_events(options)?;
        let view = self.view.clone();
        super::recoverable::subscribe(
            receiver,
            Arc::new(move || view.subscribe_events(options)),
            Arc::new(CommonCallbackExecutor::new(&self.engine)),
            self.engine.runtime_handle(),
            Some(retirement),
            callback,
        )
    }
    pub(crate) fn refresh_target(&self) -> Result<NetworkRefreshTarget, NetError> {
        self.view.refresh_target()
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn commit_refresh(
        &self,
        target: NetworkRefreshTarget,
        sample_id: u64,
        reachability: NetworkStatus,
        ip_stack: Option<IpStack>,
        network_name: impl Into<super::network_status_snapshot::NetworkNameUpdate>,
        observed_at: std::time::SystemTime,
        validate: impl FnOnce() -> Result<(), NetError>,
    ) -> Result<NetworkSnapshot, NetError> {
        let (snapshot, publication) = self.view.commit_refresh(
            target,
            sample_id,
            reachability,
            ip_stack,
            network_name,
            observed_at,
            validate,
        )?;
        let result = publication.result();
        let engine = self.engine.clone();
        self.engine.runtime_handle().spawn_blocking(move || {
            let _engine = engine;
            if let Err(error) = publication.dispatch() {
                log_error("network_refresh_publish", &error);
            }
        });
        result?;
        snapshot
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
        if local.closed || self.view.current()?.closed() || self.context.is_closed() {
            return Err(NetError::from(ErrorKind::Closed));
        }
        self.source.subscribe_committed()
    }

    #[cfg(test)]
    pub(crate) fn activate_view_for_test(&self) -> Result<NetworkSnapshot, NetError> {
        let (_, publication) = self.view.activate()?;
        publication.dispatch();
        Ok(self.view.current()?.snapshot)
    }
    #[cfg(test)]
    pub(crate) fn close_source_for_test(&self) -> Result<(), NetError> {
        self.view.close_source_for_test()
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

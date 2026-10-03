//! One versioned network snapshot with an internal WebSocket gating projection.

use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use crate::common::log::log_def::LogType;
use crate::error::{ErrorKind, NetError};
use crate::net_status::{IpStack, MonitorState, NetworkSnapshot, NetworkStatus};
use crate::subscription::{
    CallbackExecutor, StatePublication, StatePublisher, StateReceiver, StateSource,
};
use tokio::sync::watch;

/// Only SDK-owned WebSocket tasks consume this projection. Its revision keeps
/// the existing reachability-only semantics; loss history comes from the full snapshot.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct NetworkStatusSnapshot {
    pub(crate) revision: u64,
    pub(crate) loss_epoch: u64,
    pub(crate) status: Option<NetworkStatus>,
}
struct SourceState {
    generation: u64,
    active: bool,
    revision: u64,
    loss_epoch: u64,
    snapshot: Arc<NetworkSnapshot>,
    gating: NetworkStatusSnapshot,
    publication_error: Option<NetError>,
}
#[derive(Clone)]
pub(super) struct NetworkStatusSource {
    state: Arc<Mutex<SourceState>>,
    sender: watch::Sender<NetworkStatusSnapshot>,
    facts: watch::Sender<NetworkSnapshot>,
    failure: watch::Sender<Option<NetError>>,
    publisher: StatePublisher<NetworkSnapshot>,
    observations: StateSource<NetworkSnapshot>,
}
#[derive(Clone)]
pub(super) struct NetworkStatusPublisher {
    source: NetworkStatusSource,
    generation: u64,
}

/// Holds every user-owned value retired during an atomic commit. Call dispatch
/// after releasing the originating lifecycle/monitor lock, including for no-ops.
#[must_use]
pub(super) struct NetworkPublication {
    state: Option<StatePublication<NetworkSnapshot>>,
    terminal: Option<StatePublication<NetworkSnapshot>>,
    retired: Option<Arc<NetworkSnapshot>>,
    retained_input: Option<MonitorState>,
    result: Result<(), NetError>,
}
impl NetworkPublication {
    fn empty() -> Self {
        Self {
            state: None,
            terminal: None,
            retired: None,
            retained_input: None,
            result: Ok(()),
        }
    }
    pub(super) fn result(&self) -> Result<(), NetError> {
        self.result.clone()
    }
    pub(super) fn dispatch(self) -> Result<(), NetError> {
        let Self {
            state,
            terminal,
            retired,
            retained_input,
            mut result,
        } = self;
        drop((retired, retained_input));
        for publication in [state, terminal].into_iter().flatten() {
            if let Err(error) = publication.dispatch() {
                if result.is_ok() {
                    result = Err(error);
                }
            }
        }
        result
    }
}
impl NetworkStatusSource {
    pub(super) fn new(
        executor: Arc<dyn CallbackExecutor>,
        max_subscriptions: usize,
    ) -> Result<Self, NetError> {
        let snapshot = Arc::new(NetworkSnapshot {
            revision: 0,
            loss_epoch: 0,
            state: MonitorState::Stopped,
            reachability: None,
            ip_stack: None,
            observed_at: None,
            network_name: None,
        });
        let (publisher, observations) =
            StateSource::new_arc(snapshot.clone(), executor, max_subscriptions)?;
        let (sender, _) = watch::channel(NetworkStatusSnapshot::default());
        let (facts, _) = watch::channel((*snapshot).clone());
        let (failure, _) = watch::channel(None);
        Ok(Self {
            state: Arc::new(Mutex::new(SourceState {
                generation: 0,
                active: false,
                revision: 0,
                loss_epoch: 0,
                snapshot,
                gating: NetworkStatusSnapshot::default(),
                publication_error: None,
            })),
            sender,
            facts,
            failure,
            publisher,
            observations,
        })
    }
    #[cfg_attr(not(any(feature = "ws-client", test)), allow(dead_code))]
    pub(super) fn subscribe(&self) -> watch::Receiver<NetworkStatusSnapshot> {
        self.sender.subscribe()
    }
    pub(super) fn subscribe_facts(&self) -> watch::Receiver<NetworkSnapshot> {
        self.facts.subscribe()
    }
    pub(super) fn subscribe_failure(&self) -> watch::Receiver<Option<NetError>> {
        self.failure.subscribe()
    }
    pub(super) fn observe(&self) -> Result<StateReceiver<NetworkSnapshot>, NetError> {
        self.observations.subscribe()
    }
    pub(super) fn fail(&self, error: NetError) -> Result<NetworkPublication, NetError> {
        let mut state = self.state.lock().map_err(NetError::from_poison)?;
        // A permanent close wins over a late preparation failure. The caller
        // still receives its original start error, while the shared source
        // keeps the readable Closed terminal state and does not reopen its
        // failure channel after destruction.
        if matches!(state.snapshot.state, MonitorState::Closed) {
            return Ok(NetworkPublication::empty());
        }
        Ok(self.fail_publication(&mut state, error, None))
    }
    pub(super) fn snapshot(&self) -> Result<NetworkSnapshot, NetError> {
        let snapshot = {
            let state = self.state.lock().map_err(NetError::from_poison)?;
            if let Some(error) = &state.publication_error {
                return Err(error.clone());
            }
            state.snapshot.clone()
        };
        Ok((*snapshot).clone())
    }
    pub(super) fn begin_generation(
        &self,
    ) -> Result<(NetworkStatusPublisher, NetworkPublication), NetError> {
        let mut state = self.state.lock().map_err(NetError::from_poison)?;
        if let Some(error) = &state.publication_error {
            return Err(error.clone());
        }
        if matches!(state.snapshot.state, MonitorState::Closed) {
            return Err(NetError::from(ErrorKind::Closed));
        }
        let generation = increment(state.generation)?;
        let next = next_snapshot(&state, MonitorState::Starting, None, None, None, None)?;
        let publication = self.commit(&mut state, next, false)?;
        if publication.result().is_ok() {
            state.generation = generation;
            state.active = true;
        }
        Ok((
            NetworkStatusPublisher {
                source: self.clone(),
                generation,
            },
            publication,
        ))
    }
    pub(super) fn prepare_stopped(&self) -> Result<NetworkPublication, NetError> {
        self.prepare_terminal(false)
    }
    pub(super) fn prepare_closed(&self) -> Result<NetworkPublication, NetError> {
        self.prepare_terminal(true)
    }
    fn prepare_terminal(&self, permanent: bool) -> Result<NetworkPublication, NetError> {
        let mut state = self.state.lock().map_err(NetError::from_poison)?;
        if let Some(error) = &state.publication_error {
            let mut publication = NetworkPublication::empty();
            publication.result = Err(error.clone());
            return Ok(publication);
        }
        if matches!(state.snapshot.state, MonitorState::Closed)
            || (!permanent && matches!(state.snapshot.state, MonitorState::Stopped))
        {
            return Ok(NetworkPublication::empty());
        }
        let target = if permanent {
            MonitorState::Closed
        } else {
            MonitorState::Stopped
        };
        let next = match next_snapshot(&state, target, None, None, None, None) {
            Ok(next) => next,
            Err(error) => return Ok(self.fail_publication(&mut state, error, None)),
        };
        let publication = self.commit(&mut state, next, permanent)?;
        state.active = false;
        Ok(publication)
    }
    fn commit(
        &self,
        state: &mut SourceState,
        next: NetworkSnapshot,
        closing: bool,
    ) -> Result<NetworkPublication, NetError> {
        let gating = if state.gating.status != next.reachability {
            let revision = match increment(state.gating.revision) {
                Ok(revision) => revision,
                Err(error) => return Ok(self.fail_publication(state, error, None)),
            };
            Some(NetworkStatusSnapshot {
                revision,
                loss_epoch: next.loss_epoch,
                status: next.reachability,
            })
        } else {
            None
        };
        let next = Arc::new(next);
        let publication = if closing {
            self.publisher.prepare_finish_arc(next.clone())
        } else {
            self.publisher.prepare_arc(next.clone())
        };
        if let Err(error) = publication.result() {
            // Even a defensive overflow path can hold registration owners and a final
            // notification. Retire it only after the originating outer lock is released.
            return Ok(self.fail_publication(state, error, Some(publication)));
        }
        state.revision = next.revision;
        state.loss_epoch = next.loss_epoch;
        let retired = std::mem::replace(&mut state.snapshot, next);
        self.facts.send_replace((*state.snapshot).clone());
        if let Some(gating) = gating {
            state.gating = gating;
            // Preserve the existing internal gate commit timing. This watch is never
            // exposed as a public receiver and only wakes SDK WebSocket tasks.
            self.sender.send_replace(gating);
        }
        Ok(NetworkPublication {
            state: Some(publication),
            terminal: None,
            retired: Some(retired),
            retained_input: None,
            result: Ok(()),
        })
    }
    fn fail_publication(
        &self,
        state: &mut SourceState,
        error: NetError,
        original: Option<StatePublication<NetworkSnapshot>>,
    ) -> NetworkPublication {
        let terminal = self.publisher.prepare_failure(error.clone());
        state.active = false;
        state.publication_error = Some(error.clone());
        self.failure.send_replace(Some(error.clone()));
        if state.gating.status.is_some() {
            // On exhaustion there is no next version. Revoke the internal fact without
            // wrapping its counter; the public observation ends explicitly with an error.
            let revision = match state.gating.revision.checked_add(1) {
                Some(next) => next,
                None => state.gating.revision,
            };
            state.gating = NetworkStatusSnapshot {
                revision,
                loss_epoch: state.loss_epoch,
                status: None,
            };
            self.sender.send_replace(state.gating);
        }
        NetworkPublication {
            state: original,
            terminal: Some(terminal),
            retired: None,
            retained_input: None,
            result: Err(error),
        }
    }
    #[cfg(test)]
    pub(super) fn exhaust_publication_revision_for_test(&self) {
        self.publisher.exhaust_revision_for_test();
    }
    #[cfg(test)]
    pub(super) fn exhaust_revision_for_test(&self) -> Result<(), NetError> {
        self.state.lock().map_err(NetError::from_poison)?.revision = u64::MAX;
        Ok(())
    }
    #[cfg(test)]
    pub(super) fn for_test() -> Result<Self, NetError> {
        Self::new(Arc::new(TestCallbackExecutor), 16)
    }
}
impl NetworkStatusPublisher {
    pub(super) fn prepare_observation(
        &self,
        reachability: NetworkStatus,
        ip_stack: Option<IpStack>,
        network_name: Option<String>,
    ) -> Result<NetworkPublication, NetError> {
        let mut state = self.source.state.lock().map_err(NetError::from_poison)?;
        if state.generation != self.generation
            || !state.active
            || matches!(state.snapshot.state, MonitorState::Closed)
        {
            return Ok(NetworkPublication::empty());
        }
        if matches!(state.snapshot.state, MonitorState::Running)
            && state.snapshot.reachability == Some(reachability)
            && state.snapshot.ip_stack == ip_stack
            && state.snapshot.network_name == network_name
        {
            return Ok(NetworkPublication::empty());
        }
        let next = match next_snapshot(
            &state,
            MonitorState::Running,
            Some(reachability),
            ip_stack,
            Some(SystemTime::now()),
            network_name,
        ) {
            Ok(next) => next,
            Err(error) => return Ok(self.source.fail_publication(&mut state, error, None)),
        };
        self.source.commit(&mut state, next, false)
    }
    pub(super) fn prepare_finish(
        &self,
        target: MonitorState,
    ) -> Result<NetworkPublication, NetError> {
        let mut deferred = NetworkPublication::empty();
        deferred.retained_input = Some(target.clone());
        if !matches!(target, MonitorState::Stopped | MonitorState::Failed(_)) {
            deferred.result = Err(NetError::from(ErrorKind::InvalidInput));
            return Ok(deferred);
        }
        let mut state = match self.source.state.lock() {
            Ok(state) => state,
            Err(error) => {
                deferred.result = Err(NetError::from_poison(error));
                return Ok(deferred);
            }
        };
        if state.generation != self.generation
            || !state.active
            || matches!(state.snapshot.state, MonitorState::Closed)
        {
            return Ok(deferred);
        }
        let result = next_snapshot(&state, target, None, None, None, None)
            .and_then(|next| self.source.commit(&mut state, next, false));
        match result {
            Ok(mut publication) => {
                state.active = false;
                publication.retained_input = deferred.retained_input.take();
                Ok(publication)
            }
            Err(error) => {
                let mut publication = self.source.fail_publication(&mut state, error, None);
                publication.retained_input = deferred.retained_input.take();
                Ok(publication)
            }
        }
    }
}
fn increment(value: u64) -> Result<u64, NetError> {
    value
        .checked_add(1)
        .ok_or_else(|| NetError::from(ErrorKind::Internal))
}
fn next_snapshot(
    state: &SourceState,
    target: MonitorState,
    reachability: Option<NetworkStatus>,
    ip_stack: Option<IpStack>,
    observed_at: Option<SystemTime>,
    network_name: Option<String>,
) -> Result<NetworkSnapshot, NetError> {
    let revision = increment(state.revision)?;
    let loss_epoch = if reachability == Some(NetworkStatus::Unavailable)
        && state.snapshot.reachability != reachability
    {
        increment(state.loss_epoch)?
    } else {
        state.loss_epoch
    };
    Ok(NetworkSnapshot {
        revision,
        loss_epoch,
        state: target,
        reachability,
        ip_stack,
        observed_at,
        network_name,
    })
}

/// The task owns this before its first poll, so cancellation also retires its generation.
pub(super) struct NetworkStatusMonitorGuard(pub(super) NetworkStatusPublisher);
impl Drop for NetworkStatusMonitorGuard {
    fn drop(&mut self) {
        let result = self
            .0
            .prepare_finish(MonitorState::Stopped)
            .and_then(NetworkPublication::dispatch);
        if let Err(error) = result {
            crate::log_e!(LogType::Engine; "network_status_observation_stop", "error", crate::common::log::summary::error(&error));
        }
    }
}

#[cfg(test)]
struct TestCallbackExecutor;
#[cfg(test)]
impl CallbackExecutor for TestCallbackExecutor {
    fn ensure_ready(&self) -> Result<(), NetError> {
        Ok(())
    }
    fn submit(&self, job: Box<dyn FnOnce() + Send>) -> Result<(), NetError> {
        job();
        Ok(())
    }
}
#[cfg(test)]
pub(crate) struct TestNetworkStatusSource {
    publisher: NetworkStatusPublisher,
}
#[cfg(test)]
impl TestNetworkStatusSource {
    pub(crate) fn publish(&self, status: Option<NetworkStatus>) -> Result<(), NetError> {
        let publication = match status {
            Some(status) => {
                self.publisher
                    .prepare_observation(status, Some(IpStack::None), None)?
            }
            None => self.publisher.prepare_finish(MonitorState::Stopped)?,
        };
        publication.dispatch()
    }
}
#[cfg(test)]
impl Drop for TestNetworkStatusSource {
    fn drop(&mut self) {
        let result = self
            .publisher
            .prepare_finish(MonitorState::Stopped)
            .and_then(NetworkPublication::dispatch);
        if let Err(error) = result {
            crate::log_e!(LogType::Engine; "test_network_status_source_stop", "error", crate::common::log::summary::error(&error));
        }
    }
}
#[cfg(test)]
pub(crate) fn test_network_status_source() -> Result<
    (
        TestNetworkStatusSource,
        watch::Receiver<NetworkStatusSnapshot>,
    ),
    NetError,
> {
    let source = NetworkStatusSource::for_test()?;
    let receiver = source.subscribe();
    let (publisher, publication) = source.begin_generation()?;
    publication.dispatch()?;
    Ok((TestNetworkStatusSource { publisher }, receiver))
}
#[cfg(test)]
#[path = "network_status_snapshot_tests.rs"]
mod tests;

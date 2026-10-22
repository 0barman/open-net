//! One versioned network snapshot with an internal WebSocket gating projection.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, Weak};
use std::time::SystemTime;

use super::coordinated_view::{event_size, CoordinatedView, ViewPublication};
use super::network_view::NetworkView;
use crate::common::log::log_def::LogType;
use crate::common::retirement::{ExecutionScope, WorkLease, WorkTracker};
use crate::error::{ErrorKind, NetError};
use crate::module::net_status::logging::Transition;
use crate::net_status::{
    IpStack, MonitorState, NetworkEvent, NetworkEventKind, NetworkEventOptions, NetworkSnapshot,
    NetworkStatus,
};
use crate::subscription::{
    event_channel, CallbackExecutor, EventOverflow, EventQueueLimit, EventReceiver, EventSeed,
    StatePublication, StatePublisher, StateReceiver, StateSource,
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
    views: Vec<Weak<Mutex<CoordinatedView>>>,
    last_sample_id: u64,
}

#[derive(Clone)]
pub(super) struct NetworkStatusSource {
    state: Arc<Mutex<SourceState>>,
    sender: watch::Sender<NetworkStatusSnapshot>,
    facts: watch::Sender<NetworkSnapshot>,
    failure: watch::Sender<Option<NetError>>,
    publisher: StatePublisher<NetworkSnapshot>,
    observations: StateSource<NetworkSnapshot>,
    retirement: Option<Arc<WorkTracker>>,
}
#[derive(Clone)]
pub(super) struct NetworkStatusPublisher {
    source: NetworkStatusSource,
    generation: u64,
}

#[derive(Clone)]
pub(super) struct NetworkViewHandle {
    source: NetworkStatusSource,
    view: Arc<Mutex<CoordinatedView>>,
}

/// Captured before an asynchronous platform query and revalidated at commit.
#[derive(Clone, Debug)]
pub(crate) struct NetworkRefreshTarget {
    pub(crate) owner_generation: u64,
    pub(crate) source_generation: u64,
    invalidation_epoch: u64,
    invalidated: watch::Receiver<u64>,
}
impl NetworkRefreshTarget {
    pub(crate) async fn invalidated(&mut self) {
        loop {
            if *self.invalidated.borrow_and_update() > self.invalidation_epoch {
                return;
            }
            if self.invalidated.changed().await.is_err() {
                return;
            }
        }
    }
}

/// Holds every user-owned value retired during an atomic commit. Call dispatch
/// after releasing the originating lifecycle/monitor lock, including for no-ops.
#[must_use]
pub(super) struct NetworkPublication {
    transition: Option<Transition>,
    state: Option<StatePublication<NetworkSnapshot>>,
    terminal: Option<StatePublication<NetworkSnapshot>>,
    retired: Option<Arc<NetworkSnapshot>>,
    retained_input: Option<MonitorState>,
    result: Result<(), NetError>,
    views: Vec<ViewPublication>,
    work: Option<WorkLease>,
}
impl NetworkPublication {
    fn empty() -> Self {
        Self {
            transition: None,
            state: None,
            terminal: None,
            retired: None,
            retained_input: None,
            result: Ok(()),
            views: Vec::new(),
            work: None,
        }
    }
    pub(super) fn result(&self) -> Result<(), NetError> {
        self.result.clone()
    }
    pub(super) fn dispatch(self) -> Result<(), NetError> {
        let _scope = ExecutionScope::enter();
        let Self {
            transition,
            state,
            terminal,
            retired,
            retained_input,
            mut result,
            views,
            work: _work,
        } = self;
        drop((retired, retained_input));
        for publication in [state, terminal].into_iter().flatten() {
            if let Err(error) = publication.dispatch() {
                if result.is_ok() {
                    result = Err(error);
                }
            }
        }
        for view in views {
            view.dispatch_from_monitor();
        }
        if let Some(transition) = transition {
            transition.emit();
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
                views: Vec::new(),
                last_sample_id: 0,
            })),
            sender,
            facts,
            failure,
            publisher,
            observations,
            retirement: None,
        })
    }
    pub(super) fn with_retirement(mut self, retirement: Arc<WorkTracker>) -> Self {
        self.retirement = Some(retirement);
        self
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
    pub(super) fn new_view(
        &self,
        executor: Arc<dyn CallbackExecutor>,
        engine: Option<Arc<crate::common::CommonEngine>>,
    ) -> Result<NetworkViewHandle, NetError> {
        let view = Arc::new(Mutex::new(CoordinatedView::new(executor, engine)?));
        {
            let mut state = self.state.lock().map_err(NetError::from_poison)?;
            if matches!(state.snapshot.state, MonitorState::Closed) {
                return Err(ErrorKind::Closed.into());
            }
            if let Some(error) = &state.publication_error {
                return Err(error.clone());
            }
            state.views.retain(|view| view.strong_count() != 0);
            state
                .views
                .try_reserve(1)
                .map_err(|error| NetError::with_source(ErrorKind::ResourceExhausted, error))?;
            state.views.push(Arc::downgrade(&view));
        }
        Ok(NetworkViewHandle {
            source: self.clone(),
            view,
        })
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
        let next = next_snapshot(&state, MonitorState::Starting, None, None, None)?;
        let previous_generation = state.generation;
        state.generation = generation;
        let publication = self.commit(&mut state, next, false)?;
        if publication.result().is_ok() {
            state.active = true;
        } else {
            state.generation = previous_generation;
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
        let next = match next_snapshot(&state, target, None, None, None) {
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
        // Hold the lease before committing queues while the source authority
        // lock is held. Shutdown can then never overtake a prepared publication
        // paused on a caller thread before dispatch or task submission.
        let work = self.retirement.as_ref().map(|work| work.register());
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
        let transition =
            Transition::capture("source", state.generation, &state.snapshot, &next, false);
        state.revision = next.revision;
        state.loss_epoch = next.loss_epoch;
        let retired = std::mem::replace(&mut state.snapshot, next);
        if let Some(gating) = gating {
            state.gating = gating;
            // Preserve the existing internal gate commit timing. This watch is never
            // exposed as a public receiver and only wakes SDK WebSocket tasks.
            self.sender.send_replace(gating);
        }
        let mut prepared = NetworkPublication {
            transition,
            state: Some(publication),
            terminal: None,
            retired: Some(retired),
            retained_input: None,
            result: Ok(()),
            views: Vec::new(),
            work,
        };
        self.project_views(state, &mut prepared, None);
        // The cleanup bridge must not observe a source revision before its
        // named views have committed it (including per-view failure).
        self.facts.send_replace((*state.snapshot).clone());
        Ok(prepared)
    }
    fn fail_publication(
        &self,
        state: &mut SourceState,
        error: NetError,
        original: Option<StatePublication<NetworkSnapshot>>,
    ) -> NetworkPublication {
        let work = self.retirement.as_ref().map(|work| work.register());
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
        let mut prepared = NetworkPublication {
            transition: None,
            state: original,
            terminal: Some(terminal),
            retired: None,
            retained_input: None,
            result: Err(error.clone()),
            views: Vec::new(),
            work,
        };
        self.project_views(state, &mut prepared, Some(error));
        prepared
    }
    fn project_views(
        &self,
        state: &mut SourceState,
        publication: &mut NetworkPublication,
        failure: Option<NetError>,
    ) {
        state.views.retain(|view| view.strong_count() != 0);
        for weak in &state.views {
            let Some(owner) = weak.upgrade() else {
                continue;
            };
            let mut prepared = ViewPublication::empty();
            prepared.owner = Some(owner.clone());
            {
                let mut view = owner
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                prepared.track(&view.engine);
                if !view.view.closed() {
                    let previous = view.view.snapshot.clone();
                    let result = view.reserve(&mut prepared).and_then(|()| {
                        if let Some(error) = &failure {
                            return Err(error.clone());
                        }
                        view.view.import((*state.snapshot).clone(), false)
                    });
                    match result {
                        Ok(true) => {
                            view.source_revision = state.revision;
                            view.prepare(&previous, &mut prepared, false);
                        }
                        Ok(false) => {}
                        Err(error) => view.fail(error, &mut prepared),
                    }
                }
            }
            publication.views.push(prepared);
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
        {
            return Ok(NetworkPublication::empty());
        }
        let next = match next_snapshot(
            &state,
            MonitorState::Running,
            Some(reachability),
            ip_stack,
            Some(SystemTime::now()),
        ) {
            Ok(next) => next,
            Err(error) => return Ok(self.source.fail_publication(&mut state, error, None)),
        };
        self.source.commit(&mut state, next, false)
    }
    /// Ordered samples from the shared Windows worker, including unchanged facts.
    pub(super) fn prepare_sampled_observation(
        &self,
        sample_id: u64,
        reachability: NetworkStatus,
        ip_stack: Option<IpStack>,
        observed_at: SystemTime,
        validate: impl FnOnce() -> Result<bool, NetError>,
    ) -> Result<Option<NetworkPublication>, NetError> {
        let mut state = self.source.state.lock().map_err(NetError::from_poison)?;
        if state.generation != self.generation
            || !state.active
            || matches!(state.snapshot.state, MonitorState::Closed)
            || sample_id <= state.last_sample_id
        {
            return Ok(None);
        }
        // The source lock may itself outlive the sampling deadline. Validate
        // under the authority lock before advancing either facts or freshness.
        if !validate()? {
            return Ok(None);
        }
        state.last_sample_id = sample_id;
        if same_observation(&state.snapshot, reachability, ip_stack) {
            return Ok(Some(NetworkPublication::empty()));
        }
        let next = next_snapshot(
            &state,
            MonitorState::Running,
            Some(reachability),
            ip_stack,
            Some(observed_at),
        )?;
        self.source.commit(&mut state, next, false).map(Some)
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
        let result = next_snapshot(&state, target, None, None, None)
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

impl NetworkViewHandle {
    pub(super) fn current(&self) -> Result<NetworkView, NetError> {
        let view = self.view.lock().map_err(NetError::from_poison)?;
        if let Some(error) = &view.failure {
            return Err(error.clone());
        }
        Ok(view.view.clone())
    }
    pub(super) fn source(&self) -> Result<StateSource<NetworkSnapshot>, NetError> {
        Ok(self
            .view
            .lock()
            .map_err(NetError::from_poison)?
            .source
            .clone())
    }
    pub(super) fn activate(&self) -> Result<(u64, ViewPublication), NetError> {
        let source = self.source.state.lock().map_err(NetError::from_poison)?;
        if let Some(error) = &source.publication_error {
            return Err(error.clone());
        }
        let mut view = self.view.lock().map_err(NetError::from_poison)?;
        if let Some(error) = &view.failure {
            return Err(error.clone());
        }
        let mut publication = ViewPublication::empty();
        view.reserve(&mut publication)?;
        let previous = view.view.snapshot.clone();
        view.view.activate((*source.snapshot).clone())?;
        view.source_revision = source.revision;
        view.prepare(&previous, &mut publication, true);
        publication.transition = Transition::capture(
            "view",
            view.view.generation,
            &previous,
            &view.view.snapshot,
            true,
        );
        Ok((view.view.generation, publication))
    }
    pub(super) fn retire(
        &self,
        permanent: bool,
        failure: Option<NetError>,
    ) -> Result<ViewPublication, NetError> {
        let _source = self.source.state.lock().map_err(NetError::from_poison)?;
        let mut view = self.view.lock().map_err(NetError::from_poison)?;
        let mut publication = ViewPublication::empty();
        if view.view.closed() {
            return Ok(publication);
        }
        let previous = view.view.snapshot.clone();
        let result = view.reserve(&mut publication).and_then(|()| {
            if let Some(error) = failure {
                return Err(error);
            }
            view.view.stop(permanent)
        });
        match result {
            Ok(true) => view.prepare(&previous, &mut publication, false),
            Ok(false) => {}
            Err(error) => view.fail(error, &mut publication),
        }
        publication.transition = Transition::capture(
            "view",
            view.view.generation,
            &previous,
            &view.view.snapshot,
            true,
        );
        Ok(publication)
    }
    /// Compatibility refresh for passive HTTP cached reads. It is not a new
    /// observation and therefore must not fabricate a reachability event.
    pub(super) fn refresh_latest(&self) -> Result<ViewPublication, NetError> {
        let source = self.source.state.lock().map_err(NetError::from_poison)?;
        let mut view = self.view.lock().map_err(NetError::from_poison)?;
        if let Some(error) = &view.failure {
            return Err(error.clone());
        }
        let mut publication = ViewPublication::empty();
        if view.view.closed() {
            return Ok(publication);
        }
        if let Some(error) = &source.publication_error {
            return Err(error.clone());
        }
        if view.view.refresh((*source.snapshot).clone())? {
            publication.track(&view.engine);
            view.source_revision = source.revision;
            let snapshot = Arc::new(view.view.snapshot.clone());
            publication.state = Some(if view.view.closed() {
                view.publisher.prepare_finish_arc(snapshot)
            } else {
                view.publisher.prepare_arc(snapshot)
            });
            if let Some(error) = publication
                .state
                .as_ref()
                .and_then(|state| state.result().err())
            {
                view.fail(error, &mut publication);
            }
        }
        Ok(publication)
    }
    pub(super) fn subscribe_events(
        &self,
        options: NetworkEventOptions,
    ) -> Result<EventReceiver<NetworkEvent>, NetError> {
        let (executor, quota) = {
            let view = self.view.lock().map_err(NetError::from_poison)?;
            (view.executor.clone(), view.quota.clone())
        };
        let permit = quota
            .try_acquire_owned()
            .map_err(|_| NetError::from(ErrorKind::SubscriptionLimitReached))?;
        let (publisher, receiver) = event_channel(
            EventQueueLimit {
                max_items: options.capacity,
                max_bytes: options.max_bytes,
            },
            EventOverflow::Disconnect,
            executor,
            Some(permit),
        )?;
        let mut producer = Some(publisher);
        let mut publication = ViewPublication::empty();
        let result = (|| {
            let _source = self.source.state.lock().map_err(NetError::from_poison)?;
            let mut view = self.view.lock().map_err(NetError::from_poison)?;
            if !view.view.closed() {
                publication.track(&view.engine);
            }
            publication
                .events
                .try_reserve_exact(1)
                .map_err(|error| NetError::with_source(ErrorKind::ResourceExhausted, error))?;
            publication
                .retired
                .try_reserve_exact(view.events.len())
                .map_err(|error| NetError::with_source(ErrorKind::ResourceExhausted, error))?;
            let mut index = 0;
            while index < view.events.len() {
                if view.events[index].is_active() {
                    index += 1;
                } else {
                    publication.retired.push(view.events.remove(index));
                }
            }
            if !view.view.closed() {
                view.events
                    .try_reserve(1)
                    .map_err(|error| NetError::with_source(ErrorKind::ResourceExhausted, error))?;
            }
            let initial = view.event(NetworkEventKind::Initial);
            let size = event_size(&initial);
            let seed = EventSeed {
                entries: VecDeque::from([(initial, size)]),
                lagged: 0,
                source_closed: view.view.closed(),
                failure: view.failure.clone(),
            };
            let prepared = producer
                .as_ref()
                .ok_or_else(|| NetError::from(ErrorKind::Internal))?
                .prepare_seed(seed);
            let result = prepared.result();
            publication.events.push(prepared);
            result?;
            if !view.view.closed() {
                view.events.push(
                    producer
                        .take()
                        .ok_or_else(|| NetError::from(ErrorKind::Internal))?,
                );
            }
            Ok::<(), NetError>(())
        })();
        publication.dispatch();
        result?;
        Ok(receiver)
    }
    pub(crate) fn refresh_target(&self) -> Result<NetworkRefreshTarget, NetError> {
        let source = self.source.state.lock().map_err(NetError::from_poison)?;
        let view = self.view.lock().map_err(NetError::from_poison)?;
        if let Some(error) = &view.failure {
            return Err(error.clone());
        }
        if view.view.closed() || matches!(source.snapshot.state, MonitorState::Closed) {
            return Err(ErrorKind::Closed.into());
        }
        if !view.view.active
            || !source.active
            || !matches!(view.view.snapshot.state, MonitorState::Running)
            || !matches!(source.snapshot.state, MonitorState::Running)
        {
            return Err(ErrorKind::NotRunning.into());
        }
        Ok(NetworkRefreshTarget {
            owner_generation: view.view.generation,
            source_generation: source.generation,
            invalidation_epoch: view.invalidation_epoch,
            invalidated: view.invalidated.subscribe(),
        })
    }
    #[allow(clippy::too_many_arguments)]
    pub(super) fn commit_refresh(
        &self,
        target: NetworkRefreshTarget,
        sample_id: u64,
        reachability: NetworkStatus,
        _ip_stack: Option<IpStack>,
        observed_at: SystemTime,
        validate: impl FnOnce() -> Result<(), NetError>,
    ) -> Result<(Result<NetworkSnapshot, NetError>, NetworkPublication), NetError> {
        let mut source = self.source.state.lock().map_err(NetError::from_poison)?;
        {
            let view = self.view.lock().map_err(NetError::from_poison)?;
            if let Some(error) = &view.failure {
                return Err(error.clone());
            }
            if view.view.closed() || matches!(source.snapshot.state, MonitorState::Closed) {
                return Err(ErrorKind::Closed.into());
            }
            if target.owner_generation != view.view.generation
                || target.source_generation != source.generation
                || !view.view.active
                || !source.active
                || !matches!(view.view.snapshot.state, MonitorState::Running)
                || !matches!(source.snapshot.state, MonitorState::Running)
                || sample_id <= source.last_sample_id
            {
                return Err(ErrorKind::Cancelled.into());
            }
        }
        validate()?;
        let ip_stack = source.snapshot.ip_stack;
        source.last_sample_id = sample_id;
        let publication = if same_observation(&source.snapshot, reachability, ip_stack) {
            NetworkPublication::empty()
        } else {
            let next = next_snapshot(
                &source,
                MonitorState::Running,
                Some(reachability),
                ip_stack,
                Some(observed_at),
            )?;
            self.source.commit(&mut source, next, false)?
        };
        // Shared publication may succeed while this named projection fails.
        // Preserve every prepared notification even on a target-view failure;
        // the caller must dispatch it outside all locks before returning the
        // request-specific result. Other views retain the committed sample.
        let snapshot = self
            .view
            .lock()
            .map_err(NetError::from_poison)
            .and_then(|view| match &view.failure {
                Some(error) => Err(error.clone()),
                None => Ok(view.view.snapshot.clone()),
            });
        Ok((snapshot, publication))
    }
    #[cfg(test)]
    pub(super) fn exhaust_revision_for_test(&self) -> Result<(), NetError> {
        self.view
            .lock()
            .map_err(NetError::from_poison)?
            .view
            .snapshot
            .revision = u64::MAX;
        Ok(())
    }
    #[cfg(test)]
    pub(super) fn exhaust_event_sequence_for_test(&self) -> Result<(), NetError> {
        self.view.lock().map_err(NetError::from_poison)?.sequence = u64::MAX;
        Ok(())
    }
    #[cfg(test)]
    pub(super) fn close_source_for_test(&self) -> Result<(), NetError> {
        let (publisher, mut snapshot) = {
            let view = self.view.lock().map_err(NetError::from_poison)?;
            (view.publisher.clone(), view.view.snapshot.clone())
        };
        snapshot.state = MonitorState::Closed;
        publisher.finish(snapshot)
    }
}
fn same_observation(
    snapshot: &NetworkSnapshot,
    reachability: NetworkStatus,
    ip_stack: Option<IpStack>,
) -> bool {
    matches!(snapshot.state, MonitorState::Running)
        && snapshot.reachability == Some(reachability)
        && snapshot.ip_stack == ip_stack
        && snapshot.network_name.is_none()
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
        network_name: None,
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
            crate::log_e!(LogType::Engine; "net_status.drop", "event|kind|stage", "network_status_observation_stop", format!("{:?}", error.kind()), format!("{:?}", error.context().stage));
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
            Some(status) => self
                .publisher
                .prepare_observation(status, Some(IpStack::None))?,
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
            crate::log_e!(LogType::Engine; "net_status.drop", "event|kind|stage", "test_network_status_source_stop", format!("{:?}", error.kind()), format!("{:?}", error.context().stage));
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
#[path = "network_events_tests.rs"]
mod event_tests;
#[cfg(test)]
#[path = "network_status_snapshot_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "logging_tests.rs"]
mod logging_tests;

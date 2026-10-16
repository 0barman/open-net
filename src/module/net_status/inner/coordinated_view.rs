//! Named views are mutated only while the shared source coordinator is locked.
//! Their private mutex also permits cheap coherent reads without holding that lock.
use std::sync::{Arc, Mutex};

use crate::common::retirement::{ExecutionScope, WorkLease};
use crate::common::CommonEngine;
use crate::error::{ErrorKind, NetError};
use crate::net_status::{MonitorState, NetworkEvent, NetworkEventKind, NetworkSnapshot};
use crate::subscription::{
    CallbackExecutor, EventPublication, EventPublisher, StatePublication, StatePublisher,
    StateSource,
};
use tokio::sync::{watch, Semaphore};

use super::network_view::NetworkView;

pub(super) struct CoordinatedView {
    pub(super) view: NetworkView,
    pub(super) source_revision: u64,
    pub(super) sequence: u64,
    pub(super) failure: Option<NetError>,
    pub(super) publisher: StatePublisher<NetworkSnapshot>,
    pub(super) source: StateSource<NetworkSnapshot>,
    pub(super) events: Vec<EventPublisher<NetworkEvent>>,
    pub(super) executor: Arc<dyn CallbackExecutor>,
    pub(super) quota: Arc<Semaphore>,
    pub(super) invalidation_epoch: u64,
    pub(super) invalidated: watch::Sender<u64>,
    pub(super) engine: Option<Arc<CommonEngine>>,
}

/// Every queue mutation precedes this record. Retirements and notifications
/// remain here until the source and all outer lifecycle locks have been released.
#[must_use]
pub(super) struct ViewPublication {
    pub(super) state: Option<StatePublication<NetworkSnapshot>>,
    terminal: Option<StatePublication<NetworkSnapshot>>,
    pub(super) events: Vec<EventPublication<NetworkEvent>>,
    pub(super) retired: Vec<EventPublisher<NetworkEvent>>,
    pub(super) owner: Option<Arc<Mutex<CoordinatedView>>>,
    terminal_publishers: Option<Vec<EventPublisher<NetworkEvent>>>,
    terminal_failure: Option<NetError>,
    invalidation: Option<(watch::Sender<u64>, u64)>,
    pub(super) engine: Option<Arc<CommonEngine>>,
    work: Option<WorkLease>,
}
impl ViewPublication {
    pub(super) fn empty() -> Self {
        Self {
            state: None,
            terminal: None,
            events: Vec::new(),
            retired: Vec::new(),
            owner: None,
            terminal_publishers: None,
            terminal_failure: None,
            invalidation: None,
            engine: None,
            work: None,
        }
    }
    pub(super) fn track(&mut self, engine: &Option<Arc<CommonEngine>>) {
        if self.work.is_none() {
            self.work = engine
                .as_ref()
                .map(|engine| engine.retirement_work().register());
            self.engine = engine.clone();
        }
    }
    pub(super) fn is_empty(&self) -> bool {
        self.state.is_none()
            && self.terminal.is_none()
            && self.events.is_empty()
            && self.retired.is_empty()
            && self.owner.is_none()
            && self.terminal_publishers.is_none()
            && self.invalidation.is_none()
    }
    pub(super) fn notify_invalidation(&mut self) {
        if let Some((sender, epoch)) = self.invalidation.take() {
            sender.send_if_modified(|current| {
                if *current >= epoch {
                    false
                } else {
                    *current = epoch;
                    true
                }
            });
        }
    }
    pub(super) fn dispatch_from_monitor(self) {
        if let Some(engine) = &self.engine {
            // Queue admission is already committed. Arbitrary application
            // wakers must never run on the monitor task they may try to stop.
            engine.runtime_handle().spawn_blocking(move || {
                self.dispatch();
            });
        } else {
            self.dispatch();
        }
    }
    pub(super) fn dispatch(mut self) {
        let _scope = ExecutionScope::enter();
        let _work = self.work.take();
        self.notify_invalidation();
        // An observer failure never becomes a shared network monitor failure.
        for state in [self.state, self.terminal].into_iter().flatten() {
            if let Err(error) = state.dispatch() {
                log_failure(&error);
            }
        }
        for event in self.events {
            if let Err(error) = event.dispatch() {
                log_failure(&error);
            }
        }
        if let Some(publishers) = self.terminal_publishers {
            for publisher in publishers {
                let prepared = match &self.terminal_failure {
                    Some(error) => publisher.prepare_fail(error.clone()),
                    None => publisher.prepare_finish(),
                };
                if let Err(error) = prepared.dispatch() {
                    log_failure(&error);
                }
            }
        }
        drop((self.retired, self.owner));
    }
}

impl CoordinatedView {
    pub(super) fn new(
        executor: Arc<dyn CallbackExecutor>,
        engine: Option<Arc<CommonEngine>>,
    ) -> Result<Self, NetError> {
        let view = NetworkView::new();
        let quota = Arc::new(Semaphore::new(1024));
        let (invalidated, _) = watch::channel(0);
        let (publisher, source) =
            StateSource::new_arc(Arc::new(view.snapshot.clone()), executor.clone(), 1024)?;
        Ok(Self {
            view,
            source_revision: 0,
            sequence: 0,
            failure: None,
            publisher,
            source,
            events: Vec::new(),
            executor,
            quota,
            invalidation_epoch: 0,
            invalidated,
            engine,
        })
    }
    pub(super) fn event(&self, kind: NetworkEventKind) -> NetworkEvent {
        NetworkEvent {
            sequence: self.sequence,
            generation: self.view.generation,
            source_revision: self.source_revision,
            snapshot: self.view.snapshot.clone(),
            kind,
        }
    }
    pub(super) fn reserve(&self, publication: &mut ViewPublication) -> Result<(), NetError> {
        publication
            .events
            .try_reserve_exact(self.events.len().saturating_mul(2))
            .map_err(|error| NetError::with_source(ErrorKind::ResourceExhausted, error))?;
        publication
            .retired
            .try_reserve_exact(self.events.len())
            .map_err(|error| NetError::with_source(ErrorKind::ResourceExhausted, error))
    }
    pub(super) fn prepare(
        &mut self,
        previous: &NetworkSnapshot,
        publication: &mut ViewPublication,
        activated: bool,
    ) {
        publication.track(&self.engine);
        if activated || !matches!(self.view.snapshot.state, MonitorState::Running) {
            self.invalidate(publication);
        }
        let snapshot = Arc::new(self.view.snapshot.clone());
        publication.state = Some(if self.view.closed() {
            self.publisher.prepare_finish_arc(snapshot)
        } else {
            self.publisher.prepare_arc(snapshot)
        });
        if let Some(error) = publication
            .state
            .as_ref()
            .and_then(|state| state.result().err())
        {
            self.fail(error, publication);
            return;
        }
        let kind = if matches!(self.view.snapshot.state, MonitorState::Running) {
            match (previous.reachability, self.view.snapshot.reachability) {
                (_, Some(_)) if activated || !matches!(previous.state, MonitorState::Running) => {
                    Some(NetworkEventKind::ObservationReady)
                }
                (Some(previous), Some(current)) if previous != current => {
                    Some(NetworkEventKind::ReachabilityChanged { previous, current })
                }
                _ => None,
            }
        } else if activated
            || std::mem::discriminant(&previous.state)
                != std::mem::discriminant(&self.view.snapshot.state)
        {
            Some(NetworkEventKind::LifecycleChanged)
        } else {
            None
        };
        if let Some(kind) = kind {
            let Some(sequence) = self.sequence.checked_add(1) else {
                self.fail(NetError::from(ErrorKind::ResourceExhausted), publication);
                return;
            };
            self.sequence = sequence;
            let event = self.event(kind);
            let size = event_size(&event);
            let mut index = 0;
            while index < self.events.len() {
                if !self.events[index].is_active() {
                    publication.retired.push(self.events.remove(index));
                    continue;
                }
                let queued = self.events[index].prepare_try_publish(event.clone(), size);
                let error = queued.result().err();
                publication.events.push(queued);
                if let Some(error) = error {
                    publication
                        .events
                        .push(self.events[index].prepare_fail(error));
                    publication.retired.push(self.events.remove(index));
                } else {
                    index += 1;
                }
            }
        }
        if self.view.closed() {
            for publisher in self.events.drain(..) {
                publication.events.push(publisher.prepare_finish());
                publication.retired.push(publisher);
            }
        }
    }
    pub(super) fn fail(&mut self, error: NetError, publication: &mut ViewPublication) {
        if self.failure.is_some() {
            return;
        }
        publication.track(&self.engine);
        self.invalidate(publication);
        self.failure = Some(error.clone());
        self.view.active = false;
        self.view.snapshot.state = MonitorState::Closed;
        self.view.snapshot.reachability = None;
        self.view.snapshot.ip_stack = None;
        self.view.snapshot.network_name = None;
        self.view.snapshot.observed_at = None;
        publication.terminal = Some(self.publisher.prepare_failure(error.clone()));
        // Retain the publisher vector without requiring allocation on a failure path.
        publication.terminal_publishers = Some(std::mem::take(&mut self.events));
        publication.terminal_failure = Some(error);
    }
    fn invalidate(&mut self, publication: &mut ViewPublication) {
        self.invalidation_epoch = self.invalidation_epoch.saturating_add(1);
        publication.invalidation = Some((self.invalidated.clone(), self.invalidation_epoch));
    }
}
pub(super) fn event_size(event: &NetworkEvent) -> usize {
    std::mem::size_of::<NetworkEvent>()
        .saturating_add(event.snapshot.network_name.as_ref().map_or(0, String::len))
}
fn log_failure(error: &NetError) {
    crate::log_e!(crate::common::log::log_def::LogType::Engine;
        "network_event_observer", "kind", format!("{:?}", error.kind()));
}

#[cfg(test)]
#[path = "retirement_dispatch_tests.rs"]
mod retirement_tests;

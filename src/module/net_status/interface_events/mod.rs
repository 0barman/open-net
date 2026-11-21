//! A named client's opt-in collectors. Each requested source runs once per view.
//! Commit queue entries under the view lock, dispatch caller wakers after unlocking.
mod dispatch;
mod protocol;
mod worker;

use crate::common::CommonEngine;
use crate::error::{ErrorKind, ErrorStage, NetError};
use crate::net_status::{
    InterfaceEvent, InterfaceEventKind as Kind, InterfaceEventOptions,
    InterfaceEventSource as Source,
};
use crate::subscription::{lock, EventPublisher, EventReceiver, SubscriptionId};
use crate::Result;
use dispatch::Batch;
use protocol::{Record, RouteHistory};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use tokio::sync::{oneshot, watch, Semaphore};

type Registry = Arc<Mutex<State>>;
type Retirement = watch::Receiver<Option<Result<()>>>;

#[derive(Default)]
pub(crate) struct InterfaceEventScope {
    registry: Registry,
}

#[derive(Default)]
struct State {
    closed: bool,
    subscribers: BTreeMap<SubscriptionId, Subscriber>,
    sources: BTreeMap<Source, ActiveSource>,
    // A successor can be admitted while the previous process is unregistering.
    // Its first native operation waits until that process has actually exited.
    process_slots: BTreeMap<Source, Arc<Semaphore>>,
    retiring: Vec<Retirement>,
    cleanup_failure: Option<NetError>,
}

struct ActiveSource {
    generation: Arc<()>,
    // Dropping this sender requests cancellation, including before first poll.
    _cancel: oneshot::Sender<()>,
    ready_at: Option<SystemTime>,
    route: RouteHistory,
}

struct Subscriber {
    id: SubscriptionId,
    engine: Arc<CommonEngine>,
    publisher: EventPublisher<InterfaceEvent>,
    options: InterfaceEventOptions,
    sequence: u64,
}

impl Subscriber {
    fn wants(&self, source: Source) -> bool {
        match source {
            Source::Wlan => self.options.wlan,
            Source::DefaultRoute => self.options.default_route,
        }
    }

    fn publish(&mut self, event: Evidence, batch: &mut Batch) -> Result<()> {
        self.sequence = self
            .sequence
            .checked_add(1)
            .ok_or(ErrorKind::ResourceExhausted)?;
        let item = InterfaceEvent {
            sequence: self.sequence,
            source: event.source,
            observed_at: event.observed_at,
            native_time: event.native_time,
            kind: event.kind,
        };
        let bytes = item.accounted_bytes();
        let publication = self.publisher.prepare_try_publish(item, bytes);
        let result = publication.result();
        batch.queue(self, publication);
        result
    }
}

#[derive(Clone)]
struct Evidence {
    source: Source,
    observed_at: SystemTime,
    native_time: Option<SystemTime>,
    kind: Kind,
}

impl Evidence {
    fn new(source: Source, kind: Kind) -> Self {
        Self {
            source,
            kind,
            observed_at: SystemTime::now(),
            native_time: None,
        }
    }
}

fn log_error(error: &NetError) {
    crate::log_e!(crate::LogType::Engine; "interface_events.error", "kind", format!("{:?}", error.kind()));
}

fn monitor_error(kind: ErrorKind) -> NetError {
    NetError::from(kind).with_stage(ErrorStage::NetworkMonitor)
}

impl ActiveSource {
    fn baseline(&self, source: Source) -> Vec<Evidence> {
        let Some(observed_at) = self.ready_at else {
            return Vec::new();
        };
        let mut entries = vec![Evidence {
            source,
            kind: Kind::SourceReady,
            observed_at,
            native_time: None,
        }];
        if source == Source::DefaultRoute {
            entries.extend(self.route.baselines().map(|sample| Evidence {
                source,
                kind: sample.kind.clone(),
                observed_at: sample.observed_at,
                native_time: None,
            }));
        }
        entries
    }

    fn apply(&mut self, source: Source, record: Record) -> Vec<Evidence> {
        let observed_at = SystemTime::now();
        let kind = match record {
            Record::Ready => {
                self.ready_at = Some(observed_at);
                return self.baseline(source);
            }
            Record::Gap(native_code) => {
                let error = monitor_error(ErrorKind::Io);
                if source == Source::DefaultRoute {
                    Some(self.route.gap(error, native_code, observed_at))
                } else {
                    Some(Kind::SourceGap { error, native_code })
                }
            }
            Record::RouteFailed {
                family,
                native_code,
            } => self.route.failed(
                family,
                monitor_error(ErrorKind::Io),
                native_code,
                observed_at,
            ),
            Record::Route { family, state } => self.route.observe(family, state, observed_at),
            Record::Wlan {
                code,
                interface,
                at,
                reason,
            } => {
                let kind = if code == 8001 {
                    Kind::WlanConnected {
                        interface_id: interface,
                        native_code: code,
                        reason_code: reason,
                    }
                } else {
                    Kind::WlanDisconnected {
                        interface_id: interface,
                        native_code: code,
                        reason_code: reason,
                    }
                };
                return vec![Evidence {
                    source,
                    kind,
                    observed_at,
                    native_time: at,
                }];
            }
            // Terminal records retire their source and its registration together.
            Record::Failed(_) => return Vec::new(),
        };
        kind.map(|kind| Evidence {
            source,
            kind,
            observed_at,
            native_time: None,
        })
        .into_iter()
        .collect()
    }
}

impl State {
    fn stop_unused(&mut self) {
        self.sources
            .retain(|source, _| self.subscribers.values().any(|sub| sub.wants(*source)));
    }

    fn deliver(&mut self, event: Evidence, terminal: Option<&NetError>, batch: &mut Batch) {
        let mut retired = Vec::new();
        for (id, subscriber) in &mut self.subscribers {
            if !subscriber.wants(event.source) {
                continue;
            }
            let result = subscriber.publish(event.clone(), batch);
            if let Err(error) = result {
                batch.queue(subscriber, subscriber.publisher.prepare_fail(error));
                retired.push(*id);
            } else if let Some(error) = terminal {
                match event.source {
                    Source::Wlan => subscriber.options.wlan = false,
                    Source::DefaultRoute => subscriber.options.default_route = false,
                }
                if !subscriber.options.wlan && !subscriber.options.default_route {
                    batch.queue(subscriber, subscriber.publisher.prepare_fail(error.clone()));
                    retired.push(*id);
                }
            }
        }
        for id in retired {
            if let Some(sub) = self.subscribers.remove(&id) {
                batch.retire(sub);
            }
        }
        self.stop_unused();
    }
}

fn record(registry: &Registry, source: Source, generation: &Arc<()>, input: Record) -> Result<()> {
    if let Record::Failed(code) = input {
        fail(
            registry,
            source,
            generation,
            monitor_error(ErrorKind::Io),
            code,
        );
        return Ok(());
    }
    let mut batch = Batch::default();
    {
        let mut state = lock(registry);
        let active = state
            .sources
            .get_mut(&source)
            .filter(|s| Arc::ptr_eq(&s.generation, generation))
            .ok_or(ErrorKind::Closed)?;
        for event in active.apply(source, input) {
            state.deliver(event, None, &mut batch);
        }
    }
    batch.dispatch();
    Ok(())
}

fn fail(
    registry: &Registry,
    source: Source,
    generation: &Arc<()>,
    error: NetError,
    native_code: Option<i64>,
) {
    let mut batch = Batch::default();
    {
        let mut state = lock(registry);
        if !state
            .sources
            .get(&source)
            .is_some_and(|s| Arc::ptr_eq(&s.generation, generation))
        {
            return;
        }
        state.sources.remove(&source);
        let kind = Kind::SourceUnavailable {
            error: error.clone(),
            native_code,
        };
        state.deliver(Evidence::new(source, kind), Some(&error), &mut batch);
    }
    batch.dispatch();
}

fn start(engine: &Arc<CommonEngine>, registry: Registry, source: Source, state: &mut State) {
    let (cancel, mut stopped) = oneshot::channel();
    let (finished, retired) = watch::channel(None);
    state.retiring.retain_mut(|task| {
        if task.has_changed().is_ok() {
            return true;
        }
        let outcome = task
            .borrow_and_update()
            .clone()
            .unwrap_or_else(|| Err(monitor_error(ErrorKind::RuntimeUnavailable)));
        if let Err(error) = outcome {
            state.cleanup_failure.get_or_insert(error);
        }
        false
    });
    state.retiring.push(retired);
    let generation = Arc::new(());
    state.sources.insert(
        source,
        ActiveSource {
            generation: generation.clone(),
            _cancel: cancel,
            ready_at: None,
            route: RouteHistory::default(),
        },
    );
    let slot = state
        .process_slots
        .entry(source)
        .or_insert_with(|| Arc::new(Semaphore::new(1)))
        .clone();
    let work = engine.retirement_work().register();
    engine.runtime_handle().spawn(async move {
        let _work = work;
        let _slot = tokio::select! {
            biased;
            _ = &mut stopped => { finished.send_replace(Some(Ok(()))); return; }
            permit = slot.acquire_owned() => match permit {
                Ok(permit) => permit,
                Err(error) => {
                    let failure = NetError::with_source(ErrorKind::Internal, error);
                    fail(&registry, source, &generation, failure.clone(), None);
                    finished.send_replace(Some(Err(failure)));
                    return;
                }
            },
        };
        let (observation, cleanup) = worker::collect(source, stopped, |input| {
            record(&registry, source, &generation, input)
        })
        .await;
        if let Err(error) = observation {
            let code = std::error::Error::source(&error)
                .and_then(|source| source.downcast_ref::<std::io::Error>())
                .and_then(std::io::Error::raw_os_error)
                .map(i64::from);
            fail(
                &registry,
                source,
                &generation,
                error.with_stage(ErrorStage::NetworkMonitor),
                code,
            );
        }
        if let Err(error) = &cleanup {
            log_error(error);
        }
        finished.send_replace(Some(cleanup));
    });
}

impl InterfaceEventScope {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn subscribe(
        &self,
        engine: &Arc<CommonEngine>,
        options: InterfaceEventOptions,
    ) -> Result<EventReceiver<InterfaceEvent>> {
        let (publisher, receiver) =
            super::inner::interface_event_channel::channel(engine, options)?;
        let id = receiver.id();
        let weak = Arc::downgrade(&self.registry);
        publisher.on_receiver_closed(Arc::new(move || {
            if let Some(registry) = weak.upgrade() {
                let retired = {
                    let mut state = lock(&registry);
                    let retired = state.subscribers.remove(&id);
                    state.stop_unused();
                    retired
                };
                drop(retired);
            }
        }))?;
        let mut batch = Batch::default();
        {
            let mut state = lock(&self.registry);
            if state.closed {
                return Err(ErrorKind::Closed.into());
            }
            let mut subscriber = Subscriber {
                id,
                engine: engine.clone(),
                publisher,
                options,
                sequence: 0,
            };
            let mut failure = None;
            for source in [Source::Wlan, Source::DefaultRoute] {
                if !subscriber.wants(source) {
                    continue;
                }
                if let Some(active) = state.sources.get(&source) {
                    for event in active.baseline(source) {
                        if let Err(error) = subscriber.publish(event, &mut batch) {
                            failure = Some(error);
                            break;
                        }
                    }
                } else {
                    start(engine, self.registry.clone(), source, &mut state);
                }
                if failure.is_some() {
                    break;
                }
            }
            if let Some(error) = failure {
                batch.queue(&subscriber, subscriber.publisher.prepare_fail(error));
                batch.retire(subscriber);
                state.stop_unused();
            } else {
                state.subscribers.insert(id, subscriber);
            }
        }
        batch.dispatch();
        Ok(receiver)
    }

    pub(crate) fn request_close(&self) {
        let mut batch = Batch::default();
        {
            let mut state = lock(&self.registry);
            state.closed = true;
            state.sources.clear();
            for (_, sub) in std::mem::take(&mut state.subscribers) {
                batch.queue(&sub, sub.publisher.prepare_finish());
                batch.retire(sub);
            }
        }
        batch.dispatch();
    }

    pub(crate) async fn wait_cleanup(&self) -> Result<()> {
        let (tasks, mut failure) = {
            let state = lock(&self.registry);
            (state.retiring.clone(), state.cleanup_failure.clone())
        };
        for mut task in tasks {
            let result = task
                .wait_for(|value| value.is_some())
                .await
                .map_err(|_| monitor_error(ErrorKind::RuntimeUnavailable))
                .and_then(|state| {
                    state
                        .clone()
                        .ok_or_else(|| monitor_error(ErrorKind::Internal))?
                });
            if let Err(error) = result {
                failure.get_or_insert(error);
            }
        }
        failure.map_or(Ok(()), Err)
    }
}

impl Drop for InterfaceEventScope {
    fn drop(&mut self) {
        self.request_close();
    }
}

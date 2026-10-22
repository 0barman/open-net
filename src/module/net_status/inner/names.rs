//! Names have independent demand, storage and lifetime. No name publisher can
//! reach the connectivity source. Wake channels carry no identifying values.
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, Weak};
use std::time::Instant;
#[cfg(any(windows, test))]
use std::time::SystemTime;

use crate::common::CommonEngine;
use crate::error::{ErrorKind, ErrorStage, NetError};
use crate::net_status::{
    NetworkNameObserver, NetworkNameOptions, NetworkNameSnapshot, NetworkNameState,
};
use tokio::sync::watch;

#[cfg(any(windows, test))]
const MAX_CONSUMERS: usize = 32;

#[cfg(any(windows, test))]
pub(super) type NameProvider = dyn Fn(Instant, &AtomicBool) -> Result<super::platform::windows::NameSample, NetError>
    + Send
    + Sync;

fn error(kind: ErrorKind) -> NetError {
    NetError::from(kind).with_stage(ErrorStage::NetworkMonitor)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // Native provider code never runs while these locks are held. Recovering a
    // poisoned lifecycle lock still lets shutdown clear identifying values.
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct ConsumerState {
    closed: bool,
    in_flight: bool,
    revision: u64,
    latest: NetworkNameSnapshot,
    next_due: Instant,
}

pub(crate) struct NameConsumer {
    id: u64,
    service: Weak<NameService>,
    options: NetworkNameOptions,
    state: Mutex<ConsumerState>,
    changes: watch::Sender<()>,
}

impl NameConsumer {
    pub(crate) fn snapshot(&self) -> Result<(u64, NetworkNameSnapshot), NetError> {
        let state = lock(&self.state);
        Ok((state.revision, state.latest.clone()))
    }

    pub(crate) fn next_snapshot(
        &self,
        seen: u64,
        initial: bool,
    ) -> Option<(u64, NetworkNameSnapshot)> {
        let state = lock(&self.state);
        if state.closed {
            Some((state.revision, state.latest.clone()))
        } else if initial {
            Some((0, NetworkNameSnapshot::empty(NetworkNameState::Pending)))
        } else if state.revision > seen {
            Some((state.revision, state.latest.clone()))
        } else {
            // Never clone identifying data into a future that will suspend.
            None
        }
    }

    pub(crate) fn request_close(&self) {
        if let Some(service) = self.service.upgrade() {
            service.close_consumer(self.id);
        } else {
            // Service retirement already cleared and scheduled terminal wakes.
            // The observer never keeps the service or runtime alive by itself.
            self.clear();
        }
    }

    fn clear(&self) -> bool {
        let mut state = lock(&self.state);
        if !state.closed {
            state.closed = true;
            state.revision = state.revision.saturating_add(1);
            state.latest = NetworkNameSnapshot::empty(NetworkNameState::Closed);
            true
        } else {
            false
        }
    }

    pub(crate) async fn wait_cleanup(&self) -> Result<(), NetError> {
        let mut changed = self.changes.subscribe();
        loop {
            changed.borrow_and_update();
            {
                let state = lock(&self.state);
                if state.closed && !state.in_flight {
                    return Ok(());
                }
            }
            changed
                .changed()
                .await
                .map_err(|_| error(ErrorKind::RuntimeUnavailable))?;
        }
    }
}

struct ActiveSample {
    consumer: Arc<NameConsumer>,
    cancelled: Arc<AtomicBool>,
}

#[derive(Clone, Copy)]
struct StartTicket(u64);

struct ServiceState {
    closed: bool,
    #[cfg(any(windows, test))]
    next_id: u64,
    consumers: BTreeMap<u64, Arc<NameConsumer>>,
    worker_running: bool,
    worker_generation: u64,
    pending_start: Option<StartTicket>,
    active: Option<ActiveSample>,
}

/// One engine's bounded name lane. A live worker exists only while there is
/// explicit name demand (or an already-cancelled native call is retiring).
pub(crate) struct NameService {
    engine: Arc<CommonEngine>,
    state: Mutex<ServiceState>,
    wake: Condvar,
    changes: watch::Sender<()>,
    #[cfg(any(windows, test))]
    provider: Arc<NameProvider>,
}

impl NameService {
    pub(crate) fn new(engine: Arc<CommonEngine>) -> Arc<Self> {
        Self::build(
            engine,
            #[cfg(any(windows, test))]
            Arc::new(super::platform::windows::query_name),
        )
    }

    fn build(
        engine: Arc<CommonEngine>,
        #[cfg(any(windows, test))] provider: Arc<NameProvider>,
    ) -> Arc<Self> {
        Arc::new(Self {
            engine,
            state: Mutex::new(ServiceState {
                closed: false,
                #[cfg(any(windows, test))]
                next_id: 0,
                consumers: BTreeMap::new(),
                worker_running: false,
                worker_generation: 0,
                pending_start: None,
                active: None,
            }),
            wake: Condvar::new(),
            changes: watch::channel(()).0,
            #[cfg(any(windows, test))]
            provider,
        })
    }

    #[cfg(test)]
    pub(super) fn with_provider(
        engine: Arc<CommonEngine>,
        provider: Arc<NameProvider>,
    ) -> Arc<Self> {
        Self::build(engine, provider)
    }

    #[cfg(not(any(windows, test)))]
    fn prepare_observer(
        self: &Arc<Self>,
        options: NetworkNameOptions,
    ) -> Result<(NetworkNameObserver, Option<StartTicket>), NetError> {
        options.validate()?;
        if lock(&self.state).closed {
            return Err(error(ErrorKind::Closed));
        }
        Err(error(ErrorKind::Unsupported))
    }

    #[cfg(any(windows, test))]
    fn prepare_observer(
        self: &Arc<Self>,
        options: NetworkNameOptions,
    ) -> Result<(NetworkNameObserver, Option<StartTicket>), NetError> {
        options.validate()?;
        let (consumer, start_worker) = {
            let mut state = lock(&self.state);
            if state.closed {
                return Err(error(ErrorKind::Closed));
            }
            {
                if state.consumers.len() >= MAX_CONSUMERS {
                    return Err(error(ErrorKind::SubscriptionLimitReached));
                }
                let id = state
                    .next_id
                    .checked_add(1)
                    .ok_or_else(|| error(ErrorKind::ResourceExhausted))?;
                let start = if state.worker_running {
                    None
                } else {
                    let generation = state
                        .worker_generation
                        .checked_add(1)
                        .ok_or_else(|| error(ErrorKind::ResourceExhausted))?;
                    Some(StartTicket(generation))
                };
                state.next_id = id;
                let consumer = Arc::new(NameConsumer {
                    id,
                    service: Arc::downgrade(self),
                    options,
                    state: Mutex::new(ConsumerState {
                        closed: false,
                        in_flight: false,
                        revision: 0,
                        latest: NetworkNameSnapshot::empty(NetworkNameState::Pending),
                        next_due: Instant::now(),
                    }),
                    changes: watch::channel(()).0,
                });
                state.consumers.insert(id, consumer.clone());
                if let Some(ticket) = start {
                    state.worker_generation = ticket.0;
                    state.pending_start = Some(ticket);
                    state.worker_running = true;
                }
                (consumer, start)
            }
        };
        Ok((
            NetworkNameObserver {
                changes: consumer.changes.subscribe(),
                inner: consumer,
                initial: true,
                seen: 0,
                terminal: false,
            },
            start_worker,
        ))
    }

    fn start_worker(self: &Arc<Self>, ticket: StartTicket) -> Result<(), NetError> {
        {
            let mut state = lock(&self.state);
            if state.closed
                || state.consumers.is_empty()
                || state.pending_start.map(|pending| pending.0) != Some(ticket.0)
            {
                // Closing can revoke an unclaimed start. An obsolete caller
                // cannot create a thread, publish, or retire a successor's work.
                return Err(error(ErrorKind::Closed));
            }
            // From this point shutdown must await either failed registration
            // or this generation's worker exit. No cancellable gap remains.
            state.pending_start = None;
        }
        let service = self.clone();
        // Registration in the scope precedes this call, but no scope or service
        // lock is held while registry reaping joins old threads.
        if self
            .engine
            .resource_threads()
            .spawn("open-net-network-names", move || service.run(ticket))
            .is_err()
        {
            self.worker_start_failed(ticket);
            return Err(error(ErrorKind::RuntimeUnavailable));
        }
        Ok(())
    }

    fn notify(&self, changes: &watch::Sender<()>) {
        // A user-supplied Waker may block, re-enter the SDK or panic. It must
        // never run on the native sampling thread or a close initiator. This
        // publication owns no names and is covered by engine retirement.
        let changes = changes.clone();
        let engine = self.engine.clone();
        let work = engine.retirement_work().register();
        let runtime = engine.runtime_handle();
        runtime.spawn_blocking(move || {
            let _engine = engine;
            let _work = work;
            let _scope = crate::common::retirement::ExecutionScope::enter();
            if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                changes.send_replace(());
            })) {
                crate::common::retirement::discard_panic_payload(payload);
            }
        });
    }

    fn worker_start_failed(&self, ticket: StartTicket) {
        let (consumers, _publication) = {
            let mut state = lock(&self.state);
            if state.worker_generation != ticket.0 || !state.worker_running {
                return;
            }
            let publication = self.engine.retirement_work().register();
            state.worker_running = false;
            state.pending_start = None;
            let consumers = std::mem::take(&mut state.consumers);
            for consumer in consumers.values() {
                consumer.clear();
            }
            (consumers, publication)
        };
        for consumer in consumers.values() {
            self.notify(&consumer.changes);
        }
        self.notify(&self.changes);
    }

    fn close_consumer(&self, id: u64) {
        let (consumer, cancelled_start, _publication) = {
            let mut state = lock(&self.state);
            let consumer = state.consumers.remove(&id).or_else(|| {
                state
                    .active
                    .as_ref()
                    .filter(|active| active.consumer.id == id)
                    .map(|active| active.consumer.clone())
            });
            let changed = consumer.as_ref().is_some_and(|consumer| consumer.clear());
            let cancelled_start =
                state.consumers.is_empty() && state.pending_start.take().is_some();
            if cancelled_start {
                state.worker_running = false;
            }
            // Reserve retirement before exposing the removal to concurrent
            // engine shutdown. It covers the gap before deferred wakes enqueue.
            let publication =
                (changed || cancelled_start).then(|| self.engine.retirement_work().register());
            if let Some(active) = state
                .active
                .as_ref()
                .filter(|active| active.consumer.id == id)
            {
                active.cancelled.store(true, Ordering::Release);
            }
            (consumer.filter(|_| changed), cancelled_start, publication)
        };
        self.wake.notify_all();
        if let Some(consumer) = consumer {
            self.notify(&consumer.changes);
        }
        if cancelled_start {
            self.notify(&self.changes);
        }
    }

    pub(crate) fn request_close(&self) {
        let (consumers, _publication) = {
            let mut state = lock(&self.state);
            if state.closed {
                return;
            }
            let publication = self.engine.retirement_work().register();
            state.closed = true;
            if state.pending_start.take().is_some() {
                state.worker_running = false;
            }
            let mut consumers = std::mem::take(&mut state.consumers);
            if let Some(active) = &state.active {
                active.cancelled.store(true, Ordering::Release);
                consumers
                    .entry(active.consumer.id)
                    .or_insert_with(|| active.consumer.clone());
            }
            for consumer in consumers.values() {
                consumer.clear();
            }
            (consumers, publication)
        };
        self.wake.notify_all();
        for consumer in consumers.values() {
            self.notify(&consumer.changes);
        }
        self.notify(&self.changes);
    }

    pub(crate) async fn wait_cleanup(&self) -> Result<(), NetError> {
        let mut changed = self.changes.subscribe();
        loop {
            changed.borrow_and_update();
            if !lock(&self.state).worker_running {
                return Ok(());
            }
            changed
                .changed()
                .await
                .map_err(|_| error(ErrorKind::RuntimeUnavailable))?;
        }
    }

    fn run(self: Arc<Self>, ticket: StartTicket) {
        loop {
            let work = {
                let mut state = lock(&self.state);
                loop {
                    if state.worker_generation != ticket.0 {
                        break None;
                    }
                    if state.closed || state.consumers.is_empty() {
                        state.worker_running = false;
                        break None;
                    }
                    let next = state
                        .consumers
                        .values()
                        .min_by_key(|consumer| lock(&consumer.state).next_due)
                        .cloned();
                    let Some(consumer) = next else {
                        continue;
                    };
                    let due = lock(&consumer.state).next_due;
                    let now = Instant::now();
                    if due > now {
                        state = self
                            .wake
                            .wait_timeout(state, due - now)
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .0;
                        continue;
                    }
                    lock(&consumer.state).in_flight = true;
                    let cancelled = Arc::new(AtomicBool::new(false));
                    state.active = Some(ActiveSample {
                        consumer: consumer.clone(),
                        cancelled: cancelled.clone(),
                    });
                    break Some((consumer, cancelled));
                }
            };
            let Some((consumer, cancelled)) = work else {
                self.notify(&self.changes);
                return;
            };
            let deadline = Instant::now() + consumer.options.timeout();
            #[cfg(any(windows, test))]
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                (self.provider)(deadline, &cancelled)
            }))
            .unwrap_or_else(|payload| {
                crate::common::retirement::discard_panic_payload(payload);
                Err(error(ErrorKind::Internal))
            });
            #[cfg(not(any(windows, test)))]
            let result: Result<(), NetError> = Err(error(ErrorKind::Unsupported));
            // Sanitization and disposal of arbitrary native error sources happen
            // outside authority locks, before completion can be acknowledged.
            let snapshot = if Instant::now() >= deadline {
                drop(result);
                NetworkNameSnapshot::failed(&error(ErrorKind::TimedOut))
            } else {
                #[cfg(any(windows, test))]
                {
                    match result {
                        Ok(sample) => NetworkNameSnapshot::ready(
                            sample.name,
                            sample.source,
                            SystemTime::now(),
                            sample.diagnostics,
                        ),
                        Err(error) => NetworkNameSnapshot::failed(&error),
                    }
                }
                #[cfg(not(any(windows, test)))]
                {
                    drop(result);
                    NetworkNameSnapshot::failed(&error(ErrorKind::Unsupported))
                }
            };
            {
                let mut service = lock(&self.state);
                let mut state = lock(&consumer.state);
                if !state.closed && !service.closed && !cancelled.load(Ordering::Acquire) {
                    let snapshot = if Instant::now() >= deadline {
                        drop(snapshot);
                        NetworkNameSnapshot::failed(&error(ErrorKind::TimedOut))
                    } else {
                        snapshot
                    };
                    state.latest = snapshot;
                    state.revision = state.revision.saturating_add(1);
                    state.next_due = Instant::now() + consumer.options.interval();
                } else {
                    drop(snapshot);
                }
                // Drop every raw native result before marking work retired.
                // Error sources may own identifying text too.
                state.in_flight = false;
                service.active = None;
            }
            self.notify(&consumer.changes);
        }
    }
}

impl Drop for NameService {
    fn drop(&mut self) {
        self.request_close();
    }
}

struct ScopeState {
    closed: bool,
    consumers: Vec<Weak<NameConsumer>>,
}

/// A facade's ownership boundary. Weak entries remain upgradeable while a
/// dropped observer still has native work in flight, because that worker owns
/// its Consumer until all native results have been discarded.
pub(crate) struct NameScope {
    service: Arc<NameService>,
    state: Mutex<ScopeState>,
    #[cfg(test)]
    before_start: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl NameScope {
    pub(crate) fn new(service: Arc<NameService>) -> Self {
        Self {
            service,
            state: Mutex::new(ScopeState {
                closed: false,
                consumers: Vec::new(),
            }),
            #[cfg(test)]
            before_start: Mutex::new(None),
        }
    }

    pub(crate) fn observe(
        &self,
        options: NetworkNameOptions,
    ) -> Result<NetworkNameObserver, NetError> {
        options.validate()?;
        let mut state = lock(&self.state);
        if state.closed {
            return Err(error(ErrorKind::Closed));
        }
        state.consumers.retain(|consumer| {
            consumer.upgrade().is_some_and(|consumer| {
                let state = lock(&consumer.state);
                !state.closed || state.in_flight
            })
        });
        let (observer, start_worker) = self.service.prepare_observer(options)?;
        state.consumers.push(Arc::downgrade(&observer.inner));
        drop(state);
        if let Some(ticket) = start_worker {
            #[cfg(test)]
            if let Some(before_start) = lock(&self.before_start).take() {
                before_start();
            }
            self.service.start_worker(ticket)?;
        }
        self.service.wake.notify_all();
        Ok(observer)
    }

    pub(crate) fn request_close(&self) {
        let consumers: Vec<_> = {
            let mut state = lock(&self.state);
            state.closed = true;
            state.consumers.iter().filter_map(Weak::upgrade).collect()
        };
        for consumer in consumers {
            consumer.request_close();
        }
    }

    pub(crate) fn is_closed(&self) -> bool {
        lock(&self.state).closed
    }

    pub(crate) async fn wait_cleanup(&self) -> Result<(), NetError> {
        let consumers: Vec<_> = lock(&self.state)
            .consumers
            .iter()
            .filter_map(Weak::upgrade)
            .collect();
        for consumer in consumers {
            consumer.wait_cleanup().await?;
        }
        Ok(())
    }
}

impl Drop for NameScope {
    fn drop(&mut self) {
        self.request_close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net_status::NetworkNameSource;
    use std::error::Error;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    type TestResult = Result<(), crate::BoxError>;
    const WAIT: Duration = Duration::from_secs(3);
    const SECRET: &str = "private-home-network-SECRET";

    #[derive(Default)]
    struct Gate {
        opened: Mutex<bool>,
        wake: Condvar,
    }
    impl Gate {
        fn wait(&self) {
            let opened = lock(&self.opened);
            let _ = self
                .wake
                .wait_timeout_while(opened, WAIT, |opened| !*opened)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        fn open(&self) {
            *lock(&self.opened) = true;
            self.wake.notify_all();
        }
    }

    fn service(provider: Arc<NameProvider>) -> Result<Arc<NameService>, NetError> {
        Ok(NameService::with_provider(
            Arc::new(CommonEngine::new(8, 8)?),
            provider,
        ))
    }

    fn sample(name: &str) -> super::super::platform::windows::NameSample {
        super::super::platform::windows::NameSample {
            name: Some(name.to_owned()),
            source: NetworkNameSource::WifiSsid,
            diagnostics: Vec::new(),
        }
    }

    async fn current_ready(observer: &NetworkNameObserver) -> Result<(), crate::BoxError> {
        let mut changed = observer.inner.changes.subscribe();
        tokio::time::timeout(WAIT, async {
            loop {
                changed.borrow_and_update();
                if observer.snapshot()?.state() == NetworkNameState::Ready {
                    return Ok::<(), crate::BoxError>(());
                }
                changed.changed().await?;
            }
        })
        .await??;
        Ok(())
    }

    #[tokio::test]
    async fn no_name_demand_starts_no_worker_and_last_close_retires_it() -> TestResult {
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let service = service(Arc::new(move |_, _| {
            count.fetch_add(1, Ordering::SeqCst);
            Ok(sample(SECRET))
        }))?;
        let scope = NameScope::new(service.clone());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(!lock(&service.state).worker_running);
        let mut observer = scope.observe(NetworkNameOptions::default())?;
        current_ready(&observer).await?;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        observer.close().await?;
        tokio::time::timeout(WAIT, service.wait_cleanup()).await??;
        assert!(!lock(&service.state).worker_running);
        // An ordinary observer close does not permanently close the service.
        let mut again = scope.observe(NetworkNameOptions::default())?;
        current_ready(&again).await?;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        again.close().await?;
        tokio::time::timeout(WAIT, service.wait_cleanup()).await??;
        Ok(())
    }

    #[tokio::test]
    async fn scopes_share_only_worker_and_closing_one_does_not_revoke_another() -> TestResult {
        let calls = AtomicUsize::new(0);
        let service = service(Arc::new(move |_, _| {
            Ok(sample(&format!(
                "observation-{}",
                calls.fetch_add(1, Ordering::SeqCst)
            )))
        }))?;
        let first_scope = NameScope::new(service.clone());
        let second_scope = NameScope::new(service.clone());
        let first = first_scope.observe(NetworkNameOptions::default())?;
        current_ready(&first).await?;
        let mut second = second_scope.observe(NetworkNameOptions::default())?;
        current_ready(&second).await?;
        assert_eq!(first.snapshot()?.name(), Some("observation-0"));
        assert_eq!(second.snapshot()?.name(), Some("observation-1"));
        first_scope.request_close();
        first_scope.wait_cleanup().await?;
        assert_eq!(first.snapshot()?.state(), NetworkNameState::Closed);
        assert_eq!(first.snapshot()?.name(), None);
        assert_eq!(second.snapshot()?.name(), Some("observation-1"));
        second.close().await?;
        tokio::time::timeout(WAIT, service.wait_cleanup()).await??;
        Ok(())
    }

    #[tokio::test]
    async fn close_clears_latest_unread_value_and_initial_then_emits_only_terminal() -> TestResult {
        let service = service(Arc::new(|_, _| Ok(sample(SECRET))))?;
        let scope = NameScope::new(service.clone());
        let mut observer = scope.observe(NetworkNameOptions::default())?;
        current_ready(&observer).await?;
        let application_owned = observer.snapshot()?;
        assert_eq!(application_owned.name(), Some(SECRET));
        assert!(!format!("{application_owned:?}").contains(SECRET));
        observer.close().await?;
        let closed = observer.snapshot()?;
        assert_eq!(closed.state(), NetworkNameState::Closed);
        assert!(closed.name().is_none());
        assert!(closed.sampled_at().is_none());
        assert!(closed.diagnostics().is_empty());
        assert_eq!(closed.source(), NetworkNameSource::NotCollected);
        assert_eq!(
            observer.recv().await?.map(|value| value.state()),
            Some(NetworkNameState::Closed)
        );
        assert!(observer.recv().await?.is_none());
        // Closing SDK storage does not rewrite a value already owned by a caller.
        assert_eq!(application_owned.name(), Some(SECRET));
        tokio::time::timeout(WAIT, service.wait_cleanup()).await??;
        Ok(())
    }

    #[tokio::test]
    async fn closed_scope_releases_names_held_by_an_unpolled_pending_recv() -> TestResult {
        use std::future::Future;
        use std::task::{Context, Wake, Waker};

        struct IdleWake;
        impl Wake for IdleWake {
            fn wake(self: Arc<Self>) {}
        }
        let service = service(Arc::new(|_, _| Ok(sample(SECRET))))?;
        let scope = NameScope::new(service.clone());
        let mut observer = scope.observe(NetworkNameOptions::default())?;
        current_ready(&observer).await?;
        let probe = Arc::new(());
        let retained = Arc::downgrade(&probe);
        lock(&observer.inner.state)
            .latest
            .retain_probe_for_test(probe);
        // Consume the initial Pending and the current Ready value; no application
        // snapshot is retained when the subsequent recv waits for another value.
        assert_eq!(
            observer.recv().await?.map(|snapshot| snapshot.state()),
            Some(NetworkNameState::Pending)
        );
        assert_eq!(
            observer.recv().await?.map(|snapshot| snapshot.state()),
            Some(NetworkNameState::Ready)
        );
        let waker = Waker::from(Arc::new(IdleWake));
        let mut pending = Box::pin(observer.recv());
        assert!(pending
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending());
        scope.request_close();
        tokio::time::timeout(WAIT, scope.wait_cleanup()).await??;
        tokio::time::timeout(WAIT, service.wait_cleanup()).await??;
        // Deliberately do not poll or drop the recv future after close.
        assert!(
            retained.upgrade().is_none(),
            "a pending recv future retained the previous identifying snapshot after close"
        );
        drop(pending);
        Ok(())
    }

    #[tokio::test]
    async fn close_waits_for_native_retirement_and_discards_late_name() -> TestResult {
        let gate = Arc::new(Gate::default());
        let native_gate = gate.clone();
        let (entered, mut started) = tokio::sync::mpsc::unbounded_channel();
        let service = service(Arc::new(move |_, _| {
            let _ = entered.send(());
            native_gate.wait();
            Ok(sample(SECRET))
        }))?;
        let scope = NameScope::new(service.clone());
        let mut observer = scope.observe(NetworkNameOptions::default())?;
        tokio::time::timeout(WAIT, started.recv()).await?;
        assert!(
            tokio::time::timeout(Duration::from_millis(25), observer.close())
                .await
                .is_err()
        );
        assert_eq!(observer.snapshot()?.state(), NetworkNameState::Closed);
        assert!(lock(&observer.inner.state).in_flight);
        gate.open();
        tokio::time::timeout(WAIT, observer.close()).await??;
        assert!(observer.snapshot()?.name().is_none());
        assert!(!lock(&observer.inner.state).in_flight);
        tokio::time::timeout(WAIT, service.wait_cleanup()).await??;
        Ok(())
    }

    #[tokio::test]
    async fn dropped_handle_remains_in_scope_retirement_until_native_result_is_gone() -> TestResult
    {
        let gate = Arc::new(Gate::default());
        let native_gate = gate.clone();
        let (entered, mut started) = tokio::sync::mpsc::unbounded_channel();
        let service = service(Arc::new(move |_, _| {
            let _ = entered.send(());
            native_gate.wait();
            Ok(sample(SECRET))
        }))?;
        let scope = NameScope::new(service.clone());
        let observer = scope.observe(NetworkNameOptions::default())?;
        tokio::time::timeout(WAIT, started.recv()).await?;
        let consumer = Arc::downgrade(&observer.inner);
        drop(observer);
        scope.request_close();
        assert!(consumer.upgrade().is_some());
        assert!(
            tokio::time::timeout(Duration::from_millis(25), scope.wait_cleanup())
                .await
                .is_err()
        );
        gate.open();
        tokio::time::timeout(WAIT, scope.wait_cleanup()).await??;
        tokio::time::timeout(WAIT, service.wait_cleanup()).await??;
        assert!(consumer.upgrade().is_none());
        Ok(())
    }

    #[tokio::test]
    async fn timeout_discards_expired_native_name() -> TestResult {
        let service = service(Arc::new(|_, _| {
            std::thread::sleep(Duration::from_millis(140));
            Ok(sample(SECRET))
        }))?;
        let scope = NameScope::new(service.clone());
        let mut observer = scope
            .observe(NetworkNameOptions::default().with_timeout(Duration::from_millis(100)))?;
        assert_eq!(
            observer.recv().await?.map(|snapshot| snapshot.state()),
            Some(NetworkNameState::Pending)
        );
        let failed = tokio::time::timeout(WAIT, observer.recv())
            .await??
            .ok_or("missing failed sample")?;
        assert_eq!(failed.state(), NetworkNameState::Failed);
        assert_eq!(
            failed.error().map(NetError::kind),
            Some(ErrorKind::TimedOut)
        );
        assert!(failed.name().is_none());
        observer.close().await?;
        tokio::time::timeout(WAIT, service.wait_cleanup()).await??;
        Ok(())
    }

    #[tokio::test]
    async fn failures_and_fallback_diagnostics_retain_only_error_classifications() -> TestResult {
        let calls = AtomicUsize::new(0);
        let service = service(Arc::new(move |_, _| {
            let failure =
                NetError::with_source(ErrorKind::Io, std::io::Error::other(SECRET.to_owned()));
            if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                let mut sampled = sample(SECRET);
                sampled.diagnostics.push(failure);
                Ok(sampled)
            } else {
                Err(failure)
            }
        }))?;
        let scope = NameScope::new(service.clone());
        let mut observer = scope.observe(NetworkNameOptions::default())?;
        observer.recv().await?;
        let ready = tokio::time::timeout(WAIT, observer.recv())
            .await??
            .ok_or("missing ready sample")?;
        assert_eq!(ready.diagnostics().len(), 1);
        assert!(ready.diagnostics()[0].source().is_none());
        assert!(!format!("{ready:?}").contains(SECRET));
        lock(&observer.inner.state).next_due = Instant::now();
        service.wake.notify_all();
        let failed = tokio::time::timeout(WAIT, observer.recv())
            .await??
            .ok_or("missing failed sample")?;
        assert_eq!(failed.state(), NetworkNameState::Failed);
        assert!(failed.name().is_none());
        assert_eq!(failed.error().map(NetError::kind), Some(ErrorKind::Io));
        assert!(failed.error().and_then(Error::source).is_none());
        assert!(!format!("{failed:?}").contains(SECRET));
        observer.close().await?;
        tokio::time::timeout(WAIT, service.wait_cleanup()).await??;
        Ok(())
    }

    #[tokio::test]
    async fn active_consumers_are_bounded_and_shutdown_cancels_every_scope() -> TestResult {
        let gate = Arc::new(Gate::default());
        let native_gate = gate.clone();
        let service = service(Arc::new(move |_, _| {
            native_gate.wait();
            Ok(sample(SECRET))
        }))?;
        let scope = NameScope::new(service.clone());
        let mut observers = Vec::new();
        for _ in 0..MAX_CONSUMERS {
            observers.push(scope.observe(NetworkNameOptions::default())?);
        }
        assert_eq!(
            scope
                .observe(NetworkNameOptions::default())
                .unwrap_err()
                .kind(),
            ErrorKind::SubscriptionLimitReached
        );
        service.request_close();
        for observer in &observers {
            assert_eq!(observer.snapshot()?.state(), NetworkNameState::Closed);
            assert!(observer.snapshot()?.name().is_none());
        }
        gate.open();
        tokio::time::timeout(WAIT, service.wait_cleanup()).await??;
        for observer in &mut observers {
            observer.close().await?;
        }
        assert_eq!(
            scope
                .observe(NetworkNameOptions::default())
                .unwrap_err()
                .kind(),
            ErrorKind::Closed
        );
        Ok(())
    }

    #[tokio::test]
    async fn retaining_closed_handles_does_not_grow_scope_registration_history() -> TestResult {
        let service = service(Arc::new(|_, _| Ok(sample(SECRET))))?;
        let scope = NameScope::new(service.clone());
        let mut retained = Vec::new();
        for _ in 0..(MAX_CONSUMERS + 8) {
            let mut observer = scope.observe(NetworkNameOptions::default())?;
            observer.close().await?;
            retained.push(observer);
            assert!(lock(&scope.state).consumers.len() <= 1);
        }
        tokio::time::timeout(WAIT, service.wait_cleanup()).await??;
        assert_eq!(retained.len(), MAX_CONSUMERS + 8);
        Ok(())
    }

    #[tokio::test]
    async fn close_waits_for_raw_error_source_destruction_outside_authority_locks() -> TestResult {
        struct NativeError {
            gate: Arc<Gate>,
            entered: tokio::sync::mpsc::UnboundedSender<()>,
            destroyed: Arc<AtomicBool>,
        }
        impl std::fmt::Debug for NativeError {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(SECRET)
            }
        }
        impl std::fmt::Display for NativeError {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(SECRET)
            }
        }
        impl Error for NativeError {}
        impl Drop for NativeError {
            fn drop(&mut self) {
                let _ = self.entered.send(());
                self.gate.wait();
                self.destroyed.store(true, Ordering::Release);
            }
        }

        let gate = Arc::new(Gate::default());
        let native_gate = gate.clone();
        let destroyed = Arc::new(AtomicBool::new(false));
        let native_destroyed = destroyed.clone();
        let (entered, mut destroying) = tokio::sync::mpsc::unbounded_channel();
        let service = service(Arc::new(move |_, _| {
            Err(NetError::with_source(
                ErrorKind::Io,
                NativeError {
                    gate: native_gate.clone(),
                    entered: entered.clone(),
                    destroyed: native_destroyed.clone(),
                },
            ))
        }))?;
        let scope = NameScope::new(service.clone());
        let mut observer = scope.observe(NetworkNameOptions::default())?;
        tokio::time::timeout(WAIT, destroying.recv()).await?;
        // This would deadlock if source destruction still held the state locks.
        observer.inner.request_close();
        assert!(observer.snapshot()?.name().is_none());
        assert!(
            tokio::time::timeout(Duration::from_millis(25), observer.close())
                .await
                .is_err()
        );
        assert!(!destroyed.load(Ordering::Acquire));
        gate.open();
        tokio::time::timeout(WAIT, observer.close()).await??;
        assert!(destroyed.load(Ordering::Acquire));
        tokio::time::timeout(WAIT, service.wait_cleanup()).await??;
        Ok(())
    }

    #[tokio::test]
    async fn a_blocking_user_waker_cannot_block_close_initiation_or_native_sampling() -> TestResult
    {
        use std::future::Future;
        use std::task::{Context, Wake, Waker};

        struct BlockingWake {
            entered: tokio::sync::mpsc::UnboundedSender<()>,
            gate: Arc<Gate>,
            returned: Arc<AtomicBool>,
        }
        impl Wake for BlockingWake {
            fn wake(self: Arc<Self>) {
                let _ = self.entered.send(());
                self.gate.wait();
                self.returned.store(true, Ordering::Release);
            }
        }
        let native_gate = Arc::new(Gate::default());
        let provider_gate = native_gate.clone();
        let calls = AtomicUsize::new(0);
        let service = service(Arc::new(move |_, _| {
            if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                provider_gate.wait();
            }
            Ok(sample(SECRET))
        }))?;
        let scope = NameScope::new(service.clone());
        let mut observer = scope.observe(NetworkNameOptions::default())?;
        observer.recv().await?;
        let (entered, mut waking) = tokio::sync::mpsc::unbounded_channel();
        let wake_gate = Arc::new(Gate::default());
        let returned = Arc::new(AtomicBool::new(false));
        let waker = Waker::from(Arc::new(BlockingWake {
            entered,
            gate: wake_gate.clone(),
            returned: returned.clone(),
        }));
        let mut receiving = Box::pin(observer.recv());
        assert!(receiving
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending());
        scope.request_close();
        tokio::time::timeout(WAIT, waking.recv()).await?;
        assert!(!returned.load(Ordering::Acquire));
        drop(receiving);
        native_gate.open();
        tokio::time::timeout(WAIT, observer.close()).await??;
        let another_scope = NameScope::new(service.clone());
        let mut another = another_scope.observe(NetworkNameOptions::default())?;
        current_ready(&another).await?;
        assert!(!returned.load(Ordering::Acquire));
        another.close().await?;
        wake_gate.open();
        tokio::time::timeout(WAIT, service.wait_cleanup()).await??;
        Ok(())
    }

    #[tokio::test]
    async fn a_panicking_user_waker_does_not_kill_the_native_worker_or_block_restart() -> TestResult
    {
        use std::future::Future;
        use std::task::{Context, Wake, Waker};

        struct PanicWake(tokio::sync::mpsc::UnboundedSender<()>);
        impl Wake for PanicWake {
            fn wake(self: Arc<Self>) {
                let _ = self.0.send(());
                panic!("injected name observer waker panic");
            }
        }
        let native_gate = Arc::new(Gate::default());
        let provider_gate = native_gate.clone();
        let calls = AtomicUsize::new(0);
        let service = service(Arc::new(move |_, _| {
            if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                provider_gate.wait();
            }
            Ok(sample(SECRET))
        }))?;
        let scope = NameScope::new(service.clone());
        let mut observer = scope.observe(NetworkNameOptions::default())?;
        observer.recv().await?;
        let (entered, mut waking) = tokio::sync::mpsc::unbounded_channel();
        let waker = Waker::from(Arc::new(PanicWake(entered)));
        let mut receiving = Box::pin(observer.recv());
        assert!(receiving
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending());
        native_gate.open();
        tokio::time::timeout(WAIT, waking.recv()).await?;
        drop(receiving);
        tokio::time::timeout(WAIT, observer.close()).await??;
        tokio::time::timeout(WAIT, service.wait_cleanup()).await??;
        let mut again = scope.observe(NetworkNameOptions::default())?;
        current_ready(&again).await?;
        again.close().await?;
        tokio::time::timeout(WAIT, service.wait_cleanup()).await??;
        Ok(())
    }

    #[tokio::test]
    async fn closing_a_pending_start_does_not_wait_for_the_paused_observe_caller() -> TestResult {
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let service = service(Arc::new(move |_, _| {
            count.fetch_add(1, Ordering::SeqCst);
            Ok(sample(SECRET))
        }))?;
        let scope = Arc::new(NameScope::new(service.clone()));
        let (entered, reached) = tokio::sync::oneshot::channel();
        let (resume, resumed) = std::sync::mpsc::channel();
        *lock(&scope.before_start) = Some(Box::new(move || {
            let _ = entered.send(());
            let _ = resumed.recv_timeout(WAIT * 3);
        }));
        let starting_scope = scope.clone();
        let starting =
            std::thread::spawn(move || starting_scope.observe(NetworkNameOptions::default()));
        tokio::time::timeout(WAIT, reached).await??;
        scope.request_close();
        tokio::time::timeout(WAIT, scope.wait_cleanup()).await??;
        service.request_close();
        let retired_before_resume = {
            let mut cleanup = Box::pin(service.wait_cleanup());
            matches!(
                std::future::Future::poll(
                    cleanup.as_mut(),
                    &mut std::task::Context::from_waker(std::task::Waker::noop())
                ),
                std::task::Poll::Ready(Ok(()))
            )
        };
        let _ = resume.send(());
        let returned = starting.join().map_err(|_| "observer thread panicked")?;
        tokio::time::timeout(WAIT, service.wait_cleanup()).await??;
        assert!(
            retired_before_resume,
            "cleanup waited for an unclaimed worker start after all demand was revoked"
        );
        assert_eq!(returned.unwrap_err().kind(), ErrorKind::Closed);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn a_cancelled_start_ticket_cannot_block_or_replace_new_name_demand() -> TestResult {
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let service = service(Arc::new(move |_, _| {
            count.fetch_add(1, Ordering::SeqCst);
            Ok(sample(SECRET))
        }))?;
        let old_scope = Arc::new(NameScope::new(service.clone()));
        let (entered, reached) = tokio::sync::oneshot::channel();
        let (resume, resumed) = std::sync::mpsc::channel();
        *lock(&old_scope.before_start) = Some(Box::new(move || {
            let _ = entered.send(());
            let _ = resumed.recv_timeout(WAIT * 3);
        }));
        let starting_scope = old_scope.clone();
        let starting =
            std::thread::spawn(move || starting_scope.observe(NetworkNameOptions::default()));
        tokio::time::timeout(WAIT, reached).await??;
        let old_ticket = lock(&service.state)
            .pending_start
            .ok_or("missing pending start ticket")?;
        old_scope.request_close();
        tokio::time::timeout(WAIT, old_scope.wait_cleanup()).await??;
        let new_scope = NameScope::new(service.clone());
        let mut new_observer = new_scope.observe(NetworkNameOptions::default())?;
        let ready_before_old_resume = tokio::time::timeout(WAIT, current_ready(&new_observer))
            .await
            .is_ok_and(|result| result.is_ok());
        service.worker_start_failed(old_ticket);
        let _ = resume.send(());
        let returned = starting.join().map_err(|_| "observer thread panicked")?;
        current_ready(&new_observer).await?;
        new_observer.close().await?;
        tokio::time::timeout(WAIT, service.wait_cleanup()).await??;
        assert!(
            ready_before_old_resume,
            "new demand depended on a cancelled caller resuming an obsolete start"
        );
        assert_eq!(returned.unwrap_err().kind(), ErrorKind::Closed);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn closing_the_starting_scope_keeps_the_ticket_needed_by_another_consumer() -> TestResult
    {
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let service = service(Arc::new(move |_, _| {
            count.fetch_add(1, Ordering::SeqCst);
            Ok(sample(SECRET))
        }))?;
        let starting_scope = Arc::new(NameScope::new(service.clone()));
        let (entered, reached) = tokio::sync::oneshot::channel();
        let (resume, resumed) = std::sync::mpsc::channel();
        *lock(&starting_scope.before_start) = Some(Box::new(move || {
            let _ = entered.send(());
            let _ = resumed.recv_timeout(WAIT * 3);
        }));
        let observed_scope = starting_scope.clone();
        let starting =
            std::thread::spawn(move || observed_scope.observe(NetworkNameOptions::default()));
        tokio::time::timeout(WAIT, reached).await??;
        let pending_generation = lock(&service.state).pending_start.map(|ticket| ticket.0);
        let other_scope = NameScope::new(service.clone());
        let mut other = other_scope.observe(NetworkNameOptions::default())?;
        starting_scope.request_close();
        assert_eq!(
            lock(&service.state).pending_start.map(|ticket| ticket.0),
            pending_generation
        );
        assert!(pending_generation.is_some());
        let _ = resume.send(());
        let returned = starting.join().map_err(|_| "observer thread panicked")??;
        assert_eq!(returned.snapshot()?.state(), NetworkNameState::Closed);
        current_ready(&other).await?;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        other.close().await?;
        tokio::time::timeout(WAIT, service.wait_cleanup()).await??;
        Ok(())
    }
}

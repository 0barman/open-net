//! Engine-scoped network observation ownership. Consumers only retain weak
//! service contexts and explicit, idempotently releasable demand leases.
use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};

use tokio::sync::{mpsc, oneshot, watch};

use super::inner_net_status_client::InnerNetStatusClient;
use super::network_status_snapshot::NetworkStatusSnapshot;
use crate::common::CommonEngine;
use crate::error::{ErrorKind, NetError};
use crate::net_status::{MonitorState, NetworkSnapshot};

/// A read-only handle to the network facts owned by one `OpenNet` engine.
/// Cloning or reading it never starts monitoring or extends the engine lifetime.
/// A context remains tied to its original engine after that engine is dropped.
#[derive(Clone)]
pub struct NetworkStatusContext {
    service: Weak<SharedNetworkService>,
    facts: watch::Receiver<NetworkSnapshot>,
    failure: watch::Receiver<Option<NetError>>,
    gate: watch::Receiver<NetworkStatusSnapshot>,
}
impl std::fmt::Debug for NetworkStatusContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NetworkStatusContext")
            .finish_non_exhaustive()
    }
}
impl NetworkStatusContext {
    /// Read the cached observation without starting or retrying monitoring.
    pub fn snapshot(&self) -> Result<NetworkSnapshot, NetError> {
        if let Some(error) = ::tokio::sync::watch::Receiver::borrow(&self.failure).clone() {
            return Err(error);
        }
        match self.service.upgrade() {
            Some(service) => service.inner.snapshot(),
            None => Ok(::tokio::sync::watch::Receiver::borrow(&self.facts).clone()),
        }
    }
    pub(crate) fn common_engine(&self) -> Result<Arc<CommonEngine>, NetError> {
        self.service
            .upgrade()
            .map(|service| service.engine.clone())
            .ok_or_else(|| NetError::from(ErrorKind::Closed))
    }
    pub(crate) fn is_closed(&self) -> bool {
        self.service.upgrade().map_or(true, |service| {
            match service.admission.lock() {
                Ok(state) => state.closed,
                Err(_) => {
                    crate::log_e!(crate::common::log::log_def::LogType::Engine; "network_service_state", "error", "admission_lock_poisoned");
                    true
                }
            }
        })
    }
    pub(crate) fn acquire(&self) -> Result<NetworkLease, NetError> {
        let service = self
            .service
            .upgrade()
            .ok_or_else(|| NetError::from(ErrorKind::Closed))?;
        // A recoverable Failed snapshot remains valid for explicit retry. A
        // permanent publication error cannot admit new, unusable demand.
        service.inner.snapshot()?;
        let id = {
            let mut state = service.admission.lock().map_err(NetError::from_poison)?;
            if state.closed {
                return Err(NetError::from(ErrorKind::Closed));
            }
            let id = state
                .next_id
                .checked_add(1)
                .ok_or_else(|| NetError::from(ErrorKind::Internal))?;
            service
                .commands
                .send(Command::Acquire(id))
                .map_err(|_| NetError::from(ErrorKind::RuntimeUnavailable))?;
            state.next_id = id;
            state.active.insert(id);
            id
        };
        Ok(NetworkLease {
            inner: Arc::new(LeaseState {
                id,
                service: Arc::downgrade(&service),
                released: Mutex::new(None),
                closed: service.closed.clone(),
            }),
        })
    }
    pub(crate) fn subscribe_facts(&self) -> watch::Receiver<NetworkSnapshot> {
        self.facts.clone()
    }
    pub(crate) fn subscribe_failure(&self) -> watch::Receiver<Option<NetError>> {
        self.failure.clone()
    }
    pub(crate) fn subscribe_gate(&self) -> watch::Receiver<NetworkStatusSnapshot> {
        self.gate.clone()
    }
}

#[derive(Clone)]
pub(crate) struct CleanupTicket {
    finished: watch::Receiver<Option<Result<(), NetError>>>,
}
impl CleanupTicket {
    fn pending() -> (watch::Sender<Option<Result<(), NetError>>>, Self) {
        let (sender, finished) = watch::channel(None);
        (sender, Self { finished })
    }
    pub(crate) fn is_finished(&self) -> bool {
        matches!(
            *::tokio::sync::watch::Receiver::borrow(&self.finished),
            Some(Ok(()))
        )
    }
    pub(crate) async fn wait(&self) -> Result<(), NetError> {
        let mut finished = self.finished.clone();
        loop {
            if let Some(result) = finished.borrow_and_update().clone() {
                return result;
            }
            finished
                .changed()
                .await
                .map_err(|_| NetError::from(ErrorKind::RuntimeUnavailable))?;
        }
    }
}

#[derive(Clone)]
pub(crate) struct NetworkLease {
    inner: Arc<LeaseState>,
}
struct LeaseState {
    id: u64,
    service: Weak<SharedNetworkService>,
    released: Mutex<Option<CleanupTicket>>,
    closed: CleanupTicket,
}
impl NetworkLease {
    pub(crate) fn ensure_started(&self) -> Result<(), NetError> {
        self.inner.ensure(None)
    }
    pub(crate) async fn wait_started(&self) -> Result<NetworkSnapshot, NetError> {
        let (sender, received) = oneshot::channel();
        self.inner.ensure(Some(sender))?;
        received
            .await
            .map_err(|_| NetError::from(ErrorKind::RuntimeUnavailable))?
    }
    pub(crate) fn release(&self) -> CleanupTicket {
        self.inner.release()
    }
}

impl LeaseState {
    fn ensure(
        &self,
        waiter: Option<oneshot::Sender<Result<NetworkSnapshot, NetError>>>,
    ) -> Result<(), NetError> {
        let released = self.released.lock().map_err(NetError::from_poison)?;
        if released.is_some() {
            return Err(NetError::from(ErrorKind::Closed));
        }
        let service = self
            .service
            .upgrade()
            .ok_or_else(|| NetError::from(ErrorKind::Closed))?;
        let state = service.admission.lock().map_err(NetError::from_poison)?;
        if state.closed || !state.active.contains(&self.id) {
            return Err(NetError::from(ErrorKind::Closed));
        }
        service
            .commands
            .send(Command::Ensure(self.id, waiter))
            .map_err(|_| NetError::from(ErrorKind::RuntimeUnavailable))
    }
    fn release(&self) -> CleanupTicket {
        let mut released = match self.released.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                crate::log_e!(crate::common::log::log_def::LogType::Engine; "network_lease_release", "error", "poisoned_lease_recovered");
                poisoned.into_inner()
            }
        };
        if let Some(ticket) = released.as_ref() {
            return ticket.clone();
        }
        let ticket = match self.service.upgrade() {
            Some(service) => {
                let mut admission = match service.admission.lock() {
                    Ok(state) => state,
                    Err(poisoned) => {
                        crate::log_e!(crate::common::log::log_def::LogType::Engine; "network_lease_release", "error", "admission_lock_poisoned_recovered");
                        poisoned.into_inner()
                    }
                };
                admission.active.remove(&self.id);
                if admission.closed {
                    service.closed.clone()
                } else {
                    let (sender, ticket) = CleanupTicket::pending();
                    if let Err(error) = service.commands.send(Command::Release(self.id, sender)) {
                        if let Command::Release(_, sender) = error.0 {
                            Completion::new(sender, &service.engine)
                                .finish(Err(NetError::from(ErrorKind::RuntimeUnavailable)));
                        }
                    }
                    ticket
                }
            }
            None => self.closed.clone(),
        };
        *released = Some(ticket.clone());
        ticket
    }
}
impl Drop for LeaseState {
    fn drop(&mut self) {
        self.release();
    }
}

struct Admission {
    closed: bool,
    next_id: u64,
    active: HashSet<u64>,
}
pub(crate) struct SharedNetworkService {
    engine: Arc<CommonEngine>,
    inner: Arc<InnerNetStatusClient>,
    admission: Mutex<Admission>,
    commands: mpsc::UnboundedSender<Command>,
    closed: CleanupTicket,
}
enum Command {
    Acquire(u64),
    Ensure(
        u64,
        Option<oneshot::Sender<Result<NetworkSnapshot, NetError>>>,
    ),
    Release(u64, watch::Sender<Option<Result<(), NetError>>>),
    Close,
}
impl SharedNetworkService {
    pub(crate) fn new(engine: Arc<CommonEngine>) -> Result<Arc<Self>, NetError> {
        let inner = Arc::new(InnerNetStatusClient::new(engine.clone())?);
        let (commands, receiver) = mpsc::unbounded_channel();
        let (closed_sender, closed) = CleanupTicket::pending();
        engine.runtime_handle().spawn(coordinate(
            inner.clone(),
            engine.clone(),
            receiver,
            closed_sender,
        ));
        Ok(Arc::new(Self {
            engine,
            inner,
            commands,
            admission: Mutex::new(Admission {
                closed: false,
                next_id: 0,
                active: HashSet::new(),
            }),
            closed,
        }))
    }
    pub(crate) fn context(self: &Arc<Self>) -> NetworkStatusContext {
        NetworkStatusContext {
            service: Arc::downgrade(self),
            facts: self.inner.subscribe_facts(),
            failure: self.inner.subscribe_failure(),
            gate: self.inner.subscribe(),
        }
    }
    pub(crate) fn request_close(&self) {
        let newly_closed = {
            let mut state = match self.admission.lock() {
                Ok(state) => state,
                Err(poisoned) => {
                    crate::log_e!(crate::common::log::log_def::LogType::Engine; "network_service_close", "error", "admission_lock_poisoned_recovered");
                    poisoned.into_inner()
                }
            };
            if state.closed {
                false
            } else {
                state.closed = true;
                state.active.clear();
                true
            }
        };
        if newly_closed {
            // Terminal facts become visible before returning. No admission lock
            // is held while the source wakes its internal SDK readers.
            self.inner.request_destroy();
            let _ = self.commands.send(Command::Close);
        }
    }
    #[cfg(test)]
    pub(crate) fn inner_for_test(&self) -> Arc<InnerNetStatusClient> {
        self.inner.clone()
    }
    #[cfg(test)]
    pub(crate) fn active_consumers_for_test(&self) -> Result<usize, NetError> {
        Ok(self
            .admission
            .lock()
            .map_err(NetError::from_poison)?
            .active
            .len())
    }
    pub(crate) async fn shutdown(&self) -> Result<(), NetError> {
        self.request_close();
        self.closed.wait().await
    }
}

impl Drop for SharedNetworkService {
    fn drop(&mut self) {
        self.request_close();
    }
}

type StartFuture = Pin<Box<dyn Future<Output = Result<NetworkSnapshot, NetError>> + Send>>;
/// This guard also reports task cancellation; dropping a sender is never treated
/// as proof that provider resources have completed retirement.
struct Completion {
    sender: Option<watch::Sender<Option<Result<(), NetError>>>>,
    engine: Arc<CommonEngine>,
}
impl Completion {
    fn new(
        sender: watch::Sender<Option<Result<(), NetError>>>,
        engine: &Arc<CommonEngine>,
    ) -> Self {
        Self {
            sender: Some(sender),
            engine: engine.clone(),
        }
    }
    fn finish(mut self, result: Result<(), NetError>) {
        if let Some(sender) = self.sender.take() {
            let engine = self.engine.clone();
            self.engine.runtime_handle().spawn_blocking(move || {
                // A Handle alone cannot keep accepted cleanup work alive.
                let _runtime_owner = engine;
                sender.send_replace(Some(result));
            });
        }
    }
}
impl Drop for Completion {
    fn drop(&mut self) {
        if let Some(sender) = self.sender.take() {
            let engine = self.engine.clone();
            self.engine.runtime_handle().spawn_blocking(move || {
                let _runtime_owner = engine;
                sender.send_replace(Some(Err(NetError::from(ErrorKind::RuntimeUnavailable))));
            });
        }
    }
}
fn notify_waiters(
    engine: &Arc<CommonEngine>,
    waiters: Vec<oneshot::Sender<Result<NetworkSnapshot, NetError>>>,
    result: Result<NetworkSnapshot, NetError>,
) {
    for waiter in waiters {
        let retained_engine = engine.clone();
        let result = result.clone();
        engine.runtime_handle().spawn_blocking(move || {
            let _runtime_owner = retained_engine;
            // Independent consumers must not share one user-waker stack.
            let _ = waiter.send(result);
        });
    }
}

fn retire(
    inner: &InnerNetStatusClient,
    engine: &Arc<CommonEngine>,
    permanent: bool,
    sender: watch::Sender<Option<Result<(), NetError>>>,
) {
    let (finished, error) = inner.request_stop(permanent);
    let completion = Completion::new(sender, engine);
    engine.runtime_handle().spawn(async move {
        let result = InnerNetStatusClient::wait_until_finished(finished).await;
        completion.finish(result.and_then(|()| error.map_or(Ok(()), Err)));
    });
}
async fn coordinate(
    inner: Arc<InnerNetStatusClient>,
    engine: Arc<CommonEngine>,
    mut commands: mpsc::UnboundedReceiver<Command>,
    closed: watch::Sender<Option<Result<(), NetError>>>,
) {
    let closed = Completion::new(closed, &engine);
    let mut active = HashSet::new();
    let mut start: Option<StartFuture> = None;
    let mut waiters = Vec::<oneshot::Sender<Result<NetworkSnapshot, NetError>>>::new();
    let mut retry_waiters = Vec::new();
    let mut retry_requested = false;
    let mut start_revision = 0;
    loop {
        tokio::select! {
            biased;
            command = commands.recv() => {
                match command {
                    Some(Command::Acquire(id)) => { active.insert(id); }
                    Some(Command::Ensure(id, waiter)) if active.contains(&id) => {
                        let snapshot = inner.snapshot();
                        let failed_during_start = start.is_some() && snapshot.as_ref().is_ok_and(|snapshot| {
                            snapshot.revision > start_revision && matches!(snapshot.state, MonitorState::Failed(_))
                        });
                        if failed_during_start {
                            retry_requested = true;
                            if let Some(waiter) = waiter { retry_waiters.push(waiter); }
                        } else {
                            if let Some(waiter) = waiter { waiters.push(waiter); }
                            if start.is_none() {
                                start_revision = snapshot.as_ref().map_or(0, |snapshot| snapshot.revision);
                                let inner = inner.clone();
                                start = Some(Box::pin(async move { inner.start().await }));
                            }
                        }
                    }
                    Some(Command::Ensure(_, waiter)) => {
                        if let Some(waiter) = waiter { notify_waiters(&engine, vec![waiter], Err(NetError::from(ErrorKind::Closed))); }
                    }
                    Some(Command::Release(id, sender)) => {
                        active.remove(&id);
                        if active.is_empty() {
                            drop(start.take());
                            retry_requested = false;
                            waiters.append(&mut retry_waiters);
                            retire(&inner, &engine, false, sender);
                            let result = inner.snapshot();
                            notify_waiters(&engine, std::mem::take(&mut waiters), result);
                        } else {
                            Completion::new(sender, &engine).finish(Ok(()));
                        }
                    }
                    Some(Command::Close) | None => {
                        drop(start.take());
                        waiters.append(&mut retry_waiters);
                        notify_waiters(&engine, std::mem::take(&mut waiters), Err(NetError::from(ErrorKind::Closed)));
                        let (finished, error) = inner.request_stop(true);
                        // No commands need service after the terminal admission
                        // transition. Only this final shutdown waits for resources.
                        let result = InnerNetStatusClient::wait_until_finished(finished).await;
                        closed.finish(result.and_then(|()| error.map_or(Ok(()), Err)));
                        return;
                    }
                }
            }
            result = async {
                match start.as_mut() {
                    Some(start) => start.await,
                    None => std::future::pending().await,
                }
            } => {
                start = None;
                notify_waiters(&engine, std::mem::take(&mut waiters), result);
                if retry_requested && !active.is_empty() {
                    retry_requested = false;
                    waiters = std::mem::take(&mut retry_waiters);
                    start_revision = inner.snapshot().map_or(0, |snapshot| snapshot.revision);
                    let inner = inner.clone();
                    start = Some(Box::pin(async move { inner.start().await }));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
    fn check(condition: bool, message: &str) -> TestResult {
        if condition {
            Ok(())
        } else {
            Err(std::io::Error::other(message).into())
        }
    }
    fn service() -> Result<Arc<SharedNetworkService>, NetError> {
        SharedNetworkService::new(Arc::new(CommonEngine::new(16, 16)?))
    }
    struct BlockingWake {
        entered: Mutex<Option<oneshot::Sender<()>>>,
        release: Mutex<std::sync::mpsc::Receiver<()>>,
    }
    impl std::task::Wake for BlockingWake {
        fn wake(self: Arc<Self>) {
            if let Ok(mut entered) = self.entered.lock() {
                if let Some(entered) = entered.take() {
                    let _ = entered.send(());
                }
            }
            if let Ok(release) = self.release.lock() {
                let _ = release.recv_timeout(std::time::Duration::from_secs(5));
            }
        }
    }
    #[tokio::test]
    async fn accepted_waiters_are_not_blocked_by_another_callers_waker() -> TestResult {
        let engine = Arc::new(CommonEngine::new(16, 16)?);
        let (entered, entered_wait) = oneshot::channel();
        let (release, released) = std::sync::mpsc::channel();
        let waker = std::task::Waker::from(Arc::new(BlockingWake {
            entered: Mutex::new(Some(entered)),
            release: Mutex::new(released),
        }));
        let (first_sender, mut first) = oneshot::channel();
        let (second_sender, second) = oneshot::channel();
        let first_pending = Pin::new(&mut first)
            .poll(&mut std::task::Context::from_waker(&waker))
            .is_pending();
        check(first_pending, "first waiter was already complete")?;
        notify_waiters(
            &engine,
            vec![first_sender, second_sender],
            Err(NetError::from(ErrorKind::Closed)),
        );
        tokio::time::timeout(std::time::Duration::from_secs(5), entered_wait).await??;
        let second_result = tokio::time::timeout(std::time::Duration::from_secs(2), second).await;
        let _ = release.send(());
        check(
            second_result??.is_err(),
            "second caller did not receive terminal result",
        )?;
        check(
            first.await?.is_err(),
            "first caller did not receive terminal result",
        )?;
        Ok(())
    }

    #[tokio::test]
    async fn accepted_cleanup_notification_keeps_runtime_until_delivery() -> TestResult {
        let engine = Arc::new(CommonEngine::new(16, 16)?);
        let (sender, ticket) = CleanupTicket::pending();
        let completion = Completion::new(sender, &engine);
        let runtime_owner = Arc::downgrade(&engine);
        drop(engine);
        check(
            runtime_owner.upgrade().is_some(),
            "accepted cleanup lost its runtime owner",
        )?;
        completion.finish(Ok(()));
        tokio::time::timeout(std::time::Duration::from_secs(5), ticket.wait()).await??;
        Ok(())
    }

    #[tokio::test]
    async fn snapshots_are_inert_and_leases_are_logical_and_idempotent() -> TestResult {
        let service = service()?;
        let context = service.context();
        check(
            matches!(context.snapshot()?.state, MonitorState::Stopped),
            "new source is not stopped",
        )?;
        let lease = context.acquire()?;
        let clone = lease.clone();
        check(
            service
                .admission
                .lock()
                .map_err(NetError::from_poison)?
                .active
                .len()
                == 1,
            "clone added demand",
        )?;
        lease.release().wait().await?;
        clone.release().wait().await?;
        check(
            service
                .admission
                .lock()
                .map_err(NetError::from_poison)?
                .active
                .is_empty(),
            "released demand leaked",
        )?;
        check(
            matches!(context.snapshot()?.state, MonitorState::Stopped),
            "acquisition started monitoring",
        )?;
        service.shutdown().await?;
        Ok(())
    }
    #[tokio::test]
    async fn closed_context_rejects_new_leases_and_preserves_terminal_cache() -> TestResult {
        let service = service()?;
        let context = service.context();
        service.request_close();
        check(context.acquire().is_err(), "closed context admitted demand")?;
        check(
            matches!(context.snapshot()?.state, MonitorState::Closed),
            "close was not synchronous",
        )?;
        service.shutdown().await?;
        drop(service);
        check(
            matches!(context.snapshot()?.state, MonitorState::Closed),
            "terminal cache was lost",
        )?;
        Ok(())
    }
    fn gated_service() -> Result<
        (
            Arc<SharedNetworkService>,
            mpsc::UnboundedReceiver<oneshot::Sender<()>>,
            Arc<std::sync::atomic::AtomicUsize>,
        ),
        NetError,
    > {
        let service = service()?;
        let (attempts, receiver) = mpsc::unbounded_channel();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let factory_calls = calls.clone();
        service
            .inner
            .set_monitor_factory_for_test(Arc::new(move || {
                factory_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let (release, ready) = oneshot::channel();
                let _ = attempts.send(release);
                Box::pin(async move {
                    let _ = ready.await;
                    Err(NetError::from(ErrorKind::Io))
                })
            }))?;
        Ok((service, receiver, calls))
    }
    async fn attempt(
        receiver: &mut mpsc::UnboundedReceiver<oneshot::Sender<()>>,
    ) -> Result<oneshot::Sender<()>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(
            tokio::time::timeout(std::time::Duration::from_secs(5), receiver.recv())
                .await?
                .ok_or("factory missing")?,
        )
    }
    #[tokio::test]
    async fn concurrent_demand_shares_pending_initialization_and_nonlast_release_is_isolated(
    ) -> TestResult {
        let (service, mut attempts, calls) = gated_service()?;
        let a = service.context().acquire()?;
        let b = service.context().acquire()?;
        a.ensure_started()?;
        b.ensure_started()?;
        let release = attempt(&mut attempts).await?;
        a.release().wait().await?;
        check(
            calls.load(std::sync::atomic::Ordering::SeqCst) == 1,
            "concurrent demand duplicated provider",
        )?;
        check(
            !release.is_closed(),
            "nonlast release cancelled shared startup",
        )?;
        let mut waiting = Box::pin(b.wait_started());
        check(
            futures::poll!(&mut waiting).is_pending(),
            "start waiter unexpectedly completed",
        )?;
        let _ = release.send(());
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), waiting).await?;
        check(
            matches!(result, Err(error) if error.kind() == ErrorKind::Io),
            "shared startup error lost",
        )?;
        service.shutdown().await?;
        Ok(())
    }
    #[tokio::test]
    async fn failed_generation_can_retry_with_existing_lease_and_live_fact_stream() -> TestResult {
        let (service, mut attempts, calls) = gated_service()?;
        let context = service.context();
        let lease = context.acquire()?;
        let mut facts = context.subscribe_facts();
        let first = lease.clone();
        let waiting = tokio::spawn(async move { first.wait_started().await });
        let _ = attempt(&mut attempts).await?.send(());
        check(waiting.await?.is_err(), "failure was hidden")?;
        check(
            matches!(context.snapshot()?.state, MonitorState::Failed(_)),
            "failure snapshot absent",
        )?;
        lease.ensure_started()?;
        let second = attempt(&mut attempts).await?;
        check(
            calls.load(std::sync::atomic::Ordering::SeqCst) == 2,
            "existing demand could not retry",
        )?;
        check(
            facts.changed().await.is_ok(),
            "recoverable failure closed stream",
        )?;
        lease.release().wait().await?;
        check(second.is_closed(), "last release did not cancel retry")?;
        service.shutdown().await?;
        Ok(())
    }
    #[tokio::test]
    async fn close_during_pending_start_finishes_waiters_and_prevents_revival() -> TestResult {
        let (service, mut attempts, _) = gated_service()?;
        let context = service.context();
        let lease = context.acquire()?;
        let waiter = lease.clone();
        let waiting = tokio::spawn(async move { waiter.wait_started().await });
        let _attempt = attempt(&mut attempts).await?;
        service.request_close();
        check(
            matches!(context.snapshot()?.state, MonitorState::Closed),
            "close did not publish terminal",
        )?;
        check(
            matches!(waiting.await?, Err(error) if error.kind() == ErrorKind::Closed),
            "pending start survived close",
        )?;
        lease.release().wait().await?;
        check(context.acquire().is_err(), "closed service revived")?;
        service.shutdown().await?;
        Ok(())
    }
    #[tokio::test]
    async fn service_drop_does_not_require_external_context_or_lease_drop() -> TestResult {
        let (service, mut attempts, _) = gated_service()?;
        let context = service.context();
        let lease = context.acquire()?;
        lease.ensure_started()?;
        let _attempt = attempt(&mut attempts).await?;
        drop(service);
        check(
            matches!(context.snapshot()?.state, MonitorState::Closed),
            "external context kept engine alive",
        )?;
        lease.release().wait().await?;
        check(
            lease.ensure_started().is_err(),
            "released context restarted engine",
        )?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "shared_regression_tests.rs"]
mod regression_tests;

use super::registration::Registration;
use super::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Semaphore;

static LAST_ID: AtomicU64 = AtomicU64::new(0);

pub(super) fn allocate_id() -> Result<SubscriptionId> {
    next_id(&LAST_ID)
}

pub(super) fn next_id(counter: &AtomicU64) -> Result<SubscriptionId> {
    counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            value.checked_add(1)
        })
        .map(|previous| SubscriptionId(previous + 1))
        .map_err(|_| NetError::from(ErrorKind::ResourceExhausted))
}

pub(crate) struct StateSource<T> {
    pub(super) core: Arc<SourceCore<T>>,
}
pub(crate) struct StatePublisher<T> {
    owner: Arc<PublisherOwner<T>>,
}
struct PublisherOwner<T> {
    core: Arc<SourceCore<T>>,
}
pub(super) struct SourceCore<T> {
    pub(super) state: Arc<Mutex<SourceState<T>>>,
    executor: Arc<dyn CallbackExecutor>,
    slots: Arc<Semaphore>,
    #[cfg(test)]
    reserve_next: std::sync::atomic::AtomicUsize,
}
pub(super) struct SourceState<T> {
    pub(super) current: Arc<T>,
    pub(super) revision: u64,
    pub(super) closed: bool,
    pub(super) failure: Option<NetError>,
    registrations: HashMap<SubscriptionId, Weak<Registration<T>>>,
}
impl<T> Clone for StateSource<T> {
    fn clone(&self) -> Self {
        Self {
            core: self.core.clone(),
        }
    }
}
impl<T> Clone for StatePublisher<T> {
    fn clone(&self) -> Self {
        Self {
            owner: self.owner.clone(),
        }
    }
}
impl<T> StateSource<T> {
    pub(crate) fn new(
        initial: T,
        executor: Arc<dyn CallbackExecutor>,
        max_subscriptions: usize,
    ) -> Result<(StatePublisher<T>, Self)> {
        Self::new_arc(Arc::new(initial), executor, max_subscriptions)
    }
    pub(crate) fn new_arc(
        initial: Arc<T>,
        executor: Arc<dyn CallbackExecutor>,
        max_subscriptions: usize,
    ) -> Result<(StatePublisher<T>, Self)> {
        if max_subscriptions == 0 || max_subscriptions > Semaphore::MAX_PERMITS {
            return Err(NetError::config(
                "subscription.max_subscriptions",
                "must be a positive supported capacity",
            ));
        }
        Self::new_arc_with_quota(
            initial,
            executor,
            Arc::new(Semaphore::new(max_subscriptions)),
        )
    }
    /// Shares the owner's subscription budget across independent sources.
    /// Zero available permits means exhausted, not an invalid configuration.
    pub(crate) fn new_arc_with_quota(
        initial: Arc<T>,
        executor: Arc<dyn CallbackExecutor>,
        quota: Arc<Semaphore>,
    ) -> Result<(StatePublisher<T>, Self)> {
        let core = Arc::new(SourceCore {
            state: Arc::new(Mutex::new(SourceState {
                current: initial,
                revision: 0,
                closed: false,
                failure: None,
                registrations: HashMap::new(),
            })),
            executor,
            slots: quota,
            #[cfg(test)]
            reserve_next: std::sync::atomic::AtomicUsize::new(0),
        });
        Ok((
            StatePublisher {
                owner: Arc::new(PublisherOwner { core: core.clone() }),
            },
            Self { core },
        ))
    }
    pub(crate) fn subscribe(&self) -> Result<StateReceiver<T>> {
        self.subscribe_with_cache(false)
    }
    /// Reads committed state while user notification remains deferred. The initial
    /// snapshot and receiver cursor retain the ordinary subscription contract.
    pub(crate) fn subscribe_committed(&self) -> Result<StateReceiver<T>> {
        self.subscribe_with_cache(true)
    }
    fn subscribe_with_cache(&self, read_committed: bool) -> Result<StateReceiver<T>> {
        let permit = self
            .core
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| NetError::from(ErrorKind::SubscriptionLimitReached))?;
        let id = allocate_id()?;
        let registration = {
            let mut source = lock(&self.core.state);
            source
                .registrations
                .try_reserve(1)
                .map_err(|error| NetError::with_source(ErrorKind::ResourceExhausted, error))?;
            let registration = Arc::new(Registration::new(
                id,
                &self.core,
                self.core.executor.clone(),
                source.current.clone(),
                source.revision,
                source.closed,
                source.failure.clone(),
                permit,
                read_committed,
            ));
            source
                .registrations
                .insert(id, Arc::downgrade(&registration));
            registration
        };
        Ok(StateReceiver {
            registration,
            owns_subscription: true,
        })
    }
}
impl<T: Clone> StateSource<T> {
    /// Reports source infrastructure failure independently of any error stored
    /// inside the snapshot. An ordinary final snapshot remains queryable.
    pub(crate) fn current_result(&self) -> Result<T> {
        let value = {
            let source = self.core.state.lock().map_err(NetError::from_poison)?;
            if let Some(error) = &source.failure {
                return Err(error.clone());
            }
            source.current.clone()
        };
        Ok((*value).clone())
    }
    pub(crate) fn current(&self) -> T {
        let value = lock(&self.core.state).current.clone();
        (*value).clone()
    }
}
/// An atomically committed state update whose user-facing effects are deferred.
/// Dispatch only after releasing any caller-owned lifecycle or monitor locks.
#[must_use]
pub(crate) struct StatePublication<T> {
    proposed: Option<Arc<T>>,
    retired: Option<Arc<T>>,
    retained_failure: Option<NetError>,
    retired_failure: Option<NetError>,
    current: Option<Arc<T>>,
    revision: u64,
    closed: bool,
    failure: Option<NetError>,
    registrations: Vec<Arc<Registration<T>>>,
    terminal_registrations: Option<HashMap<SubscriptionId, Weak<Registration<T>>>>,
    result: Result<()>,
}
impl<T> StatePublication<T> {
    pub(crate) fn result(&self) -> Result<()> {
        self.result.clone()
    }
    pub(crate) fn dispatch(self) -> Result<()> {
        let Self {
            proposed,
            retired,
            retained_failure,
            retired_failure,
            current,
            revision,
            closed,
            failure,
            registrations,
            terminal_registrations,
            result,
        } = self;
        drop((proposed, retired, retained_failure, retired_failure));
        if let Some(current) = current {
            for registration in registrations {
                registration.update(current.clone(), revision, closed, failure.clone());
            }
            if let Some(registrations) = terminal_registrations {
                for registration in registrations
                    .into_values()
                    .filter_map(|weak| weak.upgrade())
                {
                    registration.update(current.clone(), revision, closed, failure.clone());
                }
            }
        }
        result
    }
}
impl<T> StatePublisher<T> {
    #[cfg(test)]
    pub(crate) fn exhaust_revision_for_test(&self) {
        lock(&self.owner.core.state).revision = u64::MAX;
    }
    pub(crate) fn publish(&self, value: T) -> Result<()> {
        self.prepare_arc(Arc::new(value)).dispatch()
    }
    pub(crate) fn finish(&self, final_value: T) -> Result<()> {
        self.prepare_finish_arc(Arc::new(final_value)).dispatch()
    }
    pub(crate) fn fail(&self, error: NetError) -> Result<()> {
        self.prepare_failure(error).dispatch()
    }
    pub(crate) fn prepare_failure(&self, error: NetError) -> StatePublication<T> {
        self.owner.core.prepare(None, true, Some(error))
    }
    pub(crate) fn prepare_arc(&self, value: Arc<T>) -> StatePublication<T> {
        self.owner.core.prepare(Some(value), false, None)
    }
    pub(crate) fn prepare_finish_arc(&self, value: Arc<T>) -> StatePublication<T> {
        self.owner.core.prepare(Some(value), true, None)
    }
}
impl<T> Drop for PublisherOwner<T> {
    fn drop(&mut self) {
        let _ = self.core.prepare(None, true, None).dispatch();
    }
}
impl<T> SourceCore<T> {
    pub(super) fn remove(&self, id: SubscriptionId) {
        lock(&self.state).registrations.remove(&id);
    }
    fn prepare(
        &self,
        value: Option<Arc<T>>,
        closing: bool,
        failure: Option<NetError>,
    ) -> StatePublication<T> {
        // Rejected input is also retained until dispatch. Returning a bare error here could
        // otherwise run the last user destructor while a caller-owned lock is still held.
        let mut publication = StatePublication {
            proposed: value,
            retired: None,
            retained_failure: failure,
            retired_failure: None,
            current: None,
            revision: 0,
            closed: false,
            failure: None,
            registrations: Vec::new(),
            terminal_registrations: None,
            result: Ok(()),
        };
        {
            let mut source = lock(&self.state);
            if source.closed {
                publication.result = Err(NetError::from(ErrorKind::Closed));
                return publication;
            }
            let next = if publication.proposed.is_some() {
                source.revision.checked_add(1)
            } else {
                Some(source.revision)
            };
            let overflow = if next.is_none() {
                Some(NetError::from(ErrorKind::ResourceExhausted))
            } else {
                None
            };
            if !closing && overflow.is_none() {
                #[cfg(test)]
                let forced = self.reserve_next.swap(0, Ordering::AcqRel);
                #[cfg(test)]
                let reserve = if forced == 0 {
                    source.registrations.len()
                } else {
                    forced
                };
                #[cfg(not(test))]
                let reserve = source.registrations.len();
                if let Err(error) = publication.registrations.try_reserve(reserve) {
                    publication.result =
                        Err(NetError::with_source(ErrorKind::ResourceExhausted, error));
                    return publication;
                }
            }
            if let Some(next) = next {
                if let Some(value) = &publication.proposed {
                    publication.retired =
                        Some(std::mem::replace(&mut source.current, value.clone()));
                }
                source.revision = next;
            }
            source.closed = closing || overflow.is_some();
            publication.retired_failure = std::mem::replace(
                &mut source.failure,
                publication
                    .retained_failure
                    .clone()
                    .or_else(|| overflow.clone()),
            );
            if source.closed {
                // Terminal delivery must work even when allocating a recipient list fails.
                // Move the existing registry and upgrade its weak entries only after dispatch
                // has left all source and caller-owned locks.
                publication.terminal_registrations =
                    Some(std::mem::take(&mut source.registrations));
            } else {
                publication
                    .registrations
                    .extend(source.registrations.values().filter_map(Weak::upgrade));
            }
            publication.current = Some(source.current.clone());
            publication.revision = source.revision;
            publication.closed = source.closed;
            publication.failure = source.failure.clone();
            publication.result = overflow.map_or(Ok(()), Err);
        }
        publication
    }
}

#[cfg(test)]
#[path = "source_shutdown_tests.rs"]
mod shutdown_tests;

#[cfg(test)]
#[path = "source_committed_tests.rs"]
mod committed_tests;
#[cfg(test)]
#[path = "source_shared_tests.rs"]
mod shared_tests;

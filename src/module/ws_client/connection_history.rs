//! Bounded ordinary connection observations. Reliable journal reservations and
//! the session's final result are owned separately by ConnectionSession.

use crate::error::{ErrorKind, NetError};
use crate::subscription::{
    event_channel, CallbackExecutor, EventOverflow, EventPublication, EventPublisher,
    EventQueueLimit, EventReceiver, EventSeed,
};
use crate::ws::{ClientId, ConnectionEvent, ConnectionEventKind, EventOptions, SessionId};
use crate::Result;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use tokio::sync::Semaphore;

struct HistoryState {
    scope: Option<(ClientId, SessionId)>,
    last_sequence: u64,
    entries: VecDeque<(ConnectionEvent, usize)>,
    bytes: usize,
    closed: bool,
    failure: Option<NetError>,
    publishers: Vec<EventPublisher<ConnectionEvent>>,
}

pub(super) struct ConnectionHistory {
    options: EventOptions,
    executor: Arc<dyn CallbackExecutor>,
    quota: Arc<Semaphore>,
    state: Mutex<HistoryState>,
    #[cfg(test)]
    fail_record_allocation: std::sync::atomic::AtomicBool,
}

/// Prepared queue mutations are already ordered. Only retirement, notification,
/// and callback scheduling happen in dispatch, after all caller locks are gone.
/// Every prepared result, including errors, must be dispatched after releasing
/// all caller locks. Dropping it cannot finish observers with the recorded
/// failure, and automatic dispatch in Drop would run under those locks.
#[must_use = "dispatch every result after releasing all caller locks"]
pub(super) struct HistoryPublication {
    input: Option<ConnectionEvent>,
    retained_failure: Option<NetError>,
    retired_entries: Vec<(ConnectionEvent, usize)>,
    retired_history: Option<VecDeque<(ConnectionEvent, usize)>>,
    retired_publishers: Vec<EventPublisher<ConnectionEvent>>,
    publications: Vec<EventPublication<ConnectionEvent>>,
    terminal_publishers: Option<Vec<EventPublisher<ConnectionEvent>>>,
    terminal_failure: Option<NetError>,
    result: Result<()>,
}

impl HistoryPublication {
    fn new(input: Option<ConnectionEvent>) -> Self {
        Self {
            input,
            retained_failure: None,
            retired_entries: Vec::new(),
            retired_history: None,
            retired_publishers: Vec::new(),
            publications: Vec::new(),
            terminal_publishers: None,
            terminal_failure: None,
            result: Ok(()),
        }
    }

    pub(super) fn result(&self) -> Result<()> {
        self.result.clone()
    }

    pub(super) fn dispatch(self) -> Result<()> {
        let Self {
            input,
            retained_failure,
            retired_entries,
            retired_history,
            retired_publishers,
            publications,
            terminal_publishers,
            terminal_failure,
            result,
        } = self;
        drop((
            input,
            retained_failure,
            retired_entries,
            retired_history,
            retired_publishers,
        ));
        for publication in publications {
            // An observer failure must never become a network/session failure.
            if let Err(error) = publication.dispatch() {
                log_observer_failure(&error);
            }
        }
        if let Some(publishers) = terminal_publishers {
            for publisher in publishers {
                let publication = match &terminal_failure {
                    Some(error) => publisher.prepare_fail(error.clone()),
                    None => publisher.prepare_finish(),
                };
                if let Err(error) = publication.dispatch() {
                    log_observer_failure(&error);
                }
            }
        }
        result
    }
}

impl ConnectionHistory {
    pub(super) fn new(
        options: EventOptions,
        executor: Arc<dyn CallbackExecutor>,
        quota: Arc<Semaphore>,
    ) -> Result<Self> {
        options.validate()?;
        let mut entries = VecDeque::new();
        entries
            .try_reserve_exact(options.max_events)
            .map_err(allocation_error)?;
        Ok(Self {
            options,
            executor,
            quota,
            state: Mutex::new(HistoryState {
                scope: None,
                last_sequence: 0,
                entries,
                bytes: 0,
                closed: false,
                failure: None,
                publishers: Vec::new(),
            }),
            #[cfg(test)]
            fail_record_allocation: std::sync::atomic::AtomicBool::new(false),
        })
    }

    pub(super) fn subscribe(&self) -> Result<EventReceiver<ConnectionEvent>> {
        self.subscribe_with_seed_hook(|| Ok(()))
    }

    fn subscribe_with_seed_hook(
        &self,
        after_seed: impl FnOnce() -> Result<()>,
    ) -> Result<EventReceiver<ConnectionEvent>> {
        // A new channel is not visible to any caller before its history seed and
        // membership in the live fanout are committed under the same lock.
        let permit = Arc::clone(&self.quota)
            .try_acquire_owned()
            .map_err(|_| NetError::from(ErrorKind::SubscriptionLimitReached))?;
        let (publisher, receiver) = event_channel(
            EventQueueLimit {
                max_items: self.options.max_events,
                max_bytes: self.options.max_bytes,
            },
            EventOverflow::DropOldest,
            Arc::clone(&self.executor),
            Some(permit),
        )?;
        let mut publisher = Some(publisher);
        let mut publication = HistoryPublication::new(None);
        let result: Result<()> = (|| {
            let mut state = self.state.lock().map_err(NetError::from_poison)?;
            publication
                .retired_publishers
                .try_reserve_exact(state.publishers.len())
                .map_err(allocation_error)?;
            prune(&mut state.publishers, &mut publication.retired_publishers);
            if !state.closed {
                state.publishers.try_reserve(1).map_err(allocation_error)?;
            }
            publication
                .publications
                .try_reserve_exact(1)
                .map_err(allocation_error)?;
            let mut entries = VecDeque::new();
            entries
                .try_reserve_exact(state.entries.len())
                .map_err(allocation_error)?;
            entries.extend(state.entries.iter().cloned());
            let lagged = match state.entries.front() {
                Some((event, _)) => event
                    .sequence
                    .checked_sub(1)
                    .ok_or_else(|| NetError::from(ErrorKind::Internal))?,
                None => state.last_sequence,
            };
            let seed = EventSeed {
                entries,
                lagged,
                source_closed: state.closed,
                failure: state.failure.clone(),
            };
            let producer = publisher
                .as_ref()
                .ok_or_else(|| NetError::from(ErrorKind::Internal))?;
            let prepared = producer.prepare_seed(seed);
            let result = prepared.result();
            publication.publications.push(prepared);
            result?;
            after_seed()?;
            if !state.closed {
                state.publishers.push(
                    publisher
                        .take()
                        .ok_or_else(|| NetError::from(ErrorKind::Internal))?,
                );
            }
            Ok(())
        })();
        // Drop a rejected channel only after the history guard has been released.
        publication.dispatch()?;
        result?;
        Ok(receiver)
    }

    pub(super) fn prepare_record(&self, event: ConnectionEvent) -> HistoryPublication {
        let mut publication = HistoryPublication::new(Some(event));
        let result = (|| {
            let mut state = self.state.lock().map_err(NetError::from_poison)?;
            if state.closed {
                return Err(NetError::from(ErrorKind::Closed));
            }
            let event = publication
                .input
                .as_ref()
                .ok_or_else(|| NetError::from(ErrorKind::Internal))?;
            let scope = (event.client_id, event.session_id);
            if state.last_sequence.checked_add(1) != Some(event.sequence)
                || state.scope.is_some_and(|expected| expected != scope)
            {
                return Err(NetError::from(ErrorKind::InvalidInput));
            }
            let size = match event.measured_size() {
                Ok(size) => size,
                // An invalid producer input is rejected without changing its legal cursor.
                Err(error) if error.kind() == ErrorKind::ItemTooLarge => return Err(error),
                Err(error) => {
                    fail_history(&mut state, &mut publication, error.clone());
                    return Err(error);
                }
            };
            let sequence = event.sequence;
            let closed = matches!(event.kind, ConnectionEventKind::Closed { .. });
            let oversized = size > self.options.max_bytes;
            let preparation = self.reserve_record(&mut state, size, oversized, &mut publication);
            if let Err(error) = preparation {
                fail_history(&mut state, &mut publication, error.clone());
                return Err(error);
            }
            prune(&mut state.publishers, &mut publication.retired_publishers);
            if oversized {
                // Retain a contiguous suffix: an unretainable fact removes every
                // preceding entry and is itself represented by the missing prefix.
                publication.retired_history = Some(std::mem::take(&mut state.entries));
                state.bytes = 0;
            } else {
                while state.entries.len() >= self.options.max_events
                    || state.bytes > self.options.max_bytes - size
                {
                    if let Some(entry) = state.entries.pop_front() {
                        state.bytes -= entry.1;
                        publication.retired_entries.push(entry);
                    } else {
                        let error = NetError::from(ErrorKind::Internal);
                        fail_history(&mut state, &mut publication, error.clone());
                        return Err(error);
                    }
                }
                let event = publication
                    .input
                    .take()
                    .ok_or_else(|| NetError::from(ErrorKind::Internal))?;
                state.entries.push_back((event, size));
                state.bytes += size;
            }
            state.last_sequence = sequence;
            state.scope = Some(scope);
            state.closed = closed;
            for publisher in &state.publishers {
                let prepared = if oversized {
                    publisher.prepare_skip(1)
                } else {
                    let event = state
                        .entries
                        .back()
                        .ok_or_else(|| NetError::from(ErrorKind::Internal))?;
                    publisher.prepare_try_publish(event.0.clone(), size)
                };
                let observer_failure = prepared.result().err();
                publication.publications.push(prepared);
                if let Some(error) = observer_failure {
                    // The retained input/retired values of the rejected publication
                    // stay alive until dispatch, even when this observer is revoked.
                    publication.publications.push(publisher.prepare_fail(error));
                } else if closed {
                    publication.publications.push(publisher.prepare_finish());
                }
            }
            if closed {
                publication.terminal_publishers = Some(std::mem::take(&mut state.publishers));
            }
            Ok(())
        })();
        publication.result = result;
        publication
    }

    /// Explicit observation-source failure is a successful publication operation;
    /// receivers drain the retained prefix, observe Failed(error), and finish.
    pub(super) fn prepare_fail(&self, error: NetError) -> HistoryPublication {
        let mut publication = HistoryPublication::new(None);
        publication.retained_failure = Some(error);
        let result = (|| {
            let mut state = self.state.lock().map_err(NetError::from_poison)?;
            if !state.closed {
                let error = publication
                    .retained_failure
                    .as_ref()
                    .ok_or_else(|| NetError::from(ErrorKind::Internal))?
                    .clone();
                fail_history(&mut state, &mut publication, error);
            }
            Ok(())
        })();
        publication.result = result;
        publication
    }

    fn reserve_record(
        &self,
        state: &mut HistoryState,
        size: usize,
        oversized: bool,
        publication: &mut HistoryPublication,
    ) -> Result<()> {
        #[cfg(test)]
        if self
            .fail_record_allocation
            .swap(false, std::sync::atomic::Ordering::AcqRel)
        {
            return Err(NetError::from(ErrorKind::ResourceExhausted));
        }
        let notifications = state
            .publishers
            .len()
            .checked_mul(2)
            .ok_or_else(|| NetError::from(ErrorKind::ResourceExhausted))?;
        publication
            .publications
            .try_reserve_exact(notifications)
            .map_err(allocation_error)?;
        publication
            .retired_publishers
            .try_reserve_exact(state.publishers.len())
            .map_err(allocation_error)?;
        if !oversized {
            // An earlier oversized record may have moved the complete old buffer
            // into its retirement record. Rebuild its bounded capacity fallibly.
            if state.entries.capacity() < self.options.max_events {
                state
                    .entries
                    .try_reserve_exact(self.options.max_events - state.entries.len())
                    .map_err(allocation_error)?;
            }
            let mut bytes = state.bytes;
            let mut count = 0usize;
            for (_, retained_bytes) in &state.entries {
                if state.entries.len() - count < self.options.max_events
                    && bytes <= self.options.max_bytes - size
                {
                    break;
                }
                bytes -= retained_bytes;
                count += 1;
            }
            publication
                .retired_entries
                .try_reserve_exact(count)
                .map_err(allocation_error)?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn can_lock_for_test(&self) -> bool {
        self.state.try_lock().is_ok()
    }

    #[cfg(test)]
    pub(super) fn subscribe_with_seed_hook_for_test(
        &self,
        after_seed: impl FnOnce() -> Result<()>,
    ) -> Result<EventReceiver<ConnectionEvent>> {
        self.subscribe_with_seed_hook(after_seed)
    }

    #[cfg(test)]
    pub(super) fn fail_next_record_allocation_for_test(&self) {
        self.fail_record_allocation
            .store(true, std::sync::atomic::Ordering::Release);
    }
}

fn fail_history(state: &mut HistoryState, publication: &mut HistoryPublication, error: NetError) {
    state.closed = true;
    state.failure = Some(error.clone());
    publication.terminal_failure = Some(error);
    // Taking this existing vector makes terminal delivery independent of any
    // allocation needed by ordinary fanout. Dispatch will fail observers unlocked.
    publication.terminal_publishers = Some(std::mem::take(&mut state.publishers));
}

fn prune(
    publishers: &mut Vec<EventPublisher<ConnectionEvent>>,
    retired: &mut Vec<EventPublisher<ConnectionEvent>>,
) {
    let mut index = 0;
    while index < publishers.len() {
        if publishers[index].is_active() {
            index += 1;
        } else {
            retired.push(publishers.remove(index));
        }
    }
}

fn allocation_error(error: std::collections::TryReserveError) -> NetError {
    NetError::with_source(ErrorKind::ResourceExhausted, error)
}
fn log_observer_failure(error: &NetError) {
    crate::log_e!(crate::common::log::log_def::LogType::WSC;
        "connection_history_observer", "kind", format!("{:?}", error.kind()));
}

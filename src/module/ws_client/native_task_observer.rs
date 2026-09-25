//! Admission-time reservations for native task observations.
//!
//! The existing event receiver owns delivery and callback execution. This layer
//! reserves every terminal delivery before operation admission and shares the
//! payload charge across recipients.

use super::listener_executor::ListenerExecutor;
use crate::common::log::log_def::LogType;
use crate::error::{ErrorKind, ErrorStage};
use crate::subscription::{
    event_channel, EventOverflow, EventPublisher, EventQueueLimit, EventResources,
};
use crate::ws::{
    DispatchLimits, MessageLane, SubscriptionId, TaskEvent, TaskEventOptions, TaskEvents,
};
use crate::{NetError, Result};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::Instant;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tokio_util::sync::CancellationToken;

pub(crate) struct NativeTaskObserver {
    state: Mutex<ObserverState>,
    slots: Arc<Semaphore>,
    global: Arc<Capacity>,
    executor: Arc<ListenerExecutor>,
    closed: CancellationToken,
}

struct ObserverState {
    closed: bool,
    registrations: BTreeMap<SubscriptionId, Arc<Registration>>,
}

struct Capacity {
    items: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    max_items: usize,
    max_bytes: usize,
}

struct Registration {
    publisher: EventPublisher<TaskEvent>,
    ordinary: Capacity,
    urgent: Option<Capacity>,
    state: Mutex<RegistrationState>,
}

struct RegistrationState {
    closed: bool,
    next_id: u64,
    reservations: BTreeMap<u64, Weak<Mutex<Option<DeliveryCapacity>>>>,
}

struct DeliveryCapacity {
    item: OwnedSemaphorePermit,
    bytes: OwnedSemaphorePermit,
    global_item: OwnedSemaphorePermit,
    shared_bytes: Arc<OwnedSemaphorePermit>,
}

/// A fixed, ordered set of recipients captured before waiting for admission.
pub(crate) struct TaskObserverSnapshot {
    registrations: Vec<Arc<Registration>>,
    global: Arc<Capacity>,
    closed: CancellationToken,
}

/// Kept only by an unfinished operation; consuming publication is at most once.
pub(crate) struct TaskEventReservation {
    deliveries: Vec<ReservedDelivery>,
    payload_bytes: usize,
    lane: MessageLane,
}

struct ReservedDelivery {
    registration: Arc<Registration>,
    id: u64,
    capacity: Arc<Mutex<Option<DeliveryCapacity>>>,
}

impl NativeTaskObserver {
    /// Reuses the client's task ListenerExecutor; never creates a thread pool.
    pub(crate) fn new(
        executor: Arc<ListenerExecutor>,
        limits: &DispatchLimits,
    ) -> Result<Arc<Self>> {
        limits.validate()?;
        Ok(Arc::new(Self {
            state: Mutex::new(ObserverState {
                closed: false,
                registrations: BTreeMap::new(),
            }),
            slots: Arc::new(Semaphore::new(limits.task_subscriptions)),
            global: Arc::new(Capacity::new(
                limits.task_events.max_items,
                limits.task_events.max_bytes,
            )),
            executor,
            closed: CancellationToken::new(),
        }))
    }

    /// Isolate recipients per session while sharing the client's outstanding
    /// delivery and payload allowances, including unread older-session events.
    pub(crate) fn for_session(&self) -> Result<Arc<Self>> {
        if self.closed.is_cancelled() {
            return Err(closed_error());
        }
        Ok(Arc::new(Self {
            state: Mutex::new(ObserverState {
                closed: false,
                registrations: BTreeMap::new(),
            }),
            slots: self.slots.clone(),
            global: self.global.clone(),
            executor: self.executor.clone(),
            closed: CancellationToken::new(),
        }))
    }

    pub(crate) fn subscribe(&self, options: TaskEventOptions) -> Result<TaskEvents> {
        options.validate()?;
        self.prune_detached();
        if self.closed.is_cancelled() {
            return Err(closed_error());
        }
        let slot = self.slots.clone().try_acquire_owned().map_err(|_| {
            NetError::from(ErrorKind::SubscriptionLimitReached).with_stage(ErrorStage::Dispatch)
        })?;
        let (publisher, receiver) = event_channel(
            EventQueueLimit {
                max_items: options.max_tasks,
                max_bytes: options.max_payload_bytes,
            },
            EventOverflow::Wait,
            self.executor.clone(),
            Some(slot),
        )?;
        let (ordinary, urgent) = capacity_lanes(&options)?;
        let registration = Arc::new(Registration {
            publisher,
            ordinary,
            urgent,
            state: Mutex::new(RegistrationState {
                closed: false,
                next_id: 0,
                reservations: BTreeMap::new(),
            }),
        });
        let detached = Arc::downgrade(&registration);
        registration
            .publisher
            .on_receiver_closed(Arc::new(move || {
                if let Some(registration) = detached.upgrade() {
                    registration.release_detached();
                }
            }))?;
        let mut state = lock(&self.state);
        if state.closed {
            drop(state);
            return Err(closed_error());
        }
        state.registrations.insert(receiver.id(), registration);
        Ok(receiver)
    }

    pub(crate) fn snapshot(&self) -> Result<TaskObserverSnapshot> {
        self.prune_detached();
        let state = lock(&self.state);
        if state.closed {
            return Err(closed_error());
        }
        Ok(TaskObserverSnapshot {
            registrations: state.registrations.values().cloned().collect(),
            global: self.global.clone(),
            closed: self.closed.clone(),
        })
    }

    /// Freeze new admission; previously reserved terminals still drain to EOF.
    pub(crate) fn close(&self) {
        let registrations = {
            let mut state = lock(&self.state);
            state.closed = true;
            std::mem::take(&mut state.registrations)
        };
        self.closed.cancel();
        for (_, registration) in registrations {
            registration.close();
        }
    }

    fn prune_detached(&self) {
        let retired = {
            let mut state = lock(&self.state);
            let ids: Vec<_> = state
                .registrations
                .iter()
                .filter_map(|(id, registration)| {
                    (!registration.publisher.is_active()).then_some(*id)
                })
                .collect();
            ids.into_iter()
                .filter_map(|id| state.registrations.remove(&id))
                .collect::<Vec<_>>()
        };
        for registration in retired {
            registration.release_detached();
        }
    }
}

impl Drop for NativeTaskObserver {
    fn drop(&mut self) {
        self.close();
    }
}

impl TaskObserverSnapshot {
    pub(crate) fn try_reserve(
        &self,
        payload_bytes: usize,
        lane: MessageLane,
    ) -> Result<TaskEventReservation> {
        loop {
            let registrations = self.active();
            let result = self.try_reserve_active(&registrations, payload_bytes, lane);
            if result.is_err()
                && registrations
                    .iter()
                    .any(|registration| !registration.publisher.is_active())
                && !self.closed.is_cancelled()
            {
                // A detached recipient must not keep the frozen fanout larger
                // than its live set or make an otherwise valid admission fail.
                continue;
            }
            return result;
        }
    }

    fn try_reserve_active(
        &self,
        registrations: &[Arc<Registration>],
        payload_bytes: usize,
        lane: MessageLane,
    ) -> Result<TaskEventReservation> {
        if self.closed.is_cancelled() {
            return Err(closed_error());
        }
        let mut reservation = TaskEventReservation {
            deliveries: Vec::new(),
            payload_bytes,
            lane,
        };
        if registrations.is_empty() {
            return Ok(reservation);
        }
        let count = self.recipient_count(registrations.len())?;
        let bytes = self.global.checked_bytes(payload_bytes)?;
        for registration in registrations {
            registration.lane(lane).checked_bytes(payload_bytes)?;
        }
        let shared = Arc::new(
            self.global
                .bytes
                .clone()
                .try_acquire_many_owned(bytes)
                .map_err(capacity_error)?,
        );
        let mut global_items = self
            .global
            .items
            .clone()
            .try_acquire_many_owned(count)
            .map_err(capacity_error)?;
        for registration in registrations {
            let capacity = registration.lane(lane);
            let item = capacity
                .items
                .clone()
                .try_acquire_owned()
                .map_err(capacity_error)?;
            let bytes = capacity
                .bytes
                .clone()
                .try_acquire_many_owned(capacity.checked_bytes(payload_bytes)?)
                .map_err(capacity_error)?;
            let global_item = global_items.split(1).ok_or_else(internal_error)?;
            if let Some(delivery) = registration.retain(DeliveryCapacity {
                item,
                bytes,
                global_item,
                shared_bytes: shared.clone(),
            })? {
                reservation.deliveries.push(delivery);
            }
        }
        if self.closed.is_cancelled() {
            return Err(closed_error());
        }
        Ok(reservation)
    }

    /// The caller owns the deadline wakeup on the client worker runtime; this
    /// future only checks expiry and therefore supports arbitrary executors.
    pub(crate) async fn reserve(
        &self,
        payload_bytes: usize,
        lane: MessageLane,
        cancel: &CancellationToken,
        deadline: Option<Instant>,
    ) -> Result<TaskEventReservation> {
        loop {
            if cancel.is_cancelled() {
                return Err(NetError::from(ErrorKind::Cancelled));
            }
            if self.closed.is_cancelled() {
                return Err(closed_error());
            }
            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                return Err(NetError::from(ErrorKind::TimedOut));
            }
            let registrations = self.active();
            if registrations.is_empty() {
                return Ok(TaskEventReservation {
                    deliveries: Vec::new(),
                    payload_bytes,
                    lane,
                });
            }
            let detached = futures::future::select_all(
                registrations
                    .iter()
                    .map(|registration| Box::pin(registration.publisher.receiver_closed())),
            );
            let reservation = async {
                let count = self.recipient_count(registrations.len())?;
                let bytes = self.global.checked_bytes(payload_bytes)?;
                for registration in &registrations {
                    registration.lane(lane).checked_bytes(payload_bytes)?;
                }
                let shared = Arc::new(
                    self.global
                        .bytes
                        .clone()
                        .acquire_many_owned(bytes)
                        .await
                        .map_err(|_| closed_error())?,
                );
                let mut global_items = self
                    .global
                    .items
                    .clone()
                    .acquire_many_owned(count)
                    .await
                    .map_err(|_| closed_error())?;
                let mut deliveries = Vec::new();
                for registration in &registrations {
                    let capacity = registration.lane(lane);
                    let item = capacity
                        .items
                        .clone()
                        .acquire_owned()
                        .await
                        .map_err(|_| closed_error())?;
                    let bytes = capacity
                        .bytes
                        .clone()
                        .acquire_many_owned(capacity.checked_bytes(payload_bytes)?)
                        .await
                        .map_err(|_| closed_error())?;
                    let global_item = global_items.split(1).ok_or_else(internal_error)?;
                    if let Some(delivery) = registration.retain(DeliveryCapacity {
                        item,
                        bytes,
                        global_item,
                        shared_bytes: shared.clone(),
                    })? {
                        deliveries.push(delivery);
                    }
                }
                Ok::<_, NetError>(TaskEventReservation {
                    deliveries,
                    payload_bytes,
                    lane,
                })
            };
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(NetError::from(ErrorKind::Cancelled)),
                _ = self.closed.cancelled() => return Err(closed_error()),
                _ = detached => continue,
                result = reservation => return result,
            }
        }
    }

    fn active(&self) -> Vec<Arc<Registration>> {
        self.registrations
            .iter()
            .filter_map(|registration| {
                if registration.publisher.is_active() {
                    Some(registration.clone())
                } else {
                    registration.release_detached();
                    None
                }
            })
            .collect()
    }

    fn recipient_count(&self, count: usize) -> Result<u32> {
        if count > self.global.max_items {
            return Err(NetError::from(ErrorKind::QueueFull));
        }
        u32::try_from(count).map_err(|_| NetError::from(ErrorKind::QueueFull))
    }
}

impl TaskEventReservation {
    /// Call only after selecting the terminal winner and releasing operation locks.
    pub(crate) fn publish(self, event: TaskEvent) -> Result<()> {
        if event.lane != self.lane || event.source.payload_bytes()? > self.payload_bytes {
            return Err(NetError::input(
                "task_event.source",
                "terminal event differs from its admission reservation",
            ));
        }
        let mut failure = None;
        for delivery in self.deliveries {
            if let Err(error) = delivery.publish(event.clone(), self.payload_bytes.max(1)) {
                crate::log_e!(LogType::WSC; "native_task_delivery", "kind", format!("{:?}", error.kind()));
                if failure.is_none() {
                    failure = Some(error);
                }
            }
        }
        failure.map_or(Ok(()), Err)
    }
}

impl Registration {
    fn lane(&self, lane: MessageLane) -> &Capacity {
        match (lane, &self.urgent) {
            (MessageLane::Urgent, Some(urgent)) => urgent,
            _ => &self.ordinary,
        }
    }

    fn retain(self: &Arc<Self>, capacity: DeliveryCapacity) -> Result<Option<ReservedDelivery>> {
        let capacity = Arc::new(Mutex::new(Some(capacity)));
        let mut state = lock(&self.state);
        if state.closed || !self.publisher.is_active() {
            drop(state);
            return Ok(None);
        }
        let id = state.next_id;
        state.next_id = id
            .checked_add(1)
            .ok_or_else(|| NetError::from(ErrorKind::ResourceExhausted))?;
        state.reservations.insert(id, Arc::downgrade(&capacity));
        Ok(Some(ReservedDelivery {
            registration: self.clone(),
            id,
            capacity,
        }))
    }

    fn release_detached(&self) {
        let retired = {
            let mut state = lock(&self.state);
            state.closed = true;
            std::mem::take(&mut state.reservations)
        };
        for (_, reservation) in retired {
            if let Some(capacity) = reservation.upgrade() {
                let released = lock(&capacity).take();
                drop(released);
            }
        }
    }

    fn close(&self) {
        let finish = {
            let mut state = lock(&self.state);
            state.closed = true;
            state.reservations.is_empty()
        };
        if finish {
            finish_publisher(&self.publisher);
        }
    }
}

impl ReservedDelivery {
    fn publish(self, event: TaskEvent, payload_bytes: usize) -> Result<()> {
        let registration = self.registration.clone();
        let (publication, finish) = {
            let mut state = lock(&registration.state);
            state.reservations.remove(&self.id);
            let capacity = lock(&self.capacity).take();
            let publication = if let Some(capacity) = capacity {
                Some(registration.publisher.prepare_try_publish_with_retained(
                    event,
                    payload_bytes,
                    EventResources {
                        items: Some(capacity.item),
                        bytes: Some(capacity.bytes),
                        shared_bytes: Some(capacity.shared_bytes),
                    },
                    Box::new(capacity.global_item),
                ))
            } else {
                None
            };
            (publication, state.closed && state.reservations.is_empty())
        };
        let result = match publication {
            Some(publication) => publication.dispatch(),
            None => Ok(()),
        };
        if finish {
            finish_publisher(&registration.publisher);
        }
        if let Err(error) = result {
            if error.kind() == ErrorKind::Closed && !registration.publisher.is_active() {
                return Ok(());
            }
            if let Err(delivery_error) = registration.publisher.fail(error.clone()) {
                crate::log_e!(LogType::WSC; "native_task_delivery_failure", "kind", format!("{:?}", delivery_error.kind()));
            }
            return Err(error);
        }
        Ok(())
    }
}

impl Drop for ReservedDelivery {
    fn drop(&mut self) {
        let finish = {
            let registration = self.registration.clone();
            let mut state = lock(&registration.state);
            state.reservations.remove(&self.id);
            let finish = state.closed && state.reservations.is_empty();
            drop(state);
            finish.then_some(registration)
        };
        let released = lock(&self.capacity).take();
        drop(released);
        if let Some(registration) = finish {
            finish_publisher(&registration.publisher);
        }
    }
}

impl Capacity {
    fn new(max_items: usize, max_bytes: usize) -> Self {
        Self {
            items: Arc::new(Semaphore::new(max_items)),
            bytes: Arc::new(Semaphore::new(max_bytes)),
            max_items,
            max_bytes,
        }
    }

    fn checked_bytes(&self, bytes: usize) -> Result<u32> {
        let bytes = bytes.max(1);
        if bytes > self.max_bytes {
            return Err(NetError::from(ErrorKind::ItemTooLarge));
        }
        u32::try_from(bytes).map_err(|_| NetError::from(ErrorKind::ItemTooLarge))
    }
}

fn capacity_lanes(options: &TaskEventOptions) -> Result<(Capacity, Option<Capacity>)> {
    let Some(urgent) = &options.urgent_reserve else {
        return Ok((
            Capacity::new(options.max_tasks, options.max_payload_bytes),
            None,
        ));
    };
    let ordinary_items = options
        .max_tasks
        .checked_sub(urgent.max_items)
        .ok_or_else(internal_error)?;
    let ordinary_bytes = options
        .max_payload_bytes
        .checked_sub(urgent.max_bytes)
        .ok_or_else(internal_error)?;
    Ok((
        Capacity::new(ordinary_items, ordinary_bytes),
        Some(Capacity::new(urgent.max_items, urgent.max_bytes)),
    ))
}

fn capacity_error(error: TryAcquireError) -> NetError {
    match error {
        TryAcquireError::Closed => closed_error(),
        TryAcquireError::NoPermits => NetError::from(ErrorKind::QueueFull),
    }
}

fn closed_error() -> NetError {
    NetError::from(ErrorKind::QueueClosed).with_stage(ErrorStage::Dispatch)
}

fn internal_error() -> NetError {
    NetError::from(ErrorKind::Internal).with_stage(ErrorStage::Dispatch)
}

fn finish_publisher(publisher: &EventPublisher<TaskEvent>) {
    if let Err(error) = publisher.finish() {
        if error.kind() != ErrorKind::Closed || publisher.is_active() {
            crate::log_e!(LogType::WSC; "native_task_finish", "kind", format!("{:?}", error.kind()));
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(state) => state,
        Err(poisoned) => {
            crate::log_e!(LogType::WSC; "native_task_observer", "error", "lock_poisoned_recovered");
            poisoned.into_inner()
        }
    }
}

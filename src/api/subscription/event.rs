pub(crate) use super::event_registration::EventPublication;
use super::event_registration::{Entry, EventRegistration};
use super::*;
use crate::error::{ReceiveError, TryReceiveError};
use std::collections::VecDeque;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Clone, Copy, Debug)]
pub(crate) struct EventQueueLimit {
    pub(crate) max_items: usize,
    pub(crate) max_bytes: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EventOverflow {
    Disconnect,
    DropOldest,
    Wait,
}

/// Queue-owned reservations, independent of the value delivered to application
/// code and of the subscription permit retained by callback jobs.
#[derive(Default)]
pub(crate) struct EventResources {
    pub(crate) items: Option<OwnedSemaphorePermit>,
    pub(crate) bytes: Option<OwnedSemaphorePermit>,
    pub(crate) shared_bytes: Option<Arc<OwnedSemaphorePermit>>,
}

/// A complete initial cursor, installed before its receiver is exposed.
pub(crate) struct EventSeed<T> {
    pub(crate) entries: VecDeque<(T, usize)>,
    pub(crate) lagged: u64,
    pub(crate) source_closed: bool,
    pub(crate) failure: Option<NetError>,
}

pub(crate) fn event_channel<T>(
    limit: EventQueueLimit,
    overflow: EventOverflow,
    executor: Arc<dyn CallbackExecutor>,
    permit: Option<OwnedSemaphorePermit>,
) -> Result<(EventPublisher<T>, EventReceiver<T>)> {
    if limit.max_items == 0 || limit.max_items > Semaphore::MAX_PERMITS {
        return Err(NetError::config(
            "subscription.max_items",
            "must be a positive supported capacity",
        ));
    }
    if limit.max_bytes == 0
        || limit.max_bytes > Semaphore::MAX_PERMITS
        || limit.max_bytes > u32::MAX as usize
    {
        return Err(NetError::config(
            "subscription.max_bytes",
            "must be a positive supported byte allowance",
        ));
    }
    let mut queue = VecDeque::new();
    queue
        .try_reserve_exact(limit.max_items)
        .map_err(|error| NetError::with_source(ErrorKind::ResourceExhausted, error))?;
    let registration = Arc::new(EventRegistration::new(
        source::allocate_id()?,
        limit,
        overflow,
        executor,
        permit,
        queue,
    ));
    Ok((
        EventPublisher {
            owner: Arc::new(PublisherOwner {
                registration: registration.clone(),
            }),
        },
        EventReceiver {
            registration,
            owns_subscription: true,
        },
    ))
}

pub(crate) struct EventPublisher<T> {
    owner: Arc<PublisherOwner<T>>,
}
struct PublisherOwner<T> {
    registration: Arc<EventRegistration<T>>,
}
impl<T> Clone for EventPublisher<T> {
    fn clone(&self) -> Self {
        Self {
            owner: self.owner.clone(),
        }
    }
}
impl<T> EventPublisher<T> {
    pub(crate) fn try_publish(&self, value: T, bytes: usize) -> Result<()> {
        self.prepare_try_publish(value, bytes).dispatch()
    }
    pub(crate) async fn publish(&self, value: T, bytes: usize) -> Result<()> {
        self.publish_with_resources(value, bytes, EventResources::default())
            .await
    }
    pub(crate) async fn publish_with_resources(
        &self,
        value: T,
        bytes: usize,
        resources: EventResources,
    ) -> Result<()> {
        let mut entry = Entry::new(value, bytes, resources);
        loop {
            let notified = self.owner.registration.capacity_available.notified();
            tokio::pin!(notified);
            // Register before checking capacity: dequeue/close cannot be lost
            // between that check and the first pending poll.
            notified.as_mut().enable();
            let (result, pending) = self
                .owner
                .registration
                .prepare_publish(entry)
                .dispatch_with_entry();
            if result? {
                return Ok(());
            }
            entry = pending.ok_or_else(|| NetError::from(ErrorKind::Internal))?;
            notified.await;
        }
    }
    pub(crate) fn finish(&self) -> Result<()> {
        self.prepare_finish().dispatch()
    }
    pub(crate) fn fail(&self, error: NetError) -> Result<()> {
        self.prepare_fail(error).dispatch()
    }
    pub(crate) fn prepare_try_publish(&self, value: T, bytes: usize) -> EventPublication<T> {
        self.prepare_try_publish_with_resources(value, bytes, EventResources::default())
    }
    pub(crate) fn prepare_try_publish_with_resources(
        &self,
        value: T,
        bytes: usize,
        resources: EventResources,
    ) -> EventPublication<T> {
        self.owner
            .registration
            .prepare_publish(Entry::new(value, bytes, resources))
    }
    /// Retain an additional internal reservation until dequeue or unsubscribe,
    /// without making the application-owned value keep the reservation alive.
    pub(crate) fn prepare_try_publish_with_retained(
        &self,
        value: T,
        bytes: usize,
        resources: EventResources,
        retained: Box<dyn Send + Sync>,
    ) -> EventPublication<T> {
        self.owner
            .registration
            .prepare_publish(Entry::new(value, bytes, resources).with_retained(retained))
    }
    pub(crate) fn on_receiver_closed(&self, hook: Arc<dyn Fn() + Send + Sync>) -> Result<()> {
        self.owner.registration.on_receiver_closed(hook)
    }
    pub(crate) fn prepare_skip(&self, skipped: u64) -> EventPublication<T> {
        self.owner.registration.prepare_skip(skipped)
    }
    pub(crate) fn prepare_evict_oldest(&self) -> Option<EventPublication<T>> {
        self.owner.registration.prepare_evict_oldest()
    }
    pub(crate) fn prepare_finish(&self) -> EventPublication<T> {
        self.owner.registration.prepare_finish(None)
    }
    pub(crate) fn prepare_fail(&self, error: NetError) -> EventPublication<T> {
        self.owner.registration.prepare_finish(Some(error))
    }
    pub(crate) fn prepare_seed(&self, seed: EventSeed<T>) -> EventPublication<T> {
        self.owner.registration.prepare_seed(seed)
    }
    pub(crate) fn is_active(&self) -> bool {
        self.owner.registration.is_active()
    }
    /// Waits for consumer detach, including EOF consumption. Finishing the source
    /// alone does not detach a receiver that still owns buffered entries.
    pub(crate) async fn receiver_closed(&self) {
        loop {
            let notified = self.owner.registration.receiver_closed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !self.owner.registration.is_active() {
                return;
            }
            notified.await;
        }
    }
}
impl<T> Drop for PublisherOwner<T> {
    fn drop(&mut self) {
        if let Err(error) = self.registration.prepare_finish(None).dispatch() {
            crate::log_e!(crate::common::log::log_def::LogType::Common;
                "event_publisher_drop", "id|kind", self.registration.id.as_u64(), format!("{:?}", error.kind()));
        }
    }
}

#[must_use]
/// Receives values from an event source in publication order.
///
/// Event streams retain a bounded queue measured by both item count and byte
/// cost. Depending on the source policy, a full queue either waits for the
/// consumer, drops the oldest entries and reports [`ReceiveError::Lagged`], or
/// terminates with [`ReceiveError::Failed`]. Dropping this receiver
/// unsubscribes it and releases queued values and capacity reservations.
pub struct EventReceiver<T> {
    pub(super) registration: Arc<EventRegistration<T>>,
    owns_subscription: bool,
}
impl<T> fmt::Debug for EventReceiver<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EventReceiver")
            .field("id", &self.registration.id)
            .finish_non_exhaustive()
    }
}
impl<T> EventReceiver<T> {
    /// Return the identifier of this event subscription.
    pub fn id(&self) -> SubscriptionId {
        self.registration.id
    }
    /// Await the next event.
    ///
    /// `Ok(Some(value))` is a delivered item, `Ok(None)` is end of stream, and
    /// `Err` reports lag or a source failure. The receive future is cancellation
    /// safe: polling it does not remove an item unless the item is returned.
    pub async fn recv(&mut self) -> std::result::Result<Option<T>, ReceiveError> {
        futures::future::poll_fn(|cx| self.registration.poll_next(cx))
            .await
            .transpose()
    }
    /// Try to receive one event without waiting.
    ///
    /// [`TryReceiveError::Empty`] means no event is currently ready. Lag and
    /// source failures are reported as distinct variants so callers can choose
    /// whether to continue, resynchronize, or terminate.
    pub fn try_recv(&mut self) -> std::result::Result<T, TryReceiveError> {
        self.registration.try_recv()
    }
    /// Detach the receiver and release its queue reservation.
    ///
    /// Returns `true` only for the first call that deactivates this receiver.
    /// Calling it while a callback is running does not block that callback.
    pub fn unsubscribe(&self) -> bool {
        self.registration.unsubscribe()
    }
    /// Consume the receiver and deliver subsequent events to a callback.
    ///
    /// The callback receives a [`CallbackContext`] plus either an event value,
    /// a lag notification, or a source failure. The callback must be `Send`,
    /// `Sync`, and `'static` because it may run on the engine's callback pool.
    /// If setup fails, the receiver remains usable and keeps its buffered
    /// cursor; on success, the returned [`Subscription`] owns the registration.
    pub fn into_callback<F>(mut self, callback: F) -> Result<Subscription>
    where
        T: Send + Sync + 'static,
        F: Fn(CallbackContext, std::result::Result<T, ReceiveError>) + Send + Sync + 'static,
    {
        self.try_into_callback(callback)
    }
    /// Preserve the receiver and its buffered cursor if callback setup fails.
    pub(crate) fn try_into_callback<F>(&mut self, callback: F) -> Result<Subscription>
    where
        T: Send + Sync + 'static,
        F: Fn(CallbackContext, std::result::Result<T, ReceiveError>) + Send + Sync + 'static,
    {
        self.registration.install_callback(callback)?;
        self.owns_subscription = false;
        Ok(Subscription {
            id: self.registration.id,
            control: self.registration.clone(),
        })
    }
}
impl<T> futures::Stream for EventReceiver<T> {
    type Item = std::result::Result<T, ReceiveError>;
    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.registration.poll_next(cx)
    }
}
impl<T> Drop for EventReceiver<T> {
    fn drop(&mut self) {
        if self.owns_subscription {
            self.registration.unsubscribe();
        }
    }
}

#[cfg(test)]
#[path = "event_resource_tests.rs"]
mod resource_tests;

#[cfg(test)]
#[path = "event_detach_regression_tests.rs"]
mod detach_regression_tests;

//! Owned state observations and callback subscriptions shared by all transports.

use crate::error::{ErrorKind, NetError};
use crate::Result;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::task::{Context, Poll};

pub(crate) mod common_executor;
mod dispatch;
mod event;
mod event_registration;
pub use event::EventReceiver;
pub(crate) use event::{
    event_channel, EventOverflow, EventPublisher, EventQueueLimit, EventResources,
};
#[cfg(feature = "ws-client")]
pub(crate) use event::{EventPublication, EventSeed};
mod registration;
mod source;
pub(crate) use source::{StatePublication, StatePublisher, StateSource};

/// Uses an existing engine pool. Jobs and their destructors run outside pool locks.
pub(crate) trait CallbackExecutor: Send + Sync + 'static {
    fn ensure_ready(&self) -> Result<()>;
    fn submit(&self, job: Box<dyn FnOnce() + Send + 'static>) -> Result<()>;
}
trait SubscriptionControl: Send + Sync {
    fn is_active(&self) -> bool;
    fn unsubscribe(&self) -> bool;
    fn poll_close(&self, cx: &mut Context<'_>) -> Poll<Result<()>>;
}
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            crate::log_e!(crate::common::log::log_def::LogType::Common;
                "subscription_lock", "kind|reason", format!("{:?}", ErrorKind::Internal), "poisoned mutex recovered");
            poisoned.into_inner()
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
/// Opaque identifier assigned to a state or event subscription.
///
/// IDs are unique within the running process for as long as the counter can
/// represent another value. They carry no authority and cannot be used to
/// create, resume, or access a subscription by themselves.
pub struct SubscriptionId(u64);
impl SubscriptionId {
    /// Return the numeric representation of this identifier.
    ///
    /// The value is intended for diagnostics, correlation, and logging; it is
    /// not a stable identifier across process restarts.
    pub fn as_u64(self) -> u64 {
        self.0
    }
}
impl fmt::Display for SubscriptionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[must_use]
/// Receives the latest values published by a state source.
///
/// A state receiver starts with the source's current snapshot and then yields
/// each newer revision. Dropping or explicitly unsubscribing the receiver
/// releases its bounded subscription slot. The type also implements
/// [`futures::Stream`] for integrations that prefer polling.
pub struct StateReceiver<T> {
    registration: Arc<registration::Registration<T>>,
    owns_subscription: bool,
}
impl<T> fmt::Debug for StateReceiver<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StateReceiver")
            .field("id", &self.registration.id)
            .finish_non_exhaustive()
    }
}
impl<T: Clone> StateReceiver<T> {
    /// Attach SDK observation ownership to this registration. Callback conversion
    /// keeps the same registration, so it transfers this ownership without a gap.
    #[cfg_attr(not(any(feature = "http-client", test)), allow(dead_code))]
    pub(crate) fn bind_lifetime(&self, lifetime: impl Send + Sync + 'static) -> Result<()> {
        self.registration.bind_lifetime(Box::new(lifetime))
    }
    /// Return the subscription identifier associated with this receiver.
    pub fn id(&self) -> SubscriptionId {
        self.registration.id
    }
    /// Clone and return the most recently committed state value.
    ///
    /// This method does not advance the receiver cursor and therefore does not
    /// consume a pending update.
    pub fn current(&self) -> T {
        self.registration.current()
    }
    /// Wait for the next state value.
    ///
    /// `Ok(Some(value))` is a newly observed value, `Ok(None)` means the source
    /// has terminated, and `Err` reports a source or callback infrastructure
    /// failure. The future is cancellation safe: cancelling it does not consume
    /// a value that has not been returned to the caller.
    pub async fn recv(&mut self) -> Result<Option<T>> {
        futures::future::poll_fn(|cx| self.registration.poll_next(cx))
            .await
            .transpose()
    }
    /// Stop receiving values and release this receiver's subscription slot.
    ///
    /// Returns `true` only when this call performed the transition from active
    /// to inactive. Repeated calls are harmless and return `false`.
    pub fn unsubscribe(&self) -> bool {
        self.registration.unsubscribe()
    }
    /// Replace polling with an asynchronous callback.
    ///
    /// The callback receives the subscription context and either a cloned state
    /// value or the terminal error. Buffered state and the receiver cursor are
    /// preserved if callback installation fails. On success, ownership of the
    /// subscription is returned as an RAII [`Subscription`] handle.
    pub fn into_callback<F>(mut self, callback: F) -> Result<Subscription>
    where
        T: Send + Sync + 'static,
        F: Fn(CallbackContext, Result<T>) + Send + Sync + 'static,
    {
        self.registration.install_callback(callback)?;
        self.owns_subscription = false;
        Ok(Subscription {
            id: self.registration.id,
            control: self.registration.clone(),
        })
    }
}
impl<T: Clone> futures::Stream for StateReceiver<T> {
    type Item = Result<T>;
    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.registration.poll_next(cx)
    }
}
impl<T> Drop for StateReceiver<T> {
    fn drop(&mut self) {
        if self.owns_subscription {
            self.registration.unsubscribe();
        }
    }
}

#[must_use]
/// Controls a registered callback subscription.
///
/// Dropping this handle unsubscribes immediately and without waiting for a
/// callback worker. Use [`Subscription::close`] when the caller must await the
/// retirement of an already running callback and observe any callback fault.
pub struct Subscription {
    id: SubscriptionId,
    control: Arc<dyn SubscriptionControl>,
}
impl Subscription {
    /// Return the identifier of the controlled subscription.
    pub fn id(&self) -> SubscriptionId {
        self.id
    }
    /// Report whether the subscription is still accepting deliveries.
    pub fn is_active(&self) -> bool {
        self.control.is_active()
    }
    /// Prevent future deliveries and release the subscription's capacity.
    ///
    /// This operation is synchronous and idempotent. It does not wait for a
    /// callback that is already running to finish.
    pub fn unsubscribe(&self) -> bool {
        self.control.unsubscribe()
    }
    /// Unsubscribe and wait for callback execution to retire.
    ///
    /// The returned error preserves a callback panic or dispatch failure. A
    /// successful result means the callback is no longer running; it does not
    /// imply that a previously delivered callback completed application work
    /// after returning control to the SDK.
    pub async fn close(self) -> Result<()> {
        self.unsubscribe();
        futures::future::poll_fn(|cx| self.control.poll_close(cx)).await
    }
}
impl Drop for Subscription {
    fn drop(&mut self) {
        self.control.unsubscribe();
    }
}
impl fmt::Debug for Subscription {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Subscription")
            .field("id", &self.id)
            .field("active", &self.is_active())
            .finish()
    }
}

#[derive(Clone)]
/// Context passed to every callback invocation.
///
/// The context contains only a weak control reference, so retaining it does
/// not keep the subscription or callback alive. It can be used to request
/// unsubscription from inside the callback.
pub struct CallbackContext {
    id: SubscriptionId,
    control: Weak<dyn SubscriptionControl>,
}
impl CallbackContext {
    /// Return the identifier of the subscription invoking the callback.
    pub fn id(&self) -> SubscriptionId {
        self.id
    }
    /// Request unsubscription through this callback context.
    ///
    /// Returns `false` when the subscription has already been dropped or
    /// detached. The request is synchronous and does not wait for the current
    /// callback invocation to return.
    pub fn unsubscribe(&self) -> bool {
        self.control
            .upgrade()
            .is_some_and(|control| control.unsubscribe())
    }
}
impl fmt::Debug for CallbackContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CallbackContext")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod lifetime_tests;
#[cfg(test)]
mod tests;

#[cfg(test)]
mod event_regression_tests;
#[cfg(test)]
mod event_tests;

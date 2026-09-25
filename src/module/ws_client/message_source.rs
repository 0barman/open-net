//! Session-scoped raw message subscriptions with client-wide queue accounting.

use super::data_subscription_executor::DataSubscriptionExecutor;
use crate::common::log::log_def::LogType;
use crate::error::{ErrorKind, ErrorStage, ReceiveError};
use crate::subscription::{
    event_channel, CallbackContext, EventOverflow, EventPublisher, EventQueueLimit, EventResources,
    Subscription,
};
use crate::ws::{
    DispatchLimits, IncomingMessage, IncomingPayload, InitialMessages, MessageReceiver,
    ReceiveOptions, ReceiveOverflow, SubscriptionId,
};
use crate::{NetError, Result};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

/// Retained by the client and reused by every session, including old unread inboxes.
pub(crate) struct MessageResources {
    inputs: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    deliveries: Arc<Semaphore>,
    subscriptions: Arc<Semaphore>,
    max_bytes: usize,
    max_deliveries: usize,
}

impl MessageResources {
    pub(crate) fn new(limits: &DispatchLimits) -> Result<Arc<Self>> {
        limits.validate()?;
        Ok(Arc::new(Self {
            inputs: Arc::new(Semaphore::new(limits.incoming.max_items)),
            bytes: Arc::new(Semaphore::new(limits.incoming.max_bytes)),
            deliveries: Arc::new(Semaphore::new(limits.message_deliveries)),
            subscriptions: Arc::new(Semaphore::new(limits.message_subscriptions)),
            max_bytes: limits.incoming.max_bytes,
            max_deliveries: limits.message_deliveries,
        }))
    }

    fn try_reserve(&self, bytes: usize, recipients: usize) -> Result<MessageLease> {
        if bytes > self.max_bytes {
            return Err(NetError::from(ErrorKind::ItemTooLarge).with_stage(ErrorStage::Receive));
        }
        if recipients > self.max_deliveries {
            return Err(overflow());
        }
        let bytes = u32::try_from(bytes).map_err(|_| overflow())?;
        let recipients = u32::try_from(recipients).map_err(|_| overflow())?;
        let input = Arc::new(
            self.inputs
                .clone()
                .try_acquire_owned()
                .map_err(capacity_error)?,
        );
        let bytes = Arc::new(
            self.bytes
                .clone()
                .try_acquire_many_owned(bytes)
                .map_err(capacity_error)?,
        );
        let deliveries = self
            .deliveries
            .clone()
            .try_acquire_many_owned(recipients)
            .map_err(capacity_error)?;
        Ok(MessageLease {
            input,
            bytes,
            deliveries,
        })
    }
}

struct MessageLease {
    input: Arc<OwnedSemaphorePermit>,
    bytes: Arc<OwnedSemaphorePermit>,
    deliveries: OwnedSemaphorePermit,
}

struct DeliveryLease {
    _input: Arc<OwnedSemaphorePermit>,
    _delivery: OwnedSemaphorePermit,
}

pub(crate) struct MessageSource {
    state: Mutex<SourceState>,
    resources: Arc<MessageResources>,
    executor: Arc<DataSubscriptionExecutor>,
}

struct SourceState {
    closed: bool,
    initial: Option<MessageReceiver>,
    initial_transfer: bool,
    registrations: BTreeMap<SubscriptionId, Arc<Registration>>,
}

struct Registration {
    publisher: EventPublisher<IncomingMessage>,
    include_control: bool,
    drop_oldest: bool,
}

impl MessageSource {
    /// Installs the initial inbox before the session can start network work.
    pub(crate) fn new(
        resources: Arc<MessageResources>,
        executor: Arc<DataSubscriptionExecutor>,
        initial: InitialMessages,
        manual_routing: bool,
    ) -> Result<Arc<Self>> {
        if let InitialMessages::Buffer(options) = &initial {
            options.validate()?;
            if manual_routing && options.overflow == ReceiveOverflow::DropOldest {
                return Err(NetError::config(
                    "initial_messages.overflow",
                    "Manual routing requires a lossless initial inbox",
                ));
            }
        }
        let source = Arc::new(Self {
            state: Mutex::new(SourceState {
                closed: false,
                initial: None,
                initial_transfer: false,
                registrations: BTreeMap::new(),
            }),
            resources,
            executor,
        });
        if let InitialMessages::Buffer(options) = initial {
            let receiver = source.register(options)?;
            lock(&source.state).initial = Some(receiver);
        }
        Ok(source)
    }

    pub(crate) fn take_initial(&self) -> Option<MessageReceiver> {
        lock(&self.state).initial.take()
    }

    pub(crate) fn subscribe(&self, options: ReceiveOptions) -> Result<MessageReceiver> {
        {
            let state = lock(&self.state);
            if state.initial.is_some() || state.initial_transfer {
                return Err(NetError::input(
                    "initial_messages",
                    "take or transfer the initial inbox before adding subscribers",
                ));
            }
        }
        self.register(options)
    }

    /// Callback setup prepares a real shared-pool worker with no source lock.
    /// Failed worker initialization or first submission restores the exact
    /// receiver, including its buffered prefix and cursor.
    pub(crate) fn on_initial_message<F>(&self, callback: F) -> Result<Subscription>
    where
        F: Fn(CallbackContext, std::result::Result<IncomingMessage, ReceiveError>)
            + Send
            + Sync
            + 'static,
    {
        let receiver = {
            let mut state = lock(&self.state);
            if state.initial_transfer {
                return Err(NetError::input(
                    "initial_messages",
                    "initial inbox transfer in progress",
                ));
            }
            let receiver = state.initial.take().ok_or_else(|| {
                NetError::input(
                    "initial_messages",
                    "initial inbox is disabled or already taken",
                )
            })?;
            state.initial_transfer = true;
            receiver
        };
        let mut transfer = InitialTransfer {
            source: self,
            receiver: Some(receiver),
            committed: false,
        };
        let receiver = transfer.receiver.as_mut().ok_or_else(internal_error)?;
        let subscription = receiver.try_into_callback(callback)?;
        transfer.committed = true;
        Ok(subscription)
    }

    fn register(&self, options: ReceiveOptions) -> Result<MessageReceiver> {
        options.validate()?;
        self.prune_detached();
        let permit = self
            .resources
            .subscriptions
            .clone()
            .try_acquire_owned()
            .map_err(|_| {
                NetError::from(ErrorKind::SubscriptionLimitReached).with_stage(ErrorStage::Receive)
            })?;
        let (publisher, receiver) = event_channel(
            EventQueueLimit {
                max_items: options.max_messages,
                max_bytes: options.max_bytes,
            },
            match options.overflow {
                ReceiveOverflow::Disconnect => EventOverflow::Disconnect,
                ReceiveOverflow::DropOldest => EventOverflow::DropOldest,
            },
            Arc::new(self.executor.fork()),
            Some(permit),
        )?;
        let registration = Arc::new(Registration {
            publisher,
            include_control: options.include_control_frames,
            drop_oldest: options.overflow == ReceiveOverflow::DropOldest,
        });
        let mut state = lock(&self.state);
        if state.closed {
            drop(state);
            return Err(NetError::from(ErrorKind::Closed));
        }
        state.registrations.insert(receiver.id(), registration);
        Ok(receiver)
    }

    /// The reader runs automatic/manual response routing before publishing here.
    /// Clones retain the immutable original session and physical-connection origin.
    pub(crate) fn publish(&self, message: IncomingMessage) -> Result<()> {
        self.publish_with_admission(message, |_| Ok(()), |_| Ok(()))
    }

    /// Manual response credentials are granted only after reserving observed
    /// delivery, and revoked if every captured receiver rejects the input.
    pub(crate) fn publish_with_admission<A, R>(
        &self,
        mut message: IncomingMessage,
        admit: A,
        reject: R,
    ) -> Result<()>
    where
        A: FnOnce(&mut IncomingMessage) -> Result<()>,
        R: FnOnce(&IncomingMessage) -> Result<()>,
    {
        self.prune_detached();
        let control = !matches!(message.payload(), IncomingPayload::Message(_));
        let mut registrations = {
            let state = lock(&self.state);
            if state.closed {
                return Err(NetError::from(ErrorKind::Closed));
            }
            state
                .registrations
                .values()
                .filter(|registration| !control || registration.include_control)
                .cloned()
                .collect::<Vec<_>>()
        };
        let size = payload_bytes(&message)?.max(1);
        let mut lease = loop {
            registrations.retain(|registration| registration.publisher.is_active());
            if registrations.is_empty() {
                return Ok(());
            }
            match self.resources.try_reserve(size, registrations.len()) {
                Ok(lease) => break lease,
                Err(error) if error.kind() == ErrorKind::QueueFull => {}
                Err(error) => return Err(error),
            }
            let mut evicted = false;
            let mut acquired = None;
            for registration in &registrations {
                if !registration.drop_oldest {
                    continue;
                }
                if let Some(publication) = registration.publisher.prepare_evict_oldest() {
                    publication.dispatch()?;
                    evicted = true;
                    match self.resources.try_reserve(size, registrations.len()) {
                        Ok(lease) => {
                            acquired = Some(lease);
                            break;
                        }
                        Err(error) if error.kind() == ErrorKind::QueueFull => {}
                        Err(error) => return Err(error),
                    }
                }
            }
            if let Some(lease) = acquired {
                break lease;
            }
            if !evicted {
                if registrations
                    .iter()
                    .any(|registration| !registration.publisher.is_active())
                {
                    continue;
                }
                return Err(overflow());
            }
        };
        let mut deliveries = Vec::new();
        deliveries
            .try_reserve_exact(registrations.len())
            .map_err(|error| NetError::with_source(ErrorKind::ResourceExhausted, error))?;
        for _ in &registrations {
            let delivery = lease.deliveries.split(1).ok_or_else(internal_error)?;
            deliveries.push(DeliveryLease {
                _input: lease.input.clone(),
                _delivery: delivery,
            });
        }
        let mut publications = Vec::new();
        publications
            .try_reserve_exact(registrations.len())
            .map_err(|error| NetError::with_source(ErrorKind::ResourceExhausted, error))?;
        admit(&mut message)?;
        let mut accepted = false;
        for (registration, delivery) in registrations.iter().zip(deliveries) {
            let publication = registration.publisher.prepare_try_publish_with_retained(
                message.clone(),
                size,
                EventResources {
                    items: None,
                    bytes: None,
                    shared_bytes: Some(lease.bytes.clone()),
                },
                Box::new(delivery),
            );
            accepted |= publication.result().is_ok();
            publications.push((registration, publication));
        }
        let mut failure = if accepted {
            None
        } else {
            reject(&message).err()
        };
        for (registration, publication) in publications {
            if let Err(error) = publication.dispatch() {
                if error.kind() == ErrorKind::Closed && !registration.publisher.is_active() {
                    continue;
                }
                if failure.is_none() {
                    failure = Some(error);
                }
            }
        }
        failure.map_or(Ok(()), Err)
    }

    pub(crate) fn finish(&self) {
        self.close(None);
    }

    pub(crate) fn fail(&self, error: NetError) {
        self.close(Some(error));
    }

    fn close(&self, failure: Option<NetError>) {
        let registrations = {
            let mut state = lock(&self.state);
            if state.closed {
                return;
            }
            state.closed = true;
            std::mem::take(&mut state.registrations)
        };
        for (_, registration) in registrations {
            let publication = match &failure {
                Some(error) => registration.publisher.prepare_fail(error.clone()),
                None => registration.publisher.prepare_finish(),
            };
            if let Err(error) = publication.dispatch() {
                crate::log_e!(LogType::WSC; "message_source_close", "kind", format!("{:?}", error.kind()));
            }
        }
    }

    fn prune_detached(&self) {
        let retired = {
            let mut state = lock(&self.state);
            let ids = state
                .registrations
                .iter()
                .filter_map(|(id, registration)| {
                    (!registration.publisher.is_active()).then_some(*id)
                })
                .collect::<Vec<_>>();
            ids.into_iter()
                .filter_map(|id| state.registrations.remove(&id))
                .collect::<Vec<_>>()
        };
        drop(retired);
    }
}

impl Drop for MessageSource {
    fn drop(&mut self) {
        self.finish();
    }
}

struct InitialTransfer<'a> {
    source: &'a MessageSource,
    receiver: Option<MessageReceiver>,
    committed: bool,
}

impl Drop for InitialTransfer<'_> {
    fn drop(&mut self) {
        let mut state = lock(&self.source.state);
        state.initial_transfer = false;
        if !self.committed && state.initial.is_none() {
            state.initial = self.receiver.take();
        }
        drop(state);
        // A successfully converted receiver no longer owns unsubscription.
        // Any retired receiver is destroyed only after releasing the source.
    }
}

fn payload_bytes(message: &IncomingMessage) -> Result<usize> {
    match message.payload() {
        IncomingPayload::Message(message) => Ok(message.len()),
        IncomingPayload::Ping(bytes) | IncomingPayload::Pong(bytes) => Ok(bytes.len()),
        IncomingPayload::Close(close) => {
            close.reason.len().checked_add(2).ok_or_else(|| {
                NetError::from(ErrorKind::ItemTooLarge).with_stage(ErrorStage::Receive)
            })
        }
    }
}

fn capacity_error(error: TryAcquireError) -> NetError {
    match error {
        TryAcquireError::Closed => NetError::from(ErrorKind::Closed),
        TryAcquireError::NoPermits => NetError::from(ErrorKind::QueueFull),
    }
}

fn overflow() -> NetError {
    NetError::from(ErrorKind::CallbackOverflow).with_stage(ErrorStage::Receive)
}

fn internal_error() -> NetError {
    NetError::from(ErrorKind::Internal).with_stage(ErrorStage::Receive)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(state) => state,
        Err(poisoned) => {
            crate::log_e!(LogType::WSC; "message_source", "error", "lock_poisoned_recovered");
            poisoned.into_inner()
        }
    }
}

use super::event::{EventOverflow, EventQueueLimit, EventSeed};
use super::*;
use crate::error::{ReceiveError, TryReceiveError};
use std::collections::VecDeque;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::task::Waker;
use tokio::sync::{Notify, OwnedSemaphorePermit};

type Callback<T> =
    dyn Fn(CallbackContext, std::result::Result<T, ReceiveError>) + Send + Sync + 'static;
type Driver = dyn Fn() -> Result<()> + Send + Sync + 'static;
pub(super) struct Entry<T> {
    value: T,
    bytes: usize,
    resources: EventResources,
    retained: Option<Box<dyn Send + Sync>>,
}
impl<T> Entry<T> {
    pub(super) fn new(value: T, bytes: usize, resources: EventResources) -> Self {
        Self {
            value,
            bytes,
            resources,
            retained: None,
        }
    }
    pub(super) fn with_retained(mut self, retained: Box<dyn Send + Sync>) -> Self {
        self.retained = Some(retained);
        self
    }
    /// Only called after the entry and callback execution right leave the queue
    /// lock. Permit release can synchronously invoke an application waker.
    fn into_value(self) -> T {
        let Self {
            value,
            resources,
            retained,
            ..
        } = self;
        let EventResources {
            items,
            bytes,
            shared_bytes,
        } = resources;
        drop((items, bytes, shared_bytes, retained));
        value
    }
}
pub(super) struct EventRegistration<T> {
    pub(super) id: SubscriptionId,
    pub(super) state: Mutex<EventState<T>>,
    pub(super) capacity_available: Notify,
    pub(super) receiver_closed: Notify,
    limit: EventQueueLimit,
    overflow: EventOverflow,
    #[cfg(all(test, feature = "ws-client"))]
    after_pending: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}
pub(super) struct EventState<T> {
    queue: VecDeque<Entry<T>>,
    bytes: usize,
    lagged: u64,
    active: bool,
    source_closed: bool,
    pristine: bool,
    pending_error: Option<NetError>,
    final_error: Option<NetError>,
    executor: Option<Arc<dyn CallbackExecutor>>,
    callback: Option<Arc<Callback<T>>>,
    driver: Option<Arc<Driver>>,
    permit: Option<Arc<OwnedSemaphorePermit>>,
    scheduled: bool,
    running: bool,
    receive_waker: Option<Waker>,
    close_waker: Option<Waker>,
    detach_hook: Option<Arc<dyn Fn() + Send + Sync>>,
    callback_installing: bool,
    callback_install_error: Option<NetError>,
    #[cfg(test)]
    fail_retirement_reserve: bool,
}
enum Delivery<T> {
    Item(std::result::Result<Entry<T>, ReceiveError>),
    Done,
    Pending,
}
impl<T> EventState<T> {
    fn next(&mut self) -> Delivery<T> {
        // Even an empty poll exposes the cursor and makes later reseeding invalid.
        self.pristine = false;
        if !self.active {
            return Delivery::Done;
        }
        if self.lagged != 0 {
            return Delivery::Item(Err(ReceiveError::Lagged {
                skipped: std::mem::take(&mut self.lagged),
            }));
        }
        if let Some(entry) = self.queue.pop_front() {
            self.bytes -= entry.bytes;
            return Delivery::Item(Ok(entry));
        }
        if let Some(error) = self.pending_error.take() {
            return Delivery::Item(Err(ReceiveError::Failed(error)));
        }
        if self.source_closed {
            Delivery::Done
        } else {
            Delivery::Pending
        }
    }
    fn pending(&self) -> bool {
        self.lagged != 0
            || !self.queue.is_empty()
            || self.pending_error.is_some()
            || self.source_closed
    }
}

/// Queue changes are already committed. Dispatch only after releasing every
/// caller-owned lock: rejected inputs, retired entries and wakers stay here.
#[must_use]
pub(crate) struct EventPublication<T> {
    registration: Arc<EventRegistration<T>>,
    result: Result<bool>,
    input: Option<Entry<T>>,
    seed: Option<EventSeed<T>>,
    failure: Option<NetError>,
    retired: Vec<Entry<T>>,
    wake: Option<Waker>,
    notify_capacity: bool,
    schedule: bool,
}
impl<T> EventPublication<T> {
    fn new(registration: &Arc<EventRegistration<T>>) -> Self {
        Self {
            registration: registration.clone(),
            result: Ok(true),
            input: None,
            seed: None,
            failure: None,
            retired: Vec::new(),
            wake: None,
            notify_capacity: false,
            schedule: false,
        }
    }
    pub(crate) fn result(&self) -> Result<()> {
        self.result.clone().and_then(admission_result)
    }
    pub(crate) fn dispatch(self) -> Result<()> {
        let (result, input) = self.dispatch_with_entry();
        drop(input);
        result.and_then(admission_result)
    }
    pub(super) fn dispatch_with_entry(self) -> (Result<bool>, Option<Entry<T>>) {
        let Self {
            registration,
            result,
            input,
            seed,
            failure,
            retired,
            wake,
            notify_capacity,
            schedule,
        } = self;
        drop((retired, seed, failure));
        if notify_capacity {
            registration.capacity_available.notify_waiters();
        }
        if let Some(waker) = wake {
            waker.wake();
        }
        if schedule {
            if let Err(error) = registration.schedule() {
                return (Err(error), input);
            }
        }
        (result, input)
    }
}
fn admission_result(accepted: bool) -> Result<()> {
    if accepted {
        Ok(())
    } else {
        Err(NetError::from(ErrorKind::QueueFull))
    }
}

impl<T> EventRegistration<T> {
    pub(super) fn new(
        id: SubscriptionId,
        limit: EventQueueLimit,
        overflow: EventOverflow,
        executor: Arc<dyn CallbackExecutor>,
        permit: Option<OwnedSemaphorePermit>,
        queue: VecDeque<Entry<T>>,
    ) -> Self {
        Self {
            id,
            limit,
            overflow,
            #[cfg(all(test, feature = "ws-client"))]
            after_pending: Mutex::new(None),
            capacity_available: Notify::new(),
            receiver_closed: Notify::new(),
            state: Mutex::new(EventState {
                queue,
                bytes: 0,
                lagged: 0,
                active: true,
                source_closed: false,
                pristine: true,
                pending_error: None,
                final_error: None,
                executor: Some(executor),
                callback: None,
                driver: None,
                permit: permit.map(Arc::new),
                scheduled: false,
                running: false,
                receive_waker: None,
                close_waker: None,
                detach_hook: None,
                callback_installing: false,
                callback_install_error: None,
                #[cfg(test)]
                fail_retirement_reserve: false,
            }),
        }
    }
    pub(super) fn prepare_publish(self: &Arc<Self>, entry: Entry<T>) -> EventPublication<T> {
        let mut publication = EventPublication::new(self);
        publication.input = Some(entry);
        publication.result = self.commit_publish(&mut publication);
        publication
    }
    fn commit_publish(&self, publication: &mut EventPublication<T>) -> Result<bool> {
        let bytes = publication
            .input
            .as_ref()
            .ok_or_else(|| NetError::from(ErrorKind::Internal))?
            .bytes;
        if bytes > self.limit.max_bytes {
            return Err(NetError::from(ErrorKind::ItemTooLarge));
        }
        let mut state = lock(&self.state);
        if !state.active || state.source_closed {
            return Err(NetError::from(ErrorKind::Closed));
        }
        let total = state
            .bytes
            .checked_add(bytes)
            .ok_or_else(|| NetError::from(ErrorKind::ResourceExhausted))?;
        let full = state.queue.len() == self.limit.max_items || total > self.limit.max_bytes;
        if full && self.overflow == EventOverflow::Wait {
            return Ok(false);
        }
        if full && self.overflow == EventOverflow::Disconnect {
            let error = NetError::from(ErrorKind::CallbackOverflow)
                .with_stage(crate::error::ErrorStage::Dispatch);
            state.pending_error = Some(error.clone());
            state.final_error = Some(error.clone());
            state.source_closed = true;
            state.pristine = false;
            publication.wake = state.receive_waker.take();
            publication.notify_capacity = true;
            publication.schedule = true;
            return Err(error);
        }
        if full {
            let mut remaining_bytes = state.bytes;
            let mut count = 0usize;
            for entry in &state.queue {
                remaining_bytes -= entry.bytes;
                count += 1;
                if state.queue.len() - count < self.limit.max_items
                    && remaining_bytes + bytes <= self.limit.max_bytes
                {
                    break;
                }
            }
            let skipped =
                u64::try_from(count).map_err(|_| NetError::from(ErrorKind::ResourceExhausted))?;
            let lagged = state
                .lagged
                .checked_add(skipped)
                .ok_or_else(|| NetError::from(ErrorKind::ResourceExhausted))?;
            Self::reserve_retired(&mut state, publication, count)?;
            for _ in 0..count {
                if let Some(entry) = state.queue.pop_front() {
                    publication.retired.push(entry);
                }
            }
            state.bytes = remaining_bytes;
            state.lagged = lagged;
        }
        let entry = publication
            .input
            .take()
            .ok_or_else(|| NetError::from(ErrorKind::Internal))?;
        state.bytes += bytes;
        state.queue.push_back(entry);
        state.pristine = false;
        publication.wake = state.receive_waker.take();
        publication.schedule = true;
        Ok(true)
    }
    fn reserve_retired(
        state: &mut EventState<T>,
        publication: &mut EventPublication<T>,
        count: usize,
    ) -> Result<()> {
        // Keep the live queue's original reservation. Moving its whole allocation
        // out would make the next push allocate under the caller's outer lock.
        #[cfg(test)]
        if std::mem::take(&mut state.fail_retirement_reserve) {
            return Err(NetError::from(ErrorKind::ResourceExhausted));
        }
        #[cfg(not(test))]
        let _ = state;
        publication
            .retired
            .try_reserve_exact(count)
            .map_err(|error| NetError::with_source(ErrorKind::ResourceExhausted, error))
    }
    pub(super) fn prepare_skip(self: &Arc<Self>, skipped: u64) -> EventPublication<T> {
        let mut publication = EventPublication::new(self);
        publication.result = self.commit_skip(&mut publication, skipped);
        publication
    }
    pub(super) fn prepare_evict_oldest(self: &Arc<Self>) -> Option<EventPublication<T>> {
        let mut publication = EventPublication::new(self);
        let mut state = lock(&self.state);
        if !state.active
            || state.source_closed
            || self.overflow != EventOverflow::DropOldest
            || state.queue.is_empty()
        {
            return None;
        }
        let Some(lagged) = state.lagged.checked_add(1) else {
            publication.result = Err(NetError::from(ErrorKind::ResourceExhausted));
            return Some(publication);
        };
        if let Err(error) = Self::reserve_retired(&mut state, &mut publication, 1) {
            publication.result = Err(error);
            return Some(publication);
        }
        if let Some(entry) = state.queue.pop_front() {
            state.bytes -= entry.bytes;
            state.lagged = lagged;
            state.pristine = false;
            publication.retired.push(entry);
            publication.wake = state.receive_waker.take();
            publication.notify_capacity = true;
            publication.schedule = true;
        }
        Some(publication)
    }
    fn commit_skip(&self, publication: &mut EventPublication<T>, skipped: u64) -> Result<bool> {
        if self.overflow != EventOverflow::DropOldest {
            return Err(NetError::input(
                "subscription.overflow",
                "skipping requires DropOldest overflow",
            ));
        }
        let mut state = lock(&self.state);
        if !state.active || state.source_closed {
            return Err(NetError::from(ErrorKind::Closed));
        }
        let count = state.queue.len();
        let queued =
            u64::try_from(count).map_err(|_| NetError::from(ErrorKind::ResourceExhausted))?;
        let lagged = state
            .lagged
            .checked_add(queued)
            .and_then(|value| value.checked_add(skipped))
            .ok_or_else(|| NetError::from(ErrorKind::ResourceExhausted))?;
        Self::reserve_retired(&mut state, publication, count)?;
        while let Some(entry) = state.queue.pop_front() {
            publication.retired.push(entry);
        }
        state.bytes = 0;
        state.lagged = lagged;
        state.pristine = false;
        publication.wake = state.receive_waker.take();
        publication.notify_capacity = true;
        publication.schedule = true;
        Ok(true)
    }
    pub(super) fn prepare_finish(
        self: &Arc<Self>,
        failure: Option<NetError>,
    ) -> EventPublication<T> {
        let mut publication = EventPublication::new(self);
        publication.failure = failure;
        {
            let mut state = lock(&self.state);
            if state.active && !state.source_closed {
                state.pristine = false;
                state.source_closed = true;
                state.pending_error = publication.failure.clone();
                state.final_error = publication.failure.clone();
                publication.wake = state.receive_waker.take();
                publication.notify_capacity = true;
                publication.schedule = true;
            }
        }
        publication
    }
    pub(super) fn prepare_seed(self: &Arc<Self>, seed: EventSeed<T>) -> EventPublication<T> {
        let mut publication = EventPublication::new(self);
        publication.seed = Some(seed);
        publication.result = self.commit_seed(&mut publication);
        publication
    }
    fn commit_seed(&self, publication: &mut EventPublication<T>) -> Result<bool> {
        let seed = publication
            .seed
            .as_mut()
            .ok_or_else(|| NetError::from(ErrorKind::Internal))?;
        if seed.failure.is_some() && !seed.source_closed {
            return Err(NetError::input(
                "subscription.seed",
                "a failed seed must be closed",
            ));
        }
        if seed.entries.len() > self.limit.max_items {
            return Err(NetError::from(ErrorKind::QueueFull));
        }
        let mut total = 0usize;
        for (_, bytes) in &seed.entries {
            if *bytes > self.limit.max_bytes {
                return Err(NetError::from(ErrorKind::ItemTooLarge));
            }
            total = total
                .checked_add(*bytes)
                .ok_or_else(|| NetError::from(ErrorKind::ResourceExhausted))?;
            if total > self.limit.max_bytes {
                return Err(NetError::from(ErrorKind::QueueFull));
            }
        }
        let mut state = lock(&self.state);
        if !state.active {
            return Err(NetError::from(ErrorKind::Closed));
        }
        if !state.pristine {
            return Err(NetError::input(
                "subscription.seed",
                "seed requires an unused receiver",
            ));
        }
        // The queue was reserved at channel construction, before any outer lock.
        // Every limit and invariant is checked before moving a single user value.
        while let Some((value, bytes)) = seed.entries.pop_front() {
            state
                .queue
                .push_back(Entry::new(value, bytes, EventResources::default()));
        }
        state.bytes = total;
        state.lagged = seed.lagged;
        state.source_closed = seed.source_closed;
        state.pending_error = seed.failure.clone();
        state.final_error = seed.failure.clone();
        state.pristine = false;
        publication.wake = state.receive_waker.take();
        publication.notify_capacity = seed.source_closed;
        publication.schedule = true;
        Ok(true)
    }
    pub(super) fn is_active(&self) -> bool {
        lock(&self.state).active
    }
    /// The internal owner can revoke admission reservations synchronously when
    /// this receiver detaches. The hook is invoked after every event lock ends.
    pub(super) fn on_receiver_closed(&self, hook: Arc<dyn Fn() + Send + Sync>) -> Result<()> {
        let mut state = lock(&self.state);
        if !state.active {
            return Err(NetError::from(ErrorKind::Closed));
        }
        if state.detach_hook.is_some() {
            return Err(NetError::input(
                "subscription.detach_hook",
                "a detach hook is already installed",
            ));
        }
        state.detach_hook = Some(hook);
        Ok(())
    }
    #[cfg(all(test, feature = "ws-client"))]
    pub(super) fn after_pending_for_test(&self, hook: impl FnOnce() + Send + 'static) {
        let previous = lock(&self.after_pending).replace(Box::new(hook));
        drop(previous);
    }
    #[cfg(all(test, feature = "ws-client"))]
    fn run_pending_hook(&self) {
        let hook = lock(&self.after_pending).take();
        if let Some(hook) = hook {
            hook();
        }
    }
    pub(super) fn unsubscribe(&self) -> bool {
        let retired = {
            let mut state = lock(&self.state);
            if !state.active {
                return false;
            }
            state.active = false;
            state.bytes = 0;
            state.lagged = 0;
            let close = if state.running {
                None
            } else {
                state.close_waker.take()
            };
            (
                std::mem::take(&mut state.queue),
                state.pending_error.take(),
                state.callback.take(),
                state.driver.take(),
                state.executor.take(),
                state.permit.take(),
                state.receive_waker.take(),
                close,
                state.detach_hook.take(),
            )
        };
        let (queue, error, callback, driver, executor, permit, receive, close, detach) = retired;
        // Detach is already committed. Do not make its producer notification
        // depend on a queued value's or callback capture's destructor returning.
        self.receiver_closed.notify_waiters();
        if let Some(detach) = detach {
            detach();
        }
        drop((queue, error, callback, driver, executor, permit));
        self.capacity_available.notify_waiters();
        if let Some(waker) = receive {
            waker.wake();
        }
        if let Some(waker) = close {
            waker.wake();
        }
        true
    }
    pub(super) fn poll_next(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<std::result::Result<T, ReceiveError>>> {
        let new_waker = cx.waker().clone();
        let (delivery, old) = {
            let mut state = lock(&self.state);
            let delivery = state.next();
            let old = if matches!(delivery, Delivery::Pending) {
                state.receive_waker.replace(new_waker)
            } else {
                Some(new_waker)
            };
            (delivery, old)
        };
        drop(old);
        self.deliver(delivery)
    }
    pub(super) fn try_recv(&self) -> std::result::Result<T, TryReceiveError> {
        let delivery = lock(&self.state).next();
        match self.deliver(delivery) {
            Poll::Pending => Err(TryReceiveError::Empty),
            Poll::Ready(None) => Err(TryReceiveError::Closed),
            Poll::Ready(Some(Ok(value))) => Ok(value),
            Poll::Ready(Some(Err(ReceiveError::Lagged { skipped }))) => {
                Err(TryReceiveError::Lagged { skipped })
            }
            Poll::Ready(Some(Err(ReceiveError::Failed(error)))) => {
                Err(TryReceiveError::Failed(error))
            }
        }
    }
    fn deliver(&self, delivery: Delivery<T>) -> Poll<Option<std::result::Result<T, ReceiveError>>> {
        match delivery {
            Delivery::Item(item) => {
                self.capacity_available.notify_waiters();
                Poll::Ready(Some(item.map(Entry::into_value)))
            }
            Delivery::Done => {
                self.unsubscribe();
                Poll::Ready(None)
            }
            Delivery::Pending => Poll::Pending,
        }
    }
    fn schedule(&self) -> Result<()> {
        let driver = {
            let mut state = lock(&self.state);
            if !state.active || state.scheduled || !state.pending() {
                return Ok(());
            }
            let Some(driver) = state.driver.clone() else {
                return Ok(());
            };
            state.scheduled = true;
            driver
        };
        if let Err(error) = driver() {
            self.fail(error.clone());
            return Err(error);
        }
        Ok(())
    }
    fn fail(&self, error: NetError) {
        {
            let mut state = lock(&self.state);
            if state.callback_installing {
                state
                    .callback_install_error
                    .get_or_insert_with(|| error.clone());
                return;
            }
            state.final_error.get_or_insert_with(|| error.clone());
        }
        crate::log_e!(crate::common::log::log_def::LogType::Common;
            "event_subscription", "id|kind", self.id.as_u64(), format!("{:?}", error.kind()));
        self.unsubscribe();
    }
    fn poll_close(&self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let new_waker = cx.waker().clone();
        let (result, old) = {
            let mut state = lock(&self.state);
            if state.running {
                (Poll::Pending, state.close_waker.replace(new_waker))
            } else {
                (
                    Poll::Ready(state.final_error.clone().map_or(Ok(()), Err)),
                    Some(new_waker),
                )
            }
        };
        drop(old);
        result
    }
}

impl<T: Send + Sync + 'static> EventRegistration<T> {
    #[cfg(test)]
    pub(super) fn fail_callback_for_test(&self) -> bool {
        self.complete_callback(Err(Box::new("injected callback failure")))
    }
    pub(super) fn install_callback(
        self: &Arc<Self>,
        callback: impl Fn(CallbackContext, std::result::Result<T, ReceiveError>) + Send + Sync + 'static,
    ) -> Result<()> {
        let executor = lock(&self.state)
            .executor
            .clone()
            .ok_or_else(|| NetError::from(ErrorKind::Closed))?;
        executor.ensure_ready()?;
        let callback: Arc<Callback<T>> = Arc::new(callback);
        let weak = Arc::downgrade(self);
        let permit = lock(&self.state).permit.clone();
        let driver: Arc<Driver> = Arc::new(move || {
            let weak = weak.clone();
            let abandoned = weak.clone();
            let permit = permit.clone();
            super::dispatch::submit_tracked(
                executor.as_ref(),
                Box::new(move || {
                    let _permit = permit;
                    if let Some(registration) = weak.upgrade() {
                        registration.run();
                    }
                }),
                move |error| {
                    if let Some(registration) = abandoned.upgrade() {
                        if registration.is_active() {
                            registration.fail(error);
                        }
                    }
                },
            )
        });
        {
            let mut state = lock(&self.state);
            if !state.active {
                return Err(NetError::from(ErrorKind::Closed));
            }
            if state.callback.is_some() || state.callback_installing {
                return Err(NetError::input(
                    "subscription.callback",
                    "callback already installed",
                ));
            }
            state.pristine = false;
            state.callback_installing = true;
            state.callback = Some(callback.clone());
            state.driver = Some(driver.clone());
        }
        let mut schedule_error = self.schedule().err();
        let (failure, retired) = {
            let mut state = lock(&self.state);
            if state.callback_installing {
                state.callback_installing = false;
                let failure = match state.callback_install_error.take() {
                    Some(error) => Some(error),
                    None => schedule_error.take(),
                };
                let retired = if failure.is_some() {
                    state.scheduled = false;
                    (state.callback.take(), state.driver.take())
                } else {
                    (None, None)
                };
                (failure, retired)
            } else {
                // A runner already started: registration succeeded, and any
                // later callback failure belongs to the returned subscription.
                (None, (None, None))
            }
        };
        drop(retired);
        failure.map_or(Ok(()), Err)
    }
    fn run(self: Arc<Self>) {
        {
            let mut state = lock(&self.state);
            if !state.active {
                state.scheduled = false;
                return;
            }
            state.callback_installing = false;
            state.running = true;
        }
        let mut completion = RunnerCompletion {
            registration: &self,
            armed: true,
        };
        let outcome = catch_unwind(AssertUnwindSafe(|| self.run_callbacks(&mut completion)));
        self.complete_runner(outcome);
    }
    fn run_callbacks(self: &Arc<Self>, completion: &mut RunnerCompletion<'_, T>) {
        loop {
            let (delivery, callback, close) = {
                let mut state = lock(&self.state);
                let delivery = state.next();
                let callback = if matches!(delivery, Delivery::Item(_)) {
                    state.callback.clone()
                } else {
                    None
                };
                let close = if matches!(delivery, Delivery::Pending) {
                    // Publish and idle retirement share this lock. The old guard
                    // cannot retire a successor admitted after this transition.
                    state.running = false;
                    state.scheduled = false;
                    completion.armed = false;
                    state.close_waker.take()
                } else {
                    None
                };
                (delivery, callback, close)
            };
            if let Some(waker) = close {
                waker.wake();
            }
            let outcome = match (delivery, callback) {
                (Delivery::Item(item), Some(callback)) => {
                    self.capacity_available.notify_waiters();
                    let item = item.map(Entry::into_value);
                    let control: Arc<dyn SubscriptionControl> = self.clone();
                    let context = CallbackContext {
                        id: self.id,
                        control: Arc::downgrade(&control),
                    };
                    catch_unwind(AssertUnwindSafe(|| callback(context, item)))
                }
                (Delivery::Done, _) => {
                    self.unsubscribe();
                    return;
                }
                (Delivery::Pending, _) => {
                    #[cfg(all(test, feature = "ws-client"))]
                    self.run_pending_hook();
                    return;
                }
                (_, None) => return,
            };
            if !self.complete_callback(outcome) {
                return;
            }
        }
    }
    fn complete_callback(&self, outcome: std::thread::Result<()>) -> bool {
        match outcome {
            Ok(()) => true,
            Err(payload) => {
                self.fail(callback_fault());
                drop(payload);
                false
            }
        }
    }
    fn complete_runner(&self, outcome: std::thread::Result<()>) {
        if let Err(payload) = outcome {
            let cleanup = catch_unwind(AssertUnwindSafe(|| {
                self.fail(callback_fault());
                drop(payload);
            }));
            if let Err(secondary) = cleanup {
                discard_cleanup_panic(self.id, secondary);
            }
        }
    }
    fn finish_run(&self) {
        let waker = {
            let mut state = lock(&self.state);
            state.running = false;
            state.scheduled = false;
            state.close_waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        let _ = self.schedule();
    }
}
impl<T: Send + Sync + 'static> SubscriptionControl for EventRegistration<T> {
    fn is_active(&self) -> bool {
        self.is_active()
    }
    fn unsubscribe(&self) -> bool {
        self.unsubscribe()
    }
    fn poll_close(&self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.poll_close(cx)
    }
}
fn callback_fault() -> NetError {
    NetError::from(ErrorKind::CallbackPanicked).with_stage(crate::error::ErrorStage::Dispatch)
}
struct RunnerCompletion<'a, T: Send + Sync + 'static> {
    registration: &'a EventRegistration<T>,
    armed: bool,
}
impl<T: Send + Sync + 'static> Drop for RunnerCompletion<'_, T> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if std::thread::panicking() {
            let cleanup = catch_unwind(AssertUnwindSafe(|| {
                self.registration.fail(callback_fault())
            }));
            if let Err(secondary) = cleanup {
                discard_cleanup_panic(self.registration.id, secondary);
            }
        }
        let completion = catch_unwind(AssertUnwindSafe(|| self.registration.finish_run()));
        if let Err(secondary) = completion {
            let cleanup = catch_unwind(AssertUnwindSafe(|| {
                self.registration.fail(callback_fault())
            }));
            if let Err(payload) = cleanup {
                discard_cleanup_panic(self.registration.id, payload);
            }
            discard_cleanup_panic(self.registration.id, secondary);
        }
    }
}
fn discard_cleanup_panic(id: SubscriptionId, payload: Box<dyn std::any::Any + Send>) {
    crate::log_e!(crate::common::log::log_def::LogType::Common;
        "event_subscription_cleanup", "id|kind", id.as_u64(), format!("{:?}", ErrorKind::CallbackPanicked));
    // Only an exception during cleanup is leaked to avoid a destructor panicking again.
    // Normal event values, callback captures and the first caught payload are released.
    std::mem::forget(payload);
}

#[cfg(test)]
#[path = "event_callback_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "event_prepare_tests.rs"]
mod event_prepare_tests;

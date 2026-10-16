//! One logical observer, one receiver, and at most one admitted callback.
//! Recovery is owned by the SDK runtime; callback work stays on its callback
//! pool. A cancelled driver never grants admission to a queued callback.
use crate::common::retirement::WorkLease;
use crate::error::{ErrorKind, ErrorStage, NetError, ReceiveError};
use crate::net_status::{NetworkEvent, NetworkEventKind, NetworkObservation};
use crate::subscription::{
    lock, submit_tracked, CallbackContext, CallbackExecutor, CallbackScope, EventReceiver,
    Subscription, SubscriptionControl, SubscriptionId,
};
use crate::Result;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;
use tokio::sync::{oneshot, Notify};

type Callback = dyn Fn(CallbackContext, Result<NetworkObservation>) + Send + Sync + 'static;
type Subscribe = dyn Fn() -> Result<EventReceiver<NetworkEvent>> + Send + Sync + 'static;

struct State {
    active: bool,
    driver_done: bool,
    callback_running: bool,
    receiver: Option<Arc<dyn SubscriptionControl>>,
    callback: Option<Arc<Callback>>,
    error: Option<NetError>,
    close_waker: Option<Waker>,
}

struct Control {
    id: SubscriptionId,
    state: Mutex<State>,
    cancelled: Notify,
}

fn dispatch_error(kind: ErrorKind) -> NetError {
    NetError::from(kind).with_stage(ErrorStage::Dispatch)
}

pub(super) fn subscribe<F>(
    receiver: EventReceiver<NetworkEvent>,
    subscribe: Arc<Subscribe>,
    executor: Arc<dyn CallbackExecutor>,
    runtime: tokio::runtime::Handle,
    retirement: Option<WorkLease>,
    callback: F,
) -> Result<Subscription>
where
    F: Fn(CallbackContext, Result<NetworkObservation>) + Send + Sync + 'static,
{
    executor.ensure_ready()?;
    let control = Arc::new(Control {
        id: receiver.id(),
        state: Mutex::new(State {
            active: true,
            driver_done: false,
            callback_running: false,
            receiver: Some(receiver.control()),
            callback: Some(Arc::new(callback)),
            error: None,
            close_waker: None,
        }),
        cancelled: Notify::new(),
    });
    // Capture completion before constructing/spawning the future: even a task
    // dropped without one poll must retire its control and wake close().
    let completion = DriverCompletion {
        control: control.clone(),
        finished: false,
    };
    let driver = control.clone();
    runtime.spawn(async move {
        let _retirement = retirement;
        // Move the whole Drop guard into the future. Capturing only its
        // `finished` field lets Rust 2021 retire the outer guard immediately.
        let mut completion = completion;
        use futures_util::FutureExt;
        let outcome = AssertUnwindSafe(run(&driver, receiver, subscribe, &executor))
            .catch_unwind()
            .await;
        let failure = match outcome {
            Ok(result) => result.err(),
            Err(payload) => {
                discard_panic(payload);
                Some(dispatch_error(ErrorKind::Internal))
            }
        };
        if let Some(error) = failure {
            driver.record_error(error.clone());
            // An executor fault or callback panic has already revoked
            // admission. Never recursively invoke a failed callback to report
            // that callback's own panic or retry a failed dispatcher.
            if driver.is_active() {
                let _ = driver.deliver(&executor, Err(error)).await;
            }
        }
        completion.finished = true;
        driver.finish_driver(None);
    });
    Ok(Subscription::from_control(control.id, control))
}

impl Control {
    fn record_error(&self, error: NetError) {
        let kind = error.kind();
        lock(&self.state).error.get_or_insert(error);
        crate::log_e!(crate::common::log::log_def::LogType::Common;
            "recoverable_subscription", "id|kind", self.id.as_u64(), format!("{kind:?}"));
    }

    async fn cancelled(&self) {
        loop {
            let changed = self.cancelled.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if !self.is_active() {
                return;
            }
            changed.await;
        }
    }

    fn install_receiver(&self, receiver: &EventReceiver<NetworkEvent>) -> bool {
        let next = receiver.control();
        let previous = {
            let mut state = lock(&self.state);
            if !state.active {
                return false;
            }
            state.receiver.replace(next)
        };
        if let Some(previous) = previous {
            previous.unsubscribe();
        }
        true
    }

    fn clear_receiver(&self) {
        let receiver = lock(&self.state).receiver.take();
        if let Some(receiver) = receiver {
            receiver.unsubscribe();
        }
    }

    async fn deliver(
        self: &Arc<Self>,
        executor: &Arc<dyn CallbackExecutor>,
        value: Result<NetworkObservation>,
    ) -> Result<bool> {
        if !self.is_active() {
            return Ok(false);
        }
        let (sender, received) = oneshot::channel();
        let runner = self.clone();
        let abandoned = self.clone();
        let submitted = submit_tracked(
            executor.as_ref(),
            Box::new(move || runner.invoke(value, sender)),
            move |error| {
                if abandoned.is_active() {
                    abandoned.record_error(error);
                    abandoned.unsubscribe();
                }
            },
        );
        if let Err(error) = submitted {
            self.record_error(error.clone());
            self.unsubscribe();
            return Err(error);
        }
        tokio::select! {
            biased;
            _ = self.cancelled() => Ok(false),
            result = received => match result {
                Ok(result) => result,
                Err(_) => {
                    let error = dispatch_error(ErrorKind::RuntimeUnavailable);
                    self.record_error(error.clone());
                    self.unsubscribe();
                    Err(error)
                }
            },
        }
    }

    fn invoke(
        self: Arc<Self>,
        value: Result<NetworkObservation>,
        sender: oneshot::Sender<Result<bool>>,
    ) {
        let callback = {
            let mut state = lock(&self.state);
            if !state.active {
                return;
            }
            let Some(callback) = state.callback.clone() else {
                return;
            };
            // Only the single driver dispatches, and it waits for this
            // invocation before admitting another value or a replacement.
            state.callback_running = true;
            callback
        };
        let completion = CallbackCompletion(self.clone());
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            let _scope = CallbackScope::enter();
            let control: Arc<dyn SubscriptionControl> = self.clone();
            let context = CallbackContext::from_control(self.id, &control);
            callback(context, value);
            drop(callback);
        }));
        let result = match outcome {
            Ok(()) => Ok(true),
            Err(payload) => {
                let error = dispatch_error(ErrorKind::CallbackPanicked);
                self.record_error(error.clone());
                self.unsubscribe();
                discard_panic(payload);
                Err(error)
            }
        };
        // Publish callback retirement before waking the driver. A successor
        // cannot overlap the invocation or accidentally be retired by its guard.
        drop(completion);
        let _ = sender.send(result);
    }

    fn finish_driver(&self, unexpected: Option<NetError>) {
        if let Some(error) = unexpected {
            self.record_error(error);
        }
        let (receiver, callback, wake) = {
            let mut state = lock(&self.state);
            state.active = false;
            state.driver_done = true;
            let wake = if state.callback_running {
                None
            } else {
                state.close_waker.take()
            };
            (state.receiver.take(), state.callback.take(), wake)
        };
        self.cancelled.notify_waiters();
        if let Some(receiver) = receiver {
            receiver.unsubscribe();
        }
        if let Some(wake) = wake {
            wake.wake();
        }
        drop(callback);
    }
}

impl SubscriptionControl for Control {
    fn is_active(&self) -> bool {
        lock(&self.state).active
    }

    fn unsubscribe(&self) -> bool {
        let (was_active, receiver, callback) = {
            let mut state = lock(&self.state);
            let was_active = state.active;
            state.active = false;
            (was_active, state.receiver.take(), state.callback.take())
        };
        self.cancelled.notify_waiters();
        if let Some(receiver) = receiver {
            receiver.unsubscribe();
        }
        // Application captures must never be destroyed under the control lock.
        drop(callback);
        was_active
    }

    fn poll_close(&self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let new_waker = cx.waker().clone();
        let (result, retired) = {
            let mut state = lock(&self.state);
            if state.driver_done && !state.callback_running {
                (
                    Poll::Ready(state.error.clone().map_or(Ok(()), Err)),
                    Some(new_waker),
                )
            } else {
                (Poll::Pending, state.close_waker.replace(new_waker))
            }
        };
        drop(retired);
        result
    }
}

struct DriverCompletion {
    control: Arc<Control>,
    finished: bool,
}
impl Drop for DriverCompletion {
    fn drop(&mut self) {
        if !self.finished {
            let error = self
                .control
                .is_active()
                .then(|| dispatch_error(ErrorKind::RuntimeUnavailable));
            self.control.finish_driver(error);
        }
    }
}

struct CallbackCompletion(Arc<Control>);
impl Drop for CallbackCompletion {
    fn drop(&mut self) {
        let wake = {
            let mut state = lock(&self.0.state);
            state.callback_running = false;
            if state.driver_done {
                state.close_waker.take()
            } else {
                None
            }
        };
        if let Some(wake) = wake {
            wake.wake();
        }
    }
}

fn discard_panic(payload: Box<dyn std::any::Any + Send>) {
    if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
        // A user panic payload may itself panic in Drop. Do not let it abort
        // retirement or bring down the shared callback executor.
        std::mem::forget(secondary);
    }
}

async fn run(
    control: &Arc<Control>,
    mut receiver: EventReceiver<NetworkEvent>,
    subscribe: Arc<Subscribe>,
    executor: &Arc<dyn CallbackExecutor>,
) -> Result<()> {
    let mut previous: Option<NetworkEvent> = None;
    let mut reconcile = false;
    let mut recovery_delay = Duration::from_millis(10);
    loop {
        let received = tokio::select! {
            biased;
            _ = control.cancelled() => break,
            value = receiver.recv() => value,
        };
        match received {
            Ok(Some(event)) => {
                if !matches!(event.kind, NetworkEventKind::Initial) {
                    recovery_delay = Duration::from_millis(10);
                }
                let value = if reconcile {
                    let previous = previous
                        .take()
                        .ok_or_else(|| NetError::from(ErrorKind::Internal))?;
                    NetworkObservation::Reconciled {
                        previous,
                        current: event.clone(),
                    }
                } else {
                    NetworkObservation::Event(event.clone())
                };
                if !control.deliver(executor, Ok(value)).await? {
                    break;
                }
                previous = Some(event);
                reconcile = false;
            }
            Ok(None) => break,
            Err(ReceiveError::Failed(error)) if error.kind() == ErrorKind::CallbackOverflow => {
                // The Initial and every retained entry precede this error, so
                // previous always exists after a recoverable overflow.
                control.clear_receiver();
                receiver.unsubscribe();
                tokio::select! {
                    biased;
                    _ = control.cancelled() => break,
                    _ = tokio::time::sleep(recovery_delay) => {},
                }
                recovery_delay = (recovery_delay * 2).min(Duration::from_millis(250));
                if !control.is_active() {
                    break;
                }
                let replacement = subscribe()?;
                // Cancellation may have raced construction. It permanently
                // wins publication, and close waits until this driver drops
                // any such unpublished registration.
                if !control.install_receiver(&replacement) {
                    break;
                }
                receiver = replacement;
                reconcile = true;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "recoverable_tests.rs"]
mod tests;

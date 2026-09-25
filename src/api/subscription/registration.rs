use super::source::SourceCore;
use super::*;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::task::{Context, Poll, Waker};
use tokio::sync::OwnedSemaphorePermit;

type Callback<T> = dyn Fn(CallbackContext, Result<T>) + Send + Sync + 'static;
type Driver = dyn Fn() -> Result<()> + Send + Sync + 'static;

pub(super) struct Registration<T> {
    pub(super) id: SubscriptionId,
    source: Weak<SourceCore<T>>,
    pub(super) state: Mutex<RegistrationState<T>>,
    #[cfg(all(test, feature = "ws-client"))]
    after_pending: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}
pub(super) struct RegistrationState<T> {
    executor: Option<Arc<dyn CallbackExecutor>>,
    active: bool,
    initial: Option<Arc<T>>,
    initial_revision: u64,
    latest: Arc<T>,
    revision: u64,
    seen: u64,
    source_closed: bool,
    pending_error: Option<NetError>,
    final_error: Option<NetError>,
    callback: Option<Arc<Callback<T>>>,
    driver: Option<Arc<Driver>>,
    scheduled: bool,
    running: bool,
    permit: Option<Arc<OwnedSemaphorePermit>>,
    receive_waker: Option<Waker>,
    close_waker: Option<Waker>,
}
enum Delivery<T> {
    Value(Arc<T>),
    Error(NetError),
    Done,
    Pending,
}
impl<T> RegistrationState<T> {
    fn next(&mut self) -> Delivery<T> {
        if !self.active {
            return Delivery::Done;
        }
        if let Some(initial) = self.initial.take() {
            self.seen = self.initial_revision;
            return Delivery::Value(initial);
        }
        if self.seen != self.revision {
            self.seen = self.revision;
            return Delivery::Value(self.latest.clone());
        }
        if let Some(error) = self.pending_error.take() {
            return Delivery::Error(error);
        }
        if self.source_closed {
            Delivery::Done
        } else {
            Delivery::Pending
        }
    }
    fn pending(&self) -> bool {
        self.initial.is_some()
            || self.seen != self.revision
            || self.pending_error.is_some()
            || self.source_closed
    }
}
impl<T> Registration<T> {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        id: SubscriptionId,
        source: Weak<SourceCore<T>>,
        executor: Arc<dyn CallbackExecutor>,
        current: Arc<T>,
        revision: u64,
        closed: bool,
        failure: Option<NetError>,
        permit: OwnedSemaphorePermit,
    ) -> Self {
        Self {
            id,
            source,
            #[cfg(all(test, feature = "ws-client"))]
            after_pending: Mutex::new(None),
            state: Mutex::new(RegistrationState {
                executor: Some(executor),
                active: true,
                initial: Some(current.clone()),
                initial_revision: revision,
                latest: current,
                revision,
                seen: revision,
                source_closed: closed,
                pending_error: failure.clone(),
                final_error: failure,
                callback: None,
                driver: None,
                scheduled: false,
                running: false,
                permit: Some(Arc::new(permit)),
                receive_waker: None,
                close_waker: None,
            }),
        }
    }
    pub(super) fn is_active(&self) -> bool {
        lock(&self.state).active
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
    pub(super) fn update(
        &self,
        value: Arc<T>,
        revision: u64,
        closed: bool,
        failure: Option<NetError>,
    ) {
        let retired = {
            let mut state = lock(&self.state);
            if !state.active || state.source_closed || revision < state.revision {
                return;
            }
            let old = std::mem::replace(&mut state.latest, value.clone());
            state.revision = revision;
            state.source_closed = closed;
            let old_pending = std::mem::replace(&mut state.pending_error, failure.clone());
            let old_error = std::mem::replace(&mut state.final_error, failure.clone());
            (old, old_pending, old_error, state.receive_waker.take())
        };
        let (old, old_pending, old_error, waker) = retired;
        drop((old, old_pending, old_error));
        if let Some(waker) = waker {
            waker.wake();
        }
        let _ = self.schedule();
    }
    pub(super) fn unsubscribe(&self) -> bool {
        let retired = {
            let mut state = lock(&self.state);
            if !state.active {
                return false;
            }
            state.active = false;
            let close_waker = if state.running {
                None
            } else {
                state.close_waker.take()
            };
            (
                state.initial.take(),
                state.pending_error.take(),
                state.callback.take(),
                state.driver.take(),
                state.executor.take(),
                state.permit.take(),
                state.receive_waker.take(),
                close_waker,
            )
        };
        if let Some(source) = self.source.upgrade() {
            source.remove(self.id);
        }
        let (initial, error, callback, driver, executor, permit, receive, close) = retired;
        drop((initial, error, callback, driver, executor, permit));
        if let Some(waker) = receive {
            waker.wake();
        }
        if let Some(waker) = close {
            waker.wake();
        }
        true
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
            state.final_error.get_or_insert_with(|| error.clone());
        }
        crate::log_e!(crate::common::log::log_def::LogType::Common; "subscription", "id|kind", self.id.as_u64(), format!("{:?}", error.kind()));
        self.unsubscribe();
    }
    pub(super) fn poll_close(&self, cx: &mut Context<'_>) -> Poll<Result<()>> {
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
impl<T: Clone> Registration<T> {
    pub(super) fn current(&self) -> T {
        let value = lock(&self.state).latest.clone();
        (*value).clone()
    }
    pub(super) fn poll_next(&self, cx: &mut Context<'_>) -> Poll<Option<Result<T>>> {
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
        match delivery {
            Delivery::Value(value) => Poll::Ready(Some(Ok((*value).clone()))),
            Delivery::Error(error) => Poll::Ready(Some(Err(error))),
            Delivery::Done => {
                self.unsubscribe();
                Poll::Ready(None)
            }
            Delivery::Pending => Poll::Pending,
        }
    }
}
impl<T: Clone + Send + Sync + 'static> Registration<T> {
    pub(super) fn install_callback(
        self: &Arc<Self>,
        callback: impl Fn(CallbackContext, Result<T>) + Send + Sync + 'static,
    ) -> Result<()> {
        let executor = lock(&self.state)
            .executor
            .clone()
            .ok_or_else(|| NetError::from(ErrorKind::Closed))?;
        executor.ensure_ready()?;
        let callback: Arc<Callback<T>> = Arc::new(callback);
        let weak = Arc::downgrade(self);
        let permit = lock(&self.state)
            .permit
            .clone()
            .ok_or_else(|| NetError::from(ErrorKind::Closed))?;
        let driver: Arc<Driver> = Arc::new(move || {
            let weak = weak.clone();
            let abandoned = weak.clone();
            // A cancelled, queued job still occupies capacity until the existing pool retires it.
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
            state.callback = Some(callback.clone());
            state.driver = Some(driver.clone());
        }
        self.schedule()
    }
    fn run(self: Arc<Self>) {
        {
            let mut state = lock(&self.state);
            if !state.active {
                state.scheduled = false;
                return;
            }
            state.running = true;
        }
        let mut completion = RunnerCompletion {
            registration: &self,
            armed: true,
        };
        // The outer boundary also covers callback captures and caught-panic payload destructors.
        let outcome = catch_unwind(AssertUnwindSafe(|| self.run_callbacks(&mut completion)));
        self.complete_runner(outcome);
    }
    fn run_callbacks(self: &Arc<Self>, completion: &mut RunnerCompletion<'_, T>) {
        loop {
            let (delivery, callback, close) = {
                let mut state = lock(&self.state);
                let delivery = state.next();
                let callback = match delivery {
                    Delivery::Value(_) | Delivery::Error(_) => state.callback.clone(),
                    Delivery::Done | Delivery::Pending => None,
                };
                let close = if matches!(delivery, Delivery::Pending) {
                    // The idle decision and retirement are one transition. A later
                    // publication can submit its successor before the pool closes.
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
                (Delivery::Value(value), Some(callback)) => {
                    let context = self.context();
                    catch_unwind(AssertUnwindSafe(|| callback(context, Ok((*value).clone()))))
                }
                (Delivery::Error(error), Some(callback)) => {
                    let context = self.context();
                    catch_unwind(AssertUnwindSafe(|| callback(context, Err(error))))
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
    pub(super) fn complete_callback(&self, outcome: std::thread::Result<()>) -> bool {
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
    fn context(self: &Arc<Self>) -> CallbackContext {
        let control: Arc<dyn SubscriptionControl> = self.clone();
        CallbackContext {
            id: self.id,
            control: Arc::downgrade(&control),
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
impl<T: Clone + Send + Sync + 'static> SubscriptionControl for Registration<T> {
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

// Borrowing keeps the completion guard itself from owning a final user snapshot or capture.
struct RunnerCompletion<'a, T: Clone + Send + Sync + 'static> {
    registration: &'a Registration<T>,
    armed: bool,
}
impl<T: Clone + Send + Sync + 'static> Drop for RunnerCompletion<'_, T> {
    fn drop(&mut self) {
        // A normal idle runner already retired under the state lock. Its old
        // guard must never clear flags belonging to a concurrently started job.
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
        "subscription_cleanup", "id|kind", id.as_u64(), format!("{:?}", ErrorKind::CallbackPanicked));
    // Only a second panic caught while retiring a runner is leaked: its destructor may panic
    // again. Normal callback captures, state values and the first caught payload are released.
    std::mem::forget(payload);
}

#[cfg(all(test, feature = "ws-client"))]
#[path = "runner_retirement_tests.rs"]
mod runner_retirement_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct HeldExecutor {
        jobs: Mutex<Vec<Box<dyn FnOnce() + Send>>>,
    }
    impl CallbackExecutor for HeldExecutor {
        fn ensure_ready(&self) -> Result<()> {
            Ok(())
        }
        fn submit(&self, job: Box<dyn FnOnce() + Send>) -> Result<()> {
            let mut jobs = lock(&self.jobs);
            jobs.try_reserve(1)
                .map_err(|error| NetError::with_source(ErrorKind::ResourceExhausted, error))?;
            jobs.push(job);
            Ok(())
        }
    }

    #[derive(Default)]
    struct RetiringExecutor {
        job: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    }
    impl CallbackExecutor for RetiringExecutor {
        fn ensure_ready(&self) -> Result<()> {
            Ok(())
        }
        fn submit(&self, job: Box<dyn FnOnce() + Send>) -> Result<()> {
            let old = lock(&self.job).replace(job);
            drop(old);
            Ok(())
        }
    }

    #[test]
    fn accepted_state_job_retired_without_execution_ends_subscription(
    ) -> std::result::Result<(), crate::BoxError> {
        let executor = Arc::new(RetiringExecutor::default());
        let (_publisher, source) = StateSource::new(1, executor.clone(), 1)?;
        let subscription = source.subscribe()?.into_callback(|_, _| {})?;
        let queued = lock(&executor.job).take();
        drop(queued);
        if subscription.is_active() {
            return Err("retired job left a subscription waiting forever".into());
        }
        let error = futures::executor::block_on(subscription.close())
            .err()
            .ok_or("retired job lost its dispatch failure")?;
        if error.kind() != ErrorKind::RuntimeUnavailable
            || error.context().stage != Some(crate::error::ErrorStage::Dispatch)
        {
            return Err(format!("incorrect retired job error: {error:?}").into());
        }
        drop(source.subscribe()?);
        Ok(())
    }

    #[test]
    fn completion_guard_retires_running_callback_after_caught_runner_fault(
    ) -> std::result::Result<(), crate::BoxError> {
        let (_publisher, source) = StateSource::new(1, Arc::new(HeldExecutor::default()), 1)?;
        let receiver = source.subscribe()?;
        let registration = receiver.registration.clone();
        let subscription = receiver.into_callback(|_, _| {})?;
        {
            let mut state = lock(&registration.state);
            state.running = true;
        }
        {
            let _completion = RunnerCompletion {
                registration: &registration,
                armed: true,
            };
            // Feed the outer catch boundary's error branch without generating an actual panic.
            registration.complete_runner(Err(Box::new("captured runner fault")));
        }
        let mut cx = Context::from_waker(futures::task::noop_waker_ref());
        if !matches!(registration.poll_close(&mut cx), Poll::Ready(Err(error)) if error.kind() == ErrorKind::CallbackPanicked)
        {
            return Err("runner fault left close pending or lost its error".into());
        }
        if subscription.is_active() {
            return Err("runner fault left delivery active".into());
        }
        if futures::executor::block_on(subscription.close()).is_ok() {
            return Err("close swallowed runner fault".into());
        }
        Ok(())
    }
}

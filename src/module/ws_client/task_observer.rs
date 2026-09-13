//! Task completion observation independent of the network runtime.

use crate::api::wsc::web_socket_task_event::{
    WebSocketTaskDelivery as Delivery, WebSocketTaskEndCause as Cause, WebSocketTaskEvent as Event,
    WebSocketTaskEventOptions as EventOptions, WebSocketTaskPhase as Phase,
    WebSocketTaskSource as Source, WebSocketTaskSuccess as Success,
};
use crate::common::log::log_def::LogType;
use crate::NetError;
use std::io;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{mpsc, Arc, Mutex, MutexGuard, OnceLock};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tokio_util::sync::CancellationToken;

type Listener = Box<dyn Fn(Event) + Send + Sync + 'static>;

pub(crate) struct TaskObserverStore {
    instance_id: u64,
    state: Mutex<ObserverStoreState>,
}

struct ObserverStoreState {
    closed: bool,
    registration: Option<Arc<TaskListenerRegistration>>,
}

struct CapacityLane {
    tasks: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    max_bytes: usize,
}

pub(crate) struct TaskListenerRegistration {
    sender: mpsc::Sender<DispatchedEvent>,
    ordinary: CapacityLane,
    urgent: Option<CapacityLane>,
}

/// A task reserves notification space before the engine accepts ownership. The sender keeps
/// its original listener thread alive through replacement, unregister and client shutdown.
pub(crate) struct TaskEventPermit {
    sender: mpsc::Sender<DispatchedEvent>,
    task_slot: OwnedSemaphorePermit,
    byte_slots: OwnedSemaphorePermit,
}

struct DispatchedEvent {
    event: Event,
    task_slot: OwnedSemaphorePermit,
    byte_slots: OwnedSemaphorePermit,
}

pub(crate) struct TaskObservation {
    state: Mutex<Option<ObservationState>>,
    selected_result: OnceLock<Result<Success, NetError>>,
}

struct ObservationState {
    event: Event,
    cause: Option<Cause>,
    permit: TaskEventPermit,
}

impl TaskObserverStore {
    pub(crate) fn new(instance_id: u64) -> Self {
        Self {
            instance_id,
            state: Mutex::new(ObserverStoreState {
                closed: false,
                registration: None,
            }),
        }
    }

    pub(crate) fn register(
        &self,
        listener: Listener,
        options: EventOptions,
    ) -> Result<(), NetError> {
        self.register_with(listener, options, |task| {
            std::thread::Builder::new()
                .name(format!("open-net-task-events-{}", self.instance_id))
                .spawn(task)
                .map(drop)
        })
    }

    /// Spawn is injectable so resource exhaustion can be tested without exhausting OS threads.
    fn register_with<S>(
        &self,
        listener: Listener,
        options: EventOptions,
        spawn: S,
    ) -> Result<(), NetError>
    where
        S: FnOnce(Box<dyn FnOnce() + Send>) -> io::Result<()>,
    {
        crate::log_t!(LogType::WSC; "register_task_listener", "instance_id", self.instance_id);
        if self.lock_state()?.closed {
            return Err(NetError::EngineDropped);
        }
        let replacement = Arc::new(TaskListenerRegistration::new(listener, options, spawn)?);
        let previous = {
            let mut state = self.lock_state()?;
            if state.closed {
                // The new thread owns user code. Dropping its sender after the lock is released
                // lets captured values reenter this store without deadlocking shutdown.
                drop(state);
                drop(replacement);
                return Err(NetError::EngineDropped);
            }
            state.registration.replace(replacement)
        };
        drop(previous);
        Ok(())
    }

    pub(crate) fn unregister(&self) -> Result<(), NetError> {
        crate::log_t!(LogType::WSC; "unregister_task_listener", "instance_id", self.instance_id);
        let previous = self.lock_state()?.registration.take();
        drop(previous);
        Ok(())
    }

    /// Prevents new registrations. Existing task permits retain their listener independently.
    pub(crate) fn close(&self) {
        crate::log_t!(LogType::WSC; "close_task_listener", "instance_id", self.instance_id);
        let previous = {
            let mut state = match self.state.lock() {
                Ok(state) => state,
                Err(poisoned) => {
                    crate::log_e!(LogType::WSC; "close_task_listener", "error", "store_lock_poisoned_recovered");
                    poisoned.into_inner()
                }
            };
            state.closed = true;
            state.registration.take()
        };
        drop(previous);
    }

    pub(crate) fn snapshot(&self) -> Result<Option<Arc<TaskListenerRegistration>>, NetError> {
        let state = self.lock_state()?;
        if state.closed {
            return Err(NetError::EngineDropped);
        }
        Ok(state.registration.clone())
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, ObserverStoreState>, NetError> {
        self.state.lock().map_err(|_| {
            crate::log_e!(LogType::WSC; "lock_task_observer_store", "error", "store_lock_poisoned");
            NetError::InternalError
        })
    }
}

impl TaskListenerRegistration {
    fn new<S>(listener: Listener, options: EventOptions, spawn: S) -> Result<Self, NetError>
    where
        S: FnOnce(Box<dyn FnOnce() + Send>) -> io::Result<()>,
    {
        let (ordinary, urgent) = capacity_lanes(options)?;
        let (sender, receiver) = mpsc::channel::<DispatchedEvent>();
        let task = Box::new(move || {
            // No Tokio runtime or client state is needed once an event has been submitted.
            while let Ok(dispatched) = receiver.recv() {
                let DispatchedEvent {
                    event,
                    task_slot,
                    byte_slots,
                } = dispatched;
                if catch_unwind(AssertUnwindSafe(|| listener(event))).is_err() {
                    crate::log_e!(LogType::WSC; "dispatch_task_event", "error", "user_callback_panicked");
                }
                // A blocked callback deliberately retains its bounded notification capacity.
                drop((task_slot, byte_slots));
            }
        });
        spawn(task).map_err(|error| {
            crate::log_e!(LogType::WSC; "register_task_listener", "error", crate::common::log::summary::error(&error));
            NetError::IOError
        })?;
        Ok(Self {
            sender,
            ordinary,
            urgent,
        })
    }

    fn lane(&self, urgent: bool) -> &CapacityLane {
        match (urgent, self.urgent.as_ref()) {
            (true, Some(lane)) => lane,
            _ => &self.ordinary,
        }
    }

    pub(crate) fn try_reserve(
        &self,
        size: usize,
        urgent: bool,
    ) -> Result<TaskEventPermit, NetError> {
        let lane = self.lane(urgent);
        let bytes = checked_payload_size(lane, size)?;
        let task_slot = Arc::clone(&lane.tasks)
            .try_acquire_owned()
            .map_err(map_try_capacity_error)?;
        let byte_slots = Arc::clone(&lane.bytes)
            .try_acquire_many_owned(bytes)
            .map_err(map_try_capacity_error)?;
        Ok(TaskEventPermit {
            sender: self.sender.clone(),
            task_slot,
            byte_slots,
        })
    }

    pub(crate) async fn reserve(
        &self,
        size: usize,
        urgent: bool,
        shutdown: &CancellationToken,
        admission: &CancellationToken,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<TaskEventPermit, NetError> {
        let lane = self.lane(urgent);
        let bytes = checked_payload_size(lane, size)?;
        let acquire = async {
            let task_slot = Arc::clone(&lane.tasks)
                .acquire_owned()
                .await
                .map_err(|_| NetError::QueueClosed)?;
            let byte_slots = Arc::clone(&lane.bytes)
                .acquire_many_owned(bytes)
                .await
                .map_err(|_| NetError::QueueClosed)?;
            Ok::<TaskEventPermit, NetError>(TaskEventPermit {
                sender: self.sender.clone(),
                task_slot,
                byte_slots,
            })
        };
        let timeout = async {
            match deadline {
                Some(deadline) => tokio::time::sleep_until(deadline).await,
                None => std::future::pending::<()>().await,
            }
        };
        let permit = tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Err(NetError::Cancelled),
            _ = admission.cancelled() => return Err(NetError::ConnectionClosed),
            _ = timeout => return Err(NetError::TimeoutError),
            result = acquire => result?,
        };
        // Recheck after acquisition so a cancellation observed at this boundary never admits a
        // new task merely because capacity and cancellation became ready together.
        if shutdown.is_cancelled() {
            return Err(NetError::Cancelled);
        }
        if admission.is_cancelled() {
            return Err(NetError::ConnectionClosed);
        }
        Ok(permit)
    }
}

impl TaskObservation {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        instance_id: u64,
        task_id: u64,
        request_id: Option<String>,
        source: Source,
        is_urgent: bool,
        session_context_id: Option<u64>,
        permit: TaskEventPermit,
    ) -> Arc<Self> {
        let event = Event {
            client_instance_id: instance_id,
            task_id,
            request_id,
            source,
            is_urgent,
            connection_generation: None,
            session_context_id,
            phase: Phase::WaitingForCapacity,
            delivery: Delivery::NotStarted,
            cause: Cause::EngineDropped,
            result: Err(NetError::EngineDropped),
        };
        Arc::new(Self {
            state: Mutex::new(Some(ObservationState {
                event,
                cause: None,
                permit,
            })),
            selected_result: OnceLock::new(),
        })
    }

    pub(crate) fn mark_queued(&self) {
        if let Some(state) = self.lock_state().as_mut() {
            state.event.phase = Phase::Queued;
        }
    }

    pub(crate) fn mark_writing(&self, generation: u64) {
        if let Some(state) = self.lock_state().as_mut() {
            state.event.phase = Phase::Writing;
            state.event.connection_generation = Some(generation);
        }
    }

    pub(crate) fn mark_requeued(&self) {
        self.mark_queued();
    }

    pub(crate) fn mark_written(&self) {
        if let Some(state) = self.lock_state().as_mut() {
            state.event.phase = Phase::AwaitingResponse;
            state.event.delivery = Delivery::Written;
        }
    }

    pub(crate) fn remember_unknown(&self) {
        if let Some(state) = self.lock_state().as_mut() {
            if state.event.delivery != Delivery::Written
                && state.event.delivery != Delivery::ResponseClaimed
            {
                state.event.delivery = Delivery::Unknown;
            }
        }
    }

    pub(crate) fn set_cause(&self, cause: Cause) {
        if let Some(state) = self.lock_state().as_mut() {
            state.cause.get_or_insert(cause);
        }
    }

    pub(crate) fn finish(&self, result: Result<Success, NetError>) {
        let state = {
            let mut current = self.lock_state();
            let state = current.take();
            if state.is_some() && self.selected_result.set(result).is_err() {
                crate::log_e!(LogType::WSC; "finish_task_observation", "error", "terminal_result_already_selected");
            }
            state
        };
        if let Some(state) = state {
            state.finish(result);
        }
    }

    /// This check and removal share a lock with phase transitions. A queued task remains owned
    /// by its queue/writer instead of racing an early lifecycle completion.
    pub(crate) fn finish_if_waiting(&self, error: NetError) {
        let state = {
            let mut state = self.lock_state();
            if state
                .as_ref()
                .is_some_and(|state| state.event.phase == Phase::WaitingForCapacity)
            {
                let selected = state.take();
                if self.selected_result.set(Err(error)).is_err() {
                    crate::log_e!(LogType::WSC; "finish_waiting_task_observation", "error", "terminal_result_already_selected");
                }
                selected
            } else {
                None
            }
        };
        if let Some(state) = state {
            state.finish(Err(error));
        }
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.lock_state().is_none()
    }

    pub(crate) fn selected_result(&self) -> Option<Result<Success, NetError>> {
        self.selected_result.get().copied()
    }

    fn lock_state(&self) -> MutexGuard<'_, Option<ObservationState>> {
        match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                crate::log_e!(LogType::WSC; "lock_task_observation", "error", "observation_lock_poisoned_recovered");
                poisoned.into_inner()
            }
        }
    }
}

impl Drop for TaskObservation {
    fn drop(&mut self) {
        let state = match self.state.get_mut() {
            Ok(state) => state.take(),
            Err(poisoned) => {
                crate::log_e!(LogType::WSC; "drop_task_observation", "error", "observation_lock_poisoned_recovered");
                poisoned.into_inner().take()
            }
        };
        if let Some(state) = state {
            if self
                .selected_result
                .set(Err(NetError::EngineDropped))
                .is_err()
            {
                crate::log_e!(LogType::WSC; "drop_task_observation", "error", "terminal_result_already_selected");
            }
            state.finish(Err(NetError::EngineDropped));
        }
    }
}

impl ObservationState {
    fn finish(mut self, result: Result<Success, NetError>) {
        self.event.result = result;
        self.event.cause = match result {
            Ok(_) => Cause::Completed,
            Err(error) => match self.cause {
                Some(cause) => cause,
                None => match error {
                    NetError::Cancelled => Cause::SendCancelled,
                    NetError::EngineDropped => Cause::EngineDropped,
                    _ => Cause::Failure,
                },
            },
        };
        match result {
            Ok(Success::Written) => self.event.delivery = Delivery::Written,
            Ok(Success::ResponseClaimed) => self.event.delivery = Delivery::ResponseClaimed,
            Err(NetError::DeliveryUnknown) => self.event.delivery = Delivery::Unknown,
            Err(_) => {}
        }
        let TaskEventPermit {
            sender,
            task_slot,
            byte_slots,
        } = self.permit;
        let task_id = self.event.task_id;
        let event = DispatchedEvent {
            event: self.event,
            task_slot,
            byte_slots,
        };
        if sender.send(event).is_err() {
            // The dedicated dispatcher normally lives until the final permit is released.
            crate::log_e!(LogType::WSC; "finish_task_observation", "task_id|error", task_id, "task_dispatcher_unavailable");
        }
    }
}

fn capacity_lanes(options: EventOptions) -> Result<(CapacityLane, Option<CapacityLane>), NetError> {
    if options.max_tasks == 0
        || options.max_tasks > Semaphore::MAX_PERMITS
        || options.max_payload_bytes == 0
        || options.max_payload_bytes > Semaphore::MAX_PERMITS
        || u32::try_from(options.max_payload_bytes).is_err()
        || (options.urgent_tasks == 0) != (options.urgent_payload_bytes == 0)
    {
        return Err(NetError::ConfigError);
    }
    let ordinary_tasks = options
        .max_tasks
        .checked_sub(options.urgent_tasks)
        .ok_or(NetError::ConfigError)?;
    let ordinary_bytes = options
        .max_payload_bytes
        .checked_sub(options.urgent_payload_bytes)
        .ok_or(NetError::ConfigError)?;
    if ordinary_tasks == 0 || ordinary_bytes == 0 {
        return Err(NetError::ConfigError);
    }
    let lane = |tasks, bytes| CapacityLane {
        tasks: Arc::new(Semaphore::new(tasks)),
        bytes: Arc::new(Semaphore::new(bytes)),
        max_bytes: bytes,
    };
    let urgent = (options.urgent_tasks > 0)
        .then(|| lane(options.urgent_tasks, options.urgent_payload_bytes));
    Ok((lane(ordinary_tasks, ordinary_bytes), urgent))
}

fn checked_payload_size(lane: &CapacityLane, size: usize) -> Result<u32, NetError> {
    let bytes = size.max(1);
    if bytes > lane.max_bytes {
        return Err(NetError::QueueItemTooLarge);
    }
    u32::try_from(bytes).map_err(|_| NetError::QueueItemTooLarge)
}

fn map_try_capacity_error(error: TryAcquireError) -> NetError {
    match error {
        TryAcquireError::Closed => NetError::QueueClosed,
        TryAcquireError::NoPermits => NetError::QueueFull,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::traits::ws::ws_body::WsBody;
    use crate::api::traits::ws::ws_request_trait::WSRequestTrait;
    use crate::module::ws_client::test_support::{check, check_eq, test_error, TestResult};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::time::Duration;

    fn registered(
        options: EventOptions,
    ) -> TestResult<(
        TaskObserverStore,
        Arc<TaskListenerRegistration>,
        mpsc::Receiver<Event>,
    )> {
        let store = TaskObserverStore::new(7);
        let (sender, receiver) = mpsc::channel();
        store.register(
            Box::new(move |event| {
                let _ = sender.send(event);
            }),
            options,
        )?;
        let registration = store
            .snapshot()?
            .ok_or_else(|| test_error("registration missing"))?;
        Ok((store, registration, receiver))
    }

    fn observation(
        registration: &TaskListenerRegistration,
        id: u64,
        urgent: bool,
    ) -> TestResult<Arc<TaskObservation>> {
        Ok(TaskObservation::new(
            7,
            id,
            Some("request".into()),
            Source::Body(WsBody::Text("data".into())),
            urgent,
            Some(11),
            registration.try_reserve(4, urgent)?,
        ))
    }

    #[test]
    fn terminal_event_is_once_and_preserves_source_and_identity() -> TestResult {
        let (_store, registration, events) = registered(EventOptions::new(2, 16))?;
        let task = observation(&registration, 13, false)?;
        task.mark_queued();
        task.mark_writing(17);
        task.mark_written();
        task.set_cause(Cause::Disconnect);
        task.set_cause(Cause::Shutdown);
        task.finish(Err(NetError::Cancelled));
        task.finish(Ok(Success::ResponseClaimed));
        check!(task.is_finished())?;
        drop(task);
        let event = events.recv_timeout(Duration::from_secs(2))?;
        check_eq!(event.client_instance_id(), 7)?;
        check_eq!(event.task_id(), 13)?;
        check_eq!(event.request_id(), Some("request"))?;
        check_eq!(event.connection_generation(), Some(17))?;
        check_eq!(event.session_context_id(), Some(11))?;
        check_eq!(event.phase(), Phase::AwaitingResponse)?;
        check_eq!(event.delivery(), Delivery::Written)?;
        check_eq!(event.cause(), Cause::Disconnect)?;
        check_eq!(event.result(), Err(NetError::Cancelled))?;
        check!(matches!(event.source(), Source::Body(WsBody::Text(value)) if value == "data"))?;
        check!(events.try_recv().is_err())?;
        Ok(())
    }

    #[test]
    fn selected_result_keeps_the_first_terminal_error_before_late_success() -> TestResult {
        let (_store, registration, events) = registered(EventOptions::new(2, 8))?;
        let task = observation(&registration, 1, false)?;
        check_eq!(task.selected_result(), None)?;
        task.finish(Err(NetError::ConnectionClosed));
        check!(task.is_finished())?;
        check_eq!(
            task.selected_result(),
            Some(Err(NetError::ConnectionClosed))
        )?;
        task.finish(Ok(Success::ResponseClaimed));
        check_eq!(
            task.selected_result(),
            Some(Err(NetError::ConnectionClosed))
        )?;
        check_eq!(
            events.recv_timeout(Duration::from_secs(2))?.result(),
            Err(NetError::ConnectionClosed)
        )?;

        let waiting = observation(&registration, 2, false)?;
        waiting.finish_if_waiting(NetError::Cancelled);
        check!(waiting.is_finished())?;
        check_eq!(waiting.selected_result(), Some(Err(NetError::Cancelled)))?;
        waiting.finish(Err(NetError::EngineDropped));
        check_eq!(waiting.selected_result(), Some(Err(NetError::Cancelled)))?;
        check_eq!(
            events.recv_timeout(Duration::from_secs(2))?.result(),
            Err(NetError::Cancelled)
        )?;
        Ok(())
    }

    #[test]
    fn ordinary_full_budget_cannot_take_urgent_reservation() -> TestResult {
        let (_store, registration, _events) =
            registered(EventOptions::new(2, 8).with_urgent_reserve(1, 4))?;
        let ordinary = registration.try_reserve(4, false)?;
        check!(matches!(
            registration.try_reserve(1, false),
            Err(NetError::QueueFull)
        ))?;
        let urgent = registration.try_reserve(4, true)?;
        check!(matches!(
            registration.try_reserve(1, true),
            Err(NetError::QueueFull)
        ))?;
        check!(matches!(
            registration.try_reserve(5, true),
            Err(NetError::QueueItemTooLarge)
        ))?;
        drop((ordinary, urgent));
        check!(registration.try_reserve(4, false).is_ok())?;
        check!(registration.try_reserve(4, true).is_ok())?;
        Ok(())
    }

    #[test]
    fn no_reservation_shares_capacity_and_failed_byte_reserve_releases_task_slot() -> TestResult {
        let (_store, registration, _events) = registered(EventOptions::new(2, 4))?;
        let ordinary = registration.try_reserve(4, false)?;
        check!(matches!(
            registration.try_reserve(1, true),
            Err(NetError::QueueFull)
        ))?;
        drop(ordinary);
        let first = registration.try_reserve(2, true)?;
        let second = registration.try_reserve(2, false)?;
        drop((first, second));
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_notification_capacity_waiter_releases_partial_capacity() -> TestResult {
        let (_store, registration, _events) = registered(EventOptions::new(2, 4))?;
        let blocker = registration.try_reserve(4, false)?;
        let shutdown = CancellationToken::new();
        let admission = CancellationToken::new();
        let waiting = registration.reserve(1, false, &shutdown, &admission, None);
        tokio::pin!(waiting);
        check!(futures::poll!(&mut waiting).is_pending())?;
        admission.cancel();
        check!(matches!(
            tokio::time::timeout(Duration::from_secs(2), waiting).await?,
            Err(NetError::ConnectionClosed)
        ))?;
        drop(blocker);
        let first = registration.try_reserve(2, false)?;
        let second = registration.try_reserve(2, true)?;
        drop((first, second));
        Ok(())
    }

    #[test]
    fn invalid_notification_limits_fail_without_starting_a_thread() -> TestResult {
        let store = TaskObserverStore::new(1);
        for options in [
            EventOptions::new(0, 1),
            EventOptions::new(1, 0),
            EventOptions::new(usize::MAX, 1),
            EventOptions::new(1, usize::MAX),
            EventOptions::new(2, 8).with_urgent_reserve(1, 0),
            EventOptions::new(2, 8).with_urgent_reserve(0, 1),
            EventOptions::new(2, 8).with_urgent_reserve(2, 4),
            EventOptions::new(2, 8).with_urgent_reserve(1, 8),
            EventOptions::new(2, 8).with_urgent_reserve(3, 4),
        ] {
            let started = AtomicUsize::new(0);
            let result = store.register_with(Box::new(|_| {}), options, |task| {
                started.fetch_add(1, Ordering::SeqCst);
                drop(task);
                Ok(())
            });
            check_eq!(result, Err(NetError::ConfigError))?;
            check_eq!(started.load(Ordering::SeqCst), 0)?;
            check!(store.snapshot()?.is_none())?;
        }
        Ok(())
    }

    #[test]
    fn rejected_dispatch_thread_preserves_existing_registration() -> TestResult {
        let (store, original, events) = registered(EventOptions::new(1, 8))?;
        let result = store.register_with(Box::new(|_| {}), EventOptions::new(1, 8), |task| {
            drop(task);
            Err(io::Error::other("injected thread creation failure"))
        });
        check_eq!(result, Err(NetError::IOError))?;
        let retained = store
            .snapshot()?
            .ok_or_else(|| test_error("old listener lost after failed spawn"))?;
        check!(Arc::ptr_eq(&original, &retained))?;
        observation(&original, 1, false)?.finish(Ok(Success::Written));
        check_eq!(
            events.recv_timeout(Duration::from_secs(2))?.result(),
            Ok(Success::Written)
        )?;
        Ok(())
    }

    #[test]
    fn replacement_unregister_and_close_preserve_already_reserved_listeners() -> TestResult {
        let (store, original, old_events) = registered(EventOptions::new(1, 8))?;
        let old_task = observation(&original, 1, false)?;
        let (new_tx, new_events) = mpsc::channel();
        store.register(
            Box::new(move |event| {
                let _ = new_tx.send(event);
            }),
            EventOptions::new(1, 8),
        )?;
        let replacement = store
            .snapshot()?
            .ok_or_else(|| test_error("replacement missing"))?;
        let new_task = observation(&replacement, 2, true)?;
        store.unregister()?;
        check!(store.snapshot()?.is_none())?;
        store.close();
        check!(matches!(store.snapshot(), Err(NetError::EngineDropped)))?;
        check_eq!(
            store.register(Box::new(|_| {}), EventOptions::new(1, 8)),
            Err(NetError::EngineDropped)
        )?;
        drop((original, replacement));
        old_task.finish(Err(NetError::Cancelled));
        new_task.finish(Ok(Success::Written));
        drop((old_task, new_task));
        check_eq!(
            old_events.recv_timeout(Duration::from_secs(2))?.task_id(),
            1
        )?;
        check_eq!(
            new_events.recv_timeout(Duration::from_secs(2))?.task_id(),
            2
        )?;
        check!(matches!(
            old_events.recv_timeout(Duration::from_secs(2)),
            Err(mpsc::RecvTimeoutError::Disconnected)
        ))?;
        check!(matches!(
            new_events.recv_timeout(Duration::from_secs(2)),
            Err(mpsc::RecvTimeoutError::Disconnected)
        ))?;
        Ok(())
    }

    #[test]
    fn blocked_callback_keeps_capacity_and_queued_notifications_survive_close() -> TestResult {
        let store = TaskObserverStore::new(1);
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let release_rx = Mutex::new(release_rx);
        let (events_tx, events_rx) = mpsc::channel();
        let caller = std::thread::current().id();
        store.register(Box::new(move |event| {
            let _ = entered_tx.send((std::thread::current().id(), tokio::runtime::Handle::try_current().is_ok()));
            match release_rx.lock() {
                Ok(receiver) => { let _ = receiver.recv(); }
                Err(_) => { crate::log_e!(LogType::WSC; "blocked_callback_test", "error", "release_lock_poisoned"); }
            }
            let _ = events_tx.send(event);
        }), EventOptions::new(2, 8))?;
        let registration = store
            .snapshot()?
            .ok_or_else(|| test_error("registration missing"))?;
        let first = observation(&registration, 1, false)?;
        let second = observation(&registration, 2, true)?;
        first.finish(Ok(Success::Written));
        let (callback_thread, has_runtime) = entered_rx.recv_timeout(Duration::from_secs(2))?;
        check!(callback_thread != caller)?;
        check!(!has_runtime)?;
        check!(matches!(
            registration.try_reserve(1, false),
            Err(NetError::QueueFull)
        ))?;
        second.finish(Err(NetError::Cancelled));
        store.close();
        drop((registration, first, second));
        drop(release_tx);
        check_eq!(events_rx.recv_timeout(Duration::from_secs(2))?.task_id(), 1)?;
        check_eq!(events_rx.recv_timeout(Duration::from_secs(2))?.task_id(), 2)?;
        check!(matches!(
            events_rx.recv_timeout(Duration::from_secs(2)),
            Err(mpsc::RecvTimeoutError::Disconnected)
        ))?;
        Ok(())
    }

    struct LifetimeRequest {
        dropped: Arc<AtomicUsize>,
    }

    impl WSRequestTrait for LifetimeRequest {
        fn uuid(&self) -> String {
            "original-source".into()
        }
        fn body(&self) -> Result<WsBody, NetError> {
            Ok(WsBody::Text("data".into()))
        }
    }

    impl Drop for LifetimeRequest {
        fn drop(&mut self) {
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn original_request_lives_until_the_delivered_event_is_released() -> TestResult {
        let (_store, registration, events) = registered(EventOptions::new(1, 8))?;
        let dropped = Arc::new(AtomicUsize::new(0));
        let request: Arc<dyn WSRequestTrait> = Arc::new(LifetimeRequest {
            dropped: Arc::clone(&dropped),
        });
        let weak = Arc::downgrade(&request);
        let task = TaskObservation::new(
            7,
            1,
            Some("original-source".into()),
            Source::Request(request),
            false,
            None,
            registration.try_reserve(4, false)?,
        );
        check!(weak.upgrade().is_some())?;
        task.finish(Err(NetError::Cancelled));
        drop(task);
        let event = events.recv_timeout(Duration::from_secs(2))?;
        check_eq!(dropped.load(Ordering::SeqCst), 0)?;
        check!(
            matches!(event.source(), Source::Request(request) if request.uuid() == "original-source")
        )?;
        drop(event);
        check!(weak.upgrade().is_none())?;
        check_eq!(dropped.load(Ordering::SeqCst), 1)?;
        Ok(())
    }

    #[test]
    fn delivery_history_and_response_success_keep_their_meaning() -> TestResult {
        let (_store, registration, events) = registered(EventOptions::new(8, 32))?;
        for (id, remember_unknown, success, expected_delivery) in [
            (1, false, false, Delivery::NotStarted),
            (2, true, false, Delivery::Unknown),
            (3, true, true, Delivery::ResponseClaimed),
        ] {
            let task = observation(&registration, id, false)?;
            task.mark_writing(9);
            if remember_unknown {
                task.remember_unknown();
            }
            task.mark_requeued();
            task.set_cause(Cause::Shutdown);
            if success {
                task.finish(Ok(Success::ResponseClaimed));
            } else {
                task.finish(Err(NetError::Cancelled));
            }
            let event = events.recv_timeout(Duration::from_secs(2))?;
            check_eq!(event.delivery(), expected_delivery)?;
            check_eq!(event.phase(), Phase::Queued)?;
            check_eq!(
                event.cause(),
                if success {
                    Cause::Completed
                } else {
                    Cause::Shutdown
                }
            )?;
        }
        let task = observation(&registration, 4, false)?;
        task.finish(Err(NetError::DeliveryUnknown));
        check_eq!(
            events.recv_timeout(Duration::from_secs(2))?.delivery(),
            Delivery::Unknown
        )?;
        Ok(())
    }

    #[test]
    fn waiting_completion_cannot_take_queue_ownership_and_drop_completes_once() -> TestResult {
        let (store, registration, events) = registered(EventOptions::new(3, 12))?;
        let waiting = observation(&registration, 1, false)?;
        waiting.finish_if_waiting(NetError::Cancelled);
        check!(waiting.is_finished())?;
        let queued = observation(&registration, 2, false)?;
        queued.mark_queued();
        queued.finish_if_waiting(NetError::Cancelled);
        check!(!queued.is_finished())?;
        queued.finish(Ok(Success::Written));
        let abandoned = observation(&registration, 3, false)?;
        drop((waiting, queued, abandoned));
        store.close();
        drop(registration);
        let first = events.recv_timeout(Duration::from_secs(2))?;
        let second = events.recv_timeout(Duration::from_secs(2))?;
        let third = events.recv_timeout(Duration::from_secs(2))?;
        check_eq!(first.task_id(), 1)?;
        check_eq!(first.phase(), Phase::WaitingForCapacity)?;
        check_eq!(second.task_id(), 2)?;
        check_eq!(second.result(), Ok(Success::Written))?;
        check_eq!(third.task_id(), 3)?;
        check_eq!(third.result(), Err(NetError::EngineDropped))?;
        check!(matches!(
            events.recv_timeout(Duration::from_secs(2)),
            Err(mpsc::RecvTimeoutError::Disconnected)
        ))?;
        Ok(())
    }

    #[test]
    fn concurrent_finish_has_one_authoritative_event() -> TestResult {
        let (store, registration, events) = registered(EventOptions::new(1, 8))?;
        let task = observation(&registration, 1, false)?;
        let mut start_senders = Vec::new();
        let mut threads = Vec::new();
        for result in [
            Ok(Success::Written),
            Ok(Success::ResponseClaimed),
            Err(NetError::Cancelled),
            Err(NetError::DeliveryUnknown),
        ] {
            let task = Arc::clone(&task);
            let (start_tx, start_rx) = mpsc::channel::<()>();
            start_senders.push(start_tx);
            threads.push(std::thread::Builder::new().spawn(move || {
                let _ = start_rx.recv();
                task.finish(result);
            })?);
        }
        drop(start_senders);
        for thread in threads {
            thread
                .join()
                .map_err(|_| test_error("completion competitor failed"))?;
        }
        check!(task.is_finished())?;
        store.close();
        drop((registration, task));
        check_eq!(events.recv_timeout(Duration::from_secs(2))?.task_id(), 1)?;
        check!(matches!(
            events.recv_timeout(Duration::from_secs(2)),
            Err(mpsc::RecvTimeoutError::Disconnected)
        ))?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn notification_capacity_deadline_and_shutdown_have_explicit_results() -> TestResult {
        let (_store, registration, _events) = registered(EventOptions::new(1, 4))?;
        let blocker = registration.try_reserve(4, false)?;
        let shutdown = CancellationToken::new();
        let admission = CancellationToken::new();
        let deadline = tokio::time::Instant::now()
            .checked_add(Duration::from_secs(1))
            .ok_or_else(|| test_error("test deadline unavailable"))?;
        let waiting = registration.reserve(1, false, &shutdown, &admission, Some(deadline));
        tokio::pin!(waiting);
        check!(futures::poll!(&mut waiting).is_pending())?;
        tokio::time::advance(Duration::from_secs(1)).await;
        check!(matches!(waiting.await, Err(NetError::TimeoutError)))?;
        shutdown.cancel();
        drop(blocker);
        check!(matches!(
            registration
                .reserve(4, false, &shutdown, &admission, None)
                .await,
            Err(NetError::Cancelled)
        ))?;
        check!(registration.try_reserve(4, false).is_ok())?;
        Ok(())
    }
}

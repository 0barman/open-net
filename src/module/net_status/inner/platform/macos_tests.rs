use std::cell::Cell;
use std::ffi::{c_int, c_void};
use std::io;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use super::{NativeApi, NativeNetworkMonitor};
use crate::module::net_status::inner::refresh_trigger::{
    self, RefreshTriggerEvent, RefreshTriggerReceiver,
};

type NativeCallback = unsafe extern "C" fn(*mut c_void);

#[derive(Default)]
struct MockState {
    starts: AtomicUsize,
    stops: AtomicUsize,
    callbacks: AtomicUsize,
}

struct MockPlan {
    state: Arc<MockState>,
    status: c_int,
    returns_handle: bool,
    initial_callbacks: usize,
    callback_on_stop: bool,
    callback_with_null_context: bool,
}

impl MockPlan {
    fn successful(state: &Arc<MockState>) -> Self {
        Self {
            state: Arc::clone(state),
            status: 0,
            returns_handle: true,
            initial_callbacks: 0,
            callback_on_stop: false,
            callback_with_null_context: false,
        }
    }
}

thread_local! {
    // Start consumes the plan synchronously. Separate test threads therefore
    // cannot share plans; the returned handle owns everything stop needs.
    static NEXT_START: Cell<Option<MockPlan>> = const { Cell::new(None) };
}

struct MockHandle {
    state: Arc<MockState>,
    callback: NativeCallback,
    context: *mut c_void,
    callback_on_stop: bool,
}

unsafe extern "C" fn mock_start(
    callback: Option<NativeCallback>,
    context: *mut c_void,
    output: *mut *mut c_void,
) -> c_int {
    if output.is_null() {
        return 1;
    }
    // SAFETY: NativeApi requires a writable output pointer for this call.
    unsafe { output.write(ptr::null_mut()) };
    let Some(plan) = NEXT_START.with(Cell::take) else {
        return 1;
    };
    plan.state.starts.fetch_add(1, Ordering::SeqCst);
    let Some(callback) = callback else {
        return 1;
    };
    if context.is_null() {
        return 1;
    }
    if plan.callback_with_null_context {
        // SAFETY: The Rust callback explicitly accepts a null context as a
        // no-op, so malformed native input must never unwind over the ABI.
        unsafe { callback(ptr::null_mut()) };
    }
    for _ in 0..plan.initial_callbacks {
        // SAFETY: The adapter owns a live context until mock_stop returns.
        unsafe { callback(context) };
        plan.state.callbacks.fetch_add(1, Ordering::SeqCst);
    }
    if plan.returns_handle {
        let handle = Box::new(MockHandle {
            state: plan.state,
            callback,
            context,
            callback_on_stop: plan.callback_on_stop,
        });
        // SAFETY: The caller supplied the writable output pointer; ownership
        // of this allocation transfers to exactly one mock_stop call.
        unsafe { output.write(Box::into_raw(handle).cast()) };
    }
    plan.status
}

unsafe extern "C" fn mock_stop(handle: *mut c_void) {
    if handle.is_null() {
        return;
    }
    // SAFETY: Only mock_start creates these handles. The adapter must stop
    // each successful handle exactly once, before dropping its context.
    let handle = unsafe { Box::from_raw(handle.cast::<MockHandle>()) };
    handle.state.stops.fetch_add(1, Ordering::SeqCst);
    if handle.callback_on_stop {
        // SAFETY: This deliberately models an update already queued when
        // cancellation starts. Context ownership must outlive native stop.
        unsafe { (handle.callback)(handle.context) };
        handle.state.callbacks.fetch_add(1, Ordering::SeqCst);
    }
}

fn mock_api(plan: MockPlan) -> NativeApi {
    NEXT_START.with(|next| next.set(Some(plan)));
    NativeApi {
        start: mock_start,
        stop: mock_stop,
    }
}

fn check_count(counter: &AtomicUsize, expected: usize, label: &str) -> io::Result<()> {
    let actual = counter.load(Ordering::SeqCst);
    if actual != expected {
        return Err(io::Error::other(format!(
            "{label}: expected {expected}, received {actual}"
        )));
    }
    Ok(())
}

async fn check_event(
    receiver: &mut RefreshTriggerReceiver,
    expected: RefreshTriggerEvent,
) -> io::Result<()> {
    let actual = tokio::time::timeout(Duration::from_secs(10), receiver.recv())
        .await
        .map_err(|error| io::Error::new(io::ErrorKind::TimedOut, error))?;
    if actual != expected {
        return Err(io::Error::other(format!(
            "expected {expected:?}, received {actual:?}"
        )));
    }
    Ok(())
}

async fn wait_until_closed(receiver: &mut RefreshTriggerReceiver) -> io::Result<()> {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if receiver.recv().await == RefreshTriggerEvent::ChannelClosed {
                return;
            }
        }
    })
    .await
    .map_err(|error| io::Error::new(io::ErrorKind::TimedOut, error))
}

#[test]
fn native_monitor_owner_is_send_and_sync() {
    fn require_send_sync<T: Send + Sync>() {}
    require_send_sync::<NativeNetworkMonitor>();
}

#[tokio::test]
async fn native_start_failure_releases_context_without_stopping_null_handle() -> io::Result<()> {
    let state = Arc::new(MockState::default());
    let plan = MockPlan {
        status: 2,
        returns_handle: false,
        ..MockPlan::successful(&state)
    };
    let (trigger, mut receiver) = refresh_trigger::channel();
    let error = match NativeNetworkMonitor::start_with(trigger, mock_api(plan)) {
        Ok(monitor) => {
            drop(monitor);
            return Err(io::Error::other("native start failure was discarded"));
        }
        Err(error) => error,
    };
    if !error.to_string().contains('2') {
        return Err(io::Error::other("native failure status was discarded"));
    }
    check_event(&mut receiver, RefreshTriggerEvent::ChannelClosed).await?;
    check_count(&state.starts, 1, "start calls")?;
    check_count(&state.stops, 0, "stop calls")
}

#[tokio::test]
async fn failed_start_with_a_handle_stops_before_releasing_callback_context() -> io::Result<()> {
    let state = Arc::new(MockState::default());
    let plan = MockPlan {
        status: 5,
        callback_on_stop: true,
        ..MockPlan::successful(&state)
    };
    let (trigger, mut receiver) = refresh_trigger::channel();
    if let Ok(monitor) = NativeNetworkMonitor::start_with(trigger, mock_api(plan)) {
        drop(monitor);
        return Err(io::Error::other("native start failure was discarded"));
    }
    check_count(&state.stops, 1, "stop calls after failed start")?;
    check_count(&state.callbacks, 1, "callback during failed start cleanup")?;
    check_event(&mut receiver, RefreshTriggerEvent::Notified).await?;
    check_event(&mut receiver, RefreshTriggerEvent::ChannelClosed).await
}

#[tokio::test]
async fn successful_status_with_null_handle_is_rejected_and_releases_context() -> io::Result<()> {
    let state = Arc::new(MockState::default());
    let plan = MockPlan {
        returns_handle: false,
        ..MockPlan::successful(&state)
    };
    let (trigger, mut receiver) = refresh_trigger::channel();
    if let Ok(monitor) = NativeNetworkMonitor::start_with(trigger, mock_api(plan)) {
        drop(monitor);
        return Err(io::Error::other("a null native monitor was accepted"));
    }
    check_event(&mut receiver, RefreshTriggerEvent::ChannelClosed).await?;
    check_count(&state.stops, 0, "stop calls")
}

#[tokio::test]
async fn native_callbacks_coalesce_without_reading_path_properties() -> io::Result<()> {
    let state = Arc::new(MockState::default());
    let plan = MockPlan {
        initial_callbacks: 64,
        ..MockPlan::successful(&state)
    };
    let (trigger, mut receiver) = refresh_trigger::channel();
    let monitor = NativeNetworkMonitor::start_with(trigger, mock_api(plan))?;
    drop(monitor);

    check_event(&mut receiver, RefreshTriggerEvent::Notified).await?;
    check_event(&mut receiver, RefreshTriggerEvent::ChannelClosed).await?;
    check_count(&state.callbacks, 64, "native callback calls")?;
    check_count(&state.stops, 1, "stop calls")
}

#[tokio::test]
async fn null_callback_context_is_a_safe_noop() -> io::Result<()> {
    let state = Arc::new(MockState::default());
    let plan = MockPlan {
        callback_with_null_context: true,
        ..MockPlan::successful(&state)
    };
    let (trigger, mut receiver) = refresh_trigger::channel();
    let monitor = NativeNetworkMonitor::start_with(trigger, mock_api(plan))?;
    drop(monitor);

    check_event(&mut receiver, RefreshTriggerEvent::ChannelClosed).await?;
    check_count(&state.stops, 1, "stop calls")
}

#[tokio::test]
async fn drop_keeps_callback_context_alive_until_stop_returns() -> io::Result<()> {
    let state = Arc::new(MockState::default());
    let plan = MockPlan {
        callback_on_stop: true,
        ..MockPlan::successful(&state)
    };
    let (trigger, mut receiver) = refresh_trigger::channel();
    let monitor = NativeNetworkMonitor::start_with(trigger, mock_api(plan))?;
    drop(monitor);

    check_count(&state.stops, 1, "stop calls")?;
    check_count(&state.callbacks, 1, "callback during cancellation")?;
    check_event(&mut receiver, RefreshTriggerEvent::Notified).await?;
    check_event(&mut receiver, RefreshTriggerEvent::ChannelClosed).await
}

#[test]
fn callbacks_after_receiver_closes_are_safe_during_start_and_stop() -> io::Result<()> {
    let state = Arc::new(MockState::default());
    let plan = MockPlan {
        initial_callbacks: 64,
        callback_on_stop: true,
        ..MockPlan::successful(&state)
    };
    let (trigger, receiver) = refresh_trigger::channel();
    drop(receiver);
    let monitor = NativeNetworkMonitor::start_with(trigger, mock_api(plan))?;
    drop(monitor);

    check_count(&state.callbacks, 65, "native callback calls")?;
    check_count(&state.stops, 1, "stop calls")
}

#[tokio::test]
async fn native_monitor_delivers_an_initial_hint_and_releases_callback_on_drop() -> io::Result<()> {
    let (trigger, mut receiver) = refresh_trigger::channel();
    let monitor = NativeNetworkMonitor::start(trigger)?;
    check_event(&mut receiver, RefreshTriggerEvent::Notified).await?;

    drop(monitor);
    wait_until_closed(&mut receiver).await
}

#[tokio::test]
async fn repeated_native_monitor_start_and_immediate_drop_releases_every_callback() -> io::Result<()>
{
    for _ in 0..64 {
        let (trigger, mut receiver) = refresh_trigger::channel();
        let monitor = NativeNetworkMonitor::start(trigger)?;
        drop(monitor);
        wait_until_closed(&mut receiver).await?;
    }
    Ok(())
}

#[tokio::test]
async fn native_monitor_can_be_moved_to_another_thread_and_dropped() -> io::Result<()> {
    let (trigger, mut receiver) = refresh_trigger::channel();
    let monitor = NativeNetworkMonitor::start(trigger)?;
    let worker = std::thread::Builder::new().spawn(move || drop(monitor))?;
    if worker.join().is_err() {
        return Err(io::Error::other("native monitor drop thread failed"));
    }
    wait_until_closed(&mut receiver).await
}

#[test]
fn native_receiver_can_close_before_monitor_is_dropped() -> io::Result<()> {
    let (trigger, receiver) = refresh_trigger::channel();
    drop(receiver);
    let monitor = NativeNetworkMonitor::start(trigger)?;
    drop(monitor);
    Ok(())
}

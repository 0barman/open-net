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

// 测试替身保存的 C ABI 回调类型，与生产适配层的签名一致。
type NativeCallback = unsafe extern "C" fn(*mut c_void);

// 跨测试回调与停止路径共享的计数器，用于核对启动、回收和上下文访问次数。
#[derive(Default)]
struct MockState {
    // 替身成功取到启动计划后的启动调用次数。
    starts: AtomicUsize,
    // 非空替身句柄被停止并接管回收的次数。
    stops: AtomicUsize,
    // 使用原始非空上下文完成的模拟回调次数。
    callbacks: AtomicUsize,
}

// 描述下一次原生启动替身的返回结果和需要触发的回调时机。
struct MockPlan {
    // 与测试断言及返回句柄共享的原子计数状态。
    state: Arc<MockState>,
    // 替身启动入口最终返回的原生状态码，零表示成功。
    status: c_int,
    // 是否生成非空句柄，可与失败状态组合以验证部分初始化的回收。
    returns_handle: bool,
    // 启动返回之前使用原始上下文同步触发的回调次数。
    initial_callbacks: usize,
    // 是否在停止阶段补发一次已排队更新，检查上下文是否保持存活。
    callback_on_stop: bool,
    // 是否额外用空上下文调用回调，检查其空指针防御分支。
    callback_with_null_context: bool,
}

impl MockPlan {
    // 创建成功且不主动发出回调的基础计划，供各测试只覆写所需故障或时机。
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
    // 下一次启动同步取走的线程局部计划，避免并行测试互相覆盖。
    static NEXT_START: Cell<Option<MockPlan>> = const { Cell::new(None) };
}

// 交给适配层唯一持有的模拟原生句柄，保存停止阶段所需的全部数据。
struct MockHandle {
    // 与启动计划及断言共享的生命周期计数器。
    state: Arc<MockState>,
    // 启动时传入的 Rust 回调，可用于模拟取消前已排队的更新。
    callback: NativeCallback,
    // 借用的原始 Rust 上下文地址，必须保持有效直到 mock_stop 返回。
    context: *mut c_void,
    // 是否在释放模拟句柄之前触发一次取消期间的回调。
    callback_on_stop: bool,
}

// 消费当前线程的计划，按计划触发回调并返回状态及可选句柄，模拟原生启动边界。
// 调用方须提供可写输出指针与存活的上下文；非空返回句柄只交给一次 mock_stop 回收。
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

// 接管并释放 mock_start 返回的唯一句柄，可在释放前模拟一次取消期间的回调。
// 非空句柄必须仍有效且未被回收，其借用的上下文须存活至本函数返回。
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

// 将计划安装到当前线程，返回与之配套的启动和停止函数表。
fn mock_api(plan: MockPlan) -> NativeApi {
    NEXT_START.with(|next| next.set(Some(plan)));
    NativeApi {
        start: mock_start,
        stop: mock_stop,
    }
}

// 核对原子计数值，失败时返回包含计数标签和实际值的诊断错误。
fn check_count(counter: &AtomicUsize, expected: usize, label: &str) -> io::Result<()> {
    let actual = counter.load(Ordering::SeqCst);
    if actual != expected {
        return Err(io::Error::other(format!(
            "{label}: expected {expected}, received {actual}"
        )));
    }
    Ok(())
}

// 在十秒内接收并核对一个合并提示或关闭事件，避免生命周期回归使测试无限等待。
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

// 消费可能残留的提示，并在十秒内确认最后一个发送端已随原生上下文释放。
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

// 通过编译期约束验证原生监视器所有者满足跨线程传递和共享的要求。
#[test]
fn native_monitor_owner_is_send_and_sync() {
    // 仅施加 Send + Sync 类型约束，不执行运行时操作。
    fn require_send_sync<T: Send + Sync>() {}
    require_send_sync::<NativeNetworkMonitor>();
}

// 验证启动失败且没有句柄时释放回调上下文，保留原生错误码并避免无效 stop。
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

// 验证失败启动留下部分句柄时先执行 stop，且停止回调仍能访问有效上下文。
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

// 验证成功状态搭配空句柄也被拒绝，释放上下文且不调用空句柄 stop。
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

// 验证连续原生回调仅留下一个待处理提示，无需读取任何路径属性。
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

// 验证空上下文回调安全返回，不制造刷新事件，也不妨碍正常释放句柄。
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

// 验证 Drop 等待原生 stop 返回后才释放触发器，保留取消期间最后一次通知。
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

// 验证接收端提前关闭时，启动和停止期间继续回调仍可正常完成并回收句柄。
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

// 使用真实 Network.framework 验证首次提示，以及 Drop 后回调发送端最终释放。
#[tokio::test]
async fn native_monitor_delivers_an_initial_hint_and_releases_callback_on_drop() -> io::Result<()> {
    let (trigger, mut receiver) = refresh_trigger::channel();
    let monitor = NativeNetworkMonitor::start(trigger)?;
    check_event(&mut receiver, RefreshTriggerEvent::Notified).await?;

    drop(monitor);
    wait_until_closed(&mut receiver).await
}

// 重复启动并立即释放真实监听器，检查每轮取消都释放其回调上下文。
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

// 验证真实监听器可移动到另一线程完成 Drop，且主线程能观察到提示通道关闭。
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

// 验证真实来源在接收端已关闭的情况下仍可启动和停止，回调无需等待接收端。
#[test]
fn native_receiver_can_close_before_monitor_is_dropped() -> io::Result<()> {
    let (trigger, receiver) = refresh_trigger::channel();
    drop(receiver);
    let monitor = NativeNetworkMonitor::start(trigger)?;
    drop(monitor);
    Ok(())
}

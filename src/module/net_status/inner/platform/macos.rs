//! macOS native network-change hints, independent of a Swift toolchain.
//!
//! The callback deliberately ignores the path. Its only responsibility is a
//! non-blocking hint to resample the authoritative `netwatch::State`.

use std::ffi::{c_int, c_void};
use std::io::{self, Write};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr::{self, NonNull};

use super::super::refresh_trigger::RefreshTrigger;

#[cfg(test)]
#[path = "macos_tests.rs"]
mod tests;

// 原生队列使用的 C ABI 回调；参数借用 Rust 保持存活的刷新触发器。
type NativeCallback = unsafe extern "C" fn(*mut c_void);

unsafe extern "C" {
    // 启动原生监听；context 须存活至 stop 返回，output 接收唯一句柄，返回原生状态码。
    fn open_net_path_monitor_start(
        notify: Option<NativeCallback>,
        context: *mut c_void,
        output: *mut *mut c_void,
    ) -> c_int;
    // 在私有回调队列之外同步停止并释放句柄，等待所有回调结束后才归还 context。
    fn open_net_path_monitor_stop(monitor: *mut c_void);
}

/// Private ABI boundary, also injectable by the lifecycle tests.
///
/// Start may borrow context until stop returns. A null output must never retain
/// context; each non-null output must accept exactly one synchronous stop.
/// Stop must finish every callback before returning and may run on any thread
/// except the private callback queue. These requirements also apply to tests.
// 可注入的原生 ABI 函数表；生产实现和测试替身必须满足相同的所有权与停止约定。
#[derive(Clone, Copy, Debug)]
struct NativeApi {
    // 初始化入口；非空输出句柄交给 Rust 唯一持有，空输出不得继续借用 context。
    start: unsafe extern "C" fn(Option<NativeCallback>, *mut c_void, *mut *mut c_void) -> c_int,
    // 与 start 配对的回收入口；每个非空句柄仅调用一次，返回前完成所有回调。
    stop: unsafe extern "C" fn(*mut c_void),
}

// 原生监听句柄的唯一 Rust 所有者，将回调上下文的生存期绑定到同步停止之后。
#[derive(Debug)]
pub(crate) struct NativeNetworkMonitor {
    // 成功启动获得的非空原生句柄；仅由当前所有者在 Drop 中回收。
    handle: NonNull<c_void>,
    // 创建该句柄时使用的函数表，确保释放使用匹配的实现。
    api: NativeApi,
    // 保持回调上下文地址稳定；Drop 先停止原生来源，再由 Rust 释放此字段。
    // The allocation stays stable across moves. Drop stops the native source
    // before Rust drops this field, including an update racing cancellation.
    _trigger: Box<RefreshTrigger>,
}

// SAFETY: The handle has one owner, and Network.framework delivers callbacks
// on a private serial queue. Stop synchronizes with that queue on any owning
// thread. The callback only borrows the Send + Sync RefreshTrigger.
// 允许转移所有权到其他线程；安全性依赖唯一句柄和 stop 对私有队列的同步。
unsafe impl Send for NativeNetworkMonitor {}
// SAFETY: Shared references cannot access or mutate the native handle. Stop
// requires exclusive ownership through Drop; the trigger itself is Sync.
// 共享引用不操作原生句柄；回调只借用线程安全的 RefreshTrigger。
unsafe impl Sync for NativeNetworkMonitor {}

impl NativeNetworkMonitor {
    // 使用真实 C 适配层启动网络路径监听，只发送重采样提示而不判断网络在线状态。
    pub(crate) fn start(trigger: RefreshTrigger) -> io::Result<Self> {
        Self::start_with(
            trigger,
            NativeApi {
                start: open_net_path_monitor_start,
                stop: open_net_path_monitor_stop,
            },
        )
    }

    // 固定触发器地址后调用可注入的启动入口；失败时先回收部分句柄，再释放上下文。
    fn start_with(trigger: RefreshTrigger, api: NativeApi) -> io::Result<Self> {
        let trigger = Box::new(trigger);
        let context = ptr::from_ref(trigger.as_ref()).cast_mut().cast();
        let mut handle = ptr::null_mut();
        // SAFETY: The output is writable and context points to a stable, live
        // allocation owned here. NativeApi follows the private ABI contract.
        let status = unsafe { (api.start)(Some(notify), context, &mut handle) };
        if status != 0 {
            if !handle.is_null() {
                // SAFETY: Defensively reclaim a partial native handle while
                // context is still alive, using its matching stop function.
                unsafe { (api.stop)(handle) };
            }
            return Err(io::Error::other(format!(
                "macOS network-change monitor initialization failed (native status {status})"
            )));
        }
        let handle = NonNull::new(handle).ok_or_else(|| {
            io::Error::other("macOS network-change monitor returned a null handle")
        })?;
        Ok(Self {
            handle,
            api,
            _trigger: trigger,
        })
    }
}

impl Drop for NativeNetworkMonitor {
    // 同步停止原生源，确保已排队或正执行的回调不再访问随后释放的触发器。
    // 此释放过程可能等待取消完成，不能在私有原生回调队列上执行。
    fn drop(&mut self) {
        // SAFETY: This is the unique handle owner. The callback only notifies
        // a channel and cannot drop this owner on its private native queue.
        // Stop waits for cancellation completion and drains that queue before
        // returning, so no callback can access _trigger after it is dropped.
        unsafe { (self.api.stop)(self.handle.as_ptr()) };
    }
}

// 将原生通知转为不等待容量的合并提示；空上下文直接返回，隔离可展开的 panic。
// 非空 context 必须指向仍存活的 RefreshTrigger，不能在此回调中停止其原生所有者。
unsafe extern "C" fn notify(context: *mut c_void) {
    if context.is_null() {
        return;
    }
    // Contain unwinding at the ABI boundary. No application callback runs here.
    let result = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: The native source borrows this allocation only between start
        // and synchronous stop. It never changes the pointer or its contents.
        let trigger = unsafe { &*context.cast::<RefreshTrigger>() };
        let _ = trigger.notify();
    }));
    if result.is_err() {
        // Fallible I/O avoids a second unwind while diagnosing an ABI failure.
        let _ = io::stderr()
            .lock()
            .write_all(b"open-net: macOS network-change callback failed\n");
    }
}

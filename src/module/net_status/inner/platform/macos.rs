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

type NativeCallback = unsafe extern "C" fn(*mut c_void);

unsafe extern "C" {
    fn open_net_path_monitor_start(
        notify: Option<NativeCallback>,
        context: *mut c_void,
        output: *mut *mut c_void,
    ) -> c_int;
    fn open_net_path_monitor_stop(monitor: *mut c_void);
}

/// Private ABI boundary, also injectable by the lifecycle tests.
///
/// Start may borrow context until stop returns. A null output must never retain
/// context; each non-null output must accept exactly one synchronous stop.
/// Stop must finish every callback before returning and may run on any thread
/// except the private callback queue. These requirements also apply to tests.
#[derive(Clone, Copy, Debug)]
struct NativeApi {
    start: unsafe extern "C" fn(Option<NativeCallback>, *mut c_void, *mut *mut c_void) -> c_int,
    stop: unsafe extern "C" fn(*mut c_void),
}

#[derive(Debug)]
pub(crate) struct NativeNetworkMonitor {
    handle: NonNull<c_void>,
    api: NativeApi,
    // The allocation stays stable across moves. Drop stops the native source
    // before Rust drops this field, including an update racing cancellation.
    _trigger: Box<RefreshTrigger>,
}

// SAFETY: The handle has one owner, and Network.framework delivers callbacks
// on a private serial queue. Stop synchronizes with that queue on any owning
// thread. The callback only borrows the Send + Sync RefreshTrigger.
unsafe impl Send for NativeNetworkMonitor {}
// SAFETY: Shared references cannot access or mutate the native handle. Stop
// requires exclusive ownership through Drop; the trigger itself is Sync.
unsafe impl Sync for NativeNetworkMonitor {}

impl NativeNetworkMonitor {
    pub(crate) fn start(trigger: RefreshTrigger) -> io::Result<Self> {
        Self::start_with(
            trigger,
            NativeApi {
                start: open_net_path_monitor_start,
                stop: open_net_path_monitor_stop,
            },
        )
    }

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
    fn drop(&mut self) {
        // SAFETY: This is the unique handle owner. The callback only notifies
        // a channel and cannot drop this owner on its private native queue.
        // Stop waits for cancellation completion and drains that queue before
        // returning, so no callback can access _trigger after it is dropped.
        unsafe { (self.api.stop)(self.handle.as_ptr()) };
    }
}

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

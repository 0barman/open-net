//! Platform-specific network-change trigger sources.
//!
//! Only macOS has an additional native source. Other platforms expose a
//! permanently pending monitor so the main monitor loop keeps one shared,
//! reviewable control flow without changing their observable behavior.

#[cfg(target_os = "macos")]
mod macos;

#[cfg(target_os = "macos")]
use super::refresh_trigger::{self, RefreshTriggerEvent, RefreshTriggerReceiver};

#[derive(Debug)]
pub(crate) struct PlatformNetworkMonitor {
    #[cfg(target_os = "macos")]
    _native: macos::NativeNetworkMonitor,
    #[cfg(target_os = "macos")]
    changes: RefreshTriggerReceiver,
}

impl PlatformNetworkMonitor {
    #[cfg(target_os = "macos")]
    pub(crate) fn start() -> Self {
        let (trigger, changes) = refresh_trigger::channel();
        let native = macos::NativeNetworkMonitor::start(trigger);
        Self {
            _native: native,
            changes,
        }
    }

    #[cfg(not(target_os = "macos"))]
    pub(crate) fn start() -> Self {
        Self {}
    }

    /// Wait for a platform hint. `true` requests a netwatch refresh; `false`
    /// means the native callback channel closed and has fused permanently.
    #[cfg(target_os = "macos")]
    pub(crate) async fn changed(&mut self) -> bool {
        matches!(self.changes.recv().await, RefreshTriggerEvent::Notified)
    }

    #[cfg(not(target_os = "macos"))]
    pub(crate) async fn changed(&mut self) -> bool {
        std::future::pending().await
    }
}

#[cfg(all(test, not(target_os = "macos")))]
mod tests {
    use super::PlatformNetworkMonitor;

    #[tokio::test]
    async fn non_macos_monitor_never_manufactures_a_change() {
        let mut monitor = PlatformNetworkMonitor::start();

        tokio::select! {
            biased;
            _ = monitor.changed() => panic!("non-macOS platform monitor must remain pending"),
            _ = std::future::ready(()) => {}
        }
    }
}

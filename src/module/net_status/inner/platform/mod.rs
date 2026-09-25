//! Platform-specific network-change trigger sources.
//!
//! Only macOS has an additional native source. Other platforms expose a
//! permanently pending monitor so the main monitor loop keeps one shared,
//! reviewable control flow without changing their observable behavior.

#[cfg(target_os = "macos")]
mod macos;

#[cfg(target_os = "macos")]
use super::refresh_trigger::{self, RefreshTriggerEvent, RefreshTriggerReceiver};

// 平台网络变化提示源；macOS 持有原生监听器，其他平台使用永久等待的占位实现。
#[derive(Debug)]
pub(crate) struct PlatformNetworkMonitor {
    // 保持原生监听器存活；启动失败为 None，释放时同步停止原生回调。
    #[cfg(target_os = "macos")]
    _native: Option<macos::NativeNetworkMonitor>,
    // 接收合并后的刷新提示，并记录原生提示通道是否已关闭。
    #[cfg(target_os = "macos")]
    changes: RefreshTriggerReceiver,
}

impl PlatformNetworkMonitor {
    // 创建提示通道并启动 macOS 原生来源；失败会记录诊断而不替代 netwatch 状态。
    #[cfg(target_os = "macos")]
    pub(crate) fn start() -> Self {
        let (trigger, changes) = refresh_trigger::channel();
        let native = macos::NativeNetworkMonitor::start(trigger);
        Self::from_native(native, changes)
    }

    // 将原生启动结果和接收端组合为监听器，允许测试注入启动失败及已排队的提示。
    #[cfg(target_os = "macos")]
    fn from_native(
        native: std::io::Result<macos::NativeNetworkMonitor>,
        changes: RefreshTriggerReceiver,
    ) -> Self {
        let native = match native {
            Ok(native) => Some(native),
            Err(error) => {
                // Native events are supplementary hints. Keep netwatch and
                // its existing status semantics alive if this source fails.
                crate::log_e!(crate::common::log::log_def::LogType::Engine;
                    "network_status_native_monitor_start", "error",
                    crate::common::log::summary::error(&error));
                None
            }
        };
        Self {
            _native: native,
            changes,
        }
    }

    // 为没有额外原生提示源的平台创建无状态占位监听器。
    #[cfg(not(target_os = "macos"))]
    pub(crate) fn start() -> Self {
        Self {}
    }

    // 等待原生提示：true 表示请求重采样，false 仅报告一次通道关闭，之后永久等待。
    /// Wait for a platform hint. `true` requests a netwatch refresh; `false`
    /// means the native callback channel closed and has fused permanently.
    #[cfg(target_os = "macos")]
    pub(crate) async fn changed(&mut self) -> bool {
        matches!(self.changes.recv().await, RefreshTriggerEvent::Notified)
    }

    // 在不支持原生提示的平台始终保持待定，让共享 select 循环由其他分支驱动。
    #[cfg(not(target_os = "macos"))]
    pub(crate) async fn changed(&mut self) -> bool {
        std::future::pending().await
    }
}

#[cfg(all(test, not(target_os = "macos")))]
mod tests {
    use super::PlatformNetworkMonitor;

    // 验证非 macOS 占位监听器不会虚构网络变化或产生立即就绪的忙循环。
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

#[cfg(all(test, target_os = "macos"))]
#[path = "platform_tests.rs"]
mod platform_tests;

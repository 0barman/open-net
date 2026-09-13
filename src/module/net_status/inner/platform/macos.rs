//! macOS `NWPathMonitor` adapter.
//!
//! The callback deliberately ignores every property of `PathUpdate`. Its only
//! responsibility is a non-blocking hint to resample the authoritative
//! `netwatch::State`.

use networkframework::path_monitor::{start_path_monitor, PathMonitor};

use super::super::refresh_trigger::RefreshTrigger;

#[derive(Debug)]
pub(crate) struct NativeNetworkMonitor {
    _monitor: PathMonitor,
}

impl NativeNetworkMonitor {
    pub(crate) fn start(trigger: RefreshTrigger) -> Self {
        let monitor = start_path_monitor(move |_update| {
            let _ = trigger.notify();
        });
        Self { _monitor: monitor }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::NativeNetworkMonitor;
    use crate::module::net_status::inner::refresh_trigger::{self, RefreshTriggerEvent};

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn native_monitor_owner_is_send_and_sync() {
        assert_send_sync::<NativeNetworkMonitor>();
    }

    async fn wait_until_callback_sender_closes(
        changes: &mut crate::module::net_status::inner::refresh_trigger::RefreshTriggerReceiver,
    ) {
        loop {
            let event = tokio::time::timeout(Duration::from_secs(10), changes.recv())
                .await
                .expect("PathMonitor drop should release its callback sender");
            match event {
                RefreshTriggerEvent::Notified => continue,
                RefreshTriggerEvent::ChannelClosed => break,
            }
        }
    }

    #[tokio::test]
    async fn native_monitor_delivers_an_initial_hint_and_releases_callback_on_drop() {
        let (trigger, mut changes) = refresh_trigger::channel();
        let monitor = NativeNetworkMonitor::start(trigger);

        let initial = tokio::time::timeout(Duration::from_secs(10), changes.recv())
            .await
            .expect("NWPathMonitor should deliver its initial path");
        assert_eq!(initial, RefreshTriggerEvent::Notified);

        drop(monitor);
        wait_until_callback_sender_closes(&mut changes).await;
    }

    #[tokio::test]
    async fn repeated_native_monitor_start_and_drop_releases_every_callback() {
        for _ in 0..64 {
            let (trigger, mut changes) = refresh_trigger::channel();
            let monitor = NativeNetworkMonitor::start(trigger);
            drop(monitor);
            wait_until_callback_sender_closes(&mut changes).await;
        }
    }
}

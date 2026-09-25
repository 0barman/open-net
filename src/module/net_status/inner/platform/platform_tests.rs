use std::io;
use std::time::Duration;

use super::{refresh_trigger, PlatformNetworkMonitor};
use crate::module::net_status::inner::refresh_trigger::TriggerOutcome;

// 在十秒内核对平台提示结果，区分需要刷新与原生通道已关闭。
async fn check_change(monitor: &mut PlatformNetworkMonitor, expected: bool) -> io::Result<()> {
    let actual = tokio::time::timeout(Duration::from_secs(10), monitor.changed())
        .await
        .map_err(|error| io::Error::new(io::ErrorKind::TimedOut, error))?;
    if actual != expected {
        return Err(io::Error::other(format!(
            "expected platform changed() = {expected}, received {actual}"
        )));
    }
    Ok(())
}

// 只轮询一次 changed，验证关闭已被报告的来源保持待定而不会驱动忙循环。
async fn check_pending(monitor: &mut PlatformNetworkMonitor) -> io::Result<()> {
    // Poll once without relying on elapsed wall-clock time. A fused native
    // channel must never produce another ready result or spin the monitor loop.
    tokio::select! {
        biased;
        actual = monitor.changed() => Err(io::Error::other(format!(
            "closed platform source became ready again with {actual}"
        ))),
        _ = std::future::ready(()) => Ok(()),
    }
}

// 验证原生启动失败时不持有监听器，关闭只报告一次，后续等待永久待定。
#[tokio::test]
async fn failed_native_start_disables_source_and_reports_closure_only_once() -> io::Result<()> {
    let (trigger, changes) = refresh_trigger::channel();
    drop(trigger);
    let mut monitor = PlatformNetworkMonitor::from_native(
        Err(io::Error::other("injected native initialization failure")),
        changes,
    );
    if monitor._native.is_some() {
        return Err(io::Error::other("failed native source retained a monitor"));
    }

    check_change(&mut monitor, false).await?;
    check_pending(&mut monitor).await?;
    check_pending(&mut monitor).await
}

// 验证启动失败之前已排队的提示先交付，然后报告关闭并进入永久等待。
#[tokio::test]
async fn failed_native_start_preserves_hint_queued_before_failure() -> io::Result<()> {
    let (trigger, changes) = refresh_trigger::channel();
    if trigger.notify() != TriggerOutcome::Queued {
        return Err(io::Error::other("failed to queue native startup hint"));
    }
    drop(trigger);
    let mut monitor = PlatformNetworkMonitor::from_native(
        Err(io::Error::other("injected failure after an initial hint")),
        changes,
    );
    if monitor._native.is_some() {
        return Err(io::Error::other("failed native source retained a monitor"));
    }

    check_change(&mut monitor, true).await?;
    check_change(&mut monitor, false).await?;
    check_pending(&mut monitor).await
}

// 验证真实原生来源发出刷新提示；释放唯一所有者后接收端报告关闭并停止就绪。
#[tokio::test]
async fn real_native_source_requests_refresh_and_fuses_after_source_drop() -> io::Result<()> {
    let mut monitor = PlatformNetworkMonitor::start();
    if monitor._native.is_none() {
        return Err(io::Error::other("real native source failed to initialize"));
    }
    check_change(&mut monitor, true).await?;

    // Keep the platform receiver available after releasing the sole native
    // owner, so closure proves the native callback relinquished its sender.
    drop(monitor._native.take());
    tokio::time::timeout(Duration::from_secs(10), async {
        while monitor.changed().await {}
    })
    .await
    .map_err(|error| io::Error::new(io::ErrorKind::TimedOut, error))?;
    check_pending(&mut monitor).await
}

use crate::common::log::log_def::LogType;
#[cfg(not(target_arch = "wasm32"))]
/// Sleeps asynchronously for `timeout` on the current Tokio runtime.
///
/// # Returns
///
/// This async function returns `()` after the duration elapses.
pub async fn sleep(timeout: std::time::Duration) {
    crate::log_t!(LogType::Common; "sleep", "timeout_ms", timeout.as_millis());
    tokio::time::sleep(timeout).await;
}

#[cfg(not(target_arch = "wasm32"))]
/// Returns the current Unix timestamp in milliseconds.
///
/// # Returns
///
/// Milliseconds since the Unix epoch, or `0` if the system clock is before the epoch.
pub fn now() -> i64 {
    crate::log_t!(LogType::Common; "now");
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

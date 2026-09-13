use crate::log::log_def::LogType;
#[cfg(not(target_arch = "wasm32"))]
/// Sleeps asynchronously for `timeout` on the current Tokio runtime.
///
/// # Returns
///
/// This async function returns `()` after the duration elapses.
///
/// # Examples
///
/// ```no_run
/// # async fn demo() {
/// on_common::platform::sleep(std::time::Duration::from_millis(10)).await;
/// # }
/// ```
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
///
/// # Examples
///
/// ```
/// let now = on_common::platform::now();
/// assert!(now >= 0);
/// ```
pub fn now() -> i64 {
    crate::log_t!(LogType::Common; "now");
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

use crate::common::log::log_def::LogType;
use std::future::Future;

#[cfg(not(target_arch = "wasm32"))]
use tokio::task::JoinHandle;

#[cfg(target_arch = "wasm32")]
use wasm_bindgen_futures::spawn_local;

// #[cfg(target_arch = "wasm32")]
// pub(crate) fn spawn<F>(future: F)
// where
//     F: Future<Output = ()> + 'static,
// {
//     spawn_local(future);
// }

#[cfg(not(target_arch = "wasm32"))]
/// Spawns a future on the current Tokio runtime.
///
/// # Returns
///
/// A [`tokio::task::JoinHandle`] that can be awaited for the future output.
pub fn spawn<T>(future: T) -> JoinHandle<T::Output>
where
    T: Future + Send + 'static,
    T::Output: Send + 'static,
{
    crate::log_t!(LogType::Common; "spawn", "future_type", std::any::type_name_of_val(&future));
    tokio::spawn(future)
}

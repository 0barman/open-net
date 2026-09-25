use super::LogInfo;

/// Callback invoked for each record accepted by a [`super::LogSubscription`].
/// Callbacks execute sequentially on the subscription's dedicated worker thread.
pub type LogListener = Box<dyn Fn(LogInfo) + Send + Sync + 'static>;

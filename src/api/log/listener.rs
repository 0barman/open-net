use super::LogInfo;

/// Callback invoked for each record accepted by a [`super::LogSubscription`].
/// Callbacks execute sequentially on the subscription's dedicated worker thread.
pub type LogListener = Box<dyn Fn(LogInfo) + Send + Sync + 'static>;

/// Callback receiving a shared record without cloning its string fields.
/// Callbacks execute sequentially on the subscription's dedicated worker thread.
/// Retaining the record after delivery does not consume the subscription's queue
/// budget; the application is responsible for bounding any such retained storage.
pub type SharedLogListener = Box<dyn Fn(std::sync::Arc<LogInfo>) + Send + Sync + 'static>;

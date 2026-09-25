use tokio::sync::oneshot;

/// Work submitted to the client's original bounded callback dispatcher.
pub(crate) enum CallbackEvent {
    SessionJob {
        session_id: crate::ws::SessionId,
        job: crate::module::ws_client::listener_executor::CallbackJob,
    },
    /// Finish queued callbacks when the entire client shuts down.
    DataDrain { delivered: oneshot::Sender<()> },
}

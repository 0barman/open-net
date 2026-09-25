use super::*;

impl WSClientInner {
    /// The target identity remains fixed while the command waits for its worker.
    pub(crate) async fn close_session(
        &self,
        session: &Arc<ConnectionSession>,
        frame: Option<crate::ws::CloseFrame>,
    ) -> Result<crate::ws::SessionEnd, NetError> {
        if let Some(result) = session.terminal_result() {
            return result;
        }
        let deadline = tokio::time::Instant::now()
            .checked_add(self.close_timeout)
            .ok_or_else(|| {
                NetError::config("close_timeout", "cannot represent a close deadline")
                    .with_stage(crate::error::ErrorStage::Close)
            })?;
        if !session.request_close(frame.clone(), deadline)? {
            return session.closed().await;
        }
        let command = ClientCommand::CloseSession {
            session: Arc::clone(session),
            frame,
            deadline,
        };
        // The session signal is authoritative. This hint never waits for command
        // capacity, and the worker observes a full queue's request independently.
        match self.command_tx.try_send(command) {
            Ok(()) | Err(mpsc::error::TrySendError::Full(_)) => session.closed().await,
            Err(mpsc::error::TrySendError::Closed(_)) => match session.terminal_result() {
                Some(result) => result,
                None => Err(NetError::from(crate::error::ErrorKind::EngineDropped)
                    .with_stage(crate::error::ErrorStage::Close)),
            },
        }
    }
}

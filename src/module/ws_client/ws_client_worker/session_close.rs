use super::*;

impl WSClientWorker {
    pub(super) async fn close_target_session(
        &mut self,
        session: Arc<ConnectionSession>,
        frame: Option<crate::ws::CloseFrame>,
        deadline: Instant,
    ) -> Result<(), NetError> {
        if let Some(result) = session.terminal_result() {
            return result.map(|_| ());
        }
        if !self
            .context_session()
            .is_some_and(|current| Arc::ptr_eq(&current, &session))
        {
            return Err(NetError::from(crate::error::ErrorKind::Closed)
                .with_stage(crate::error::ErrorStage::Close));
        }

        let cancellation = NetError::from(crate::error::ErrorKind::Cancelled);
        self.end_runtime(cancellation.clone(), TaskEndCause::Cancelled);
        self.begin_session_closing();
        self.set_status(ConnectionStatus::Closing).await;
        self.cancel_connect();
        let graceful = !session.cancel_token().is_cancelled();
        let stopped = self
            .stop_active_io_with_close(graceful, frame, Some(deadline))
            .await;

        fail_requests(
            self.urgent_queue.drain_with_error(cancellation.clone()),
            cancellation.clone(),
        );
        fail_requests(
            self.queue.drain_with_error(cancellation.clone()),
            cancellation,
        );

        let (reason, failure) = if session.cancel_token().is_cancelled() {
            (
                TerminationReason::Cancelled,
                Some(Self::cancellation_failure()),
            )
        } else {
            match stopped.result {
                Ok(()) => (TerminationReason::LocalClose, None),
                Err(error) => (
                    TerminationReason::IoFailure,
                    Some(ConnectionFailure::new(
                        error,
                        ConnectStage::WebSocketIo,
                        None,
                        false,
                    )),
                ),
            }
        };
        let io_end_kind = failure
            .as_ref()
            .filter(|_| reason == TerminationReason::IoFailure)
            .map(|failure| match failure.error().context().io_end {
                Some(kind) => kind,
                None => IoEndKind::Other,
            });
        self.set_status(ConnectionStatus::Idle).await;
        let published = session.terminate_with_details(
            reason,
            failure,
            ConnectionTerminationDetails {
                peer_close: stopped.peer_close,
                io_end_kind,
            },
        );
        self.connect_target = None;
        published?;
        match session.terminal_result() {
            Some(result) => result.map(|_| ()),
            None => Err(NetError::from(crate::error::ErrorKind::Internal)
                .with_stage(crate::error::ErrorStage::Close)),
        }
    }
}

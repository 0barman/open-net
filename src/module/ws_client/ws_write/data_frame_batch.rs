//! Scoped writer adapter for bounded data-frame coalescing.
//!
//! In data mode, a flush may finish only the current fragment's enqueue step.
//! The owning writer forces the first/final frame and every control boundary to
//! the real sink; it never reports the message Written before the final drain.
//! This adapter must not escape that writer or be used as a general-purpose Sink.
use super::*;
use std::task::{Context, Poll};

const MAX_FRAMES: usize = 4;
const TARGET_BYTES: usize = 128 * 1024;
// Checked at safe frame boundaries, not an independent real-time flush deadline.
const MAX_DEFER: Duration = Duration::from_micros(250);
const CONTROL_RESERVE: usize = 125 + 14;

pub(super) struct DataFrameBatch<'a, W> {
    inner: &'a mut W,
    capacity: usize,
    pending_frames: usize,
    pending_bytes: usize,
    oldest: Option<TimeoutBudget>,
    timer: Option<Pin<Box<tokio::time::Sleep>>>,
    expired: Option<TimeoutBudget>,
    began: Option<Instant>,
    data_mode: bool,
    force: bool,
    incoming_wire_bytes: usize,
    incoming_deadline: Option<TimeoutBudget>,
}

impl<'a, W> DataFrameBatch<'a, W> {
    pub(super) fn new(inner: &'a mut W, capacity: usize) -> Self {
        Self {
            inner,
            capacity,
            pending_frames: 0,
            pending_bytes: 0,
            oldest: None,
            timer: (capacity > 0).then(|| Box::pin(tokio::time::sleep(Duration::from_secs(3600)))),
            expired: None,
            began: None,
            data_mode: false,
            force: false,
            incoming_wire_bytes: 0,
            incoming_deadline: None,
        }
    }
    pub(super) fn effective_budget(&self, request: TimeoutBudget) -> TimeoutBudget {
        TimeoutBudget::earlier(Some(request), self.oldest).unwrap_or(request)
    }
    pub(super) fn selected_flush_timeout(&self, request: TimeoutBudget) -> Option<TimeoutBudget> {
        self.expired.filter(|budget| budget.at < request.at)
    }
    pub(super) fn expired_frame_budget(&self, request: TimeoutBudget) -> Option<TimeoutBudget> {
        self.expired
            .or(self.oldest)
            .filter(|budget| budget.at < request.at && Instant::now().into_std() >= budget.at)
    }
    pub(super) fn needs_capacity_flush(&self, payload: usize) -> bool {
        self.pending_frames > 0
            && self
                .pending_bytes
                .saturating_add(payload)
                .saturating_add(14 + CONTROL_RESERVE)
                > self.capacity
    }
    pub(super) fn begin_data(
        &mut self,
        payload: usize,
        force: bool,
        deadline: Option<TimeoutBudget>,
    ) {
        self.data_mode = true;
        self.incoming_wire_bytes = payload.saturating_add(14);
        self.incoming_deadline = deadline;
        self.force =
            force || self.incoming_wire_bytes.saturating_add(CONTROL_RESERVE) > self.capacity;
    }
    pub(super) fn end_data(&mut self) {
        self.data_mode = false;
    }
}

impl<W: Sink<Message, Error = WsError> + Unpin> DataFrameBatch<'_, W> {
    fn flush_inner(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), WsError>> {
        // A pending data frame keeps its own deadline even when a control write
        // drains it. Once that drain succeeds, the control retains its original
        // request/control budget; the old frame deadline cannot cancel it later.
        if let Some(budget) = self.oldest {
            if self
                .timer
                .as_mut()
                .is_some_and(|timer| std::future::Future::poll(timer.as_mut(), cx).is_ready())
            {
                self.expired = Some(budget);
                return Poll::Ready(Err(WsError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "pending data-frame batch expired",
                ))));
            }
        }
        let result = Pin::new(&mut *self.inner).poll_flush(cx);
        if matches!(&result, Poll::Ready(Ok(()))) {
            self.pending_frames = 0;
            self.pending_bytes = 0;
            self.oldest = None;
            self.began = None;
        }
        result
    }
}

impl<W: Sink<Message, Error = WsError> + Unpin> Sink<Message> for DataFrameBatch<'_, W> {
    type Error = WsError;
    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), WsError>> {
        // Control writes always drain preceding data before accepting the control.
        if !self.data_mode && self.pending_frames > 0 {
            match self.flush_inner(cx) {
                Poll::Ready(Ok(())) => {}
                other => return other,
            }
        }
        Pin::new(&mut *self.inner).poll_ready(cx)
    }
    fn start_send(mut self: Pin<&mut Self>, item: Message) -> Result<(), WsError> {
        Pin::new(&mut *self.inner).start_send(item)?;
        if self.data_mode {
            if self.pending_frames == 0 {
                self.oldest = self.incoming_deadline;
                if let Some(budget) = self.oldest {
                    if let Some(timer) = self.timer.as_mut() {
                        timer.as_mut().reset(Instant::from_std(budget.at));
                    }
                }
                self.began = Some(Instant::now());
            }
            self.pending_frames += 1;
            self.pending_bytes = self.pending_bytes.saturating_add(self.incoming_wire_bytes);
        }
        Ok(())
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), WsError>> {
        if self.data_mode
            && !self.force
            && self.pending_frames < MAX_FRAMES
            && self.pending_bytes < TARGET_BYTES
            && self
                .began
                .is_some_and(|at| Instant::now().saturating_duration_since(at) < MAX_DEFER)
        {
            return Poll::Ready(Ok(()));
        }
        self.flush_inner(cx)
    }
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), WsError>> {
        match self.flush_inner(cx) {
            Poll::Ready(Ok(())) => Pin::new(&mut *self.inner).poll_close(cx),
            other => other,
        }
    }
}

pub(super) async fn flush_pending_data_with_phase<W: Sink<Message, Error = WsError> + Unpin>(
    batch: &mut DataFrameBatch<'_, W>,
    budget: TimeoutBudget,
    connection_cancel: &CancellationToken,
    dispatch_cancel: &CancellationToken,
    phase: &OperationControl,
) -> Result<(), RequestWriteFailure> {
    let original_budget = budget;
    let budget = batch.effective_budget(budget);
    let selected_timeout = || {
        if budget.at < original_budget.at {
            RequestWriteFailure::connection(budget_delivery_unknown(budget), true)
        } else {
            RequestWriteFailure::deadline(true, true, budget)
        }
    };
    let deadline = Instant::from_std(budget.at);
    if Instant::now() >= deadline {
        return Err(selected_timeout());
    }
    #[cfg(feature = "quality-doctor")]
    let _flush = phase
        .quality()
        .map(|probe| probe.phase(crate::api::quality::PhaseKind::Flush));
    let _ = phase;
    tokio::select! {
        biased;
        _ = dispatch_cancel.cancelled() => Err(RequestWriteFailure::cancelled(true)),
        result = tokio::time::timeout_at(deadline, batch.flush()) => match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(RequestWriteFailure::connection(unknown_delivery_error(error), true)),
            Err(_) => Err(selected_timeout()),
        },
        _ = connection_cancel.cancelled() => Err(RequestWriteFailure::connection(
            NetError::from(crate::error::ErrorKind::Cancelled), true)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[derive(Default)]
    struct ReadySink {
        starts: usize,
        flushes: usize,
        block_flush: bool,
        observed_starts: Option<Arc<AtomicUsize>>,
        observed_flushes: Option<Arc<AtomicUsize>>,
    }
    impl Sink<Message> for ReadySink {
        type Error = WsError;
        fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), WsError>> {
            Poll::Ready(Ok(()))
        }
        fn start_send(mut self: Pin<&mut Self>, _: Message) -> Result<(), WsError> {
            self.starts += 1;
            if let Some(observed) = &self.observed_starts {
                observed.store(self.starts, Ordering::SeqCst);
            }
            Ok(())
        }
        fn poll_flush(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), WsError>> {
            self.flushes += 1;
            if self.block_flush {
                Poll::Pending
            } else {
                if let Some(observed) = &self.observed_flushes {
                    observed.store(self.flushes, Ordering::SeqCst);
                }
                Poll::Ready(Ok(()))
            }
        }
        fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), WsError>> {
            Poll::Ready(Ok(()))
        }
    }
    fn budget(seconds: u64) -> TimeoutBudget {
        TimeoutBudget::new(
            (Instant::now() + Duration::from_secs(seconds)).into_std(),
            crate::ws::WsTimeoutPhase::Write,
        )
    }
    async fn data(batch: &mut DataFrameBatch<'_, ReadySink>, force: bool, deadline: TimeoutBudget) {
        batch.begin_data(32 * 1024, force, Some(deadline));
        batch
            .send(Message::Binary(Bytes::from_static(b"synthetic")))
            .await
            .unwrap();
        batch.end_data();
    }
    #[tokio::test(start_paused = true)]
    async fn flush_batch_preserves_oldest_deadline_and_final_flush() {
        let mut inner = ReadySink::default();
        let mut batch = DataFrameBatch::new(&mut inner, 4 * 1024 * 1024);
        let first = budget(1);
        data(&mut batch, false, first).await;
        assert_eq!(batch.inner.flushes, 0);
        tokio::time::advance(Duration::from_micros(1)).await;
        data(&mut batch, false, budget(2)).await;
        assert_eq!(batch.effective_budget(budget(10)), first);
        data(&mut batch, true, budget(3)).await;
        assert_eq!(batch.inner.flushes, 1);
        assert_eq!(batch.pending_frames, 0);
        assert_eq!(batch.oldest, None);
    }
    #[tokio::test(start_paused = true)]
    async fn flush_batch_is_bounded_and_control_drains_it() {
        let mut inner = ReadySink::default();
        let mut batch = DataFrameBatch::new(&mut inner, 4 * 1024 * 1024);
        for _ in 0..4 {
            data(&mut batch, false, budget(5)).await;
        }
        assert_eq!(batch.inner.flushes, 1);
        data(&mut batch, false, budget(5)).await;
        assert_eq!(batch.pending_frames, 1);
        batch.send(Message::Ping(Bytes::new())).await.unwrap();
        assert_eq!(batch.pending_frames, 0);
        // Before accepting a control and after accepting it, both flushes are real.
        assert_eq!(batch.inner.flushes, 3);
    }
    #[tokio::test(start_paused = true)]
    async fn flush_batch_small_legal_buffer_preserves_per_frame_flush() {
        let mut inner = ReadySink::default();
        let mut batch = DataFrameBatch::new(&mut inner, 32 * 1024 + 14);
        for _ in 0..4 {
            data(&mut batch, false, budget(5)).await;
        }
        assert_eq!(batch.inner.flushes, 4);
        assert_eq!(batch.pending_bytes, 0);
    }
    #[tokio::test(start_paused = true)]
    async fn flush_batch_time_bound_and_capacity_check_are_effective() {
        let mut inner = ReadySink::default();
        let mut batch = DataFrameBatch::new(&mut inner, 64 * 1024 + 14);
        data(&mut batch, false, budget(5)).await;
        assert!(batch.needs_capacity_flush(32 * 1024));
        tokio::time::advance(MAX_DEFER).await;
        batch.begin_data(0, false, Some(budget(5)));
        batch.flush().await.unwrap();
        assert_eq!(batch.inner.flushes, 1);
    }
    #[tokio::test(start_paused = true)]
    async fn oldest_unflushed_frame_timeout_preserves_idempotent_retry_policy() {
        let starts = Arc::new(AtomicUsize::new(0));
        let flushes = Arc::new(AtomicUsize::new(0));
        let mut sink = ReadySink {
            observed_starts: Some(starts.clone()),
            observed_flushes: Some(flushes.clone()),
            ..ReadySink::default()
        };
        let (_tx, mut rx) = mpsc::channel(1);
        let mut heartbeat = HeartbeatSchedule::new(None).unwrap();
        let state = HeartbeatState::new(42);
        let connection_cancel = CancellationToken::new();
        let dispatch_cancel = CancellationToken::new();
        let mut sending = Box::pin(super::super::send_request_message(
            &mut sink,
            Message::Binary(Bytes::from(vec![0; 1024 * 1024])),
            Some(32768),
            MAX_READY_CONTROLS_PER_BOUNDARY,
            Instant::now() + Duration::from_secs(30),
            &mut rx,
            Duration::from_secs(5),
            Duration::from_secs(1),
            &mut heartbeat,
            &state,
            &connection_cancel,
            &dispatch_cancel,
        ));
        // Observe a real safe boundary, independent of yields per frame: the
        // first frame is flushed and the next three remain in the pending batch.
        for _ in 0..4 {
            if starts.load(Ordering::SeqCst) >= 4 {
                break;
            }
            assert!(sending.as_mut().now_or_never().is_none());
        }
        assert_eq!(starts.load(Ordering::SeqCst), 4);
        assert_eq!(flushes.load(Ordering::SeqCst), 1);
        tokio::time::advance(Duration::from_secs(1)).await;
        let failure = sending
            .now_or_never()
            .expect("expired frame must stop")
            .unwrap_err();
        assert_eq!(
            failure.request_error.kind(),
            crate::error::ErrorKind::DeliveryUnknown
        );
        assert_eq!(
            failure.requeue,
            super::super::RequestRequeue::IdempotentRetry
        );
    }

    struct SlowControlReadySink {
        data: usize,
        observed_data: Arc<AtomicUsize>,
        pings: usize,
        delayed: bool,
        timer: Pin<Box<tokio::time::Sleep>>,
    }
    impl Sink<Message> for SlowControlReadySink {
        type Error = WsError;
        fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), WsError>> {
            if self.data == 4 && !self.delayed {
                if std::future::Future::poll(self.timer.as_mut(), cx).is_pending() {
                    return Poll::Pending;
                }
                self.delayed = true;
            }
            Poll::Ready(Ok(()))
        }
        fn start_send(mut self: Pin<&mut Self>, item: Message) -> Result<(), WsError> {
            if matches!(item, Message::Ping(_)) {
                self.pings += 1;
            } else {
                self.data += 1;
                self.observed_data.store(self.data, Ordering::SeqCst);
            }
            Ok(())
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), WsError>> {
            Poll::Ready(Ok(()))
        }
        fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), WsError>> {
            Poll::Ready(Ok(()))
        }
    }
    #[tokio::test(start_paused = true)]
    async fn completed_data_flush_does_not_leave_stale_deadline_on_control() {
        let started = Instant::now();
        let observed_data = Arc::new(AtomicUsize::new(0));
        let mut sink = SlowControlReadySink {
            data: 0,
            observed_data: observed_data.clone(),
            pings: 0,
            delayed: false,
            timer: Box::pin(tokio::time::sleep(Duration::from_secs(2))),
        };
        let (_tx, mut rx) = mpsc::channel(1);
        let mut heartbeat = HeartbeatSchedule::from_interval(
            tokio::time::interval_at(
                started + Duration::from_millis(1),
                Duration::from_secs(3600),
            ),
            Duration::from_secs(3600),
        );
        let state = HeartbeatState::new(42);
        let connection_cancel = CancellationToken::new();
        let dispatch_cancel = CancellationToken::new();
        let mut sending = Box::pin(super::super::send_request_message(
            &mut sink,
            Message::Binary(Bytes::from(vec![0; 5 * 32768])),
            Some(32768),
            MAX_READY_CONTROLS_PER_BOUNDARY,
            started + Duration::from_secs(30),
            &mut rx,
            Duration::from_secs(5),
            Duration::from_secs(1),
            &mut heartbeat,
            &state,
            &connection_cancel,
            &dispatch_cancel,
        ));
        // Both a per-frame writer and a bounded-yield writer must expose the
        // four-frame boundary before the final frame. Inject the heartbeat only
        // after three continuations are accepted but still awaiting their flush.
        for _ in 0..4 {
            if observed_data.load(Ordering::SeqCst) >= 4 {
                break;
            }
            assert!(sending.as_mut().now_or_never().is_none());
        }
        assert_eq!(observed_data.load(Ordering::SeqCst), 4);
        tokio::time::advance(Duration::from_millis(1)).await;
        sending.await.unwrap();
        assert_eq!(sink.data, 5);
        assert_eq!(sink.pings, 1);
        assert!(started.elapsed() >= Duration::from_secs(2));
    }

    #[tokio::test(start_paused = true)]
    async fn control_flush_observes_pending_data_deadline_independently() {
        let mut inner = ReadySink::default();
        let mut batch = DataFrameBatch::new(&mut inner, 4 * 1024 * 1024);
        let started = Instant::now();
        let oldest = budget(1);
        data(&mut batch, false, oldest).await;
        batch.inner.block_flush = true;
        assert!(batch.flush().await.is_err());
        assert_eq!(started.elapsed(), Duration::from_secs(1));
        assert_eq!(batch.selected_flush_timeout(budget(10)), Some(oldest));
    }

    #[tokio::test(start_paused = true)]
    async fn equal_batch_frame_and_request_deadlines_preserve_request_precedence() {
        let mut sink = ReadySink {
            block_flush: true,
            ..ReadySink::default()
        };
        let (_tx, mut rx) = mpsc::channel(1);
        let mut heartbeat = HeartbeatSchedule::new(None).unwrap();
        let state = HeartbeatState::new(42);
        let connection_cancel = CancellationToken::new();
        let dispatch_cancel = CancellationToken::new();
        let started = Instant::now();
        let failure = super::super::send_request_message(
            &mut sink,
            Message::Binary(Bytes::from(vec![0; 1024 * 1024])),
            Some(32768),
            MAX_READY_CONTROLS_PER_BOUNDARY,
            started + Duration::from_secs(1),
            &mut rx,
            Duration::from_secs(5),
            Duration::from_secs(1),
            &mut heartbeat,
            &state,
            &connection_cancel,
            &dispatch_cancel,
        )
        .await
        .unwrap_err();
        assert_eq!(started.elapsed(), Duration::from_secs(1));
        assert_eq!(
            failure.request_error.kind(),
            crate::error::ErrorKind::TimedOut
        );
        assert_eq!(failure.requeue, super::super::RequestRequeue::Never);
    }

    #[cfg(feature = "quality-doctor")]
    #[tokio::test(start_paused = true)]
    async fn active_quality_probe_keeps_real_per_frame_flush_intervals() {
        use crate::api::quality::{
            PhaseKind, QualityDoctorConfig, QualityLimits, QualityOperationKind, QualityProtocol,
            QualityQuery,
        };
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        struct Gate {
            starts: Arc<AtomicUsize>,
            released: Arc<AtomicBool>,
            completed_flushes: usize,
        }
        impl Sink<Message> for Gate {
            type Error = WsError;
            fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), WsError>> {
                Poll::Ready(Ok(()))
            }
            fn start_send(self: Pin<&mut Self>, _: Message) -> Result<(), WsError> {
                self.starts.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
            fn poll_flush(
                mut self: Pin<&mut Self>,
                _: &mut Context<'_>,
            ) -> Poll<Result<(), WsError>> {
                if self.starts.load(Ordering::SeqCst) == 2 && !self.released.load(Ordering::SeqCst)
                {
                    return Poll::Pending;
                }
                self.completed_flushes += 1;
                Poll::Ready(Ok(()))
            }
            fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), WsError>> {
                Poll::Ready(Ok(()))
            }
        }
        let owner = crate::module::quality::QualityOwner::new(
            QualityDoctorConfig::detailed()
                .with_limits(QualityLimits::default().with_max_phases(256)),
        );
        let doctor = owner.doctor().unwrap();
        let probe = owner
            .context()
            .begin(
                QualityProtocol::WebSocket,
                QualityOperationKind::WsMessage,
                7,
            )
            .unwrap();
        let phase = crate::module::ws_client::v2_test_support::operation(
            &crate::ws::SendOptions::default(),
            false,
            1,
        )
        .unwrap();
        phase.bind_quality(Some(probe));
        phase.enqueue().unwrap();
        assert!(phase.mark_writing(crate::ws::ConnectionId::from_allocated(42)));
        let starts = Arc::new(AtomicUsize::new(0));
        let released = Arc::new(AtomicBool::new(false));
        let mut sink = Gate {
            starts: starts.clone(),
            released: released.clone(),
            completed_flushes: 0,
        };
        let (_tx, mut rx) = mpsc::channel(1);
        let mut heartbeat = HeartbeatSchedule::new(None).unwrap();
        let state = HeartbeatState::new(42);
        let mut recovery = HeartbeatRecovery::new(HeartbeatRetryConfig::default());
        let connection_cancel = CancellationToken::new();
        let dispatch_cancel = CancellationToken::new();
        let mut sending = Box::pin(super::super::send_request_message_with_phase(
            &mut sink,
            Message::Binary(Bytes::from(vec![0; 5 * 32768])),
            Some(32768),
            4 * 1024 * 1024,
            MAX_READY_CONTROLS_PER_BOUNDARY,
            budget(30),
            &mut rx,
            Duration::from_secs(5),
            Duration::from_secs(1),
            &mut heartbeat,
            &state,
            &mut recovery,
            &connection_cancel,
            &dispatch_cancel,
            &phase,
        ));
        assert!(sending.as_mut().now_or_never().is_none());
        assert!(sending.as_mut().now_or_never().is_none());
        assert!(sending.as_mut().now_or_never().is_none());
        assert_eq!(
            starts.load(Ordering::SeqCst),
            2,
            "the second actual flush must remain Pending"
        );
        tokio::time::advance(Duration::from_millis(10)).await;
        released.store(true, Ordering::SeqCst);
        sending.await.unwrap();
        assert_eq!(sink.completed_flushes, 5);
        let snapshot = doctor.snapshot(QualityQuery::default()).unwrap();
        assert_eq!(snapshot.reports().len(), 1);
        let flushes: Vec<_> = snapshot.reports()[0]
            .phases()
            .iter()
            .filter(|p| p.kind() == PhaseKind::Flush)
            .collect();
        assert_eq!(flushes.len(), 5);
        assert!(flushes.iter().any(|p| p
            .duration()
            .is_some_and(|duration| duration >= Duration::from_millis(10))));
    }
}

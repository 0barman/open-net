//! Coalescing bridge between a native network-change callback and `netwatch`.
//!
//! A native callback must never block. The channel therefore has capacity one:
//! once a refresh is pending, further notifications are deliberately folded
//! into that pending request. The receiver owns the only code path that may
//! invoke the asynchronous `netwatch` refresh operation.

use std::future::Future;

#[cfg(any(target_os = "macos", test))]
use tokio::sync::mpsc;
use tokio::sync::oneshot;

#[cfg(any(target_os = "macos", test))]
const TRIGGER_CAPACITY: usize = 1;

#[cfg(any(target_os = "macos", test))]
#[derive(Clone, Debug)]
pub(crate) struct RefreshTrigger {
    sender: mpsc::Sender<()>,
}

#[cfg(any(target_os = "macos", test))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TriggerOutcome {
    Queued,
    Coalesced,
    Closed,
}

#[cfg(any(target_os = "macos", test))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RefreshTriggerEvent {
    Notified,
    ChannelClosed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RefreshWorkOutcome {
    Requested,
    RequestFailed,
    Stopped,
}

#[cfg(any(target_os = "macos", test))]
#[derive(Debug)]
pub(crate) struct RefreshTriggerReceiver {
    receiver: mpsc::Receiver<()>,
    closed: bool,
}

#[cfg(any(target_os = "macos", test))]
pub(crate) fn channel() -> (RefreshTrigger, RefreshTriggerReceiver) {
    let (sender, receiver) = mpsc::channel(TRIGGER_CAPACITY);
    (
        RefreshTrigger { sender },
        RefreshTriggerReceiver {
            receiver,
            closed: false,
        },
    )
}

#[cfg(any(target_os = "macos", test))]
impl RefreshTrigger {
    /// Notify the monitor task without ever blocking the native callback.
    pub(crate) fn notify(&self) -> TriggerOutcome {
        match self.sender.try_send(()) {
            Ok(()) => TriggerOutcome::Queued,
            Err(mpsc::error::TrySendError::Full(())) => TriggerOutcome::Coalesced,
            Err(mpsc::error::TrySendError::Closed(())) => TriggerOutcome::Closed,
        }
    }
}

#[cfg(any(target_os = "macos", test))]
impl RefreshTriggerReceiver {
    /// Wait for one coalesced notification without performing the refresh.
    ///
    /// Keeping the asynchronous refresh outside this future makes it safe to
    /// use `recv` in `tokio::select!`: cancelling an empty `recv` does not
    /// consume a notification, and a completed `recv` selects the branch
    /// before refresh work begins.
    ///
    /// Closure is reported once. Later calls remain pending forever so a
    /// closed native channel cannot turn a monitor `select!` loop into a busy
    /// loop.
    pub(crate) async fn recv(&mut self) -> RefreshTriggerEvent {
        if self.closed {
            return std::future::pending().await;
        }

        match self.receiver.recv().await {
            Some(()) => RefreshTriggerEvent::Notified,
            None => {
                self.closed = true;
                RefreshTriggerEvent::ChannelClosed
            }
        }
    }
}

/// Run one asynchronous refresh unless shutdown is already requested or
/// arrives while the refresh is waiting for capacity in the netwatch actor.
///
/// The biased stop branch guarantees that no new refresh starts when stop and
/// refresh are both ready in the same poll. Refresh errors remain diagnostic
/// only and never manufacture a network state.
pub(crate) async fn request_refresh_or_stop<F, Fut, E>(
    stop_receiver: &mut oneshot::Receiver<()>,
    request: F,
) -> RefreshWorkOutcome
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<(), E>>,
{
    tokio::select! {
        biased;
        _ = &mut *stop_receiver => RefreshWorkOutcome::Stopped,
        result = request() => match result {
            Ok(()) => RefreshWorkOutcome::Requested,
            Err(_) => RefreshWorkOutcome::RequestFailed,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};

    use super::{
        channel, request_refresh_or_stop, RefreshTriggerEvent, RefreshWorkOutcome, TriggerOutcome,
    };

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn native_trigger_handle_is_send_and_sync() {
        assert_send_sync::<super::RefreshTrigger>();
    }

    #[tokio::test]
    async fn stop_wins_when_stop_and_refresh_are_both_ready() {
        let (stop_sender, mut stop_receiver) = tokio::sync::oneshot::channel();
        let _ = stop_sender.send(());

        let request_was_polled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let request_was_polled_in_future = Arc::clone(&request_was_polled);
        let outcome = request_refresh_or_stop(&mut stop_receiver, || async move {
            request_was_polled_in_future.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok::<(), ()>(())
        })
        .await;

        assert_eq!(outcome, RefreshWorkOutcome::Stopped);
        assert!(!request_was_polled.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn stop_cancels_refresh_work_that_is_already_waiting() {
        let (stop_sender, mut stop_receiver) = tokio::sync::oneshot::channel();
        let (started_sender, started_receiver) = tokio::sync::oneshot::channel();

        let task = tokio::spawn(async move {
            request_refresh_or_stop(&mut stop_receiver, || async move {
                let _ = started_sender.send(());
                std::future::pending::<Result<(), ()>>().await
            })
            .await
        });

        started_receiver.await.expect("refresh work should start");
        let _ = stop_sender.send(());
        assert_eq!(
            task.await.expect("refresh/stop task should join"),
            RefreshWorkOutcome::Stopped
        );
    }

    #[tokio::test]
    async fn refresh_result_is_preserved_when_no_stop_arrives() {
        let (_stop_sender, mut stop_receiver) = tokio::sync::oneshot::channel();

        assert_eq!(
            request_refresh_or_stop(&mut stop_receiver, || async { Ok::<(), ()>(()) }).await,
            RefreshWorkOutcome::Requested
        );
        assert_eq!(
            request_refresh_or_stop(&mut stop_receiver, || async { Err::<(), ()>(()) }).await,
            RefreshWorkOutcome::RequestFailed
        );
    }

    #[tokio::test]
    async fn full_channel_coalesces_without_losing_the_pending_request() {
        let (trigger, mut receiver) = channel();
        assert_eq!(trigger.notify(), TriggerOutcome::Queued);
        assert_eq!(trigger.notify(), TriggerOutcome::Coalesced);

        assert_eq!(receiver.recv().await, RefreshTriggerEvent::Notified);
    }

    #[tokio::test]
    async fn update_during_refresh_leaves_one_more_request_pending() {
        let (trigger, mut receiver) = channel();
        assert_eq!(trigger.notify(), TriggerOutcome::Queued);

        assert_eq!(receiver.recv().await, RefreshTriggerEvent::Notified);

        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let refresh = tokio::spawn(async move {
            let _ = started_tx.send(());
            let _ = release_rx.await;
        });
        started_rx.await.expect("refresh should start");
        assert_eq!(trigger.notify(), TriggerOutcome::Queued);
        assert_eq!(trigger.notify(), TriggerOutcome::Coalesced);
        let _ = release_tx.send(());
        refresh.await.expect("refresh task should join");

        assert_eq!(receiver.recv().await, RefreshTriggerEvent::Notified);
    }

    #[tokio::test]
    async fn cancelling_an_empty_recv_does_not_consume_the_next_update() {
        let (trigger, mut receiver) = channel();

        tokio::select! {
            biased;
            _ = receiver.recv() => panic!("an empty receiver must remain pending"),
            _ = std::future::ready(()) => {}
        }

        assert_eq!(trigger.notify(), TriggerOutcome::Queued);
        assert_eq!(receiver.recv().await, RefreshTriggerEvent::Notified);
    }

    #[tokio::test]
    async fn cancelling_refresh_work_cannot_poison_the_receiver() {
        let (trigger, mut receiver) = channel();
        assert_eq!(trigger.notify(), TriggerOutcome::Queued);
        assert_eq!(receiver.recv().await, RefreshTriggerEvent::Notified);

        tokio::select! {
            biased;
            _ = std::future::pending::<()>() => unreachable!(),
            _ = std::future::ready(()) => {}
        }

        assert_eq!(trigger.notify(), TriggerOutcome::Queued);
        assert_eq!(receiver.recv().await, RefreshTriggerEvent::Notified);
    }

    #[tokio::test]
    async fn sender_close_is_reported_once_then_the_receiver_is_fused() {
        let (trigger, mut receiver) = channel();
        drop(trigger);

        assert_eq!(receiver.recv().await, RefreshTriggerEvent::ChannelClosed);
        tokio::select! {
            biased;
            _ = receiver.recv() => panic!("a fused receiver must not busy-loop after closure"),
            _ = std::future::ready(()) => {}
        }
    }

    #[tokio::test]
    async fn queued_update_is_delivered_before_sender_close() {
        let (trigger, mut receiver) = channel();
        assert_eq!(trigger.notify(), TriggerOutcome::Queued);
        drop(trigger);

        assert_eq!(receiver.recv().await, RefreshTriggerEvent::Notified);
        assert_eq!(receiver.recv().await, RefreshTriggerEvent::ChannelClosed);
    }

    #[test]
    fn concurrent_native_updates_leave_exactly_one_request_pending() {
        const PRODUCERS: usize = 32;

        let (trigger, _receiver) = channel();
        let barrier = Arc::new(Barrier::new(PRODUCERS));
        let workers = (0..PRODUCERS)
            .map(|_| {
                let trigger = trigger.clone();
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    trigger.notify()
                })
            })
            .collect::<Vec<_>>();

        let outcomes = workers
            .into_iter()
            .map(|worker| worker.join().expect("native callback worker should join"))
            .collect::<Vec<_>>();

        assert_eq!(
            outcomes
                .iter()
                .filter(|&&outcome| outcome == TriggerOutcome::Queued)
                .count(),
            1
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|&&outcome| outcome == TriggerOutcome::Coalesced)
                .count(),
            PRODUCERS - 1
        );
    }

    #[test]
    fn receiver_close_turns_late_native_updates_into_a_safe_noop() {
        let (trigger, receiver) = channel();
        drop(receiver);

        assert_eq!(trigger.notify(), TriggerOutcome::Closed);
        assert_eq!(trigger.notify(), TriggerOutcome::Closed);
    }
}

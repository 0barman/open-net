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

// 容量固定为一：保留一次待处理重采样，重复原生提示合并到该请求中。
#[cfg(any(target_os = "macos", test))]
const TRIGGER_CAPACITY: usize = 1;

#[cfg(any(target_os = "macos", test))]
// 可跨线程克隆的原生通知句柄，仅尝试投递合并提示，不执行异步刷新。
#[derive(Clone, Debug)]
pub(crate) struct RefreshTrigger {
    // 容量为一的提示发送端，使用 try_send 避免原生回调等待队列空间。
    sender: mpsc::Sender<()>,
}

#[cfg(any(target_os = "macos", test))]
// 单次原生通知的投递结果，用于区分排队、合并及接收端已释放。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TriggerOutcome {
    // 新提示成功占据唯一待处理位置。
    Queued,
    // 已有提示待处理，本次变化合并其中，无需额外排队。
    Coalesced,
    // 接收端已关闭，本次提示无法交付。
    Closed,
}

#[cfg(any(target_os = "macos", test))]
// 接收端交给监听循环的事件；通道关闭事件最多交付一次。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RefreshTriggerEvent {
    // 已取到一个提示，应由监听循环请求 netwatch 重新采样。
    Notified,
    // 所有发送端已释放且提示已排空，后续接收永久待定。
    ChannelClosed,
}

// 异步刷新请求与停止信号竞争后的结果，不代表网络在线状态。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RefreshWorkOutcome {
    // 刷新请求返回成功；不意味着此处已得出新的网络状态。
    Requested,
    // 刷新请求返回错误，仅供诊断，不能据此制造网络状态。
    RequestFailed,
    // 停止分支先完成，刷新 Future 未继续执行。
    Stopped,
}

#[cfg(any(target_os = "macos", test))]
// 由监听任务独占的合并提示接收端，避免通道关闭后反复立即返回。
#[derive(Debug)]
pub(crate) struct RefreshTriggerReceiver {
    // 接收待处理的单个重采样提示；具体刷新工作在 recv 完成之后执行。
    receiver: mpsc::Receiver<()>,
    // 是否已经报告通道关闭；置位后 recv 永久等待，防止 select 循环空转。
    closed: bool,
}

#[cfg(any(target_os = "macos", test))]
// 创建容量为一的非阻塞通知桥，返回可克隆发送句柄和唯一接收端。
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
    // 尝试排入提示；满时合并，关闭时返回 Closed，始终不等待通道容量。
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
    // 可取消地等待一个提示，不在接收 Future 中启动刷新；取消空等待不会吞掉后续通知。
    // 先交付已排队提示，再将关闭报告一次；此后的调用永久待定以避免忙循环。
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

// 执行一次异步刷新，并让停止信号优先于同时就绪的刷新 Future；请求失败只返回诊断结果。
// select 可能先构造刷新 Future；优先停止保证的是不继续轮询该 Future，而非不调用构造闭包。
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

    // 用编译期约束验证类型能够安全跨线程移动和共享。
    fn assert_send_sync<T: Send + Sync>() {}

    // 验证原生回调持有的触发器满足 Send + Sync，支持来自原生队列的共享通知。
    #[test]
    fn native_trigger_handle_is_send_and_sync() {
        assert_send_sync::<super::RefreshTrigger>();
    }

    // 验证停止信号和刷新同时就绪时，刷新 Future 的主体不会被轮询执行。
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

    // 验证刷新 Future 已经进入等待时，后续停止信号仍能取消它并结束任务。
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

    // 验证没有停止信号时保留刷新请求的成功或失败结果，不互相混淆。
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

    // 验证通道满时重复通知被合并，原先排队的那次请求仍可接收。
    #[tokio::test]
    async fn full_channel_coalesces_without_losing_the_pending_request() {
        let (trigger, mut receiver) = channel();
        assert_eq!(trigger.notify(), TriggerOutcome::Queued);
        assert_eq!(trigger.notify(), TriggerOutcome::Coalesced);

        assert_eq!(receiver.recv().await, RefreshTriggerEvent::Notified);
    }

    // 验证刷新期间的新变化可以留下下一次待处理提示，更多变化继续合并。
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

    // 验证 select 取消尚无消息的 recv 后，下一次原生通知仍正常交付。
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

    // 验证提示已取出后取消外部等待，不会破坏接收端继续处理下一次提示的能力。
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

    // 验证关闭事件只报告一次，后续 recv 保持待定而不会让监听循环空转。
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

    // 验证最后一个发送端释放时，已经排队的提示先于关闭事件交付。
    #[tokio::test]
    async fn queued_update_is_delivered_before_sender_close() {
        let (trigger, mut receiver) = channel();
        assert_eq!(trigger.notify(), TriggerOutcome::Queued);
        drop(trigger);

        assert_eq!(receiver.recv().await, RefreshTriggerEvent::Notified);
        assert_eq!(receiver.recv().await, RefreshTriggerEvent::ChannelClosed);
    }

    // 同时触发多线程通知，验证只排入一个请求，其余均返回合并结果。
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

    // 验证接收端关闭后的重复原生通知都立即返回 Closed，不继续排队。
    #[test]
    fn receiver_close_turns_late_native_updates_into_a_safe_noop() {
        let (trigger, receiver) = channel();
        drop(receiver);

        assert_eq!(trigger.notify(), TriggerOutcome::Closed);
        assert_eq!(trigger.notify(), TriggerOutcome::Closed);
    }
}

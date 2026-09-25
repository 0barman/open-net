use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

/// 标记一个 worker 已交给某次回调；回调返回、展开或派发失败时归还运行槽。
pub(super) struct DataCallbackWorkerLease {
    /// 与 worker 共享的占用标记；先释放此标记，再发布回调完成通知。
    busy: Arc<AtomicBool>,
    available: Arc<Notify>,
}

impl DataCallbackWorkerLease {
    /// 只允许一个已准入任务占用该 worker，失败时由调用者返回明确派发错误。
    pub(super) fn try_claim(busy: &Arc<AtomicBool>, available: &Arc<Notify>) -> Option<Self> {
        busy.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()
            .map(|_| Self {
                busy: Arc::clone(busy),
                available: Arc::clone(available),
            })
    }
}

impl Drop for DataCallbackWorkerLease {
    fn drop(&mut self) {
        self.busy.store(false, Ordering::Release);
        self.available.notify_waiters();
    }
}

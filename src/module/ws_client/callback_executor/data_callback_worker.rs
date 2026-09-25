use super::data_callback_worker_lease::DataCallbackWorkerLease;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use tokio::sync::Notify;

/// 一个无 Tokio 上下文的专用数据回调线程及其单任务邮箱。
///
/// 工作器不持有邮箱发送端，客户端关闭入口后可自然退出；不保存 JoinHandle，避免
/// 用户回调永久阻塞时把网络关闭流程也拖住。
pub(super) struct DataCallbackWorker {
    /// 仅接受已占用该 worker 运行槽的任务，不构成另一条待准入业务队列。
    sender: mpsc::SyncSender<Box<dyn FnOnce() + Send>>,
    /// 在派发前置位，用户回调完成或派发失败后释放。
    busy: Arc<AtomicBool>,
    alive: Arc<AtomicBool>,
    available: Arc<Notify>,
}

impl DataCallbackWorker {
    /// 按需创建一个线程；资源不足时保留操作系统错误供连接工作器处理。
    pub(super) fn start(available: Arc<Notify>) -> io::Result<Self> {
        let (sender, receiver) = mpsc::sync_channel::<Box<dyn FnOnce() + Send>>(1);
        let alive = Arc::new(AtomicBool::new(true));
        let worker_alive = Arc::clone(&alive);
        let worker_available = Arc::clone(&available);
        std::thread::Builder::new()
            .name("open-net-ws-data-callback".to_string())
            .spawn(move || {
                let _lifetime = WorkerLifetime {
                    alive: worker_alive,
                    available: worker_available,
                };
                while let Ok(callback) = receiver.recv() {
                    // 回调包装器负责捕获用户代码展开，并发布运行槽和完成通知。
                    callback();
                }
            })
            .map(drop)?;
        Ok(Self {
            sender,
            busy: Arc::new(AtomicBool::new(false)),
            alive,
            available,
        })
    }

    /// 单一派发循环用此快照查找可复用 worker，实际占用仍通过原子比较交换确认。
    pub(super) fn is_available(&self) -> bool {
        self.is_alive() && !self.busy.load(Ordering::Acquire)
    }

    pub(super) fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Acquire)
    }

    /// 占用该线程的唯一运行槽，回调结束时由返回的守卫归还。
    pub(super) fn claim(&self) -> Option<DataCallbackWorkerLease> {
        DataCallbackWorkerLease::try_claim(&self.busy, &self.available)
    }

    /// 发送端只在提交包装任务期间临时克隆，执行中的回调不会延长邮箱生命周期。
    pub(super) fn sender(&self) -> mpsc::SyncSender<Box<dyn FnOnce() + Send>> {
        self.sender.clone()
    }
}

struct WorkerLifetime {
    alive: Arc<AtomicBool>,
    available: Arc<Notify>,
}

impl Drop for WorkerLifetime {
    fn drop(&mut self) {
        self.alive.store(false, Ordering::Release);
        self.available.notify_waiters();
    }
}

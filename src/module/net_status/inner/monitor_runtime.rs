use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use tokio::sync::{oneshot, watch};

use super::monitor_state::MonitorState;
use crate::error::NetError;

// 一代监控的初始化结果；成功、失败及主动停止分别通知所有同代 start 等待者。
#[derive(Clone, Debug)]
pub(crate) enum MonitorInitialization {
    // 检测器与首轮观测尚未就绪，此时不对外声明已启动。
    Pending,
    // 检测器已创建且首轮观测成功发布。
    Ready,
    // 初始化失败，错误在该代退休后交付，之后允许直接重新 start。
    Failed(NetError),
    // 初始化期间发生可恢复停止，保留 start 与 stop 交错时的成功结束约定。
    Stopped,
}

// 客户端持有的单次监控运行句柄，组合停止、初始化、完成信号及该代共享状态。
pub(crate) struct MonitorRuntime {
    // 一次性停止通知；取出并发送后置空，避免重复通知同一个监控任务。
    pub(crate) stop_sender: Option<oneshot::Sender<()>>,
    // 当前代不可混淆的初始化结果，多个启动调用共享同一个接收源。
    pub(crate) initial_state: watch::Receiver<MonitorInitialization>,
    // 监控任务的完成信号；关闭与销毁通过它等待任务所持资源释放。
    pub(crate) finished: watch::Receiver<bool>,
    // 该代身份和发布权限；公开可观测值由统一快照源持有。
    pub(crate) state: Arc<Mutex<MonitorState>>,
    // 与共享状态引用同一有效性标志，停止时先屏蔽旧代检测更新。
    pub(crate) active: Arc<AtomicBool>,
}

/// Completion is also published if the runtime cancels the task before its
/// first poll or if the monitor unwinds, so shutdown cannot lose its waiter.
// 任务完成守卫；无论正常退出、取消还是首次轮询前被丢弃，析构时都发出完成信号。
pub(crate) struct MonitorCompletion(pub(crate) watch::Sender<bool>); // 唯一字段：向关闭等待者发布完成状态的发送端。

impl Drop for MonitorCompletion {
    // 将完成状态置为 true；即使当前没有接收者，也保留已完成的最新值。
    fn drop(&mut self) {
        self.0.send_replace(true);
    }
}

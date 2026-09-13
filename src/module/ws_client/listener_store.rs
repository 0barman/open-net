use crate::{ConnectionStatus, WSCResponse};
use std::sync::{Arc, RwLock};
use tokio_util::sync::CancellationToken;

/// 可在线程间共享的数据响应监听器。
///
/// 回调取得 `WSCResponse` 的所有权，以便按 UUID 原子认领关联的原请求。
pub(crate) type DataListener = Arc<dyn Fn(WSCResponse) + Send + Sync + 'static>;

/// 可在线程间共享的连接状态监听器。
pub(crate) type StatusListener = Arc<dyn Fn(ConnectionStatus) + Send + Sync + 'static>;

/// A registration owns its initial-delivery barrier independently of replacements.
pub(crate) struct StatusRegistration {
    pub(crate) listener: StatusListener,
    pub(crate) initial_done: CancellationToken,
    /// Replacement/removal wakes a lane waiting for this registration's initial call.
    pub(crate) retired: CancellationToken,
}

impl StatusRegistration {
    pub(crate) fn new(listener: StatusListener) -> Self {
        Self {
            listener,
            initial_done: CancellationToken::new(),
            retired: CancellationToken::new(),
        }
    }
}

/// WebSocket 客户端当前注册的回调集合。
///
/// 注册、替换和注销可与回调任务并发。回调任务只在读锁内克隆 `Arc`，随后释放锁再执行
/// 用户代码，因此慢回调不会持锁阻塞监听器更新；已经克隆的旧监听器不受随后注销影响。
/// 任一锁中毒时，对应监听器的读取或更新会被当作不存在/失败处理。
#[derive(Default)]
pub(crate) struct ListenerStore {
    /// 当前客户端的 WSC/Common 日志订阅；替换、注销和 worker 终态清理会释放句柄。
    pub(crate) log: std::sync::Mutex<Option<crate::common::log::logger::LogSubscription>>,
    /// 至多一个数据响应监听器。
    pub(crate) data: RwLock<Option<DataListener>>,
    /// 至多一个连接状态监听器。
    pub(crate) status: RwLock<Option<Arc<StatusRegistration>>>,
}

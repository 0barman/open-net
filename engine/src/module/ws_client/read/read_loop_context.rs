use crate::api::wsc::wsc_response::PendingRequestView;
use crate::module::ws_client::callback_event::CallbackEvent;
use crate::module::ws_client::heartbeat_state::HeartbeatState;
use crate::module::ws_client::io_event::IoEvent;
use crate::module::ws_client::write::control_message::ControlMessage;
use std::sync::Arc;
use tokio::sync::{mpsc, Semaphore};
use tokio_util::sync::CancellationToken;

/// 单个 WebSocket 连接世代的读循环运行上下文。
///
/// 该结构体在连接成功后由 worker 组装，然后整体移入独立的异步读任务。
/// 其中的通道和共享状态用于将控制消息、业务消息及读取终态分别路由
/// 给写循环、回调循环和客户端 worker。
pub(crate) struct ReadLoopContext {
    /// 发往写循环的控制消息通道，用于优先 flush 收到 Ping/Close 后由 Tungstenite
    /// 自动排好的协议回复。
    ///
    /// 通道已满时读循环 fail-fast 结束当前连接，不等待容量而阻塞后续网络读取。
    pub(crate) control_tx: mpsc::Sender<ControlMessage>,
    /// 发往回调循环的事件通道，用于交付收到的文本或二进制业务消息。
    ///
    /// 通道已满时读循环上报 `CallbackQueueOverflow` 并结束当前连接，不静默丢消息。
    pub(crate) data_callback_tx: mpsc::Sender<CallbackEvent>,
    /// 数据 callback lane 尚未处理 payload 的共享字节预算。
    pub(crate) data_callback_bytes: Arc<Semaphore>,
    /// 单条消息及累计 payload 可占用的最大字节数。
    pub(crate) data_callback_max_bytes: usize,
    /// 当前客户端的待处理请求视图，会被克隆到每个业务响应中供调用方关联请求。
    pub(crate) pending_requests: PendingRequestView,
    /// 发往 worker 的 I/O 事件通道，用于报告读端关闭或失败。
    ///
    /// 上报会异步等待有界通道容量，该等待不受本上下文的取消令牌中断。
    pub(crate) io_event_tx: mpsc::Sender<IoEvent>,
    /// 所属物理连接的世代号。
    ///
    /// worker 使用它忽略已被更新连接取代的过期 I/O 终态事件；它也会被
    /// 写入 [`crate::api::wsc::wsc_response::WSCResponse`] 供业务层识别响应来自哪一次连接。
    pub(crate) generation: u64,
    /// 本世代 I/O 任务的协作式取消令牌。
    ///
    /// 读循环在等待下一条网络消息时观察它并静默退出；业务与控制 lane 均不等待容量。
    pub(crate) cancel: CancellationToken,
    /// 当前连接代的共享心跳状态。
    ///
    /// 读循环仅用 payload 完全匹配的 Pong 清除当前 probe；无关或迟到的 Pong
    /// 不会刷新存活状态。
    pub(crate) heartbeat: Arc<HeartbeatState>,
}

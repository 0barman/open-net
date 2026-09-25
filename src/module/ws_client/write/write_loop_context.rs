use crate::module::ws_client::heartbeat_state::HeartbeatState;
use crate::module::ws_client::io_event::IoEvent;
use crate::module::ws_client::native_pending::NativePending;
use crate::module::ws_client::write::control_message::ControlMessage;
use crate::module::ws_client::write::priority_write_queue::PriorityWriteQueue;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// 启动单个已连接 WebSocket 写循环所需的共享状态。
///
/// 每次连接成功都会为当前连接代次创建新的上下文。业务队列与待响应视图可跨
/// 自动重连共享，而控制通道、取消令牌、心跳关联状态和连接代次只属于当前 I/O。
pub(crate) struct WriteLoopContext {
    pub(crate) cancel_domain_gate: Option<Arc<crate::module::ws_client::network_io::DomainIoGate>>,
    /// 跨连接共享的有界业务请求优先级队列。
    pub(crate) queue: Arc<PriorityWriteQueue>,
    /// 跨连接共享、为应用 ACK/NACK 等单向消息保留的紧急队列。
    pub(crate) urgent_queue: Arc<PriorityWriteQueue>,
    /// 接收读循环生成的 Pong 指令和工作线程生成的关闭指令。
    pub(crate) control_rx: mpsc::Receiver<ControlMessage>,
    /// 记录请求从排队、写入到等待响应状态的共享视图。
    pub(crate) pending_requests: Arc<NativePending>,
    /// 向客户端工作线程报告当前写循环终止原因的通道。
    pub(crate) io_event_tx: mpsc::Sender<IoEvent>,
    /// 当前连接的代次，用于让工作线程忽略旧 I/O 任务的迟到事件。
    pub(crate) generation: u64,
    /// 仅用于终止当前连接读写任务的取消令牌。
    pub(crate) cancel: CancellationToken,
    /// 大消息的 data frame payload 上限；`None` 表示不主动分帧。
    pub(crate) data_frame_payload_size: Option<usize>,
    /// 单个 Ping、Pong、Close 写入或 sink 关闭的最长等待时间。
    pub(crate) control_write_timeout: Duration,
    /// 单个业务 data frame 写入的最长等待时间。
    pub(crate) data_frame_write_timeout: Duration,
    /// Optional active Ping schedule; None leaves peer-control handling enabled.
    pub(crate) heartbeat_config: Option<crate::ws::HeartbeatConfig>,
    /// 当前连接代的 Ping/Pong payload 关联及单 outstanding probe 状态。
    pub(crate) heartbeat: Arc<HeartbeatState>,
}

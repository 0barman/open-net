use crate::module::ws_client::write::control_message::ControlMessage;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// 当前连接代次正在运行的 WebSocket I/O 资源。
///
/// worker 在握手成功后为该代连接创建一对读写任务。停止连接时会先按需通过控制通道
/// 请求写任务发送关闭帧，再取消并中止两个任务；代次不匹配的旧资源只会被直接终止。
pub(super) struct ActiveIo {
    /// 创建这些资源时的连接代次，用于避免旧连接影响当前连接。
    pub(super) generation: u64,
    /// 由读写任务共享的取消令牌，用于协同停止本代 I/O。
    pub(super) cancel: CancellationToken,
    /// 发往写任务的控制消息通道。
    ///
    /// 读任务用它回复 Pong，worker 用它请求发送 Close 并等待写侧确认。
    pub(super) control_tx: mpsc::Sender<ControlMessage>,
    /// 本代 WebSocket 读任务的句柄。
    pub(super) read_handle: JoinHandle<()>,
    /// 本代 WebSocket 写任务的句柄。
    pub(super) write_handle: JoinHandle<()>,
}

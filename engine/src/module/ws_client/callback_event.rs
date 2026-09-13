use crate::api::traits::ws::connection_status::ConnectionStatus;
use crate::api::wsc::wsc_response::WSCResponse;
use crate::module::ws_client::listener_store::DataListener;
use tokio::sync::{oneshot, OwnedSemaphorePermit};

/// 由网络与状态机投递给专用回调任务的事件。
///
/// 事件通过有界通道按出队顺序处理。数据事件在 reader 接收完整消息时捕获监听器
/// 快照，再由回调任务在可脱离 runtime 的 OS 线程中调用；在 `panic=unwind` 构建中，
/// 可展开的监听器 panic 会被隔离。
pub(crate) enum CallbackEvent {
    /// 向消息接收时注册的数据监听器交付一条完整的文本或二进制响应。
    Data {
        /// reader 接收消息时捕获的监听器；当时未注册监听器则为 `None`。
        listener: Option<DataListener>,
        /// 包含消息、接收时间、连接代次和共享待请求视图的响应对象。
        response: WSCResponse,
        /// 覆盖排队和用户回调执行期间 payload 内存预算的许可。
        byte_permit: OwnedSemaphorePermit,
    },
    /// 请求数据回调 lane 完成此前已入队的事件和正在执行的监听器后确认并停止。
    DataDrain {
        /// lane 完成 drain 时发送的确认；等待方超时消失不会阻止 lane 退出。
        delivered: oneshot::Sender<()>,
    },
    /// 通知当前连接状态监听器状态已经发生变化。
    Status {
        /// 生成该事件的那次连接状态转换所对应的状态。
        status: ConnectionStatus,
        /// 可选的投递完成确认发送端。
        ///
        /// 回调循环在监听器返回（或当前没有监听器）后发送确认。关闭流程用它等待
        /// `Closed` 事件被处理；接收端消失或可展开的监听器 panic 都不会阻止循环继续运行。
        delivered: Option<oneshot::Sender<()>>,
    },
}

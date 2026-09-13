use tokio::sync::oneshot;

/// 发送给当前 WebSocket 写循环的控制指令。
///
/// 控制指令通过独立于业务请求优先级队列的通道传递。写循环使用有偏选择，
/// 因而在控制指令和业务请求同时就绪时优先处理控制指令。
pub(crate) enum ControlMessage {
    /// Flush Tungstenite's automatically queued Pong response.
    ///
    /// Sending another explicit Pong could replace or duplicate the automatic reply if
    /// the read half progressed first, so the writer only drives the shared sink flush.
    FlushAutomatic,
    /// Flush Tungstenite's automatically queued peer-Close reply, then stop the writer.
    PeerClose(oneshot::Sender<()>),
    /// 尝试发送 Close 帧并关闭写 sink。
    Close(
        /// 关闭尝试完成后的通知发送端。
        ///
        /// 收到通知只表示写循环已执行 Close 帧发送与 sink 关闭操作，
        /// 不保证对端已完成关闭握手；底层发送或关闭错误也不会通过该通道返回。
        oneshot::Sender<()>,
    ),
}

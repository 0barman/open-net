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
    PeerClose(oneshot::Sender<Result<(), crate::error::NetError>>),
    /// Send the selected Close and flush within the original shutdown budget.
    CloseWith {
        frame: Option<crate::ws::CloseFrame>,
        deadline: tokio::time::Instant,
        reply: oneshot::Sender<Result<(), crate::error::NetError>>,
    },
}

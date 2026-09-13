use crate::api::traits::ws::connection_status::ConnectionStatus;
use crate::api::wsc::wsc_response::WSCResponse;

/// WebSocket 业务消息监听器。
///
/// 回调在独立于客户端 Tokio 运行时的专用操作系统线程执行，因此其中不能假定存在当前
/// Tokio 事件驱动器，也不应直接调用 `tokio::spawn`。需要运行异步业务逻辑时，应把数据
/// 投递到应用已有的运行时或通道。配置的数据回调并发数大于 1 时，本闭包可能被并发调用。
pub type WebSocketClientDataReceiveListener = Box<dyn Fn(WSCResponse) + Send + Sync + 'static>;

/// 已接收的单个 WebSocket 任务的终态结果监听器。回调在本次监听器注册对应的专用
/// 操作系统线程执行，该线程没有 Tokio 运行时上下文。替换或注销监听器，以及关闭
/// 客户端后，已有任务仍保留其原始监听器。
pub type WebSocketClientTaskCompleteListener =
    Box<dyn Fn(crate::api::wsc::WebSocketTaskEvent) + Send + Sync + 'static>;

/// WebSocket 连接状态监听器。
///
/// 首次快照及后续通知都在没有 Tokio 运行时上下文的专用操作系统线程执行，注册不等待
/// 用户回调。同一次注册的首次通知完成后才执行后续通知；后续通知读取最新状态，允许
/// 合并或重复。替换/注销后已提交的回调仍可完成，不同注册之间可并发。异步业务应
/// 转交给应用自己的运行时或通道。
pub type WebSocketClientConnectStatusListener =
    Box<dyn Fn(ConnectionStatus) + Send + Sync + 'static>;

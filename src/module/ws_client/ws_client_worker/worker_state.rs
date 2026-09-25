use super::*;

/// WebSocket 客户端的后台工作器。
///
/// 每个客户端拥有一个工作器，并由 [`WSClientWorker::run`] 在独立操作系统线程上的
/// 单线程 Tokio 运行时中驱动。工作器串行处理客户端命令与 I/O 任务事件，负责连接状态
/// 转换、连接及重连任务、读写任务、回调分发以及关闭时的资源清理。
pub(crate) struct WSClientWorker {
    /// 终态任务监听登记表，用于连接结束和关闭时完成尚未结算的观察任务。
    pub(in crate::module::ws_client) task_observers: Arc<NativeTaskObserver>,
    /// 创建客户端时编译好的代理、TLS 和网络传输配置快照。
    pub(in crate::module::ws_client) network: Arc<CompiledNetworkConfig>,
    /// 限制同步握手上下文提供器并发，防止超时后遗留用户线程无限累积。
    pub(in crate::module::ws_client) context_provider_slots: Arc<Semaphore>,
    /// 客户端级配置，提供心跳、Pong 检测、关闭等待及回调队列等运行参数。
    pub(in crate::module::ws_client) config: WebSocketClientConfig,
    /// 从客户端句柄接收连接、断开和关闭命令的通道。
    ///
    /// 工作器是该通道的唯一消费者，因此所有会改变生命周期的外部命令均在事件循环中
    /// 串行执行。
    pub(in crate::module::ws_client) command_rx: mpsc::Receiver<ClientCommand>,
    /// 向工作器回送连接、读取或写入结果的通道发送端。
    ///
    /// 该发送端会被克隆给连接任务和每一代读写任务。
    pub(in crate::module::ws_client) io_event_tx: mpsc::Sender<IoEvent>,
    /// 接收后台连接及读写任务结果的通道接收端。
    pub(in crate::module::ws_client) io_event_rx: mpsc::Receiver<IoEvent>,
    /// 将业务数据送往用户回调循环的有界通道发送端；满载时 reader fail-fast 断开。
    pub(in crate::module::ws_client) data_callback_tx: mpsc::Sender<CallbackEvent>,
    /// 数据回调通道的接收端。
    ///
    /// 使用 `Option` 是为了让 [`WSClientWorker::run_async`] 在启动时恰好取走一次所有权，
    /// 并将其交给独立的回调循环。
    pub(in crate::module::ws_client) data_callback_rx: Option<mpsc::Receiver<CallbackEvent>>,
    /// The same lazily initialized pool used by subscription setup and dispatch.
    pub(in crate::module::ws_client) data_callback_pool: Arc<DataCallbackPool>,
    /// 业务消息的共享有界优先级队列。
    ///
    /// 客户端调用方负责入队，当前连接代的写任务负责出队；断线及关闭流程会按策略筛选
    /// 或排空其中的请求。
    pub(in crate::module::ws_client) queue: Arc<PriorityWriteQueue>,
    /// 应用层 ACK/NACK 等无需响应消息使用的保留紧急队列。
    pub(in crate::module::ws_client) urgent_queue: Arc<PriorityWriteQueue>,
    /// Worker-local state used to reject stale transport events.
    pub(in crate::module::ws_client) state: Arc<RwLock<ConnectionStatus>>,
    /// 各 Session 共用的连接状态、事件订阅配额和回调执行器。
    pub(in crate::module::ws_client) listeners: Arc<ListenerStore>,
    /// 整个客户端生命周期共用的关闭令牌。
    ///
    /// 令牌被取消后，主事件循环会执行关闭命令；队列入队路径也用它中止等待。
    pub(in crate::module::ws_client) shutdown: CancellationToken,
    pub(in crate::module::ws_client) shutdown_reason:
        Arc<crate::module::ws_client::ws_client_inner::ShutdownReason>,
    /// Shared once-only notification published after final worker cleanup.
    pub(in crate::module::ws_client) shutdown_complete: CancellationToken,
    /// 网络恢复通知器。
    ///
    /// 提前结束当前会话重试前的退避等待；会话终态后必须显式新建会话。
    pub(in crate::module::ws_client) network_available: Arc<Notify>,
    /// 可选的共享网络状态监控客户端，工作器启动时请求开启监控。
    pub(in crate::module::ws_client) net_status_client: Option<Arc<InnerNetStatusClient>>,
    /// 订阅到的网络状态快照，用于暂停或恢复连接尝试。
    pub(in crate::module::ws_client) network_status: Option<watch::Receiver<NetworkStatusSnapshot>>,
    /// 最近一次观测到的网络丢失代次，用于识别连接过程中错过的断网。
    pub(in crate::module::ws_client) network_loss_epoch: u64,
    /// 当前连接代编号。
    ///
    /// 每次启动新的连接周期都会递增并保持非零。连接及读写事件携带该编号；工作器会
    /// 忽略代次不匹配的事件。连接成功、失败及读写终止事件还会校验当前状态，避免
    /// 主动断开后迟到的同代连接失败污染 Idle 状态。
    pub(in crate::module::ws_client) generation: u64,
    /// 最近一次成功接受的连接目标及选项。
    ///
    /// 初始连接后会保留该值以支持自动重连；主动断开或关闭时清除。
    pub(in crate::module::ws_client) connect_target: Option<ConnectTarget>,
    /// 当前连接/重试任务使用的取消令牌。
    pub(in crate::module::ws_client) connect_cancel: Option<CancellationToken>,
    /// 当前连接/重试任务的句柄，用于在新连接周期、断开或关闭时强制终止任务。
    pub(in crate::module::ws_client) connect_handle: Option<JoinHandle<()>>,
    /// 已建立连接当前正在运行的读写任务及其控制资源。
    pub(in crate::module::ws_client) active_io: Option<ActiveIo>,
}

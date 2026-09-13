use crate::api::net_error::NetError;
use crate::api::traits::ws::connection_status::ConnectionStatus;
use crate::api::web_socket_client::WebSocketClientConfig;
use crate::api::wsc::wsc_response::PendingRequestView;
use crate::api::wsc::WebSocketTaskEndCause;
use crate::api::wsc::{
    WebSocketConnectStage, WebSocketConnectionFailure, WebSocketTerminationReason,
};
use crate::common::log::log_def::LogType;
use crate::common::platform::spawn;
use crate::module::net_status::inner::inner_net_status_client::InnerNetStatusClient;
use crate::module::net_status::inner::network_status_snapshot::NetworkStatusSnapshot;
use crate::module::net_status::NetworkStatus;
use crate::module::transport::CompiledNetworkConfig;
use crate::module::ws_client::active_io::ActiveIo;
use crate::module::ws_client::callback_event::CallbackEvent;
use crate::module::ws_client::callback_executor::{run_user_callback, try_start_user_callback};
use crate::module::ws_client::client_command::ClientCommand;
use crate::module::ws_client::connect_target::{ConnectTarget, ContextConnectTarget};
use crate::module::ws_client::connection_session::ConnectionSession;
use crate::module::ws_client::heartbeat_state::HeartbeatState;
use crate::module::ws_client::io_event::IoEvent;
use crate::module::ws_client::listener_store::ListenerStore;
use crate::module::ws_client::read::read_loop_context::ReadLoopContext;
use crate::module::ws_client::send_admission::SendAdmission;
use crate::module::ws_client::task_observer::TaskObserverStore;
use crate::module::ws_client::write::control_message::ControlMessage;
use crate::module::ws_client::write::priority_write_queue::PriorityWriteQueue;
use crate::module::ws_client::write::write_loop_context::WriteLoopContext;
use crate::module::ws_client::ws_read::run_read_loop;
use crate::module::ws_client::ws_write::{fail_requests, run_write_loop};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use socket2::{SockRef, TcpKeepalive};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch, Notify, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::{HeaderName, HeaderValue, Request};
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
#[cfg(test)]
use tokio_tungstenite::tungstenite::Error as WsError;
use tokio_util::sync::CancellationToken;

/// 为重连退避生成伪随机数的全局 xorshift 状态。
///
/// 状态使用非零种子避免 xorshift 序列停在零值；原子访问只用于在并发调用时安全推进
/// 序列，不提供密码学随机性，也不参与连接状态的同步。
static JITTER_STATE: AtomicU64 = AtomicU64::new(0x9e37_79b9_7f4a_7c15);

/// 从总关闭预算中最多保留给取消后的读写任务清理。
const MAX_COOPERATIVE_IO_CANCEL_GRACE: Duration = Duration::from_millis(100);

#[cfg(test)]
mod callback_budget_tests;
#[cfg(test)]
mod callback_dispatch_lifecycle_tests;
#[cfg(test)]
mod callback_dispatch_tests;
mod context_connect;
mod network_gate;
#[cfg(test)]
mod network_status_tests;
#[cfg(test)]
mod reconnect_recovery_tests;

/// WebSocket 客户端的后台工作器。
///
/// 每个客户端拥有一个工作器，并由 [`WSClientWorker::run`] 在独立操作系统线程上的
/// 单线程 Tokio 运行时中驱动。工作器串行处理客户端命令与 I/O 任务事件，负责连接状态
/// 转换、连接及重连任务、读写任务、回调分发以及关闭时的资源清理。
pub(crate) struct WSClientWorker {
    pub(super) task_observers: Arc<TaskObserverStore>,
    pub(super) network: Arc<CompiledNetworkConfig>,
    pub(super) context_provider_slots: Arc<Semaphore>,
    /// 客户端级配置，提供心跳、Pong 检测、关闭等待及回调队列等运行参数。
    pub(super) config: WebSocketClientConfig,
    /// 从客户端句柄接收连接、断开、网络恢复和关闭命令的通道。
    ///
    /// 工作器是该通道的唯一消费者，因此所有会改变生命周期的外部命令均在事件循环中
    /// 串行执行。
    pub(super) command_rx: mpsc::Receiver<ClientCommand>,
    /// 向工作器回送连接、读取或写入结果的通道发送端。
    ///
    /// 该发送端会被克隆给连接任务和每一代读写任务。
    pub(super) io_event_tx: mpsc::Sender<IoEvent>,
    /// 接收后台连接及读写任务结果的通道接收端。
    pub(super) io_event_rx: mpsc::Receiver<IoEvent>,
    /// 将业务数据送往用户回调循环的有界通道发送端；满载时 reader fail-fast 断开。
    pub(super) data_callback_tx: mpsc::Sender<CallbackEvent>,
    /// 数据回调通道的接收端。
    ///
    /// 使用 `Option` 是为了让 [`WSClientWorker::run_async`] 在启动时恰好取走一次所有权，
    /// 并将其交给独立的回调循环。
    pub(super) data_callback_rx: Option<mpsc::Receiver<CallbackEvent>>,
    /// 数据 callback lane 的 payload 字节预算。
    pub(super) data_callback_bytes: Arc<Semaphore>,
    /// 状态回调使用独立通道，避免业务数据回调积压阻塞生命周期状态机。
    pub(super) status_callback_tx: mpsc::Sender<CallbackEvent>,
    /// 状态回调通道的唯一接收端，启动后移交给专用串行回调任务。
    pub(super) status_callback_rx: Option<mpsc::Receiver<CallbackEvent>>,
    /// 业务消息的共享有界优先级队列。
    ///
    /// 客户端调用方负责入队，当前连接代的写任务负责出队；断线及关闭流程会按策略筛选
    /// 或排空其中的请求。
    pub(super) queue: Arc<PriorityWriteQueue>,
    /// 应用层 ACK/NACK 等无需响应消息使用的保留紧急队列。
    pub(super) urgent_queue: Arc<PriorityWriteQueue>,
    /// 对外共享的当前连接状态快照。
    ///
    /// 生命周期状态通常仅由本工作器写入，客户端句柄可从其他线程并发读取。
    pub(super) state: Arc<RwLock<ConnectionStatus>>,
    /// 最近一次连接建立或 I/O 终止错误，供公开句柄查询恢复原因。
    pub(super) last_connection_error: Arc<RwLock<Option<NetError>>>,
    /// 最近一次终态 HTTP 握手失败状态码。
    pub(super) last_handshake_http_status: Arc<RwLock<Option<u16>>>,
    /// 数据与连接状态监听器的共享存储。
    ///
    /// reader 在接收业务消息时从中捕获数据监听器快照；状态回调循环则在处理事件时读取
    /// 当前状态监听器。因而数据监听器的注册或注销只影响之后收到的业务消息。
    pub(super) listeners: Arc<ListenerStore>,
    /// 尚待响应的业务请求视图，用于跨发送、接收和断线清理路径关联请求。
    pub(super) pending_requests: PendingRequestView,
    /// 连接/会话级发送准入取消代次。
    pub(super) send_admission: Arc<SendAdmission>,
    /// 整个客户端生命周期共用的关闭令牌。
    ///
    /// 令牌被取消后，主事件循环会执行关闭命令；队列入队路径也用它中止等待。
    pub(super) shutdown: CancellationToken,
    /// Shared once-only notification published after final worker cleanup.
    pub(super) shutdown_complete: CancellationToken,
    /// 网络恢复通知器。
    ///
    /// 通知既可提前结束重试前的退避等待，也与 `NetworkAvailable` 命令配合，在客户端已
    /// 进入断开状态时启动新的重连周期。
    pub(super) network_available: Arc<Notify>,
    pub(super) net_status_client: Option<Arc<InnerNetStatusClient>>,
    pub(super) network_status: Option<watch::Receiver<NetworkStatusSnapshot>>,
    pub(super) network_loss_epoch: u64,
    /// 当前连接代编号。
    ///
    /// 每次启动新的连接周期都会递增并保持非零。连接及读写事件携带该编号；工作器会
    /// 忽略代次不匹配的事件。连接成功、失败及读写终止事件还会校验当前状态，避免
    /// 主动断开后迟到的同代连接失败污染 Idle 状态。
    pub(super) generation: u64,
    /// 最近一次成功接受的连接目标及选项。
    ///
    /// 初始连接后会保留该值以支持自动重连；主动断开或关闭时清除。
    pub(super) connect_target: Option<ConnectTarget>,
    /// 等待初始 `connect` 调用结果的单次回复发送端。
    ///
    /// 回复会在该连接周期最终成功、失败或被取消时发送；后台自动重连不创建此回复。
    pub(super) connect_reply: Option<oneshot::Sender<Result<(), NetError>>>,
    /// 当前连接/重试任务使用的取消令牌。
    pub(super) connect_cancel: Option<CancellationToken>,
    /// 当前连接/重试任务的句柄，用于在新连接周期、断开或关闭时强制终止任务。
    pub(super) connect_handle: Option<JoinHandle<()>>,
    /// 已建立连接当前正在运行的读写任务及其控制资源。
    pub(super) active_io: Option<ActiveIo>,
}

impl WSClientWorker {
    /// 在调用线程上创建单线程 Tokio 运行时并运行工作器，直至客户端关闭。
    ///
    /// 创建者会在独立操作系统线程中调用本方法。运行时创建结果通过 `ready_tx` 同步返回：
    /// 创建失败时发送 [`NetError::RuntimeError`] 并立即返回；成功时先发送 `Ok(())`，再阻塞
    /// 当前线程执行异步事件循环。若创建者已放弃接收就绪结果，发送失败不会阻止工作器按
    /// 既定生命周期运行。
    ///
    /// # 参数
    ///
    /// - `ready_tx`：向客户端创建流程报告工作线程是否完成运行时初始化的同步通道。
    pub(crate) fn run(self, ready_tx: std::sync::mpsc::Sender<Result<(), NetError>>) {
        crate::log_t!(LogType::WSC; "run", "generation", self.generation);
        let runtime = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(error) => {
                crate::log_e!(LogType::WSC; "run", "stage|error", "runtime_build", crate::common::log::summary::error(&error));
                let _ = ready_tx.send(Err(NetError::RuntimeError));
                return;
            }
        };
        crate::log_s!(LogType::WSC; "run", "state", "runtime_ready");
        if ready_tx.send(Ok(())).is_err() {
            crate::log_s!(LogType::WSC; "run", "state", "ready_receiver_dropped");
        }
        runtime.block_on(self.run_async());
        crate::log_s!(LogType::WSC; "run", "state", "worker_exited");
    }

    /// 运行主事件循环，并在退出前统一清理连接、队列和待处理请求。
    ///
    /// 启动时会把回调接收端移交给独立回调任务。事件选择带有偏序：已触发的全局关闭
    /// 优先于客户端命令，客户端命令又优先于 I/O 事件。循环结束后会取消连接任务、
    /// 非优雅地停止残留 I/O、关闭并排空当时可见的队列项，再清空待响应注册表。被排空
    /// 的请求以 [`NetError::Cancelled`] 完成；已被写任务取出却随任务中止而析构的请求
    /// 会由 `QueuedRequest` 的 drop fallback 保守报告 [`NetError::DeliveryUnknown`]。
    /// 数据回调 lane 会在 response grace 的原截止时间前尝试排空；到期后不再等待，
    /// 剩余回调任务随 runtime 退出。
    async fn run_async(mut self) {
        crate::log_t!(LogType::WSC; "run_async", "generation", self.generation);
        // Keep absolute registration deadlines active through shutdown's accepted-response
        // cleanup. The worker owns this lane and cancels/joins it before shutdown completes.
        let timer_cancel = CancellationToken::new();
        let _timer_cancel_guard = timer_cancel.clone().drop_guard();
        let registration_timer =
            spawn(self.pending_requests.clone().run_registration_deadlines(
                timer_cancel.clone(),
                self.config.response_dispatch_grace,
            ));
        if let Some(client) = self.net_status_client.clone() {
            let shutdown = self.shutdown.clone();
            spawn(async move {
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => {}
                    result = client.start() => {
                        if let Err(error) = result {
                            crate::log_e!(LogType::WSC; "network_monitor_start", "error", format!("{error:?}"));
                        }
                    }
                }
            });
        }
        if let Some(callback_rx) = self.data_callback_rx.take() {
            spawn(data_callback_loop(
                callback_rx,
                self.config.data_callback_concurrency,
                self.io_event_tx.clone(),
                self.shutdown.clone(),
            ));
        }
        if let Some(callback_rx) = self.status_callback_rx.take() {
            spawn(status_callback_loop(
                Arc::clone(&self.listeners),
                Arc::clone(&self.state),
                callback_rx,
            ));
        }
        let mut should_exit = false;
        let mut terminal_response_deadline = None;
        while !should_exit {
            let session_cancel = self.context_session().map(|session| session.cancel_token());
            tokio::select! {
                biased;
                _ = self.shutdown.cancelled() => {
                    should_exit = self
                        .handle_command(
                            ClientCommand::Shutdown { reply: None },
                            &mut terminal_response_deadline,
                        )
                        .await;
                }
                _ = async {
                    match session_cancel {
                        Some(token) => token.cancelled().await,
                        None => std::future::pending::<()>().await,
                    }
                } => {
                    let (reply, _received) = oneshot::channel();
                    self.handle_command(ClientCommand::Disconnect { reply }, &mut terminal_response_deadline).await;
                }
                command = self.command_rx.recv() => {
                    match command {
                        Some(command) => {
                            should_exit = self
                                .handle_command(command, &mut terminal_response_deadline)
                                .await;
                        }
                        None => {
                            crate::log_s!(LogType::WSC; "run_async", "state", "command_channel_closed");
                            should_exit = true;
                        },
                    }
                }
                changed = async {
                    match self.network_status.as_mut() {
                        Some(receiver) => receiver.changed().await,
                        None => std::future::pending().await,
                    }
                } => {
                    if changed.is_err() {
                        self.network_status = None;
                    } else {
                        self.reconcile_network_status().await;
                    }
                }
                event = self.io_event_rx.recv() => {
                    if let Some(event) = event {
                        self.handle_io_event(event).await;
                    }
                }
            }
        }
        self.shutdown.cancel();
        self.task_observers.close();
        self.send_admission
            .end_session_with_cause(WebSocketTaskEndCause::Shutdown);
        self.cancel_connect(NetError::Cancelled);
        self.stop_active_io(false).await;
        self.finish_context_session(
            WebSocketTerminationReason::Shutdown,
            Some(Self::cancellation_failure()),
        );
        self.connect_target = None;
        self.urgent_queue.close();
        self.queue.close();
        fail_requests(
            self.urgent_queue.drain(),
            &self.pending_requests,
            NetError::Cancelled,
        );
        fail_requests(
            self.queue.drain(),
            &self.pending_requests,
            NetError::Cancelled,
        );
        let terminal_response_deadline =
            terminal_response_deadline.or_else(|| self.response_dispatch_grace_deadline());
        self.pending_requests
            .fail_all_with_response_grace(NetError::Cancelled, self.config.response_dispatch_grace);
        self.drain_data_callbacks_until(terminal_response_deadline)
            .await;
        self.pending_requests
            .wait_for_deferred_responses_until(terminal_response_deadline)
            .await;
        // `shutdown_complete` is an authoritative cleanup boundary. Even if a response guard has
        // already left the dispatch map while its entry cleanup is still in progress, the final
        // force drains every remaining entry and preserves its first deferred terminal error.
        self.pending_requests
            .force_finish_all_response_dispatches(NetError::Cancelled);
        crate::log_s!(LogType::WSC; "run_async", "state|generation", "shutdown_cleanup_complete", self.generation);
        self.clear_listeners();
        if let Some(client) = self.net_status_client.take() {
            if let Err(error) = client.destroy().await {
                crate::log_e!(LogType::WSC; "network_monitor_destroy", "error", format!("{error:?}"));
            }
        }
        timer_cancel.cancel();
        if let Err(error) = registration_timer.await {
            crate::log_e!(LogType::WSC; "registration_timer_join", "error", format!("{error:?}"));
        }
        self.shutdown_complete.cancel();
    }

    /// 处理一条客户端控制命令。
    ///
    /// 连接命令会验证当前状态和重连参数，保存连接目标并启动初始连接周期；断开命令会
    /// 尝试发送关闭帧、清理请求后回到空闲状态；网络恢复命令仅在已断开且允许自动重连时
    /// 启动新周期；关闭命令则进入不可恢复的终态并请求事件循环退出。状态回调及部分关闭
    /// 操作可能异步等待，但同一时刻不会并发处理另一条命令。
    ///
    /// # 参数
    ///
    /// - `command`：要执行的生命周期命令及其可选回复通道。
    /// - `terminal_response_deadline`：首次终态 pending 清理对应的 response grace 截止时间。
    ///
    /// # 返回值
    ///
    /// 仅完成 `Shutdown` 命令时返回 `true`，通知主事件循环退出；其他命令均返回 `false`。
    async fn handle_command(
        &mut self,
        command: ClientCommand,
        terminal_response_deadline: &mut Option<Instant>,
    ) -> bool {
        crate::log_t!(LogType::WSC; "handle_command", "command|terminal_response_deadline", match &command { ClientCommand::Connect { .. } => "connect", ClientCommand::ConnectWithContext { .. } => "connect_with_context", ClientCommand::Disconnect { .. } => "disconnect", ClientCommand::NetworkAvailable => "network_available", ClientCommand::Shutdown { .. } => "shutdown" }, terminal_response_deadline.is_some());
        match command {
            ClientCommand::ConnectWithContext {
                options,
                session,
                reply,
            } => {
                let status = self.current_status();
                let admission = if status == ConnectionStatus::Closed {
                    Err(NetError::EngineDropped)
                } else if matches!(
                    status,
                    ConnectionStatus::Connecting
                        | ConnectionStatus::Connected
                        | ConnectionStatus::Reconnecting
                        | ConnectionStatus::Closing
                ) {
                    Err(NetError::ConnectionExists)
                } else if session.cancel_token().is_cancelled() {
                    Err(NetError::Cancelled)
                } else if self.network.network_status_policy()
                    == crate::api::network_config::NetworkStatusPolicy::PauseOnUnavailable
                    && options.reconnect.max_elapsed.is_none()
                {
                    Err(NetError::ConfigError)
                } else {
                    options.validate()
                };
                if let Err(error) = admission {
                    if let Err(delivery_error) = session.terminate(
                        WebSocketTerminationReason::ConnectFailed,
                        Some(WebSocketConnectionFailure::new(
                            error,
                            WebSocketConnectStage::RequestBuild,
                            None,
                            false,
                        )),
                    ) {
                        crate::log_e!(LogType::WSC; "connect_with_context", "stage|error", "admission_event", format!("{delivery_error:?}"));
                    }
                    let _ = reply.send(Err(error));
                    return false;
                }
                self.connect_target = Some(ConnectTarget {
                    url: options.url.trim().to_string(),
                    options: crate::api::web_socket_client::WebSocketConnectOptions {
                        headers: options.headers.clone(),
                        header_provider: None,
                        reconnect: options.reconnect.clone(),
                    },
                    context: Some(ContextConnectTarget { options, session }),
                });
                self.start_connection(false).await;
                if reply.send(Ok(())).is_err() {
                    if let Some(session) = self.context_session() {
                        session.request_cancel();
                    }
                }
            }
            ClientCommand::Connect {
                url,
                options,
                reply,
            } => {
                let status = self.current_status();
                if matches!(
                    status,
                    ConnectionStatus::Connecting
                        | ConnectionStatus::Connected
                        | ConnectionStatus::Reconnecting
                        | ConnectionStatus::Closing
                ) {
                    crate::log_e!(LogType::WSC; "handle_command", "command|error", "connect", "ConnectionExists");
                    if reply.send(Err(NetError::ConnectionExists)).is_err() {
                        crate::log_s!(LogType::WSC; "handle_command", "state", "command_reply_receiver_dropped");
                    }
                    return false;
                }
                if status == ConnectionStatus::Closed {
                    crate::log_e!(LogType::WSC; "handle_command", "command|error", "connect", "EngineDropped");
                    if reply.send(Err(NetError::EngineDropped)).is_err() {
                        crate::log_s!(LogType::WSC; "handle_command", "state", "command_reply_receiver_dropped");
                    }
                    return false;
                }
                if !options.reconnect.is_valid()
                    || (self.network.network_status_policy()
                        == crate::api::network_config::NetworkStatusPolicy::PauseOnUnavailable
                        && options.reconnect.max_elapsed.is_none())
                {
                    crate::log_e!(LogType::WSC; "handle_command", "command|error", "connect", "ConfigError");
                    if reply.send(Err(NetError::ConfigError)).is_err() {
                        crate::log_s!(LogType::WSC; "handle_command", "state", "command_reply_receiver_dropped");
                    }
                    return false;
                }
                crate::log_s!(LogType::WSC; "handle_command", "command|url", "connect_accepted", crate::common::log::summary::url(&url));
                self.connect_target = Some(ConnectTarget {
                    url,
                    options,
                    context: None,
                });
                self.start_connection(false).await;
                self.connect_reply = Some(reply);
            }
            ClientCommand::Disconnect { reply } => {
                crate::log_s!(LogType::WSC; "handle_command", "command|generation", "disconnect", self.generation);
                // Invalidate admissions before draining. An enqueue that already holds the
                // queue lock is drained afterwards; all later pushes observe cancellation.
                self.send_admission
                    .end_session_with_cause(WebSocketTaskEndCause::Disconnect);
                self.set_status(ConnectionStatus::Closing).await;
                self.cancel_connect(NetError::Cancelled);
                let graceful = !self
                    .current_request_scope()
                    .is_some_and(|scope| scope.is_cancelled());
                self.stop_active_io(graceful).await;
                fail_requests(
                    self.urgent_queue.drain(),
                    &self.pending_requests,
                    NetError::Cancelled,
                );
                fail_requests(
                    self.queue.drain(),
                    &self.pending_requests,
                    NetError::Cancelled,
                );
                self.pending_requests.fail_all_with_response_grace(
                    NetError::Cancelled,
                    self.config.response_dispatch_grace,
                );
                self.set_last_connection_error(None);
                self.set_last_handshake_http_status(None);
                self.set_status(ConnectionStatus::Idle).await;
                let reason = if self
                    .context_session()
                    .is_some_and(|session| session.cancel_token().is_cancelled())
                {
                    WebSocketTerminationReason::Cancelled
                } else {
                    WebSocketTerminationReason::Disconnected
                };
                self.finish_context_session(reason, Some(Self::cancellation_failure()));
                self.connect_target = None;
                if reply.send(Ok(())).is_err() {
                    crate::log_s!(LogType::WSC; "handle_command", "state", "command_reply_receiver_dropped");
                }
            }
            ClientCommand::NetworkAvailable => {
                crate::log_s!(LogType::WSC; "handle_command", "command|generation", "network_available", self.generation);
                if self.current_status() == ConnectionStatus::Disconnected
                    && self
                        .connect_target
                        .as_ref()
                        .is_some_and(|target| target.options.reconnect.enabled)
                {
                    self.start_connection(true).await;
                }
            }
            ClientCommand::Shutdown { reply } => {
                crate::log_s!(LogType::WSC; "handle_command", "command|generation", "shutdown", self.generation);
                self.task_observers.close();
                self.send_admission
                    .end_session_with_cause(WebSocketTaskEndCause::Shutdown);
                self.set_status(ConnectionStatus::Closing).await;
                self.cancel_connect(NetError::Cancelled);
                self.stop_active_io(true).await;
                self.urgent_queue.close();
                self.queue.close();
                fail_requests(
                    self.urgent_queue.drain(),
                    &self.pending_requests,
                    NetError::Cancelled,
                );
                fail_requests(
                    self.queue.drain(),
                    &self.pending_requests,
                    NetError::Cancelled,
                );
                *terminal_response_deadline = self.response_dispatch_grace_deadline();
                self.pending_requests.fail_all_with_response_grace(
                    NetError::Cancelled,
                    self.config.response_dispatch_grace,
                );
                self.finish_context_session(
                    WebSocketTerminationReason::Shutdown,
                    Some(Self::cancellation_failure()),
                );
                self.connect_target = None;
                if let Some(deadline) = Instant::now().checked_add(self.config.close_timeout) {
                    let delivered = self.set_status_and_wait(ConnectionStatus::Closed);
                    if tokio::time::timeout_at(deadline, delivered).await.is_err() {
                        crate::log_s!(LogType::WSC; "handle_command", "state", "closed_status_delivery_timed_out");
                    }
                } else {
                    // The transition itself is mandatory even if an extreme duration can no
                    // longer form a deadline after a very long-lived client.
                    self.set_status(ConnectionStatus::Closed).await;
                }
                if let Some(reply) = reply {
                    if reply.send(Ok(())).is_err() {
                        crate::log_s!(LogType::WSC; "handle_command", "state", "command_reply_receiver_dropped");
                    }
                }
                return true;
            }
        }
        false
    }

    /// 处理连接任务或当前读写任务发回的 I/O 事件。
    ///
    /// 所有事件首先按 `generation` 校验；来自不同连接代的迟到结果会被忽略。连接成功和
    /// 连接失败还要求状态处于建连或重连中，读写终止事件则要求仍处于已连接状态，因此
    /// 主动断开后迟到的同代事件也不会覆盖新状态。连接成功时拆分 WebSocket 流并启动
    /// 成对的读写任务，连接失败时结束当前周期并失败当时可见的排队请求。当前连接的任一
    /// 读写任务异常结束时，会停止另一侧并按请求断线策略筛选队列：启用自动重连则保留
    /// 可等待/可重试项并开始重连，否则将状态置为断开并失败剩余请求。
    ///
    /// # 参数
    ///
    /// - `event`：包含来源连接代及结果或错误的内部 I/O 事件。
    async fn handle_io_event(&mut self, event: IoEvent) {
        if self.shutdown.is_cancelled() {
            return;
        }
        self.reconcile_network_status().await;
        crate::log_t!(LogType::WSC; "handle_io_event", "event|generation", match &event { IoEvent::ConnectSucceeded { .. } => "connect_succeeded", IoEvent::ConnectFailed { .. } => "connect_failed", IoEvent::ReadEnded { .. } => "read_ended", IoEvent::WriteEnded { .. } => "write_ended", IoEvent::ContextAttemptStarted { .. } => "context_attempt_started", IoEvent::ContextPrepared { .. } => "context_prepared", IoEvent::ContextAttemptFailed { .. } => "context_attempt_failed", IoEvent::CallbackDispatchFailed { .. } => "callback_dispatch_failed" }, self.generation);
        match event {
            IoEvent::ContextAttemptStarted {
                generation,
                attempt,
                reservation,
                accepted,
            } => {
                let result = self
                    .connecting_context(generation)
                    .and_then(|session| session.begin_attempt(attempt, reservation));
                self.acknowledge_context_event(result, accepted);
            }
            IoEvent::ContextPrepared {
                generation,
                attempt_id,
                context_id,
                accepted,
            } => {
                let result = self.connecting_context(generation).and_then(|session| {
                    session.set_attempt_context(generation, attempt_id, context_id)
                });
                self.acknowledge_context_event(result, accepted);
            }
            IoEvent::ContextAttemptFailed {
                generation,
                attempt_id,
                failure,
                will_retry,
                accepted,
            } => {
                let result = self.connecting_context(generation).and_then(|session| {
                    session.attempt_failed(generation, attempt_id, failure, will_retry)
                });
                self.acknowledge_context_event(result, accepted);
            }
            IoEvent::ConnectSucceeded {
                generation,
                stream,
                context_attempt_id,
                network_loss_epoch,
                accepted,
            } => {
                crate::log_s!(LogType::WSC; "handle_io_event", "event|generation", "connect_succeeded", generation);
                if self.shutdown.is_cancelled()
                    || generation != self.generation
                    || !matches!(
                        self.current_status(),
                        ConnectionStatus::Connecting | ConnectionStatus::Reconnecting
                    )
                {
                    crate::log_s!(LogType::WSC; "handle_io_event", "state|event_generation|current_generation", "stale_connection_event_ignored", generation, self.generation);
                    if let Some(accepted) = accepted {
                        let _ = accepted.send(Err(NetError::Cancelled));
                    }
                    return;
                }
                if !self.network_epoch_is_current(network_loss_epoch) {
                    if let Some(accepted) = accepted {
                        let _ = accepted.send(Err(NetError::NetworkError));
                    }
                    return;
                }
                if let Some(attempt_id) = context_attempt_id {
                    let result = self
                        .connecting_context(generation)
                        .and_then(|session| session.prepare_established(generation, attempt_id));
                    if let Err(error) = result {
                        crate::log_e!(LogType::WSC; "handle_io_event", "stage|error", "publish_established", format!("{error:?}"));
                        if let Some(session) = self.context_session() {
                            session.request_cancel();
                        }
                        if let Some(accepted) = accepted {
                            let _ = accepted.send(Err(error));
                        }
                        return;
                    }
                }
                self.connect_cancel.take();
                self.connect_handle.take();
                let (write, read) = (*stream).split();
                let write_retirement = CancellationToken::new();
                let request_scope = self.current_request_scope();
                let established_loss_epoch = network_loss_epoch.unwrap_or(self.network_loss_epoch);
                self.network_loss_epoch = established_loss_epoch;
                let write = crate::module::ws_client::network_io::NetworkAwareSink::new(
                    write,
                    self.network_status.clone(),
                    established_loss_epoch,
                )
                .with_request_scope(request_scope.as_ref())
                .with_write_retirement(&write_retirement);
                let read = crate::module::ws_client::network_io::NetworkAwareStream::new(
                    read,
                    self.network_status.clone(),
                    established_loss_epoch,
                )
                .with_request_scope(request_scope.as_ref())
                .with_write_retirement(&write_retirement);
                let (control_tx, control_rx) = mpsc::channel(32);
                let cancel = match request_scope.as_ref() {
                    Some(scope) => scope.cancel_token().child_token(),
                    None => CancellationToken::new(),
                };
                let heartbeat = Arc::new(HeartbeatState::new(generation));
                let read_handle = spawn(run_read_loop(
                    read,
                    ReadLoopContext {
                        control_tx: control_tx.clone(),
                        data_callback_tx: self.data_callback_tx.clone(),
                        data_callback_bytes: Arc::clone(&self.data_callback_bytes),
                        data_callback_max_bytes: self.config.callback_queue_max_bytes,
                        pending_requests: self.pending_requests.clone(),
                        io_event_tx: self.io_event_tx.clone(),
                        generation,
                        cancel: cancel.clone(),
                        heartbeat: Arc::clone(&heartbeat),
                    },
                    Arc::clone(&self.listeners),
                ));
                let write_handle = spawn(run_write_loop(
                    write,
                    WriteLoopContext {
                        queue: Arc::clone(&self.queue),
                        urgent_queue: Arc::clone(&self.urgent_queue),
                        control_rx,
                        pending_requests: self.pending_requests.clone(),
                        io_event_tx: self.io_event_tx.clone(),
                        generation,
                        cancel: cancel.clone(),
                        data_frame_payload_size: self.config.data_frame_payload_size,
                        control_write_timeout: self.config.control_write_timeout,
                        data_frame_write_timeout: self.config.data_frame_write_timeout,
                        heartbeat_interval: self.config.heartbeat_interval,
                        pong_timeout: self.config.pong_timeout,
                        response_dispatch_grace: self.config.response_dispatch_grace,
                        heartbeat,
                    },
                    write_retirement,
                ));
                self.active_io = Some(ActiveIo {
                    generation,
                    cancel,
                    control_tx,
                    read_handle,
                    write_handle,
                });
                self.send_admission
                    .connection_succeeded_with_network_epoch(established_loss_epoch);
                self.set_last_handshake_http_status(None);
                self.set_status(ConnectionStatus::Connected).await;
                if let Some(attempt_id) = context_attempt_id {
                    if let Some(session) = self.context_session() {
                        if let Err(error) = session.commit_established(generation, attempt_id) {
                            crate::log_e!(LogType::WSC; "handle_io_event", "stage|error", "commit_established", format!("{error:?}"));
                            session.request_cancel();
                        }
                    }
                }
                if let Some(reply) = self.connect_reply.take() {
                    if reply.send(Ok(())).is_err() {
                        crate::log_s!(LogType::WSC; "handle_io_event", "state", "command_reply_receiver_dropped");
                    }
                }
                if let Some(accepted) = accepted {
                    let _ = accepted.send(Ok(()));
                }
            }
            IoEvent::ConnectFailed {
                generation,
                failure,
                reason,
            } => {
                let error = failure.error();
                // Keep the legacy public snapshot limited to WebSocket Upgrade responses.
                // Proxy status and local failures must not masquerade as endpoint HTTP errors.
                let http_status = if error == NetError::ConnectError
                    && failure.stage() == WebSocketConnectStage::WebSocketUpgrade
                {
                    failure.http_status()
                } else {
                    None
                };
                crate::log_e!(LogType::WSC; "handle_io_event", "event|generation|error|http_status", "connect_failed", generation, format!("{error:?}"), http_status);
                if generation != self.generation
                    || !matches!(
                        self.current_status(),
                        ConnectionStatus::Connecting | ConnectionStatus::Reconnecting
                    )
                {
                    crate::log_s!(LogType::WSC; "handle_io_event", "state|event_generation|current_generation", "stale_connection_event_ignored", generation, self.generation);
                    return;
                }
                self.connect_cancel.take();
                self.connect_handle.take();
                self.send_admission.end_session();
                self.set_last_connection_error(Some(error));
                self.set_last_handshake_http_status(http_status);
                self.set_status(ConnectionStatus::Disconnected).await;
                if let Some(reply) = self.connect_reply.take() {
                    if reply.send(Err(error)).is_err() {
                        crate::log_s!(LogType::WSC; "handle_io_event", "state", "command_reply_receiver_dropped");
                    }
                }
                fail_requests(self.urgent_queue.drain(), &self.pending_requests, error);
                fail_requests(self.queue.drain(), &self.pending_requests, error);
                if self.context_session().is_some() {
                    self.finish_context_session(reason, Some(failure));
                    self.connect_target = None;
                } else if reason != WebSocketTerminationReason::RetryExhausted {
                    // Only a transport cycle that used up its retry budget may be restarted
                    // by a hint. Nonretryable failures require an explicit new connection.
                    self.connect_target = None;
                }
            }
            IoEvent::CallbackDispatchFailed { generation } => {
                if generation != self.generation
                    || self.current_status() != ConnectionStatus::Connected
                {
                    crate::log_s!(LogType::WSC; "handle_io_event", "state|event_generation|current_generation", "stale_callback_failure_ignored", generation, self.generation);
                    return;
                }
                self.connection_lost_at_stage(
                    NetError::TaskInterruptionError,
                    WebSocketTerminationReason::IoFailure,
                    WebSocketConnectStage::EventDelivery,
                )
                .await;
            }
            IoEvent::ReadEnded { generation, error }
            | IoEvent::WriteEnded { generation, error } => {
                crate::log_s!(LogType::WSC; "handle_io_event", "event|generation|error", "io_ended", generation, format!("{error:?}"));
                if generation != self.generation
                    || self.current_status() != ConnectionStatus::Connected
                {
                    crate::log_s!(LogType::WSC; "handle_io_event", "state|event_generation|current_generation", "stale_io_event_ignored", generation, self.generation);
                    return;
                }
                self.connection_lost(error, WebSocketTerminationReason::IoFailure)
                    .await;
            }
        }
    }

    fn network_epoch_is_current(&self, epoch: Option<u64>) -> bool {
        match (epoch, self.network_status.as_ref()) {
            (Some(epoch), Some(receiver)) => {
                let mut receiver = receiver.clone();
                let snapshot = *receiver.borrow_and_update();
                snapshot.loss_epoch == epoch && snapshot.status != Some(NetworkStatus::Unavailable)
            }
            _ => true,
        }
    }

    async fn reconcile_network_status(&mut self) {
        let Some(receiver) = self.network_status.as_mut() else {
            return;
        };
        let snapshot = *receiver.borrow_and_update();
        if snapshot.loss_epoch > self.network_loss_epoch {
            self.network_loss_epoch = snapshot.loss_epoch;
            if self.current_status() == ConnectionStatus::Connected {
                self.connection_lost(
                    NetError::NetworkError,
                    WebSocketTerminationReason::NetworkUnavailable,
                )
                .await;
            }
        }
    }

    async fn connection_lost(&mut self, error: NetError, reason: WebSocketTerminationReason) {
        let stage = if reason == WebSocketTerminationReason::NetworkUnavailable {
            WebSocketConnectStage::EventDelivery
        } else {
            WebSocketConnectStage::WebSocketIo
        };
        self.connection_lost_at_stage(error, reason, stage).await;
    }

    async fn connection_lost_at_stage(
        &mut self,
        error: NetError,
        reason: WebSocketTerminationReason,
        stage: WebSocketConnectStage,
    ) {
        self.send_admission.connection_ended();
        self.set_last_connection_error(Some(error));
        self.stop_active_io(false).await;
        if let Some(session) = self.context_session() {
            if let Err(event_error) = session.connection_terminated(
                reason,
                Some(WebSocketConnectionFailure::new(error, stage, None, false)),
            ) {
                crate::log_e!(LogType::WSC; "handle_io_event", "stage|error", "publish_connection_terminated", format!("{event_error:?}"));
                session.request_cancel();
            }
        }
        self.pending_requests.fail_for_generation(
            self.generation,
            error,
            self.config.response_dispatch_grace,
        );
        fail_requests(
            self.urgent_queue.drain_rejected_on_disconnect(),
            &self.pending_requests,
            error,
        );
        fail_requests(
            self.queue.drain_rejected_on_disconnect(),
            &self.pending_requests,
            error,
        );
        if self
            .connect_target
            .as_ref()
            .is_some_and(|target| target.options.reconnect.enabled)
        {
            self.start_connection(true).await;
        } else {
            self.send_admission.end_session();
            self.set_status(ConnectionStatus::Disconnected).await;
            fail_requests(self.urgent_queue.drain(), &self.pending_requests, error);
            fail_requests(self.queue.drain(), &self.pending_requests, error);
            self.finish_context_session(
                reason,
                Some(WebSocketConnectionFailure::new(error, stage, None, false)),
            );
            if self.context_session().is_some() {
                self.connect_target = None;
            }
        }
    }

    fn context_session(&self) -> Option<Arc<ConnectionSession>> {
        self.connect_target
            .as_ref()
            .and_then(|target| target.context.as_ref())
            .map(|target| Arc::clone(&target.session))
    }

    fn current_request_scope(&self) -> Option<crate::api::wsc::RequestScope> {
        self.connect_target
            .as_ref()
            .and_then(|target| target.context.as_ref())
            .and_then(|context| context.options.request_scope.clone())
    }

    fn connecting_context(&self, generation: u64) -> Result<Arc<ConnectionSession>, NetError> {
        if generation != self.generation
            || !matches!(
                self.current_status(),
                ConnectionStatus::Connecting | ConnectionStatus::Reconnecting
            )
        {
            return Err(NetError::Cancelled);
        }
        let session = self.context_session().ok_or(NetError::Cancelled)?;
        if session.cancel_token().is_cancelled() {
            return Err(NetError::Cancelled);
        }
        Ok(session)
    }

    fn acknowledge_context_event(
        &self,
        result: Result<(), NetError>,
        accepted: oneshot::Sender<Result<(), NetError>>,
    ) {
        if let Err(error) = result {
            crate::log_e!(LogType::WSC; "acknowledge_context_event", "error", format!("{error:?}"));
            // Stale events must not revoke a later session. Other internal publication failures
            // fail closed, and the worker cancellation branch performs cleanup.
            if error != NetError::Cancelled {
                if let Some(session) = self.context_session() {
                    session.request_cancel();
                }
            }
        }
        let _ = accepted.send(result);
    }

    fn cancellation_failure() -> WebSocketConnectionFailure {
        WebSocketConnectionFailure::new(
            NetError::Cancelled,
            WebSocketConnectStage::EventDelivery,
            None,
            false,
        )
    }

    fn finish_context_session(
        &self,
        reason: WebSocketTerminationReason,
        failure: Option<WebSocketConnectionFailure>,
    ) {
        if let Some(session) = self.context_session() {
            if let Err(error) = session.terminate(reason, failure) {
                crate::log_e!(LogType::WSC; "finish_context_session", "error", format!("{error:?}"));
            }
        }
    }

    /// 为已保存的目标启动一个新的连接及重试周期。
    ///
    /// 若没有连接目标则直接返回。启动前会取消上一连接任务及其待回复的初始连接调用，随后
    /// 递增非零 `generation`、建立新的取消令牌，将状态设为 `Connecting` 或
    /// `Reconnecting`，并派生 [`connect_with_retry`] 任务。该方法只负责启动，不等待握手
    /// 成功或失败。
    ///
    /// # 参数
    ///
    /// - `reconnecting`：为 `true` 时表示断线后的自动重连，状态设为 `Reconnecting`；为
    ///   `false` 时表示用户发起的初始连接，状态设为 `Connecting`。
    async fn start_connection(&mut self, reconnecting: bool) {
        crate::log_t!(LogType::WSC; "start_connection", "reconnecting|generation", reconnecting, self.generation);
        if self.shutdown.is_cancelled()
            || self
                .context_session()
                .is_some_and(|session| session.cancel_token().is_cancelled())
        {
            return;
        }
        let Some(target) = self.connect_target.clone() else {
            crate::log_s!(LogType::WSC; "start_connection", "state", "no_connect_target");
            return;
        };
        if !reconnecting || self.current_status() == ConnectionStatus::Disconnected {
            self.send_admission.begin_session_with_scope(
                target
                    .context
                    .as_ref()
                    .map(|context| context.options.session_context_id),
                target
                    .context
                    .as_ref()
                    .and_then(|context| context.options.request_scope.clone()),
            );
        }
        self.cancel_connect(NetError::Cancelled);
        if !reconnecting {
            self.set_last_connection_error(None);
            self.set_last_handshake_http_status(None);
        }
        if target.context.is_some() {
            let Some(generation) = self.generation.checked_add(1) else {
                self.send_admission.end_session();
                self.set_last_connection_error(Some(NetError::InternalError));
                self.set_status(ConnectionStatus::Disconnected).await;
                self.finish_context_session(
                    WebSocketTerminationReason::ConnectFailed,
                    Some(WebSocketConnectionFailure::new(
                        NetError::InternalError,
                        WebSocketConnectStage::EventDelivery,
                        None,
                        false,
                    )),
                );
                self.connect_target = None;
                return;
            };
            self.generation = generation;
        } else {
            self.generation = self.generation.wrapping_add(1).max(1);
        }
        let generation = self.generation;
        crate::log_s!(LogType::WSC; "start_connection", "state|generation|reconnecting", "connection_cycle_started", generation, reconnecting);
        let cancel = CancellationToken::new();
        self.connect_cancel = Some(cancel.clone());
        self.set_status(if reconnecting {
            ConnectionStatus::Reconnecting
        } else {
            ConnectionStatus::Connecting
        })
        .await;
        let event_tx = self.io_event_tx.clone();
        let network_available = Arc::clone(&self.network_available);
        let network_status = self.network_status.clone();
        let client_config = self.config.clone();
        let network = Arc::clone(&self.network);
        let provider_slots = Arc::clone(&self.context_provider_slots);
        self.connect_handle = Some(spawn(async move {
            if let Some(context) = target.context.clone() {
                context_connect::ContextConnectTask {
                    target,
                    context,
                    client_config,
                    network,
                    provider_slots,
                    generation,
                    cancel,
                    network_available,
                    network_status,
                    event_tx,
                }
                .run()
                .await;
                return;
            }
            connect_with_retry(
                target,
                client_config,
                network,
                generation,
                cancel,
                network_available,
                network_status,
                provider_slots,
                event_tx,
            )
            .await;
        }));
    }

    /// 取消并移除当前连接任务，同时结束尚未完成的初始连接回复。
    ///
    /// 该操作既取消令牌又调用任务句柄的 `abort`，使连接任务的异步部分停止。若动态请求
    /// 头提供器已经在 `spawn_blocking` 中执行，任务中止不能终止该同步调用。若存在
    /// `connect` 调用的回复发送端，会以给定错误完成；本方法本身不修改连接状态，也不
    /// 处理已经建立的读写任务。
    ///
    /// # 参数
    ///
    /// - `error`：返回给尚在等待的初始连接调用的终止原因。
    fn cancel_connect(&mut self, error: NetError) {
        crate::log_t!(LogType::WSC; "cancel_connect", "error|generation", format!("{error:?}"), self.generation);
        if let Some(cancel) = self.connect_cancel.take() {
            crate::log_s!(LogType::WSC; "cancel_connect", "state|generation", "connection_attempt_cancelled", self.generation);
            cancel.cancel();
        }
        if let Some(handle) = self.connect_handle.take() {
            handle.abort();
        }
        if let Some(reply) = self.connect_reply.take() {
            if reply.send(Err(error)).is_err() {
                crate::log_s!(LogType::WSC; "cancel_connect", "state", "command_reply_receiver_dropped");
            }
        }
    }

    /// 停止并移除当前连接代的读写任务。
    ///
    /// `graceful` 为 `true` 时，一个 `close_timeout` 总预算覆盖 Close 入队、写侧确认及
    /// 读写任务协作退出。预算尾部会预留不超过 100ms 给取消后的任务清理，避免 Close
    /// 尝试耗尽全部时间后立即 abort。确认不代表对端完成关闭握手。最终 abort 后仍等待
    /// task 析构，使 writer 手中请求的 drop fallback 能报告 `DeliveryUnknown`。
    ///
    /// # 参数
    ///
    /// - `graceful`：是否在强制停止前尝试发送 WebSocket Close 帧并等待确认。
    async fn stop_active_io(&mut self, graceful: bool) {
        crate::log_t!(LogType::WSC; "stop_active_io", "graceful|generation", graceful, self.generation);
        let Some(active) = self.active_io.take() else {
            crate::log_s!(LogType::WSC; "stop_active_io", "state", "no_active_io");
            return;
        };
        let ActiveIo {
            generation,
            cancel,
            control_tx,
            mut read_handle,
            mut write_handle,
        } = active;
        let started_at = Instant::now();
        let deadline = started_at
            .checked_add(self.config.close_timeout)
            .unwrap_or(started_at);
        let cancel_grace = (self.config.close_timeout / 2)
            .min(MAX_COOPERATIVE_IO_CANCEL_GRACE)
            .max(Duration::from_nanos(1));
        let graceful_deadline = deadline.checked_sub(cancel_grace).unwrap_or(started_at);

        if graceful && generation == self.generation {
            // Keep a real tail budget for cancellation cleanup even when a data-frame
            // write prevents the writer from observing this command in time.
            let graceful_result = tokio::time::timeout_at(graceful_deadline, async {
                let (done_tx, done_rx) = oneshot::channel();
                control_tx
                    .send(ControlMessage::Close(done_tx))
                    .await
                    .map_err(|_| NetError::ConnectionClosed)?;
                done_rx.await.map_err(|_| NetError::ConnectionClosed)
            })
            .await;
            match graceful_result {
                Ok(Ok(())) => {
                    crate::log_s!(LogType::WSC; "stop_active_io", "state|generation", "close_write_acknowledged", generation)
                }
                Ok(Err(error)) => {
                    crate::log_s!(LogType::WSC; "stop_active_io", "state|generation|error", "close_channel_unavailable", generation, format!("{error:?}"))
                }
                Err(_) => {
                    crate::log_s!(LogType::WSC; "stop_active_io", "state|generation", "graceful_close_timed_out", generation)
                }
            }
        }

        // Give both loops a chance to observe cancellation. In particular, this lets the
        // writer complete the request currently held outside the queue instead of merely
        // dropping its oneshot sender and surfacing `EngineDropped` to the caller.
        cancel.cancel();
        let joined = tokio::time::timeout_at(deadline, async {
            let (read_result, write_result) = tokio::join!(&mut read_handle, &mut write_handle);
            if let Err(error) = read_result {
                crate::log_e!(LogType::WSC; "stop_active_io", "task|error", "read", crate::common::log::summary::error(&error));
            }
            if let Err(error) = write_result {
                crate::log_e!(LogType::WSC; "stop_active_io", "task|error", "write", crate::common::log::summary::error(&error));
            }
        })
        .await;
        if joined.is_err() {
            crate::log_s!(LogType::WSC; "stop_active_io", "state|generation", "cooperative_stop_timed_out_aborting_io", generation);
            read_handle.abort();
            write_handle.abort();
            let _ = tokio::join!(read_handle, write_handle);
        }
    }

    /// 读取当前连接状态快照。
    ///
    /// # 返回值
    ///
    /// 成功加读锁时返回共享状态值；若状态锁已中毒，则保守地返回不可恢复的
    /// [`ConnectionStatus::Closed`]。
    fn current_status(&self) -> ConnectionStatus {
        crate::log_t!(LogType::WSC; "current_status");
        self.state.read().map(|state| *state).unwrap_or_else(|_| {
            crate::log_e!(LogType::WSC; "current_status", "error", "state_lock_poisoned");
            ConnectionStatus::Closed
        })
    }

    /// 更新最近一次连接错误；锁中毒时保守忽略，不能影响生命周期推进。
    fn set_last_connection_error(&self, error: Option<NetError>) {
        crate::log_t!(LogType::WSC; "set_last_connection_error", "error", format!("{error:?}"));
        if let Ok(mut current) = self.last_connection_error.write() {
            *current = error;
        } else {
            crate::log_e!(LogType::WSC; "set_last_connection_error", "error", "snapshot_lock_poisoned");
        }
    }

    /// 更新最近一次终态 HTTP 握手状态；锁中毒时不阻塞生命周期。
    fn set_last_handshake_http_status(&self, status: Option<u16>) {
        crate::log_t!(LogType::WSC; "set_last_handshake_http_status", "status", status);
        if let Ok(mut current) = self.last_handshake_http_status.write() {
            *current = status;
        } else {
            crate::log_e!(LogType::WSC; "set_last_handshake_http_status", "error", "snapshot_lock_poisoned");
        }
    }

    /// Breaks listener/client reference cycles once the client reaches terminal shutdown.
    fn clear_listeners(&self) {
        crate::log_t!(LogType::WSC; "clear_listeners");
        if let Ok(mut listener) = self.listeners.data.write() {
            listener.take();
        } else {
            crate::log_e!(LogType::WSC; "clear_listeners", "error", "data_listener_lock_poisoned");
        }
        let status_listener = if let Ok(mut listener) = self.listeners.status.write() {
            listener.take()
        } else {
            crate::log_e!(LogType::WSC; "clear_listeners", "error", "status_listener_lock_poisoned");
            None
        };
        if let Some(listener) = status_listener {
            listener.retired.cancel();
            drop(listener);
        }
        crate::log_s!(LogType::WSC; "clear_listeners", "state", "terminal_listener_cleanup_finished");
        let subscription = self
            .listeners
            .log
            .lock()
            .unwrap_or_else(|poisoned| {
                crate::log_e!(LogType::WSC; "clear_listeners", "error", "log_listener_lock_poisoned_recovered");
                poisoned.into_inner()
            })
            .take();
        drop(subscription);
    }

    /// 返回终态响应分发宽限的绝对截止时间；零宽限或不可表示的 deadline 不等待。
    fn response_dispatch_grace_deadline(&self) -> Option<Instant> {
        crate::log_t!(LogType::WSC; "response_dispatch_grace_deadline");
        (!self.config.response_dispatch_grace.is_zero())
            .then(|| Instant::now().checked_add(self.config.response_dispatch_grace))
            .flatten()
    }

    /// 在既定终态宽限截止时间前等待数据回调 lane 排空。
    ///
    /// barrier 与 reader 使用同一 FIFO 通道。终态清理会先停止活动 I/O，因此 barrier
    /// 之前包含所有可能的已接收业务事件；回调 lane 还会等待此前已启动的 listener。
    /// 若通道满、listener 永久阻塞或 lane 已停止，等待最多持续到原 response grace
    /// deadline，超时后由 runtime 退出负责丢弃剩余事件。零宽限不会入队或等待 barrier。
    async fn drain_data_callbacks_until(&self, deadline: Option<Instant>) {
        crate::log_t!(LogType::WSC; "drain_data_callbacks_until", "deadline_present", deadline.is_some());
        let Some(deadline) = deadline else {
            return;
        };
        let (delivered_tx, delivered_rx) = oneshot::channel();
        let drain = async {
            if self
                .data_callback_tx
                .send(CallbackEvent::DataDrain {
                    delivered: delivered_tx,
                })
                .await
                .is_ok()
            {
                let _ = delivered_rx.await;
            }
        };
        if tokio::time::timeout_at(deadline, drain).await.is_err() {
            crate::log_s!(LogType::WSC; "drain_data_callbacks_until", "state", "response_grace_expired");
        } else {
            crate::log_s!(LogType::WSC; "drain_data_callbacks_until", "state", "data_lane_drain_finished");
        }
    }

    /// 按状态机约束更新连接状态，并异步排入一次状态回调。
    ///
    /// 与当前状态相同、状态锁中毒或 [`connection_status_transition_allowed`] 拒绝的转换均
    /// 不修改状态且不产生回调；非法转换会记录错误日志。成功修改后同步写入
    /// 独立的状态回调通道，不等待用户监听器执行。
    ///
    /// # 参数
    ///
    /// - `status`：期望进入的新连接状态。
    async fn set_status(&self, status: ConnectionStatus) {
        crate::log_t!(LogType::WSC; "set_status", "status|generation", format!("{status:?}"), self.generation);
        let (changed, previous) = if let Ok(mut current) = self.state.write() {
            let previous = *current;
            if previous == status {
                (false, Some(previous))
            } else if !connection_status_transition_allowed(previous, status) {
                crate::log_e!(LogType::WSC; "set_status", "error|from|to", "invalid_status_transition", format!("{previous:?}"), format!("{status:?}"));
                (false, Some(previous))
            } else {
                *current = status;
                (true, Some(previous))
            }
        } else {
            crate::log_e!(LogType::WSC; "set_status", "error", "state_lock_poisoned");
            (false, None)
        };
        crate::log_s!(LogType::WSC; "set_status", "from|to|changed|generation", format!("{previous:?}"), format!("{status:?}"), changed, self.generation);
        if changed {
            match self.status_callback_tx.try_send(CallbackEvent::Status {
                status,
                delivered: None,
            }) {
                Ok(()) => {
                    crate::log_s!(LogType::WSC; "set_status", "state", "status_notification_queued")
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    crate::log_s!(LogType::WSC; "set_status", "state", "intermediate_status_notification_coalesced")
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    crate::log_s!(LogType::WSC; "set_status", "state", "status_callback_lane_closed")
                }
            }
        }
    }

    /// 按状态机约束更新连接状态，并等待对应状态回调完成分发。
    ///
    /// 状态确实变化时，会在回调事件中附带确认通道；只有回调循环处理完当时注册的状态
    /// 监听器（包括捕获其可展开 panic）后才返回。若状态未变化、转换非法、状态锁中毒
    /// 或回调通道已关闭，则不会等待。本方法自身没有超时，调用方在关闭流程中负责以
    /// `config.close_timeout` 包裹它。外层超时会放弃等待确认；已开始的监听器线程可能继续
    /// 运行，但与 worker runtime 脱离，不会阻止 runtime 最终退出。
    ///
    /// # 参数
    ///
    /// - `status`：期望进入且需要确认已分发的新连接状态。
    async fn set_status_and_wait(&self, status: ConnectionStatus) {
        crate::log_t!(LogType::WSC; "set_status_and_wait", "status|generation", format!("{status:?}"), self.generation);
        let (changed, previous) = if let Ok(mut current) = self.state.write() {
            let previous = *current;
            if previous == status {
                (false, Some(previous))
            } else if !connection_status_transition_allowed(previous, status) {
                crate::log_e!(LogType::WSC; "set_status_and_wait", "error|from|to", "invalid_status_transition", format!("{previous:?}"), format!("{status:?}"));
                (false, Some(previous))
            } else {
                *current = status;
                (true, Some(previous))
            }
        } else {
            crate::log_e!(LogType::WSC; "set_status_and_wait", "error", "state_lock_poisoned");
            (false, None)
        };
        crate::log_s!(LogType::WSC; "set_status_and_wait", "from|to|changed|generation", format!("{previous:?}"), format!("{status:?}"), changed, self.generation);
        if !changed {
            return;
        }
        let (delivered_tx, delivered_rx) = oneshot::channel();
        if self
            .status_callback_tx
            .send(CallbackEvent::Status {
                status,
                delivered: Some(delivered_tx),
            })
            .await
            .is_ok()
        {
            let _ = delivered_rx.await;
        }
    }
}

/// 判断一条连接状态转换是否符合工作器的生命周期状态机。
///
/// 状态机只接受显式列出的不同状态转换；相同状态由状态设置方法提前视为无变化。`Closed`
/// 是终态，没有任何出边；`Closing` 只能回到可复用的 `Idle` 或进入终态 `Closed`。
///
/// # 参数
///
/// - `from`：转换前状态。
/// - `to`：候选目标状态。
///
/// # 返回值
///
/// 若工作器允许从 `from` 转换到 `to`，返回 `true`。
fn connection_status_transition_allowed(from: ConnectionStatus, to: ConnectionStatus) -> bool {
    crate::log_t!(LogType::WSC; "connection_status_transition_allowed", "from|to", format!("{from:?}"), format!("{to:?}"));
    use ConnectionStatus::{
        Closed, Closing, Connected, Connecting, Disconnected, Idle, Reconnecting,
    };

    matches!(
        (from, to),
        (Idle, Connecting | Closing)
            | (Connecting, Connected | Disconnected | Closing)
            | (Connected, Reconnecting | Disconnected | Closing)
            | (Reconnecting, Connected | Disconnected | Closing)
            | (Disconnected, Connecting | Reconnecting | Closing)
            | (Closing, Idle | Closed)
    )
}

/// 按入队顺序接收数据回调事件，并在可脱离 runtime 的线程有界并发调用已捕获的监听器。
///
/// reader 在接收完整消息时已经取得监听器快照，因此之后的注销、替换或关闭清理不会
/// 丢弃已接收事件，也不会把它们改投给新监听器。每个监听器调用均由 `catch_unwind`
/// 隔离；在 `panic=unwind` 构建中，可展开的 panic 不会终止回调循环，但 `panic=abort`
/// 无法被捕获。事件按通道顺序出队，但并发值大于 1 时，操作系统调度不保证用户回调的
/// 可观察执行或结束顺序；严格有序场景须把并发设为 1。并发数达到配置上限后才暂停
/// 出队。多个生产者向通道发送事件时，其跨生产者先后顺序仍由通道实际入队顺序决定。
///
/// # 参数
///
/// - `callback_rx`：数据回调事件的唯一接收端。
/// - `max_concurrency`：同时运行的数据监听器调用数。
async fn data_callback_loop(
    callback_rx: mpsc::Receiver<CallbackEvent>,
    max_concurrency: usize,
    io_event_tx: mpsc::Sender<IoEvent>,
    shutdown: CancellationToken,
) {
    data_callback_loop_with_dispatch(
        callback_rx,
        max_concurrency,
        io_event_tx,
        shutdown,
        |callback| try_start_user_callback("open-net-ws-data-callback", callback),
    )
    .await;
}

/// Injection is local to this dispatcher; tests never change process-wide thread creation.
async fn data_callback_loop_with_dispatch<D, F>(
    mut callback_rx: mpsc::Receiver<CallbackEvent>,
    max_concurrency: usize,
    io_event_tx: mpsc::Sender<IoEvent>,
    shutdown: CancellationToken,
    mut dispatch: D,
) where
    D: FnMut(Box<dyn FnOnce() + Send>) -> std::io::Result<F>,
    F: std::future::Future<Output = ()> + Send,
{
    crate::log_t!(LogType::WSC; "data_callback_loop", "max_concurrency", max_concurrency);
    let mut in_flight = FuturesUnordered::new();
    loop {
        let event = if in_flight.len() >= max_concurrency {
            let _ = in_flight.next().await;
            continue;
        } else if in_flight.is_empty() {
            callback_rx.recv().await
        } else {
            tokio::select! {
                biased;
                event = callback_rx.recv() => event,
                _ = in_flight.next() => continue,
            }
        };
        let Some(event) = event else {
            break;
        };
        match event {
            CallbackEvent::Data {
                listener,
                response,
                byte_permit,
            } => {
                crate::log_s!(LogType::WSC; "data_callback_loop", "state|listener_present", "data_event_dequeued", listener.is_some());
                if let Some(listener) = listener {
                    let generation = response.connection_generation();
                    match dispatch(Box::new(move || listener(response))) {
                        Ok(callback_done) => in_flight.push(async move {
                            callback_done.await;
                            drop(byte_permit);
                        }),
                        Err(error) => {
                            drop(byte_permit);
                            crate::log_e!(LogType::WSC; "data_callback_loop", "stage|generation|error", "callback_thread_spawn_failed", generation, crate::common::log::summary::error(&error));
                            // One bounded, inline report. During shutdown the worker no longer
                            // consumes I/O events, but still drains callbacks within response grace.
                            // Cancel only this report, never the remaining callbacks/drain barrier.
                            tokio::select! {
                                biased;
                                _ = shutdown.cancelled() => {
                                    crate::log_s!(LogType::WSC; "data_callback_loop", "state|generation", "callback_failure_after_shutdown", generation);
                                }
                                sent = io_event_tx.send(IoEvent::CallbackDispatchFailed { generation }) => {
                                    if sent.is_err() {
                                        crate::log_e!(LogType::WSC; "data_callback_loop", "stage|generation", "callback_failure_receiver_closed", generation);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            CallbackEvent::DataDrain { delivered } => {
                while in_flight.next().await.is_some() {}
                let _ = delivered.send(());
                break;
            }
            CallbackEvent::Status { delivered, .. } => {
                crate::log_e!(LogType::WSC; "data_callback_loop", "error", "status_event_in_data_lane");
                if let Some(delivered) = delivered {
                    let _ = delivered.send(());
                }
            }
        }
    }
    while in_flight.next().await.is_some() {}
}

/// 串行执行状态监听器。该通道与数据回调 lane 独立，因此慢数据监听器不会拖住状态机。
async fn status_callback_loop(
    listeners: Arc<ListenerStore>,
    state: Arc<RwLock<ConnectionStatus>>,
    mut callback_rx: mpsc::Receiver<CallbackEvent>,
) {
    crate::log_t!(LogType::WSC; "status_callback_loop", "listeners|state|queued_events", "shared", "shared", callback_rx.len());
    while let Some(event) = callback_rx.recv().await {
        match event {
            CallbackEvent::Status { status, delivered } => {
                loop {
                    let registration = match listeners.status.read() {
                        Ok(listener) => listener.as_ref().map(Arc::clone),
                        Err(_) => {
                            crate::log_e!(LogType::WSC; "status_callback_loop", "error", "status_listener_lock_poisoned");
                            None
                        }
                    };
                    let Some(registration) = registration else {
                        break;
                    };
                    // Only this callback task waits. Replacement/removal must wake a
                    // task waiting on an old initial callback that may never return.
                    tokio::select! {
                        biased;
                        _ = registration.retired.cancelled() => continue,
                        _ = registration.initial_done.cancelled() => {}
                    }
                    if registration.retired.is_cancelled() {
                        continue;
                    }
                    // Read after initialization: coalesced events must not deliver an
                    // old Connecting snapshot after a slow initial call has completed.
                    let status = match state.read() {
                        Ok(current) => *current,
                        Err(_) => {
                            crate::log_e!(LogType::WSC; "status_callback_loop", "error", "state_lock_poisoned");
                            status
                        }
                    };
                    crate::log_s!(LogType::WSC; "status_callback_loop", "status|listener_present", format!("{status:?}"), true);
                    run_user_callback("open-net-ws-status-callback", move || {
                        (registration.listener)(status)
                    })
                    .await;
                    break;
                }
                if let Some(delivered) = delivered {
                    let _ = delivered.send(());
                }
            }
            CallbackEvent::Data { .. } => {
                crate::log_e!(LogType::WSC; "status_callback_loop", "error", "data_event_in_status_lane");
            }
            CallbackEvent::DataDrain { delivered } => {
                crate::log_e!(LogType::WSC; "status_callback_loop", "error", "drain_event_in_status_lane");
                let _ = delivered.send(());
            }
        }
    }
}

/// 按连接目标的重连策略执行一次完整的 WebSocket 连接周期。
///
/// 第零次尝试立即进行；后续每次尝试前使用 [`full_jitter_delay`] 计算指数退避。退避等待可
/// 被取消令牌终止，也可被网络恢复通知提前唤醒。每次尝试都会重新构建请求，因此动态请求
/// 头提供器也会重新执行；请求构建和网络握手共同受单次 `handshake_timeout` 限制。
/// 超时不能强制终止已经开始的同步 provider，但不会继续阻塞连接任务。仅
/// transport 分类认可的暂态错误或握手超时可以按策略继续尝试。
///
/// 成功时发送带本连接代编号的 `ConnectSucceeded` 后返回；遇到不可重试错误、重试次数
/// 耗尽或已达到累计时长限制时，发送一次 `ConnectFailed`。动态头提供器返回错误或阻塞
/// 任务异常结束时会立即结束本周期，不参与连接错误重试。取消时直接返回且不发送终结
/// 事件，由取消方负责后续状态与回复清理。累计时长只在一次握手失败后决定是否再试，
/// 不是对请求构建、退避和握手整个流程的硬超时。
///
/// # 参数
///
/// - `target`：本周期使用的 URL、请求头与重连策略快照。
/// - `generation`：标识本周期的连接代编号，随结果事件原样返回。
/// - `cancel`：取消退避或握手等待的令牌。
/// - `network_available`：允许网络恢复信号提前结束退避等待的通知器。
/// - `event_tx`：向工作器报告最终连接成功或失败的内部事件通道。
#[allow(clippy::too_many_arguments)]
async fn connect_with_retry(
    target: ConnectTarget,
    client_config: WebSocketClientConfig,
    network: Arc<CompiledNetworkConfig>,
    generation: u64,
    cancel: CancellationToken,
    network_available: Arc<Notify>,
    network_status: Option<watch::Receiver<NetworkStatusSnapshot>>,
    provider_slots: Arc<Semaphore>,
    event_tx: mpsc::Sender<IoEvent>,
) {
    let run = async {
        let started_at = Instant::now();
        let policy = target.options.reconnect.clone();
        let observes_network = network_status.is_some();
        let cycle_deadline = match network_gate::cycle_deadline(started_at, policy.max_elapsed) {
            Ok(deadline) => deadline,
            Err(error) => {
                let (failure, reason) = local_cycle_failure(error);
                publish_legacy_failure(&event_tx, generation, failure, reason).await;
                return;
            }
        };
        let mut gate = network_gate::NetworkGate::new(network_status);
        let mut attempt = 0usize;
        let (failure, reason) = loop {
            if attempt > 0 {
                if let Err(error) = gate
                    .backoff(
                        full_jitter_delay(&policy, attempt),
                        &network_available,
                        if observes_network {
                            cycle_deadline
                        } else {
                            None
                        },
                    )
                    .await
                {
                    break local_cycle_failure(error);
                }
                if observes_network
                    && cycle_deadline.is_some_and(|deadline| Instant::now() >= deadline)
                {
                    break local_cycle_failure(NetError::RetryExhausted);
                }
            }
            let epoch = match gate.wait_available(cycle_deadline).await {
                Ok(epoch) => epoch,
                Err(error) => break local_cycle_failure(error),
            };
            let Some(attempt_deadline) = Instant::now().checked_add(policy.handshake_timeout)
            else {
                break local_cycle_failure(NetError::ConfigError);
            };
            let operation = async {
                let slots = observes_network.then(|| Arc::clone(&provider_slots));
                let request = build_connect_request(&target, attempt_deadline, slots)
                    .await
                    .map_err(|error| {
                        let (error, stage) = match error {
                            ConnectAttemptError::Build(error) => {
                                (error, WebSocketConnectStage::RequestBuild)
                            }
                            ConnectAttemptError::ProviderTimedOut => {
                                (NetError::TimeoutError, WebSocketConnectStage::Provider)
                            }
                        };
                        WebSocketConnectionFailure::new(error, stage, None, false)
                    })?;
                let protocol = WebSocketConfig::default()
                    .read_buffer_size(client_config.read_buffer_size)
                    .write_buffer_size(client_config.write_buffer_size)
                    .max_write_buffer_size(client_config.max_write_buffer_size)
                    .max_message_size(client_config.max_message_size)
                    .max_frame_size(client_config.max_frame_size);
                network
                    .dial(
                        request,
                        protocol,
                        client_config.tcp_nodelay,
                        attempt_deadline,
                    )
                    .await
            };
            let result = match gate.run(epoch, operation).await {
                Ok(result) => result,
                Err(error) => Err(network_interruption_failure(error)),
            };
            let result = match result {
                Ok(stream) => {
                    configure_tcp_socket(
                        &stream,
                        client_config.tcp_send_buffer_size,
                        client_config.tcp_keepalive.as_ref(),
                    );
                    publish_connected(&event_tx, generation, None, stream, epoch)
                        .await
                        .map_err(network_interruption_failure)
                }
                Err(error) => Err(error),
            };
            match result {
                Ok(()) => return,
                Err(failure) => {
                    if !failure.retryable()
                        || !retry_allowed(&policy, attempt, started_at.elapsed())
                    {
                        let reason = if failure.retryable() && policy.enabled {
                            WebSocketTerminationReason::RetryExhausted
                        } else {
                            WebSocketTerminationReason::ConnectFailed
                        };
                        break (failure, reason);
                    }
                }
            }
            let Some(next_attempt) = attempt.checked_add(1) else {
                break local_cycle_failure(NetError::InternalError);
            };
            attempt = next_attempt;
        };
        publish_legacy_failure(&event_tx, generation, failure, reason).await;
    };
    tokio::select! {
        biased;
        _ = cancel.cancelled() => {}
        _ = run => {}
    }
}

async fn publish_legacy_failure(
    event_tx: &mpsc::Sender<IoEvent>,
    generation: u64,
    failure: WebSocketConnectionFailure,
    reason: WebSocketTerminationReason,
) {
    if event_tx
        .send(IoEvent::ConnectFailed {
            generation,
            failure,
            reason,
        })
        .await
        .is_err()
    {
        crate::log_e!(LogType::WSC; "connect_with_retry", "error", "worker_event_receiver_closed");
    }
}

/// Only local budget expiry grants recovery eligibility. User providers may return
/// any NetError, including RetryExhausted, and retain their original failure class.
fn local_cycle_failure(
    error: NetError,
) -> (WebSocketConnectionFailure, WebSocketTerminationReason) {
    let reason = if error == NetError::RetryExhausted {
        WebSocketTerminationReason::RetryExhausted
    } else {
        WebSocketTerminationReason::ConnectFailed
    };
    (
        WebSocketConnectionFailure::new(error, WebSocketConnectStage::EventDelivery, None, false),
        reason,
    )
}

fn network_interruption_failure(error: NetError) -> WebSocketConnectionFailure {
    WebSocketConnectionFailure::new(
        error,
        WebSocketConnectStage::EventDelivery,
        None,
        error == NetError::NetworkError,
    )
}

async fn publish_connected(
    events: &mpsc::Sender<IoEvent>,
    generation: u64,
    context_attempt_id: Option<u64>,
    stream: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    network_loss_epoch: Option<u64>,
) -> Result<(), NetError> {
    let (accepted, received) = oneshot::channel();
    events
        .send(IoEvent::ConnectSucceeded {
            generation,
            context_attempt_id,
            stream: Box::new(stream),
            network_loss_epoch,
            accepted: network_loss_epoch.map(|_| accepted),
        })
        .await
        .map_err(|_| NetError::EngineDropped)?;
    if network_loss_epoch.is_some() {
        received.await.map_err(|_| NetError::EngineDropped)?
    } else {
        Ok(())
    }
}

/// 一次握手尝试中，区分本地请求构建失败与真正的 WebSocket 连接失败。
enum ConnectAttemptError {
    Build(NetError),
    ProviderTimedOut,
}

/// 尽力配置已建立连接的底层 TCP socket。
///
/// 发送缓冲和 keepalive 在平台不支持或参数设置失败时保持连接可用；操作系统可能调整
/// 请求的缓冲大小，应用层仍须以对端观测到的 Ping/Pong 延迟作为最终验收指标。
fn configure_tcp_socket(
    stream: &tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    send_buffer_size: Option<usize>,
    config: Option<&crate::api::web_socket_client::TcpKeepaliveConfig>,
) {
    crate::log_t!(LogType::WSC; "configure_tcp_socket", "send_buffer_size|keepalive_enabled", send_buffer_size, config.is_some());
    let socket = SockRef::from(stream.get_ref().get_ref());
    if let Some(send_buffer_size) = send_buffer_size {
        if let Err(error) = socket.set_send_buffer_size(send_buffer_size) {
            crate::log_e!(LogType::WSC; "configure_tcp_socket", "option|requested_size|error", "send_buffer_size", send_buffer_size, crate::common::log::summary::error(&error));
        } else {
            crate::log_s!(LogType::WSC; "configure_tcp_socket", "option|state", "send_buffer_size", "applied");
        }
    }
    if let Some(config) = config {
        let keepalive = TcpKeepalive::new()
            .with_time(config.idle)
            .with_interval(config.interval);
        if let Err(error) = socket.set_tcp_keepalive(&keepalive) {
            crate::log_e!(LogType::WSC; "configure_tcp_socket", "option|error", "tcp_keepalive", crate::common::log::summary::error(&error));
        } else {
            crate::log_s!(LogType::WSC; "configure_tcp_socket", "option|state", "tcp_keepalive", "applied");
        }
    }
}

/// 根据连接目标构建一次 WebSocket 握手请求。
///
/// 首先由 URL 生成 Tungstenite 请求，再合并静态请求头与动态请求头。动态提供器在可与
/// Tokio runtime 脱离的 OS 线程中调用；这样即使用户闭包永久阻塞，取消/销毁 worker
/// 也不会被 runtime 析构等待拖住。动态头追加在静态头之后，因此同名项由较后的值覆盖。
/// 提供器与随后网络握手共享 `deadline`；提供器超时会结束本连接周期，不再启动更多
/// 可能同样阻塞的提供器线程。
///
/// # 参数
///
/// - `target`：包含 URL、静态请求头和可选动态请求头提供器的连接目标；
/// - `deadline`：本次请求构造与网络握手共享的绝对时限。
///
/// # 返回值
///
/// 成功时返回可交给 `connect_async` 的握手请求。确定性的构造错误包装为 `Build`；
/// 动态提供器未在 deadline 前结束则返回 `ProviderTimedOut`。
async fn build_connect_request(
    target: &ConnectTarget,
    deadline: Instant,
    provider_slots: Option<Arc<Semaphore>>,
) -> Result<Request<()>, ConnectAttemptError> {
    crate::log_t!(LogType::WSC; "build_connect_request", "url|header_count|provider_present|deadline_elapsed", crate::common::log::summary::url(&target.url), target.options.headers.len(), target.options.header_provider.is_some(), Instant::now() >= deadline);
    let mut request = target
        .url
        .as_str()
        .into_client_request()
        .map_err(|error| {
            crate::log_e!(LogType::WSC; "build_connect_request", "stage|error", "url_request", crate::common::log::summary::error(&error));
            ConnectAttemptError::Build(NetError::InvalidUrl)
        })?;
    let mut headers = target.options.headers.clone();
    if let Some(provider) = target.options.header_provider.as_ref() {
        // timeout_at polls its inner future first, including when the deadline is
        // already elapsed. Avoid spawning a user closure after its attempt budget.
        if Instant::now() >= deadline {
            return Err(ConnectAttemptError::ProviderTimedOut);
        }
        let permit = match provider_slots {
            Some(slots) => Some(
                tokio::time::timeout_at(deadline, slots.acquire_owned())
                    .await
                    .map_err(|_| ConnectAttemptError::ProviderTimedOut)?
                    .map_err(|_| ConnectAttemptError::Build(NetError::EngineDropped))?,
            ),
            None => None,
        };
        if Instant::now() >= deadline {
            return Err(ConnectAttemptError::ProviderTimedOut);
        }
        let dynamic_headers =
            tokio::time::timeout_at(deadline, run_header_provider_detached(Arc::clone(provider), permit))
                .await
                .map_err(|_| {
                    crate::log_e!(LogType::WSC; "build_connect_request", "stage|error", "header_provider", "timeout");
                    ConnectAttemptError::ProviderTimedOut
                })?
                .map_err(ConnectAttemptError::Build)?;
        headers.extend(dynamic_headers);
    }
    for (name, value) in headers {
        let name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|error| {
                crate::log_e!(LogType::WSC; "build_connect_request", "stage|error", "header_validation", crate::common::log::summary::error(&error));
                ConnectAttemptError::Build(NetError::ConfigError)
            })?;
        let value = HeaderValue::from_str(value.as_str())
            .map_err(|error| {
                crate::log_e!(LogType::WSC; "build_connect_request", "stage|error", "header_validation", crate::common::log::summary::error(&error));
                ConnectAttemptError::Build(NetError::ConfigError)
            })?;
        request.headers_mut().insert(name, value);
    }
    crate::log_s!(LogType::WSC; "build_connect_request", "state|header_count", "request_built", request.headers().len());
    Ok(request)
}

/// Executes a synchronous user header provider without attaching it to Tokio's blocking pool.
async fn run_header_provider_detached(
    provider: crate::api::web_socket_client::WebSocketHeaderProvider,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
) -> Result<Vec<(String, String)>, NetError> {
    crate::log_t!(LogType::WSC; "run_header_provider_detached", "provider", "registered");
    let (result_tx, result_rx) = oneshot::channel();
    std::thread::Builder::new()
        .name("open-net-ws-header-provider".to_string())
        .spawn(move || {
            let _permit = permit;
            let result = catch_unwind(AssertUnwindSafe(|| provider()))
                .unwrap_or_else(|_| {
                    crate::log_e!(LogType::WSC; "run_header_provider_detached", "error", "header_provider_panicked");
                    Err(NetError::TaskInterruptionError)
                });
            if let Err(error) = &result {
                crate::log_e!(LogType::WSC; "run_header_provider_detached", "stage|error", "header_provider", format!("{error:?}"));
            }
            if result_tx.send(result).is_err() {
                crate::log_s!(LogType::WSC; "run_header_provider_detached", "state", "result_receiver_dropped");
            }
        })
        .map_err(|error| {
            crate::log_e!(LogType::WSC; "run_header_provider_detached", "stage|error", "thread_spawn", crate::common::log::summary::error(&error));
            NetError::TaskInterruptionError
        })?;
    result_rx
        .await
        .unwrap_or_else(|_| {
            crate::log_e!(LogType::WSC; "run_header_provider_detached", "error", "provider_result_channel_closed");
            Err(NetError::TaskInterruptionError)
        })
}

/// 判断一次失败后是否仍允许安排下一次连接尝试。
///
/// `attempt` 是刚失败尝试的零基编号：初始尝试为 `0`，因此条件 `attempt < max_retries`
/// 使 `max_retries` 精确表示初始尝试之后最多允许的额外次数。只有策略启用、额外次数尚未
/// 耗尽，且失败发生时的累计耗时仍小于可选 `max_elapsed`，才允许继续。
///
/// # 参数
///
/// - `policy`：当前连接周期的重连策略。
/// - `attempt`：刚结束的连接尝试的零基编号。
/// - `elapsed`：从本连接周期开始到该次失败时已经经过的时间。
///
/// # 返回值
///
/// 若可在本次失败后再安排一次尝试，返回 `true`。
fn retry_allowed(
    policy: &crate::api::web_socket_client::ReconnectPolicy,
    attempt: usize,
    elapsed: Duration,
) -> bool {
    crate::log_t!(LogType::WSC; "retry_allowed", "attempt|elapsed_seconds|max_retries|enabled", attempt, elapsed.as_secs_f64(), policy.max_retries, policy.enabled);
    if !policy.enabled || attempt >= policy.max_retries {
        return false;
    }
    policy
        .max_elapsed
        .is_none_or(|max_elapsed| elapsed < max_elapsed)
}

/// 计算一次“全抖动”指数退避时长。
///
/// 第一次重试（`attempt == 1`）的上限为 `initial_delay`，之后按二的幂增长，并受
/// `max_delay` 限制；指数移位最多增长到 31 位，乘法溢出时直接采用最大延迟。时长换算为
/// 纳秒时会在 `u64::MAX` 封顶，以避免超大 `Duration` 溢出。当上限小于
/// `u64::MAX` 纳秒时，最终从包含两端的区间 `[0, cap]` 取样；饱和到
/// `u64::MAX` 时，由于模数上界也采用饱和加法，最大可取到 `u64::MAX - 1` 纳秒。
///
/// # 参数
///
/// - `policy`：提供初始延迟与最大延迟的重连策略。
/// - `attempt`：即将开始的重试编号，从 `1` 起计；传入 `0` 时也按第一次重试处理。
///
/// # 返回值
///
/// 返回本次重试前应等待的随机时长，保证不超过计算得到的退避上限。
fn full_jitter_delay(
    policy: &crate::api::web_socket_client::ReconnectPolicy,
    attempt: usize,
) -> Duration {
    crate::log_t!(LogType::WSC; "full_jitter_delay", "attempt|max_delay_seconds", attempt, policy.max_delay.as_secs_f64());
    let shift = (attempt.saturating_sub(1)).min(31) as u32;
    let factor = 1u32.checked_shl(shift).unwrap_or(u32::MAX);
    let cap = policy
        .initial_delay
        .checked_mul(factor)
        .unwrap_or(policy.max_delay)
        .min(policy.max_delay);
    let cap_nanos = cap.as_nanos().min(u64::MAX as u128) as u64;
    if cap_nanos == 0 {
        return Duration::ZERO;
    }
    let random = next_jitter_random();
    Duration::from_nanos(random % cap_nanos.saturating_add(1))
}

/// 以无锁方式推进全局 xorshift 状态并返回下一伪随机值。
///
/// 通过弱比较交换循环处理并发竞争；`Relaxed` 顺序足以保证原子状态不撕裂，因为该状态与
/// 其他内存没有同步关系。生成结果仅用于分散重连时间，不具备密码学安全性。
///
/// # 返回值
///
/// 返回推进成功后存入 [`JITTER_STATE`] 的 64 位伪随机值。
fn next_jitter_random() -> u64 {
    crate::log_t!(LogType::WSC; "next_jitter_random");
    let mut current = JITTER_STATE.load(Ordering::Relaxed);
    loop {
        let mut next = current;
        next ^= next << 13;
        next ^= next >> 7;
        next ^= next << 17;
        match JITTER_STATE.compare_exchange_weak(
            current,
            next,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => return next,
            Err(observed) => current = observed,
        }
    }
}

#[cfg(test)]
#[path = "ws_client_worker/status_dispatch_tests.rs"]
mod status_dispatch_tests;

#[cfg(test)]
/// WebSocket 工作器状态机与退避算法的单元测试。
mod tests {
    use super::*;
    use crate::api::traits::ws::ws_body::WsBody;
    use crate::api::traits::ws::ws_request_trait::WSRequestTrait;
    use crate::api::web_socket_client::ReconnectPolicy;
    use crate::module::ws_client::test_support::{check, check_eq, test_error, TestResult};
    use crate::module::ws_client::write::queued_request::DispatchPhase;
    use crate::module::ws_client::ws_client_inner::WSClientInner;
    use std::sync::{Condvar, Mutex};
    use tokio_tungstenite::tungstenite::Message;

    #[derive(Default)]
    struct CallbackConcurrencyState {
        first_started: bool,
        second_started: bool,
        release_first: bool,
    }

    type SharedCallbackState = Arc<(Mutex<CallbackConcurrencyState>, Condvar)>;

    /// Unblock callback threads even when a test returns early with an error.
    struct CallbackRelease(SharedCallbackState);

    impl CallbackRelease {
        fn release(&self) -> TestResult {
            let (lock, changed) = &*self.0;
            lock.lock()
                .map_err(|error| test_error(format!("callback state lock: {error:?}")))?
                .release_first = true;
            changed.notify_all();
            Ok(())
        }
    }

    impl Drop for CallbackRelease {
        fn drop(&mut self) {
            let (lock, changed) = &*self.0;
            let mut state = match lock.lock() {
                Ok(state) => state,
                Err(error) => {
                    crate::log_e!(LogType::WSC; "test_callback", "error", format!("callback state lock poisoned during test cleanup: {error}"));
                    error.into_inner()
                }
            };
            state.release_first = true;
            changed.notify_all();
        }
    }

    fn concurrency_listener(
        state: SharedCallbackState,
        results: std::sync::mpsc::Sender<TestResult>,
        request_id: Option<&'static str>,
    ) -> crate::module::ws_client::listener_store::DataListener {
        Arc::new(move |response| {
            let result = (|| -> TestResult {
                let Message::Binary(payload) = response.message() else {
                    return Err(test_error("expected binary callback payload"));
                };
                let (lock, changed) = &*state;
                match payload.first().copied() {
                    Some(1) => {
                        let mut state = lock.lock().map_err(|error| {
                            test_error(format!("callback state lock: {error:?}"))
                        })?;
                        state.first_started = true;
                        changed.notify_all();
                        while !state.release_first {
                            state = changed.wait(state).map_err(|error| {
                                test_error(format!("callback state lock: {error:?}"))
                            })?;
                        }
                    }
                    Some(2) => {
                        if let Some(request_id) = request_id {
                            check!(
                                response.take_request(request_id).is_some(),
                                "queued response must claim its pending request"
                            )?;
                        }
                        let mut state = lock.lock().map_err(|error| {
                            test_error(format!("callback state lock: {error:?}"))
                        })?;
                        state.second_started = true;
                        changed.notify_all();
                    }
                    payload => {
                        return Err(test_error(format!(
                            "unexpected callback payload: {payload:?}"
                        )))
                    }
                }
                Ok(())
            })();
            if results.send(result).is_err() {
                crate::log_e!(LogType::WSC; "test_callback", "error", format!("callback result receiver closed"));
            }
        })
    }

    async fn wait_for_callbacks(
        state: &SharedCallbackState,
        ready: impl Fn(&CallbackConcurrencyState) -> bool,
        context: &str,
    ) -> TestResult {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let is_ready = {
                    let state = state
                        .0
                        .lock()
                        .map_err(|error| test_error(format!("callback state lock: {error:?}")))?;
                    ready(&state)
                };
                if is_ready {
                    break Ok::<(), Box<dyn std::error::Error + Send + Sync>>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|error| test_error(format!("{context}: {error}")))?
    }

    struct CallbackTestRequest(&'static str);

    impl WSRequestTrait for CallbackTestRequest {
        fn uuid(&self) -> String {
            self.0.to_string()
        }

        fn body(&self) -> Result<WsBody, NetError> {
            Ok(WsBody::Text(self.0.to_string()))
        }
    }

    #[tokio::test]
    async fn connection_lifecycle_logs_committed_status_transitions() -> TestResult {
        use crate::common::log::logger::Logger;
        let (log_tx, log_rx) = std::sync::mpsc::channel();
        let generation = 9_876_543_210_u64;
        let _subscription = Logger::register_log_listener_with_capacity(
            Box::new(move |record| {
                if record.tag == "ON_WSC-set_status-S" {
                    let result = serde_json::from_str::<serde_json::Value>(&record.content)
                        .map_err(|error| test_error(format!("log JSON: {error}")));
                    let relevant = match &result {
                        Ok(content) => content["generation"] == generation && content["changed"] == true,
                        Err(_) => true,
                    };
                    if relevant && log_tx.send(result).is_err() {
                        crate::log_e!(LogType::WSC; "test_callback", "error", format!("state transition log receiver closed"));
                    }
                }
            }),
            &[LogType::WSC],
            16_384,
        )
        .map_err(|error| test_error(format!("log subscription: {error:?}")))?;
        let (_inner, mut worker) = WSClientInner::new(WebSocketClientConfig::default())
            .map_err(|error| test_error(format!("worker: {error:?}")))?;
        worker.generation = generation;
        for status in [
            ConnectionStatus::Connecting,
            ConnectionStatus::Connected,
            ConnectionStatus::Closing,
            ConnectionStatus::Closed,
        ] {
            worker.set_status(status).await;
        }
        let mut previous = "Idle";
        for expected in ["Connecting", "Connected", "Closing", "Closed"] {
            let content = log_rx
                .recv_timeout(Duration::from_secs(2))
                .map_err(|error| test_error(format!("state transition log: {error:?}")))??;
            check_eq!(content["from"], format!("Some({previous})"))?;
            check_eq!(content["to"], expected)?;
            previous = expected;
        }
        check_eq!(worker.current_status(), ConnectionStatus::Closed)?;
        Ok(())
    }

    #[tokio::test]
    async fn data_callback_concurrency_prevents_one_blocked_response_from_stalling_the_next(
    ) -> TestResult {
        let listeners = Arc::new(ListenerStore::default());
        let state = Arc::new((
            Mutex::new(CallbackConcurrencyState::default()),
            Condvar::new(),
        ));
        let release = CallbackRelease(Arc::clone(&state));
        let (callback_result_tx, callback_result_rx) = std::sync::mpsc::channel();
        *listeners
            .data
            .write()
            .map_err(|error| test_error(format!("listener lock: {error:?}")))? = Some(
            concurrency_listener(Arc::clone(&state), callback_result_tx, None),
        );
        let (callback_tx, callback_rx) = mpsc::channel(2);
        let callback_bytes = Arc::new(Semaphore::new(2));
        let listener = listeners
            .data
            .read()
            .map_err(|error| test_error(format!("listener lock: {error:?}")))?
            .as_ref()
            .map(Arc::clone)
            .ok_or_else(|| test_error("data listener"))?;
        let callback_task = tokio::spawn(data_callback_loop(
            callback_rx,
            2,
            mpsc::channel(1).0,
            CancellationToken::new(),
        ));
        for payload in [1_u8, 2] {
            callback_tx
                .send(CallbackEvent::Data {
                    listener: Some(Arc::clone(&listener)),
                    response: crate::WSCResponse::new(
                        Message::Binary(vec![payload].into()),
                        PendingRequestView::default(),
                        1,
                    ),
                    byte_permit: Arc::clone(&callback_bytes)
                        .try_acquire_owned()
                        .map_err(|error| test_error(format!("callback byte permit: {error:?}")))?,
                })
                .await
                .map_err(|error| test_error(format!("queue callback: {error:?}")))?;
        }

        wait_for_callbacks(
            &state,
            |state| state.first_started && state.second_started,
            "both callbacks start concurrently",
        )
        .await?;

        release.release()?;
        drop(callback_tx);
        tokio::time::timeout(Duration::from_secs(1), callback_task)
            .await
            .map_err(|error| test_error(format!("callback loop stops: {error:?}")))?
            .map_err(|error| test_error(format!("callback task: {error:?}")))?;
        for _ in 0..2 {
            callback_result_rx.recv_timeout(Duration::from_secs(1))??;
        }
        Ok(())
    }

    #[tokio::test]
    async fn queued_data_callback_survives_listener_clear_while_lane_is_blocked() -> TestResult {
        let listeners = Arc::new(ListenerStore::default());
        let state = Arc::new((
            Mutex::new(CallbackConcurrencyState::default()),
            Condvar::new(),
        ));
        let release = CallbackRelease(Arc::clone(&state));
        let (callback_result_tx, callback_result_rx) = std::sync::mpsc::channel();
        *listeners
            .data
            .write()
            .map_err(|error| test_error(format!("listener lock: {error:?}")))? = Some(
            concurrency_listener(Arc::clone(&state), callback_result_tx, None),
        );
        let listener = listeners
            .data
            .read()
            .map_err(|error| test_error(format!("listener lock: {error:?}")))?
            .as_ref()
            .map(Arc::clone)
            .ok_or_else(|| test_error("data listener"))?;
        let (callback_tx, callback_rx) = mpsc::channel(2);
        let callback_bytes = Arc::new(Semaphore::new(2));
        let callback_task = tokio::spawn(data_callback_loop(
            callback_rx,
            1,
            mpsc::channel(1).0,
            CancellationToken::new(),
        ));

        for payload in [1_u8, 2] {
            callback_tx
                .send(CallbackEvent::Data {
                    listener: Some(Arc::clone(&listener)),
                    response: crate::WSCResponse::new(
                        Message::Binary(vec![payload].into()),
                        PendingRequestView::default(),
                        1,
                    ),
                    byte_permit: Arc::clone(&callback_bytes)
                        .try_acquire_owned()
                        .map_err(|error| test_error(format!("callback byte permit: {error:?}")))?,
                })
                .await
                .map_err(|error| test_error(format!("queue callback: {error:?}")))?;
        }

        wait_for_callbacks(&state, |state| state.first_started, "first callback starts").await?;

        *listeners
            .data
            .write()
            .map_err(|error| test_error(format!("listener lock: {error:?}")))? = None;
        release.release()?;

        wait_for_callbacks(
            &state,
            |state| state.second_started,
            "second callback starts after listener clear",
        )
        .await?;

        drop(callback_tx);
        tokio::time::timeout(Duration::from_secs(1), callback_task)
            .await
            .map_err(|error| test_error(format!("callback loop stops: {error:?}")))?
            .map_err(|error| test_error(format!("callback task: {error:?}")))?;
        for _ in 0..2 {
            callback_result_rx.recv_timeout(Duration::from_secs(1))??;
        }
        Ok(())
    }

    #[tokio::test]
    async fn terminal_drain_allows_queued_response_to_claim_within_original_grace() -> TestResult {
        const REQUEST_ID: &str = "queued-response-during-shutdown";
        let (inner, worker) = WSClientInner::new(WebSocketClientConfig {
            data_callback_concurrency: 1,
            response_dispatch_grace: Duration::from_millis(500),
            ..WebSocketClientConfig::default()
        })
        .map_err(|error| test_error(format!("create worker: {error:?}")))?;
        let pending = worker.pending_requests.clone();
        let (token, completion) = pending
            .reserve(
                Arc::new(CallbackTestRequest(REQUEST_ID)),
                &crate::WSRequestConfig::default(),
            )
            .map_err(|error| test_error(format!("reserve pending request: {error:?}")))?;
        check!(pending.mark_writing(REQUEST_ID, token, 1, 1))?;
        check!(pending.mark_sent(REQUEST_ID, token).is_some())?;

        let state = Arc::new((
            Mutex::new(CallbackConcurrencyState::default()),
            Condvar::new(),
        ));
        let release = CallbackRelease(Arc::clone(&state));
        let (callback_result_tx, callback_result_rx) = std::sync::mpsc::channel();
        *worker
            .listeners
            .data
            .write()
            .map_err(|error| test_error(format!("listener lock: {error:?}")))? = Some(
            concurrency_listener(Arc::clone(&state), callback_result_tx, Some(REQUEST_ID)),
        );
        let captured_listener = worker
            .listeners
            .data
            .read()
            .map_err(|error| test_error(format!("listener lock: {error:?}")))?
            .as_ref()
            .map(Arc::clone)
            .ok_or_else(|| test_error("data listener"))?;
        for payload in [1_u8, 2] {
            check!(worker
                .data_callback_tx
                .try_send(CallbackEvent::Data {
                    listener: Some(Arc::clone(&captured_listener)),
                    response: crate::WSCResponse::new(
                        Message::Binary(vec![payload].into()),
                        pending.clone(),
                        1,
                    ),
                    byte_permit: Arc::clone(&worker.data_callback_bytes)
                        .try_acquire_owned()
                        .map_err(|error| test_error(format!("callback byte permit: {error:?}")))?,
                })
                .is_ok())?;
        }

        let shutdown = worker.shutdown.clone();
        let shutdown_complete = worker.shutdown_complete.clone();
        let listeners = Arc::clone(&worker.listeners);
        let worker_task = tokio::spawn(worker.run_async());
        wait_for_callbacks(&state, |state| state.first_started, "first callback starts").await?;

        shutdown.cancel();
        tokio::time::timeout(Duration::from_millis(200), async {
            while inner.connection_status() != ConnectionStatus::Closed {
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|error| {
            test_error(format!(
                "worker enters Closed before grace expires: {error:?}"
            ))
        })?;
        release.release()?;

        tokio::time::timeout(Duration::from_secs(1), shutdown_complete.cancelled())
            .await
            .map_err(|error| test_error(format!("terminal callback drain completes: {error:?}")))?;
        check_eq!(completion.wait().await, Ok(()))?;
        check!(
            state
                .0
                .lock()
                .map_err(|error| test_error(format!("callback state lock: {error:?}")))?
                .second_started
        )?;
        check!(listeners
            .data
            .read()
            .map_err(|error| test_error(format!("listener lock: {error:?}")))?
            .is_none())?;
        worker_task
            .await
            .map_err(|error| test_error(format!("worker task: {error:?}")))?;
        for _ in 0..2 {
            callback_result_rx.recv_timeout(Duration::from_secs(1))??;
        }
        Ok(())
    }

    #[tokio::test]
    async fn terminal_grace_survives_response_handoff_beyond_listener_return() -> TestResult {
        const REQUEST_ID: &str = "response-handed-to-application-runtime";
        let (inner, worker) = WSClientInner::new(WebSocketClientConfig {
            response_dispatch_grace: Duration::from_millis(500),
            ..WebSocketClientConfig::default()
        })
        .map_err(|error| test_error(format!("create worker: {error:?}")))?;
        let pending = worker.pending_requests.clone();
        let (token, completion) = pending
            .reserve(
                Arc::new(CallbackTestRequest(REQUEST_ID)),
                &crate::WSRequestConfig::default(),
            )
            .map_err(|error| test_error(format!("reserve pending request: {error:?}")))?;
        check!(pending.mark_writing(REQUEST_ID, token, 1, 1))?;
        check!(pending.mark_sent(REQUEST_ID, token).is_some())?;

        let (response_tx, response_rx) = std::sync::mpsc::channel();
        let (handoff_tx, handoff_rx) = std::sync::mpsc::channel();
        let listener: crate::module::ws_client::listener_store::DataListener = Arc::new(
            move |response| {
                let result = response_tx.send(response).map_err(|error| {
                    test_error(format!(
                        "handoff response to application runtime: {error:?}"
                    ))
                });
                if handoff_tx.send(result).is_err() {
                    crate::log_e!(LogType::WSC; "test_callback", "error", format!("response handoff result receiver closed"));
                }
            },
        );
        *worker
            .listeners
            .data
            .write()
            .map_err(|error| test_error(format!("listener lock: {error:?}")))? =
            Some(Arc::clone(&listener));
        worker
            .data_callback_tx
            .try_send(CallbackEvent::Data {
                listener: Some(listener),
                response: crate::WSCResponse::new(
                    Message::Binary(vec![1].into()),
                    pending.clone(),
                    1,
                ),
                byte_permit: Arc::clone(&worker.data_callback_bytes)
                    .try_acquire_owned()
                    .map_err(|error| test_error(format!("callback byte permit: {error:?}")))?,
            })
            .map_err(|error| test_error(format!("queue response callback: {error:?}")))?;

        let data_lane = worker.data_callback_tx.clone();
        let shutdown = worker.shutdown.clone();
        let shutdown_complete = worker.shutdown_complete.clone();
        let worker_task = tokio::spawn(worker.run_async());
        let response = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                match response_rx.try_recv() {
                    Ok(response) => break Ok(response),
                    Err(std::sync::mpsc::TryRecvError::Empty) => tokio::task::yield_now().await,
                    Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                        break Err(test_error("application response channel closed"))
                    }
                }
            }
        })
        .await
        .map_err(|error| test_error(format!("listener hands response off: {error:?}")))??;
        handoff_rx.recv_timeout(Duration::from_secs(1))??;

        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(1), data_lane.closed())
            .await
            .map_err(|error| {
                test_error(format!("terminal data-lane barrier completes: {error:?}"))
            })?;
        check!(
            response.take_request(REQUEST_ID).is_some(),
            "handoff consumer must retain the original grace after listener return"
        )?;

        tokio::time::timeout(Duration::from_secs(1), shutdown_complete.cancelled())
            .await
            .map_err(|error| {
                test_error(format!(
                    "shutdown completes after response claim: {error:?}"
                ))
            })?;
        check_eq!(completion.wait().await, Ok(()))?;
        check!(pending.is_empty())?;
        check_eq!(inner.connection_status(), ConnectionStatus::Closed)?;
        worker_task
            .await
            .map_err(|error| test_error(format!("worker task: {error:?}")))?;
        Ok(())
    }

    #[tokio::test]
    async fn zero_response_grace_does_not_enqueue_a_data_drain_barrier() -> TestResult {
        let (_inner, mut worker) = WSClientInner::new(WebSocketClientConfig {
            response_dispatch_grace: Duration::ZERO,
            ..WebSocketClientConfig::default()
        })
        .map_err(|error| test_error(format!("create worker: {error:?}")))?;
        let mut callback_rx = worker
            .data_callback_rx
            .take()
            .ok_or_else(|| test_error("data callback receiver"))?;

        worker
            .drain_data_callbacks_until(worker.response_dispatch_grace_deadline())
            .await;

        check!(matches!(
            callback_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ))?;
        Ok(())
    }

    #[test]
    /// 验证多次重试的全抖动结果始终不超过策略配置的全局最大延迟。
    fn jitter_is_within_exponential_cap() -> TestResult {
        let policy = ReconnectPolicy {
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_millis(500),
            ..ReconnectPolicy::default()
        };
        for attempt in 1..10 {
            check!(full_jitter_delay(&policy, attempt) <= policy.max_delay)?;
        }
        Ok(())
    }

    #[test]
    /// 抽查代表性的合法生命周期边，并验证跨阶段跳转及从终态重新连接会被拒绝。
    fn connection_state_machine_rejects_terminal_and_skipped_transitions() -> TestResult {
        check!(connection_status_transition_allowed(
            ConnectionStatus::Idle,
            ConnectionStatus::Connecting
        ))?;
        check!(connection_status_transition_allowed(
            ConnectionStatus::Connected,
            ConnectionStatus::Reconnecting
        ))?;
        check!(connection_status_transition_allowed(
            ConnectionStatus::Closing,
            ConnectionStatus::Closed
        ))?;
        check!(!connection_status_transition_allowed(
            ConnectionStatus::Idle,
            ConnectionStatus::Connected
        ))?;
        check!(!connection_status_transition_allowed(
            ConnectionStatus::Closed,
            ConnectionStatus::Connecting
        ))?;
        Ok(())
    }

    #[test]
    fn websocket_http_error_keeps_status_code() -> TestResult {
        let response = tokio_tungstenite::tungstenite::http::Response::builder()
            .status(429)
            .body(Some(Vec::new()))
            .map_err(|error| test_error(format!("HTTP response: {error:?}")))?;
        check_eq!(
            crate::module::transport::classify_upgrade_error(WsError::Http(Box::new(response)))
                .http_status(),
            Some(429)
        )?;
        Ok(())
    }

    #[tokio::test]
    async fn invalid_status_transitions_log_errors_and_preserve_state() -> TestResult {
        use crate::common::log::logger::Logger;
        let (log_tx, log_rx) = std::sync::mpsc::channel();
        let _subscription = Logger::register_log_listener_with_capacity(
            Box::new(move |record| {
                if matches!(
                    record.tag.as_str(),
                    "ON_WSC-set_status-E" | "ON_WSC-set_status_and_wait-E"
                ) {
                    let result = serde_json::from_str::<serde_json::Value>(&record.content)
                        .map_err(|error| test_error(format!("status error log JSON: {error}")));
                    if log_tx.send(result).is_err() {
                        crate::log_e!(LogType::WSC; "test_callback", "error", format!("status error log receiver closed"));
                    }
                }
            }),
            &[LogType::WSC],
            16_384,
        )?;
        let (_inner, mut worker) = WSClientInner::new(WebSocketClientConfig::default())?;
        let status_rx = worker
            .status_callback_rx
            .take()
            .ok_or_else(|| test_error("status callback receiver"))?;

        worker.set_status(ConnectionStatus::Connected).await;
        check_eq!(worker.current_status(), ConnectionStatus::Idle)?;
        check_eq!(status_rx.len(), 0)?;
        tokio::time::timeout(
            Duration::from_secs(1),
            worker.set_status_and_wait(ConnectionStatus::Connected),
        )
        .await?;
        check_eq!(worker.current_status(), ConnectionStatus::Idle)?;
        check_eq!(status_rx.len(), 0)?;
        for _ in 0..2 {
            let content = log_rx.recv_timeout(Duration::from_secs(1))??;
            check_eq!(content["error"], "invalid_status_transition")?;
            check_eq!(content["from"], "Idle")?;
            check_eq!(content["to"], "Connected")?;
        }

        worker.set_status(ConnectionStatus::Connecting).await;
        check_eq!(worker.current_status(), ConnectionStatus::Connecting)?;
        check_eq!(status_rx.len(), 1)?;
        Ok(())
    }

    #[tokio::test]
    async fn misrouted_callback_events_log_acknowledge_and_release_capacity() -> TestResult {
        use crate::common::log::logger::Logger;
        let (log_tx, log_rx) = std::sync::mpsc::channel();
        let _subscription = Logger::register_log_listener_with_capacity(
            Box::new(move |record| {
                if matches!(
                    record.tag.as_str(),
                    "ON_WSC-data_callback_loop-E" | "ON_WSC-status_callback_loop-E"
                ) {
                    let result = serde_json::from_str::<serde_json::Value>(&record.content)
                        .map_err(|error| test_error(format!("callback error log JSON: {error}")));
                    if log_tx.send(result).is_err() {
                        crate::log_e!(LogType::WSC; "test_callback", "error", format!("callback error log receiver closed"));
                    }
                }
            }),
            &[LogType::WSC],
            16_384,
        )?;
        let (data_tx, data_rx) = mpsc::channel(3);
        let data_bytes = Arc::new(Semaphore::new(1));
        let (misrouted_status_tx, misrouted_status_rx) = oneshot::channel();
        data_tx
            .send(CallbackEvent::Status {
                status: ConnectionStatus::Connected,
                delivered: Some(misrouted_status_tx),
            })
            .await?;
        let (data_seen_tx, data_seen_rx) = std::sync::mpsc::channel();
        data_tx.send(CallbackEvent::Data {
            listener: Some(Arc::new(move |_response| {
                if data_seen_tx.send(()).is_err() {
                    crate::log_e!(LogType::WSC; "test_callback", "error", format!("data callback observation receiver closed"));
                }
            })),
            response: crate::WSCResponse::new(Message::Binary(vec![1].into()), PendingRequestView::default(), 1),
            byte_permit: Arc::clone(&data_bytes).try_acquire_owned()?,
        }).await?;
        let (data_drained_tx, data_drained_rx) = oneshot::channel();
        data_tx
            .send(CallbackEvent::DataDrain {
                delivered: data_drained_tx,
            })
            .await?;
        drop(data_tx);
        tokio::time::timeout(
            Duration::from_secs(1),
            data_callback_loop(data_rx, 1, mpsc::channel(1).0, CancellationToken::new()),
        )
        .await?;
        tokio::time::timeout(Duration::from_secs(1), misrouted_status_rx).await??;
        tokio::time::timeout(Duration::from_secs(1), data_drained_rx).await??;
        data_seen_rx.recv_timeout(Duration::from_secs(1))?;
        check_eq!(data_bytes.available_permits(), 1)?;

        let listeners = Arc::new(ListenerStore::default());
        let state = Arc::new(RwLock::new(ConnectionStatus::Connected));
        let (status_seen_tx, status_seen_rx) = std::sync::mpsc::channel();
        let registration = Arc::new(
            crate::module::ws_client::listener_store::StatusRegistration::new(Arc::new(
                move |status| {
                    if status_seen_tx.send(status).is_err() {
                        crate::log_e!(LogType::WSC; "test_callback", "error", format!("status callback observation receiver closed"));
                    }
                },
            )),
        );
        registration.initial_done.cancel();
        *listeners
            .status
            .write()
            .map_err(|error| test_error(format!("status listener lock: {error}")))? =
            Some(registration);
        let (status_tx, status_rx) = mpsc::channel(3);
        let status_bytes = Arc::new(Semaphore::new(1));
        status_tx
            .send(CallbackEvent::Data {
                listener: None,
                response: crate::WSCResponse::new(
                    Message::Binary(vec![2].into()),
                    PendingRequestView::default(),
                    1,
                ),
                byte_permit: Arc::clone(&status_bytes).try_acquire_owned()?,
            })
            .await?;
        let (misrouted_drain_tx, misrouted_drain_rx) = oneshot::channel();
        status_tx
            .send(CallbackEvent::DataDrain {
                delivered: misrouted_drain_tx,
            })
            .await?;
        let (status_delivered_tx, status_delivered_rx) = oneshot::channel();
        status_tx
            .send(CallbackEvent::Status {
                status: ConnectionStatus::Connecting,
                delivered: Some(status_delivered_tx),
            })
            .await?;
        drop(status_tx);
        tokio::time::timeout(
            Duration::from_secs(1),
            status_callback_loop(listeners, state, status_rx),
        )
        .await?;
        tokio::time::timeout(Duration::from_secs(1), misrouted_drain_rx).await??;
        tokio::time::timeout(Duration::from_secs(1), status_delivered_rx).await??;
        check_eq!(
            status_seen_rx.recv_timeout(Duration::from_secs(1))?,
            ConnectionStatus::Connected
        )?;
        check_eq!(status_bytes.available_permits(), 1)?;
        for expected in [
            "status_event_in_data_lane",
            "data_event_in_status_lane",
            "drain_event_in_status_lane",
        ] {
            let content = log_rx.recv_timeout(Duration::from_secs(1))??;
            check_eq!(content["error"], expected)?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn full_status_callback_lane_stays_bounded_and_does_not_block_state() -> TestResult {
        let (_inner, mut worker) = WSClientInner::new(WebSocketClientConfig {
            status_callback_queue_capacity: 1,
            ..WebSocketClientConfig::default()
        })
        .map_err(|error| test_error(format!("create worker: {error:?}")))?;
        let mut callback_rx = worker
            .status_callback_rx
            .take()
            .ok_or_else(|| test_error("status callback receiver"))?;

        worker.set_status(ConnectionStatus::Connecting).await;
        worker.set_status(ConnectionStatus::Connected).await;

        check_eq!(worker.current_status(), ConnectionStatus::Connected)?;
        check_eq!(callback_rx.len(), 1)?;
        check!(matches!(
            callback_rx.try_recv(),
            Ok(CallbackEvent::Status {
                status: ConnectionStatus::Connecting,
                delivered: None,
            })
        ))?;
        Ok(())
    }

    #[tokio::test]
    async fn forced_io_abort_preserves_in_flight_delivery_unknown() -> TestResult {
        use crate::api::wsc::{
            WebSocketTaskDelivery, WebSocketTaskEventOptions, WebSocketTaskPhase,
            WebSocketTaskSource,
        };
        use crate::module::ws_client::task_observer::TaskObservation;

        let (inner, mut worker) = WSClientInner::new(WebSocketClientConfig {
            close_timeout: Duration::from_millis(40),
            data_frame_write_timeout: Duration::from_secs(5),
            ..WebSocketClientConfig::default()
        })
        .map_err(|error| test_error(format!("create worker: {error:?}")))?;
        let (event_tx, events) = std::sync::mpsc::channel();
        worker.task_observers.register(
            Box::new(move |event| {
                if event_tx.send(event).is_err() {
                    eprintln!("forced abort task event receiver closed");
                }
            }),
            WebSocketTaskEventOptions::new(1, 1024),
        )?;
        let registration = worker
            .task_observers
            .snapshot()?
            .ok_or_else(|| test_error("forced abort task registration missing"))?;
        let observation = TaskObservation::new(
            1,
            1,
            None,
            WebSocketTaskSource::Body(crate::WsBody::Binary(vec![1; 1024].into())),
            false,
            None,
            registration.try_reserve(1024, false)?,
        );
        let dispatch_cancel = CancellationToken::new();
        let dispatch_phase = DispatchPhase::with_observation(Some(Arc::clone(&observation)));
        let result_rx = worker
            .queue
            .enqueue(
                "in-flight".to_string(),
                None,
                Message::Binary(vec![1; 1024].into()),
                1024,
                crate::WSRequestConfig::default(),
                &worker.shutdown,
                dispatch_cancel,
                dispatch_phase.clone(),
                CancellationToken::new(),
            )
            .await
            .map_err(|error| test_error(format!("enqueue request: {error:?}")))?;
        let request = worker
            .queue
            .next()
            .await
            .ok_or_else(|| test_error("writer takes request"))?;
        check!(dispatch_phase.start_writing())?;
        check!(dispatch_phase.start_data_write())?;
        observation.mark_writing(worker.generation);
        observation.set_cause(WebSocketTaskEndCause::Shutdown);

        let cancel = CancellationToken::new();
        let (control_tx, control_rx) = mpsc::channel(1);
        let read_handle = tokio::spawn(std::future::pending::<()>());
        let write_handle = tokio::spawn(async move {
            let _request = request;
            let _keep_control_open = control_rx;
            std::future::pending::<()>().await;
        });
        worker.active_io = Some(ActiveIo {
            generation: worker.generation,
            cancel,
            control_tx,
            read_handle,
            write_handle,
        });

        let started_at = Instant::now();
        worker.stop_active_io(true).await;

        check!(started_at.elapsed() < Duration::from_millis(250))?;
        check_eq!(result_rx.await, Ok(Err(NetError::DeliveryUnknown)))?;
        let event = events.recv_timeout(Duration::from_secs(1))?;
        check_eq!(event.result(), Err(NetError::DeliveryUnknown))?;
        check_eq!(event.delivery(), WebSocketTaskDelivery::Unknown)?;
        check_eq!(event.phase(), WebSocketTaskPhase::Writing)?;
        check_eq!(event.cause(), WebSocketTaskEndCause::Shutdown)?;
        check!(
            matches!(event.source(), WebSocketTaskSource::Body(crate::WsBody::Binary(body)) if body == &vec![1; 1024])
        )?;
        check!(events.try_recv().is_err(), "forced abort notified twice")?;
        check!(inner.pending_requests().is_empty())?;
        Ok(())
    }

    #[tokio::test]
    async fn terminal_http_handshake_status_remains_queryable() -> TestResult {
        let (inner, mut worker) = WSClientInner::new(WebSocketClientConfig::default())
            .map_err(|error| test_error(format!("create worker: {error:?}")))?;
        worker.generation = 7;
        *worker
            .state
            .write()
            .map_err(|error| test_error(format!("connection state lock: {error:?}")))? =
            ConnectionStatus::Connecting;

        worker
            .handle_io_event(IoEvent::ConnectFailed {
                generation: 7,
                failure: WebSocketConnectionFailure::new(
                    NetError::ConnectError,
                    WebSocketConnectStage::WebSocketUpgrade,
                    Some(401),
                    false,
                ),
                reason: WebSocketTerminationReason::ConnectFailed,
            })
            .await;

        check_eq!(inner.last_connection_error(), Some(NetError::ConnectError))?;
        check_eq!(inner.last_handshake_http_status(), Some(401))?;
        check_eq!(inner.connection_status(), ConnectionStatus::Disconnected)?;
        Ok(())
    }
}

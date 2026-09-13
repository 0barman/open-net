use crate::api::listener::{
    WebSocketClientConnectStatusListener, WebSocketClientDataReceiveListener,
};
use crate::api::net_error::NetError;
use crate::api::traits::ws::connection_status::ConnectionStatus;
use crate::api::traits::ws::ws_body::WsBody;
use crate::api::traits::ws::ws_request_config::{DisconnectedTaskPolicy, WSRequestConfig};
use crate::api::traits::ws::ws_request_trait::WSRequestTrait;
use crate::api::web_socket_client::{WebSocketClientConfig, WebSocketConnectOptions};
use crate::api::wsc::duration_fits_instant;
use crate::api::wsc::pending_request_completion::PendingRequestCompletion;
use crate::api::wsc::queued_request_completion::QueuedRequestCompletion;
use crate::api::wsc::wsc_response::PendingRequestView;
use crate::api::wsc::{
    PreparedRequest, RequestScope, WebSocketConnectionEvents, WebSocketContextConnectOptions,
    WebSocketRequestOptions,
};
use crate::api::wsc::{WebSocketTaskEndCause, WebSocketTaskSource};
use crate::common::log::listener::LogListener;
use crate::common::log::log_def::LogType;
use crate::module::net_status::inner::inner_net_status_client::InnerNetStatusClient;
use crate::module::net_status::inner::network_status_snapshot::NetworkStatusSnapshot;
use crate::module::net_status::NetworkStatus;
use crate::module::transport::CompiledNetworkConfig;
use crate::module::ws_client::callback_executor::start_user_callback;
use crate::module::ws_client::client_command::ClientCommand;
use crate::module::ws_client::connection_session::ConnectionSession;
use crate::module::ws_client::listener_store::{ListenerStore, StatusRegistration};
use crate::module::ws_client::send_admission::{SendAdmission, SendLease};
use crate::module::ws_client::task_observer::{TaskObservation, TaskObserverStore};
use crate::module::ws_client::write::priority_write_queue::PriorityWriteQueue;
use crate::module::ws_client::write::queued_request::{DispatchCancellation, DispatchPhase};
use crate::module::ws_client::ws_client_worker::WSClientWorker;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tokio::sync::{mpsc, oneshot, watch, Notify, Semaphore};
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

static NEXT_UNTRACKED_REQUEST_ID: AtomicU64 = AtomicU64::new(1);
static NEXT_CLIENT_INSTANCE_ID: AtomicU64 = AtomicU64::new(1);

#[cfg(test)]
#[path = "ws_client_inner/context_tests.rs"]
mod context_tests;

#[cfg(test)]
#[path = "ws_client_inner/network_admission_tests.rs"]
mod network_admission_tests;

#[cfg(test)]
#[path = "ws_client_inner/network_recovery_tests.rs"]
mod network_recovery_tests;

#[path = "ws_client_inner/task_observation.rs"]
mod task_observation;

#[cfg(test)]
#[path = "ws_client_inner/task_observation_tests.rs"]
mod task_observation_tests;

#[cfg(test)]
#[path = "ws_client_inner/deadline_tests.rs"]
mod deadline_tests;
#[cfg(test)]
#[path = "ws_client_inner/registration_tests.rs"]
mod registration_tests;

#[path = "ws_client_inner/registered_send.rs"]
mod registered_send;

fn next_connection_identity(counter: &AtomicU64) -> Result<u64, NetError> {
    counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            value.checked_add(1)
        })
        .map_err(|_| NetError::InternalError)
}

/// 在发送 Future 被取消时撤销尚未完成的调度，并清理由本次发送预留的 pending。
struct DispatchCancellationGuard {
    cancel: CancellationToken,
    phase: DispatchPhase,
    pending: Option<(PendingRequestView, String, u64)>,
    queue: Option<Arc<PriorityWriteQueue>>,
    armed: bool,
}

impl DispatchCancellationGuard {
    fn new(
        cancel: CancellationToken,
        phase: DispatchPhase,
        pending: Option<(PendingRequestView, String, u64)>,
    ) -> Self {
        crate::log_t!(LogType::WSC; "new");
        Self {
            cancel,
            phase,
            pending,
            queue: None,
            armed: true,
        }
    }

    fn mark_enqueued(&mut self, queue: Arc<PriorityWriteQueue>) {
        crate::log_t!(LogType::WSC; "mark_enqueued");
        self.queue = Some(queue);
    }

    fn disarm(&mut self) {
        crate::log_t!(LogType::WSC; "disarm");
        self.armed = false;
        self.pending.take();
    }
}

impl Drop for DispatchCancellationGuard {
    fn drop(&mut self) {
        crate::log_t!(LogType::WSC; "drop");
        if !self.armed {
            return;
        }
        match self.phase.cancel() {
            DispatchCancellation::Queued => {
                if let Some(observation) = self.phase.observation() {
                    observation.set_cause(WebSocketTaskEndCause::SendCancelled);
                }
                crate::log_s!(LogType::WSC; "drop", "stage", "cancel_queued_dispatch");
                self.cancel.cancel();
                if let Some(queue) = self.queue.take() {
                    queue.cancel_queued(&self.cancel);
                }
                if let Some((pending, uuid, token)) = self.pending.take() {
                    pending.remove_if_token(uuid.as_str(), token, NetError::Cancelled);
                }
                if let Some(observation) = self.phase.observation() {
                    observation.finish(Err(NetError::Cancelled));
                }
            }
            DispatchCancellation::Writing => {
                if let Some(observation) = self.phase.observation() {
                    observation.set_cause(WebSocketTaskEndCause::SendCancelled);
                }
                crate::log_s!(LogType::WSC; "drop", "stage", "cancel_in_flight_dispatch");
                // Writer owns pending cleanup once it has begun. Cancelling the token
                // makes its next safe boundary (or in-flight poll) choose the correct
                // Cancelled/DeliveryUnknown result without racing an eager removal.
                self.cancel.cancel();
            }
            DispatchCancellation::AlreadyResolved => {}
        }
    }
}

/// 同步非阻塞请求完成本地入队后，由调用入口决定保留或丢弃的观察端。
struct TrySendAdmission {
    write_result: oneshot::Receiver<Result<(), NetError>>,
    pending_completion: Option<PendingRequestCompletion>,
}

/// WebSocket 客户端句柄共享的内部状态与命令入口。
///
/// 该类型不直接执行网络 I/O；连接、断开和关闭操作会通过命令通道交给唯一的
/// `WSClientWorker` 串行处理，业务消息则进入共享的优先级写队列。克隆外层客户端时，
/// 各句柄会共享这里的连接状态、监听器和待响应请求注册表。
pub(crate) struct WSClientInner {
    /// Private monitor ownership is isolated from public named status clients.
    net_status_client: Option<Arc<InnerNetStatusClient>>,
    network_status: Option<watch::Receiver<NetworkStatusSnapshot>>,
    instance_id: u64,
    next_session_id: AtomicU64,
    next_task_id: AtomicU64,
    task_observers: Arc<TaskObserverStore>,
    /// 发往客户端 worker 的有界命令通道发送端。
    ///
    /// 需要确认结果的命令会在自身携带的一次性通道上返回处理结果。
    command_tx: mpsc::Sender<ClientCommand>,
    /// 跨连接代次复用的有界业务写队列。
    queue: Arc<PriorityWriteQueue>,
    /// 为应用 ACK 等单向完整消息保留的有界紧急写队列。
    urgent_queue: Arc<PriorityWriteQueue>,
    /// worker 发布的当前连接状态。
    ///
    /// 读写锁中毒时，状态查询会保守地按 `ConnectionStatus::Closed` 处理。
    state: Arc<RwLock<ConnectionStatus>>,
    /// 最近一次连接建立、读取或写入失败的原因；主动新建连接或断开时清空。
    last_connection_error: Arc<RwLock<Option<NetError>>>,
    /// 最近一次终态握手 HTTP 响应码；非 HTTP 失败和成功连接会清空。
    last_handshake_http_status: Arc<RwLock<Option<u16>>>,
    /// 数据与连接状态监听器的并发存储。
    listeners: Arc<ListenerStore>,
    /// 与 worker 共享的待响应请求注册表视图。
    pending_requests: PendingRequestView,
    /// 将状态准入与断线 drain 连接起来的连接/会话取消代次。
    send_admission: Arc<SendAdmission>,
    /// 客户端级关闭令牌。
    ///
    /// 该令牌既唤醒 worker 的关闭分支，也会取消仍在等待写队列容量的发送调用。
    shutdown: CancellationToken,
    /// Worker 完成全部终态清理后取消；所有并发 shutdown 调用共享该完成信号。
    shutdown_complete: CancellationToken,
    /// 用于提前结束连接重试退避等待的网络可用通知器。
    network_available: Arc<Notify>,
}

impl WSClientInner {
    #[cfg(test)]
    pub(crate) fn network_status_policy_for_test(&self) -> crate::NetworkStatusPolicy {
        if self.network_status.is_some() {
            crate::NetworkStatusPolicy::PauseOnUnavailable
        } else {
            crate::NetworkStatusPolicy::Ignore
        }
    }

    /// 校验客户端配置并创建一对共享句柄与专属 worker。
    ///
    /// 命令和 I/O 事件通道的容量均为 64；数据回调通道、pending 表及业务队列容量由
    /// `config` 提供，状态回调使用独立通道。返回的 worker 尚未运行。
    ///
    /// # 错误
    ///
    /// 零容量/零超时、不合法的 frame 大小、Pong 超时不大于心跳间隔、写缓冲上限
    /// 不大于目标缓冲、零消息限制、非法 keepalive 参数，或超过 Tokio 有界通道/
    /// 信号量支持的容量都会返回 `NetError::ConfigError`，不会把构造 panic 暴露给调用方。
    #[cfg(test)]
    pub(crate) fn new(
        config: WebSocketClientConfig,
    ) -> Result<(Arc<Self>, WSClientWorker), NetError> {
        Self::new_with_network(
            config,
            Arc::new(CompiledNetworkConfig::new(
                crate::api::network_config::NetworkConfig::default(),
            )?),
            None,
        )
    }

    pub(crate) fn new_with_network(
        config: WebSocketClientConfig,
        network: Arc<CompiledNetworkConfig>,
        net_status_client: Option<Arc<InnerNetStatusClient>>,
    ) -> Result<(Arc<Self>, WSClientWorker), NetError> {
        crate::log_t!(LogType::WSC; "new", "config", format!("{:?}", config));
        let result: Result<(Arc<Self>, WSClientWorker), NetError> = (|| {
            let max_outgoing_frame_payload = config.data_frame_payload_size.unwrap_or_else(|| {
                config
                    .business_queue_max_bytes
                    .max(config.urgent_queue_max_bytes)
            });
            let write_buffer_holds_one_frame = max_outgoing_frame_payload
                .checked_add(WebSocketClientConfig::MAX_CLIENT_FRAME_OVERHEAD)
                .is_some_and(|required| config.max_write_buffer_size >= required);
            if config.callback_queue_capacity == 0
                || config.callback_queue_capacity > Semaphore::MAX_PERMITS
                || config.callback_queue_max_bytes == 0
                || config.callback_queue_max_bytes > u32::MAX as usize
                || config.callback_queue_max_bytes > Semaphore::MAX_PERMITS
                || config.data_callback_concurrency == 0
                || config.data_callback_concurrency > Semaphore::MAX_PERMITS
                || config.status_callback_queue_capacity == 0
                || config.status_callback_queue_capacity > Semaphore::MAX_PERMITS
                || config.pending_request_capacity == 0
                || config.urgent_queue_capacity == 0
                || config.urgent_queue_max_bytes == 0
                || config.heartbeat_interval.is_zero()
                || config.pong_timeout <= config.heartbeat_interval
                || config.close_timeout.is_zero()
                || config.control_write_timeout.is_zero()
                || config.data_frame_write_timeout.is_zero()
                || !duration_fits_instant(config.heartbeat_interval)
                || !duration_fits_instant(config.pong_timeout)
                || !duration_fits_instant(config.close_timeout)
                || !duration_fits_instant(config.control_write_timeout)
                || !duration_fits_instant(config.data_frame_write_timeout)
                || !duration_fits_instant(config.response_dispatch_grace)
                || config.read_buffer_size == 0
                || config
                    .data_frame_payload_size
                    .is_some_and(|size| size < WebSocketClientConfig::MIN_DATA_FRAME_PAYLOAD_SIZE)
                || config.max_write_buffer_size <= config.write_buffer_size
                || !write_buffer_holds_one_frame
                || config.max_message_size == Some(0)
                || config.max_frame_size == Some(0)
                || config.tcp_send_buffer_size == Some(0)
                || config.tcp_keepalive.as_ref().is_some_and(|keepalive| {
                    keepalive.idle.is_zero() || keepalive.interval.is_zero()
                })
            {
                return Err(NetError::ConfigError);
            }
            let queue = PriorityWriteQueue::new(
                config.business_queue_capacity,
                config.business_queue_max_bytes,
            )?;
            let urgent_queue = PriorityWriteQueue::new(
                config.urgent_queue_capacity,
                config.urgent_queue_max_bytes,
            )?;
            let (command_tx, command_rx) = mpsc::channel(64);
            let (io_event_tx, io_event_rx) = mpsc::channel(64);
            let (data_callback_tx, data_callback_rx) =
                mpsc::channel(config.callback_queue_capacity);
            let data_callback_bytes = Arc::new(Semaphore::new(config.callback_queue_max_bytes));
            // 状态与数据使用独立的有界 lane；普通状态更新满载时可丢弃中间事件，终态
            // 则在 close_timeout 内等待容量及监听器完成，避免无限积压。
            let (status_callback_tx, status_callback_rx) =
                mpsc::channel(config.status_callback_queue_capacity);
            let state = Arc::new(RwLock::new(ConnectionStatus::Idle));
            let last_connection_error = Arc::new(RwLock::new(None));
            let last_handshake_http_status = Arc::new(RwLock::new(None));
            let listeners = Arc::new(ListenerStore::default());
            let pending_requests =
                PendingRequestView::with_capacity(config.pending_request_capacity);
            let send_admission = Arc::new(SendAdmission::default());
            let shutdown = CancellationToken::new();
            let shutdown_complete = CancellationToken::new();
            let network_available = Arc::new(Notify::new());
            let instance_id = next_connection_identity(&NEXT_CLIENT_INSTANCE_ID)?;
            let task_observers = Arc::new(TaskObserverStore::new(instance_id));
            let network_status = net_status_client.as_ref().map(|client| client.subscribe());

            let inner = Arc::new(Self {
                net_status_client: net_status_client.clone(),
                network_status: network_status.clone(),
                instance_id,
                next_session_id: AtomicU64::new(1),
                next_task_id: AtomicU64::new(1),
                task_observers: Arc::clone(&task_observers),
                command_tx,
                queue: Arc::clone(&queue),
                urgent_queue: Arc::clone(&urgent_queue),
                state: Arc::clone(&state),
                last_connection_error: Arc::clone(&last_connection_error),
                last_handshake_http_status: Arc::clone(&last_handshake_http_status),
                listeners: Arc::clone(&listeners),
                pending_requests: pending_requests.clone(),
                send_admission: Arc::clone(&send_admission),
                shutdown: shutdown.clone(),
                shutdown_complete: shutdown_complete.clone(),
                network_available: Arc::clone(&network_available),
            });
            let worker = WSClientWorker {
                net_status_client,
                network_status,
                network_loss_epoch: 0,
                task_observers,
                network,
                context_provider_slots: Arc::new(Semaphore::new(1)),
                config,
                command_rx,
                io_event_tx,
                io_event_rx,
                data_callback_tx,
                data_callback_rx: Some(data_callback_rx),
                data_callback_bytes,
                status_callback_tx,
                status_callback_rx: Some(status_callback_rx),
                queue,
                urgent_queue,
                state,
                last_connection_error,
                last_handshake_http_status,
                listeners,
                pending_requests,
                send_admission,
                shutdown,
                shutdown_complete,
                network_available,
                generation: 0,
                connect_target: None,
                connect_reply: None,
                connect_cancel: None,
                connect_handle: None,
                active_io: None,
            };
            crate::log_s!(LogType::WSC; "new", "status", "Idle");
            Ok((inner, worker))
        })();
        result.inspect_err(|error| {
            crate::log_e!(LogType::WSC; "new", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    /// 注册数据接收监听器，并替换此前注册的监听器。
    ///
    /// 数据回调由专用回调任务按事件顺序出队，并按配置串行或有界并发执行。回调任务会
    /// 先克隆监听器再释放读锁，
    /// 因此本方法不会等待正在执行的旧监听器；与替换操作并发时，已经取得旧监听器的
    /// 事件仍可能调用旧监听器。永久关闭请求发布后，本次注册会在同一写锁临界区内
    /// 被忽略，防止监听器捕获 client 后重新形成引用环。若监听器锁已中毒也会忽略。
    pub(crate) fn register_data_listener(&self, listener: WebSocketClientDataReceiveListener) {
        crate::log_t!(LogType::WSC; "register_data_listener", "listener", "callback");
        if let Ok(mut current) = self.listeners.data.write() {
            if self.shutdown.is_cancelled() {
                current.take();
            } else {
                *current = Some(Arc::from(listener));
            }
        } else {
            crate::log_e!(LogType::WSC; "register_data_listener", "error", "data_listener_lock_poisoned");
        }
    }

    /// 注销当前数据接收监听器。
    ///
    /// 注销只影响后续从存储中取得监听器的事件；已克隆监听器或已经开始的回调仍可完成。
    /// 若监听器锁已中毒，本次操作会被忽略。
    pub(crate) fn unregister_data_listener(&self) {
        crate::log_t!(LogType::WSC; "unregister_data_listener");
        if let Ok(mut current) = self.listeners.data.write() {
            current.take();
        } else {
            crate::log_e!(LogType::WSC; "unregister_data_listener", "error", "data_listener_lock_poisoned");
        }
    }

    /// 注册连接状态监听器、替换旧监听器，并在独立 OS 线程通知一次当前状态快照。
    ///
    /// 本方法不等待首次回调返回。同一登记的后续状态回调在首次完成后读取最新状态，
    /// 状态调度任务可异步等待，常规网络操作不等待首次通知；关闭仍有有限等待预算。
    /// 回调不持有内部锁；在 `panic=unwind` 构建中隔离可展开 panic。
    /// 替换和注销唤醒等待旧登记的调度任务，
    /// 已提交的首次通知仍可完成。永久关闭请求发布后不保存监听器，只异步通知 `Closed`。
    /// 线程创建失败记录日志并解除首次完成屏障，不在注册调用线程执行回调。
    pub(crate) fn register_status_listener(&self, listener: WebSocketClientConnectStatusListener) {
        crate::log_t!(LogType::WSC; "register_status_listener", "listener", "callback");
        let registration = Arc::new(StatusRegistration::new(Arc::from(listener)));
        // Capture the guard before exposing the registration or attempting spawn.
        let initial_done = registration.initial_done.clone().drop_guard();
        let (terminal, previous) = if let Ok(mut current) = self.listeners.status.write() {
            if self.shutdown.is_cancelled() {
                (true, current.take())
            } else {
                (false, current.replace(Arc::clone(&registration)))
            }
        } else {
            crate::log_e!(LogType::WSC; "register_status_listener", "error", "status_listener_lock_poisoned");
            (self.shutdown.is_cancelled(), None)
        };
        // Dropping the last callback capture may reenter this API; release the lock first.
        if let Some(previous) = previous {
            previous.retired.cancel();
            drop(previous);
        }
        let status = if terminal {
            ConnectionStatus::Closed
        } else {
            self.connection_status()
        };
        drop(start_user_callback(
            "open-net-ws-status-callback",
            move || {
                let _initial_done = initial_done;
                (registration.listener)(status);
            },
        ));
    }

    /// 注销当前连接状态监听器。
    ///
    /// 注销会解除调度任务对本登记首次回调的等待；已提交的回调仍可完成。
    /// 若监听器锁已中毒，本次操作会被忽略。
    pub(crate) fn unregister_status_listener(&self) {
        crate::log_t!(LogType::WSC; "unregister_status_listener");
        let previous = if let Ok(mut current) = self.listeners.status.write() {
            current.take()
        } else {
            crate::log_e!(LogType::WSC; "unregister_status_listener", "error", "status_listener_lock_poisoned");
            None
        };
        if let Some(previous) = previous {
            previous.retired.cancel();
            drop(previous);
        }
    }

    /// 返回调用时观察到的连接状态快照。
    ///
    /// 状态可在返回后立即被 worker 更新；若状态锁已中毒，则返回终止态
    /// `ConnectionStatus::Closed`。
    pub(crate) fn connection_status(&self) -> ConnectionStatus {
        crate::log_t!(LogType::WSC; "connection_status");
        self.state.read().map(|state| *state).unwrap_or_else(|_| {
            crate::log_e!(LogType::WSC; "connection_status", "error", "state_lock_poisoned");
            ConnectionStatus::Closed
        })
    }

    /// 返回最近一次导致连接失败或终止的错误快照。
    ///
    /// 主动开始新的初始连接或主动断开时会清空该值；自动重连成功后仍保留最近一次
    /// 故障，避免状态事件合并或快速重连使恢复原因不可观察。
    pub(crate) fn last_connection_error(&self) -> Option<NetError> {
        crate::log_t!(LogType::WSC; "last_connection_error");
        self.last_connection_error
            .read()
            .map(|error| *error)
            .unwrap_or_else(|_| {
                crate::log_e!(LogType::WSC; "last_connection_error", "error", "snapshot_lock_poisoned");
                None
            })
    }

    /// 返回最近一次终态 WebSocket 握手失败的 HTTP 状态码。
    pub(crate) fn last_handshake_http_status(&self) -> Option<u16> {
        crate::log_t!(LogType::WSC; "last_handshake_http_status");
        self.last_handshake_http_status
            .read()
            .map(|status| *status)
            .unwrap_or_else(|_| {
                crate::log_e!(LogType::WSC; "last_handshake_http_status", "error", "snapshot_lock_poisoned");
                None
            })
    }

    /// 返回待响应请求注册表的共享访问视图。
    ///
    /// 返回值与客户端共享底层注册表，并非调用时刻的静态快照；之后的排队、写入、
    /// 响应认领、超时和连接清理都会反映在该视图中。该视图虽不暴露内部写入
    /// 接口，但调用方可通过其 `take_request` 操作原子移除并取回请求。
    pub(crate) fn pending_requests(&self) -> PendingRequestView {
        crate::log_t!(LogType::WSC; "pending_requests");
        self.pending_requests.clone()
    }

    /// 请求 worker 连接到指定 URL，并等待本轮初始连接流程结束。
    ///
    /// 命令进入有界通道时可能异步等待容量。成功返回表示 WebSocket 握手成功且本代读写
    /// 任务已经启动；若连接选项允许重试，则等待范围包含该初始流程中的所有重试。
    ///
    /// # 错误
    ///
    /// worker 会返回状态冲突、连接选项无效、握手或重试的最终错误。若命令通道关闭，
    /// 或 worker 未能通过一次性通道回复，则返回 `NetError::EngineDropped`。并发的断开或
    /// 关闭会取消尚未完成的连接流程。
    pub(crate) async fn connect(
        &self,
        url: String,
        options: WebSocketConnectOptions,
    ) -> Result<(), NetError> {
        crate::log_t!(LogType::WSC; "connect", "url|header_count|has_header_provider|reconnect", crate::common::log::summary::url(&url), options.headers.len(), options.header_provider.is_some(), format!("{:?}", options.reconnect));
        let result: Result<(), NetError> = async {
            if !options.reconnect.is_valid() {
                return Err(NetError::ConfigError);
            }
            let (reply_tx, reply_rx) = oneshot::channel();
            self.command_tx
                .send(ClientCommand::Connect {
                    url,
                    options,
                    reply: reply_tx,
                })
                .await
                .map_err(|_| NetError::EngineDropped)?;
            reply_rx.await.map_err(|_| NetError::EngineDropped)?
        }
        .await;
        result.inspect_err(|error| {
            crate::log_e!(LogType::WSC; "connect", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    pub(crate) async fn start_connect_with_context(
        &self,
        options: WebSocketContextConnectOptions,
    ) -> Result<WebSocketConnectionEvents, NetError> {
        options.validate()?;
        let session_id = next_connection_identity(&self.next_session_id)?;
        let (session, events) = ConnectionSession::new_with_scope(
            self.instance_id,
            session_id,
            options.session_context_id,
            options.event_capacity,
            options.request_scope.as_ref(),
        )?;
        let (reply, received) = oneshot::channel();
        // `events` remains owned by this Future until returned. Its Drop revokes the session
        // even when the command has already reached the worker or is waiting for admission.
        self.command_tx
            .send(ClientCommand::ConnectWithContext {
                options,
                session,
                reply,
            })
            .await
            .map_err(|_| NetError::EngineDropped)?;
        received.await.map_err(|_| NetError::EngineDropped)??;
        Ok(events)
    }

    /// 主动断开当前连接，并等待 worker 完成本次断开流程。
    ///
    /// worker 会取消连接尝试，并以一个 `close_timeout` deadline 覆盖 Close 控制消息
    /// 入队、写侧确认及读写任务协作退出；超时后强制中止。随后清理业务队列和 pending、
    /// 移除自动重连目标并回到 Idle。状态回调使用独立通道，不受数据回调积压影响。
    ///
    /// # 错误
    ///
    /// 若命令通道关闭或 worker 未能回复，则返回 `NetError::EngineDropped`。
    pub(crate) async fn disconnect(&self) -> Result<(), NetError> {
        crate::log_t!(LogType::WSC; "disconnect");
        let result: Result<(), NetError> = async {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.command_tx
                .send(ClientCommand::Disconnect { reply: reply_tx })
                .await
                .map_err(|_| NetError::EngineDropped)?;
            reply_rx.await.map_err(|_| NetError::EngineDropped)?
        }
        .await;
        result.inspect_err(|error| {
            crate::log_e!(LogType::WSC; "disconnect", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    /// 以尽力而为的方式通知客户端网络已经恢复可用。
    ///
    /// 通知会唤醒当前正在退避的连接任务；同时尝试向 worker 投递命令，使处于
    /// `Disconnected` 且启用自动重连的客户端重新开始连接。该方法不等待处理结果，
    /// 通知也不是持久的网络状态：命令通道已满或关闭时命令会被丢弃，且没有正在等待的
    /// 连接任务时唤醒信号不会被保留。
    /// legacy 的不可重试终态已撤销目标；context 的任何会话终态也会撤销目标。
    /// 这两种情况都需要调用方通过显式连接入口创建新周期或新会话。
    pub(crate) fn notify_network_available(&self) {
        crate::log_t!(LogType::WSC; "notify_network_available");
        self.network_available.notify_waiters();
        let notified = self
            .command_tx
            .try_send(ClientCommand::NetworkAvailable)
            .is_ok();
        crate::log_s!(LogType::WSC; "notify_network_available", "command_queued", notified);
    }

    /// 发出不等待完成的客户端关闭请求。
    ///
    /// 取消令牌会保持已取消状态，不会像 `try_send` 命令一样因通道已满而丢失；附加的
    /// 关闭命令仅作尽力投递，通道已满或已关闭时会被忽略。worker 仍需获得执行机会并
    /// 观察该令牌，之后才会执行关闭清理。本方法不等待关闭帧、最终状态回调或资源释放
    /// 完成；重复调用是安全的。
    pub(crate) fn request_shutdown(&self) {
        crate::log_t!(LogType::WSC; "request_shutdown");
        // Publish the terminal admission boundary synchronously. A sender that is
        // still waiting for queue capacity must not slip into the old session while
        // the worker is waiting for its next scheduling turn.
        self.task_observers.close();
        self.send_admission
            .end_session_with_cause(WebSocketTaskEndCause::Shutdown);
        self.shutdown.cancel();
        if let Some(client) = self.net_status_client.as_ref() {
            client.request_destroy();
        }
        crate::log_s!(LogType::WSC; "request_shutdown", "stage", "shutdown_requested");
        let _ = self
            .command_tx
            .try_send(ClientCommand::Shutdown { reply: None });
    }

    /// 将一条业务请求排入优先级写队列，并等待本次网络写入得出结果。
    ///
    /// 本方法先依据状态和断线策略进行准入检查，再生成消息、以 UUID 预留待响应记录并
    /// 等待队列的任务数及字节数容量。相同 UUID 在其旧记录仍存在时不能再次提交；每次
    /// 预留生成的 token 会保护失败或超时清理，避免旧异步任务误删后来复用 UUID 的请求。
    ///
    /// 返回 `Ok(())` 表示消息已由 WebSocket sink 确认写入，或业务监听器已在 sink flush
    /// 尚未得出结论时通过 UUID 认领了服务端响应；它不表示调用方已经等待了业务响应。
    /// 正常写入后，记录会保留到数据监听器认领、响应超时或连接清理为止。若响应认领先于
    /// 一个不确定的 flush 错误，响应对该请求的成功结论优先，但物理连接仍会被废弃。
    ///
    /// pending 预留到入队之间由析构 guard 保护；发送 Future 在容量等待中被丢弃会立即
    /// 释放 pending，入队后被丢弃则撤销尚未开始的队列项。若取消撞上已经开始的 sink
    /// 写入，当前连接会按 `DeliveryUnknown` 废弃。队列的关闭检查与入堆在同一锁内
    /// 串行化，关闭后不会产生无人消费的迟到队列项。
    ///
    /// # 错误
    ///
    /// 非幂等请求配置发送重试，或请求时长无法表示为平台 deadline 时返回
    /// `NetError::ConfigError`；当前状态不允许准入时返回对应的未连接、正在关闭或引擎已
    /// 释放错误。请求构造、UUID 校验或去重、队列容量与超时、取消、写入和重试产生的
    /// 错误也会原样传播。
    pub(crate) async fn send(
        &self,
        request: Arc<dyn WSRequestTrait>,
        config: WSRequestConfig,
    ) -> Result<(), NetError> {
        crate::log_t!(LogType::WSC; "send", "config|request_type", format!("{:?}", config), "dyn WSRequestTrait");
        let result: Result<(), NetError> =
            async { self.send_internal(request, config).await.map(|_| ()) }.await;
        result.inspect_err(|error| {
            crate::log_e!(LogType::WSC; "send", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    /// 发送请求并返回其响应关联的唯一终态通知。
    pub(crate) async fn send_with_completion(
        &self,
        request: Arc<dyn WSRequestTrait>,
        config: WSRequestConfig,
    ) -> Result<PendingRequestCompletion, NetError> {
        crate::log_t!(LogType::WSC; "send_with_completion", "config|request_type", format!("{:?}", config), "dyn WSRequestTrait");
        let result: Result<PendingRequestCompletion, NetError> = async {
            if !config.expect_response {
                return Err(NetError::ConfigError);
            }
            self.send_internal(request, config)
                .await?
                .ok_or(NetError::InternalError)
        }
        .await;
        result.inspect_err(|error| {
            crate::log_e!(LogType::WSC; "send_with_completion", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    async fn send_internal(
        &self,
        request: Arc<dyn WSRequestTrait>,
        mut config: WSRequestConfig,
    ) -> Result<Option<PendingRequestCompletion>, NetError> {
        crate::log_t!(LogType::WSC; "send_internal", "config|request_type", format!("{:?}", config), "dyn WSRequestTrait");
        let result: Result<Option<PendingRequestCompletion>, NetError> = async {
        let admission_cancel = self.validate_send_admission(&config)?;

        let uuid = request.uuid();
        let body = request.body().inspect_err(|error| {
            crate::log_e!(LogType::WSC; "send_internal", "stage|error", "build_body", format!("{error:?}"));
        })?;
        let size = body.len();
        if uuid.trim().is_empty() { return Err(NetError::ParameterEmpty); }
        let observation = self.observe_task(
            || WebSocketTaskSource::Request(Arc::clone(&request)), Some(uuid.clone()),
            size, false, &admission_cancel, &mut config,
        ).await?;
        crate::log_s!(LogType::WSC; "send_internal", "stage|uuid|body_bytes", "body_ready", uuid, size);
        let message = match body {
            WsBody::Text(value) => Message::Text(value.into()),
            WsBody::Binary(value) => Message::Binary(value),
        };
        let (pending_token, completion) = if config.expect_response {
            let reserved = self.pending_requests.reserve_uuid_observed(uuid.clone(), request, &config, observation.clone(), Some(&admission_cancel.cancel));
            let (token, completion) = match reserved {
                Ok(value) => value,
                Err(error) => { return Err(Self::finish_observed_error(&observation, error)); }
            };
            (Some(token), Some(completion))
        } else {
            if uuid.trim().is_empty() {
                return Err(NetError::ParameterEmpty);
            }
            (None, None)
        };
        self.enqueue_and_wait(
            Arc::clone(&self.queue),
            uuid,
            pending_token,
            message,
            size,
            config,
            admission_cancel.cancel,
            observation,
        )
        .await?;
        Ok(completion)
    }.await;
        result.inspect_err(|error| {
            crate::log_e!(LogType::WSC; "send_internal", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    /// 同步、非阻塞地尝试把 tracked/untracked 请求加入业务写队列。
    ///
    /// 成功只表示本次调用完成准入、pending 预留和队列容量申请；写入及响应终态继续由
    /// worker 异步处理。该路径不依赖调用线程存在 Tokio runtime，且返回后不会因缺少
    /// 一个等待发送结果的 Future 而撤销队列项。
    pub(crate) fn try_send(
        &self,
        request: Arc<dyn WSRequestTrait>,
        config: WSRequestConfig,
    ) -> Result<(), NetError> {
        crate::log_t!(LogType::WSC; "try_send", "config|request_type", format!("{:?}", config), "dyn WSRequestTrait");
        let result: Result<(), NetError> = (|| {
            let TrySendAdmission {
                write_result,
                pending_completion,
            } = self.try_send_internal(request, config)?;
            drop(write_result);
            drop(pending_completion);
            Ok(())
        })();
        result.inspect_err(|error| {
            crate::log_e!(LogType::WSC; "try_send", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    /// 同步入队并返回可移交给异步执行器的写入/响应两阶段回执。
    pub(crate) fn try_send_with_completion(
        &self,
        request: Arc<dyn WSRequestTrait>,
        config: WSRequestConfig,
    ) -> Result<QueuedRequestCompletion, NetError> {
        crate::log_t!(LogType::WSC; "try_send_with_completion", "config|request_type", format!("{:?}", config), "dyn WSRequestTrait");
        let result: Result<QueuedRequestCompletion, NetError> = (|| {
            if !config.expect_response {
                return Err(NetError::ConfigError);
            }
            let TrySendAdmission {
                write_result,
                pending_completion,
            } = self.try_send_internal(request, config)?;
            let pending_completion = pending_completion.ok_or(NetError::InternalError)?;
            Ok(QueuedRequestCompletion::new(
                write_result,
                pending_completion,
            ))
        })();
        result.inspect_err(|error| {
            crate::log_e!(LogType::WSC; "try_send_with_completion", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    fn try_send_internal(
        &self,
        request: Arc<dyn WSRequestTrait>,
        config: WSRequestConfig,
    ) -> Result<TrySendAdmission, NetError> {
        crate::log_t!(LogType::WSC; "try_send_internal", "config|request_type", format!("{:?}", config), "dyn WSRequestTrait");
        let result: Result<TrySendAdmission, NetError> = (|| {
            let admission_cancel = self.validate_send_admission(&config)?;
            let uuid = request.uuid();
            let body = request.body().inspect_err(|error| {
            crate::log_e!(LogType::WSC; "try_send_internal", "stage|error", "build_body", format!("{error:?}"));
        })?;
            let size = body.len();
            if uuid.trim().is_empty() {
                return Err(NetError::ParameterEmpty);
            }
            let observation = self.try_observe_task(
                || WebSocketTaskSource::Request(Arc::clone(&request)),
                Some(uuid.clone()),
                size,
                false,
                &admission_cancel,
            )?;
            crate::log_s!(LogType::WSC; "try_send_internal", "stage|uuid|body_bytes", "body_ready", uuid, size);
            let message = match body {
                WsBody::Text(value) => Message::Text(value.into()),
                WsBody::Binary(value) => Message::Binary(value),
            };
            let (pending_token, completion) = if config.expect_response {
                let reserved = self.pending_requests.reserve_uuid_observed(
                    uuid.clone(),
                    request,
                    &config,
                    observation.clone(),
                    Some(&admission_cancel.cancel),
                );
                let (token, completion) = match reserved {
                    Ok(value) => value,
                    Err(error) => {
                        return Err(Self::finish_observed_error(&observation, error));
                    }
                };
                (Some(token), Some(completion))
            } else {
                if uuid.trim().is_empty() {
                    return Err(NetError::ParameterEmpty);
                }
                (None, None)
            };
            let write_result = self.try_enqueue(
                Arc::clone(&self.queue),
                uuid,
                pending_token,
                message,
                size,
                config,
                admission_cancel.cancel,
                observation,
            )?;
            Ok(TrySendAdmission {
                write_result,
                pending_completion: completion,
            })
        })();
        result.inspect_err(|error| {
            crate::log_e!(LogType::WSC; "try_send_internal", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    /// 发送一条不期待业务响应的完整 Text/Binary 消息。
    pub(crate) async fn send_untracked(
        &self,
        body: WsBody,
        config: WSRequestConfig,
        urgent: bool,
    ) -> Result<(), NetError> {
        self.send_untracked_scoped(body, config, urgent, None).await
    }

    pub(crate) async fn send_untracked_scoped(
        &self,
        body: WsBody,
        mut config: WSRequestConfig,
        urgent: bool,
        scope: Option<RequestScope>,
    ) -> Result<(), NetError> {
        crate::log_t!(LogType::WSC; "send_untracked", "config|body_bytes|urgent", format!("{:?}", config), match &body { WsBody::Text(v) => v.len(), WsBody::Binary(v) => v.len() }, urgent);
        let result: Result<(), NetError> = async {
            config.expect_response = false;
            let admission_cancel = self.validate_send_admission_scoped(&config, scope.as_ref())?;
            let sequence = NEXT_UNTRACKED_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
            let size = body.len();
            let observation = self
                .observe_task(
                    || WebSocketTaskSource::Body(body.clone()),
                    None,
                    size,
                    urgent,
                    &admission_cancel,
                    &mut config,
                )
                .await?;
            let message = match body {
                WsBody::Text(value) => Message::Text(value.into()),
                WsBody::Binary(value) => Message::Binary(value),
            };
            self.enqueue_and_wait_scoped(
                if urgent {
                    Arc::clone(&self.urgent_queue)
                } else {
                    Arc::clone(&self.queue)
                },
                format!("__open_net_untracked:{sequence}"),
                None,
                message,
                size,
                config,
                admission_cancel.cancel,
                observation,
                scope,
            )
            .await
        }
        .await;
        result.inspect_err(|error| {
            crate::log_e!(LogType::WSC; "send_untracked", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    /// 同步、非阻塞地尝试加入一条不期待响应的完整消息。
    pub(crate) fn try_send_untracked(
        &self,
        body: WsBody,
        config: WSRequestConfig,
        urgent: bool,
    ) -> Result<(), NetError> {
        self.try_send_untracked_scoped(body, config, urgent, None)
    }

    pub(crate) fn try_send_untracked_scoped(
        &self,
        body: WsBody,
        mut config: WSRequestConfig,
        urgent: bool,
        scope: Option<RequestScope>,
    ) -> Result<(), NetError> {
        crate::log_t!(LogType::WSC; "try_send_untracked", "config|body_bytes|urgent", format!("{:?}", config), match &body { WsBody::Text(v) => v.len(), WsBody::Binary(v) => v.len() }, urgent);
        let result: Result<(), NetError> = (|| {
            config.expect_response = false;
            let admission_cancel = self.validate_send_admission_scoped(&config, scope.as_ref())?;
            let sequence = NEXT_UNTRACKED_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
            let size = body.len();
            let observation = self.try_observe_task(
                || WebSocketTaskSource::Body(body.clone()),
                None,
                size,
                urgent,
                &admission_cancel,
            )?;
            let message = match body {
                WsBody::Text(value) => Message::Text(value.into()),
                WsBody::Binary(value) => Message::Binary(value),
            };
            let write_result = self.try_enqueue_scoped(
                if urgent {
                    Arc::clone(&self.urgent_queue)
                } else {
                    Arc::clone(&self.queue)
                },
                format!("__open_net_untracked:{sequence}"),
                None,
                message,
                size,
                config,
                admission_cancel.cancel,
                observation,
                scope,
            )?;
            drop(write_result);
            Ok(())
        })();
        result.inspect_err(|error| {
            crate::log_e!(LogType::WSC; "try_send_untracked", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    /// 校验通用于 tracked/untracked 两条入口的重试配置和当前状态准入。
    fn validate_send_admission(&self, config: &WSRequestConfig) -> Result<SendLease, NetError> {
        self.validate_send_admission_scoped(config, None)
    }

    fn validate_send_admission_scoped(
        &self,
        config: &WSRequestConfig,
        scope: Option<&RequestScope>,
    ) -> Result<SendLease, NetError> {
        crate::log_t!(LogType::WSC; "validate_send_admission", "config", format!("{:?}", config));
        let result: Result<SendLease, NetError> = (|| {
            if (config.send_retry_count > 0 && !config.idempotent)
                || config
                    .enqueue_timeout
                    .is_some_and(|timeout| !duration_fits_instant(timeout))
                || !duration_fits_instant(config.write_timeout)
                || !duration_fits_instant(config.response_timeout)
            {
                return Err(NetError::ConfigError);
            }
            // Capture the unique cancellation epoch before reading status. If lifecycle
            // transitions all the way through a reconnect between these operations, this
            // old lease is cancelled; taking the lease afterwards would admit an ABA-crossing
            // request into the new connection/session.
            let lease = self
                .send_admission
                .lease_observed(config.disconnected_policy);
            lease.validate_scope(scope)?;
            let status = self.connection_status();
            let can_wait = matches!(
                status,
                ConnectionStatus::Connecting | ConnectionStatus::Reconnecting
            ) && config.disconnected_policy
                == DisconnectedTaskPolicy::WaitForReconnect;
            if status == ConnectionStatus::Connected || can_wait {
                if lease.cancel.is_cancelled() {
                    return Err(NetError::ConnectionClosed);
                }
                // Public status is committed by the worker. A route observation
                // can invalidate that connection before the worker is scheduled;
                // do not admit a Reject request using this temporarily stale state.
                // Waiting requests still belong to the surviving send session.
                if config.disconnected_policy == DisconnectedTaskPolicy::Reject {
                    if let Some(mut receiver) = self.network_status.clone() {
                        let snapshot = *receiver.borrow_and_update();
                        if snapshot.status == Some(NetworkStatus::Unavailable)
                            || snapshot.loss_epoch != lease.network_loss_epoch
                        {
                            return Err(NetError::NetworkError);
                        }
                    }
                }
                return (!lease.cancel.is_cancelled())
                    .then_some(lease)
                    .ok_or(NetError::ConnectionClosed);
            }
            match status {
                ConnectionStatus::Closing => Err(NetError::ConnectionClosing),
                ConnectionStatus::Closed => Err(NetError::EngineDropped),
                _ => Err(NetError::SocketNotOpened),
            }
        })();
        result.inspect_err(|error| {
            crate::log_e!(LogType::WSC; "validate_send_admission", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    /// 将已经构造好的消息排队并等待写入终态。
    ///
    /// guard 在整个容量等待和写入等待期间保持有效：Future 在入队前被丢弃会清理
    /// pending；入队后被丢弃还会取消队列项。writer 只会在尚未开始写入时安全跳过，
    /// 若取消撞上正在进行的 sink 写入则废弃连接并按 `DeliveryUnknown` 处理。
    #[allow(clippy::too_many_arguments)]
    async fn enqueue_and_wait(
        &self,
        queue: Arc<PriorityWriteQueue>,
        uuid: String,
        pending_token: Option<u64>,
        message: Message,
        size: usize,
        config: WSRequestConfig,
        admission_cancel: CancellationToken,
        observation: Option<Arc<TaskObservation>>,
    ) -> Result<(), NetError> {
        self.enqueue_and_wait_scoped(
            queue,
            uuid,
            pending_token,
            message,
            size,
            config,
            admission_cancel,
            observation,
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn enqueue_and_wait_scoped(
        &self,
        queue: Arc<PriorityWriteQueue>,
        uuid: String,
        pending_token: Option<u64>,
        message: Message,
        size: usize,
        config: WSRequestConfig,
        admission_cancel: CancellationToken,
        observation: Option<Arc<TaskObservation>>,
        scope: Option<RequestScope>,
    ) -> Result<(), NetError> {
        crate::log_t!(LogType::WSC; "enqueue_and_wait", "config|uuid|pending_token|size", format!("{:?}", config), uuid, pending_token, size);
        let result: Result<(), NetError> = async {
        let dispatch_cancel = scope.as_ref().map(|scope| scope.cancel_token().child_token()).unwrap_or_default();
        let dispatch_phase = DispatchPhase::with_observation(observation.clone());
        let pending =
            pending_token.map(|token| (self.pending_requests.clone(), uuid.clone(), token));
        if let Some((pending_requests, pending_uuid, token)) = &pending {
            if let Err(error) = dispatch_phase.set_pending_cleanup(
                pending_requests.clone(),
                pending_uuid.clone(),
                *token,
            ) {
                pending_requests.remove_if_token(pending_uuid, *token, error);
                return Err(error);
            }
        }
        let mut guard = DispatchCancellationGuard::new(
            dispatch_cancel.clone(),
            dispatch_phase.clone(),
            pending.clone(),
        );
        let result_rx = queue
            .enqueue(
                uuid.clone(),
                pending_token,
                message,
                size,
                config,
                &self.shutdown,
                dispatch_cancel,
                dispatch_phase.clone(),
                admission_cancel,
            )
            .await;
        let result_rx = match result_rx {
            Ok(receiver) => receiver,
            Err(error) => {
                if let Some((pending, uuid, token)) = &pending { pending.remove_if_token(uuid, *token, error); }
                let error = Self::finish_observed_error(&observation, error);
                guard.disarm();
                return Err(error);
            }
        };
        guard.mark_enqueued(Arc::clone(&queue));
        crate::log_s!(LogType::WSC; "enqueue_and_wait", "stage|uuid|pending_token|size", "queued", uuid, pending_token, size);
        let result = match result_rx.await {
            Ok(result) => {
                if let (Err(error), Some((pending, uuid, token))) = (result, &pending) {
                    pending.remove_if_token(uuid.as_str(), *token, error);
                }
                result
            }
            Err(_) => {
                if let Some((pending, uuid, token)) = &pending {
                    pending.remove_if_token(uuid.as_str(), *token, NetError::EngineDropped);
                }
                Err(NetError::EngineDropped)
            }
        };
        guard.disarm();
        crate::log_s!(LogType::WSC; "enqueue_and_wait", "stage|uuid|success", "write_resolved", uuid, result.is_ok());
        result
    }.await;
        result.inspect_err(|error| {
            crate::log_e!(LogType::WSC; "enqueue_and_wait", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    /// 将消息同步入队并放弃写结果接收端；队列/worker 继续拥有请求生命周期。
    #[allow(clippy::too_many_arguments)]
    fn try_enqueue(
        &self,
        queue: Arc<PriorityWriteQueue>,
        uuid: String,
        pending_token: Option<u64>,
        message: Message,
        size: usize,
        config: WSRequestConfig,
        admission_cancel: CancellationToken,
        observation: Option<Arc<TaskObservation>>,
    ) -> Result<oneshot::Receiver<Result<(), NetError>>, NetError> {
        self.try_enqueue_scoped(
            queue,
            uuid,
            pending_token,
            message,
            size,
            config,
            admission_cancel,
            observation,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn try_enqueue_scoped(
        &self,
        queue: Arc<PriorityWriteQueue>,
        uuid: String,
        pending_token: Option<u64>,
        message: Message,
        size: usize,
        config: WSRequestConfig,
        admission_cancel: CancellationToken,
        observation: Option<Arc<TaskObservation>>,
        scope: Option<RequestScope>,
    ) -> Result<oneshot::Receiver<Result<(), NetError>>, NetError> {
        crate::log_t!(LogType::WSC; "try_enqueue", "config|uuid|pending_token|size", format!("{:?}", config), uuid, pending_token, size);
        let result: Result<oneshot::Receiver<Result<(), NetError>>, NetError> = (|| {
            let dispatch_cancel = scope
                .as_ref()
                .map(|scope| scope.cancel_token().child_token())
                .unwrap_or_default();
            let dispatch_phase = DispatchPhase::with_observation(observation.clone());
            let pending =
                pending_token.map(|token| (self.pending_requests.clone(), uuid.clone(), token));
            if let Some((pending_requests, pending_uuid, token)) = &pending {
                if let Err(error) = dispatch_phase.set_pending_cleanup(
                    pending_requests.clone(),
                    pending_uuid.clone(),
                    *token,
                ) {
                    pending_requests.remove_if_token(pending_uuid, *token, error);
                    return Err(error);
                }
            }
            let mut guard = DispatchCancellationGuard::new(
                dispatch_cancel.clone(),
                dispatch_phase.clone(),
                pending.clone(),
            );
            let result_rx = queue.try_enqueue(
                uuid,
                pending_token,
                message,
                size,
                config,
                &self.shutdown,
                dispatch_cancel,
                dispatch_phase.clone(),
                admission_cancel,
            );
            let result_rx = match result_rx {
                Ok(receiver) => receiver,
                Err(error) => {
                    if let Some((pending, uuid, token)) = &pending {
                        pending.remove_if_token(uuid, *token, error);
                    }
                    let error = Self::finish_observed_error(&observation, error);
                    guard.disarm();
                    return Err(error);
                }
            };
            guard.mark_enqueued(queue);
            guard.disarm();
            crate::log_s!(LogType::WSC; "try_enqueue", "stage|pending_token|size", "queued", pending_token, size);
            Ok(result_rx)
        })();
        result.inspect_err(|error| {
            crate::log_e!(LogType::WSC; "try_enqueue", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    /// 请求永久关闭客户端，并等待 worker 完成关闭确认。
    ///
    /// 已关闭时立即成功。否则命令会取消建连、尽力优雅停止活动 I/O、永久关闭写队列、
    /// 清理待处理请求，并将状态置为 `ConnectionStatus::Closed`。I/O 停止阶段使用一个
    /// `close_timeout` deadline 覆盖 Close 入队、写侧确认及协作退出；`Closed` 状态监听器
    /// 另受同一配置的一次等待上限保护。用户回调在可脱离 runtime 的线程执行，因此即使
    /// 超时后仍阻塞也不会阻止 worker 退出。客户端关闭后不可重新连接。
    ///
    /// # 错误
    ///
    /// 若命令通道关闭或 worker 未能通过一次性通道回复，则返回
    /// `NetError::EngineDropped`。
    pub(crate) async fn shutdown(&self) -> Result<(), NetError> {
        crate::log_t!(LogType::WSC; "shutdown");
        let result: Result<(), NetError> = async {
            if self.shutdown_complete.is_cancelled() {
                return Ok(());
            }
            // Cancellation is persistent and synchronous, so dropping the first shutdown
            // future cannot strand later waiters before a command reaches the worker.
            self.request_shutdown();
            tokio::select! {
                biased;
                _ = self.shutdown_complete.cancelled() => Ok(()),
                _ = self.command_tx.closed() => {
                    if self.shutdown_complete.is_cancelled() {
                        Ok(())
                    } else {
                        Err(NetError::EngineDropped)
                    }
                }
            }
        }
        .await;
        result.inspect_err(|error| {
            crate::log_e!(LogType::WSC; "shutdown", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    pub fn set_log_listener(&self, listener: Option<LogListener>) {
        crate::log_t!(LogType::WSC; "set_log_listener", "listener", listener.is_some());
        if let Err(error) = self.try_set_log_listener(listener) {
            crate::log_e!(LogType::WSC; "set_log_listener", "error", crate::common::log::summary::error(&error));
        }
    }

    pub(crate) fn try_set_log_listener(
        &self,
        listener: Option<LogListener>,
    ) -> std::io::Result<()> {
        crate::log_t!(LogType::WSC; "try_set_log_listener", "listener", listener.is_some());
        // Retain the user's callback outside the store lock. A failed OS thread
        // spawn may drop its closure; a captured object's destructor may re-enter
        // this method and must not run while the store lock is still held.
        let listener = listener.map(Arc::new);
        let enabled;
        let previous = {
            let mut current = self.listeners.log.lock().unwrap_or_else(|poisoned| {
                crate::log_e!(LogType::WSC; "try_set_log_listener", "error", "log_listener_lock_poisoned_recovered");
                poisoned.into_inner()
            });
            let replacement = if self.shutdown.is_cancelled() {
                None
            } else {
                listener
                    .as_ref()
                    .map(|listener| {
                        let listener = Arc::clone(listener);
                        crate::common::log::logger::Logger::register_log_listener(
                            Box::new(move |log| listener(log)),
                            &[LogType::WSC, LogType::Common],
                        )
                    })
                    .transpose()?
            };
            enabled = replacement.is_some();
            std::mem::replace(&mut *current, replacement)
        };
        // Dropping a subscription must not retain a listener-store lock while a
        // callback is concurrently replacing or removing its own subscription.
        drop(previous);
        crate::log_s!(LogType::WSC; "try_set_log_listener", "registered", enabled);
        Ok(())
    }
}

impl Drop for WSClientInner {
    /// 在最后一个共享客户端句柄释放时触发尽力而为的后台关闭。
    ///
    /// 析构过程不能异步等待，因此这里只取消客户端级令牌并尝试投递一个无回复通道的
    /// 关闭命令；真正的关闭帧发送、任务终止和队列清理由 worker 完成。通道已满或关闭
    /// 不会导致析构 panic。
    fn drop(&mut self) {
        crate::log_t!(LogType::WSC; "drop");
        self.task_observers.close();
        self.send_admission
            .end_session_with_cause(WebSocketTaskEndCause::Shutdown);
        self.shutdown.cancel();
        if let Some(client) = self.net_status_client.as_ref() {
            client.request_destroy();
        }
        let subscription = self
            .listeners
            .log
            .lock()
            .unwrap_or_else(|poisoned| {
                crate::log_e!(LogType::WSC; "drop", "error", "log_listener_lock_poisoned_recovered");
                poisoned.into_inner()
            })
            .take();
        drop(subscription);
        let _ = self
            .command_tx
            .try_send(ClientCommand::Shutdown { reply: None });
    }
}

#[cfg(test)]
#[path = "ws_client_inner/status_listener_tests.rs"]
mod status_listener_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::module::ws_client::test_support::{check, check_eq, test_error, TestResult};
    use std::sync::Barrier;
    use std::time::Duration;

    struct TestRequest(&'static str);

    impl WSRequestTrait for TestRequest {
        fn uuid(&self) -> String {
            self.0.to_string()
        }

        fn body(&self) -> Result<WsBody, NetError> {
            Ok(WsBody::Text(self.0.to_string()))
        }
    }

    fn connected_inner(config: WebSocketClientConfig) -> TestResult<Arc<WSClientInner>> {
        let (inner, _worker) = WSClientInner::new(config)
            .map_err(|error| test_error(format!("create client internals: {error:?}")))?;
        inner.send_admission.begin_session();
        inner.send_admission.connection_succeeded();
        *inner
            .state
            .write()
            .map_err(|error| test_error(format!("connection state lock: {error:?}")))? =
            ConnectionStatus::Connected;
        Ok(inner)
    }

    #[tokio::test]
    async fn dropping_send_after_writer_commit_keeps_response_correlation() -> TestResult {
        let pending = PendingRequestView::with_capacity(1);
        let (token, completion) = pending
            .reserve(
                Arc::new(TestRequest("committed-before-drop")),
                &WSRequestConfig::default(),
            )
            .map_err(|error| test_error(format!("reserve pending request: {error:?}")))?;
        let cancel = CancellationToken::new();
        let phase = DispatchPhase::new();
        let guard = DispatchCancellationGuard::new(
            cancel.clone(),
            phase.clone(),
            Some((pending.clone(), "committed-before-drop".to_string(), token)),
        );
        check!(phase.start_writing())?;
        check!(phase.commit())?;

        drop(guard);

        check!(!cancel.is_cancelled())?;
        check_eq!(pending.len(), 1)?;
        pending.remove_if_token("committed-before-drop", token, NetError::ConnectionClosed);
        check_eq!(completion.wait().await, Err(NetError::ConnectionClosed))?;
        Ok(())
    }

    #[tokio::test]
    async fn dropping_send_while_writer_owns_it_defers_pending_cleanup_to_writer() -> TestResult {
        let pending = PendingRequestView::with_capacity(1);
        let (token, completion) = pending
            .reserve(
                Arc::new(TestRequest("writing-before-drop")),
                &WSRequestConfig::default(),
            )
            .map_err(|error| test_error(format!("reserve pending request: {error:?}")))?;
        let cancel = CancellationToken::new();
        let phase = DispatchPhase::new();
        let guard = DispatchCancellationGuard::new(
            cancel.clone(),
            phase.clone(),
            Some((pending.clone(), "writing-before-drop".to_string(), token)),
        );
        check!(phase.start_writing())?;

        drop(guard);

        check!(cancel.is_cancelled())?;
        check!(phase.is_cancelled())?;
        check_eq!(pending.len(), 1)?;
        pending.remove_if_token("writing-before-drop", token, NetError::DeliveryUnknown);
        check_eq!(completion.wait().await, Err(NetError::DeliveryUnknown))?;
        Ok(())
    }

    #[test]
    fn data_listener_registration_racing_shutdown_cannot_retain_client() -> TestResult {
        let inner = connected_inner(WebSocketClientConfig::default())?;
        let weak = Arc::downgrade(&inner);
        let held_listener_lock = inner
            .listeners
            .data
            .write()
            .map_err(|error| test_error(format!("listener lock: {error:?}")))?;
        let start = Arc::new(Barrier::new(2));
        let register_inner = Arc::clone(&inner);
        let captured_inner = Arc::clone(&inner);
        let register_start = Arc::clone(&start);
        let register = std::thread::Builder::new().spawn(move || {
            register_start.wait();
            register_inner.register_data_listener(Box::new(move |_response| {
                let _keep_alive = &captured_inner;
            }));
        })?;
        start.wait();

        inner.request_shutdown();
        drop(held_listener_lock);
        register
            .join()
            .map_err(|error| test_error(format!("registration thread: {error:?}")))?;

        check!(inner
            .listeners
            .data
            .read()
            .map_err(|error| test_error(format!("listener lock: {error:?}")))?
            .is_none())?;
        drop(inner);
        check!(weak.upgrade().is_none())?;
        Ok(())
    }

    #[test]
    fn status_listener_registered_after_shutdown_is_not_stored_and_observes_closed() -> TestResult {
        let inner = connected_inner(WebSocketClientConfig::default())?;
        inner.request_shutdown();
        let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
        let observed_by_listener = Arc::clone(&observed);
        let (callback_tx, callback_rx) = std::sync::mpsc::channel();

        inner.register_status_listener(Box::new(move |status| {
            let result = observed_by_listener
                .lock()
                .map_err(|error| test_error(format!("observed status lock: {error:?}")))
                .map(|mut observed| observed.push(status));
            if callback_tx.send(result).is_err() {
                crate::log_e!(LogType::WSC; "test_callback", "error", format!("status callback result receiver closed"));
            }
        }));

        callback_rx.recv_timeout(Duration::from_secs(1))??;
        check!(inner
            .listeners
            .status
            .read()
            .map_err(|error| test_error(format!("listener lock: {error:?}")))?
            .is_none())?;
        check_eq!(
            observed
                .lock()
                .map_err(|error| test_error(format!("observed status lock: {error:?}")))?
                .as_slice(),
            &[ConnectionStatus::Closed]
        )?;
        Ok(())
    }

    #[test]
    fn synchronous_try_send_works_without_runtime_and_remains_queued_after_return() -> TestResult {
        let inner = connected_inner(WebSocketClientConfig {
            business_queue_capacity: 1,
            business_queue_max_bytes: 32,
            pending_request_capacity: 1,
            ..WebSocketClientConfig::default()
        })?;

        check_eq!(
            inner.try_send(
                Arc::new(TestRequest("sync-tracked")),
                WSRequestConfig::default(),
            ),
            Ok(())
        )?;
        check_eq!(inner.pending_requests.len(), 1)?;
        let mut queued = inner.queue.drain();
        check_eq!(
            queued.len(),
            1,
            "successful return must not cancel the item"
        )?;
        let request = queued.pop().ok_or_else(|| test_error("queued request"))?;
        let token = request
            .pending_token
            .ok_or_else(|| test_error("tracked token"))?;
        inner
            .pending_requests
            .remove_if_token("sync-tracked", token, NetError::Cancelled);
        request.complete(Err(NetError::Cancelled));
        check!(inner.pending_requests.is_empty())?;
        Ok(())
    }

    #[test]
    fn synchronous_try_send_rolls_back_pending_when_queue_is_full() -> TestResult {
        let inner = connected_inner(WebSocketClientConfig {
            business_queue_capacity: 1,
            business_queue_max_bytes: 32,
            pending_request_capacity: 1,
            ..WebSocketClientConfig::default()
        })?;
        check_eq!(
            inner.try_send_untracked(
                WsBody::Binary(vec![0; 32].into()),
                WSRequestConfig::default(),
                false,
            ),
            Ok(())
        )?;

        check_eq!(
            inner.try_send(
                Arc::new(TestRequest("retry-after-full")),
                WSRequestConfig::default(),
            ),
            Err(NetError::QueueFull)
        )?;
        check!(inner.pending_requests.is_empty())?;

        for request in inner.queue.drain() {
            request.complete(Err(NetError::Cancelled));
        }
        check_eq!(
            inner.try_send(
                Arc::new(TestRequest("retry-after-full")),
                WSRequestConfig::default(),
            ),
            Ok(()),
            "failed admission must not strand the UUID or capacity"
        )?;
        let request = inner
            .queue
            .drain()
            .pop()
            .ok_or_else(|| test_error("retried request"))?;
        let token = request
            .pending_token
            .ok_or_else(|| test_error("tracked token"))?;
        inner
            .pending_requests
            .remove_if_token("retry-after-full", token, NetError::Cancelled);
        request.complete(Err(NetError::Cancelled));
        Ok(())
    }

    #[tokio::test]
    async fn queued_completion_reports_write_failure_and_cleans_pending() -> TestResult {
        let inner = connected_inner(WebSocketClientConfig {
            business_queue_capacity: 1,
            business_queue_max_bytes: 32,
            pending_request_capacity: 1,
            ..WebSocketClientConfig::default()
        })?;
        let receipt = inner
            .try_send_with_completion(
                Arc::new(TestRequest("queued-write-error")),
                WSRequestConfig::default(),
            )
            .map_err(|error| test_error(format!("nonblocking enqueue with receipt: {error:?}")))?;
        let request = inner
            .queue
            .next()
            .await
            .ok_or_else(|| test_error("queued request"))?;
        let token = request
            .pending_token
            .ok_or_else(|| test_error("tracked token"))?;
        check!(request.dispatch_phase.start_writing())?;
        check!(inner
            .pending_requests
            .mark_writing("queued-write-error", token, 0, 47,))?;
        inner
            .pending_requests
            .remove_if_token("queued-write-error", token, NetError::NetworkError);
        request.complete(Err(NetError::NetworkError));

        check!(inner.pending_requests.is_empty())?;
        check!(matches!(
            receipt.wait_until_written().await,
            Err(NetError::NetworkError)
        ))?;
        Ok(())
    }

    #[tokio::test]
    async fn dropped_in_flight_request_cleans_pending_before_receipt_error() -> TestResult {
        let inner = connected_inner(WebSocketClientConfig {
            business_queue_capacity: 1,
            business_queue_max_bytes: 32,
            pending_request_capacity: 1,
            ..WebSocketClientConfig::default()
        })?;
        let receipt = inner
            .try_send_with_completion(
                Arc::new(TestRequest("dropped-in-flight")),
                WSRequestConfig::default(),
            )
            .map_err(|error| test_error(format!("nonblocking enqueue with receipt: {error:?}")))?;
        let request = inner
            .queue
            .next()
            .await
            .ok_or_else(|| test_error("queued request"))?;
        let token = request
            .pending_token
            .ok_or_else(|| test_error("tracked token"))?;
        check!(request.dispatch_phase.start_writing())?;
        check!(inner
            .pending_requests
            .mark_writing("dropped-in-flight", token, 0, 47))?;
        check!(request.dispatch_phase.start_data_write())?;

        // Models an abort after data admission: QueuedRequest::drop is the only cleanup path.
        drop(request);
        check!(matches!(
            receipt.wait_until_written().await,
            Err(NetError::DeliveryUnknown)
        ))?;
        check!(
            inner.pending_requests.is_empty(),
            "an observed write error must not race ahead of pending cleanup"
        )?;

        inner
            .try_send(
                Arc::new(TestRequest("dropped-in-flight")),
                WSRequestConfig::default(),
            )
            .map_err(|error| {
                test_error(format!(
                    "the UUID must be immediately reusable after the receipt error: {error:?}"
                ))
            })?;
        for request in inner.queue.drain() {
            request.complete(Err(NetError::Cancelled));
        }
        check!(inner.pending_requests.is_empty())?;
        Ok(())
    }

    #[tokio::test]
    async fn every_send_path_rejects_unrepresentable_request_deadlines_before_enqueue() -> TestResult
    {
        let inner = connected_inner(WebSocketClientConfig::default())?;
        let oversized_write = WSRequestConfig {
            write_timeout: Duration::MAX,
            ..WSRequestConfig::default()
        };
        check_eq!(
            inner
                .send(
                    Arc::new(TestRequest("async-write")),
                    oversized_write.clone()
                )
                .await,
            Err(NetError::ConfigError)
        )?;
        check_eq!(
            inner.try_send(Arc::new(TestRequest("sync-write")), oversized_write.clone(),),
            Err(NetError::ConfigError)
        )?;
        check_eq!(
            inner
                .send_untracked(
                    WsBody::Text("async-untracked".to_string()),
                    oversized_write.clone(),
                    false,
                )
                .await,
            Err(NetError::ConfigError)
        )?;
        check_eq!(
            inner.try_send_untracked(
                WsBody::Text("sync-untracked".to_string()),
                oversized_write,
                false,
            ),
            Err(NetError::ConfigError)
        )?;

        let oversized_response = WSRequestConfig {
            response_timeout: Duration::MAX,
            ..WSRequestConfig::default()
        };
        check_eq!(
            inner.try_send(Arc::new(TestRequest("sync-response")), oversized_response,),
            Err(NetError::ConfigError)
        )?;
        let oversized_enqueue = WSRequestConfig {
            enqueue_timeout: Some(Duration::MAX),
            ..WSRequestConfig::default()
        };
        check_eq!(
            inner.try_send(Arc::new(TestRequest("sync-enqueue")), oversized_enqueue,),
            Err(NetError::ConfigError)
        )?;
        check!(inner.pending_requests.is_empty())?;
        check!(inner.queue.drain().is_empty())?;
        Ok(())
    }

    #[tokio::test]
    async fn dropping_send_while_waiting_for_queue_capacity_releases_pending() -> TestResult {
        let inner = connected_inner(WebSocketClientConfig {
            business_queue_capacity: 1,
            business_queue_max_bytes: 32,
            pending_request_capacity: 1,
            ..WebSocketClientConfig::default()
        })?;
        let blocker_result = inner
            .queue
            .enqueue(
                "blocker".to_string(),
                None,
                Message::Text("blocker".into()),
                7,
                WSRequestConfig::default(),
                &inner.shutdown,
                CancellationToken::new(),
                DispatchPhase::new(),
                CancellationToken::new(),
            )
            .await
            .map_err(|error| test_error(format!("fill business queue: {error:?}")))?;

        let send_inner = Arc::clone(&inner);
        let send = tokio::spawn(async move {
            send_inner
                .send(
                    Arc::new(TestRequest("cancelled")),
                    WSRequestConfig::default(),
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while inner.pending_requests.is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|error| {
            test_error(format!(
                "send must reserve pending before waiting for capacity: {error:?}"
            ))
        })?;

        send.abort();
        check!(matches!(send.await, Err(error) if error.is_cancelled()))?;
        check!(inner.pending_requests.is_empty())?;

        for request in inner.queue.drain() {
            request.complete(Err(NetError::Cancelled));
        }
        check!(matches!(blocker_result.await, Ok(Err(NetError::Cancelled))))?;
        Ok(())
    }

    #[tokio::test]
    async fn urgent_untracked_message_bypasses_full_pending_and_business_queue() -> TestResult {
        let inner = connected_inner(WebSocketClientConfig {
            business_queue_capacity: 1,
            business_queue_max_bytes: 32,
            urgent_queue_capacity: 1,
            urgent_queue_max_bytes: 32,
            pending_request_capacity: 1,
            ..WebSocketClientConfig::default()
        })?;
        let (tracked_token, _completion) = inner
            .pending_requests
            .reserve(
                Arc::new(TestRequest("tracked")),
                &WSRequestConfig::default(),
            )
            .map_err(|error| test_error(format!("fill pending table: {error:?}")))?;
        let blocker_result = inner
            .queue
            .enqueue(
                "blocker".to_string(),
                None,
                Message::Text("blocker".into()),
                7,
                WSRequestConfig::default(),
                &inner.shutdown,
                CancellationToken::new(),
                DispatchPhase::new(),
                CancellationToken::new(),
            )
            .await
            .map_err(|error| test_error(format!("fill business queue: {error:?}")))?;

        let send_inner = Arc::clone(&inner);
        let send = tokio::spawn(async move {
            send_inner
                .send_untracked(
                    WsBody::Text("ack".to_string()),
                    WSRequestConfig::default(),
                    true,
                )
                .await
        });
        let request = tokio::time::timeout(Duration::from_secs(1), inner.urgent_queue.next())
            .await
            .map_err(|error| {
                test_error(format!(
                    "urgent queue admission must not wait for business capacity: {error:?}"
                ))
            })?
            .ok_or_else(|| test_error("urgent request"))?;
        check!(request.pending_token.is_none())?;
        request.complete(Ok(()));
        check_eq!(
            send.await
                .map_err(|error| test_error(format!("send task: {error:?}")))?,
            Ok(())
        )?;
        check_eq!(inner.pending_requests.len(), 1)?;

        inner
            .pending_requests
            .remove_if_token("tracked", tracked_token, NetError::Cancelled);
        for request in inner.queue.drain() {
            request.complete(Err(NetError::Cancelled));
        }
        check_eq!(blocker_result.await, Ok(Err(NetError::Cancelled)))?;
        Ok(())
    }

    #[tokio::test]
    async fn connection_end_cancels_a_reject_send_still_waiting_for_capacity() -> TestResult {
        let inner = connected_inner(WebSocketClientConfig {
            business_queue_capacity: 1,
            business_queue_max_bytes: 32,
            ..WebSocketClientConfig::default()
        })?;
        let blocker = inner
            .queue
            .enqueue(
                "blocker".to_string(),
                None,
                Message::Text("blocker".into()),
                7,
                WSRequestConfig::default(),
                &inner.shutdown,
                CancellationToken::new(),
                DispatchPhase::new(),
                CancellationToken::new(),
            )
            .await
            .map_err(|error| test_error(format!("fill queue: {error:?}")))?;
        let send_inner = Arc::clone(&inner);
        let send = tokio::spawn(async move {
            send_inner
                .send(Arc::new(TestRequest("late")), WSRequestConfig::default())
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while inner.pending_requests.is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|error| test_error(format!("send reserves pending: {error:?}")))?;

        inner.send_admission.connection_ended();
        for request in inner.queue.drain() {
            request.complete(Err(NetError::ConnectionClosed));
        }

        check_eq!(
            send.await
                .map_err(|error| test_error(format!("send task: {error:?}")))?,
            Err(NetError::ConnectionClosed)
        )?;
        check!(inner.pending_requests.is_empty())?;
        check_eq!(blocker.await, Ok(Err(NetError::ConnectionClosed)))?;
        Ok(())
    }

    #[tokio::test]
    async fn shutdown_cancels_a_capacity_waiter_before_it_can_enqueue() -> TestResult {
        let inner = connected_inner(WebSocketClientConfig {
            business_queue_capacity: 1,
            business_queue_max_bytes: 32,
            ..WebSocketClientConfig::default()
        })?;
        let blocker = inner
            .queue
            .enqueue(
                "blocker".to_string(),
                None,
                Message::Text("blocker".into()),
                7,
                WSRequestConfig::default(),
                &inner.shutdown,
                CancellationToken::new(),
                DispatchPhase::new(),
                CancellationToken::new(),
            )
            .await
            .map_err(|error| test_error(format!("fill queue: {error:?}")))?;
        let send_inner = Arc::clone(&inner);
        let send = tokio::spawn(async move {
            send_inner
                .send(
                    Arc::new(TestRequest("shutdown-waiter")),
                    WSRequestConfig::default(),
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while inner.pending_requests.is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|error| test_error(format!("send reserves pending: {error:?}")))?;

        inner.request_shutdown();
        for request in inner.queue.drain() {
            request.complete(Err(NetError::Cancelled));
        }

        check!(matches!(
            send.await
                .map_err(|error| test_error(format!("send task: {error:?}")))?,
            Err(NetError::Cancelled | NetError::ConnectionClosed)
        ))?;
        check!(inner.pending_requests.is_empty())?;
        check!(inner.queue.drain().is_empty())?;
        check_eq!(blocker.await, Ok(Err(NetError::Cancelled)))?;
        Ok(())
    }

    #[test]
    fn oversized_tokio_channel_capacity_is_rejected_without_panicking() -> TestResult {
        let result = WSClientInner::new(WebSocketClientConfig {
            callback_queue_capacity: Semaphore::MAX_PERMITS + 1,
            ..WebSocketClientConfig::default()
        });
        check!(matches!(result, Err(NetError::ConfigError)))?;
        Ok(())
    }

    #[test]
    fn unrepresentable_client_deadlines_are_rejected_without_a_runtime() -> TestResult {
        check!(!duration_fits_instant(Duration::MAX))?;
        for config in [
            WebSocketClientConfig {
                heartbeat_interval: Duration::MAX,
                pong_timeout: Duration::MAX,
                ..WebSocketClientConfig::default()
            },
            WebSocketClientConfig {
                pong_timeout: Duration::MAX,
                ..WebSocketClientConfig::default()
            },
            WebSocketClientConfig {
                close_timeout: Duration::MAX,
                ..WebSocketClientConfig::default()
            },
            WebSocketClientConfig {
                control_write_timeout: Duration::MAX,
                ..WebSocketClientConfig::default()
            },
            WebSocketClientConfig {
                data_frame_write_timeout: Duration::MAX,
                ..WebSocketClientConfig::default()
            },
            WebSocketClientConfig {
                response_dispatch_grace: Duration::MAX,
                ..WebSocketClientConfig::default()
            },
        ] {
            check!(matches!(
                WSClientInner::new(config),
                Err(NetError::ConfigError)
            ))?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn connect_rejects_unrepresentable_reconnect_deadlines_before_dispatch() -> TestResult {
        let (inner, _worker) = WSClientInner::new(WebSocketClientConfig::default())
            .map_err(|error| test_error(format!("create client internals: {error:?}")))?;
        let result = inner
            .connect(
                "ws://127.0.0.1:1".to_string(),
                WebSocketConnectOptions {
                    reconnect: crate::ReconnectPolicy {
                        handshake_timeout: Duration::MAX,
                        ..crate::ReconnectPolicy::default()
                    },
                    ..WebSocketConnectOptions::default()
                },
            )
            .await;
        check!(matches!(result, Err(NetError::ConfigError)))?;
        Ok(())
    }

    #[test]
    fn zero_read_buffer_or_callback_concurrency_is_rejected() -> TestResult {
        let zero_read_buffer = WSClientInner::new(WebSocketClientConfig {
            read_buffer_size: 0,
            ..WebSocketClientConfig::default()
        });
        check!(matches!(zero_read_buffer, Err(NetError::ConfigError)))?;

        let zero_callback_concurrency = WSClientInner::new(WebSocketClientConfig {
            data_callback_concurrency: 0,
            ..WebSocketClientConfig::default()
        });
        check!(matches!(
            zero_callback_concurrency,
            Err(NetError::ConfigError)
        ))?;
        Ok(())
    }

    #[test]
    fn write_buffer_must_hold_one_complete_masked_data_frame() -> TestResult {
        let result = WSClientInner::new(WebSocketClientConfig {
            data_frame_payload_size: Some(WebSocketClientConfig::MIN_DATA_FRAME_PAYLOAD_SIZE),
            write_buffer_size: 0,
            max_write_buffer_size: WebSocketClientConfig::MIN_DATA_FRAME_PAYLOAD_SIZE
                + WebSocketClientConfig::MAX_CLIENT_FRAME_OVERHEAD
                - 1,
            ..WebSocketClientConfig::default()
        });
        check!(matches!(result, Err(NetError::ConfigError)))?;
        Ok(())
    }
}

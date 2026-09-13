use crate::api::listener::{
    WebSocketClientConnectStatusListener, WebSocketClientDataReceiveListener,
};
use crate::api::net_error::NetError;
use crate::api::traits::ws::connection_status::ConnectionStatus;
use crate::api::traits::ws::ws_body::WsBody;
use crate::api::traits::ws::ws_request_config::WSRequestConfig;
use crate::api::traits::ws::ws_request_trait::WSRequestTrait;
use crate::api::wsc::pending_request_completion::PendingRequestCompletion;
use crate::api::wsc::queued_request_completion::QueuedRequestCompletion;
pub use crate::api::wsc::reconnect_policy::ReconnectPolicy;
pub use crate::api::wsc::web_socket_client_config::{TcpKeepaliveConfig, WebSocketClientConfig};
pub use crate::api::wsc::web_socket_connect_options::WebSocketConnectOptions;
use crate::api::wsc::wsc_response::PendingRequestView;
use crate::api::wsc::{WebSocketConnectionEvents, WebSocketContextConnectOptions};
use crate::module::ws_client::ws_client_inner::WSClientInner;
use on_common::log::listener::LogListener;
use on_common::log::log_def::LogType;
use std::sync::Arc;

pub type WebSocketHeaderProvider =
    Arc<dyn Fn() -> Result<Vec<(String, String)>, NetError> + Send + Sync + 'static>;

#[derive(Clone)]
pub struct WebSocketClient {
    pub(crate) inner: Arc<WSClientInner>,
}

impl WebSocketClient {
    /// Prepare a bounded request registration without allowing any socket write.
    ///
    /// Bind the returned registration to the business owner, then call `commit`.
    /// Requires `expect_response = true`. Notification and queue admission share
    /// `enqueue_timeout`. Response timeout defaults to `AfterWritten`; explicitly selecting
    /// `AtRegistration` includes subsequent capacity and owner-binding waits. The optional
    /// registration deadline limits pending insertion, not when this method returns.
    /// A dropped preparation cancels the registration. Scope must match the connection target.
    pub async fn prepare_registered(
        &self,
        request: Arc<dyn WSRequestTrait>,
        options: crate::WebSocketRequestOptions,
    ) -> Result<crate::PreparedRequest, NetError> {
        self.inner.prepare_registered(request, options).await
    }

    /// Nonblocking preparation; no Tokio runtime is required. Full capacity returns `QueueFull`.
    /// The client's worker owns any registration timer, including before `commit`.
    pub fn try_prepare_registered(
        &self,
        request: Arc<dyn WSRequestTrait>,
        options: crate::WebSocketRequestOptions,
    ) -> Result<crate::PreparedRequest, NetError> {
        self.inner.try_prepare_registered(request, options)
    }

    /// Send a complete untracked message using an explicit original business scope.
    /// This forces `expect_response = false`, just like `send_message_with_config`.
    /// Registration deadlines and `AtRegistration` are invalid here (`ConfigError`).
    pub async fn send_message_with_options(
        &self,
        body: WsBody,
        options: crate::WebSocketRequestOptions,
    ) -> Result<(), NetError> {
        options.validate_untracked()?;
        self.inner
            .send_untracked_scoped(body, options.config, false, options.scope)
            .await
    }

    /// Nonblocking untracked submission with explicit scope; success means local enqueue.
    /// Registration deadlines and `AtRegistration` are invalid here (`ConfigError`).
    pub fn try_send_message_with_options(
        &self,
        body: WsBody,
        options: crate::WebSocketRequestOptions,
    ) -> Result<(), NetError> {
        options.validate_untracked()?;
        self.inner
            .try_send_untracked_scoped(body, options.config, false, options.scope)
    }

    /// Send on the reserved application urgent lane. Urgency never bypasses scope validation.
    /// Registration deadlines and `AtRegistration` are invalid here (`ConfigError`).
    pub async fn send_urgent_message_with_options(
        &self,
        body: WsBody,
        options: crate::WebSocketRequestOptions,
    ) -> Result<(), NetError> {
        options.validate_untracked()?;
        self.inner
            .send_untracked_scoped(body, options.config, true, options.scope)
            .await
    }

    /// Nonblocking urgent submission with the same scope checks as ordinary messages.
    /// Registration deadlines and `AtRegistration` are invalid here (`ConfigError`).
    pub fn try_send_urgent_message_with_options(
        &self,
        body: WsBody,
        options: crate::WebSocketRequestOptions,
    ) -> Result<(), NetError> {
        options.validate_untracked()?;
        self.inner
            .try_send_untracked_scoped(body, options.config, true, options.scope)
    }

    pub(crate) fn from_inner(inner: Arc<WSClientInner>) -> Self {
        on_common::log_t!(LogType::WSC; "from_inner");
        Self { inner }
    }
}

impl WebSocketClient {
    /// 注册或替换监听器，用于接收此后获准任务的终态结果。
    ///
    /// 每个任务始终保留最初的监听器，替换、注销、断开连接、关闭或销毁均不改变这一点。
    /// 同一次注册的回调在没有 Tokio 运行时上下文的专用操作系统线程上串行执行，
    /// 不同注册的回调可以并发执行。
    ///
    /// 接收任务前先预留通知容量，直至该任务的回调返回才释放。回调处理缓慢会使新的发送
    /// 等待容量；非阻塞发送返回 `QueueFull`，异步发送则让通知准入与入队共享同一超时。
    /// 通知准入前即遭拒绝的调用不会生成结果事件；一旦获准，即使随后因写队列或待响应表
    /// 限制而被拒绝，也会生成一次事件。现有 `Result` 和完成通知接口的传输语义保持不变。
    ///
    /// 关闭会等待任务结算，但不会等待任意用户回调。尚未交付的通知可在网络工作任务结束后
    /// 继续交付。注册失败保留原监听器；永久关闭后再注册返回 `EngineDropped`。
    /// 如需异步处理业务，应将通知数据转交应用自己的运行时或通道。
    pub fn register_web_socket_client_task_complete_listener(
        &self,
        listener: crate::api::listener::WebSocketClientTaskCompleteListener,
        options: crate::api::wsc::WebSocketTaskEventOptions,
    ) -> Result<(), NetError> {
        self.inner.register_task_listener(listener, options)
    }

    /// 停止监听新任务；已获准的任务仍通知各自最初的监听器。
    /// 本操作不等待回调，也不取消请求或连接。
    pub fn unregister_web_socket_client_task_complete_listener(&self) -> Result<(), NetError> {
        self.inner.unregister_task_listener()
    }

    /// 启动可通过事件监听的连接会话，在后台工作任务接收后返回。
    ///
    /// 读取事件直至收到 `Established`，即可确认握手成功；启用自动重连时应继续消费事件。
    /// 事件保留产生它的实际尝试上下文。消费缓慢会暂停后续尝试，但仍可主动断开或关闭。
    /// 在异步操作返回前将其丢弃，或丢弃返回的事件句柄，只会取消本次会话。
    /// 握手最终失败会终结会话并移除其目标；`notify_network_available` 不能重启该会话。
    /// 收到 `SessionTerminated` 后，如需恢复，应确认业务连接意图仍有效，再调用本方法
    /// 创建新会话；即使网络一直可用、没有新的网络状态通知，也可以显式发起。
    /// 旧事件句柄的取消或丢弃不会取消新会话。
    ///
    /// 命令通道已满时，本方法等待容量并返回后台工作任务的准入结果。对于同一客户端，
    /// 连接中、重连中、已连接或正在关闭时返回 `ConnectionExists`，不会替换现有连接。
    /// 这不提供跨客户端或跨业务恢复任务的幂等保证；调用方应统一自己的连接入口。
    pub async fn start_connect_with_context(
        &self,
        options: WebSocketContextConnectOptions,
    ) -> Result<WebSocketConnectionEvents, NetError> {
        self.inner.start_connect_with_context(options).await
    }

    /// 注册 WebSocket 客户端数据接收监听
    ///
    /// 回调在没有 Tokio 运行时上下文的独立操作系统线程执行，网络工作线程不等待用户
    /// 处理。默认串行；`data_callback_concurrency` 大于 1 时允许并发且不保证执行顺序。
    /// 队列或字节预算满时会以 `CallbackQueueOverflow` 结束当前连接的读取。
    /// 数据回调线程创建失败会记录原连接代次的错误；若工作任务处理该故障时该代仍处于
    /// 已连接状态，则以 `TaskInterruptionError` 终止连接。旧代故障不会终止新连接。
    /// 需要逐次观察连接终止原因时，应使用 [`Self::start_connect_with_context`] 的事件流。
    pub fn register_web_socket_client_data_receive_listener(
        &self,
        listener: WebSocketClientDataReceiveListener,
    ) {
        on_common::log_t!(LogType::WSC; "register_web_socket_client_data_receive_listener", "listener", "callback");
        self.inner.register_data_listener(listener);
    }

    pub fn unregister_web_socket_client_data_receive_listener(&self) {
        on_common::log_t!(LogType::WSC; "unregister_web_socket_client_data_receive_listener");
        self.inner.unregister_data_listener();
    }

    /// 注册 WebSocket 客户端连接状态监听
    ///
    /// 首次状态快照和后续通知均在没有 Tokio 运行时上下文的专用操作系统线程执行。
    /// 注册返回不代表首次通知已完成；同一次注册的后续通知在首次完成后读取最新状态，
    /// 允许合并和重复。替换或注销不会取消已提交的回调，不同注册的回调可并发。
    /// 关闭后注册只异步通知一次 `Closed`，不保存监听器。线程创建失败会记录错误日志。
    pub fn register_web_socket_client_connect_status_listener(
        &self,
        listener: WebSocketClientConnectStatusListener,
    ) {
        on_common::log_t!(LogType::WSC; "register_web_socket_client_connect_status_listener", "listener", "callback");
        self.inner.register_status_listener(listener);
    }

    pub fn unregister_web_socket_client_connect_status_listener(&self) {
        on_common::log_t!(LogType::WSC; "unregister_web_socket_client_connect_status_listener");
        self.inner.unregister_status_listener();
    }

    pub fn connection_status(&self) -> ConnectionStatus {
        on_common::log_t!(LogType::WSC; "connection_status");
        self.inner.connection_status()
    }

    /// 返回最近一次连接失败或异常断开的原因。
    ///
    /// 状态监听器收到 `Disconnected`/`Reconnecting` 后可用该快照区分网络错误、
    /// 心跳超时、回调队列溢出等原因。自动重连成功后仍保留最近故障；主动开始新的
    /// 初始连接或主动断开时清空。
    /// 后续故障会覆盖该值；状态回调也允许合并，因此本快照不能代替逐次连接事件。
    pub fn last_connection_error(&self) -> Option<NetError> {
        on_common::log_t!(LogType::WSC; "last_connection_error");
        self.inner.last_connection_error()
    }

    /// 返回最近一次终态 WebSocket HTTP 握手失败的状态码。
    ///
    /// `connect` 仍返回稳定的 [`NetError`]；当它或自动重连以 `ConnectError` 结束时，
    /// 调用方可用本快照区分 401/403/429/5xx
    /// 限流或服务端错误策略。成功连接、主动新建连接或主动断开会清空该值。
    pub fn last_handshake_http_status(&self) -> Option<u16> {
        on_common::log_t!(LogType::WSC; "last_handshake_http_status");
        self.inner.last_handshake_http_status()
    }

    pub fn pending_requests(&self) -> PendingRequestView {
        on_common::log_t!(LogType::WSC; "pending_requests");
        self.inner.pending_requests()
    }

    /// 使用默认连接选项显式连接，并等待本轮初始握手及其重试结束。
    ///
    /// 失败后可以再次显式调用；同一客户端仍在连接中、重连中、已连接或正在关闭时
    /// 返回 `ConnectionExists`。需要可靠关联每次尝试和耗尽后的恢复时，应使用
    /// [`Self::start_connect_with_context`] 并持续消费会话事件。
    pub async fn connect(&self, url: &str) -> Result<(), NetError> {
        on_common::log_t!(LogType::WSC; "connect", "url", on_common::log::summary::url(url));
        let result: Result<(), NetError> = async {
            self.connect_with_options(url, WebSocketConnectOptions::default())
                .await
        }
        .await;
        result.inspect_err(|error| {
            on_common::log_e!(LogType::WSC; "connect", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    /// 使用指定选项显式连接，并等待本轮初始握手及其重试结束。
    ///
    /// 本方法与 [`Self::connect`] 共享准入规则。命令通道已满时等待容量；握手成功
    /// 后返回 `Ok(())`，失败则返回本轮最终错误。失败后可以用更新后的选项重新调用。
    /// 暂态失败耗尽后，启用自动重连的目标仍可由 [`Self::notify_network_available`]
    /// 提示恢复；不可重试失败会移除自动恢复目标，必须显式发起新连接。
    pub async fn connect_with_options(
        &self,
        url: &str,
        options: WebSocketConnectOptions,
    ) -> Result<(), NetError> {
        on_common::log_t!(LogType::WSC; "connect_with_options", "url|header_count|has_header_provider|reconnect", on_common::log::summary::url(url), options.headers.len(), options.header_provider.is_some(), format!("{:?}", options.reconnect));
        let result: Result<(), NetError> = async {
            if url.trim().is_empty() {
                return Err(NetError::ParameterEmpty);
            }
            self.inner.connect(url.trim().to_string(), options).await
        }
        .await;
        result.inspect_err(|error| {
            on_common::log_e!(LogType::WSC; "connect_with_options", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    pub async fn disconnect(&self) -> Result<(), NetError> {
        on_common::log_t!(LogType::WSC; "disconnect");
        let result: Result<(), NetError> = async { self.inner.disconnect().await }.await;
        result.inspect_err(|error| {
            on_common::log_e!(LogType::WSC; "disconnect", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    /// 尽力提示网络已恢复，可提前唤醒退避，并尝试启动符合条件的重连周期。
    ///
    /// 本方法不等待处理结果，也不保存持久网络状态。命令通道已满或关闭时，投递的
    /// 命令会丢失；没有连接任务等待时，退避唤醒信号也不会保留。
    /// 对 `connect` / `connect_with_options` 创建的连接，仅在已断开、仍有目标且
    /// 启用自动重连时允许启动新周期。暂态失败耗尽仍可恢复；401/403、代理鉴权失败
    /// 或确定性的参数、请求头、provider、TLS 失败等不可重试失败会移除自动恢复目标。
    /// 主动断开或关闭后不恢复旧目标，重复提示也不会替换健康连接。
    ///
    /// 通过 [`Self::start_connect_with_context`] 创建的会话一旦产生 `SessionTerminated`，
    /// 本方法不能重启它。需要可靠恢复时，应确认业务连接意图仍有效并显式创建新会话，
    /// 不能仅依赖此提示。
    pub fn notify_network_available(&self) {
        on_common::log_t!(LogType::WSC; "notify_network_available");
        self.inner.notify_network_available();
    }

    pub async fn send<R>(&self, request: R) -> Result<(), NetError>
    where
        R: WSRequestTrait,
    {
        on_common::log_t!(LogType::WSC; "send", "request_type", std::any::type_name::<R>());
        let result: Result<(), NetError> = async {
            self.send_with_config(request, WSRequestConfig::default())
                .await
        }
        .await;
        result.inspect_err(|error| {
            on_common::log_e!(LogType::WSC; "send", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    pub async fn send_with_config<R>(
        &self,
        request: R,
        config: WSRequestConfig,
    ) -> Result<(), NetError>
    where
        R: WSRequestTrait,
    {
        on_common::log_t!(LogType::WSC; "send_with_config", "config|request_type", format!("{:?}", config), std::any::type_name::<R>());
        let result: Result<(), NetError> =
            async { self.inner.send(Arc::new(request), config).await }.await;
        result.inspect_err(|error| {
            on_common::log_e!(LogType::WSC; "send_with_config", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    /// 发送请求，并返回可等待“响应被认领/超时/断线”的唯一完成通知。
    ///
    /// 本方法先等待写入端确认写入（或已认领响应证明送达），随后返回句柄。应用仍在
    /// 数据监听器中解析协议响应，并通过 `WSCResponse::take_request` 完成关联；句柄的
    /// `wait` 在认领时返回成功，在响应超时、连接代次终止、主动断开或关闭时返回对应错误。
    /// 既无确认写入也无已认领响应时的写入失败，或在句柄返回前发送异步操作被取消，
    /// 会由本方法本身返回/终止，尚不存在可交给调用方的句柄。
    pub async fn send_with_completion<R>(
        &self,
        request: R,
        config: WSRequestConfig,
    ) -> Result<PendingRequestCompletion, NetError>
    where
        R: WSRequestTrait,
    {
        on_common::log_t!(LogType::WSC; "send_with_completion", "config|request_type", format!("{:?}", config), std::any::type_name::<R>());
        let result: Result<PendingRequestCompletion, NetError> = async {
            self.inner
                .send_with_completion(Arc::new(request), config)
                .await
        }
        .await;
        result.inspect_err(|error| {
            on_common::log_e!(LogType::WSC; "send_with_completion", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    /// `send_with_completion` 的共享特征对象变体。
    pub async fn send_shared_with_completion(
        &self,
        request: Arc<dyn WSRequestTrait>,
        config: WSRequestConfig,
    ) -> Result<PendingRequestCompletion, NetError> {
        on_common::log_t!(LogType::WSC; "send_shared_with_completion", "config|request_type", format!("{:?}", config), "dyn WSRequestTrait");
        let result: Result<PendingRequestCompletion, NetError> =
            async { self.inner.send_with_completion(request, config).await }.await;
        result.inspect_err(|error| {
            on_common::log_e!(LogType::WSC; "send_shared_with_completion", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    /// `send_with_completion` 的装箱特征对象变体。
    pub async fn send_boxed_with_completion(
        &self,
        request: Box<dyn WSRequestTrait>,
        config: WSRequestConfig,
    ) -> Result<PendingRequestCompletion, NetError> {
        on_common::log_t!(LogType::WSC; "send_boxed_with_completion", "config|request_type", format!("{:?}", config), "dyn WSRequestTrait");
        let result: Result<PendingRequestCompletion, NetError> = async {
            self.inner
                .send_with_completion(Arc::from(request), config)
                .await
        }
        .await;
        result.inspect_err(|error| {
            on_common::log_e!(LogType::WSC; "send_boxed_with_completion", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    pub async fn send_shared(
        &self,
        request: Arc<dyn WSRequestTrait>,
        config: WSRequestConfig,
    ) -> Result<(), NetError> {
        on_common::log_t!(LogType::WSC; "send_shared", "config|request_type", format!("{:?}", config), "dyn WSRequestTrait");
        let result: Result<(), NetError> = async { self.inner.send(request, config).await }.await;
        result.inspect_err(|error| {
            on_common::log_e!(LogType::WSC; "send_shared", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    pub async fn send_boxed(
        &self,
        request: Box<dyn WSRequestTrait>,
        config: WSRequestConfig,
    ) -> Result<(), NetError> {
        on_common::log_t!(LogType::WSC; "send_boxed", "config|request_type", format!("{:?}", config), "dyn WSRequestTrait");
        let result: Result<(), NetError> =
            async { self.inner.send(Arc::from(request), config).await }.await;
        result.inspect_err(|error| {
            on_common::log_e!(LogType::WSC; "send_boxed", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    /// 同步、非阻塞地尝试把请求加入普通业务写队列。
    ///
    /// 本方法可从没有 Tokio 运行时的回调线程调用。`Ok(())` 只表示已经完成本地准入
    /// 和入队，并不表示消息已经写入网络或收到业务响应；容量暂不可用时立即返回
    /// [`NetError::QueueFull`]。`config.enqueue_timeout` 在本入口不生效。
    pub fn try_send<R>(&self, request: R) -> Result<(), NetError>
    where
        R: WSRequestTrait,
    {
        on_common::log_t!(LogType::WSC; "try_send", "request_type", std::any::type_name::<R>());
        let result: Result<(), NetError> =
            self.try_send_with_config(request, WSRequestConfig::default());
        result.inspect_err(|error| {
            on_common::log_e!(LogType::WSC; "try_send", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    /// 使用指定策略同步、非阻塞入队请求。
    pub fn try_send_with_config<R>(
        &self,
        request: R,
        config: WSRequestConfig,
    ) -> Result<(), NetError>
    where
        R: WSRequestTrait,
    {
        on_common::log_t!(LogType::WSC; "try_send_with_config", "config|request_type", format!("{:?}", config), std::any::type_name::<R>());
        let result: Result<(), NetError> = self.inner.try_send(Arc::new(request), config);
        result.inspect_err(|error| {
            on_common::log_e!(LogType::WSC; "try_send_with_config", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    /// `try_send_with_config` 的共享特征对象变体。
    pub fn try_send_shared(
        &self,
        request: Arc<dyn WSRequestTrait>,
        config: WSRequestConfig,
    ) -> Result<(), NetError> {
        on_common::log_t!(LogType::WSC; "try_send_shared", "config|request_type", format!("{:?}", config), "dyn WSRequestTrait");
        let result: Result<(), NetError> = self.inner.try_send(request, config);
        result.inspect_err(|error| {
            on_common::log_e!(LogType::WSC; "try_send_shared", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    /// `try_send_with_config` 的装箱特征对象变体。
    pub fn try_send_boxed(
        &self,
        request: Box<dyn WSRequestTrait>,
        config: WSRequestConfig,
    ) -> Result<(), NetError> {
        on_common::log_t!(LogType::WSC; "try_send_boxed", "config|request_type", format!("{:?}", config), "dyn WSRequestTrait");
        let result: Result<(), NetError> = self.inner.try_send(Arc::from(request), config);
        result.inspect_err(|error| {
            on_common::log_e!(LogType::WSC; "try_send_boxed", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    /// 同步、非阻塞入队，并返回可在异步执行器中观察写入及响应终态的两阶段回执。
    ///
    /// 创建回执不要求当前线程存在 Tokio 运行时。调用方应把回执转交给已有运行时，
    /// `wait_until_written().await` 成功后再等待返回的 `PendingRequestCompletion`。本方法要求
    /// `config.expect_response == true`。
    pub fn try_send_with_completion<R>(
        &self,
        request: R,
        config: WSRequestConfig,
    ) -> Result<QueuedRequestCompletion, NetError>
    where
        R: WSRequestTrait,
    {
        on_common::log_t!(LogType::WSC; "try_send_with_completion", "config|request_type", format!("{:?}", config), std::any::type_name::<R>());
        let result: Result<QueuedRequestCompletion, NetError> = {
            self.inner
                .try_send_with_completion(Arc::new(request), config)
        };
        result.inspect_err(|error| {
            on_common::log_e!(LogType::WSC; "try_send_with_completion", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    /// `try_send_with_completion` 的共享特征对象变体。
    pub fn try_send_shared_with_completion(
        &self,
        request: Arc<dyn WSRequestTrait>,
        config: WSRequestConfig,
    ) -> Result<QueuedRequestCompletion, NetError> {
        on_common::log_t!(LogType::WSC; "try_send_shared_with_completion", "config|request_type", format!("{:?}", config), "dyn WSRequestTrait");
        let result: Result<QueuedRequestCompletion, NetError> =
            self.inner.try_send_with_completion(request, config);
        result.inspect_err(|error| {
            on_common::log_e!(LogType::WSC; "try_send_shared_with_completion", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    /// `try_send_with_completion` 的装箱特征对象变体。
    pub fn try_send_boxed_with_completion(
        &self,
        request: Box<dyn WSRequestTrait>,
        config: WSRequestConfig,
    ) -> Result<QueuedRequestCompletion, NetError> {
        on_common::log_t!(LogType::WSC; "try_send_boxed_with_completion", "config|request_type", format!("{:?}", config), "dyn WSRequestTrait");
        let result: Result<QueuedRequestCompletion, NetError> = {
            self.inner
                .try_send_with_completion(Arc::from(request), config)
        };
        result.inspect_err(|error| {
            on_common::log_e!(LogType::WSC; "try_send_boxed_with_completion", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    /// 发送一条不期待业务响应的 Text/Binary 消息，并等待其写入 WebSocket 写入端。
    ///
    /// 该入口适用于应用 ACK、通知等无需等待响应的数据。它仍使用业务优先级队列，
    /// 但写入成功后不会保留待响应请求记录。
    pub async fn send_message(&self, body: WsBody) -> Result<(), NetError> {
        on_common::log_t!(LogType::WSC; "send_message", "body_bytes", match &body { WsBody::Text(v) => v.len(), WsBody::Binary(v) => v.len() });
        let result: Result<(), NetError> = async {
            self.send_message_with_config(body, WSRequestConfig::default())
                .await
        }
        .await;
        result.inspect_err(|error| {
            on_common::log_e!(LogType::WSC; "send_message", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    /// 使用指定调度与写入策略发送不期待响应的 Text/Binary 消息。
    pub async fn send_message_with_config(
        &self,
        body: WsBody,
        config: WSRequestConfig,
    ) -> Result<(), NetError> {
        on_common::log_t!(LogType::WSC; "send_message_with_config", "config|body_bytes", format!("{:?}", config), match &body { WsBody::Text(v) => v.len(), WsBody::Binary(v) => v.len() });
        let result: Result<(), NetError> =
            async { self.inner.send_untracked(body, config, false).await }.await;
        result.inspect_err(|error| {
            on_common::log_e!(LogType::WSC; "send_message_with_config", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    /// 同步、非阻塞地尝试入队一条普通单向消息。
    pub fn try_send_message(&self, body: WsBody) -> Result<(), NetError> {
        on_common::log_t!(LogType::WSC; "try_send_message", "body_bytes", match &body { WsBody::Text(v) => v.len(), WsBody::Binary(v) => v.len() });
        let result: Result<(), NetError> =
            self.try_send_message_with_config(body, WSRequestConfig::default());
        result.inspect_err(|error| {
            on_common::log_e!(LogType::WSC; "try_send_message", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    /// 使用指定策略同步、非阻塞地尝试入队一条普通单向消息。
    pub fn try_send_message_with_config(
        &self,
        body: WsBody,
        config: WSRequestConfig,
    ) -> Result<(), NetError> {
        on_common::log_t!(LogType::WSC; "try_send_message_with_config", "config|body_bytes", format!("{:?}", config), match &body { WsBody::Text(v) => v.len(), WsBody::Binary(v) => v.len() });
        let result: Result<(), NetError> = self.inner.try_send_untracked(body, config, false);
        result.inspect_err(|error| {
            on_common::log_e!(LogType::WSC; "try_send_message_with_config", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    /// 通过保留的应用层紧急通道发送一条不期待响应的完整消息。
    ///
    /// 适用于文件分片 ACK/NACK 等恢复控制数据。紧急消息会在当前完整 WebSocket
    /// 消息结束后优先于普通业务队列发送；它不是 RFC 控制帧，不能插入一条正在发送的
    /// 包含延续分片的消息。需要 ACK 与发送节奏控制的大负载仍应由应用协议拆成多条独立消息。
    pub async fn send_urgent_message(&self, body: WsBody) -> Result<(), NetError> {
        on_common::log_t!(LogType::WSC; "send_urgent_message", "body_bytes", match &body { WsBody::Text(v) => v.len(), WsBody::Binary(v) => v.len() });
        let result: Result<(), NetError> = async {
            self.send_urgent_message_with_config(body, WSRequestConfig::default())
                .await
        }
        .await;
        result.inspect_err(|error| {
            on_common::log_e!(LogType::WSC; "send_urgent_message", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    /// 使用指定调度/写入策略从应用层紧急通道发送单向消息。
    pub async fn send_urgent_message_with_config(
        &self,
        body: WsBody,
        config: WSRequestConfig,
    ) -> Result<(), NetError> {
        on_common::log_t!(LogType::WSC; "send_urgent_message_with_config", "config|body_bytes", format!("{:?}", config), match &body { WsBody::Text(v) => v.len(), WsBody::Binary(v) => v.len() });
        let result: Result<(), NetError> =
            async { self.inner.send_untracked(body, config, true).await }.await;
        result.inspect_err(|error| {
            on_common::log_e!(LogType::WSC; "send_urgent_message_with_config", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    /// 同步、非阻塞地尝试入队一条紧急单向消息。
    pub fn try_send_urgent_message(&self, body: WsBody) -> Result<(), NetError> {
        on_common::log_t!(LogType::WSC; "try_send_urgent_message", "body_bytes", match &body { WsBody::Text(v) => v.len(), WsBody::Binary(v) => v.len() });
        let result: Result<(), NetError> =
            self.try_send_urgent_message_with_config(body, WSRequestConfig::default());
        result.inspect_err(|error| {
            on_common::log_e!(LogType::WSC; "try_send_urgent_message", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    /// 使用指定策略同步、非阻塞地尝试入队一条紧急单向消息。
    pub fn try_send_urgent_message_with_config(
        &self,
        body: WsBody,
        config: WSRequestConfig,
    ) -> Result<(), NetError> {
        on_common::log_t!(LogType::WSC; "try_send_urgent_message_with_config", "config|body_bytes", format!("{:?}", config), match &body { WsBody::Text(v) => v.len(), WsBody::Binary(v) => v.len() });
        let result: Result<(), NetError> = self.inner.try_send_untracked(body, config, true);
        result.inspect_err(|error| {
            on_common::log_e!(LogType::WSC; "try_send_urgent_message_with_config", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    /// 关闭客户端后台工作任务。通常应通过 `OpenNet::destroy_ws_client` 调用。
    pub async fn shutdown(&self) -> Result<(), NetError> {
        on_common::log_t!(LogType::WSC; "shutdown");
        let result: Result<(), NetError> = async { self.inner.shutdown().await }.await;
        result.inspect_err(|error| {
            on_common::log_e!(LogType::WSC; "shutdown", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    /// 设置或替换 WebSocket 客户端模块及公共库的日志监听器，`None` 注销。
    ///
    /// 克隆句柄共享设置，不同客户端各自订阅。范围为进程内的 `WSC | Common`，
    /// 并非只包含本实例；HTTP、服务端及未分类日志不会交付。注册前日志不重放。
    /// 回调在独立操作系统线程执行，没有 Tokio 运行时；队列有界且过载可丢弃，恢复时
    /// 会交付带丢弃数量的状态日志。回调内同步产生的日志不再分发，以避免反馈循环。
    /// 关闭后不会再注册；注销不等待已经开始的回调，尚未交付的日志可被丢弃。
    /// 若线程资源不足则保留旧监听器；需要观察注册失败时使用 `try_set_log_listener`。
    pub fn set_log_listener(&self, listener: Option<LogListener>) {
        on_common::log_t!(LogType::WSC; "set_log_listener", "listener", listener.is_some());
        self.inner.set_log_listener(listener);
    }

    /// 与 `set_log_listener` 相同，但返回创建日志回调线程时的系统错误。
    pub fn try_set_log_listener(&self, listener: Option<LogListener>) -> std::io::Result<()> {
        on_common::log_t!(LogType::WSC; "try_set_log_listener", "listener", listener.is_some());
        self.inner.try_set_log_listener(listener)
    }
}

mod worker_state;
pub(crate) use worker_state::WSClientWorker;

use crate::module::ws_client::connection_status::ConnectionStatus;
use crate::ws::WebSocketClientConfig;

use crate::common::log::log_def::LogType;
use crate::common::platform::spawn;
use crate::error::NetError;
use crate::module::net_status::inner::inner_net_status_client::InnerNetStatusClient;
use crate::module::net_status::inner::network_status_snapshot::NetworkStatusSnapshot;
use crate::module::net_status::NetworkStatus;
use crate::module::transport::compiled_network_config::CompiledNetworkConfig;
use crate::module::transport::failure::{ConnectStage, ConnectionFailure};
use crate::module::ws_client::active_io::ActiveIo;
use crate::module::ws_client::callback_event::CallbackEvent;
use crate::module::ws_client::callback_executor::DataCallbackPool;
use crate::module::ws_client::client_command::ClientCommand;
use crate::module::ws_client::connect_target::ConnectTarget;
use crate::module::ws_client::connection_session::ConnectionSession;
use crate::module::ws_client::heartbeat_state::HeartbeatState;
use crate::module::ws_client::io_diagnostics::{
    ConnectionTerminationDetails, PeerCloseObservation,
};
use crate::module::ws_client::io_event::IoEvent;
use crate::module::ws_client::listener_store::ListenerStore;
use crate::module::ws_client::native_pending::NativePending;
use crate::module::ws_client::native_task_observer::NativeTaskObserver;
use crate::module::ws_client::read::read_loop_context::ReadLoopContext;
use crate::module::ws_client::session_runtime::SessionRuntime;
use crate::module::ws_client::write::control_message::ControlMessage;
use crate::module::ws_client::write::priority_write_queue::PriorityWriteQueue;
use crate::module::ws_client::write::write_loop_context::WriteLoopContext;
use crate::module::ws_client::ws_read::run_read_loop;
use crate::module::ws_client::ws_write::{fail_requests, run_write_loop};
use crate::ws::TerminationReason;
use crate::ws::{ConnectionId, TaskEndCause};
use crate::ws::{IoEndKind, PeerClose, RetryDecision};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use socket2::{SockRef, TcpKeepalive};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch, Notify, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::Request;
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

struct StoppedIo {
    peer_close: Option<PeerClose>,
    result: Result<(), NetError>,
}

mod session_close;

#[cfg(test)]
mod admission_budget_tests;
#[cfg(test)]
mod callback_budget_tests;
#[cfg(test)]
mod callback_dispatch_lifecycle_tests;
#[cfg(test)]
mod callback_dispatch_tests;
#[cfg(test)]
mod callback_pool_tests;
#[cfg(test)]
mod close_details_tests;
pub(super) mod connection_budget;
#[cfg(test)]
#[path = "data_subscription_executor_tests.rs"]
mod data_subscription_executor_tests;
#[cfg(test)]
mod journal_detach_tests;
#[cfg(test)]
mod observation_lifecycle_tests;
use connection_budget::{BudgetDeadline, ConnectionBudget, HandshakeAdmission};
mod context_connect;
#[cfg(test)]
mod data_listener_lifecycle_tests;
mod network_gate;
#[cfg(test)]
mod network_status_tests;
#[cfg(test)]
mod reconnect_recovery_tests;

impl WSClientWorker {
    /// 在调用线程上创建单线程 Tokio 运行时并运行工作器，直至客户端关闭。
    ///
    /// 创建者会在独立操作系统线程中调用本方法。运行时创建结果通过 `ready_tx` 同步返回：
    /// 创建失败时发送 [`ErrorKind::RuntimeUnavailable`] 并立即返回；成功时先发送 `Ok(())`，再阻塞
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
                let _ = ready_tx.send(Err(NetError::from(
                    crate::error::ErrorKind::RuntimeUnavailable,
                )));
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
    /// 的请求以 [`ErrorKind::Cancelled`] 完成；已被写任务取出却随任务中止而析构的请求
    /// 会由 `QueuedRequest` 的 drop fallback 保守报告 [`ErrorKind::DeliveryUnknown`]。
    /// 数据回调 lane 会在 response grace 的原截止时间前尝试排空；到期后不再等待，
    /// 剩余回调任务随 runtime 退出。
    async fn run_async(mut self) {
        crate::log_t!(LogType::WSC; "run_async", "generation", self.generation);
        if let Some(client) = self.net_status_client.clone() {
            let shutdown = self.shutdown.clone();
            spawn(async move {
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => {}
                    result = client.start() => {
                        if let Err(error) = &result {
                            crate::log_e!(LogType::WSC; "network_monitor_start", "error", format!("{error:?}"));
                        }
                    }
                }
            });
        }
        if let Some(callback_rx) = self.data_callback_rx.take() {
            spawn(data_callback_loop(
                callback_rx,
                self.config.dispatch.message_callback_workers,
                self.io_event_tx.clone(),
                self.shutdown.clone(),
                Arc::clone(&self.data_callback_pool),
            ));
        }
        let mut should_exit = false;
        let mut terminal_response_deadline = None;
        while !should_exit {
            let session_cancel = self.context_session().map(|session| session.cancel_token());
            let closing_session = self.context_session();
            let session_close = closing_session
                .as_ref()
                .map(|session| session.close_token());
            tokio::select! {
                biased;
                _ = self.shutdown.cancelled() => {
                    should_exit = self
                        .handle_command(
                            ClientCommand::Shutdown,
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
                    if let Some(session) = closing_session {
                        let now = Instant::now();
                        let deadline = now.checked_add(self.config.close_timeout).map_or(now, |deadline| deadline);
                        if let Err(error) = self.close_target_session(session, None, deadline).await {
                            crate::log_e!(LogType::WSC; "cancel_session", "error", format!("{error:?}"));
                        }
                    }
                }
                _ = async {
                    match session_close {
                        Some(token) => token.cancelled().await,
                        None => std::future::pending::<()>().await,
                    }
                } => {
                    if let Some(session) = closing_session {
                        match session.take_close_request() {
                            Ok(Some(request)) => {
                                if let Err(error) = self.close_target_session(
                                    session, request.frame, request.deadline,
                                ).await {
                                    crate::log_e!(LogType::WSC; "close_session", "error", format!("{error:?}"));
                                }
                            }
                            Ok(None) => { session.request_cancel(); },
                            Err(error) => {
                                crate::log_e!(LogType::WSC; "close_session", "error", format!("{error:?}"));
                                session.request_cancel();
                            }
                        }
                    }
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
        self.shutdown_reason.client_shutdown();
        self.shutdown.cancel();
        self.task_observers.close();
        let operation_error = match self.shutdown_failure() {
            Some(failure) => failure.error(),
            None => NetError::from(crate::error::ErrorKind::Cancelled),
        };
        self.end_runtime(operation_error.clone(), TaskEndCause::Shutdown);
        self.begin_session_closing();
        self.cancel_connect();
        let peer_close = self.stop_active_io(false).await;
        self.finish_context_session_with_details(
            self.shutdown_reason.selected(),
            self.shutdown_failure(),
            ConnectionTerminationDetails {
                peer_close,
                io_end_kind: None,
            },
        );
        self.connect_target = None;
        self.urgent_queue.close();
        self.queue.close();
        fail_requests(
            self.urgent_queue.drain_with_error(operation_error.clone()),
            operation_error.clone(),
        );
        fail_requests(
            self.queue.drain_with_error(operation_error.clone()),
            operation_error,
        );
        let terminal_response_deadline =
            terminal_response_deadline.or_else(|| self.response_dispatch_grace_deadline());
        self.drain_data_callbacks_until(terminal_response_deadline)
            .await;
        self.clear_listeners();
        if let Some(client) = self.net_status_client.take() {
            if let Err(error) = client.destroy().await {
                crate::log_e!(LogType::WSC; "network_monitor_destroy", "error", format!("{error:?}"));
            }
        }
        self.shutdown_complete.cancel();
    }

    /// 处理一条客户端控制命令。
    ///
    /// 连接命令会验证当前状态和重连参数，保存连接目标并启动初始连接周期；断开命令会
    /// 尝试发送关闭帧、清理请求后回到空闲状态；关闭命令则进入不可恢复的终态并请求
    /// 事件循环退出。状态回调及部分关闭
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
        crate::log_t!(LogType::WSC; "handle_command", "command|terminal_response_deadline", match &command { ClientCommand::Connect { .. } => "start_session", ClientCommand::CloseSession { .. } => "close_session", ClientCommand::Shutdown => "shutdown" }, terminal_response_deadline.is_some());
        match command {
            ClientCommand::Connect {
                options,
                initial_connect_deadline,
                session,
                runtime,
                reply,
            } => {
                let status = self.current_status();
                let admission = if status == ConnectionStatus::Closed {
                    Err(NetError::from(crate::error::ErrorKind::EngineDropped))
                } else if matches!(
                    status,
                    ConnectionStatus::Connecting
                        | ConnectionStatus::Connected
                        | ConnectionStatus::Reconnecting
                        | ConnectionStatus::Closing
                ) {
                    Err(NetError::from(
                        crate::error::ErrorKind::SessionAlreadyExists,
                    ))
                } else if initial_connect_deadline
                    .is_some_and(|deadline| Instant::now() >= deadline)
                {
                    Err(NetError::from(crate::error::ErrorKind::TimedOut)
                        .with_stage(crate::error::ErrorStage::Admission))
                } else if session.cancel_token().is_cancelled() {
                    Err(NetError::from(crate::error::ErrorKind::Cancelled))
                } else if self.network.network_status_policy()
                    == crate::api::network_config::NetworkStatusPolicy::PauseOnUnavailable
                    && match &options.reconnect {
                        crate::ws::ReconnectPolicy::Disabled => options.connect_timeout.is_none(),
                        crate::ws::ReconnectPolicy::Backoff(backoff) => {
                            backoff.max_elapsed.is_none()
                        }
                    }
                {
                    let field = match &options.reconnect {
                        crate::ws::ReconnectPolicy::Disabled => "connect_timeout",
                        crate::ws::ReconnectPolicy::Backoff(_) => "reconnect.max_elapsed",
                    };
                    Err(NetError::config(
                        field,
                        "must be finite while network availability pauses connection",
                    ))
                } else {
                    options.validate()
                }
                .and_then(|()| runtime.bind_worker_runtime());
                if let Err(error) = admission {
                    runtime.end(error.clone(), TaskEndCause::Failed);
                    if let Err(delivery_error) = session.terminate(
                        TerminationReason::ConnectFailed,
                        Some(ConnectionFailure::new(
                            error.clone(),
                            ConnectStage::RequestBuild,
                            None,
                            false,
                        )),
                    ) {
                        crate::log_e!(LogType::WSC; "connect_with_context", "stage|error", "admission_event", format!("{delivery_error:?}"));
                    }
                    let _ = reply.send(Err(error));
                    return false;
                }
                let mut options = options;
                options.url = options.url.trim().to_string();
                spawn(NativePending::run_timers(
                    Arc::downgrade(&runtime.pending),
                    runtime.shutdown.clone(),
                ));
                spawn(SessionRuntime::run_timers(Arc::downgrade(&runtime)));
                self.connect_target = Some(ConnectTarget {
                    options,
                    initial_connect_deadline,
                    session,
                    runtime,
                });
                self.start_connection(false).await;
                if reply.send(Ok(())).is_err() {
                    if let Some(session) = self.context_session() {
                        session.request_cancel();
                    }
                }
            }
            ClientCommand::CloseSession {
                session,
                frame,
                deadline,
            } => {
                if let Err(error) = self.close_target_session(session, frame, deadline).await {
                    crate::log_e!(LogType::WSC; "close_session", "error", format!("{error:?}"));
                }
            }
            ClientCommand::Shutdown => {
                self.shutdown_reason.client_shutdown();
                self.task_observers.close();
                let error = match self.shutdown_failure() {
                    Some(failure) => failure.error(),
                    None => NetError::from(crate::error::ErrorKind::Cancelled),
                };
                self.end_runtime(error.clone(), TaskEndCause::Shutdown);
                self.begin_session_closing();
                self.set_status(ConnectionStatus::Closing).await;
                self.cancel_connect();
                let peer_close = self.stop_active_io(true).await;
                self.urgent_queue.close();
                self.queue.close();
                fail_requests(
                    self.urgent_queue.drain_with_error(error.clone()),
                    error.clone(),
                );
                fail_requests(self.queue.drain_with_error(error.clone()), error);
                *terminal_response_deadline = self.response_dispatch_grace_deadline();
                self.finish_context_session_with_details(
                    self.shutdown_reason.selected(),
                    self.shutdown_failure(),
                    ConnectionTerminationDetails {
                        peer_close,
                        io_end_kind: None,
                    },
                );
                self.connect_target = None;
                self.set_status(ConnectionStatus::Closed).await;
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
                deadline,
                generation,
                attempt,
                reservation,
                accepted,
            } => {
                let result = self.connecting_context(generation).and_then(|session| {
                    if let Some(error) = deadline.expired_error() {
                        return Err(error);
                    }
                    session.begin_attempt(attempt, reservation)
                });
                self.acknowledge_context_event(result, accepted);
            }
            IoEvent::ContextPrepared {
                deadline,
                generation,
                attempt_id,
                credential_version,
                accepted,
            } => {
                let result = self.connecting_context(generation).and_then(|session| {
                    if let Some(error) = deadline.expired_error() {
                        return Err(error);
                    }
                    session.set_attempt_credential_version(
                        generation,
                        attempt_id,
                        credential_version,
                    )
                });
                self.acknowledge_context_event(result, accepted);
            }
            IoEvent::ContextAttemptFailed {
                generation,
                attempt_id,
                failure,
                diagnostic,
                retry,
                accepted,
            } => {
                let result = self.connecting_context(generation).and_then(|session| {
                    session.attempt_failed_with_diagnostic(
                        generation, attempt_id, failure, retry, diagnostic,
                    )
                });
                self.acknowledge_context_event(result, accepted);
            }
            IoEvent::ConnectSucceeded {
                admission,
                generation,
                stream,
                attempt_id,
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
                    let _ = accepted.send(Err(NetError::from(crate::error::ErrorKind::Cancelled)));
                    return;
                }
                if !self.network_epoch_is_current(network_loss_epoch) {
                    let _ = accepted.send(Err(NetError::from(crate::error::ErrorKind::Io)));
                    return;
                }
                {
                    let result = self.connecting_context(generation).and_then(|session| {
                        admission.try_accept()?;
                        session.prepare_established(generation, attempt_id)
                    });
                    if let Err(error) = &result {
                        crate::log_e!(LogType::WSC; "handle_io_event", "stage|error", "publish_established", format!("{error:?}"));
                        if !matches!(
                            error.kind(),
                            crate::error::ErrorKind::TimedOut
                                | crate::error::ErrorKind::RetryExhausted
                        ) {
                            if let Some(session) = self.context_session() {
                                session.request_cancel();
                            }
                        }
                        let _ = accepted.send(Err(error.clone()));
                        return;
                    }
                }
                if let Some(target) = self.connect_target.as_mut() {
                    target.initial_connect_deadline = None;
                }
                self.connect_cancel.take();
                self.connect_handle.take();
                let (write, read) = (*stream).split();
                let write_retirement = CancellationToken::new();
                let cancel_domain_gate =
                    Arc::new(crate::module::ws_client::network_io::DomainIoGate::default());
                let Some(runtime) = self.context_runtime() else {
                    let _ = accepted.send(Err(NetError::from(crate::error::ErrorKind::Closed)));
                    return;
                };
                if let Err(error) =
                    runtime.activate_connection(ConnectionId::from_allocated(generation))
                {
                    runtime.lifecycle.request_cancel();
                    let _ = accepted.send(Err(error));
                    return;
                }
                let session_cancel = runtime.lifecycle.cancel_token();
                let established_loss_epoch = network_loss_epoch.unwrap_or(self.network_loss_epoch);
                self.network_loss_epoch = established_loss_epoch;
                let write = crate::module::ws_client::network_io::NetworkAwareSink::new(
                    write,
                    self.network_status.clone(),
                    established_loss_epoch,
                )
                .with_session_cancel(&session_cancel)
                .with_write_retirement(&write_retirement)
                .with_cancel_domain_gate(cancel_domain_gate.clone());
                let read = crate::module::ws_client::network_io::NetworkAwareStream::new(
                    read,
                    self.network_status.clone(),
                    established_loss_epoch,
                )
                .with_session_cancel(&session_cancel)
                .with_write_retirement(&write_retirement)
                .with_cancel_domain_gate(cancel_domain_gate.clone());
                let (control_tx, control_rx) = mpsc::channel(32);
                let cancel = session_cancel.child_token();
                let heartbeat = Arc::new(HeartbeatState::new(generation));
                let peer_close = Arc::new(PeerCloseObservation::new());
                let read_handle = spawn(run_read_loop(
                    read,
                    ReadLoopContext {
                        peer_close: Arc::clone(&peer_close),
                        control_tx: control_tx.clone(),
                        runtime: Arc::clone(&runtime),
                        io_event_tx: self.io_event_tx.clone(),
                        generation,
                        cancel: cancel.clone(),
                        heartbeat: Arc::clone(&heartbeat),
                    },
                ));
                let write_handle = spawn(run_write_loop(
                    write,
                    WriteLoopContext {
                        cancel_domain_gate: Some(cancel_domain_gate),
                        queue: Arc::clone(&self.queue),
                        urgent_queue: Arc::clone(&self.urgent_queue),
                        control_rx,
                        pending_requests: Arc::clone(&runtime.pending),
                        io_event_tx: self.io_event_tx.clone(),
                        generation,
                        cancel: cancel.clone(),
                        data_frame_payload_size: self.config.frames.data_frame_payload_size,
                        control_write_timeout: self.config.frames.control_write_timeout,
                        data_frame_write_timeout: self.config.frames.data_frame_write_timeout,
                        heartbeat_config: self.config.heartbeat.clone(),
                        heartbeat,
                    },
                    write_retirement,
                ));
                self.active_io = Some(ActiveIo {
                    peer_close,
                    generation,
                    cancel,
                    control_tx,
                    read_handle,
                    write_handle,
                });
                self.set_status(ConnectionStatus::Connected).await;
                {
                    if let Some(session) = self.context_session() {
                        if let Err(error) = session.commit_established(generation, attempt_id) {
                            crate::log_e!(LogType::WSC; "handle_io_event", "stage|error", "commit_established", format!("{error:?}"));
                            session.request_cancel();
                        }
                    }
                }
                let _ = accepted.send(Ok(()));
            }
            IoEvent::ConnectFailed {
                generation,
                failure,
                reason,
            } => {
                let error = failure.error();
                // Keep the public snapshot limited to WebSocket Upgrade responses.
                // Proxy status and local failures must not masquerade as endpoint HTTP errors.
                let http_status = if error.kind() == crate::error::ErrorKind::HandshakeRejected
                    && failure.stage() == ConnectStage::WebSocketUpgrade
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
                self.end_runtime(error.clone(), TaskEndCause::Failed);
                self.set_status(ConnectionStatus::Disconnected).await;
                fail_requests(
                    self.urgent_queue.drain_with_error(error.clone()),
                    error.clone(),
                );
                fail_requests(self.queue.drain_with_error(error.clone()), error);
                self.finish_context_session(reason, Some(failure));
                self.connect_target = None;
            }
            IoEvent::CallbackDispatchFailed { session_id } => {
                if !self
                    .context_runtime()
                    .is_some_and(|runtime| runtime.id == session_id)
                    || self.current_status() != ConnectionStatus::Connected
                {
                    crate::log_s!(LogType::WSC; "handle_io_event", "state|event_generation|current_generation", "stale_callback_failure_ignored", session_id.as_u64(), self.generation);
                    return;
                }
                self.connection_lost_at_stage(
                    NetError::from(crate::error::ErrorKind::Internal),
                    TerminationReason::IoFailure,
                    ConnectStage::EventDelivery,
                    None,
                )
                .await;
            }
            IoEvent::ReadEnded {
                generation,
                error,
                kind,
            }
            | IoEvent::WriteEnded {
                generation,
                error,
                kind,
            } => {
                crate::log_s!(LogType::WSC; "handle_io_event", "event|generation|error", "io_ended", generation, format!("{error:?}"));
                if generation != self.generation
                    || self.current_status() != ConnectionStatus::Connected
                {
                    crate::log_s!(LogType::WSC; "handle_io_event", "state|event_generation|current_generation", "stale_io_event_ignored", generation, self.generation);
                    return;
                }
                self.connection_lost_at_stage(
                    error,
                    TerminationReason::IoFailure,
                    ConnectStage::WebSocketIo,
                    Some(kind),
                )
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
                    NetError::from(crate::error::ErrorKind::Io),
                    TerminationReason::NetworkUnavailable,
                )
                .await;
            }
        }
    }

    async fn connection_lost(&mut self, error: NetError, reason: TerminationReason) {
        let stage = if reason == TerminationReason::NetworkUnavailable {
            ConnectStage::EventDelivery
        } else {
            ConnectStage::WebSocketIo
        };
        self.connection_lost_at_stage(error, reason, stage, None)
            .await;
    }

    async fn connection_lost_at_stage(
        &mut self,
        error: NetError,
        reason: TerminationReason,
        stage: ConnectStage,
        io_end_kind: Option<IoEndKind>,
    ) {
        let peer_close = self.stop_active_io(false).await;
        let normal_peer_close = io_end_kind == Some(IoEndKind::PeerClose)
            && peer_close
                .as_ref()
                .is_some_and(|close| matches!(close.code, None | Some(1000 | 1001)));
        let reason = if normal_peer_close {
            TerminationReason::PeerClose
        } else {
            reason
        };
        let failure =
            (!normal_peer_close).then(|| ConnectionFailure::new(error.clone(), stage, None, false));
        let next_cycle = if self.connect_target.as_ref().is_some_and(|target| {
            matches!(
                target.options.reconnect,
                crate::ws::ReconnectPolicy::Backoff(_)
            )
        }) {
            self.generation.checked_add(1)
        } else {
            None
        };
        if let Some(session) = self.context_session() {
            if let Err(event_error) = session.connection_terminated_with_details(
                reason,
                failure.clone(),
                ConnectionTerminationDetails {
                    peer_close,
                    io_end_kind,
                },
                next_cycle,
            ) {
                crate::log_e!(LogType::WSC; "handle_io_event", "stage|error", "publish_connection_terminated", format!("{event_error:?}"));
                session.request_cancel();
            }
        }
        if let Some(runtime) = self.context_runtime() {
            if let Err(failure) = runtime
                .connection_ended(ConnectionId::from_allocated(self.generation), error.clone())
            {
                crate::log_e!(LogType::WSC; "session_connection_ended", "error", format!("{failure:?}"));
                runtime.lifecycle.request_cancel();
            }
        }
        fail_requests(
            self.urgent_queue
                .drain_rejected_on_disconnect_with_error(error.clone()),
            error.clone(),
        );
        fail_requests(
            self.queue
                .drain_rejected_on_disconnect_with_error(error.clone()),
            error.clone(),
        );
        if self.connect_target.as_ref().is_some_and(|target| {
            matches!(
                target.options.reconnect,
                crate::ws::ReconnectPolicy::Backoff(_)
            )
        }) {
            self.start_connection(true).await;
        } else {
            self.end_runtime(error.clone(), TaskEndCause::Disconnected);
            self.set_status(ConnectionStatus::Disconnected).await;
            fail_requests(
                self.urgent_queue.drain_with_error(error.clone()),
                error.clone(),
            );
            fail_requests(self.queue.drain_with_error(error.clone()), error.clone());
            self.finish_context_session(reason, failure);
            self.connect_target = None;
        }
    }

    fn context_session(&self) -> Option<Arc<ConnectionSession>> {
        self.connect_target
            .as_ref()
            .map(|target| Arc::clone(&target.session))
    }

    fn begin_session_closing(&self) {
        if let Some(session) = self.context_session() {
            if let Err(error) = session.begin_closing() {
                crate::log_e!(LogType::WSC; "connection_state_closing", "kind", format!("{:?}", error.kind()));
            }
        }
    }

    fn context_runtime(&self) -> Option<Arc<SessionRuntime>> {
        self.connect_target
            .as_ref()
            .map(|target| Arc::clone(&target.runtime))
    }

    fn end_runtime(&self, error: NetError, cause: TaskEndCause) {
        if let Some(runtime) = self.context_runtime() {
            runtime.end(error, cause);
        }
    }

    fn connecting_context(&self, generation: u64) -> Result<Arc<ConnectionSession>, NetError> {
        if generation != self.generation
            || !matches!(
                self.current_status(),
                ConnectionStatus::Connecting | ConnectionStatus::Reconnecting
            )
        {
            return Err(NetError::from(crate::error::ErrorKind::Cancelled));
        }
        let session = self
            .context_session()
            .ok_or(NetError::from(crate::error::ErrorKind::Cancelled))?;
        if session.cancel_token().is_cancelled() {
            return Err(NetError::from(crate::error::ErrorKind::Cancelled));
        }
        Ok(session)
    }

    fn acknowledge_context_event(
        &self,
        result: Result<(), NetError>,
        accepted: oneshot::Sender<Result<(), NetError>>,
    ) {
        if let Err(error) = &result {
            crate::log_e!(LogType::WSC; "acknowledge_context_event", "error", format!("{error:?}"));
            // Stale events must not revoke a later session. Other internal publication failures
            // fail closed, and the worker cancellation branch performs cleanup.
            if !matches!(
                error.kind(),
                crate::error::ErrorKind::Cancelled
                    | crate::error::ErrorKind::TimedOut
                    | crate::error::ErrorKind::RetryExhausted
            ) {
                if let Some(session) = self.context_session() {
                    session.request_cancel();
                }
            }
        }
        let _ = accepted.send(result);
    }

    fn shutdown_failure(&self) -> Option<ConnectionFailure> {
        (self.shutdown_reason.selected() == TerminationReason::EngineDropped).then(|| {
            ConnectionFailure::new(
                NetError::from(crate::error::ErrorKind::EngineDropped),
                ConnectStage::EventDelivery,
                None,
                false,
            )
        })
    }

    fn cancellation_failure() -> ConnectionFailure {
        ConnectionFailure::new(
            NetError::from(crate::error::ErrorKind::Cancelled),
            ConnectStage::EventDelivery,
            None,
            false,
        )
    }

    fn finish_context_session(
        &self,
        reason: TerminationReason,
        failure: Option<ConnectionFailure>,
    ) {
        self.finish_context_session_with_details(
            reason,
            failure,
            ConnectionTerminationDetails::default(),
        );
    }

    fn finish_context_session_with_details(
        &self,
        reason: TerminationReason,
        failure: Option<ConnectionFailure>,
        details: ConnectionTerminationDetails,
    ) {
        if let Some(session) = self.context_session() {
            if let Err(error) = session.terminate_with_details(reason, failure, details) {
                crate::log_e!(LogType::WSC; "finish_context_session", "error", format!("{error:?}"));
            }
        }
    }

    /// 为已保存的目标启动一个新的连接及重试周期。
    ///
    /// 若没有连接目标则直接返回。启动前会取消上一连接任务，随后
    /// 递增非零 `generation`、建立新的取消令牌，将状态设为 `Connecting` 或
    /// `Reconnecting`，并派生连接会话任务。该方法只负责启动，不等待握手
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
        let max_elapsed = match &target.options.reconnect {
            crate::ws::ReconnectPolicy::Disabled => None,
            crate::ws::ReconnectPolicy::Backoff(backoff) => backoff.max_elapsed,
        };
        let budget = match ConnectionBudget::new(
            Instant::now(),
            target.initial_connect_deadline,
            max_elapsed,
        ) {
            Ok(budget) => budget,
            Err(error) => {
                self.end_runtime(error.clone(), TaskEndCause::Failed);
                self.set_status(ConnectionStatus::Disconnected).await;
                self.finish_context_session(
                    TerminationReason::ConnectFailed,
                    Some(ConnectionFailure::new(
                        error,
                        ConnectStage::RequestBuild,
                        None,
                        false,
                    )),
                );
                self.connect_target = None;
                return;
            }
        };
        self.cancel_connect();
        {
            let Some(generation) = self.generation.checked_add(1) else {
                self.end_runtime(
                    NetError::from(crate::error::ErrorKind::ResourceExhausted),
                    TaskEndCause::Failed,
                );
                self.set_status(ConnectionStatus::Disconnected).await;
                self.finish_context_session(
                    TerminationReason::ConnectFailed,
                    Some(ConnectionFailure::new(
                        NetError::from(crate::error::ErrorKind::ResourceExhausted),
                        ConnectStage::EventDelivery,
                        None,
                        false,
                    )),
                );
                self.connect_target = None;
                return;
            };
            self.generation = generation;
        }
        let generation = self.generation;
        if let Err(error) = target.session.begin_cycle(generation, reconnecting) {
            crate::log_e!(LogType::WSC; "connection_state_cycle", "cycle|kind", generation, format!("{:?}", error.kind()));
        }
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
            context_connect::ContextConnectTask {
                target,
                budget,
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
        }));
    }

    /// 停止异步建连任务；实际执行中的 provider 继续持有额度直到退出。
    fn cancel_connect(&mut self) {
        crate::log_t!(LogType::WSC; "cancel_connect", "generation", self.generation);
        if let Some(cancel) = self.connect_cancel.take() {
            crate::log_s!(LogType::WSC; "cancel_connect", "state|generation", "connection_attempt_cancelled", self.generation);
            cancel.cancel();
        }
        if let Some(handle) = self.connect_handle.take() {
            handle.abort();
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
    async fn stop_active_io(&mut self, graceful: bool) -> Option<PeerClose> {
        let stopped = self.stop_active_io_with_close(graceful, None, None).await;
        if let Err(error) = stopped.result {
            crate::log_e!(LogType::WSC; "stop_active_io", "error", format!("{error:?}"));
        }
        stopped.peer_close
    }

    async fn stop_active_io_with_close(
        &mut self,
        graceful: bool,
        frame: Option<crate::ws::CloseFrame>,
        requested_deadline: Option<Instant>,
    ) -> StoppedIo {
        crate::log_t!(LogType::WSC; "stop_active_io", "graceful|generation", graceful, self.generation);
        let Some(active) = self.active_io.take() else {
            crate::log_s!(LogType::WSC; "stop_active_io", "state", "no_active_io");
            return StoppedIo {
                peer_close: None,
                result: Ok(()),
            };
        };
        let ActiveIo {
            peer_close,
            generation,
            cancel,
            control_tx,
            mut read_handle,
            mut write_handle,
        } = active;
        let started_at = Instant::now();
        let mut result = Ok(());
        let deadline = match requested_deadline
            .or_else(|| started_at.checked_add(self.config.close_timeout))
        {
            Some(deadline) => deadline,
            None => {
                result = Err(NetError::config(
                    "close_timeout",
                    "cannot represent a close deadline",
                )
                .with_stage(crate::error::ErrorStage::Close));
                started_at
            }
        };
        let cancel_grace = (self.config.close_timeout / 2)
            .min(MAX_COOPERATIVE_IO_CANCEL_GRACE)
            .max(Duration::from_nanos(1));
        let graceful_deadline = match deadline.checked_sub(cancel_grace) {
            Some(deadline) => deadline,
            None => deadline,
        };

        if graceful && generation == self.generation && result.is_ok() {
            // Keep a real tail budget for cancellation cleanup even when a data-frame
            // write prevents the writer from observing this command in time.
            let graceful_result = tokio::time::timeout_at(graceful_deadline, async {
                let (done_tx, done_rx) = oneshot::channel();
                control_tx
                    .send(ControlMessage::CloseWith {
                        frame,
                        deadline: graceful_deadline,
                        reply: done_tx,
                    })
                    .await
                    .map_err(|_| {
                        NetError::from(crate::error::ErrorKind::Closed)
                            .with_stage(crate::error::ErrorStage::Close)
                    })?;
                done_rx.await.map_err(|_| {
                    NetError::from(crate::error::ErrorKind::Closed)
                        .with_stage(crate::error::ErrorStage::Close)
                })?
            })
            .await;
            match graceful_result {
                Ok(Ok(())) => {
                    crate::log_s!(LogType::WSC; "stop_active_io", "state|generation", "close_write_acknowledged", generation)
                }
                Ok(Err(error)) => {
                    crate::log_s!(LogType::WSC; "stop_active_io", "state|generation|error", "close_write_failed", generation, format!("{error:?}"));
                    result = Err(error);
                }
                Err(_) => {
                    crate::log_s!(LogType::WSC; "stop_active_io", "state|generation", "graceful_close_timed_out", generation);
                    result = Err(NetError::from(crate::error::ErrorKind::TimedOut)
                        .with_stage(crate::error::ErrorStage::Close));
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
                return Err(NetError::with_source(crate::error::ErrorKind::Internal, error)
                    .with_stage(crate::error::ErrorStage::Close));
            }
            if let Err(error) = write_result {
                crate::log_e!(LogType::WSC; "stop_active_io", "task|error", "write", crate::common::log::summary::error(&error));
                return Err(NetError::with_source(crate::error::ErrorKind::Internal, error)
                    .with_stage(crate::error::ErrorStage::Close));
            }
            Ok(())
        })
        .await;
        if joined.is_err() {
            crate::log_s!(LogType::WSC; "stop_active_io", "state|generation", "cooperative_stop_timed_out_aborting_io", generation);
            read_handle.abort();
            write_handle.abort();
            let _ = tokio::join!(read_handle, write_handle);
            if result.is_ok() {
                result = Err(NetError::from(crate::error::ErrorKind::TimedOut)
                    .with_stage(crate::error::ErrorStage::Close));
            }
        } else if result.is_ok() {
            if let Ok(join_result) = joined {
                result = join_result;
            }
        }
        let peer_close = if generation == self.generation {
            peer_close.get().cloned()
        } else {
            None
        };
        StoppedIo { peer_close, result }
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

    /// Breaks listener/client reference cycles once the client reaches terminal shutdown.
    fn clear_listeners(&self) {
        crate::log_t!(LogType::WSC; "clear_listeners");
        if let Err(error) = self.listeners.clear() {
            crate::log_e!(LogType::WSC; "clear_listeners", "error", format!("{error:?}"));
        }
        crate::log_s!(LogType::WSC; "clear_listeners", "state", "terminal_listener_cleanup_finished");
    }

    /// 返回终态响应分发宽限的绝对截止时间；零宽限或不可表示的 deadline 不等待。
    fn response_dispatch_grace_deadline(&self) -> Option<Instant> {
        crate::log_t!(LogType::WSC; "response_dispatch_grace_deadline");
        (!self.config.requests.manual_response_grace.is_zero())
            .then(|| Instant::now().checked_add(self.config.requests.manual_response_grace))
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

    /// 按状态机约束更新 worker 内部状态，用于过滤迟到的传输事件。
    ///
    /// 相同状态不重复更新；非法转换和锁中毒记录错误日志。
    /// 对外状态与事件由对应的 ConnectionSession 发布。
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
/// reader 在接收完整消息时取得订阅快照，之后注册的新订阅不会收到旧消息。
/// 显式注销撤销尚未执行的投递，已经执行的回调继续结束。每个监听器调用由 `catch_unwind`
/// 隔离；在 `panic=unwind` 构建中，可展开的 panic 不会终止回调循环，但 `panic=abort`
/// 无法被捕获。事件按通道顺序出队，但并发值大于 1 时，操作系统调度不保证用户回调的
/// 可观察执行或结束顺序；严格有序场景须把并发设为 1。并发数达到配置上限后才暂停
/// 出队。多个生产者向通道发送事件时，其跨生产者先后顺序仍由通道实际入队顺序决定。
///
/// # 参数
///
/// - `callback_rx`：数据回调事件的唯一接收端。
/// - `max_concurrency`：同时运行的数据监听器调用数。
fn data_callback_loop(
    callback_rx: mpsc::Receiver<CallbackEvent>,
    max_concurrency: usize,
    io_event_tx: mpsc::Sender<IoEvent>,
    shutdown: CancellationToken,
    pool: Arc<DataCallbackPool>,
) -> impl std::future::Future<Output = ()> + Send {
    let retirement = DataPoolRetirement(Arc::clone(&pool));
    async move {
        let _retirement = retirement;
        let asynchronous_failures = io_event_tx.clone();
        let dispatch_shutdown = shutdown.clone();
        data_callback_loop_with_session_dispatch(
            callback_rx,
            max_concurrency,
            io_event_tx,
            shutdown,
            move |session_id, callback| {
                let completed = pool.dispatch(callback)?;
                let failures = asynchronous_failures.clone();
                let shutdown = dispatch_shutdown.clone();
                Ok(async move {
                    if let Err(error) = completed.await {
                        report_callback_dispatch_failure(&failures, &shutdown, session_id, error)
                            .await;
                    }
                })
            },
        )
        .await;
    }
}

struct DataPoolRetirement(Arc<DataCallbackPool>);

impl Drop for DataPoolRetirement {
    fn drop(&mut self) {
        self.0.close();
    }
}

/// Injection is local to this dispatcher; tests never change process-wide thread creation.
#[cfg(test)]
async fn data_callback_loop_with_dispatch<D, F>(
    callback_rx: mpsc::Receiver<CallbackEvent>,
    max_concurrency: usize,
    io_event_tx: mpsc::Sender<IoEvent>,
    shutdown: CancellationToken,
    mut dispatch: D,
) where
    D: FnMut(Box<dyn FnOnce() + Send>) -> std::io::Result<F>,
    F: std::future::Future<Output = ()> + Send,
{
    data_callback_loop_with_session_dispatch(
        callback_rx,
        max_concurrency,
        io_event_tx,
        shutdown,
        move |_, callback| dispatch(callback),
    )
    .await;
}

async fn data_callback_loop_with_session_dispatch<D, F>(
    mut callback_rx: mpsc::Receiver<CallbackEvent>,
    max_concurrency: usize,
    io_event_tx: mpsc::Sender<IoEvent>,
    shutdown: CancellationToken,
    mut dispatch: D,
) where
    D: FnMut(crate::ws::SessionId, Box<dyn FnOnce() + Send>) -> std::io::Result<F>,
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
            CallbackEvent::SessionJob { session_id, job } => match dispatch(session_id, job) {
                Ok(callback_done) => in_flight.push(callback_done),
                Err(error) => {
                    report_callback_dispatch_failure(&io_event_tx, &shutdown, session_id, error)
                        .await;
                }
            },
            CallbackEvent::DataDrain { delivered } => {
                while in_flight.next().await.is_some() {}
                let _ = delivered.send(());
                break;
            }
        }
    }
    while in_flight.next().await.is_some() {}
}

async fn report_callback_dispatch_failure(
    events: &mpsc::Sender<IoEvent>,
    shutdown: &CancellationToken,
    session_id: crate::ws::SessionId,
    error: std::io::Error,
) {
    crate::log_e!(LogType::WSC; "data_callback_loop", "stage|session|error", "subscription_dispatch_failed", session_id.as_u64(), crate::common::log::summary::error(&error));
    tokio::select! {
        biased;
        _ = shutdown.cancelled() => {}
        sent = events.send(IoEvent::CallbackDispatchFailed { session_id }) => {
            if sent.is_err() {
                crate::log_e!(LogType::WSC; "data_callback_loop", "error", "callback_failure_receiver_closed");
            }
        }
    }
}

/// 网络门禁中断的错误分类，不沿用先前握手的 HTTP 状态。
fn network_interruption_failure(error: NetError) -> ConnectionFailure {
    ConnectionFailure::new(
        error.clone(),
        ConnectStage::EventDelivery,
        None,
        error.kind() == crate::error::ErrorKind::Io,
    )
}

async fn publish_connected(
    events: &mpsc::Sender<IoEvent>,
    generation: u64,
    attempt_id: u64,
    stream: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    network_loss_epoch: Option<u64>,
    deadline: BudgetDeadline,
) -> Result<(), NetError> {
    let slot = tokio::time::timeout_at(deadline.at(), events.reserve())
        .await
        .map_err(|_| deadline.error())?
        .map_err(|_| NetError::from(crate::error::ErrorKind::EngineDropped))?;
    if let Some(error) = deadline.expired_error() {
        return Err(error);
    }
    let admission = Arc::new(HandshakeAdmission::new(deadline));
    let (accepted, mut received) = oneshot::channel();
    slot.send(IoEvent::ConnectSucceeded {
        generation,
        attempt_id,
        stream: Box::new(stream),
        network_loss_epoch,
        admission: Arc::clone(&admission),
        accepted,
    });
    tokio::select! {
        biased;
        result = &mut received => result.map_err(|_| NetError::from(crate::error::ErrorKind::EngineDropped))?,
        _ = tokio::time::sleep_until(deadline.at()) => {
            if admission.expire() {
                Err(deadline.error())
            } else {
                // The worker already selected success within the budget. Its original
                // prepare/commit sequence owns completion and cannot be overwritten.
                received.await.map_err(|_| NetError::from(crate::error::ErrorKind::EngineDropped))?
            }
        }
    }
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
    config: Option<&crate::ws::TcpKeepaliveConfig>,
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
fn retry_allowed(policy: &crate::ws::ReconnectPolicy, attempt: usize, elapsed: Duration) -> bool {
    let crate::ws::ReconnectPolicy::Backoff(policy) = policy else {
        return false;
    };
    crate::log_t!(LogType::WSC; "retry_allowed", "attempt|elapsed_seconds|max_retries", attempt, elapsed.as_secs_f64(), policy.max_retries);
    if attempt >= policy.max_retries {
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
fn full_jitter_delay(policy: &crate::ws::ReconnectPolicy, attempt: usize) -> Duration {
    let crate::ws::ReconnectPolicy::Backoff(policy) = policy else {
        return Duration::ZERO;
    };
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
mod tests;

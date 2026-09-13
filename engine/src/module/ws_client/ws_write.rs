use crate::api::net_error::NetError;
use crate::api::wsc::wsc_response::PendingRequestView;
use crate::module::ws_client::heartbeat_state::{HeartbeatState, HeartbeatTick};
use crate::module::ws_client::io_event::IoEvent;
use crate::module::ws_client::write::control_message::ControlMessage;
use crate::module::ws_client::write::priority_write_queue::PriorityWriteQueue;
use crate::module::ws_client::write::queued_request::{DispatchPhase, QueuedRequest};
use crate::module::ws_client::write::write_loop_context::WriteLoopContext;
use bytes::Bytes;
use futures::{FutureExt, Sink, SinkExt};
use on_common::log::log_def::LogType;
use on_common::platform::spawn;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{Instant, Interval, MissedTickBehavior};
use tokio_tungstenite::tungstenite::protocol::frame::{
    coding::{Data as OpData, OpCode},
    Frame,
};
use tokio_tungstenite::tungstenite::{Error as WsError, Message, Utf8Bytes};
use tokio_util::sync::CancellationToken;

/// 防止持续就绪的自动 Pong/Close flush 在一个安全边界内无限占用 writer。
///
/// 到达上限后，调用方会重新检查请求 deadline 与 heartbeat，并至少推进一个
/// data frame；主循环在请求出队前已经处理的控制也会从首帧额度中扣除，避免
/// 两段控制处理叠加成双倍窗口。控制通道本身仍保持高优先级，下一帧边界会
/// 继续处理剩余项。
const MAX_READY_CONTROLS_PER_BOUNDARY: usize = 16;

/// 驱动一个连接代次的 WebSocket 写半部，直至取消、关闭或发生 I/O 错误。
///
/// 循环以“取消、控制指令、心跳、业务请求”的有偏顺序选择就绪事件。大业务消息
/// 按配置拆成一个首帧和若干 continuation frame；同一逻辑消息的数据帧不会与
/// 其他业务消息交错，但每个数据帧写入前都会处理有界批次的就绪控制指令及
/// 已到期的心跳。这样不能抢占正在执行的单帧写入，却能把控制帧最坏等待窗口
/// 限制在一个 data frame 的有界写入时间内。每个成功 data frame 后还会主动让出
/// 一次调度权，让刚就绪的读循环有机会把 Pong/Close 指令送入控制通道。
///
/// 控制指令负责响应对端 Ping 及执行主动关闭。心跳始终只保留一个 outstanding
/// Ping，且每次使用连接代与递增序号组成的非空唯一 payload；只有读循环收到完全
/// 匹配的 Pong 才会确认该 probe。`pong_timeout` 从 Ping 成功写完时开始，而正在
/// 写入的 Ping 由 `control_write_timeout` 约束。心跳 interval 使用
/// `MissedTickBehavior::Skip`，长时间阻塞后不会集中补发 Ping。
///
/// 业务请求按队列优先级写入。一次逻辑消息从开始处理起共享同一个
/// `write_timeout` deadline；每个 data frame 还受独立的 `data_frame_write_timeout`
/// 限制，实际采用两者中更早的 deadline。任一 data frame 超时都会返回
/// `NetError::DeliveryUnknown` 并终止当前连接，因为无法判断该帧有多少数据已进入
/// 底层。非取消错误在
/// 请求声明幂等且仍有重试额度时，会保留原容量许可和 FIFO 序号重新入队；
/// 随后写循环报告连接错误并退出，由工作线程决定是否重连并继续重试。
/// 写入成功仅表示 sink 接受了消息。待响应记录在成功标记为等待响应后会启动
/// `response_timeout` 清理，也可更早被响应监听器认领或被断线/关闭清理。响应可能
/// 在 sink 发送返回后立即被取走，先于写循环更新状态；此时不会启动超时
/// 任务，但网络写入仍以成功完成。若写入前因记录缺失、token 不匹配或锁中毒而
/// 无法标记为写入中，请求不会上网，而是以 `NetError::Cancelled` 完成。
///
/// 收到主动关闭指令、取消令牌触发、控制通道关闭，或业务队列关闭且已经排空时，
/// 当前循环会不发送 `WriteEnded` 而结束；需要驱动连接状态变化的非取消 I/O
/// 故障会携带 `generation` 发送 `IoEvent::WriteEnded`。
///
/// Ping、Pong、Close 写入及 sink 关闭受 `control_write_timeout` 限制，也会观察
/// 取消令牌；控制写入超时以 `DeliveryUnknown` 报告并废弃当前连接。控制指令不会
/// 取消一个已经开始的 data frame 写入，以免继续复用可能只写出半帧的 sink。
pub(crate) async fn run_write_loop<W>(
    mut write: W,
    context: WriteLoopContext,
    write_retirement: CancellationToken,
) where
    W: Sink<Message, Error = WsError> + Unpin + Send + 'static,
{
    on_common::log_t!(LogType::WSC; "run_write_loop", "generation|data_frame_payload_size", context.generation, context.data_frame_payload_size);
    let WriteLoopContext {
        queue,
        urgent_queue,
        mut control_rx,
        pending_requests,
        io_event_tx,
        generation,
        cancel,
        data_frame_payload_size,
        control_write_timeout,
        data_frame_write_timeout,
        heartbeat_interval,
        pong_timeout,
        response_dispatch_grace,
        heartbeat: heartbeat_state,
    } = context;
    let Some(start) = Instant::now().checked_add(heartbeat_interval) else {
        send_write_end(&io_event_tx, generation, NetError::ConfigError, &cancel).await;
        return;
    };
    let mut heartbeat = tokio::time::interval_at(start, heartbeat_interval);
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut consecutive_controls = 0usize;
    loop {
        // `select!` intentionally prioritizes control traffic, but a peer can keep that
        // branch continuously ready. Re-check before every selection so a Ping flood
        // cannot starve the independent matching-Pong deadline indefinitely.
        if cancel.is_cancelled() {
            on_common::log_s!(LogType::WSC; "run_write_loop", "state|generation", "write_cancelled", generation);
            return;
        }
        if heartbeat_state.is_timed_out(Instant::now()) {
            // Tungstenite queues the peer-Close reply while reading the Close frame and then
            // rejects later Ping writes. Give one already-ready control a chance to flush that
            // reply before retiring the connection for a simultaneous Pong timeout.
            match try_handle_ready_control(
                &mut write,
                &mut control_rx,
                control_write_timeout,
                &cancel,
            )
            .await
            {
                Some(ControlAction::Continue) => {
                    consecutive_controls = consecutive_controls.saturating_add(1);
                }
                Some(ControlAction::Stop(_)) => return,
                Some(ControlAction::Failed {
                    error: NetError::Cancelled,
                    ..
                }) => return,
                Some(ControlAction::Failed { error, .. }) => {
                    send_write_end(&io_event_tx, generation, error, &cancel).await;
                    return;
                }
                None => {}
            }
            // A matching Pong may have raced with the ready control above.
            if heartbeat_state.is_timed_out(Instant::now()) {
                send_write_end(
                    &io_event_tx,
                    generation,
                    NetError::SocketRecvTimeout,
                    &cancel,
                )
                .await;
                return;
            }
        }
        // Consume a due tick before the biased selection, but first service one ready control.
        // This preserves the peer-Close handshake without allowing a control stream to reset the
        // bounded fairness counter: after the heartbeat, the next iteration still forces a ready
        // business request once the shared control budget has been reached.
        if heartbeat.tick().now_or_never().is_some() {
            match try_handle_ready_control(
                &mut write,
                &mut control_rx,
                control_write_timeout,
                &cancel,
            )
            .await
            {
                Some(ControlAction::Continue) => {
                    consecutive_controls = consecutive_controls.saturating_add(1);
                }
                Some(ControlAction::Stop(_)) => return,
                Some(ControlAction::Failed {
                    error: NetError::Cancelled,
                    ..
                }) => return,
                Some(ControlAction::Failed { error, .. }) => {
                    send_write_end(&io_event_tx, generation, error, &cancel).await;
                    return;
                }
                None => {}
            }
            if let Err(error) = handle_heartbeat_tick(
                &mut write,
                &heartbeat_state,
                pong_timeout,
                control_write_timeout,
                &cancel,
            )
            .await
            {
                if error != NetError::Cancelled {
                    send_write_end(&io_event_tx, generation, error, &cancel).await;
                }
                return;
            }
            continue;
        }
        // Controls normally win the biased selection. After a bounded consecutive burst,
        // synchronously take one already-ready business item (urgent first) so a peer that
        // continuously sends Ping cannot starve the application queue forever.
        let fairness_request = (consecutive_controls >= MAX_READY_CONTROLS_PER_BOUNDARY)
            .then(|| {
                urgent_queue
                    .try_next()
                    .map(|request| (request, Arc::clone(&urgent_queue)))
                    .or_else(|| {
                        queue
                            .try_next()
                            .map(|request| (request, Arc::clone(&queue)))
                    })
            })
            .flatten();
        let event = if let Some((request, source_queue)) = fairness_request {
            WriteLoopEvent::Request(Some(request), source_queue, 0)
        } else {
            if consecutive_controls >= MAX_READY_CONTROLS_PER_BOUNDARY {
                consecutive_controls = 0;
                tokio::task::yield_now().await;
            }
            tokio::select! {
                biased;
                _ = cancel.cancelled() => WriteLoopEvent::Cancelled,
                control = control_rx.recv() => WriteLoopEvent::Control(control),
                _ = wait_for_pong_deadline(&heartbeat_state) => WriteLoopEvent::HeartbeatTimedOut,
                _ = heartbeat.tick() => WriteLoopEvent::Heartbeat,
                request = urgent_queue.next() => WriteLoopEvent::Request(
                    request,
                    Arc::clone(&urgent_queue),
                    MAX_READY_CONTROLS_PER_BOUNDARY
                        .saturating_sub(consecutive_controls),
                ),
                request = queue.next() => WriteLoopEvent::Request(
                    request,
                    Arc::clone(&queue),
                    MAX_READY_CONTROLS_PER_BOUNDARY
                        .saturating_sub(consecutive_controls),
                ),
            }
        };
        match event {
            WriteLoopEvent::Cancelled => {
                on_common::log_s!(LogType::WSC; "run_write_loop", "state|generation", "write_cancelled", generation);
                return;
            }
            WriteLoopEvent::Control(Some(control)) => {
                consecutive_controls = consecutive_controls.saturating_add(1);
                match handle_control_message(&mut write, control, control_write_timeout, &cancel)
                    .await
                {
                    ControlAction::Continue => {}
                    ControlAction::Stop(_) => return,
                    ControlAction::Failed {
                        error: NetError::Cancelled,
                        ..
                    } => return,
                    ControlAction::Failed { error, .. } => {
                        send_write_end(&io_event_tx, generation, error, &cancel).await;
                        return;
                    }
                }
            }
            WriteLoopEvent::Control(None) => {
                on_common::log_s!(LogType::WSC; "run_write_loop", "state|generation", "control_channel_closed", generation);
                return;
            }
            WriteLoopEvent::HeartbeatTimedOut => {
                // The sleep may refer to a probe that a racing matching Pong already
                // cleared. Re-check shared state before terminating the connection.
                if heartbeat_state.is_timed_out(Instant::now()) {
                    send_write_end(
                        &io_event_tx,
                        generation,
                        NetError::SocketRecvTimeout,
                        &cancel,
                    )
                    .await;
                    return;
                }
            }
            WriteLoopEvent::Heartbeat => {
                consecutive_controls = 0;
                if let Err(error) = handle_heartbeat_tick(
                    &mut write,
                    &heartbeat_state,
                    pong_timeout,
                    control_write_timeout,
                    &cancel,
                )
                .await
                {
                    if error != NetError::Cancelled {
                        send_write_end(&io_event_tx, generation, error, &cancel).await;
                    }
                    return;
                }
            }
            WriteLoopEvent::Request(Some(request), source_queue, first_frame_control_budget) => {
                consecutive_controls = 0;
                request
                    .dispatch_phase
                    .bind_write_retirement(write_retirement.clone());
                match handle_queued_request(
                    &mut write,
                    request,
                    &source_queue,
                    &pending_requests,
                    generation,
                    data_frame_payload_size,
                    first_frame_control_budget,
                    &mut control_rx,
                    control_write_timeout,
                    data_frame_write_timeout,
                    &mut heartbeat,
                    &heartbeat_state,
                    pong_timeout,
                    response_dispatch_grace,
                    &cancel,
                )
                .await
                {
                    RequestAction::Continue => {}
                    RequestAction::Stop => return,
                    RequestAction::StopWithError(error) => {
                        send_write_end(&io_event_tx, generation, error, &cancel).await;
                        return;
                    }
                }
            }
            // 两个队列只在客户端永久关闭时关闭；任何一个返回 `None` 都意味着
            // 当前 writer 不应再从本连接继续取任务。
            WriteLoopEvent::Request(None, _, _) => {
                on_common::log_s!(LogType::WSC; "run_write_loop", "state|generation", "request_queue_closed", generation);
                return;
            }
        }
    }
}

/// Processes at most one control that is already queued without waiting for new traffic.
async fn try_handle_ready_control<W>(
    write: &mut W,
    control_rx: &mut mpsc::Receiver<ControlMessage>,
    timeout: Duration,
    cancel: &CancellationToken,
) -> Option<ControlAction>
where
    W: Sink<Message, Error = WsError> + Unpin,
{
    on_common::log_t!(LogType::WSC; "try_handle_ready_control", "timeout_seconds|cancelled", timeout.as_secs_f64(), cancel.is_cancelled());
    match control_rx.try_recv() {
        Ok(control) => Some(handle_control_message(write, control, timeout, cancel).await),
        Err(mpsc::error::TryRecvError::Empty) => None,
        Err(mpsc::error::TryRecvError::Disconnected) => {
            Some(ControlAction::Stop(ControlStop::ConnectionClosed))
        }
    }
}

/// 写循环一次 `select!` 得到的事件。
#[allow(clippy::large_enum_variant)]
enum WriteLoopEvent {
    Cancelled,
    Control(Option<ControlMessage>),
    HeartbeatTimedOut,
    Heartbeat,
    Request(
        Option<QueuedRequest>,
        Arc<PriorityWriteQueue>,
        /// Remaining ready-control allowance before this request's first data frame.
        usize,
    ),
}

/// How a failed write may return to a queue after the current connection is retired.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RequestRequeue {
    /// Cancellation and the request's own deadline are terminal for this dispatch.
    Never,
    /// No business data started, so only `WaitForReconnect` may retain the request.
    WaitForReconnect,
    /// Business delivery is unknown, so only an explicitly idempotent retry may run again.
    IdempotentRetry,
}

/// Separates the request-visible result from the connection-level action.
///
/// A control-frame timeout can make the shared sink unsafe while the current business request is
/// still provably unsent. In that case `request_error` stays a safe `ConnectionClosed`, while
/// `connection_action` carries `DeliveryUnknown` and retires the physical connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RequestWriteFailure {
    request_error: NetError,
    connection_action: RequestAction,
    requeue: RequestRequeue,
}

impl RequestWriteFailure {
    fn connection(error: NetError, data_started: bool) -> Self {
        on_common::log_t!(LogType::WSC; "connection", "error|data_started", format!("{error:?}"), data_started);
        if matches!(error, NetError::Cancelled | NetError::ConnectionClosed) {
            on_common::log_s!(LogType::WSC; "connection", "error|data_started", format!("{error:?}"), data_started);
        } else {
            on_common::log_e!(LogType::WSC; "connection", "error|data_started", format!("{error:?}"), data_started);
        }
        let request_error = if data_started {
            NetError::DeliveryUnknown
        } else if error == NetError::DeliveryUnknown {
            NetError::ConnectionClosed
        } else {
            error
        };
        let connection_action = if matches!(error, NetError::Cancelled | NetError::ConnectionClosed)
        {
            RequestAction::Stop
        } else {
            RequestAction::StopWithError(error)
        };
        let requeue = if data_started {
            RequestRequeue::IdempotentRetry
        } else {
            RequestRequeue::WaitForReconnect
        };
        Self {
            request_error,
            connection_action,
            requeue,
        }
    }

    fn cancelled(data_started: bool) -> Self {
        on_common::log_t!(LogType::WSC; "cancelled", "data_started", data_started);
        on_common::log_s!(LogType::WSC; "cancelled", "state|data_started", "dispatch_cancelled", data_started);
        Self {
            request_error: if data_started {
                NetError::DeliveryUnknown
            } else {
                NetError::Cancelled
            },
            connection_action: if data_started {
                RequestAction::StopWithError(NetError::DeliveryUnknown)
            } else {
                RequestAction::Continue
            },
            requeue: RequestRequeue::Never,
        }
    }

    fn local_close(data_started: bool, error: Option<NetError>) -> Self {
        on_common::log_t!(LogType::WSC; "local_close", "data_started|error", data_started, format!("{error:?}"));
        Self {
            request_error: if data_started {
                NetError::DeliveryUnknown
            } else {
                NetError::Cancelled
            },
            connection_action: match error {
                Some(NetError::Cancelled) | None => RequestAction::Stop,
                Some(error) => RequestAction::StopWithError(error),
            },
            requeue: RequestRequeue::Never,
        }
    }

    fn deadline(data_started: bool, connection_uncertain: bool) -> Self {
        on_common::log_t!(LogType::WSC; "deadline", "data_started|connection_uncertain", data_started, connection_uncertain);
        on_common::log_e!(LogType::WSC; "deadline", "error|data_started|connection_uncertain", "request_deadline_elapsed", data_started, connection_uncertain);
        Self {
            request_error: if data_started {
                NetError::DeliveryUnknown
            } else {
                NetError::TimeoutError
            },
            connection_action: if data_started || connection_uncertain {
                RequestAction::StopWithError(NetError::DeliveryUnknown)
            } else {
                RequestAction::Continue
            },
            requeue: RequestRequeue::Never,
        }
    }
}

/// 处理一条普通或紧急队列请求，并给主循环返回连接级动作。
#[allow(clippy::too_many_arguments)]
async fn handle_queued_request<W>(
    write: &mut W,
    mut request: QueuedRequest,
    source_queue: &Arc<PriorityWriteQueue>,
    pending_requests: &PendingRequestView,
    generation: u64,
    data_frame_payload_size: Option<usize>,
    first_frame_control_budget: usize,
    control_rx: &mut mpsc::Receiver<ControlMessage>,
    control_write_timeout: Duration,
    data_frame_write_timeout: Duration,
    heartbeat: &mut Interval,
    heartbeat_state: &HeartbeatState,
    pong_timeout: Duration,
    response_dispatch_grace: Duration,
    connection_cancel: &CancellationToken,
) -> RequestAction
where
    W: Sink<Message, Error = WsError> + Unpin,
{
    on_common::log_t!(LogType::WSC; "handle_queued_request", "uuid|generation|attempt|message_bytes|pending_token|data_frame_payload_size", &request.uuid, generation, request.attempt, request.message.len(), request.pending_token, data_frame_payload_size);
    if request
        .admission_cancel
        .as_ref()
        .is_some_and(CancellationToken::is_cancelled)
    {
        on_common::log_s!(LogType::WSC; "handle_queued_request", "state|uuid|generation", "admission_cancelled_before_write", &request.uuid, generation);
        remove_pending_for_request(pending_requests, &request, NetError::ConnectionClosed);
        request.complete(Err(NetError::ConnectionClosed));
        return RequestAction::Continue;
    }
    // The writer has committed this admission. A retry after an actual write error is
    // governed by request idempotency/policy rather than the old connection lease.
    request.admission_cancel = None;
    if !request.dispatch_phase.start_writing() || request.dispatch_cancel.is_cancelled() {
        on_common::log_s!(LogType::WSC; "handle_queued_request", "state|uuid|generation", "dispatch_cancelled_before_write", &request.uuid, generation);
        remove_pending_for_request(pending_requests, &request, NetError::Cancelled);
        request.complete(Err(NetError::Cancelled));
        return RequestAction::Continue;
    }
    if let Some(token) = request.pending_token {
        if !pending_requests.mark_writing(request.uuid.as_str(), token, request.attempt, generation)
        {
            on_common::log_s!(LogType::WSC; "handle_queued_request", "state|uuid|generation", "pending_unavailable_before_write", &request.uuid, generation);
            request.complete(Err(NetError::Cancelled));
            return RequestAction::Continue;
        }
    }
    if let Some(observation) = request.dispatch_phase.observation() {
        observation.mark_writing(generation);
    }

    let Some(write_deadline) = Instant::now().checked_add(request.config.write_timeout) else {
        on_common::log_e!(LogType::WSC; "handle_queued_request", "error|uuid", "unrepresentable_write_deadline", &request.uuid);
        remove_pending_for_request(pending_requests, &request, NetError::ConfigError);
        request.complete(Err(NetError::ConfigError));
        return RequestAction::Continue;
    };
    let write_deadline = request
        .dispatch_phase
        .response_deadline()
        .map_or(write_deadline, |deadline| {
            write_deadline.min(Instant::from_std(deadline))
        });
    on_common::log_s!(LogType::WSC; "handle_queued_request", "state|uuid|generation|attempt", "write_started", &request.uuid, generation, request.attempt);
    let send_result = send_request_message_with_phase(
        write,
        request.message.clone(),
        data_frame_payload_size,
        first_frame_control_budget,
        write_deadline,
        control_rx,
        control_write_timeout,
        data_frame_write_timeout,
        heartbeat,
        heartbeat_state,
        pong_timeout,
        connection_cancel,
        &request.dispatch_cancel,
        &request.dispatch_phase,
    )
    .await;
    // Publish the successful sink commit before notifying the awaiting Future. If
    // cancellation won the atomic race while writing, the connection is no longer
    // safe to reuse even when the sink future happened to return Ok immediately after.
    // A scope can revoke the child token without changing the dispatch phase. Honor
    // that signal when observed before commit; later revocation cannot undo this write.
    let send_result = match send_result {
        Ok(()) if !request.dispatch_cancel.is_cancelled() && request.dispatch_phase.commit() => {
            Ok(())
        }
        Ok(()) => Err(RequestWriteFailure::cancelled(true)),
        Err(failure) => Err(failure),
    };

    match send_result {
        Ok(()) => {
            on_common::log_s!(LogType::WSC; "handle_queued_request", "state|uuid|generation|expect_response", "sink_write_committed", &request.uuid, generation, request.pending_token.is_some());
            if request.pending_token.is_some() {
                if let Some(observation) = request.dispatch_phase.observation() {
                    observation.mark_written();
                }
            }
            if let Some(token) = request.pending_token {
                let uuid = request.uuid.clone();
                let response_timeout = request.config.response_timeout;
                if let Some(timeout_cancel) = pending_requests.mark_sent(uuid.as_str(), token) {
                    let timeout_pending = pending_requests.clone();
                    spawn(expire_pending_after_response_timeout(
                        timeout_pending,
                        uuid,
                        token,
                        timeout_cancel,
                        response_timeout,
                        response_dispatch_grace,
                    ));
                }
            }
            // 响应或发送 Future 的取消可能已经认领/移除了 pending；这不改变网络写入成功。
            request.complete(Ok(()));
            RequestAction::Continue
        }
        Err(failure) => {
            if failure.request_error == NetError::Cancelled {
                on_common::log_s!(LogType::WSC; "handle_queued_request", "state|uuid|generation", "request_cancelled", &request.uuid, generation);
            } else {
                on_common::log_e!(LogType::WSC; "handle_queued_request", "uuid|generation|attempt|request_error|connection_action", &request.uuid, generation, request.attempt, format!("{:?}", failure.request_error), format!("{:?}", failure.connection_action));
            }
            let request_error = failure.request_error;
            request.remember_delivery_error(request_error);
            let dispatch_cancelled =
                request.dispatch_phase.is_cancelled() || request.dispatch_cancel.is_cancelled();
            let connection_action = failure.connection_action;
            if connection_action != RequestAction::Continue {
                request.dispatch_phase.retire_write();
            }
            if let Some(token) = request.pending_token {
                if let Err(error) = pending_requests.enforce_registration_deadline(
                    request.uuid.as_str(),
                    token,
                    response_dispatch_grace,
                ) {
                    on_common::log_e!(LogType::WSC; "handle_queued_request", "stage|error", "registration_deadline", format!("{error:?}"));
                    remove_pending_for_request(pending_requests, &request, error);
                    request.complete(Err(error));
                    return connection_action;
                }
                match pending_requests.defer_registration_write(&mut request) {
                    Ok(true) => return connection_action,
                    Ok(false) => {}
                    Err(error) => {
                        on_common::log_e!(LogType::WSC; "handle_queued_request", "stage|error", "defer_registration_receipt", format!("{error:?}"));
                        remove_pending_for_request(pending_requests, &request, error);
                        request.complete(Err(error));
                        return connection_action;
                    }
                }
            }
            // A listener may correlate a response after the peer received the request but
            // before Tungstenite finishes flushing it. That successful business response is
            // authoritative for this request, although the ambiguous I/O error still retires
            // the physical connection.
            if request.dispatch_phase.take_write_response_claim() {
                on_common::log_s!(LogType::WSC; "handle_queued_request", "state|uuid|generation", "response_claim_won_over_write_error", &request.uuid, generation);
                request.complete(Ok(()));
                return connection_action;
            }
            let wait_for_reconnect = failure.requeue == RequestRequeue::WaitForReconnect
                && !dispatch_cancelled
                && request.config.disconnected_policy
                    == crate::api::traits::ws::ws_request_config::DisconnectedTaskPolicy::WaitForReconnect;
            let can_retry = failure.requeue == RequestRequeue::IdempotentRetry
                && request.config.idempotent
                && request.attempt < request.config.send_retry_count
                && !dispatch_cancelled;
            let should_requeue =
                (can_retry || wait_for_reconnect) && request.dispatch_phase.requeue();
            if should_requeue {
                on_common::log_s!(LogType::WSC; "handle_queued_request", "state|uuid|generation|attempt|idempotent_retry|wait_for_reconnect", "request_requeue", &request.uuid, generation, request.attempt, can_retry, wait_for_reconnect);
                if can_retry {
                    request.attempt += 1;
                }
                if !mark_request_queued(pending_requests, &request) {
                    if request.dispatch_phase.take_write_response_claim() {
                        request.complete(Ok(()));
                    } else {
                        let terminal_error = request.terminal_error(request_error);
                        request.complete(Err(terminal_error));
                    }
                    return connection_action;
                }
                if let Err(request) = source_queue.push_existing(request) {
                    let request = *request;
                    let fallback_error = if request.dispatch_cancel.is_cancelled() {
                        NetError::Cancelled
                    } else {
                        NetError::QueueClosed
                    };
                    let push_error = request.terminal_error(fallback_error);
                    on_common::log_e!(LogType::WSC; "handle_queued_request", "stage|uuid|error", "requeue_failed", &request.uuid, format!("{push_error:?}"));
                    remove_pending_for_request(pending_requests, &request, push_error);
                    request.complete(Err(push_error));
                    return if push_error == NetError::Cancelled {
                        connection_action
                    } else {
                        RequestAction::StopWithError(push_error)
                    };
                }
            } else {
                let terminal_error = request.terminal_error(request_error);
                remove_pending_for_request(pending_requests, &request, terminal_error);
                request.complete(Err(terminal_error));
            }

            connection_action
        }
    }
}

/// 在请求响应 deadline 后清理关联记录，并保护 deadline 前已进入回调 lane 的响应。
///
/// 通用层无法在业务 listener 解析之前得知响应 UUID，因此只要目标请求所属连接代仍有
/// 任意已接收响应，便保守地给予一次有界宽限。宽限不会循环续期；用户回调永久阻塞时，
/// 请求仍会在确定的最长期限内以 `TimeoutError` 完成。
async fn expire_pending_after_response_timeout(
    pending_requests: PendingRequestView,
    uuid: String,
    token: u64,
    timeout_cancel: CancellationToken,
    response_timeout: Duration,
    response_dispatch_grace: Duration,
) {
    on_common::log_t!(LogType::WSC; "expire_pending_after_response_timeout", "uuid|token|response_timeout_seconds|response_grace_seconds", &uuid, token, response_timeout.as_secs_f64(), response_dispatch_grace.as_secs_f64());
    let response_deadline = match pending_requests.response_deadline(uuid.as_str(), token) {
        Ok(Some(deadline)) => Instant::from_std(deadline),
        Ok(None) => return,
        Err(error) => {
            on_common::log_e!(LogType::WSC; "expire_pending_after_response_timeout", "uuid|error", &uuid, format!("{error:?}"));
            pending_requests.remove_if_token(uuid.as_str(), token, error);
            return;
        }
    };
    tokio::select! {
        _ = timeout_cancel.cancelled() => {
            on_common::log_s!(LogType::WSC; "expire_pending_after_response_timeout", "uuid|token|state", &uuid, token, "response_timeout_cancelled");
            return;
        },
        _ = tokio::time::sleep_until(response_deadline) => {}
    }
    if !pending_requests.record_deferred_error(uuid.as_str(), token, NetError::TimeoutError) {
        on_common::log_s!(LogType::WSC; "expire_pending_after_response_timeout", "uuid|token|state", &uuid, token, "request_already_completed");
        return;
    }
    on_common::log_e!(LogType::WSC; "expire_pending_after_response_timeout", "uuid|token|error", &uuid, token, "response_timeout");
    if pending_requests.response_dispatch_in_flight(uuid.as_str(), token)
        && !response_dispatch_grace.is_zero()
    {
        if let Some(grace_deadline) = response_deadline.checked_add(response_dispatch_grace) {
            on_common::log_s!(LogType::WSC; "expire_pending_after_response_timeout", "uuid|token|state", &uuid, token, "accepted_response_claim_grace");
            tokio::select! {
                    _ = timeout_cancel.cancelled() => {
                on_common::log_s!(LogType::WSC; "expire_pending_after_response_timeout", "uuid|token|state", &uuid, token, "response_timeout_cancelled");
                return;
            },
                    _ = tokio::time::sleep_until(grace_deadline) => {}
                }
        }
    }
    pending_requests.remove_if_token(uuid.as_str(), token, NetError::TimeoutError);
    on_common::log_s!(LogType::WSC; "expire_pending_after_response_timeout", "uuid|token|state", &uuid, token, "response_timeout_cleanup_finished");
}

fn mark_request_queued(pending_requests: &PendingRequestView, request: &QueuedRequest) -> bool {
    on_common::log_t!(LogType::WSC; "mark_request_queued", "uuid|token", &request.uuid, request.pending_token);
    if let Some(token) = request.pending_token {
        pending_requests.mark_queued(request.uuid.as_str(), token)
    } else {
        true
    }
}

fn remove_pending_for_request(
    pending_requests: &PendingRequestView,
    request: &QueuedRequest,
    error: NetError,
) {
    on_common::log_t!(LogType::WSC; "remove_pending_for_request", "uuid|token|error", &request.uuid, request.pending_token, format!("{error:?}"));
    if let Some(token) = request.pending_token {
        pending_requests.remove_if_token(
            request.uuid.as_str(),
            token,
            request.terminal_error(error),
        );
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RequestAction {
    Continue,
    Stop,
    StopWithError(NetError),
}

/// 处理一条控制指令。控制写超时后返回 `DeliveryUnknown`，调用方必须废弃连接。
async fn handle_control_message<W>(
    write: &mut W,
    control: ControlMessage,
    timeout: Duration,
    cancel: &CancellationToken,
) -> ControlAction
where
    W: Sink<Message, Error = WsError> + Unpin,
{
    on_common::log_t!(LogType::WSC; "handle_control_message", "control|timeout_seconds", match &control { ControlMessage::FlushAutomatic => "flush_automatic", ControlMessage::PeerClose(_) => "peer_close", ControlMessage::Close(_) => "local_close" }, timeout.as_secs_f64());
    match control {
        ControlMessage::FlushAutomatic => match flush_sink(write, timeout, cancel).await {
            Ok(()) => ControlAction::Continue,
            Err(error) => ControlAction::Failed {
                error,
                origin: ControlOrigin::Automatic,
            },
        },
        ControlMessage::PeerClose(done_tx) => {
            on_common::log_s!(LogType::WSC; "handle_control_message", "state", "peer_close_reply_flush_started");
            let result = flush_sink(write, timeout, cancel).await;
            let _ = done_tx.send(());
            match result {
                Ok(()) => ControlAction::Stop(ControlStop::ConnectionClosed),
                Err(error) => ControlAction::Failed {
                    error,
                    origin: ControlOrigin::PeerClose,
                },
            }
        }
        ControlMessage::Close(done_tx) => {
            on_common::log_s!(LogType::WSC; "handle_control_message", "state", "local_close_started");
            let result = async {
                send_control_frame(write, Message::Close(None), timeout, cancel).await?;
                close_sink(write, timeout, cancel).await
            }
            .await;
            let _ = done_tx.send(());
            match result {
                Ok(()) => ControlAction::Stop(ControlStop::LocalClose),
                Err(error) => ControlAction::Failed {
                    error,
                    origin: ControlOrigin::LocalClose,
                },
            }
        }
    }
}

/// Flushes Tungstenite's automatically queued Pong or peer-Close response.
async fn flush_sink<W>(
    write: &mut W,
    timeout: Duration,
    cancel: &CancellationToken,
) -> Result<(), NetError>
where
    W: Sink<Message, Error = WsError> + Unpin,
{
    on_common::log_t!(LogType::WSC; "flush_sink", "timeout_seconds|cancelled", timeout.as_secs_f64(), cancel.is_cancelled());
    let deadline = Instant::now().checked_add(timeout).ok_or_else(|| {
        on_common::log_e!(LogType::WSC; "flush_sink", "error", "unrepresentable_deadline");
        NetError::ConfigError
    })?;
    tokio::select! {
        biased;
        _ = cancel.cancelled() => {
            on_common::log_s!(LogType::WSC; "flush_sink", "state", "cancelled");
            Err(NetError::Cancelled)
        },
        result = tokio::time::timeout_at(deadline, write.flush()) => match result {
            Ok(Ok(())) => {
                on_common::log_s!(LogType::WSC; "flush_sink", "state", "sink_operation_completed");
                Ok(())
            },
            Ok(Err(error)) => {
                on_common::log_e!(LogType::WSC; "flush_sink", "error", on_common::log::summary::error(&error));
                Err(error.into())
            },
            Err(_) => {
                on_common::log_e!(LogType::WSC; "flush_sink", "error|delivery", "sink_operation_timeout", "unknown");
                Err(NetError::DeliveryUnknown)
            },
        },
    }
}

/// 在写下一个 data frame 前处理一批已经进入控制通道的指令。
#[allow(clippy::too_many_arguments)]
async fn drain_ready_controls<W>(
    write: &mut W,
    control_rx: &mut mpsc::Receiver<ControlMessage>,
    timeout: Duration,
    cancel: &CancellationToken,
    heartbeat: &mut Interval,
    heartbeat_state: &HeartbeatState,
    pong_timeout: Duration,
    request_deadline: Instant,
    remaining_controls: &mut usize,
) -> Result<ControlAction, NetError>
where
    W: Sink<Message, Error = WsError> + Unpin,
{
    on_common::log_t!(LogType::WSC; "drain_ready_controls", "timeout_seconds|remaining_controls|deadline_elapsed", timeout.as_secs_f64(), *remaining_controls, Instant::now() >= request_deadline);
    loop {
        // A burst of ready controls may itself consume multiple write deadlines.
        // Re-check between every control so request expiry is not delayed by the whole burst.
        let now = Instant::now();
        if now >= request_deadline {
            on_common::log_e!(LogType::WSC; "drain_ready_controls", "error", "TimeoutError");
            return Err(NetError::TimeoutError);
        }
        let pong_timed_out = heartbeat_state.is_timed_out(now);
        if *remaining_controls == 0 && !pong_timed_out {
            if control_rx.is_closed() && control_rx.is_empty() {
                return Ok(ControlAction::Stop(ControlStop::ConnectionClosed));
            }
            break;
        }
        match control_rx.try_recv() {
            Ok(control) => {
                // A simultaneous peer Close must get one opportunity to flush Tungstenite's
                // automatic reply before an expired Pong probe retires the connection. This
                // mirrors the top-level loop. The timeout exception consumes no normal budget
                // when that budget was already exhausted, and always stops after this control.
                if *remaining_controls > 0 {
                    *remaining_controls -= 1;
                }
                let bounded_timeout = timeout.min(request_deadline.saturating_duration_since(now));
                let action = handle_control_message(write, control, bounded_timeout, cancel).await;
                if matches!(
                    action,
                    ControlAction::Stop(_) | ControlAction::Failed { .. }
                ) {
                    return Ok(action);
                }
                if pong_timed_out || heartbeat_state.is_timed_out(Instant::now()) {
                    on_common::log_e!(LogType::WSC; "drain_ready_controls", "error", "SocketRecvTimeout");
                    return Err(NetError::SocketRecvTimeout);
                }
                // A ready-control producer can refill the channel between every receive.
                // Poll the interval after each control, not merely after the whole batch, so
                // creation of a new Ping is delayed by at most one bounded control write.
                if heartbeat.tick().now_or_never().is_some() {
                    let now = Instant::now();
                    if now >= request_deadline {
                        on_common::log_e!(LogType::WSC; "drain_ready_controls", "error", "TimeoutError");
                        return Err(NetError::TimeoutError);
                    }
                    handle_heartbeat_tick(
                        write,
                        heartbeat_state,
                        pong_timeout,
                        timeout.min(request_deadline.saturating_duration_since(now)),
                        cancel,
                    )
                    .await?;
                }
            }
            Err(mpsc::error::TryRecvError::Empty) => {
                if pong_timed_out {
                    on_common::log_e!(LogType::WSC; "drain_ready_controls", "error", "SocketRecvTimeout");
                    return Err(NetError::SocketRecvTimeout);
                }
                return Ok(ControlAction::Continue);
            }
            Err(mpsc::error::TryRecvError::Disconnected) => {
                return Ok(ControlAction::Stop(ControlStop::ConnectionClosed));
            }
        }
    }
    // Give the read loop and heartbeat timer a scheduling point before the caller
    // re-checks both deadlines and advances the fragmented business message.
    tokio::task::yield_now().await;
    Ok(ControlAction::Continue)
}

/// 检查当前 probe 的 Pong deadline，或发送一次新的有界等待 Ping。
async fn handle_heartbeat_tick<W>(
    write: &mut W,
    heartbeat_state: &HeartbeatState,
    pong_timeout: Duration,
    control_write_timeout: Duration,
    cancel: &CancellationToken,
) -> Result<(), NetError>
where
    W: Sink<Message, Error = WsError> + Unpin,
{
    on_common::log_t!(LogType::WSC; "handle_heartbeat_tick", "pong_timeout_seconds|control_write_timeout_seconds", pong_timeout.as_secs_f64(), control_write_timeout.as_secs_f64());
    let payload = match heartbeat_state.on_tick(Instant::now(), pong_timeout) {
        HeartbeatTick::Waiting => {
            on_common::log_s!(LogType::WSC; "handle_heartbeat_tick", "state", "probe_already_outstanding");
            return Ok(());
        }
        HeartbeatTick::TimedOut => {
            on_common::log_e!(LogType::WSC; "handle_heartbeat_tick", "error", "pong_timeout");
            return Err(NetError::SocketRecvTimeout);
        }
        HeartbeatTick::SendProbe(payload) => {
            on_common::log_s!(LogType::WSC; "handle_heartbeat_tick", "state|payload_bytes", "ping_write_started", payload.len());
            payload
        }
    };
    let send_result = send_control_frame(
        write,
        Message::Ping(payload.clone()),
        control_write_timeout,
        cancel,
    )
    .await;
    match send_result {
        Ok(()) => {
            // If the matching Pong raced ahead of `send` completion, the reader
            // has already cleared this payload and `mark_sent` intentionally does nothing.
            let awaiting_pong = heartbeat_state.mark_sent(payload.as_ref(), Instant::now());
            on_common::log_s!(LogType::WSC; "handle_heartbeat_tick", "state|awaiting_pong", "ping_written", awaiting_pong);
            Ok(())
        }
        Err(error) => {
            heartbeat_state.abandon_probe(payload.as_ref());
            on_common::log_s!(LogType::WSC; "handle_heartbeat_tick", "state|error", "probe_abandoned", format!("{error:?}"));
            Err(error)
        }
    }
}

/// Waits for the currently outstanding probe's Pong deadline.
///
/// When no successfully written probe exists this future remains pending until
/// the surrounding writer selects another event and constructs a fresh snapshot.
/// A matching Pong can make the captured deadline stale, so the caller must
/// re-check [`HeartbeatState::is_timed_out`] after this future completes.
async fn wait_for_pong_deadline(heartbeat_state: &HeartbeatState) {
    on_common::log_t!(LogType::WSC; "wait_for_pong_deadline", "heartbeat_state", "shared");
    match heartbeat_state.pong_deadline() {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending::<()>().await,
    }
}

/// 写完一条逻辑消息。只有控制帧和心跳能在其相邻 data frame 之间穿插。
#[allow(clippy::too_many_arguments)]
async fn send_request_message_with_phase<W>(
    write: &mut W,
    message: Message,
    data_frame_payload_size: Option<usize>,
    first_frame_control_budget: usize,
    write_deadline: Instant,
    control_rx: &mut mpsc::Receiver<ControlMessage>,
    control_write_timeout: Duration,
    data_frame_write_timeout: Duration,
    heartbeat: &mut Interval,
    heartbeat_state: &HeartbeatState,
    pong_timeout: Duration,
    connection_cancel: &CancellationToken,
    dispatch_cancel: &CancellationToken,
    dispatch_phase: &DispatchPhase,
) -> Result<(), RequestWriteFailure>
where
    W: Sink<Message, Error = WsError> + Unpin,
{
    on_common::log_t!(LogType::WSC; "send_request_message", "message_type|message_bytes|data_frame_payload_size|control_budget", match &message { Message::Text(_) => "text", Message::Binary(_) => "binary", Message::Ping(_) => "ping", Message::Pong(_) => "pong", Message::Close(_) => "close", Message::Frame(_) => "frame" }, message.len(), data_frame_payload_size, first_frame_control_budget);
    let mut frames = OutgoingMessageFrames::new(message, data_frame_payload_size).peekable();
    let mut wrote_frame = false;
    let mut next_control_budget = first_frame_control_budget.min(MAX_READY_CONTROLS_PER_BOUNDARY);
    while let Some(frame) = frames.next() {
        // A data-frame write cannot be safely pre-empted once it starts. Check the
        // independent Pong deadline at every safe frame boundary; any overshoot is
        // therefore bounded by the configured per-frame write timeout.
        if dispatch_cancel.is_cancelled() {
            return Err(RequestWriteFailure::cancelled(wrote_frame));
        }
        if Instant::now() >= write_deadline {
            return Err(RequestWriteFailure::deadline(wrote_frame, wrote_frame));
        }
        if connection_cancel.is_cancelled() {
            return Err(RequestWriteFailure::connection(
                NetError::Cancelled,
                wrote_frame,
            ));
        }
        let mut remaining_controls = next_control_budget;
        next_control_budget = MAX_READY_CONTROLS_PER_BOUNDARY;
        // 控制指令优先于已经到期的 heartbeat。
        let control_action = drain_ready_controls(
            write,
            control_rx,
            control_write_timeout,
            connection_cancel,
            heartbeat,
            heartbeat_state,
            pong_timeout,
            write_deadline,
            &mut remaining_controls,
        )
        .await
        .map_err(|error| classify_boundary_failure(error, wrote_frame, write_deadline))?;
        match control_action {
            ControlAction::Continue => {}
            ControlAction::Stop(ControlStop::ConnectionClosed) => {
                return Err(RequestWriteFailure::connection(
                    NetError::ConnectionClosed,
                    wrote_frame,
                ));
            }
            ControlAction::Stop(ControlStop::LocalClose) => {
                return Err(RequestWriteFailure::local_close(wrote_frame, None));
            }
            ControlAction::Failed { error, origin } => {
                return Err(classify_control_failure(
                    error,
                    origin,
                    wrote_frame,
                    write_deadline,
                ));
            }
        }
        if heartbeat.tick().now_or_never().is_some() {
            let now = Instant::now();
            if now >= write_deadline {
                return Err(RequestWriteFailure::deadline(wrote_frame, wrote_frame));
            }
            handle_heartbeat_tick(
                write,
                heartbeat_state,
                pong_timeout,
                control_write_timeout.min(write_deadline.saturating_duration_since(now)),
                connection_cancel,
            )
            .await
            .map_err(|error| classify_boundary_failure(error, wrote_frame, write_deadline))?;
        }
        // heartbeat 写入期间可能又收到 Ping/Close，因此紧邻 data frame 再排空一次。
        if remaining_controls > 0 {
            let control_action = drain_ready_controls(
                write,
                control_rx,
                control_write_timeout,
                connection_cancel,
                heartbeat,
                heartbeat_state,
                pong_timeout,
                write_deadline,
                &mut remaining_controls,
            )
            .await
            .map_err(|error| classify_boundary_failure(error, wrote_frame, write_deadline))?;
            match control_action {
                ControlAction::Continue => {}
                ControlAction::Stop(ControlStop::ConnectionClosed) => {
                    return Err(RequestWriteFailure::connection(
                        NetError::ConnectionClosed,
                        wrote_frame,
                    ));
                }
                ControlAction::Stop(ControlStop::LocalClose) => {
                    return Err(RequestWriteFailure::local_close(wrote_frame, None));
                }
                ControlAction::Failed { error, origin } => {
                    return Err(classify_control_failure(
                        error,
                        origin,
                        wrote_frame,
                        write_deadline,
                    ));
                }
            }
        }
        send_data_frame_with_phase(
            write,
            frame,
            wrote_frame,
            write_deadline,
            data_frame_write_timeout,
            connection_cancel,
            dispatch_cancel,
            dispatch_phase,
        )
        .await?;
        wrote_frame = true;
        if frames.peek().is_some() {
            tokio::task::yield_now().await;
        }
    }
    Ok(())
}

/// Classifies a control/heartbeat boundary failure without confusing the control frame's
/// uncertain write state with delivery of a business frame that has not started.
fn classify_boundary_failure(
    error: NetError,
    data_started: bool,
    request_deadline: Instant,
) -> RequestWriteFailure {
    on_common::log_t!(LogType::WSC; "classify_boundary_failure", "error|data_started|deadline_elapsed", format!("{error:?}"), data_started, Instant::now() >= request_deadline);
    if error == NetError::TimeoutError {
        RequestWriteFailure::deadline(data_started, data_started)
    } else if error == NetError::DeliveryUnknown && Instant::now() >= request_deadline {
        // The request deadline bounded an in-flight control write. The business request is still
        // known-unsent when `data_started` is false, but the shared sink must be discarded.
        RequestWriteFailure::deadline(data_started, true)
    } else {
        RequestWriteFailure::connection(error, data_started)
    }
}

fn classify_control_failure(
    error: NetError,
    origin: ControlOrigin,
    data_started: bool,
    request_deadline: Instant,
) -> RequestWriteFailure {
    on_common::log_t!(LogType::WSC; "classify_control_failure", "error|origin|data_started|deadline_elapsed", format!("{error:?}"), format!("{origin:?}"), data_started, Instant::now() >= request_deadline);
    if origin == ControlOrigin::LocalClose {
        RequestWriteFailure::local_close(data_started, Some(error))
    } else {
        classify_boundary_failure(error, data_started, request_deadline)
    }
}

/// 在业务消息的统一 deadline 与单帧 deadline 中较早者之前写一个 data frame。
/// `data_started` includes earlier fragments: cancellation after a boundary await
/// cannot make an already written message prefix safe to retry as an unsent request.
#[allow(clippy::too_many_arguments)]
async fn send_data_frame_with_phase<W>(
    write: &mut W,
    frame: Message,
    data_started: bool,
    request_deadline: Instant,
    frame_write_timeout: Duration,
    connection_cancel: &CancellationToken,
    dispatch_cancel: &CancellationToken,
    dispatch_phase: &DispatchPhase,
) -> Result<(), RequestWriteFailure>
where
    W: Sink<Message, Error = WsError> + Unpin,
{
    on_common::log_t!(LogType::WSC; "send_data_frame", "frame_bytes|frame_write_timeout_seconds|deadline_elapsed", frame.len(), frame_write_timeout.as_secs_f64(), Instant::now() >= request_deadline);
    let now = Instant::now();
    if dispatch_cancel.is_cancelled() {
        return Err(RequestWriteFailure::cancelled(data_started));
    }
    if now >= request_deadline {
        return Err(RequestWriteFailure::deadline(data_started, data_started));
    }
    if connection_cancel.is_cancelled() {
        return Err(RequestWriteFailure::connection(
            NetError::Cancelled,
            data_started,
        ));
    }
    let frame_deadline = match now.checked_add(frame_write_timeout) {
        Some(deadline) => deadline,
        None => request_deadline,
    };
    let deadline = request_deadline.min(frame_deadline);
    let request_deadline_wins = request_deadline <= frame_deadline;
    let mut frame = Some(frame);
    let send = async {
        futures::future::poll_fn(|cx| {
            match Pin::new(&mut *write).poll_ready(cx) {
                std::task::Poll::Pending => return std::task::Poll::Pending,
                std::task::Poll::Ready(Err(error)) => {
                    on_common::log_e!(LogType::WSC; "send_data_frame", "error|stage", on_common::log::summary::error(&error), "readiness");
                    return std::task::Poll::Ready(Err(RequestWriteFailure::connection(error.into(), data_started || dispatch_phase.data_write_started())));
                }
                std::task::Poll::Ready(Ok(())) => {}
            }
            if connection_cancel.is_cancelled() {
                return std::task::Poll::Ready(Err(RequestWriteFailure::connection(NetError::Cancelled, data_started || dispatch_phase.data_write_started())));
            }
            if Instant::now() >= deadline {
                return std::task::Poll::Ready(Err(RequestWriteFailure::deadline(data_started || dispatch_phase.data_write_started(), true)));
            }
            if dispatch_cancel.is_cancelled() || !dispatch_phase.start_data_write() {
                return std::task::Poll::Ready(Err(RequestWriteFailure::cancelled(data_started || dispatch_phase.data_write_started())));
            }
            let Some(frame) = frame.take() else {
                on_common::log_e!(LogType::WSC; "send_data_frame", "error", "data_frame_already_consumed");
                return std::task::Poll::Ready(Err(RequestWriteFailure::connection(NetError::InternalError, true)));
            };
            // No await separates this CAS from start_send. Cancellation that wins
            // the same atomic state prevents this request from entering the sink.
            std::task::Poll::Ready(Pin::new(&mut *write).start_send(frame).map_err(|error| {
                on_common::log_e!(LogType::WSC; "send_data_frame", "error|delivery", on_common::log::summary::error(&error), "unknown");
                RequestWriteFailure::connection(NetError::DeliveryUnknown, true)
            }))
        }).await?;
        write.flush().await.map_err(|error| {
            on_common::log_e!(LogType::WSC; "send_data_frame", "error|delivery", on_common::log::summary::error(&error), "unknown");
            RequestWriteFailure::connection(NetError::DeliveryUnknown, true)
        })
    };
    tokio::select! {
        biased;
        _ = dispatch_cancel.cancelled() => Err(RequestWriteFailure::cancelled(data_started || dispatch_phase.data_write_started())),
        result = tokio::time::timeout_at(deadline, send) => match result {
            Ok(Ok(())) => {
                on_common::log_s!(LogType::WSC; "send_data_frame", "state", "frame_written");
                Ok(())
            },
            Ok(Err(failure)) => Err(failure),
            Err(_) if request_deadline_wins => {
                Err(RequestWriteFailure::deadline(data_started || dispatch_phase.data_write_started(), true))
            }
            Err(_) => {
                on_common::log_e!(LogType::WSC; "send_data_frame", "error|delivery", "frame_write_timeout", "unknown");
                Err(RequestWriteFailure::connection(NetError::DeliveryUnknown, data_started || dispatch_phase.data_write_started()))
            },
        },
        _ = connection_cancel.cancelled() => {
            Err(RequestWriteFailure::connection(NetError::Cancelled, data_started || dispatch_phase.data_write_started()))
        },
    }
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
async fn send_request_message<W>(
    write: &mut W,
    message: Message,
    data_frame_payload_size: Option<usize>,
    first_frame_control_budget: usize,
    write_deadline: Instant,
    control_rx: &mut mpsc::Receiver<ControlMessage>,
    control_write_timeout: Duration,
    data_frame_write_timeout: Duration,
    heartbeat: &mut Interval,
    heartbeat_state: &HeartbeatState,
    pong_timeout: Duration,
    connection_cancel: &CancellationToken,
    dispatch_cancel: &CancellationToken,
) -> Result<(), RequestWriteFailure>
where
    W: Sink<Message, Error = WsError> + Unpin,
{
    let phase = DispatchPhase::new();
    if !phase.start_writing() {
        return Err(RequestWriteFailure::connection(
            NetError::InternalError,
            false,
        ));
    }
    send_request_message_with_phase(
        write,
        message,
        data_frame_payload_size,
        first_frame_control_budget,
        write_deadline,
        control_rx,
        control_write_timeout,
        data_frame_write_timeout,
        heartbeat,
        heartbeat_state,
        pong_timeout,
        connection_cancel,
        dispatch_cancel,
        &phase,
    )
    .await
}

#[cfg(test)]
async fn send_data_frame<W>(
    write: &mut W,
    frame: Message,
    data_started: bool,
    request_deadline: Instant,
    frame_write_timeout: Duration,
    connection_cancel: &CancellationToken,
    dispatch_cancel: &CancellationToken,
) -> Result<(), RequestWriteFailure>
where
    W: Sink<Message, Error = WsError> + Unpin,
{
    let phase = DispatchPhase::new();
    if !phase.start_writing() {
        return Err(RequestWriteFailure::connection(
            NetError::InternalError,
            false,
        ));
    }
    send_data_frame_with_phase(
        write,
        frame,
        data_started,
        request_deadline,
        frame_write_timeout,
        connection_cancel,
        dispatch_cancel,
        &phase,
    )
    .await
}

/// 发送一个控制帧；超时意味着底层提交状态未知，当前连接不得继续使用。
async fn send_control_frame<W>(
    write: &mut W,
    frame: Message,
    timeout: Duration,
    cancel: &CancellationToken,
) -> Result<(), NetError>
where
    W: Sink<Message, Error = WsError> + Unpin,
{
    on_common::log_t!(LogType::WSC; "send_control_frame", "frame_type|frame_bytes|timeout_seconds", match &frame { Message::Ping(_) => "ping", Message::Pong(_) => "pong", Message::Close(_) => "close", _ => "data" }, frame.len(), timeout.as_secs_f64());
    let deadline = Instant::now().checked_add(timeout).ok_or_else(|| {
        on_common::log_e!(LogType::WSC; "send_control_frame", "error", "unrepresentable_deadline");
        NetError::ConfigError
    })?;
    tokio::select! {
        biased;
        _ = cancel.cancelled() => {
            on_common::log_s!(LogType::WSC; "send_control_frame", "state", "cancelled");
            Err(NetError::Cancelled)
        },
        result = tokio::time::timeout_at(deadline, write.send(frame)) => match result {
            Ok(Ok(())) => {
                on_common::log_s!(LogType::WSC; "send_control_frame", "state", "sink_operation_completed");
                Ok(())
            },
            Ok(Err(error)) => {
                on_common::log_e!(LogType::WSC; "send_control_frame", "error", on_common::log::summary::error(&error));
                Err(error.into())
            },
            Err(_) => {
                on_common::log_e!(LogType::WSC; "send_control_frame", "error|delivery", "sink_operation_timeout", "unknown");
                Err(NetError::DeliveryUnknown)
            },
        },
    }
}

/// 在控制写 deadline 内关闭 sink。
async fn close_sink<W>(
    write: &mut W,
    timeout: Duration,
    cancel: &CancellationToken,
) -> Result<(), NetError>
where
    W: Sink<Message, Error = WsError> + Unpin,
{
    on_common::log_t!(LogType::WSC; "close_sink", "timeout_seconds|cancelled", timeout.as_secs_f64(), cancel.is_cancelled());
    let deadline = Instant::now().checked_add(timeout).ok_or_else(|| {
        on_common::log_e!(LogType::WSC; "close_sink", "error", "unrepresentable_deadline");
        NetError::ConfigError
    })?;
    tokio::select! {
        biased;
        _ = cancel.cancelled() => {
            on_common::log_s!(LogType::WSC; "close_sink", "state", "cancelled");
            Err(NetError::Cancelled)
        },
        result = tokio::time::timeout_at(deadline, write.close()) => match result {
            Ok(Ok(())) => {
                on_common::log_s!(LogType::WSC; "close_sink", "state", "sink_operation_completed");
                Ok(())
            },
            Ok(Err(error)) => {
                on_common::log_e!(LogType::WSC; "close_sink", "error", on_common::log::summary::error(&error));
                Err(error.into())
            },
            Err(_) => {
                on_common::log_e!(LogType::WSC; "close_sink", "error|delivery", "sink_operation_timeout", "unknown");
                Err(NetError::DeliveryUnknown)
            },
        },
    }
}

/// 控制消息处理后写循环应继续还是停止。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ControlAction {
    Continue,
    Stop(ControlStop),
    Failed {
        error: NetError,
        origin: ControlOrigin,
    },
}

/// Distinguishes an unexpected peer/read-side termination from an intentional local Close.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ControlStop {
    ConnectionClosed,
    LocalClose,
}

/// Preserves the source even when a control operation itself fails.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ControlOrigin {
    Automatic,
    PeerClose,
    LocalClose,
}

/// 把一条完整 Text/Binary 消息按需转换成 RFC 6455 data frame 序列。
///
/// `Bytes::slice` 共享原始分配，不会为每个分片复制正文。非 Text/Binary 消息、
/// 关闭分片或正文不超过上限时，保持原消息不变。
enum OutgoingMessageFrames {
    Single(Option<Message>),
    Fragmented {
        payload: Bytes,
        first_opcode: OpData,
        frame_size: usize,
        offset: usize,
    },
}

impl OutgoingMessageFrames {
    fn new(message: Message, frame_size: Option<usize>) -> Self {
        on_common::log_t!(LogType::WSC; "new", "message_bytes|frame_size", message.len(), frame_size);
        let Some(frame_size) = frame_size.filter(|size| *size > 0) else {
            return Self::Single(Some(message));
        };
        match message {
            Message::Text(payload) if payload.len() > frame_size => Self::Fragmented {
                payload: utf8_payload(payload),
                first_opcode: OpData::Text,
                frame_size,
                offset: 0,
            },
            Message::Binary(payload) if payload.len() > frame_size => Self::Fragmented {
                payload,
                first_opcode: OpData::Binary,
                frame_size,
                offset: 0,
            },
            message => Self::Single(Some(message)),
        }
    }
}

impl Iterator for OutgoingMessageFrames {
    type Item = Message;

    fn next(&mut self) -> Option<Self::Item> {
        on_common::log_t!(LogType::WSC; "next");
        match self {
            Self::Single(message) => message.take(),
            Self::Fragmented {
                payload,
                first_opcode,
                frame_size,
                offset,
            } => {
                if *offset >= payload.len() {
                    return None;
                }
                let start = *offset;
                let end = start.saturating_add(*frame_size).min(payload.len());
                let opcode = if start == 0 {
                    *first_opcode
                } else {
                    OpData::Continue
                };
                *offset = end;
                Some(Message::Frame(Frame::message(
                    payload.slice(start..end),
                    OpCode::Data(opcode),
                    end == payload.len(),
                )))
            }
        }
    }
}

/// 取出 `Utf8Bytes` 的底层共享字节存储。
fn utf8_payload(payload: Utf8Bytes) -> Bytes {
    on_common::log_t!(LogType::WSC; "utf8_payload", "payload_bytes", payload.len());
    payload.into()
}

/// 向客户端工作线程报告指定连接代次的写循环终止错误。
///
/// 工作线程会用 `generation` 丢弃旧连接的迟到事件。若本代已由 worker 取消，则
/// 不再等待可能已满的事件通道，因为 worker 已掌握终止原因并正在等待本任务退出。
/// 若事件接收端已关闭则忽略发送失败。
async fn send_write_end(
    io_event_tx: &mpsc::Sender<IoEvent>,
    generation: u64,
    error: NetError,
    cancel: &CancellationToken,
) {
    on_common::log_t!(LogType::WSC; "send_write_end", "generation|error|cancelled", generation, format!("{error:?}"), cancel.is_cancelled());
    if matches!(error, NetError::Cancelled | NetError::ConnectionClosed) {
        on_common::log_s!(LogType::WSC; "send_write_end", "state|generation|error", "write_ended", generation, format!("{error:?}"));
    } else {
        on_common::log_e!(LogType::WSC; "send_write_end", "generation|error", generation, format!("{error:?}"));
    }
    tokio::select! {
        biased;
        _ = cancel.cancelled() => {
            on_common::log_s!(LogType::WSC; "send_write_end", "state|generation", "end_notification_cancelled", generation);
        }
        result = io_event_tx.send(IoEvent::WriteEnded { generation, error }) => {
            if result.is_err() {
                on_common::log_s!(LogType::WSC; "send_write_end", "state|generation", "worker_event_receiver_closed", generation);
            }
        }
    }
}

/// 以同一个终态错误结束一批已从队列移出的请求。
///
/// 对每条请求先按 UUID 与令牌删除匹配的待响应记录，再释放其任务/字节许可，
/// 并通知原始发送调用方。令牌检查可避免迟到清理删除后来复用相同 UUID 的记录。
pub(crate) fn fail_requests(
    requests: Vec<QueuedRequest>,
    pending_requests: &PendingRequestView,
    error: NetError,
) {
    on_common::log_t!(LogType::WSC; "fail_requests", "count|error", requests.len(), format!("{error:?}"));
    on_common::log_s!(LogType::WSC; "fail_requests", "state|count|error", "queued_requests_terminated", requests.len(), format!("{error:?}"));
    for request in requests {
        let terminal_error = request.terminal_error(error);
        remove_pending_for_request(pending_requests, &request, terminal_error);
        request.complete(Err(terminal_error));
    }
}

#[cfg(test)]
#[path = "ws_write/continuation_queue_tests.rs"]
mod continuation_queue_tests;

#[cfg(test)]
#[path = "ws_write/continuation_peer_tests.rs"]
mod continuation_peer_tests;

#[cfg(test)]
#[path = "ws_write/continuation_in_flight_tests.rs"]
mod continuation_in_flight_tests;

#[cfg(test)]
#[path = "ws_write/registration_cancellation_tests.rs"]
mod registration_cancellation_tests;

#[cfg(test)]
#[path = "ws_write/response_deadline_tests.rs"]
mod response_deadline_tests;

#[cfg(test)]
/// WebSocket 业务写队列与重连保留规则的单元测试。
mod tests {
    use super::*;
    use crate::api::traits::ws::ws_body::WsBody;
    use crate::api::traits::ws::ws_request_config::{
        DisconnectedTaskPolicy, WSRequestConfig, WSRequestPriority,
    };
    use crate::api::traits::ws::ws_request_trait::WSRequestTrait;
    use crate::module::ws_client::test_support::{
        check, check_eq, check_ne, test_error, TestResult,
    };
    use crate::module::ws_client::write::priority_write_queue::PriorityWriteQueue;
    use std::collections::BinaryHeap;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};
    use tokio::sync::oneshot;

    struct TimeoutTestRequest(&'static str);

    impl WSRequestTrait for TimeoutTestRequest {
        fn uuid(&self) -> String {
            self.0.to_string()
        }

        fn body(&self) -> Result<WsBody, NetError> {
            Ok(WsBody::Text(self.0.to_string()))
        }
    }

    #[tokio::test(start_paused = true)]
    async fn response_received_before_deadline_gets_one_bounded_claim_grace() -> TestResult {
        let pending = PendingRequestView::with_capacity(1);
        let config = WSRequestConfig::default();
        let (token, completion) = pending
            .reserve(Arc::new(TimeoutTestRequest("grace-success")), &config)
            .map_err(|error| test_error(format!("reserve request: {error:?}")))?;
        check!(pending.mark_writing("grace-success", token, 0, 41))?;
        let timeout_cancel = pending
            .mark_sent("grace-success", token)
            .ok_or_else(|| test_error("mark request sent"))?;
        let response_guard = pending.begin_response_dispatch(41);
        let timeout = tokio::spawn(expire_pending_after_response_timeout(
            pending.clone(),
            "grace-success".to_string(),
            token,
            timeout_cancel,
            Duration::from_secs(10),
            Duration::from_secs(3),
        ));
        tokio::task::yield_now().await;

        tokio::time::advance(Duration::from_secs(10)).await;
        tokio::task::yield_now().await;
        let survived_deadline = pending.len();
        let response_claimed = pending.take_request("grace-success", 41).is_some();
        drop(response_guard);
        let response_result = check_eq!(
            survived_deadline,
            1,
            "accepted response must survive deadline"
        )
        .and_then(|()| check!(response_claimed, "response must claim the tracked request"));
        if response_result.is_err() {
            timeout.abort();
            let _ = timeout.await;
            return response_result;
        }

        timeout
            .await
            .map_err(|error| test_error(format!("timeout task: {error:?}")))?;
        check_eq!(completion.wait().await, Ok(()))?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn blocked_response_cannot_extend_request_timeout_more_than_once() -> TestResult {
        let pending = PendingRequestView::with_capacity(1);
        let config = WSRequestConfig::default();
        let (token, completion) = pending
            .reserve(Arc::new(TimeoutTestRequest("grace-timeout")), &config)
            .map_err(|error| test_error(format!("reserve request: {error:?}")))?;
        check!(pending.mark_writing("grace-timeout", token, 0, 43))?;
        let timeout_cancel = pending
            .mark_sent("grace-timeout", token)
            .ok_or_else(|| test_error("mark request sent"))?;
        let _response_guard = pending.begin_response_dispatch(43);
        let timeout = tokio::spawn(expire_pending_after_response_timeout(
            pending.clone(),
            "grace-timeout".to_string(),
            token,
            timeout_cancel,
            Duration::from_secs(10),
            Duration::from_secs(3),
        ));
        tokio::task::yield_now().await;

        tokio::time::advance(Duration::from_secs(13)).await;
        tokio::task::yield_now().await;
        timeout
            .await
            .map_err(|error| test_error(format!("timeout task: {error:?}")))?;

        check_eq!(completion.wait().await, Err(NetError::TimeoutError))?;
        check!(pending.is_empty())?;
        Ok(())
    }

    #[derive(Clone, Copy)]
    enum InjectedControlKind {
        FlushAutomatic,
        PeerClose,
    }

    /// 记录收到的消息，并可在首个 data frame 入 sink 后异步注入控制指令。
    struct RecordingSink {
        messages: Arc<Mutex<Vec<Message>>>,
        inject_control: Option<mpsc::Sender<ControlMessage>>,
        injected_control_kind: InjectedControlKind,
        injected: bool,
        flushes: Arc<AtomicUsize>,
        fail_on_flush: Option<usize>,
        cancel_after_flush: Option<CancellationToken>,
        control_injection: Option<tokio::task::JoinHandle<TestResult>>,
    }

    impl RecordingSink {
        fn new(inject_control: Option<mpsc::Sender<ControlMessage>>) -> Self {
            Self {
                messages: Arc::new(Mutex::new(Vec::new())),
                inject_control,
                injected_control_kind: InjectedControlKind::FlushAutomatic,
                injected: false,
                flushes: Arc::new(AtomicUsize::new(0)),
                fail_on_flush: None,
                cancel_after_flush: None,
                control_injection: None,
            }
        }

        fn with_peer_close(control_tx: mpsc::Sender<ControlMessage>) -> Self {
            Self {
                messages: Arc::new(Mutex::new(Vec::new())),
                inject_control: Some(control_tx),
                injected_control_kind: InjectedControlKind::PeerClose,
                injected: false,
                flushes: Arc::new(AtomicUsize::new(0)),
                fail_on_flush: None,
                cancel_after_flush: None,
                control_injection: None,
            }
        }

        fn with_failing_automatic_flush(
            control_tx: mpsc::Sender<ControlMessage>,
            fail_on_flush: usize,
        ) -> Self {
            Self {
                messages: Arc::new(Mutex::new(Vec::new())),
                inject_control: Some(control_tx),
                injected_control_kind: InjectedControlKind::FlushAutomatic,
                injected: false,
                flushes: Arc::new(AtomicUsize::new(0)),
                fail_on_flush: Some(fail_on_flush),
                cancel_after_flush: None,
                control_injection: None,
            }
        }

        fn recorded(&self) -> Arc<Mutex<Vec<Message>>> {
            Arc::clone(&self.messages)
        }

        fn flushes(&self) -> Arc<AtomicUsize> {
            Arc::clone(&self.flushes)
        }

        async fn finish_injection(&mut self) -> TestResult {
            if let Some(injection) = self.control_injection.take() {
                injection.await??;
            }
            Ok(())
        }
    }

    impl Drop for RecordingSink {
        fn drop(&mut self) {
            if let Some(injection) = self.control_injection.take() {
                injection.abort();
            }
        }
    }

    impl Sink<Message> for RecordingSink {
        type Error = WsError;

        fn poll_ready(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, message: Message) -> Result<(), Self::Error> {
            let this = self.get_mut();
            let is_data_frame = matches!(
                &message,
                Message::Frame(frame) if matches!(frame.header().opcode, OpCode::Data(_))
            );
            this.messages
                .lock()
                .map_err(|error| {
                    WsError::Io(std::io::Error::other(format!(
                        "recorded messages lock: {error}"
                    )))
                })?
                .push(message);
            if is_data_frame && !this.injected {
                this.injected = true;
                if let Some(control_tx) = &this.inject_control {
                    let control_tx = control_tx.clone();
                    let control_kind = this.injected_control_kind;
                    this.control_injection = Some(tokio::spawn(async move {
                        let control = match control_kind {
                            InjectedControlKind::FlushAutomatic => ControlMessage::FlushAutomatic,
                            InjectedControlKind::PeerClose => {
                                let (done_tx, _done_rx) = oneshot::channel();
                                ControlMessage::PeerClose(done_tx)
                            }
                        };
                        control_tx.send(control).await.map_err(|error| {
                            test_error(format!("inject control message: {error:?}"))
                        })?;
                        Ok(())
                    }));
                }
            }
            Ok(())
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            let this = self.get_mut();
            let flush_count = this.flushes.fetch_add(1, Ordering::Relaxed) + 1;
            if this.fail_on_flush == Some(flush_count) {
                Poll::Ready(Err(WsError::Io(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "injected automatic flush failure",
                ))))
            } else {
                if let Some(cancel) = this.cancel_after_flush.take() {
                    cancel.cancel();
                }
                Poll::Ready(Ok(()))
            }
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn failed_control_injection_is_reported_to_the_test() -> TestResult {
        let (control_tx, control_rx) = mpsc::channel(1);
        drop(control_rx);
        let mut sink = RecordingSink::new(Some(control_tx));
        sink.send(Message::Frame(Frame::message(
            Bytes::from_static(b"fragment"),
            OpCode::Data(OpData::Binary),
            false,
        )))
        .await?;

        let failure = sink
            .finish_injection()
            .await
            .err()
            .ok_or_else(|| test_error("closed control channel must fail the injection"))?;
        check!(failure.to_string().contains("inject control message"))?;
        Ok(())
    }

    /// Records how many automatic-control flushes complete before the first data write starts.
    struct ControlFairnessSink {
        flushes: Arc<AtomicUsize>,
        flushes_before_first_data: Arc<AtomicUsize>,
    }

    impl ControlFairnessSink {
        fn new() -> Self {
            Self {
                flushes: Arc::new(AtomicUsize::new(0)),
                flushes_before_first_data: Arc::new(AtomicUsize::new(usize::MAX)),
            }
        }

        fn flushes_before_first_data(&self) -> Arc<AtomicUsize> {
            Arc::clone(&self.flushes_before_first_data)
        }
    }

    impl Sink<Message> for ControlFairnessSink {
        type Error = WsError;

        fn poll_ready(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, message: Message) -> Result<(), Self::Error> {
            let this = self.get_mut();
            let is_data = matches!(
                message,
                Message::Text(_) | Message::Binary(_) | Message::Frame(_)
            );
            if is_data {
                let observed = this.flushes.load(Ordering::Relaxed);
                let _ = this.flushes_before_first_data.compare_exchange(
                    usize::MAX,
                    observed,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                );
            }
            Ok(())
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            self.flushes.fetch_add(1, Ordering::Relaxed);
            Poll::Ready(Ok(()))
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    /// 接受消息后在指定的 flush 次数返回 I/O 错误，模拟提交边界不确定。
    struct NthFlushErrorSink {
        messages: Arc<Mutex<Vec<Message>>>,
        flush_count: usize,
        fail_on_flush: usize,
    }

    impl NthFlushErrorSink {
        fn new(fail_on_flush: usize) -> Self {
            Self {
                messages: Arc::new(Mutex::new(Vec::new())),
                flush_count: 0,
                fail_on_flush,
            }
        }

        fn recorded(&self) -> Arc<Mutex<Vec<Message>>> {
            Arc::clone(&self.messages)
        }
    }

    impl Sink<Message> for NthFlushErrorSink {
        type Error = WsError;

        fn poll_ready(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, message: Message) -> Result<(), Self::Error> {
            self.messages
                .lock()
                .map_err(|error| {
                    WsError::Io(std::io::Error::other(format!(
                        "recorded messages lock: {error}"
                    )))
                })?
                .push(message);
            Ok(())
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            let this = self.get_mut();
            this.flush_count += 1;
            if this.flush_count == this.fail_on_flush {
                Poll::Ready(Err(WsError::Io(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "injected flush failure",
                ))))
            } else {
                Poll::Ready(Ok(()))
            }
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    /// Claims the correlated response while `Sink::send` is flushing, then fails that flush.
    struct ResponseClaimThenFlushErrorSink {
        pending: PendingRequestView,
        uuid: &'static str,
        generation: u64,
        claimed: bool,
        claim_result: Option<TestResult>,
    }

    impl Sink<Message> for ResponseClaimThenFlushErrorSink {
        type Error = WsError;

        fn poll_ready(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, _message: Message) -> Result<(), Self::Error> {
            Ok(())
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            let this = self.get_mut();
            if !this.claimed {
                this.claimed = true;
                this.claim_result = Some(check!(
                    this.pending
                        .take_request(this.uuid, this.generation)
                        .is_some(),
                    "response must claim the writing request"
                ));
            }
            Poll::Ready(Err(WsError::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "flush failed after peer response",
            ))))
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    /// 永远不接受写入的 sink，用于验证控制写 deadline。
    #[derive(Default)]
    struct PendingSink {
        unexpected_send: bool,
    }

    impl PendingSink {
        fn verify(&self) -> TestResult {
            check!(
                !self.unexpected_send,
                "pending sink must not accept a message"
            )
        }
    }

    impl Sink<Message> for PendingSink {
        type Error = WsError;

        fn poll_ready(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Pending
        }

        fn start_send(self: Pin<&mut Self>, _message: Message) -> Result<(), Self::Error> {
            self.get_mut().unexpected_send = true;
            Err(WsError::Io(std::io::Error::other(
                "pending sink must not accept a message",
            )))
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Pending
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Pending
        }
    }

    /// 接受一个 frame 后让 flush 永久 Pending，用于模拟底层写缓冲无法排空。
    struct FlushPendingSink {
        messages: Arc<Mutex<Vec<Message>>>,
        started_tx: Option<oneshot::Sender<()>>,
    }

    impl FlushPendingSink {
        fn new(started_tx: oneshot::Sender<()>) -> Self {
            Self {
                messages: Arc::new(Mutex::new(Vec::new())),
                started_tx: Some(started_tx),
            }
        }

        fn recorded(&self) -> Arc<Mutex<Vec<Message>>> {
            Arc::clone(&self.messages)
        }
    }

    impl Sink<Message> for FlushPendingSink {
        type Error = WsError;

        fn poll_ready(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, message: Message) -> Result<(), Self::Error> {
            let this = self.get_mut();
            this.messages
                .lock()
                .map_err(|error| {
                    WsError::Io(std::io::Error::other(format!(
                        "recorded messages lock: {error}"
                    )))
                })?
                .push(message);
            if let Some(started_tx) = this.started_tx.take() {
                let _ = started_tx.send(());
            }
            Ok(())
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Pending
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Pending
        }
    }

    /// 断言分片 opcode/FIN，并返回重新拼接后的 payload。
    fn check_fragment_frames(
        message: Message,
        frame_size: usize,
        first_opcode: OpData,
    ) -> TestResult<Vec<u8>> {
        let frames = OutgoingMessageFrames::new(message, Some(frame_size)).collect::<Vec<_>>();
        check!(frames.len() >= 2)?;
        let frame_count = frames.len();
        let mut reconstructed = Vec::new();
        for (index, message) in frames.into_iter().enumerate() {
            let Message::Frame(frame) = message else {
                return Err(test_error("fragmented message must emit raw frames"));
            };
            let expected_opcode = if index == 0 {
                first_opcode
            } else {
                OpData::Continue
            };
            check_eq!(frame.header().opcode, OpCode::Data(expected_opcode))?;
            check_eq!(frame.header().is_final, index + 1 == frame_count)?;
            check!(frame.payload().len() <= frame_size)?;
            reconstructed.extend_from_slice(frame.payload());
        }
        Ok(reconstructed)
    }

    /// 构造不占用信号量许可的测试请求。
    ///
    /// UUID 与待响应令牌均由 `sequence` 派生，其他请求配置使用默认值。
    fn request(priority: WSRequestPriority, sequence: u64) -> QueuedRequest {
        let (result_tx, _result_rx) = oneshot::channel();
        QueuedRequest {
            uuid: sequence.to_string(),
            pending_token: Some(sequence),
            message: Message::Text(sequence.to_string().into()),
            config: WSRequestConfig {
                priority,
                ..WSRequestConfig::default()
            },
            attempt: 0,
            prior_delivery_unknown: false,
            result_tx: Some(result_tx),
            dispatch_cancel: CancellationToken::new(),
            dispatch_phase: crate::module::ws_client::write::queued_request::DispatchPhase::new(),
            admission_cancel: None,
            sequence,
            slot_permit: None,
            byte_permit: None,
        }
    }

    /// Constructs an untracked handler-level request whose receipt can be asserted independently.
    fn request_with_receipt(
        uuid: &'static str,
        message: Message,
        config: WSRequestConfig,
        sequence: u64,
        dispatch_cancel: CancellationToken,
    ) -> (QueuedRequest, oneshot::Receiver<Result<(), NetError>>) {
        let (result_tx, result_rx) = oneshot::channel();
        (
            QueuedRequest {
                uuid: uuid.to_string(),
                pending_token: None,
                message,
                config,
                attempt: 0,
                prior_delivery_unknown: false,
                result_tx: Some(result_tx),
                dispatch_cancel,
                dispatch_phase: crate::module::ws_client::write::queued_request::DispatchPhase::new(
                ),
                admission_cancel: None,
                sequence,
                slot_permit: None,
                byte_permit: None,
            },
            result_rx,
        )
    }

    #[test]
    /// 验证严格优先级调度以及相同优先级内的 FIFO 顺序。
    fn priority_is_strict_and_equal_priority_is_fifo() -> TestResult {
        let mut heap = BinaryHeap::new();
        heap.push(request(WSRequestPriority::Low, 1));
        heap.push(request(WSRequestPriority::High, 2));
        heap.push(request(WSRequestPriority::High, 3));
        heap.push(request(WSRequestPriority::Normal, 4));

        check_eq!(heap.pop().map(|item| item.sequence), Some(2))?;
        check_eq!(heap.pop().map(|item| item.sequence), Some(3))?;
        check_eq!(heap.pop().map(|item| item.sequence), Some(4))?;
        check_eq!(heap.pop().map(|item| item.sequence), Some(1))?;
        Ok(())
    }

    #[tokio::test]
    async fn data_frame_failure_logs_original_cause_without_changing_delivery_unknown() -> TestResult
    {
        use on_common::log::log_level::LogLevel;
        use on_common::log::logger::Logger;

        let (log_tx, log_rx) = std::sync::mpsc::channel();
        let _subscription = Logger::register_log_listener_with_capacity(
            Box::new(move |record| {
                if record.tag == "ON_WSC-send_data_frame-E"
                    && record.content.contains("ws-write-log-original-cause")
                {
                    let _ = log_tx.send(record);
                }
            }),
            &[LogType::WSC],
            16_384,
        )
        .map_err(|error| test_error(format!("log subscription: {error:?}")))?;
        let mut sink = Box::pin(futures::sink::unfold((), |(), _message: Message| async {
            Err::<(), _>(WsError::Io(std::io::Error::other(
                "ws-write-log-original-cause",
            )))
        }));
        let connection_cancel = CancellationToken::new();
        let dispatch_cancel = CancellationToken::new();
        let failure = send_data_frame(
            &mut sink,
            Message::Binary(Bytes::from_static(b"payload must not appear in logs")),
            false,
            Instant::now() + Duration::from_secs(1),
            Duration::from_secs(1),
            &connection_cancel,
            &dispatch_cancel,
        )
        .await
        .err()
        .ok_or_else(|| test_error("expected transport failure"))?;
        check_eq!(failure.request_error, NetError::DeliveryUnknown)?;
        let record = log_rx
            .recv_timeout(Duration::from_secs(2))
            .map_err(|error| test_error(format!("original error log: {error:?}")))?;
        check_eq!(record.log_type, LogType::WSC)?;
        check_eq!(record.level, LogLevel::Error)?;
        check!(!record.content.contains("payload must not appear"))?;
        Ok(())
    }

    #[tokio::test]
    /// 验证任务容量被占满时后续入队会等待，并遵守统一的入队超时。
    async fn bounded_queue_waits_and_honors_enqueue_timeout() -> TestResult {
        let queue = PriorityWriteQueue::new(1, 32)
            .map_err(|error| test_error(format!("create queue: {error:?}")))?;
        let shutdown = CancellationToken::new();
        let first = queue
            .enqueue(
                "first".to_string(),
                Some(1),
                Message::Text("first".into()),
                5,
                WSRequestConfig::default(),
                &shutdown,
                CancellationToken::new(),
                crate::module::ws_client::write::queued_request::DispatchPhase::new(),
                CancellationToken::new(),
            )
            .await;
        check!(first.is_ok())?;

        let second = queue
            .enqueue(
                "second".to_string(),
                Some(2),
                Message::Text("second".into()),
                6,
                WSRequestConfig {
                    enqueue_timeout: Some(Duration::from_millis(10)),
                    ..WSRequestConfig::default()
                },
                &shutdown,
                CancellationToken::new(),
                crate::module::ws_client::write::queued_request::DispatchPhase::new(),
                CancellationToken::new(),
            )
            .await;
        check!(matches!(second, Err(NetError::TimeoutError)))?;
        for request in queue.drain() {
            request.complete(Err(NetError::Cancelled));
        }
        Ok(())
    }

    #[test]
    /// 验证断线时默认拒绝请求会被移出，而正在执行的幂等重试会被保留。
    fn reconnect_retains_only_waiting_or_idempotent_retry_tasks() -> TestResult {
        let queue = PriorityWriteQueue::new(4, 128)
            .map_err(|error| test_error(format!("create queue: {error:?}")))?;
        let rejected = request(WSRequestPriority::Normal, 1);
        let mut retrying = request(WSRequestPriority::Normal, 2);
        retrying.config.idempotent = true;
        retrying.config.send_retry_count = 1;
        retrying.attempt = 1;
        check!(queue.push_existing(rejected).is_ok())?;
        check!(queue.push_existing(retrying).is_ok())?;

        let rejected = queue.drain_rejected_on_disconnect();
        check_eq!(rejected.len(), 1)?;
        check_eq!(rejected[0].uuid, "1")?;
        let retained = queue.drain();
        check_eq!(retained.len(), 1)?;
        check_eq!(retained[0].uuid, "2")?;
        Ok(())
    }

    #[tokio::test]
    /// 验证 shutdown 清队列不会覆盖幂等重试此前发生的投递不确定性。
    async fn terminal_drain_preserves_prior_delivery_unknown() -> TestResult {
        let (result_tx, result_rx) = oneshot::channel();
        let request = QueuedRequest {
            uuid: "uncertain".to_string(),
            pending_token: None,
            message: Message::Text("uncertain".into()),
            config: WSRequestConfig::default(),
            attempt: 1,
            prior_delivery_unknown: true,
            result_tx: Some(result_tx),
            dispatch_cancel: CancellationToken::new(),
            dispatch_phase: crate::module::ws_client::write::queued_request::DispatchPhase::new(),
            admission_cancel: None,
            sequence: 1,
            slot_permit: None,
            byte_permit: None,
        };

        fail_requests(
            vec![request],
            &PendingRequestView::default(),
            NetError::Cancelled,
        );

        check_eq!(result_rx.await, Ok(Err(NetError::DeliveryUnknown)))?;
        Ok(())
    }

    #[tokio::test]
    /// writer 在 sink commit 后、通知发送方前被中止时仍保守报告投递不确定。
    async fn drop_after_sink_commit_reports_delivery_unknown() -> TestResult {
        let (result_tx, result_rx) = oneshot::channel();
        let dispatch_phase = crate::module::ws_client::write::queued_request::DispatchPhase::new();
        check!(dispatch_phase.start_writing())?;
        check!(dispatch_phase.commit())?;
        let request = QueuedRequest {
            uuid: "committed-drop".to_string(),
            pending_token: None,
            message: Message::Text("committed-drop".into()),
            config: WSRequestConfig::default(),
            attempt: 0,
            prior_delivery_unknown: false,
            result_tx: Some(result_tx),
            dispatch_cancel: CancellationToken::new(),
            dispatch_phase,
            admission_cancel: None,
            sequence: 1,
            slot_permit: None,
            byte_permit: None,
        };

        drop(request);

        check_eq!(result_rx.await, Ok(Err(NetError::DeliveryUnknown)))?;
        Ok(())
    }

    #[test]
    /// 验证 Binary 使用首个 Binary 帧、后续 continuation 和唯一 FIN 末帧。
    fn binary_fragmentation_preserves_payload_and_frame_sequence() -> TestResult {
        let payload = b"0123456789";
        let reconstructed = check_fragment_frames(
            Message::Binary(Bytes::copy_from_slice(payload)),
            3,
            OpData::Binary,
        )?;
        check_eq!(reconstructed, payload)?;
        Ok(())
    }

    #[test]
    /// 验证 Text 即使在 UTF-8 码点内部切帧，完整重组后的消息仍保持原字节序列。
    fn text_fragmentation_preserves_utf8_payload_and_frame_sequence() -> TestResult {
        let text = "你好 websocket";
        let reconstructed = check_fragment_frames(Message::Text(text.into()), 2, OpData::Text)?;
        check_eq!(reconstructed, text.as_bytes())?;
        check_eq!(std::str::from_utf8(&reconstructed), Ok(text))?;
        Ok(())
    }

    #[tokio::test]
    /// 验证首个 data frame 写完时到达的 Pong 会先于 continuation frame 写出。
    async fn ready_control_is_written_between_fragmented_data_frames() -> TestResult {
        let (control_tx, mut control_rx) = mpsc::channel(4);
        let mut sink = RecordingSink::new(Some(control_tx));
        let recorded = sink.recorded();
        let flushes = sink.flushes();
        let cancel = CancellationToken::new();
        let heartbeat_state = HeartbeatState::new(1);
        let heartbeat_interval = Duration::from_secs(3_600);
        let mut heartbeat =
            tokio::time::interval_at(Instant::now() + heartbeat_interval, heartbeat_interval);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let result = send_request_message(
            &mut sink,
            Message::Binary(Bytes::from_static(b"abcdef")),
            Some(3),
            MAX_READY_CONTROLS_PER_BOUNDARY,
            Instant::now() + Duration::from_secs(1),
            &mut control_rx,
            Duration::from_secs(1),
            Duration::from_secs(1),
            &mut heartbeat,
            &heartbeat_state,
            Duration::from_secs(7_200),
            &cancel,
            &CancellationToken::new(),
        )
        .await;
        sink.finish_injection().await?;
        check_eq!(result, Ok(()))?;

        let messages = recorded
            .lock()
            .map_err(|error| test_error(format!("recorded messages lock: {error:?}")))?;
        check_eq!(messages.len(), 2)?;
        check!(matches!(
            &messages[0],
            Message::Frame(frame)
                if frame.header().opcode == OpCode::Data(OpData::Binary)
                    && !frame.header().is_final
                    && frame.payload() == b"abc"
        ))?;
        check!(matches!(
            &messages[1],
            Message::Frame(frame)
                if frame.header().opcode == OpCode::Data(OpData::Continue)
                    && frame.header().is_final
                    && frame.payload() == b"def"
        ))?;
        check_eq!(flushes.load(Ordering::Relaxed), 3)?;
        Ok(())
    }

    /// Holds the last automatic flush at a selected data-frame boundary. The test can then
    /// release the flush and stop at the boundary's fairness yield before continuation.
    struct BoundaryFlushGateSink {
        messages: Vec<Message>,
        controls: mpsc::Sender<ControlMessage>,
        completed_flushes: usize,
        preceding_data_frames: usize,
        gate_after_flushes: usize,
        entered: Option<oneshot::Sender<()>>,
        release: Option<oneshot::Receiver<()>>,
    }

    impl Sink<Message> for BoundaryFlushGateSink {
        type Error = WsError;

        fn poll_ready(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, message: Message) -> Result<(), Self::Error> {
            let this = self.get_mut();
            this.messages.push(message);
            if this.messages.len() == this.preceding_data_frames {
                for _ in 0..MAX_READY_CONTROLS_PER_BOUNDARY {
                    this.controls
                        .try_send(ControlMessage::FlushAutomatic)
                        .map_err(|error| {
                            WsError::Io(std::io::Error::other(format!(
                                "inject frame-boundary control: {error}"
                            )))
                        })?;
                }
            }
            Ok(())
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            let this = self.get_mut();
            if this.completed_flushes == this.gate_after_flushes {
                if let Some(entered) = this.entered.take() {
                    if entered.send(()).is_err() {
                        return Poll::Ready(Err(WsError::Io(std::io::Error::other(
                            "boundary observer closed",
                        ))));
                    }
                }
                if let Some(release) = this.release.as_mut() {
                    match std::future::Future::poll(Pin::new(release), context) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Ok(())) => this.release = None,
                        Poll::Ready(Err(error)) => {
                            return Poll::Ready(Err(WsError::Io(std::io::Error::other(format!(
                                "boundary release closed: {error}"
                            )))));
                        }
                    }
                }
            }
            let Some(completed) = this.completed_flushes.checked_add(1) else {
                return Poll::Ready(Err(WsError::Io(std::io::Error::other(
                    "boundary flush counter overflow",
                ))));
            };
            this.completed_flushes = completed;
            Poll::Ready(Ok(()))
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    #[derive(Clone, Copy)]
    enum BoundaryInterruption {
        DispatchCancelled,
        ConnectionCancelled,
        Deadline,
    }

    async fn check_fragmented_boundary_interruption(
        interruption: BoundaryInterruption,
    ) -> TestResult {
        for (message, first_opcode) in [
            (
                Message::Binary(Bytes::from_static(b"abcdefghi")),
                OpData::Binary,
            ),
            (Message::Text("a中b文c".into()), OpData::Text),
        ] {
            for preceding_data_frames in [1, 2] {
                check_boundary_interruption_case(
                    interruption,
                    message.clone(),
                    first_opcode,
                    preceding_data_frames,
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn check_boundary_interruption_case(
        interruption: BoundaryInterruption,
        message: Message,
        first_opcode: OpData,
        preceding_data_frames: usize,
    ) -> TestResult {
        let expected_payload = match &message {
            Message::Binary(payload) => payload.to_vec(),
            Message::Text(payload) => payload.as_bytes().to_vec(),
            _ => return Err(test_error("boundary case requires a data message")),
        };
        let gate_after_flushes = preceding_data_frames
            .checked_add(MAX_READY_CONTROLS_PER_BOUNDARY)
            .and_then(|flushes| flushes.checked_sub(1))
            .ok_or_else(|| test_error("boundary flush index overflow"))?;
        let (control_tx, mut control_rx) = mpsc::channel(MAX_READY_CONTROLS_PER_BOUNDARY);
        if preceding_data_frames == 0 {
            for _ in 0..MAX_READY_CONTROLS_PER_BOUNDARY {
                control_tx
                    .try_send(ControlMessage::FlushAutomatic)
                    .map_err(|error| test_error(format!("queue pre-data control: {error}")))?;
            }
        }
        let (entered, mut entered_rx) = oneshot::channel();
        let (release_tx, release) = oneshot::channel();
        let mut sink = BoundaryFlushGateSink {
            messages: Vec::new(),
            controls: control_tx,
            completed_flushes: 0,
            preceding_data_frames,
            gate_after_flushes,
            entered: Some(entered),
            release: Some(release),
        };
        let connection_cancel = CancellationToken::new();
        let dispatch_cancel = CancellationToken::new();
        let period = Duration::from_secs(3_600);
        let mut heartbeat = tokio::time::interval_at(Instant::now() + period, period);
        let heartbeat_state = HeartbeatState::new(1);
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut sending = Box::pin(send_request_message(
            &mut sink,
            message,
            Some(3),
            MAX_READY_CONTROLS_PER_BOUNDARY,
            deadline,
            &mut control_rx,
            Duration::from_secs(10),
            Duration::from_secs(10),
            &mut heartbeat,
            &heartbeat_state,
            period,
            &connection_cancel,
            &dispatch_cancel,
        ));

        // Each preceding frame completes and yields. The next poll reaches the
        // final control flush after the selected frame's outer pre-send checks.
        for _ in 0..preceding_data_frames {
            check!(sending.as_mut().now_or_never().is_none())?;
        }
        check!(sending.as_mut().now_or_never().is_none())?;
        entered_rx
            .try_recv()
            .map_err(|error| test_error(format!("boundary flush was not reached: {error}")))?;
        release_tx
            .send(())
            .map_err(|_| test_error("boundary flush release receiver closed"))?;
        // Finish every control write, then stop at the fairness yield. Injection
        // here must reach send_data_frame's prechecks, not a control failure.
        check!(sending.as_mut().now_or_never().is_none())?;

        match interruption {
            BoundaryInterruption::DispatchCancelled => dispatch_cancel.cancel(),
            BoundaryInterruption::ConnectionCancelled => connection_cancel.cancel(),
            BoundaryInterruption::Deadline => {
                tokio::time::advance(deadline.saturating_duration_since(Instant::now())).await;
            }
        }
        let failure = sending
            .await
            .err()
            .ok_or_else(|| test_error("interrupted fragmented request unexpectedly completed"))?;
        check_eq!(sink.messages.len(), preceding_data_frames)?;
        let mut written_payload = Vec::new();
        for (index, message) in sink.messages.iter().enumerate() {
            let Message::Frame(frame) = message else {
                return Err(test_error("boundary case must emit raw data frames"));
            };
            let expected_opcode = if index == 0 {
                first_opcode
            } else {
                OpData::Continue
            };
            check_eq!(frame.header().opcode, OpCode::Data(expected_opcode))?;
            check!(!frame.header().is_final)?;
            check_eq!(frame.payload().len(), 3)?;
            written_payload.extend_from_slice(frame.payload());
        }
        let expected_prefix_len = preceding_data_frames
            .checked_mul(3)
            .ok_or_else(|| test_error("boundary prefix length overflow"))?;
        let expected_prefix = expected_payload
            .get(..expected_prefix_len)
            .ok_or_else(|| test_error("boundary prefix exceeds payload"))?;
        check_eq!(written_payload.as_slice(), expected_prefix)?;
        if first_opcode == OpData::Text && preceding_data_frames > 0 {
            check!(
                std::str::from_utf8(&written_payload).is_err(),
                "Text cases must stop inside a UTF-8 code point"
            )?;
        }
        let data_started = preceding_data_frames > 0;
        let (expected_error, expected_action, expected_requeue) = match interruption {
            BoundaryInterruption::DispatchCancelled => (
                if data_started {
                    NetError::DeliveryUnknown
                } else {
                    NetError::Cancelled
                },
                if data_started {
                    RequestAction::StopWithError(NetError::DeliveryUnknown)
                } else {
                    RequestAction::Continue
                },
                RequestRequeue::Never,
            ),
            BoundaryInterruption::Deadline => (
                if data_started {
                    NetError::DeliveryUnknown
                } else {
                    NetError::TimeoutError
                },
                if data_started {
                    RequestAction::StopWithError(NetError::DeliveryUnknown)
                } else {
                    RequestAction::Continue
                },
                RequestRequeue::Never,
            ),
            BoundaryInterruption::ConnectionCancelled => (
                if data_started {
                    NetError::DeliveryUnknown
                } else {
                    NetError::Cancelled
                },
                RequestAction::Stop,
                if data_started {
                    RequestRequeue::IdempotentRetry
                } else {
                    RequestRequeue::WaitForReconnect
                },
            ),
        };
        check_eq!(failure.request_error, expected_error)?;
        check_eq!(failure.connection_action, expected_action)?;
        check_eq!(failure.requeue, expected_requeue)?;
        Ok(())
    }

    async fn check_first_frame_boundary_interruption(
        interruption: BoundaryInterruption,
    ) -> TestResult {
        for (message, first_opcode) in [
            (
                Message::Binary(Bytes::from_static(b"abcdefghi")),
                OpData::Binary,
            ),
            (Message::Text("a中b文c".into()), OpData::Text),
        ] {
            check_boundary_interruption_case(interruption, message, first_opcode, 0).await?;
        }
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn fragmented_boundary_dispatch_cancel_preserves_written_prefix() -> TestResult {
        check_fragmented_boundary_interruption(BoundaryInterruption::DispatchCancelled).await
    }

    #[tokio::test(start_paused = true)]
    async fn fragmented_boundary_connection_cancel_preserves_written_prefix() -> TestResult {
        check_fragmented_boundary_interruption(BoundaryInterruption::ConnectionCancelled).await
    }

    #[tokio::test(start_paused = true)]
    async fn fragmented_boundary_deadline_preserves_written_prefix() -> TestResult {
        check_fragmented_boundary_interruption(BoundaryInterruption::Deadline).await
    }

    #[tokio::test(start_paused = true)]
    async fn first_frame_boundary_dispatch_cancel_preserves_unsent_state() -> TestResult {
        check_first_frame_boundary_interruption(BoundaryInterruption::DispatchCancelled).await
    }

    #[tokio::test(start_paused = true)]
    async fn first_frame_boundary_connection_cancel_preserves_unsent_state() -> TestResult {
        check_first_frame_boundary_interruption(BoundaryInterruption::ConnectionCancelled).await
    }

    #[tokio::test(start_paused = true)]
    async fn first_frame_boundary_deadline_preserves_unsent_state() -> TestResult {
        check_first_frame_boundary_interruption(BoundaryInterruption::Deadline).await
    }

    #[tokio::test]
    async fn peer_close_before_first_frame_requeues_without_mutating_order_or_attempt() -> TestResult
    {
        let source_queue = PriorityWriteQueue::new(1, 128)
            .map_err(|error| test_error(format!("source queue: {error:?}")))?;
        let config = WSRequestConfig {
            disconnected_policy: DisconnectedTaskPolicy::WaitForReconnect,
            ..WSRequestConfig::default()
        };
        let sequence = 77;
        let (request, mut receipt) = request_with_receipt(
            "peer-close-before-first-frame",
            Message::Text("business".into()),
            config,
            sequence,
            CancellationToken::new(),
        );
        let (control_tx, mut control_rx) = mpsc::channel(1);
        let (done_tx, done_rx) = oneshot::channel();
        control_tx
            .send(ControlMessage::PeerClose(done_tx))
            .await
            .map_err(|error| test_error(format!("queue peer Close: {error:?}")))?;
        let due_at = Instant::now()
            .checked_sub(Duration::from_millis(1))
            .ok_or_else(|| test_error("test instant supports subtraction"))?;
        let mut heartbeat = tokio::time::interval_at(due_at, Duration::from_secs(3_600));
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
        tokio::task::yield_now().await;
        let mut sink = RecordingSink::new(None);
        let recorded = sink.recorded();
        let flushes = sink.flushes();

        let action = handle_queued_request(
            &mut sink,
            request,
            &source_queue,
            &PendingRequestView::default(),
            71,
            None,
            MAX_READY_CONTROLS_PER_BOUNDARY,
            &mut control_rx,
            Duration::from_secs(1),
            Duration::from_secs(1),
            &mut heartbeat,
            &HeartbeatState::new(71),
            Duration::from_secs(7_200),
            Duration::from_secs(1),
            &CancellationToken::new(),
        )
        .await;

        check_eq!(action, RequestAction::Stop)?;
        check_eq!(done_rx.await, Ok(()), "peer Close reply must be flushed")?;
        check_eq!(flushes.load(Ordering::Relaxed), 1)?;
        check!(
            recorded
                .lock()
                .map_err(|error| test_error(format!("recorded messages lock: {error:?}")))?
                .is_empty(),
            "neither heartbeat Ping nor business data may overtake peer Close"
        )?;
        check!(matches!(
            receipt.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ))?;
        let requeued = source_queue
            .try_next()
            .ok_or_else(|| test_error("request must wait for reconnect"))?;
        check_eq!(requeued.sequence, sequence)?;
        check_eq!(requeued.attempt, 0)?;
        check!(!requeued.prior_delivery_unknown)?;
        requeued.complete(Err(NetError::Cancelled));
        check_eq!(receipt.await, Ok(Err(NetError::Cancelled)))?;
        Ok(())
    }

    #[tokio::test]
    async fn disconnected_control_channel_before_first_frame_requeues_waiting_request() -> TestResult
    {
        let source_queue = PriorityWriteQueue::new(1, 128)
            .map_err(|error| test_error(format!("source queue: {error:?}")))?;
        let config = WSRequestConfig {
            disconnected_policy: DisconnectedTaskPolicy::WaitForReconnect,
            ..WSRequestConfig::default()
        };
        let (request, mut receipt) = request_with_receipt(
            "control-channel-disconnected",
            Message::Text("business".into()),
            config,
            79,
            CancellationToken::new(),
        );
        let (control_tx, mut control_rx) = mpsc::channel(1);
        drop(control_tx);
        let heartbeat_interval = Duration::from_secs(3_600);
        let mut heartbeat =
            tokio::time::interval_at(Instant::now() + heartbeat_interval, heartbeat_interval);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut sink = RecordingSink::new(None);

        let action = handle_queued_request(
            &mut sink,
            request,
            &source_queue,
            &PendingRequestView::default(),
            73,
            None,
            0,
            &mut control_rx,
            Duration::from_secs(1),
            Duration::from_secs(1),
            &mut heartbeat,
            &HeartbeatState::new(73),
            Duration::from_secs(7_200),
            Duration::from_secs(1),
            &CancellationToken::new(),
        )
        .await;

        check_eq!(action, RequestAction::Stop)?;
        check!(matches!(
            receipt.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ))?;
        let requeued = source_queue
            .try_next()
            .ok_or_else(|| test_error("request must wait for reconnect"))?;
        check_eq!(requeued.sequence, 79)?;
        check_eq!(requeued.attempt, 0)?;
        check!(!requeued.prior_delivery_unknown)?;
        requeued.complete(Err(NetError::Cancelled));
        check_eq!(receipt.await, Ok(Err(NetError::Cancelled)))?;
        Ok(())
    }

    #[tokio::test]
    async fn heartbeat_timeout_before_first_frame_requeues_waiting_request() -> TestResult {
        let source_queue = PriorityWriteQueue::new(1, 128)
            .map_err(|error| test_error(format!("source queue: {error:?}")))?;
        let config = WSRequestConfig {
            disconnected_policy: DisconnectedTaskPolicy::WaitForReconnect,
            ..WSRequestConfig::default()
        };
        let (request, mut receipt) = request_with_receipt(
            "heartbeat-before-first-frame",
            Message::Text("business".into()),
            config,
            83,
            CancellationToken::new(),
        );
        let (_control_tx, mut control_rx) = mpsc::channel(1);
        let heartbeat_interval = Duration::from_secs(3_600);
        let mut heartbeat =
            tokio::time::interval_at(Instant::now() + heartbeat_interval, heartbeat_interval);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let heartbeat_state = HeartbeatState::new(75);
        let now = Instant::now();
        let HeartbeatTick::SendProbe(payload) = heartbeat_state.on_tick(now, Duration::ZERO) else {
            return Err(test_error("first heartbeat tick must create a probe"));
        };
        check!(heartbeat_state.mark_sent(payload.as_ref(), now))?;
        let mut sink = RecordingSink::new(None);

        let action = handle_queued_request(
            &mut sink,
            request,
            &source_queue,
            &PendingRequestView::default(),
            75,
            None,
            MAX_READY_CONTROLS_PER_BOUNDARY,
            &mut control_rx,
            Duration::from_secs(1),
            Duration::from_secs(1),
            &mut heartbeat,
            &heartbeat_state,
            Duration::ZERO,
            Duration::from_secs(1),
            &CancellationToken::new(),
        )
        .await;

        check_eq!(
            action,
            RequestAction::StopWithError(NetError::SocketRecvTimeout)
        )?;
        check!(matches!(
            receipt.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ))?;
        let requeued = source_queue
            .try_next()
            .ok_or_else(|| test_error("request must wait for reconnect"))?;
        check_eq!(requeued.sequence, 83)?;
        check_eq!(requeued.attempt, 0)?;
        check!(!requeued.prior_delivery_unknown)?;
        requeued.complete(Err(NetError::Cancelled));
        check_eq!(receipt.await, Ok(Err(NetError::Cancelled)))?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn control_timeout_before_first_frame_is_known_unsent_for_reject_policy() -> TestResult {
        let mut sink = PendingSink::default();
        let source_queue = PriorityWriteQueue::new(1, 128)
            .map_err(|error| test_error(format!("source queue: {error:?}")))?;
        let (request, receipt) = request_with_receipt(
            "reject-known-unsent",
            Message::Text("business".into()),
            WSRequestConfig::default(),
            89,
            CancellationToken::new(),
        );
        let (control_tx, mut control_rx) = mpsc::channel(1);
        control_tx
            .send(ControlMessage::FlushAutomatic)
            .await
            .map_err(|error| test_error(format!("queue automatic Pong flush: {error:?}")))?;
        let heartbeat_interval = Duration::from_secs(3_600);
        let mut heartbeat =
            tokio::time::interval_at(Instant::now() + heartbeat_interval, heartbeat_interval);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let action = handle_queued_request(
            &mut sink,
            request,
            &source_queue,
            &PendingRequestView::default(),
            77,
            None,
            MAX_READY_CONTROLS_PER_BOUNDARY,
            &mut control_rx,
            Duration::from_millis(25),
            Duration::from_secs(1),
            &mut heartbeat,
            &HeartbeatState::new(77),
            Duration::from_secs(7_200),
            Duration::from_secs(1),
            &CancellationToken::new(),
        )
        .await;

        sink.verify()?;
        check_eq!(
            action,
            RequestAction::StopWithError(NetError::DeliveryUnknown),
            "the ambiguous control write still retires the shared connection"
        )?;
        check_eq!(receipt.await, Ok(Err(NetError::ConnectionClosed)))?;
        check!(source_queue.try_next().is_none())?;
        Ok(())
    }

    #[tokio::test]
    async fn local_close_dispatch_cancel_and_deadline_never_requeue() -> TestResult {
        let pending = PendingRequestView::default();
        let waiting_config = WSRequestConfig {
            disconnected_policy: DisconnectedTaskPolicy::WaitForReconnect,
            ..WSRequestConfig::default()
        };

        let close_queue = PriorityWriteQueue::new(1, 128)
            .map_err(|error| test_error(format!("close queue: {error:?}")))?;
        let (close_request, close_receipt) = request_with_receipt(
            "local-close",
            Message::Text("business".into()),
            waiting_config.clone(),
            97,
            CancellationToken::new(),
        );
        let (close_tx, mut close_rx) = mpsc::channel(1);
        let (done_tx, done_rx) = oneshot::channel();
        close_tx
            .send(ControlMessage::Close(done_tx))
            .await
            .map_err(|error| test_error(format!("queue local Close: {error:?}")))?;
        let period = Duration::from_secs(3_600);
        let mut close_heartbeat = tokio::time::interval_at(Instant::now() + period, period);
        let mut close_sink = RecordingSink::new(None);
        let close_messages = close_sink.recorded();
        let close_action = handle_queued_request(
            &mut close_sink,
            close_request,
            &close_queue,
            &pending,
            79,
            None,
            MAX_READY_CONTROLS_PER_BOUNDARY,
            &mut close_rx,
            Duration::from_secs(1),
            Duration::from_secs(1),
            &mut close_heartbeat,
            &HeartbeatState::new(79),
            Duration::from_secs(7_200),
            Duration::from_secs(1),
            &CancellationToken::new(),
        )
        .await;
        check_eq!(close_action, RequestAction::Stop)?;
        check_eq!(done_rx.await, Ok(()))?;
        check_eq!(close_receipt.await, Ok(Err(NetError::Cancelled)))?;
        check!(close_queue.try_next().is_none())?;
        check!(matches!(
            close_messages
                .lock()
                .map_err(|error| test_error(format!("recorded close messages lock: {error:?}")))?
                .as_slice(),
            [Message::Close(None)]
        ))?;

        let cancel_queue = PriorityWriteQueue::new(1, 128)
            .map_err(|error| test_error(format!("cancel queue: {error:?}")))?;
        let dispatch_cancel = CancellationToken::new();
        dispatch_cancel.cancel();
        let (cancel_request, cancel_receipt) = request_with_receipt(
            "dispatch-cancel",
            Message::Text("business".into()),
            waiting_config.clone(),
            101,
            dispatch_cancel,
        );
        let (_cancel_control_tx, mut cancel_control_rx) = mpsc::channel(1);
        let mut cancel_heartbeat = tokio::time::interval_at(Instant::now() + period, period);
        let cancel_action = handle_queued_request(
            &mut RecordingSink::new(None),
            cancel_request,
            &cancel_queue,
            &pending,
            81,
            None,
            MAX_READY_CONTROLS_PER_BOUNDARY,
            &mut cancel_control_rx,
            Duration::from_secs(1),
            Duration::from_secs(1),
            &mut cancel_heartbeat,
            &HeartbeatState::new(81),
            Duration::from_secs(7_200),
            Duration::from_secs(1),
            &CancellationToken::new(),
        )
        .await;
        check_eq!(cancel_action, RequestAction::Continue)?;
        check_eq!(cancel_receipt.await, Ok(Err(NetError::Cancelled)))?;
        check!(cancel_queue.try_next().is_none())?;

        let deadline_queue = PriorityWriteQueue::new(1, 128)
            .map_err(|error| test_error(format!("deadline queue: {error:?}")))?;
        let (deadline_request, deadline_receipt) = request_with_receipt(
            "request-deadline",
            Message::Text("business".into()),
            WSRequestConfig {
                write_timeout: Duration::ZERO,
                ..waiting_config
            },
            103,
            CancellationToken::new(),
        );
        let (_deadline_control_tx, mut deadline_control_rx) = mpsc::channel(1);
        let mut deadline_heartbeat = tokio::time::interval_at(Instant::now() + period, period);
        let deadline_action = handle_queued_request(
            &mut RecordingSink::new(None),
            deadline_request,
            &deadline_queue,
            &pending,
            83,
            None,
            MAX_READY_CONTROLS_PER_BOUNDARY,
            &mut deadline_control_rx,
            Duration::from_secs(1),
            Duration::from_secs(1),
            &mut deadline_heartbeat,
            &HeartbeatState::new(83),
            Duration::from_secs(7_200),
            Duration::from_secs(1),
            &CancellationToken::new(),
        )
        .await;
        check_eq!(deadline_action, RequestAction::Continue)?;
        check_eq!(deadline_receipt.await, Ok(Err(NetError::TimeoutError)))?;
        check!(deadline_queue.try_next().is_none())?;
        Ok(())
    }

    #[tokio::test]
    async fn first_fragment_failure_keeps_delivery_unknown_and_allows_idempotent_retry(
    ) -> TestResult {
        let source_queue = PriorityWriteQueue::new(1, 128)
            .map_err(|error| test_error(format!("source queue: {error:?}")))?;
        let config = WSRequestConfig {
            idempotent: true,
            send_retry_count: 1,
            ..WSRequestConfig::default()
        };
        let (request, mut receipt) = request_with_receipt(
            "idempotent-fragment-retry",
            Message::Binary(Bytes::from_static(b"abcdef")),
            config,
            107,
            CancellationToken::new(),
        );
        let (first_control_tx, mut first_control_rx) = mpsc::channel(1);
        let mut first_sink = RecordingSink::with_peer_close(first_control_tx);
        let first_period = Duration::from_secs(3_600);
        let mut first_heartbeat =
            tokio::time::interval_at(Instant::now() + first_period, first_period);
        first_heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let first_action = handle_queued_request(
            &mut first_sink,
            request,
            &source_queue,
            &PendingRequestView::default(),
            85,
            Some(3),
            MAX_READY_CONTROLS_PER_BOUNDARY,
            &mut first_control_rx,
            Duration::from_secs(1),
            Duration::from_secs(1),
            &mut first_heartbeat,
            &HeartbeatState::new(85),
            Duration::from_secs(7_200),
            Duration::from_secs(1),
            &CancellationToken::new(),
        )
        .await;

        first_sink.finish_injection().await?;
        check_eq!(first_action, RequestAction::Stop)?;
        check!(matches!(
            receipt.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ))?;
        let retry = source_queue
            .try_next()
            .ok_or_else(|| test_error("idempotent retry must be queued"))?;
        check_eq!(retry.sequence, 107)?;
        check_eq!(retry.attempt, 1)?;
        check!(retry.prior_delivery_unknown)?;

        let (_second_control_tx, mut second_control_rx) = mpsc::channel(1);
        let mut second_heartbeat =
            tokio::time::interval_at(Instant::now() + first_period, first_period);
        second_heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut second_sink = RecordingSink::new(None);
        let second_action = handle_queued_request(
            &mut second_sink,
            retry,
            &source_queue,
            &PendingRequestView::default(),
            87,
            Some(3),
            MAX_READY_CONTROLS_PER_BOUNDARY,
            &mut second_control_rx,
            Duration::from_secs(1),
            Duration::from_secs(1),
            &mut second_heartbeat,
            &HeartbeatState::new(87),
            Duration::from_secs(7_200),
            Duration::from_secs(1),
            &CancellationToken::new(),
        )
        .await;

        check_eq!(second_action, RequestAction::Continue)?;
        check_eq!(receipt.await, Ok(Ok(())))?;
        check!(source_queue.try_next().is_none())?;
        Ok(())
    }

    async fn write_scope_test_request<W>(
        sink: &mut W,
        request: QueuedRequest,
        queue: &Arc<PriorityWriteQueue>,
        pending: &PendingRequestView,
        generation: u64,
        connection_cancel: &CancellationToken,
    ) -> RequestAction
    where
        W: Sink<Message, Error = WsError> + Unpin,
    {
        let (_controls, mut controls_rx) = mpsc::channel(1);
        let period = Duration::from_secs(3_600);
        let mut heartbeat = tokio::time::interval_at(Instant::now() + period, period);
        handle_queued_request(
            sink,
            request,
            queue,
            pending,
            generation,
            Some(3),
            MAX_READY_CONTROLS_PER_BOUNDARY,
            &mut controls_rx,
            Duration::from_secs(1),
            Duration::from_secs(1),
            &mut heartbeat,
            &HeartbeatState::new(generation),
            period,
            Duration::from_secs(1),
            connection_cancel,
        )
        .await
    }

    #[tokio::test]
    async fn scope_retry_keeps_registration_body_and_original_cancellation_owner() -> TestResult {
        for revoke_before_retry in [false, true] {
            let scope = CancellationToken::new();
            let next_owner = CancellationToken::new();
            let dispatch = scope.child_token();
            let config = WSRequestConfig {
                idempotent: true,
                send_retry_count: 1,
                disconnected_policy: DisconnectedTaskPolicy::WaitForReconnect,
                ..WSRequestConfig::default()
            };
            let uuid = "scope-bound-retry";
            let body = Message::Text(uuid.into());
            let pending = PendingRequestView::default();
            let (token, completion) = pending
                .reserve(Arc::new(TimeoutTestRequest(uuid)), &config)
                .map_err(|error| test_error(format!("reserve scoped retry: {error:?}")))?;
            let (mut request, mut receipt) =
                request_with_receipt(uuid, body.clone(), config, 211, dispatch.clone());
            request.pending_token = Some(token);
            request.admission_cancel = Some(CancellationToken::new());
            request
                .dispatch_phase
                .set_pending_cleanup(pending.clone(), uuid.to_owned(), token)?;
            let queue = PriorityWriteQueue::new(1, 128)?;
            let mut first_sink = NthFlushErrorSink::new(1);
            let first_action = write_scope_test_request(
                &mut first_sink,
                request,
                &queue,
                &pending,
                601,
                &CancellationToken::new(),
            )
            .await;
            check_eq!(
                first_action,
                RequestAction::StopWithError(NetError::DeliveryUnknown)
            )?;
            check!(matches!(
                receipt.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ))?;
            let retry = queue
                .try_next()
                .ok_or_else(|| test_error("same-scope retry was not retained"))?;
            check_eq!(retry.pending_token, Some(token))?;
            check_eq!(retry.message, body)?;
            check_eq!(retry.uuid, uuid)?;
            check_eq!(retry.sequence, 211)?;
            check_eq!(retry.attempt, 1)?;
            check!(retry.admission_cancel.is_none())?;
            check!(retry.dispatch_cancel == dispatch)?;
            check!(retry.prior_delivery_unknown)?;
            if revoke_before_retry {
                scope.cancel();
                check!(retry.dispatch_cancel.is_cancelled())?;
                check!(!next_owner.is_cancelled())?;
            }
            let mut second_sink = RecordingSink::new(None);
            let second_action = write_scope_test_request(
                &mut second_sink,
                retry,
                &queue,
                &pending,
                602,
                &CancellationToken::new(),
            )
            .await;
            check_eq!(second_action, RequestAction::Continue)?;
            check!(queue.try_next().is_none())?;
            if revoke_before_retry {
                check_eq!(receipt.await?, Err(NetError::DeliveryUnknown))?;
                check_eq!(completion.wait().await, Err(NetError::DeliveryUnknown))?;
                check!(pending.is_empty())?;
                check!(second_sink
                    .recorded()
                    .lock()
                    .map_err(|error| test_error(format!("scoped retry messages: {error}")))?
                    .is_empty())?;
            } else {
                check_eq!(receipt.await?, Ok(()))?;
                check!(pending.take_request(uuid, 601).is_none())?;
                check!(pending.take_request(uuid, 602).is_some())?;
                check_eq!(completion.wait().await, Ok(()))?;
                let frames = second_sink.recorded();
                let messages = frames
                    .lock()
                    .map_err(|error| test_error(format!("scoped retry messages: {error}")))?;
                let mut restored = Vec::new();
                for message in messages.iter() {
                    let Message::Frame(frame) = message else {
                        return Err(test_error("scoped retry did not preserve fragmentation"));
                    };
                    restored.extend_from_slice(frame.payload());
                }
                check_eq!(restored.as_slice(), uuid.as_bytes())?;
            }
        }
        Ok(())
    }

    async fn check_registration_termination_after_unknown_retry(
        expire: bool,
        before_queue_publication: bool,
    ) -> TestResult {
        use crate::api::wsc::request_registration::{
            RegistrationControl, RequestTerminationOutcome,
        };
        let uuid = "registration-unknown-retry";
        let config = WSRequestConfig {
            idempotent: true,
            send_retry_count: 1,
            disconnected_policy: DisconnectedTaskPolicy::WaitForReconnect,
            ..WSRequestConfig::default()
        };
        let queue = PriorityWriteQueue::new(1, 128)?;
        let pending = PendingRequestView::default();
        let cancel = CancellationToken::new();
        let (mut request, receipt) = request_with_receipt(
            uuid,
            Message::Text("original body".into()),
            config.clone(),
            701,
            cancel.clone(),
        );
        let control = RegistrationControl::new(
            request.dispatch_phase.clone(),
            cancel,
            Arc::downgrade(&queue),
        );
        let (registration, completion) = pending.reserve_snapshot_observed(
            uuid.to_owned(),
            Arc::new(TimeoutTestRequest(uuid)),
            &config,
            None,
            None,
            Some(control),
            None,
        )?;
        request.pending_token = Some(registration.raw_token());
        request.dispatch_phase.set_pending_cleanup(
            pending.clone(),
            uuid.to_owned(),
            registration.raw_token(),
        )?;
        let mut sink = NthFlushErrorSink::new(1);
        let action = write_scope_test_request(
            &mut sink,
            request,
            &queue,
            &pending,
            701,
            &CancellationToken::new(),
        )
        .await;
        check_eq!(
            action,
            RequestAction::StopWithError(NetError::DeliveryUnknown)
        )?;
        let retained = queue
            .try_next()
            .ok_or_else(|| test_error("missing ambiguous retry"))?;
        check!(retained.prior_delivery_unknown)?;
        check_eq!(retained.attempt, 1)?;
        // Holding the already-requeued dispatch outside the heap reproduces the
        // writer's gap between its requeue CAS and push_existing publication.
        let unpublished = if before_queue_publication {
            Some(retained)
        } else {
            queue
                .push_existing(retained)
                .map_err(|_| test_error("could not restore ambiguous retry"))?;
            None
        };
        let outcome = if expire {
            registration.expire()?
        } else {
            registration.cancel()?
        };
        if let Some(unpublished) = unpublished {
            let rejected = queue
                .push_existing(unpublished)
                .err()
                .ok_or_else(|| test_error("cancelled retry was published after cancellation"))?;
            rejected.complete(Err(NetError::Cancelled));
        }
        let written = receipt.await?;
        let response = completion.wait().await;
        check!(
            written == Err(NetError::DeliveryUnknown)
                && response == Err(NetError::DeliveryUnknown)
                && outcome == RequestTerminationOutcome::Terminated { error: NetError::DeliveryUnknown },
            "ambiguous retry lost terminal identity: written={written:?}, pending={response:?}, outcome={outcome:?}"
        )?;
        check!(pending.is_empty())?;
        check!(queue.try_next().is_none())?;
        Ok(())
    }

    #[tokio::test]
    async fn registration_cancel_of_queued_unknown_retry_preserves_both_receipts() -> TestResult {
        check_registration_termination_after_unknown_retry(false, false).await
    }

    #[tokio::test]
    async fn registration_expire_of_queued_unknown_retry_preserves_both_receipts() -> TestResult {
        check_registration_termination_after_unknown_retry(true, false).await
    }

    #[tokio::test]
    async fn registration_cancel_before_unknown_retry_publication_preserves_both_receipts(
    ) -> TestResult {
        check_registration_termination_after_unknown_retry(false, true).await
    }

    #[tokio::test]
    async fn registration_expire_before_unknown_retry_publication_preserves_both_receipts(
    ) -> TestResult {
        check_registration_termination_after_unknown_retry(true, true).await
    }

    #[tokio::test]
    async fn scope_cancel_before_first_write_is_unsent_and_never_requeued() -> TestResult {
        let scope = CancellationToken::new();
        let dispatch = scope.child_token();
        let (request, receipt) = request_with_receipt(
            "scope-before-write",
            Message::Text("body".into()),
            WSRequestConfig {
                idempotent: true,
                send_retry_count: 2,
                disconnected_policy: DisconnectedTaskPolicy::WaitForReconnect,
                ..WSRequestConfig::default()
            },
            213,
            dispatch,
        );
        scope.cancel();
        let queue = PriorityWriteQueue::new(1, 128)?;
        let mut sink = RecordingSink::new(None);
        let action = write_scope_test_request(
            &mut sink,
            request,
            &queue,
            &PendingRequestView::default(),
            603,
            &CancellationToken::new(),
        )
        .await;
        check_eq!(action, RequestAction::Continue)?;
        check_eq!(receipt.await?, Err(NetError::Cancelled))?;
        check!(queue.try_next().is_none())?;
        check!(sink
            .recorded()
            .lock()
            .map_err(|error| test_error(format!("cancelled scope messages: {error}")))?
            .is_empty())?;
        Ok(())
    }

    #[tokio::test]
    async fn scope_cancel_during_write_retires_socket_and_cannot_requeue_idempotent_request(
    ) -> TestResult {
        let scope = CancellationToken::new();
        let connection_cancel = scope.child_token();
        let (request, receipt) = request_with_receipt(
            "scope-during-write",
            Message::Text("body".into()),
            WSRequestConfig {
                idempotent: true,
                send_retry_count: 2,
                disconnected_policy: DisconnectedTaskPolicy::WaitForReconnect,
                ..WSRequestConfig::default()
            },
            215,
            scope.child_token(),
        );
        let phase = request.dispatch_phase.clone();
        let queue = PriorityWriteQueue::new(1, 128)?;
        let pending = PendingRequestView::default();
        let (entered_tx, mut entered_rx) = oneshot::channel();
        let mut sink = FlushPendingSink::new(entered_tx);
        let mut writing = Box::pin(write_scope_test_request(
            &mut sink,
            request,
            &queue,
            &pending,
            605,
            &connection_cancel,
        ));
        check!(writing.as_mut().now_or_never().is_none())?;
        entered_rx
            .try_recv()
            .map_err(|error| test_error(format!("scoped data never entered sink: {error}")))?;
        // Scope propagation alone must work, without a registration-cancel phase CAS.
        scope.cancel();
        check!(!phase.is_cancelled())?;
        let action = writing.await;
        check_eq!(
            action,
            RequestAction::StopWithError(NetError::DeliveryUnknown)
        )?;
        check_eq!(receipt.await?, Err(NetError::DeliveryUnknown))?;
        check!(queue.try_next().is_none())?;
        check_eq!(
            sink.recorded()
                .lock()
                .map_err(|error| test_error(format!("in-flight scoped messages: {error}")))?
                .len(),
            1
        )?;
        Ok(())
    }

    #[tokio::test]
    async fn scope_cancel_during_final_flush_cannot_commit_success() -> TestResult {
        let scope = CancellationToken::new();
        let (request, receipt) = request_with_receipt(
            "scope-final-flush",
            Message::Text("abc".into()),
            WSRequestConfig {
                idempotent: true,
                send_retry_count: 2,
                ..WSRequestConfig::default()
            },
            217,
            scope.child_token(),
        );
        let queue = PriorityWriteQueue::new(1, 128)?;
        let mut sink = RecordingSink::new(None);
        sink.cancel_after_flush = Some(scope.clone());
        let action = write_scope_test_request(
            &mut sink,
            request,
            &queue,
            &PendingRequestView::default(),
            607,
            &scope.child_token(),
        )
        .await;
        check!(scope.is_cancelled())?;
        check_eq!(receipt.await?, Err(NetError::DeliveryUnknown))?;
        check_eq!(
            action,
            RequestAction::StopWithError(NetError::DeliveryUnknown)
        )?;
        check!(queue.try_next().is_none())?;
        Ok(())
    }

    #[tokio::test]
    async fn one_frame_boundary_processes_only_a_bounded_control_burst() -> TestResult {
        let capacity = MAX_READY_CONTROLS_PER_BOUNDARY + 3;
        let (control_tx, mut control_rx) = mpsc::channel(capacity);
        for _ in 0..capacity {
            control_tx
                .send(ControlMessage::FlushAutomatic)
                .await
                .map_err(|error| test_error(format!("queue automatic Pong flush: {error:?}")))?;
        }
        let mut sink = RecordingSink::new(None);
        let flushes = sink.flushes();
        let heartbeat_interval = Duration::from_secs(3_600);
        let mut heartbeat =
            tokio::time::interval_at(Instant::now() + heartbeat_interval, heartbeat_interval);
        let mut remaining_controls = MAX_READY_CONTROLS_PER_BOUNDARY;

        let action = drain_ready_controls(
            &mut sink,
            &mut control_rx,
            Duration::from_secs(1),
            &CancellationToken::new(),
            &mut heartbeat,
            &HeartbeatState::new(1),
            Duration::from_secs(7_200),
            Instant::now() + Duration::from_secs(1),
            &mut remaining_controls,
        )
        .await;

        check_eq!(action, Ok(ControlAction::Continue))?;
        check_eq!(
            flushes.load(Ordering::Relaxed),
            MAX_READY_CONTROLS_PER_BOUNDARY
        )?;
        check!(matches!(
            control_rx.try_recv(),
            Ok(ControlMessage::FlushAutomatic)
        ))?;
        Ok(())
    }

    #[tokio::test]
    async fn a_due_heartbeat_is_checked_between_ready_controls() -> TestResult {
        let (control_tx, mut control_rx) = mpsc::channel(3);
        for _ in 0..3 {
            control_tx
                .send(ControlMessage::FlushAutomatic)
                .await
                .map_err(|error| test_error(format!("queue automatic Pong flush: {error:?}")))?;
        }
        let mut sink = RecordingSink::new(None);
        let recorded = sink.recorded();
        let heartbeat_period = Duration::from_secs(3_600);
        let due_at = Instant::now()
            .checked_sub(Duration::from_secs(1))
            .ok_or_else(|| test_error("test instant supports subtraction"))?;
        let mut heartbeat = tokio::time::interval_at(due_at, heartbeat_period);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
        tokio::task::yield_now().await;
        let mut remaining_controls = MAX_READY_CONTROLS_PER_BOUNDARY;

        let action = drain_ready_controls(
            &mut sink,
            &mut control_rx,
            Duration::from_secs(1),
            &CancellationToken::new(),
            &mut heartbeat,
            &HeartbeatState::new(7),
            Duration::from_secs(7_200),
            Instant::now() + Duration::from_secs(1),
            &mut remaining_controls,
        )
        .await;

        check_eq!(action, Ok(ControlAction::Continue))?;
        check!(matches!(
            recorded.lock().map_err(|error| test_error(format!("recorded messages lock: {error:?}")))?.as_slice(),
            [Message::Ping(payload)] if !payload.is_empty()
        ))?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn request_deadline_bounds_a_ready_control_flush() -> TestResult {
        let (control_tx, mut control_rx) = mpsc::channel(1);
        control_tx
            .send(ControlMessage::FlushAutomatic)
            .await
            .map_err(|error| test_error(format!("queue automatic Pong flush: {error:?}")))?;
        let mut sink = PendingSink::default();
        let heartbeat_state = HeartbeatState::new(1);
        let heartbeat_interval = Duration::from_secs(3_600);
        let mut heartbeat =
            tokio::time::interval_at(Instant::now() + heartbeat_interval, heartbeat_interval);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let request_timeout = Duration::from_millis(25);
        let started_at = Instant::now();

        let result = send_request_message(
            &mut sink,
            Message::Text("deadline".into()),
            None,
            MAX_READY_CONTROLS_PER_BOUNDARY,
            started_at + request_timeout,
            &mut control_rx,
            Duration::from_secs(30),
            Duration::from_secs(30),
            &mut heartbeat,
            &heartbeat_state,
            Duration::from_secs(7_200),
            &CancellationToken::new(),
            &CancellationToken::new(),
        )
        .await;

        sink.verify()?;
        check_eq!(
            result.map_err(|failure| failure.request_error),
            Err(NetError::TimeoutError)
        )?;
        check_eq!(started_at.elapsed(), request_timeout)?;
        Ok(())
    }

    #[tokio::test]
    /// 验证逻辑消息已有分片写入后收到 peer Close 时不会误报为安全取消。
    async fn peer_close_after_first_fragment_reports_delivery_unknown() -> TestResult {
        let (control_tx, mut control_rx) = mpsc::channel(4);
        let mut sink = RecordingSink::with_peer_close(control_tx);
        let recorded = sink.recorded();
        let cancel = CancellationToken::new();
        let heartbeat_state = HeartbeatState::new(1);
        let heartbeat_interval = Duration::from_secs(3_600);
        let mut heartbeat =
            tokio::time::interval_at(Instant::now() + heartbeat_interval, heartbeat_interval);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let result = send_request_message(
            &mut sink,
            Message::Binary(Bytes::from_static(b"abcdef")),
            Some(3),
            MAX_READY_CONTROLS_PER_BOUNDARY,
            Instant::now() + Duration::from_secs(1),
            &mut control_rx,
            Duration::from_secs(1),
            Duration::from_secs(1),
            &mut heartbeat,
            &heartbeat_state,
            Duration::from_secs(7_200),
            &cancel,
            &CancellationToken::new(),
        )
        .await;

        sink.finish_injection().await?;
        check_eq!(
            result.map_err(|failure| failure.request_error),
            Err(NetError::DeliveryUnknown)
        )?;
        let messages = recorded
            .lock()
            .map_err(|error| test_error(format!("recorded messages lock: {error:?}")))?;
        check_eq!(messages.len(), 1)?;
        check!(matches!(
            &messages[0],
            Message::Frame(frame)
                if frame.header().opcode == OpCode::Data(OpData::Binary)
                    && !frame.header().is_final
                    && frame.payload() == b"abc"
        ))?;
        Ok(())
    }

    #[tokio::test]
    async fn control_io_error_after_first_fragment_reports_delivery_unknown() -> TestResult {
        let (control_tx, mut control_rx) = mpsc::channel(4);
        let mut sink = RecordingSink::with_failing_automatic_flush(control_tx, 2);
        let recorded = sink.recorded();
        let heartbeat_state = HeartbeatState::new(1);
        let heartbeat_interval = Duration::from_secs(3_600);
        let mut heartbeat =
            tokio::time::interval_at(Instant::now() + heartbeat_interval, heartbeat_interval);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let result = send_request_message(
            &mut sink,
            Message::Binary(Bytes::from_static(b"abcdef")),
            Some(3),
            MAX_READY_CONTROLS_PER_BOUNDARY,
            Instant::now() + Duration::from_secs(1),
            &mut control_rx,
            Duration::from_secs(1),
            Duration::from_secs(1),
            &mut heartbeat,
            &heartbeat_state,
            Duration::from_secs(7_200),
            &CancellationToken::new(),
            &CancellationToken::new(),
        )
        .await;

        sink.finish_injection().await?;
        check_eq!(
            result.map_err(|failure| failure.request_error),
            Err(NetError::DeliveryUnknown)
        )?;
        check_eq!(
            recorded
                .lock()
                .map_err(|error| test_error(format!("recorded messages lock: {error:?}")))?
                .len(),
            1
        )?;
        Ok(())
    }

    #[tokio::test]
    async fn response_claim_wins_request_receipt_before_an_ambiguous_flush_error() -> TestResult {
        let uuid = "response-before-flush-error";
        let generation = 53;
        let pending = PendingRequestView::with_capacity(1);
        let config = WSRequestConfig {
            idempotent: true,
            send_retry_count: 1,
            ..WSRequestConfig::default()
        };
        let (token, completion) = pending
            .reserve(Arc::new(TimeoutTestRequest(uuid)), &config)
            .map_err(|error| test_error(format!("reserve tracked request: {error:?}")))?;
        let dispatch_phase = crate::module::ws_client::write::queued_request::DispatchPhase::new();
        dispatch_phase.set_pending_cleanup(pending.clone(), uuid.to_string(), token)?;
        let (result_tx, result_rx) = oneshot::channel();
        let request = QueuedRequest {
            uuid: uuid.to_string(),
            pending_token: Some(token),
            message: Message::Text(uuid.into()),
            config,
            attempt: 0,
            prior_delivery_unknown: false,
            result_tx: Some(result_tx),
            dispatch_cancel: CancellationToken::new(),
            dispatch_phase,
            admission_cancel: None,
            sequence: 1,
            slot_permit: None,
            byte_permit: None,
        };
        let source_queue = PriorityWriteQueue::new(1, 128)
            .map_err(|error| test_error(format!("source queue: {error:?}")))?;
        let (_control_tx, mut control_rx) = mpsc::channel(1);
        let heartbeat_state = HeartbeatState::new(generation);
        let heartbeat_interval = Duration::from_secs(3_600);
        let mut heartbeat =
            tokio::time::interval_at(Instant::now() + heartbeat_interval, heartbeat_interval);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut sink = ResponseClaimThenFlushErrorSink {
            pending: pending.clone(),
            uuid,
            generation,
            claimed: false,
            claim_result: None,
        };

        let action = handle_queued_request(
            &mut sink,
            request,
            &source_queue,
            &pending,
            generation,
            None,
            MAX_READY_CONTROLS_PER_BOUNDARY,
            &mut control_rx,
            Duration::from_secs(1),
            Duration::from_secs(1),
            &mut heartbeat,
            &heartbeat_state,
            Duration::from_secs(7_200),
            Duration::from_secs(1),
            &CancellationToken::new(),
        )
        .await;

        sink.claim_result
            .take()
            .ok_or_else(|| test_error("flush must attempt response claim"))??;
        check_eq!(
            action,
            RequestAction::StopWithError(NetError::DeliveryUnknown)
        )?;
        check_eq!(result_rx.await, Ok(Ok(())))?;
        check_eq!(completion.wait().await, Ok(()))?;
        check!(pending.is_empty())?;
        check!(
            source_queue.drain().is_empty(),
            "response must suppress retry"
        )?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    /// 验证底层 flush 卡住时，单帧 deadline 早于请求级 deadline 生效。
    async fn data_frame_timeout_bounds_a_stalled_flush() -> TestResult {
        let (started_tx, started_rx) = oneshot::channel();
        let mut sink = FlushPendingSink::new(started_tx);
        let recorded = sink.recorded();
        let (control_tx, mut control_rx) = mpsc::channel(4);
        let cancel = CancellationToken::new();
        let heartbeat_state = HeartbeatState::new(1);
        let heartbeat_interval = Duration::from_secs(3_600);
        let mut heartbeat =
            tokio::time::interval_at(Instant::now() + heartbeat_interval, heartbeat_interval);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let frame_timeout = Duration::from_millis(25);
        let started_at = Instant::now();
        let dispatch_cancel = CancellationToken::new();

        let send = send_request_message(
            &mut sink,
            Message::Binary(Bytes::from_static(b"abcdef")),
            Some(3),
            MAX_READY_CONTROLS_PER_BOUNDARY,
            Instant::now() + Duration::from_secs(30),
            &mut control_rx,
            Duration::from_secs(1),
            frame_timeout,
            &mut heartbeat,
            &heartbeat_state,
            Duration::from_secs(7_200),
            &cancel,
            &dispatch_cancel,
        );
        let inject_control = async move {
            tokio::time::timeout(Duration::from_secs(1), started_rx)
                .await
                .map_err(|error| test_error(format!("first frame did not enter sink: {error}")))?
                .map_err(|error| test_error(format!("first frame entered sink: {error}")))?;
            control_tx
                .send(ControlMessage::FlushAutomatic)
                .await
                .map_err(|error| {
                    test_error(format!("queue Pong while flush is pending: {error:?}"))
                })?;
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        };
        let (result, injection_result) = tokio::join!(send, inject_control);
        injection_result?;

        check_eq!(
            result.map_err(|failure| failure.request_error),
            Err(NetError::DeliveryUnknown)
        )?;
        check_eq!(started_at.elapsed(), frame_timeout)?;
        check!(matches!(
            control_rx.try_recv(),
            Ok(ControlMessage::FlushAutomatic)
        ))?;
        let messages = recorded
            .lock()
            .map_err(|error| test_error(format!("recorded messages lock: {error:?}")))?;
        check_eq!(messages.len(), 1)?;
        check!(matches!(
            &messages[0],
            Message::Frame(frame)
                if frame.header().opcode == OpCode::Data(OpData::Binary)
                    && !frame.header().is_final
                    && frame.payload() == b"abc"
        ))?;
        Ok(())
    }

    #[tokio::test]
    async fn cancelling_connection_while_first_data_readiness_is_pending_keeps_unsent_state(
    ) -> TestResult {
        let connection_cancel = CancellationToken::new();
        let cancel_from_test = connection_cancel.clone();
        let dispatch_cancel = CancellationToken::new();
        let mut sink = PendingSink::default();
        let send = send_data_frame(
            &mut sink,
            Message::Binary(Bytes::from_static(b"in flight")),
            false,
            Instant::now() + Duration::from_secs(5),
            Duration::from_secs(5),
            &connection_cancel,
            &dispatch_cancel,
        );
        let cancel = async move {
            tokio::task::yield_now().await;
            cancel_from_test.cancel();
        };

        let (result, ()) = tokio::join!(send, cancel);

        sink.verify()?;
        check_eq!(
            result.map_err(|failure| failure.request_error),
            Err(NetError::Cancelled)
        )?;
        Ok(())
    }

    #[tokio::test]
    async fn first_data_frame_io_error_is_delivery_unknown() -> TestResult {
        let mut sink = NthFlushErrorSink::new(1);
        let result = send_data_frame(
            &mut sink,
            Message::Binary(Bytes::from_static(b"first frame")),
            false,
            Instant::now() + Duration::from_secs(1),
            Duration::from_secs(1),
            &CancellationToken::new(),
            &CancellationToken::new(),
        )
        .await;

        check_eq!(
            result.map_err(|failure| failure.request_error),
            Err(NetError::DeliveryUnknown)
        )?;
        check_eq!(
            sink.recorded()
                .lock()
                .map_err(|error| test_error(format!("recorded messages lock: {error:?}")))?
                .len(),
            1
        )?;
        Ok(())
    }

    #[tokio::test]
    async fn final_continuation_io_error_is_delivery_unknown() -> TestResult {
        let (control_tx, mut control_rx) = mpsc::channel(1);
        let _control_tx = control_tx;
        let mut sink = NthFlushErrorSink::new(2);
        let recorded = sink.recorded();
        let heartbeat_state = HeartbeatState::new(1);
        let heartbeat_interval = Duration::from_secs(3_600);
        let mut heartbeat =
            tokio::time::interval_at(Instant::now() + heartbeat_interval, heartbeat_interval);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let result = send_request_message(
            &mut sink,
            Message::Binary(Bytes::from_static(b"abcdef")),
            Some(3),
            MAX_READY_CONTROLS_PER_BOUNDARY,
            Instant::now() + Duration::from_secs(1),
            &mut control_rx,
            Duration::from_secs(1),
            Duration::from_secs(1),
            &mut heartbeat,
            &heartbeat_state,
            Duration::from_secs(7_200),
            &CancellationToken::new(),
            &CancellationToken::new(),
        )
        .await;

        check_eq!(
            result.map_err(|failure| failure.request_error),
            Err(NetError::DeliveryUnknown)
        )?;
        check_eq!(
            recorded
                .lock()
                .map_err(|error| test_error(format!("recorded messages lock: {error:?}")))?
                .len(),
            2
        )?;
        Ok(())
    }

    #[tokio::test]
    /// 验证控制帧超过写入时限时返回 DeliveryUnknown，不继续复用 sink。
    async fn control_write_timeout_reports_delivery_unknown() -> TestResult {
        let mut sink = PendingSink::default();
        let error = send_control_frame(
            &mut sink,
            Message::Ping(Bytes::new()),
            Duration::from_millis(10),
            &CancellationToken::new(),
        )
        .await;
        sink.verify()?;
        check_eq!(error, Err(NetError::DeliveryUnknown))?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_delayed_first_tick_sends_a_probe_instead_of_timing_out_old_idle_time() -> TestResult
    {
        let heartbeat_state = HeartbeatState::new(23);
        tokio::time::advance(Duration::from_secs(3_600)).await;
        let mut sink = RecordingSink::new(None);
        let recorded = sink.recorded();

        let result = handle_heartbeat_tick(
            &mut sink,
            &heartbeat_state,
            Duration::from_secs(5),
            Duration::from_secs(1),
            &CancellationToken::new(),
        )
        .await;

        check_eq!(result, Ok(()))?;
        let messages = recorded
            .lock()
            .map_err(|error| test_error(format!("recorded messages lock: {error:?}")))?;
        check!(matches!(&messages[..], [Message::Ping(payload)] if !payload.is_empty()))?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_matching_pong_allows_the_next_unique_probe() -> TestResult {
        let heartbeat_state = HeartbeatState::new(29);
        let mut sink = RecordingSink::new(None);
        let recorded = sink.recorded();
        let cancel = CancellationToken::new();

        check_eq!(
            handle_heartbeat_tick(
                &mut sink,
                &heartbeat_state,
                Duration::from_secs(5),
                Duration::from_secs(1),
                &cancel,
            )
            .await,
            Ok(())
        )?;
        let first_payload = {
            let messages = recorded
                .lock()
                .map_err(|error| test_error(format!("recorded messages lock: {error:?}")))?;
            let Some(Message::Ping(payload)) = messages.first() else {
                return Err(test_error("heartbeat must write Ping"));
            };
            payload.clone()
        };
        check!(heartbeat_state.acknowledge_pong(first_payload.as_ref()))?;

        check_eq!(
            handle_heartbeat_tick(
                &mut sink,
                &heartbeat_state,
                Duration::from_secs(5),
                Duration::from_secs(1),
                &cancel,
            )
            .await,
            Ok(())
        )?;
        let messages = recorded
            .lock()
            .map_err(|error| test_error(format!("recorded messages lock: {error:?}")))?;
        let Some(Message::Ping(second_payload)) = messages.get(1) else {
            return Err(test_error("second heartbeat must write Ping"));
        };
        check!(!second_payload.is_empty())?;
        check_ne!(&first_payload, second_payload)?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn an_unmatched_pong_does_not_mask_a_missing_pong_timeout() -> TestResult {
        let heartbeat_state = HeartbeatState::new(31);
        let mut sink = RecordingSink::new(None);
        let recorded = sink.recorded();
        let cancel = CancellationToken::new();
        let pong_timeout = Duration::from_secs(5);

        check_eq!(
            handle_heartbeat_tick(
                &mut sink,
                &heartbeat_state,
                pong_timeout,
                Duration::from_secs(1),
                &cancel,
            )
            .await,
            Ok(())
        )?;
        check!(!heartbeat_state.acknowledge_pong(b"some-other-pong"))?;
        tokio::time::advance(pong_timeout).await;

        check_eq!(
            handle_heartbeat_tick(
                &mut sink,
                &heartbeat_state,
                pong_timeout,
                Duration::from_secs(1),
                &cancel,
            )
            .await,
            Err(NetError::SocketRecvTimeout)
        )?;
        check_eq!(
            recorded
                .lock()
                .map_err(|error| test_error(format!("recorded messages lock: {error:?}")))?
                .len(),
            1
        )?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn pong_deadline_fires_before_the_next_heartbeat_interval_tick() -> TestResult {
        let heartbeat_state = Arc::new(HeartbeatState::new(33));
        let started_at = Instant::now();
        let HeartbeatTick::SendProbe(payload) =
            heartbeat_state.on_tick(started_at, Duration::from_secs(5))
        else {
            return Err(test_error("first heartbeat tick must create a probe"));
        };
        check!(heartbeat_state.mark_sent(payload.as_ref(), started_at))?;

        let business_queue = PriorityWriteQueue::new(1, 64)
            .map_err(|error| test_error(format!("business queue: {error:?}")))?;
        let urgent_queue = PriorityWriteQueue::new(1, 64)
            .map_err(|error| test_error(format!("urgent queue: {error:?}")))?;
        let (_control_tx, control_rx) = mpsc::channel(1);
        let (io_event_tx, mut io_event_rx) = mpsc::channel(1);
        let writer = tokio::spawn(run_write_loop(
            RecordingSink::new(None),
            WriteLoopContext {
                queue: business_queue,
                urgent_queue,
                control_rx,
                pending_requests: PendingRequestView::default(),
                io_event_tx,
                generation: 33,
                cancel: CancellationToken::new(),
                data_frame_payload_size: Some(1024),
                control_write_timeout: Duration::from_secs(1),
                data_frame_write_timeout: Duration::from_secs(1),
                heartbeat_interval: Duration::from_secs(60),
                pong_timeout: Duration::from_secs(5),
                response_dispatch_grace: Duration::from_secs(1),
                heartbeat: heartbeat_state,
            },
            CancellationToken::new(),
        ));

        tokio::time::advance(Duration::from_secs(5)).await;
        tokio::task::yield_now().await;

        let event = io_event_rx.recv().await;
        writer
            .await
            .map_err(|error| test_error(format!("writer task: {error:?}")))?;
        let event = event.ok_or_else(|| test_error("heartbeat timeout event"))?;
        check!(matches!(
            event,
            IoEvent::WriteEnded {
                generation: 33,
                error: NetError::SocketRecvTimeout
            }
        ))?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn ready_control_traffic_cannot_starve_an_expired_pong_deadline() -> TestResult {
        let heartbeat_state = Arc::new(HeartbeatState::new(35));
        let started_at = Instant::now();
        let HeartbeatTick::SendProbe(payload) =
            heartbeat_state.on_tick(started_at, Duration::from_secs(5))
        else {
            return Err(test_error("first heartbeat tick must create a probe"));
        };
        check!(heartbeat_state.mark_sent(payload.as_ref(), started_at))?;
        tokio::time::advance(Duration::from_secs(5)).await;

        let business_queue = PriorityWriteQueue::new(1, 64)
            .map_err(|error| test_error(format!("business queue: {error:?}")))?;
        let urgent_queue = PriorityWriteQueue::new(1, 64)
            .map_err(|error| test_error(format!("urgent queue: {error:?}")))?;
        let (control_tx, control_rx) = mpsc::channel(1);
        control_tx
            .send(ControlMessage::FlushAutomatic)
            .await
            .map_err(|error| test_error(format!("queue ready control: {error:?}")))?;
        let (io_event_tx, mut io_event_rx) = mpsc::channel(1);
        let sink = RecordingSink::new(None);
        let flushes = sink.flushes();
        let writer = tokio::spawn(run_write_loop(
            sink,
            WriteLoopContext {
                queue: business_queue,
                urgent_queue,
                control_rx,
                pending_requests: PendingRequestView::default(),
                io_event_tx,
                generation: 35,
                cancel: CancellationToken::new(),
                data_frame_payload_size: Some(1024),
                control_write_timeout: Duration::from_secs(1),
                data_frame_write_timeout: Duration::from_secs(1),
                heartbeat_interval: Duration::from_secs(60),
                pong_timeout: Duration::from_secs(5),
                response_dispatch_grace: Duration::from_secs(1),
                heartbeat: heartbeat_state,
            },
            CancellationToken::new(),
        ));

        let event = io_event_rx.recv().await;
        writer
            .await
            .map_err(|error| test_error(format!("writer task: {error:?}")))?;
        check!(matches!(
            event,
            Some(IoEvent::WriteEnded {
                generation: 35,
                error: NetError::SocketRecvTimeout,
            })
        ))?;
        check_eq!(
            flushes.load(Ordering::Relaxed),
            1,
            "one ready control may flush a simultaneous peer Close before timeout"
        )?;
        Ok(())
    }

    #[tokio::test]
    async fn ready_control_traffic_cannot_starve_a_business_request() -> TestResult {
        let business_queue = PriorityWriteQueue::new(1, 64)
            .map_err(|error| test_error(format!("business queue: {error:?}")))?;
        let urgent_queue = PriorityWriteQueue::new(1, 64)
            .map_err(|error| test_error(format!("urgent queue: {error:?}")))?;
        let shutdown = CancellationToken::new();
        let business_result = business_queue
            .enqueue(
                "bounded-control-fairness".to_string(),
                None,
                Message::Text("business".into()),
                8,
                WSRequestConfig::default(),
                &shutdown,
                CancellationToken::new(),
                crate::module::ws_client::write::queued_request::DispatchPhase::new(),
                CancellationToken::new(),
            )
            .await
            .map_err(|error| test_error(format!("enqueue business request: {error:?}")))?;

        let ready_controls = MAX_READY_CONTROLS_PER_BOUNDARY * 3 + 3;
        let (control_tx, control_rx) = mpsc::channel(ready_controls);
        for _ in 0..ready_controls {
            control_tx
                .send(ControlMessage::FlushAutomatic)
                .await
                .map_err(|error| test_error(format!("queue automatic Pong flush: {error:?}")))?;
        }

        let sink = ControlFairnessSink::new();
        let flushes_before_first_data = sink.flushes_before_first_data();
        let (io_event_tx, _io_event_rx) = mpsc::channel(1);
        let cancel = CancellationToken::new();
        let writer = tokio::spawn(run_write_loop(
            sink,
            WriteLoopContext {
                queue: Arc::clone(&business_queue),
                urgent_queue,
                control_rx,
                pending_requests: PendingRequestView::default(),
                io_event_tx,
                generation: 36,
                cancel: cancel.clone(),
                data_frame_payload_size: Some(1024),
                control_write_timeout: Duration::from_secs(1),
                data_frame_write_timeout: Duration::from_secs(1),
                heartbeat_interval: Duration::from_secs(3_600),
                pong_timeout: Duration::from_secs(7_200),
                response_dispatch_grace: Duration::from_secs(1),
                heartbeat: Arc::new(HeartbeatState::new(36)),
            },
            CancellationToken::new(),
        ));

        let business_result = business_result.await;
        let controls_before_data = flushes_before_first_data.load(Ordering::Relaxed);
        cancel.cancel();
        drop(control_tx);
        writer
            .await
            .map_err(|error| test_error(format!("writer task: {error:?}")))?;

        check_eq!(business_result, Ok(Ok(())))?;
        check!(
            controls_before_data <= MAX_READY_CONTROLS_PER_BOUNDARY,
            "main-loop controls and the first frame boundary must share one total budget"
        )?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_blocked_ping_uses_control_write_timeout_not_pong_timeout() -> TestResult {
        let mut sink = PendingSink::default();
        let heartbeat_state = HeartbeatState::new(37);
        let started_at = Instant::now();
        let control_timeout = Duration::from_millis(25);

        let result = handle_heartbeat_tick(
            &mut sink,
            &heartbeat_state,
            Duration::from_millis(5),
            control_timeout,
            &CancellationToken::new(),
        )
        .await;

        sink.verify()?;
        check_eq!(result, Err(NetError::DeliveryUnknown))?;
        check_eq!(started_at.elapsed(), control_timeout)?;
        check!(matches!(
            heartbeat_state.on_tick(Instant::now(), Duration::from_millis(5)),
            HeartbeatTick::SendProbe(payload) if !payload.is_empty()
        ))?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn heartbeat_interval_skips_missed_ticks_instead_of_bursting() -> TestResult {
        let period = Duration::from_secs(5);
        let mut heartbeat = tokio::time::interval_at(Instant::now() + period, period);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
        tokio::time::advance(Duration::from_secs(60)).await;

        heartbeat.tick().await;

        check!(heartbeat.tick().now_or_never().is_none())?;
        Ok(())
    }

    #[tokio::test]
    async fn urgent_application_message_precedes_ready_business_message() -> TestResult {
        let business_queue = PriorityWriteQueue::new(2, 64)
            .map_err(|error| test_error(format!("business queue: {error:?}")))?;
        let urgent_queue = PriorityWriteQueue::new(2, 64)
            .map_err(|error| test_error(format!("urgent queue: {error:?}")))?;
        let shutdown = CancellationToken::new();
        let business_result = business_queue
            .enqueue(
                "business".to_string(),
                None,
                Message::Text("business".into()),
                8,
                WSRequestConfig::default(),
                &shutdown,
                CancellationToken::new(),
                crate::module::ws_client::write::queued_request::DispatchPhase::new(),
                CancellationToken::new(),
            )
            .await
            .map_err(|error| test_error(format!("enqueue business: {error:?}")))?;
        let urgent_result = urgent_queue
            .enqueue(
                "urgent".to_string(),
                None,
                Message::Text("urgent".into()),
                6,
                WSRequestConfig::default(),
                &shutdown,
                CancellationToken::new(),
                crate::module::ws_client::write::queued_request::DispatchPhase::new(),
                CancellationToken::new(),
            )
            .await
            .map_err(|error| test_error(format!("enqueue urgent: {error:?}")))?;

        let sink = RecordingSink::new(None);
        let recorded = sink.recorded();
        let (control_tx, control_rx) = mpsc::channel(4);
        let (io_event_tx, _io_event_rx) = mpsc::channel(4);
        let cancel = CancellationToken::new();
        let writer = tokio::spawn(run_write_loop(
            sink,
            WriteLoopContext {
                queue: Arc::clone(&business_queue),
                urgent_queue: Arc::clone(&urgent_queue),
                control_rx,
                pending_requests: PendingRequestView::default(),
                io_event_tx,
                generation: 1,
                cancel: cancel.clone(),
                data_frame_payload_size: Some(1024),
                control_write_timeout: Duration::from_secs(1),
                data_frame_write_timeout: Duration::from_secs(1),
                heartbeat_interval: Duration::from_secs(3_600),
                pong_timeout: Duration::from_secs(7_200),
                response_dispatch_grace: Duration::from_secs(1),
                heartbeat: Arc::new(HeartbeatState::new(1)),
            },
            CancellationToken::new(),
        ));

        let urgent_result = urgent_result.await;
        let business_result = business_result.await;
        cancel.cancel();
        drop(control_tx);
        writer
            .await
            .map_err(|error| test_error(format!("writer task: {error:?}")))?;
        check_eq!(urgent_result, Ok(Ok(())))?;
        check_eq!(business_result, Ok(Ok(())))?;

        let messages = recorded
            .lock()
            .map_err(|error| test_error(format!("recorded messages lock: {error:?}")))?;
        check!(
            matches!(messages.first(), Some(Message::Text(value)) if value.as_str() == "urgent")
        )?;
        check!(
            matches!(messages.get(1), Some(Message::Text(value)) if value.as_str() == "business")
        )?;
        Ok(())
    }
}

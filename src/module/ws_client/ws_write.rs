use crate::common::log::log_def::LogType;
use crate::error::NetError;
use crate::module::ws_client::heartbeat_schedule::HeartbeatSchedule;
use crate::module::ws_client::heartbeat_state::{HeartbeatState, HeartbeatTick};
use crate::module::ws_client::io_diagnostics::{classify_io_error, ClassifiedSink};
use crate::module::ws_client::io_event::IoEvent;
use crate::module::ws_client::native_pending::NativePending;
use crate::module::ws_client::operation_control::OperationControl;
use crate::module::ws_client::timeout_budget::TimeoutBudget;
use crate::module::ws_client::write::control_message::ControlMessage;
use crate::module::ws_client::write::priority_write_queue::PriorityWriteQueue;
use crate::module::ws_client::write::queued_request::QueuedRequest;
use crate::module::ws_client::write::write_loop_context::WriteLoopContext;
use crate::ws::HeartbeatRetryConfig;
use crate::ws::IoEndKind;
use bytes::Bytes;
use futures_util::{FutureExt, Sink, SinkExt};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;
#[cfg(test)]
use tokio::time::{Interval, MissedTickBehavior};
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

struct HeartbeatRecovery {
    config: HeartbeatRetryConfig,
    active: bool,
    rounds_started: usize,
    next_deadline: Option<Instant>,
    awaiting_pong: bool,
    normal_retry_payload: Option<Bytes>,
    normal_retry_attempts: usize,
    normal_retry_deadline: Option<Instant>,
    recovery_flush_pending: bool,
}

impl HeartbeatRecovery {
    fn new(config: HeartbeatRetryConfig) -> Self {
        Self {
            config,
            active: false,
            rounds_started: 0,
            next_deadline: None,
            awaiting_pong: false,
            normal_retry_payload: None,
            normal_retry_attempts: 0,
            normal_retry_deadline: None,
            recovery_flush_pending: false,
        }
    }

    fn is_active(&self) -> bool {
        self.active
    }

    fn start(&mut self) {
        self.active = true;
        self.rounds_started = 0;
        self.next_deadline = None;
        self.awaiting_pong = false;
        self.clear_normal_retry();
        self.recovery_flush_pending = false;
    }

    fn finish(&mut self) {
        self.active = false;
        self.rounds_started = 0;
        self.next_deadline = None;
        self.awaiting_pong = false;
        self.clear_normal_retry();
        self.recovery_flush_pending = false;
    }

    fn due(&self, now: Instant) -> bool {
        self.active && self.next_deadline.is_some_and(|deadline| now >= deadline)
    }

    fn next_deadline(&self) -> Option<Instant> {
        self.next_deadline
    }

    fn awaiting_pong(&self) -> bool {
        self.awaiting_pong
    }

    fn normal_retry_deadline(&self) -> Option<Instant> {
        self.normal_retry_deadline
    }

    fn normal_retry_due(&self, now: Instant) -> bool {
        self.normal_retry_deadline
            .is_some_and(|deadline| now >= deadline)
    }

    fn clear_normal_retry(&mut self) {
        self.normal_retry_payload = None;
        self.normal_retry_attempts = 0;
        self.normal_retry_deadline = None;
    }

    fn schedule_normal_retry(&mut self, payload: Bytes, now: Instant) -> Result<(), NetError> {
        let deadline = now
            .checked_add(self.config.ping_write_retry_interval)
            .ok_or_else(|| {
                NetError::config(
                    "heartbeat.retry.ping_write_retry_interval",
                    "cannot represent the retry deadline",
                )
            })?;
        self.normal_retry_payload = Some(payload);
        self.normal_retry_attempts = 1;
        self.normal_retry_deadline = Some(deadline);
        Ok(())
    }

    fn advance_normal_retry(&mut self, now: Instant) -> Result<(), NetError> {
        self.normal_retry_attempts = self.normal_retry_attempts.saturating_add(1);
        self.normal_retry_deadline = Some(
            now.checked_add(self.config.ping_write_retry_interval)
                .ok_or_else(|| {
                    NetError::config(
                        "heartbeat.retry.ping_write_retry_interval",
                        "cannot represent the retry deadline",
                    )
                })?,
        );
        Ok(())
    }

    fn begin_round(&mut self) -> bool {
        if self.rounds_started >= self.config.pong_recovery_attempts {
            return false;
        }
        self.rounds_started = self.rounds_started.saturating_add(1);
        self.next_deadline = None;
        true
    }

    fn arm_wait(&mut self, now: Instant) -> Result<(), NetError> {
        let deadline = now
            .checked_add(self.config.pong_retry_interval)
            .ok_or_else(|| {
                NetError::config(
                    "heartbeat.retry.pong_retry_interval",
                    "cannot represent the recovery deadline",
                )
            })?;
        self.next_deadline = Some(deadline);
        Ok(())
    }
}

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
/// `ErrorKind::DeliveryUnknown` 并终止当前连接，因为无法判断该帧有多少数据已进入
/// 底层。非取消错误在
/// 请求声明幂等且仍有重试额度时，会保留原容量许可和 FIFO 序号重新入队；
/// 随后写循环报告连接错误并退出，由工作线程决定是否重连并继续重试。
/// 写入成功仅表示 sink 接受了消息。待响应记录在成功标记为等待响应后会启动
/// `response_timeout` 清理，也可更早被响应监听器认领或被断线/关闭清理。响应可能
/// 在 sink 发送返回后立即被取走，先于写循环更新状态；此时不会启动超时
/// 任务，但网络写入仍以成功完成。若写入前因记录缺失、token 不匹配或锁中毒而
/// 无法标记为写入中，请求不会上网，而是以 `ErrorKind::Cancelled` 完成。
///
/// 收到主动关闭指令、取消令牌触发、控制通道关闭，或业务队列关闭且已经排空时，
/// 当前循环会不发送 `WriteEnded` 而结束；需要驱动连接状态变化的非取消 I/O
/// 故障会携带 `generation` 发送 `IoEvent::WriteEnded`。
///
/// Ping、Pong、Close 写入及 sink 关闭受 `control_write_timeout` 限制，也会观察
/// 取消令牌；控制写入超时以 `DeliveryUnknown` 报告并废弃当前连接。控制指令不会
/// 取消一个已经开始的 data frame 写入，以免继续复用可能只写出半帧的 sink。
pub(crate) async fn run_write_loop<W>(
    write: W,
    context: WriteLoopContext,
    write_retirement: CancellationToken,
) where
    W: Sink<Message, Error = WsError> + Unpin + Send + 'static,
{
    crate::log_t!(LogType::WSC; "ws.write.run_write_loop", "arg_write_present|arg_context_generation|arg_context_data_frame_payload_size|arg_context_control_write_timeout|arg_context_data_frame_write_timeout|arg_context_heartbeat_enabled|arg_write_retirement_present", true, context.generation, context.data_frame_payload_size, context.control_write_timeout, context.data_frame_write_timeout, context.heartbeat_config.is_some(), true);
    let mut write = ClassifiedSink::new(write);
    let WriteLoopContext {
        cancel_domain_gate,
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
        heartbeat_config,
        heartbeat: heartbeat_state,
    } = context;
    let heartbeat_retry = heartbeat_config
        .as_ref()
        .map(|config| config.retry.clone())
        .map_or_else(HeartbeatRetryConfig::default, |retry| retry);
    let mut heartbeat = match HeartbeatSchedule::new(heartbeat_config) {
        Ok(schedule) => schedule,
        Err(error) => {
            send_write_end(&io_event_tx, generation, error, write.end_kind(), &cancel).await;
            return;
        }
    };
    let mut recovery = HeartbeatRecovery::new(heartbeat_retry);
    let mut consecutive_controls = 0usize;
    loop {
        // `select!` intentionally prioritizes control traffic, but a peer can keep that
        // branch continuously ready. Re-check before every selection so a Ping flood
        // cannot starve the independent matching-Pong deadline indefinitely.
        if cancel.is_cancelled() {
            crate::log_s!(LogType::WSC; "ws.write.run_write_loop", "state|generation", "write_cancelled", generation);
            return;
        }
        if recovery.is_active()
            && !heartbeat_state.has_outstanding_probe()
            && recovery.awaiting_pong()
        {
            recovery.finish();
        }
        if heartbeat_state.is_timed_out(Instant::now())
            || recovery.due(Instant::now())
            || recovery.normal_retry_due(Instant::now())
        {
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
                Some(ControlAction::Failed { error, .. })
                    if error.kind() == crate::error::ErrorKind::Cancelled =>
                {
                    return
                }
                Some(ControlAction::Failed { error, .. }) => {
                    send_write_end(&io_event_tx, generation, error, write.end_kind(), &cancel)
                        .await;
                    return;
                }
                None => {}
            }
            if let Err(error) = service_due_heartbeat_work(
                &mut write,
                &heartbeat_state,
                &mut recovery,
                control_write_timeout,
                None,
                &cancel,
            )
            .await
            {
                if error.kind() != crate::error::ErrorKind::Cancelled {
                    send_write_end(&io_event_tx, generation, error, write.end_kind(), &cancel)
                        .await;
                }
                return;
            }
        }
        // Consume a due tick before the biased selection, but first service one ready control.
        // This preserves the peer-Close handshake without allowing a control stream to reset the
        // bounded fairness counter: after the heartbeat, the next iteration still forces a ready
        // business request once the shared control budget has been reached.
        if let Some(pong_timeout) = heartbeat.tick().now_or_never() {
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
                Some(ControlAction::Failed { error, .. })
                    if error.kind() == crate::error::ErrorKind::Cancelled =>
                {
                    return
                }
                Some(ControlAction::Failed { error, .. }) => {
                    send_write_end(&io_event_tx, generation, error, write.end_kind(), &cancel)
                        .await;
                    return;
                }
                None => {}
            }
            if let Err(error) = handle_heartbeat_tick_with_recovery(
                &mut write,
                &heartbeat_state,
                &mut recovery,
                pong_timeout,
                control_write_timeout,
                &cancel,
            )
            .await
            {
                if error.kind() != crate::error::ErrorKind::Cancelled {
                    send_write_end(&io_event_tx, generation, error, write.end_kind(), &cancel)
                        .await;
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
                _ = wait_for_recovery_deadline(recovery.next_deadline()) => WriteLoopEvent::HeartbeatRecovery,
                _ = wait_for_recovery_deadline(recovery.normal_retry_deadline()) => WriteLoopEvent::HeartbeatRetry,
                pong_timeout = heartbeat.tick() => WriteLoopEvent::Heartbeat(pong_timeout),
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
                crate::log_s!(LogType::WSC; "ws.write.run_write_loop", "state|generation", "write_cancelled", generation);
                return;
            }
            WriteLoopEvent::Control(Some(control)) => {
                consecutive_controls = consecutive_controls.saturating_add(1);
                match handle_control_message(&mut write, control, control_write_timeout, &cancel)
                    .await
                {
                    ControlAction::Continue => {}
                    ControlAction::Stop(_) => return,
                    ControlAction::Failed { error, .. }
                        if error.kind() == crate::error::ErrorKind::Cancelled =>
                    {
                        return
                    }
                    ControlAction::Failed { error, .. } => {
                        send_write_end(&io_event_tx, generation, error, write.end_kind(), &cancel)
                            .await;
                        return;
                    }
                }
            }
            WriteLoopEvent::Control(None) => {
                crate::log_s!(LogType::WSC; "ws.write.run_write_loop", "state|generation", "control_channel_closed", generation);
                return;
            }
            WriteLoopEvent::HeartbeatTimedOut => {
                if let Err(error) = drive_heartbeat_recovery(
                    &mut write,
                    &heartbeat_state,
                    &mut recovery,
                    control_write_timeout,
                    &cancel,
                )
                .await
                {
                    if error.kind() != crate::error::ErrorKind::Cancelled {
                        send_write_end(&io_event_tx, generation, error, write.end_kind(), &cancel)
                            .await;
                    }
                    return;
                }
            }
            WriteLoopEvent::HeartbeatRecovery => {
                if recovery.is_active()
                    && !heartbeat_state.has_outstanding_probe()
                    && recovery.awaiting_pong()
                {
                    recovery.finish();
                } else if let Err(error) = drive_heartbeat_recovery(
                    &mut write,
                    &heartbeat_state,
                    &mut recovery,
                    control_write_timeout,
                    &cancel,
                )
                .await
                {
                    if error.kind() != crate::error::ErrorKind::Cancelled {
                        send_write_end(&io_event_tx, generation, error, write.end_kind(), &cancel)
                            .await;
                    }
                    return;
                }
            }
            WriteLoopEvent::HeartbeatRetry => {
                if let Err(error) = drive_normal_ping_retry(
                    &mut write,
                    &heartbeat_state,
                    &mut recovery,
                    control_write_timeout,
                    None,
                    &cancel,
                )
                .await
                {
                    if error.kind() != crate::error::ErrorKind::Cancelled {
                        send_write_end(&io_event_tx, generation, error, write.end_kind(), &cancel)
                            .await;
                    }
                    return;
                }
            }
            WriteLoopEvent::Heartbeat(pong_timeout) => {
                consecutive_controls = 0;
                if let Err(error) = handle_heartbeat_tick_with_recovery(
                    &mut write,
                    &heartbeat_state,
                    &mut recovery,
                    pong_timeout,
                    control_write_timeout,
                    &cancel,
                )
                .await
                {
                    if error.kind() != crate::error::ErrorKind::Cancelled {
                        send_write_end(&io_event_tx, generation, error, write.end_kind(), &cancel)
                            .await;
                    }
                    return;
                }
            }
            WriteLoopEvent::Request(Some(request), source_queue, first_frame_control_budget) => {
                consecutive_controls = 0;
                request
                    .dispatch_phase
                    .bind_write_retirement(write_retirement.clone());
                let domain_io = match &cancel_domain_gate {
                    Some(gate) if request.dispatch_phase.cancel_domain().is_some() => {
                        match gate.activate(request.dispatch_phase.clone()) {
                            Ok(active) => Some(active),
                            Err(error) => {
                                request.complete(Err(error.clone()));
                                send_write_end(
                                    &io_event_tx,
                                    generation,
                                    error,
                                    write.end_kind(),
                                    &cancel,
                                )
                                .await;
                                return;
                            }
                        }
                    }
                    _ => None,
                };
                let action = handle_queued_request(
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
                    &mut recovery,
                    &cancel,
                )
                .await;
                drop(domain_io);
                match action {
                    RequestAction::Continue => {}
                    RequestAction::Stop => return,
                    RequestAction::StopWithError(error) => {
                        send_write_end(&io_event_tx, generation, error, write.end_kind(), &cancel)
                            .await;
                        return;
                    }
                }
            }
            // 两个队列只在客户端永久关闭时关闭；任何一个返回 `None` 都意味着
            // 当前 writer 不应再从本连接继续取任务。
            WriteLoopEvent::Request(None, _, _) => {
                crate::log_s!(LogType::WSC; "ws.write.run_write_loop", "state|generation", "request_queue_closed", generation);
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
    crate::log_t!(LogType::WSC; "ws.write.try_handle_ready_control", "arg_write_present|arg_control_rx_present|timeout|arg_cancel_present|cancelled", true, true, timeout, true, cancel.is_cancelled());
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
    HeartbeatRecovery,
    HeartbeatRetry,
    Heartbeat(Duration),
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
#[derive(Clone, Debug)]
struct RequestWriteFailure {
    request_error: NetError,
    connection_action: RequestAction,
    requeue: RequestRequeue,
}

impl RequestWriteFailure {
    fn connection(error: NetError, data_started: bool) -> Self {
        crate::log_t!(LogType::WSC; "ws.write.connection", "error|data_started", format!("{:?}/{:?}", error.kind(), error.context().stage), data_started);
        if matches!(
            error.kind(),
            crate::error::ErrorKind::Cancelled | crate::error::ErrorKind::Closed
        ) {
            crate::log_s!(LogType::WSC; "ws.write.connection", "error|data_started", format!("{:?}/{:?}", error.kind(), error.context().stage), data_started);
        } else {
            crate::log_e!(LogType::WSC; "ws.write.connection", "error|data_started", format!("{:?}/{:?}", error.kind(), error.context().stage), data_started);
        }
        let request_error =
            if data_started && error.kind() != crate::error::ErrorKind::DeliveryUnknown {
                NetError::with_source(crate::error::ErrorKind::DeliveryUnknown, error.clone())
                    .with_context(error.context().clone())
            } else if !data_started && error.kind() == crate::error::ErrorKind::DeliveryUnknown {
                NetError::with_source(crate::error::ErrorKind::Closed, error.clone())
                    .with_context(error.context().clone())
            } else {
                error.clone()
            };
        let connection_action = if matches!(
            error.kind(),
            crate::error::ErrorKind::Cancelled | crate::error::ErrorKind::Closed
        ) {
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
        crate::log_t!(LogType::WSC; "cancelled", "data_started", data_started);
        crate::log_s!(LogType::WSC; "cancelled", "state|data_started", "dispatch_cancelled", data_started);
        Self {
            request_error: NetError::from(crate::error::ErrorKind::Cancelled),
            connection_action: if data_started {
                RequestAction::StopWithError(NetError::from(
                    crate::error::ErrorKind::DeliveryUnknown,
                ))
            } else {
                RequestAction::Continue
            },
            requeue: RequestRequeue::Never,
        }
    }

    fn local_close(data_started: bool, error: Option<NetError>) -> Self {
        crate::log_t!(LogType::WSC; "ws.write.local_close", "data_started|error_kind|error_stage", data_started, format!("{:?}", error.as_ref().map(NetError::kind)), format!("{:?}", error.as_ref().and_then(|error| error.context().stage)));
        Self {
            request_error: NetError::from(crate::error::ErrorKind::Cancelled),
            connection_action: match error {
                None => RequestAction::Stop,
                Some(error) if error.kind() == crate::error::ErrorKind::Cancelled => {
                    RequestAction::Stop
                }
                Some(error) => RequestAction::StopWithError(error),
            },
            requeue: RequestRequeue::Never,
        }
    }

    fn deadline(data_started: bool, connection_uncertain: bool, budget: TimeoutBudget) -> Self {
        crate::log_t!(LogType::WSC; "deadline", "data_started|connection_uncertain", data_started, connection_uncertain);
        crate::log_e!(LogType::WSC; "deadline", "error|data_started|connection_uncertain", "request_deadline_elapsed", data_started, connection_uncertain);
        Self {
            request_error: budget
                .timeout_error()
                .with_stage(crate::error::ErrorStage::Write),
            connection_action: if data_started || connection_uncertain {
                RequestAction::StopWithError(NetError::from(
                    crate::error::ErrorKind::DeliveryUnknown,
                ))
            } else {
                RequestAction::Continue
            },
            requeue: RequestRequeue::Never,
        }
    }
}

/// Writes one accepted operation while retaining the existing frame/control scheduler.
#[allow(clippy::too_many_arguments)]
async fn handle_queued_request<W>(
    write: &mut W,
    mut request: QueuedRequest,
    source_queue: &Arc<PriorityWriteQueue>,
    pending_requests: &Arc<NativePending>,
    generation: u64,
    data_frame_payload_size: Option<usize>,
    first_frame_control_budget: usize,
    control_rx: &mut mpsc::Receiver<ControlMessage>,
    control_write_timeout: Duration,
    data_frame_write_timeout: Duration,
    heartbeat: &mut HeartbeatSchedule,
    heartbeat_state: &HeartbeatState,
    recovery: &mut HeartbeatRecovery,
    connection_cancel: &CancellationToken,
) -> RequestAction
where
    W: Sink<Message, Error = WsError> + Unpin,
{
    let connection = crate::ws::ConnectionId::from_allocated(generation);
    // Observation ownership spans flush/continuations even if a response wins first.
    #[cfg(feature = "quality-doctor")]
    let _quality_write = request.dispatch_phase.quality_scope_guard();
    if request
        .admission_cancel
        .as_ref()
        .is_some_and(CancellationToken::is_cancelled)
    {
        request.complete(Err(NetError::from(crate::error::ErrorKind::Closed)));
        return RequestAction::Continue;
    }
    request.admission_cancel = None;
    if !request.dispatch_phase.mark_writing(connection) || request.dispatch_cancel.is_cancelled() {
        let error = inactive_operation_error(&request.dispatch_phase);
        request.complete(Err(error));
        return RequestAction::Continue;
    }
    if let Some(registration) = &request.registration {
        match pending_requests.bind_connection(registration, connection) {
            Ok(true) => {}
            Ok(false) => {
                let error = inactive_operation_error(&request.dispatch_phase);
                request.complete(Err(error));
                return RequestAction::Continue;
            }
            Err(error) => {
                request.complete(Err(error));
                return RequestAction::Continue;
            }
        }
    }
    let Some(write_deadline) = Instant::now().checked_add(request.config.write_timeout) else {
        request.complete(Err(NetError::config(
            "write_timeout",
            "exceeds monotonic clock range",
        )));
        return RequestAction::Continue;
    };
    let write_budget =
        TimeoutBudget::new(write_deadline.into_std(), crate::ws::WsTimeoutPhase::Write);
    let write_budget =
        TimeoutBudget::earlier(request.dispatch_phase.absolute_budget(), Some(write_budget))
            .map_or(write_budget, |budget| budget);
    // Registered response expiry is arbitrated by NativePending, including its
    // bounded Manual grace. Freezing that deadline here would cancel a request
    // even after the table extended an already accepted response's claim window.
    let result = send_request_message_with_phase(
        write,
        request.message.clone(),
        data_frame_payload_size,
        first_frame_control_budget,
        write_budget,
        control_rx,
        control_write_timeout,
        data_frame_write_timeout,
        heartbeat,
        heartbeat_state,
        recovery,
        connection_cancel,
        &request.dispatch_cancel,
        &request.dispatch_phase,
    )
    .await;
    match result {
        Ok(()) => {
            let control = request.dispatch_phase.clone();
            // Completion releases queue permits before publishing write observation.
            // NativePending starts Written-origin response timing exactly once.
            request.complete(Ok(()));
            match control.snapshot() {
                Ok(snapshot)
                    if matches!(
                        snapshot.delivery,
                        crate::ws::DeliveryEvidence::Written
                            | crate::ws::DeliveryEvidence::ResponseConfirmed
                    ) =>
                {
                    RequestAction::Continue
                }
                Ok(snapshot) => match snapshot.result {
                    Some(Err(error)) => {
                        control.retire_write();
                        RequestAction::StopWithError(error)
                    }
                    _ => RequestAction::Continue,
                },
                Err(error) => {
                    control.retire_write();
                    RequestAction::StopWithError(error)
                }
            }
        }
        Err(failure) => {
            let action = failure.connection_action;
            if !matches!(action, RequestAction::Continue) {
                request.dispatch_phase.retire_write();
            }
            // An already correlated response remains authoritative even if flush fails.
            if request.dispatch_phase.response_confirmed() {
                request.release();
                if !matches!(action, RequestAction::Continue) {
                    if let Err(error) =
                        pending_requests.fail_connection(connection, failure.request_error)
                    {
                        return RequestAction::StopWithError(error);
                    }
                }
                return action;
            }
            let cancelled =
                request.dispatch_cancel.is_cancelled() || request.dispatch_phase.is_finished();
            let retry_delay = if !cancelled
                && request.registration.is_none()
                && request.dispatch_phase.has_write_attempt()
                && (failure.requeue != RequestRequeue::Never
                    || failure.request_error.kind() == crate::error::ErrorKind::TimedOut)
            {
                match request
                    .dispatch_phase
                    .raw_retry_after(&failure.request_error)
                {
                    Ok(delay) => delay,
                    Err(error) => {
                        request.complete(Err(error));
                        return action;
                    }
                }
            } else {
                None
            };
            let retry = retry_delay.is_some();
            let wait = !cancelled
                && failure.requeue == RequestRequeue::WaitForReconnect
                && (!request.dispatch_phase.has_write_attempt() || request.registration.is_some())
                && request.can_wait_for_reconnect();
            let requeued = if retry || wait {
                match &request.registration {
                    Some(registration) => pending_requests.requeue(registration),
                    None => Ok(request.dispatch_phase.requeue()),
                }
            } else {
                Ok(false)
            };
            match requeued {
                Err(error) => {
                    request.complete(Err(error.clone()));
                    RequestAction::StopWithError(error)
                }
                Ok(true) => {
                    if let Some(delay) = retry_delay {
                        let Some(ready_at) = std::time::Instant::now().checked_add(delay) else {
                            request.complete(Err(NetError::from(
                                crate::error::ErrorKind::InvalidPolicy,
                            )));
                            return action;
                        };
                        request.ready_at = Some(ready_at);
                        let Some(attempt) = request.attempt.checked_add(1) else {
                            request.complete(Err(NetError::from(
                                crate::error::ErrorKind::ResourceExhausted,
                            )));
                            return action;
                        };
                        request.attempt = attempt;
                    }
                    if let Err((request, error)) = source_queue.push_existing(request) {
                        request.complete(Err(error));
                    }
                    // The requeued registration has relinquished this physical identity.
                    // Retire other pending requests without destroying the queued retry.
                    if !matches!(action, RequestAction::Continue) {
                        if let Err(error) =
                            pending_requests.fail_connection(connection, failure.request_error)
                        {
                            return RequestAction::StopWithError(error);
                        }
                    }
                    action
                }
                Ok(false)
                    if failure.requeue != RequestRequeue::Never
                        && request.registration.is_some() =>
                {
                    // The table may retain an already accepted Manual response for one
                    // bounded grace. Releasing queue ownership must not select a failure.
                    request.release();
                    if let Err(error) =
                        pending_requests.fail_connection(connection, failure.request_error)
                    {
                        return RequestAction::StopWithError(error);
                    }
                    action
                }
                Ok(false) => {
                    request.complete(Err(failure.request_error));
                    action
                }
            }
        }
    }
}

fn inactive_operation_error(control: &OperationControl) -> NetError {
    control.selected_error().map_or_else(
        || {
            if control
                .deadline()
                .is_some_and(|deadline| Instant::now().into_std() >= deadline)
            {
                control.deadline_error()
            } else {
                NetError::from(crate::error::ErrorKind::Cancelled)
            }
        },
        |error| error,
    )
}

#[derive(Clone, Debug)]
enum RequestAction {
    Continue,
    Stop,
    StopWithError(NetError),
}

async fn handle_control_message<W>(
    write: &mut W,
    control: ControlMessage,
    timeout: Duration,
    cancel: &CancellationToken,
) -> ControlAction
where
    W: Sink<Message, Error = WsError> + Unpin,
{
    handle_control_message_with_budget(write, control, timeout, cancel, None).await
}

async fn handle_control_message_with_budget<W>(
    write: &mut W,
    control: ControlMessage,
    timeout: Duration,
    cancel: &CancellationToken,
    request_budget: Option<TimeoutBudget>,
) -> ControlAction
where
    W: Sink<Message, Error = WsError> + Unpin,
{
    crate::log_t!(LogType::WSC; "ws.write.handle_control_message", "arg_write_present|arg_control_kind|timeout|arg_cancel_present", true, match &control { ControlMessage::FlushAutomatic => "flush_automatic", ControlMessage::PeerClose(_) => "peer_close", ControlMessage::CloseWith { .. } => "local_close" }, timeout, true);
    match control {
        ControlMessage::FlushAutomatic => {
            match flush_sink_with_budget(write, timeout, cancel, request_budget).await {
                Ok(()) => ControlAction::Continue,
                Err(error) => ControlAction::Failed {
                    error,
                    origin: ControlOrigin::Automatic,
                },
            }
        }
        ControlMessage::PeerClose(done_tx) => {
            crate::log_s!(LogType::WSC; "ws.write.handle_control_message", "state", "peer_close_reply_flush_started");
            let result = flush_sink_with_budget(write, timeout, cancel, request_budget).await;
            let _ = done_tx.send(result.clone());
            match result {
                Ok(()) => ControlAction::Stop(ControlStop::ConnectionClosed),
                Err(error) => ControlAction::Failed {
                    error,
                    origin: ControlOrigin::PeerClose,
                },
            }
        }
        ControlMessage::CloseWith {
            frame,
            deadline,
            reply,
        } => {
            let result = send_close_frame(write, frame, deadline, timeout, cancel).await;
            let _ = reply.send(result.clone());
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

/// Sending and flushing share the remaining original close budget.
async fn send_close_frame<W>(
    write: &mut W,
    frame: Option<crate::ws::CloseFrame>,
    deadline: Instant,
    control_timeout: Duration,
    cancel: &CancellationToken,
) -> Result<(), NetError>
where
    W: Sink<Message, Error = WsError> + Unpin,
{
    use crate::error::{ErrorKind, ErrorStage};
    let started = Instant::now();
    let control_deadline = started.checked_add(control_timeout).ok_or_else(|| {
        NetError::config(
            "frames.control_write_timeout",
            "cannot represent a close deadline",
        )
        .with_stage(ErrorStage::Close)
    })?;
    let deadline = deadline.min(control_deadline);
    if started >= deadline {
        return Err(NetError::from(ErrorKind::TimedOut).with_stage(ErrorStage::Close));
    }
    let message = Message::Close(frame.map(crate::ws::CloseFrame::into_wire));
    tokio::select! {
        biased;
        _ = cancel.cancelled() => {
            Err(NetError::from(ErrorKind::Cancelled).with_stage(ErrorStage::Close))
        }
        result = tokio::time::timeout_at(deadline, async {
            write.send(message).await?;
            write.close().await
        }) => match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => {
                let kind = crate::module::ws_client::io_diagnostics::classify_io_error(&error);
                let error = NetError::from(error);
                let error = if error.context().stage.is_none() {
                    error.with_stage(ErrorStage::Close)
                } else {
                    error
                };
                let mut context = error.context().clone();
                context.io_end = Some(kind);
                Err(error.with_context(context))
            },
            Err(_) => Err(NetError::from(ErrorKind::DeliveryUnknown).with_stage(ErrorStage::Close)),
        }
    }
}

/// Flushes Tungstenite's automatically queued Pong or peer-Close response.
async fn flush_sink_with_budget<W>(
    write: &mut W,
    timeout: Duration,
    cancel: &CancellationToken,
    request_budget: Option<TimeoutBudget>,
) -> Result<(), NetError>
where
    W: Sink<Message, Error = WsError> + Unpin,
{
    crate::log_t!(LogType::WSC; "ws.write.flush_sink", "arg_write_present|timeout|arg_cancel_present|cancelled", true, timeout, true, cancel.is_cancelled());
    let deadline = Instant::now().checked_add(timeout).ok_or_else(|| {
        crate::log_e!(LogType::WSC; "ws.write.flush_sink", "error", "unrepresentable_deadline");
        NetError::from(crate::error::ErrorKind::InvalidConfig)
    })?;
    let selected_budget = request_budget.filter(|budget| budget.at <= deadline.into_std());
    let deadline = selected_budget.map_or(deadline, |budget| Instant::from_std(budget.at));
    tokio::select! {
        biased;
        _ = cancel.cancelled() => {
            crate::log_s!(LogType::WSC; "ws.write.flush_sink", "state", "cancelled");
            Err(NetError::from(crate::error::ErrorKind::Cancelled))
        },
        result = tokio::time::timeout_at(deadline, write.flush()) => match result {
            Ok(Ok(())) => {
                crate::log_s!(LogType::WSC; "ws.write.flush_sink", "state", "sink_operation_completed");
                Ok(())
            },
            Ok(Err(error)) => {
                crate::log_e!(LogType::WSC; "ws.write.flush_sink", "error", crate::common::log::summary::error(&error));
                let io_end = classify_io_error(&error);
                let error = NetError::from(error);
                let mut context = error.context().clone();
                context.io_end = Some(io_end);
                Err(error.with_context(context))
            },
            Err(_) => {
                crate::log_e!(LogType::WSC; "ws.write.flush_sink", "error|delivery", "sink_operation_timeout", "unknown");
                Err(selected_budget.map_or_else(
                    || NetError::from(crate::error::ErrorKind::DeliveryUnknown), budget_delivery_unknown))
            },
        },
    }
}

/// Preserve the budget that actually stopped an in-flight control write.
fn budget_delivery_unknown(budget: TimeoutBudget) -> NetError {
    let error = budget
        .timeout_error()
        .with_stage(crate::error::ErrorStage::Write);
    let context = error.context().clone();
    NetError::with_source(crate::error::ErrorKind::DeliveryUnknown, error).with_context(context)
}

async fn within_request_budget<T>(
    budget: TimeoutBudget,
    future: impl std::future::Future<Output = Result<T, NetError>>,
) -> Result<T, NetError> {
    match tokio::time::timeout_at(Instant::from_std(budget.at), future).await {
        Ok(result) => result,
        Err(_) => Err(budget_delivery_unknown(budget)),
    }
}

/// 在写下一个 data frame 前处理一批已经进入控制通道的指令。
#[allow(clippy::too_many_arguments)]
async fn drain_ready_controls_with_recovery<W>(
    write: &mut W,
    control_rx: &mut mpsc::Receiver<ControlMessage>,
    timeout: Duration,
    cancel: &CancellationToken,
    heartbeat: &mut HeartbeatSchedule,
    heartbeat_state: &HeartbeatState,
    recovery: &mut HeartbeatRecovery,
    request_budget: TimeoutBudget,
    remaining_controls: &mut usize,
) -> Result<ControlAction, NetError>
where
    W: Sink<Message, Error = WsError> + Unpin,
{
    let request_deadline = Instant::from_std(request_budget.at);
    crate::log_t!(LogType::WSC; "ws.write.drain_ready_controls_with_recovery", "arg_write_present|arg_control_rx_present|timeout|arg_cancel_present|arg_heartbeat_present|arg_heartbeat_state_present|arg_recovery_active|request_deadline|remaining_controls|deadline_elapsed", true, true, timeout, true, true, true, recovery.active, format!("{request_deadline:?}"), *remaining_controls, Instant::now() >= request_deadline);
    loop {
        // A burst of ready controls may itself consume multiple write deadlines.
        // Re-check between every control so request expiry is not delayed by the whole burst.
        let now = Instant::now();
        if now >= request_deadline {
            crate::log_e!(LogType::WSC; "ws.write.drain_ready_controls_with_recovery", "error", "TimeoutError");
            return Err(request_budget.timeout_error());
        }
        within_request_budget(
            request_budget,
            service_due_heartbeat_work(
                write,
                heartbeat_state,
                recovery,
                timeout,
                Some(request_budget),
                cancel,
            ),
        )
        .await?;
        if *remaining_controls == 0 {
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
                let action = handle_control_message_with_budget(
                    write,
                    control,
                    timeout,
                    cancel,
                    Some(request_budget),
                )
                .await;
                if matches!(
                    action,
                    ControlAction::Stop(_) | ControlAction::Failed { .. }
                ) {
                    return Ok(action);
                }
                let now = Instant::now();
                if now >= request_deadline {
                    return Err(request_budget.timeout_error());
                }
                within_request_budget(
                    request_budget,
                    service_due_heartbeat_work(
                        write,
                        heartbeat_state,
                        recovery,
                        timeout,
                        Some(request_budget),
                        cancel,
                    ),
                )
                .await?;
                // A ready-control producer can refill the channel between every receive.
                // Poll the interval after each control, not merely after the whole batch, so
                // creation of a new Ping is delayed by at most one bounded control write.
                if let Some(pong_timeout) = heartbeat.tick().now_or_never() {
                    let now = Instant::now();
                    if now >= request_deadline {
                        crate::log_e!(LogType::WSC; "ws.write.drain_ready_controls_with_recovery", "error", "TimeoutError");
                        return Err(request_budget.timeout_error());
                    }
                    within_request_budget(
                        request_budget,
                        handle_heartbeat_tick_with_budget(
                            write,
                            heartbeat_state,
                            recovery,
                            pong_timeout,
                            timeout,
                            cancel,
                            Some(request_budget),
                        ),
                    )
                    .await?;
                }
            }
            Err(mpsc::error::TryRecvError::Empty) => {
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

#[cfg(test)]
async fn drain_ready_controls<W>(
    write: &mut W,
    control_rx: &mut mpsc::Receiver<ControlMessage>,
    timeout: Duration,
    cancel: &CancellationToken,
    heartbeat: &mut HeartbeatSchedule,
    heartbeat_state: &HeartbeatState,
    request_deadline: Instant,
    remaining_controls: &mut usize,
) -> Result<ControlAction, NetError>
where
    W: Sink<Message, Error = WsError> + Unpin,
{
    let mut recovery = HeartbeatRecovery::new(HeartbeatRetryConfig::default());
    drain_ready_controls_with_recovery(
        write,
        control_rx,
        timeout,
        cancel,
        heartbeat,
        heartbeat_state,
        &mut recovery,
        TimeoutBudget::new(
            request_deadline.into_std(),
            crate::ws::WsTimeoutPhase::Write,
        ),
        remaining_controls,
    )
    .await
}

/// 检查当前 probe 的 Pong deadline，或发送一次新的有界等待 Ping。
async fn handle_heartbeat_tick_with_recovery<W>(
    write: &mut W,
    heartbeat_state: &HeartbeatState,
    recovery: &mut HeartbeatRecovery,
    pong_timeout: Duration,
    control_write_timeout: Duration,
    cancel: &CancellationToken,
) -> Result<(), NetError>
where
    W: Sink<Message, Error = WsError> + Unpin,
{
    handle_heartbeat_tick_with_budget(
        write,
        heartbeat_state,
        recovery,
        pong_timeout,
        control_write_timeout,
        cancel,
        None,
    )
    .await
}

async fn handle_heartbeat_tick_with_budget<W>(
    write: &mut W,
    heartbeat_state: &HeartbeatState,
    recovery: &mut HeartbeatRecovery,
    pong_timeout: Duration,
    control_write_timeout: Duration,
    cancel: &CancellationToken,
    request_budget: Option<TimeoutBudget>,
) -> Result<(), NetError>
where
    W: Sink<Message, Error = WsError> + Unpin,
{
    crate::log_t!(LogType::WSC; "ws.write.handle_heartbeat_tick_with_recovery", "arg_write_present|arg_heartbeat_state_present|arg_recovery_active|pong_timeout|control_write_timeout|arg_cancel_present", true, true, recovery.active, pong_timeout, control_write_timeout, true);
    if recovery.is_active() {
        if !heartbeat_state.has_outstanding_probe() && recovery.awaiting_pong() {
            recovery.finish();
            return Ok(());
        }
        if heartbeat_state.is_timed_out(Instant::now()) || recovery.due(Instant::now()) {
            return drive_heartbeat_recovery_with_budget(
                write,
                heartbeat_state,
                recovery,
                control_write_timeout,
                cancel,
                request_budget,
            )
            .await;
        }
        return Ok(());
    }
    let payload = match heartbeat_state.on_tick(Instant::now(), pong_timeout) {
        HeartbeatTick::Waiting => {
            crate::log_s!(LogType::WSC; "ws.write.handle_heartbeat_tick_with_recovery", "state", "probe_already_outstanding");
            return Ok(());
        }
        HeartbeatTick::TimedOut => {
            return drive_heartbeat_recovery_with_budget(
                write,
                heartbeat_state,
                recovery,
                control_write_timeout,
                cancel,
                request_budget,
            )
            .await;
        }
        HeartbeatTick::SendProbe(payload) => {
            crate::log_s!(LogType::WSC; "ws.write.handle_heartbeat_tick_with_recovery", "state|payload_bytes", "ping_write_started", payload.len());
            payload
        }
    };
    let send_result = send_control_frame_outcome(
        write,
        Message::Ping(payload.clone()),
        control_write_timeout,
        cancel,
        request_budget,
    )
    .await;
    match send_result {
        ControlWriteOutcome::Complete => {
            // If the matching Pong raced ahead of `send` completion, the reader
            // has already cleared this payload and `mark_sent` intentionally does nothing.
            let awaiting_pong = heartbeat_state.mark_sent(payload.as_ref(), Instant::now());
            crate::log_s!(LogType::WSC; "ws.write.handle_heartbeat_tick_with_recovery", "state|awaiting_pong", "ping_written", awaiting_pong);
            Ok(())
        }
        ControlWriteOutcome::Retryable(error) if recovery.config.ping_write_retries > 0 => {
            recovery.schedule_normal_retry(payload.clone(), Instant::now())?;
            crate::log_s!(LogType::WSC; "ws.write.handle_heartbeat_tick_with_recovery", "state|error", "ping_write_retry_scheduled", format!("{:?}/{:?}", error.kind(), error.context().stage));
            Ok(())
        }
        ControlWriteOutcome::Retryable(error) | ControlWriteOutcome::Failed(error) => {
            heartbeat_state.abandon_probe(payload.as_ref());
            crate::log_s!(LogType::WSC; "ws.write.handle_heartbeat_tick_with_recovery", "state|error", "probe_abandoned", format!("{:?}/{:?}", error.kind(), error.context().stage));
            Err(error)
        }
    }
}

async fn drive_heartbeat_recovery<W>(
    write: &mut W,
    heartbeat_state: &HeartbeatState,
    recovery: &mut HeartbeatRecovery,
    control_write_timeout: Duration,
    cancel: &CancellationToken,
) -> Result<(), NetError>
where
    W: Sink<Message, Error = WsError> + Unpin,
{
    drive_heartbeat_recovery_with_budget(
        write,
        heartbeat_state,
        recovery,
        control_write_timeout,
        cancel,
        None,
    )
    .await
}

async fn drive_heartbeat_recovery_with_budget<W>(
    write: &mut W,
    heartbeat_state: &HeartbeatState,
    recovery: &mut HeartbeatRecovery,
    control_write_timeout: Duration,
    cancel: &CancellationToken,
    request_budget: Option<TimeoutBudget>,
) -> Result<(), NetError>
where
    W: Sink<Message, Error = WsError> + Unpin,
{
    if recovery.is_active() && !heartbeat_state.has_outstanding_probe() && recovery.awaiting_pong()
    {
        recovery.finish();
        return Ok(());
    }
    if !recovery.is_active() {
        if !heartbeat_state.is_timed_out(Instant::now()) {
            return Ok(());
        }
        if recovery.config.pong_recovery_attempts == 0 {
            return Err(NetError::from(crate::error::ErrorKind::TimedOut)
                .with_stage(crate::error::ErrorStage::Heartbeat));
        }
        recovery.start();
    }
    if heartbeat_state.has_outstanding_probe() && !heartbeat_state.is_timed_out(Instant::now()) {
        return Ok(());
    }
    if heartbeat_state.is_timed_out(Instant::now())
        || recovery.due(Instant::now())
        || recovery.next_deadline().is_none()
    {
        if !recovery.begin_round() {
            return Err(NetError::from(crate::error::ErrorKind::TimedOut)
                .with_stage(crate::error::ErrorStage::Heartbeat));
        }
        if recovery.recovery_flush_pending {
            if let Err(error) =
                flush_sink_with_budget(write, control_write_timeout, cancel, request_budget).await
            {
                recovery.recovery_flush_pending = false;
                return Err(error);
            }
            recovery.recovery_flush_pending = false;
        }
        let payload = heartbeat_state
            .start_recovery_probe(Instant::now(), recovery.config.pong_retry_interval)
            .ok_or_else(|| {
                NetError::from(crate::error::ErrorKind::Internal)
                    .with_stage(crate::error::ErrorStage::Heartbeat)
            })?;
        match send_control_frame_outcome(
            write,
            Message::Ping(payload.clone()),
            control_write_timeout,
            cancel,
            request_budget,
        )
        .await
        {
            ControlWriteOutcome::Complete => {
                heartbeat_state.mark_sent(payload.as_ref(), Instant::now());
                if !heartbeat_state.has_outstanding_probe() {
                    recovery.finish();
                } else {
                    recovery.awaiting_pong = true;
                    recovery.arm_wait(Instant::now())?;
                }
                Ok(())
            }
            ControlWriteOutcome::Retryable(error) => {
                heartbeat_state.abandon_probe(payload.as_ref());
                recovery.awaiting_pong = false;
                recovery.recovery_flush_pending = true;
                recovery.arm_wait(Instant::now())?;
                crate::log_s!(LogType::WSC; "ws.write.drive_heartbeat_recovery", "state|error_kind|error_stage|arg_write_present|arg_heartbeat_state_present|arg_recovery_active|control_write_timeout|arg_cancel_present", "retryable_probe_write", format!("{:?}", error.kind()), format!("{:?}", error.context().stage), true, true, recovery.active, control_write_timeout, true);
                Ok(())
            }
            ControlWriteOutcome::Failed(error) => {
                heartbeat_state.abandon_probe(payload.as_ref());
                Err(error)
            }
        }
    } else {
        Ok(())
    }
}

async fn drive_normal_ping_retry<W>(
    write: &mut W,
    heartbeat_state: &HeartbeatState,
    recovery: &mut HeartbeatRecovery,
    control_write_timeout: Duration,
    request_budget: Option<TimeoutBudget>,
    cancel: &CancellationToken,
) -> Result<(), NetError>
where
    W: Sink<Message, Error = WsError> + Unpin,
{
    let Some(payload) = recovery.normal_retry_payload.clone() else {
        return Ok(());
    };
    // A matching Pong can race with the retry timer. If the reader already
    // cleared the exact probe, do not emit a duplicate Ping.
    if !heartbeat_state.has_outstanding_probe() {
        recovery.clear_normal_retry();
        return Ok(());
    }
    let request_deadline = request_budget.map(|budget| Instant::from_std(budget.at));
    if let Some(budget) = request_budget {
        if Instant::now() >= Instant::from_std(budget.at) {
            heartbeat_state.abandon_probe(payload.as_ref());
            recovery.clear_normal_retry();
            return Err(budget.timeout_error());
        }
    }
    // WriteBufferFull means the attempted Ping was returned before acceptance. Flush the
    // already-buffered frames first, then retry the same exact payload from this writer.
    if let Err(error) =
        flush_sink_with_budget(write, control_write_timeout, cancel, request_budget).await
    {
        heartbeat_state.abandon_probe(payload.as_ref());
        recovery.clear_normal_retry();
        return Err(error);
    }
    match send_control_frame_outcome(
        write,
        Message::Ping(payload.clone()),
        control_write_timeout,
        cancel,
        request_budget,
    )
    .await
    {
        ControlWriteOutcome::Complete => {
            heartbeat_state.mark_sent(payload.as_ref(), Instant::now());
            recovery.clear_normal_retry();
            Ok(())
        }
        ControlWriteOutcome::Retryable(error)
            if recovery.normal_retry_attempts
                < recovery.config.ping_write_retries.saturating_add(1) =>
        {
            recovery.advance_normal_retry(Instant::now())?;
            crate::log_s!(LogType::WSC; "ws.write.drive_normal_ping_retry", "state|error_kind|error_stage|arg_write_present|arg_heartbeat_state_present|arg_recovery_active|control_write_timeout|request_deadline|arg_cancel_present", "retryable_ping_write", format!("{:?}", error.kind()), format!("{:?}", error.context().stage), true, true, recovery.active, control_write_timeout, format!("{request_deadline:?}"), true);
            Ok(())
        }
        ControlWriteOutcome::Retryable(error) | ControlWriteOutcome::Failed(error) => {
            heartbeat_state.abandon_probe(payload.as_ref());
            recovery.clear_normal_retry();
            Err(error)
        }
    }
}

async fn service_due_heartbeat_work<W>(
    write: &mut W,
    heartbeat_state: &HeartbeatState,
    recovery: &mut HeartbeatRecovery,
    control_write_timeout: Duration,
    request_budget: Option<TimeoutBudget>,
    cancel: &CancellationToken,
) -> Result<(), NetError>
where
    W: Sink<Message, Error = WsError> + Unpin,
{
    if recovery.normal_retry_due(Instant::now()) {
        drive_normal_ping_retry(
            write,
            heartbeat_state,
            recovery,
            control_write_timeout,
            request_budget,
            cancel,
        )
        .await?;
    }
    if heartbeat_state.is_timed_out(Instant::now()) || recovery.due(Instant::now()) {
        drive_heartbeat_recovery_with_budget(
            write,
            heartbeat_state,
            recovery,
            control_write_timeout,
            cancel,
            request_budget,
        )
        .await?;
    }
    Ok(())
}

async fn wait_for_recovery_deadline(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending::<()>().await,
    }
}

#[cfg(test)]
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
    let mut recovery = HeartbeatRecovery::new(HeartbeatRetryConfig::default());
    handle_heartbeat_tick_with_recovery(
        write,
        heartbeat_state,
        &mut recovery,
        pong_timeout,
        control_write_timeout,
        cancel,
    )
    .await
}

/// Waits for the currently outstanding probe's Pong deadline.
///
/// When no successfully written probe exists this future remains pending until
/// the surrounding writer selects another event and constructs a fresh snapshot.
/// A matching Pong can make the captured deadline stale, so the caller must
/// re-check [`HeartbeatState::is_timed_out`] after this future completes.
async fn wait_for_pong_deadline(heartbeat_state: &HeartbeatState) {
    crate::log_t!(LogType::WSC; "wait_for_pong_deadline", "heartbeat_state", "shared");
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
    write_budget: TimeoutBudget,
    control_rx: &mut mpsc::Receiver<ControlMessage>,
    control_write_timeout: Duration,
    data_frame_write_timeout: Duration,
    heartbeat: &mut HeartbeatSchedule,
    heartbeat_state: &HeartbeatState,
    recovery: &mut HeartbeatRecovery,
    connection_cancel: &CancellationToken,
    dispatch_cancel: &CancellationToken,
    dispatch_phase: &OperationControl,
) -> Result<(), RequestWriteFailure>
where
    W: Sink<Message, Error = WsError> + Unpin,
{
    let write_deadline = Instant::from_std(write_budget.at);
    crate::log_t!(LogType::WSC; "ws.write.send_request_message_with_phase", "arg_write_present|arg_message_type|arg_message_bytes|data_frame_payload_size|first_frame_control_budget|write_deadline|arg_control_rx_present|control_write_timeout|data_frame_write_timeout|arg_heartbeat_present|arg_heartbeat_state_present|arg_recovery_active|arg_connection_cancel_present|arg_dispatch_cancel_present|arg_dispatch_phase_present", true, match &message { Message::Text(_) => "text", Message::Binary(_) => "binary", Message::Ping(_) => "ping", Message::Pong(_) => "pong", Message::Close(_) => "close", Message::Frame(_) => "frame" }, message.len(), data_frame_payload_size, first_frame_control_budget, format!("{write_deadline:?}"), true, control_write_timeout, data_frame_write_timeout, true, true, recovery.active, true, true, true);
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
            return Err(RequestWriteFailure::deadline(
                wrote_frame,
                wrote_frame,
                write_budget,
            ));
        }
        if connection_cancel.is_cancelled() {
            return Err(RequestWriteFailure::connection(
                NetError::from(crate::error::ErrorKind::Cancelled),
                wrote_frame,
            ));
        }
        let mut remaining_controls = next_control_budget;
        next_control_budget = MAX_READY_CONTROLS_PER_BOUNDARY;
        // 控制指令优先于已经到期的 heartbeat。
        let control_action = drain_ready_controls_with_recovery(
            write,
            control_rx,
            control_write_timeout,
            connection_cancel,
            heartbeat,
            heartbeat_state,
            recovery,
            write_budget,
            &mut remaining_controls,
        )
        .await
        .map_err(|error| classify_boundary_failure(error, wrote_frame, write_budget))?;
        match control_action {
            ControlAction::Continue => {}
            ControlAction::Stop(ControlStop::ConnectionClosed) => {
                return Err(RequestWriteFailure::connection(
                    NetError::from(crate::error::ErrorKind::Closed),
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
                    write_budget,
                ));
            }
        }
        if let Some(pong_timeout) = heartbeat.tick().now_or_never() {
            let now = Instant::now();
            if now >= write_deadline {
                return Err(RequestWriteFailure::deadline(
                    wrote_frame,
                    wrote_frame,
                    write_budget,
                ));
            }
            within_request_budget(
                write_budget,
                handle_heartbeat_tick_with_budget(
                    write,
                    heartbeat_state,
                    recovery,
                    pong_timeout,
                    control_write_timeout,
                    connection_cancel,
                    Some(write_budget),
                ),
            )
            .await
            .map_err(|error| classify_boundary_failure(error, wrote_frame, write_budget))?;
        }
        // heartbeat 写入期间可能又收到 Ping/Close，因此紧邻 data frame 再排空一次。
        if remaining_controls > 0 {
            let control_action = drain_ready_controls_with_recovery(
                write,
                control_rx,
                control_write_timeout,
                connection_cancel,
                heartbeat,
                heartbeat_state,
                recovery,
                write_budget,
                &mut remaining_controls,
            )
            .await
            .map_err(|error| classify_boundary_failure(error, wrote_frame, write_budget))?;
            match control_action {
                ControlAction::Continue => {}
                ControlAction::Stop(ControlStop::ConnectionClosed) => {
                    return Err(RequestWriteFailure::connection(
                        NetError::from(crate::error::ErrorKind::Closed),
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
                        write_budget,
                    ));
                }
            }
        }
        send_data_frame_with_phase(
            write,
            frame,
            wrote_frame,
            write_budget,
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

/// Preserve the TLS layer even when a partially sent frame has unknown delivery.
fn unknown_delivery_error(error: WsError) -> NetError {
    let stage = match &error {
        WsError::Io(error) => crate::module::transport::tls_error_stage(error)
            .unwrap_or(crate::error::ErrorStage::Write),
        WsError::Tls(_) => crate::error::ErrorStage::Tls,
        _ => crate::error::ErrorStage::Write,
    };
    NetError::with_source(crate::error::ErrorKind::DeliveryUnknown, error).with_stage(stage)
}

/// Classifies a control/heartbeat boundary failure without confusing the control frame's
/// uncertain write state with delivery of a business frame that has not started.
fn classify_boundary_failure(
    error: NetError,
    data_started: bool,
    request_budget: TimeoutBudget,
) -> RequestWriteFailure {
    let request_deadline = Instant::from_std(request_budget.at);
    crate::log_t!(LogType::WSC; "ws.write.classify_boundary_failure", "error_kind|error_stage|data_started|request_deadline|deadline_elapsed", format!("{:?}", error.kind()), format!("{:?}", error.context().stage), data_started, format!("{request_deadline:?}"), Instant::now() >= request_deadline);
    if error.kind() == crate::error::ErrorKind::TimedOut
        && error.context().stage != Some(crate::error::ErrorStage::Heartbeat)
    {
        RequestWriteFailure::deadline(data_started, data_started, request_budget)
    } else if error.kind() == crate::error::ErrorKind::DeliveryUnknown
        && error.ws_timeout_phase().is_some()
    {
        // The request deadline bounded an in-flight control write. The business request is still
        // known-unsent when `data_started` is false, but the shared sink must be discarded.
        RequestWriteFailure::deadline(data_started, true, request_budget)
    } else {
        RequestWriteFailure::connection(error, data_started)
    }
}

fn classify_control_failure(
    error: NetError,
    origin: ControlOrigin,
    data_started: bool,
    request_budget: TimeoutBudget,
) -> RequestWriteFailure {
    let request_deadline = Instant::from_std(request_budget.at);
    crate::log_t!(LogType::WSC; "ws.write.classify_control_failure", "error_kind|error_stage|origin|data_started|request_deadline|deadline_elapsed", format!("{:?}", error.kind()), format!("{:?}", error.context().stage), format!("{origin:?}"), data_started, format!("{request_deadline:?}"), Instant::now() >= request_deadline);
    if origin == ControlOrigin::LocalClose {
        RequestWriteFailure::local_close(data_started, Some(error))
    } else {
        classify_boundary_failure(error, data_started, request_budget)
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
    request_budget: TimeoutBudget,
    frame_write_timeout: Duration,
    connection_cancel: &CancellationToken,
    dispatch_cancel: &CancellationToken,
    dispatch_phase: &OperationControl,
) -> Result<(), RequestWriteFailure>
where
    W: Sink<Message, Error = WsError> + Unpin,
{
    let request_deadline = Instant::from_std(request_budget.at);
    crate::log_t!(LogType::WSC; "ws.write.send_data_frame_with_phase", "arg_write_present|arg_frame_bytes|data_started|request_deadline|frame_write_timeout|arg_connection_cancel_present|arg_dispatch_cancel_present|arg_dispatch_phase_present|deadline_elapsed", true, frame.len(), data_started, format!("{request_deadline:?}"), frame_write_timeout, true, true, true, Instant::now() >= request_deadline);
    let now = Instant::now();
    if dispatch_cancel.is_cancelled() {
        return Err(RequestWriteFailure::cancelled(data_started));
    }
    if now >= request_deadline {
        return Err(RequestWriteFailure::deadline(
            data_started,
            data_started,
            request_budget,
        ));
    }
    if connection_cancel.is_cancelled() {
        return Err(RequestWriteFailure::connection(
            NetError::from(crate::error::ErrorKind::Cancelled),
            data_started,
        ));
    }
    let frame_deadline = match now.checked_add(frame_write_timeout) {
        Some(deadline) => deadline,
        None => request_deadline,
    };
    let request_deadline_wins = request_deadline <= frame_deadline;
    let deadline = request_deadline.min(frame_deadline);
    let selected_budget = if request_deadline_wins {
        request_budget
    } else {
        TimeoutBudget::new(frame_deadline.into_std(), crate::ws::WsTimeoutPhase::Write)
    };
    if let Err(error) = dispatch_phase.begin_write_attempt() {
        return Err(RequestWriteFailure::connection(
            error,
            data_started || dispatch_phase.data_write_started(),
        ));
    }
    let mut frame = Some(frame);
    let send = async {
        #[cfg(feature = "quality-doctor")]
        let readiness = dispatch_phase.quality().map(|probe| probe.phase(crate::api::quality::PhaseKind::SinkReadiness));
        std::future::poll_fn(|cx| {
            match Pin::new(&mut *write).poll_ready(cx) {
                std::task::Poll::Pending => return std::task::Poll::Pending,
                std::task::Poll::Ready(Err(error)) => {
                    crate::log_e!(LogType::WSC; "ws.write.send_data_frame_with_phase", "error|stage", crate::common::log::summary::error(&error), "readiness");
                    return std::task::Poll::Ready(Err(RequestWriteFailure::connection(error.into(), data_started || dispatch_phase.data_write_started())));
                }
                std::task::Poll::Ready(Ok(())) => {}
            }
            if connection_cancel.is_cancelled() {
                return std::task::Poll::Ready(Err(RequestWriteFailure::connection(NetError::from(crate::error::ErrorKind::Cancelled), data_started || dispatch_phase.data_write_started())));
            }
            if Instant::now() >= deadline {
                return std::task::Poll::Ready(Err(RequestWriteFailure::deadline(data_started || dispatch_phase.data_write_started(), true, selected_budget)));
            }
            let may_write = match dispatch_phase.start_data_write(data_started) {
                Ok(may_write) => may_write,
                Err(error) => {
                    return std::task::Poll::Ready(Err(RequestWriteFailure::connection(
                        error,
                        data_started || dispatch_phase.data_write_started(),
                    )));
                }
            };
            if dispatch_cancel.is_cancelled() || !may_write {
                return std::task::Poll::Ready(Err(RequestWriteFailure::cancelled(data_started || dispatch_phase.data_write_started())));
            }
            let Some(frame) = frame.take() else {
                crate::log_e!(LogType::WSC; "ws.write.send_data_frame_with_phase", "error", "data_frame_already_consumed");
                return std::task::Poll::Ready(Err(RequestWriteFailure::connection(NetError::from(crate::error::ErrorKind::Internal), true)));
            };
            // No await separates this CAS from start_send. Cancellation that wins
            // the same atomic state prevents this request from entering the sink.
            std::task::Poll::Ready(Pin::new(&mut *write).start_send(frame).map_err(|error| {
                crate::log_e!(LogType::WSC; "ws.write.send_data_frame_with_phase", "error|delivery", crate::common::log::summary::error(&error), "unknown");
                RequestWriteFailure::connection(unknown_delivery_error(error), true)
            }))
        }).await?;
        // The cancellation CAS -> start_send sequence above stays untouched.
        #[cfg(feature = "quality-doctor")]
        {
            drop(readiness);
            if let Some(probe) = dispatch_phase.quality() {
                probe.mark(crate::api::quality::PhaseKind::StartSend);
            }
        }
        #[cfg(feature = "quality-doctor")]
        let _flush = dispatch_phase.quality().map(|probe| probe.phase(crate::api::quality::PhaseKind::Flush));
        write.flush().await.map_err(|error| {
            crate::log_e!(LogType::WSC; "ws.write.send_data_frame_with_phase", "error|delivery", crate::common::log::summary::error(&error), "unknown");
            RequestWriteFailure::connection(unknown_delivery_error(error), true)
        })
    };
    tokio::select! {
        biased;
        _ = dispatch_cancel.cancelled() => Err(RequestWriteFailure::cancelled(data_started || dispatch_phase.data_write_started())),
        result = tokio::time::timeout_at(deadline, send) => match result {
            Ok(Ok(())) => {
                crate::log_s!(LogType::WSC; "ws.write.send_data_frame_with_phase", "state", "frame_written");
                Ok(())
            },
            Ok(Err(failure)) => Err(failure),
            Err(_) => {
                let data_started = data_started || dispatch_phase.data_write_started();
                if request_deadline_wins || !data_started {
                    // Readiness expiry is a Write timeout even when no business
                    // data entered the sink; Closed would prohibit its retry.
                    Err(RequestWriteFailure::deadline(data_started, true, selected_budget))
                } else {
                    Err(RequestWriteFailure::connection(budget_delivery_unknown(selected_budget), true))
                }
            },
        },
        _ = connection_cancel.cancelled() => {
            Err(RequestWriteFailure::connection(NetError::from(crate::error::ErrorKind::Cancelled), data_started || dispatch_phase.data_write_started()))
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
    heartbeat: &mut HeartbeatSchedule,
    heartbeat_state: &HeartbeatState,
    connection_cancel: &CancellationToken,
    dispatch_cancel: &CancellationToken,
) -> Result<(), RequestWriteFailure>
where
    W: Sink<Message, Error = WsError> + Unpin,
{
    let phase = crate::module::ws_client::v2_test_support::operation(
        &crate::ws::SendOptions::default(),
        false,
        1,
    )
    .map_err(|error| RequestWriteFailure::connection(error, false))?;
    phase
        .enqueue()
        .map_err(|error| RequestWriteFailure::connection(error, false))?;
    if !phase.mark_writing(crate::ws::ConnectionId::from_allocated(1)) {
        return Err(RequestWriteFailure::connection(
            NetError::from(crate::error::ErrorKind::Internal),
            false,
        ));
    }
    let mut recovery = HeartbeatRecovery::new(HeartbeatRetryConfig::default());
    send_request_message_with_phase(
        write,
        message,
        data_frame_payload_size,
        first_frame_control_budget,
        TimeoutBudget::new(write_deadline.into_std(), crate::ws::WsTimeoutPhase::Write),
        control_rx,
        control_write_timeout,
        data_frame_write_timeout,
        heartbeat,
        heartbeat_state,
        &mut recovery,
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
    let phase = crate::module::ws_client::v2_test_support::operation(
        &crate::ws::SendOptions::default(),
        false,
        1,
    )
    .map_err(|error| RequestWriteFailure::connection(error, false))?;
    phase
        .enqueue()
        .map_err(|error| RequestWriteFailure::connection(error, false))?;
    if !phase.mark_writing(crate::ws::ConnectionId::from_allocated(1)) {
        return Err(RequestWriteFailure::connection(
            NetError::from(crate::error::ErrorKind::Internal),
            false,
        ));
    }
    send_data_frame_with_phase(
        write,
        frame,
        data_started,
        TimeoutBudget::new(
            request_deadline.into_std(),
            crate::ws::WsTimeoutPhase::Write,
        ),
        frame_write_timeout,
        connection_cancel,
        dispatch_cancel,
        &phase,
    )
    .await
}

/// 发送一个控制帧；超时意味着底层提交状态未知，当前连接不得继续使用。
enum ControlWriteOutcome {
    Complete,
    Retryable(NetError),
    Failed(NetError),
}

async fn send_control_frame_outcome<W>(
    write: &mut W,
    frame: Message,
    timeout: Duration,
    cancel: &CancellationToken,
    request_budget: Option<TimeoutBudget>,
) -> ControlWriteOutcome
where
    W: Sink<Message, Error = WsError> + Unpin,
{
    let expected_ping = match &frame {
        Message::Ping(payload) => Some(payload.clone()),
        _ => None,
    };
    let deadline = match Instant::now().checked_add(timeout) {
        Some(deadline) => deadline,
        None => {
            return ControlWriteOutcome::Failed(NetError::from(
                crate::error::ErrorKind::InvalidConfig,
            ));
        }
    };
    let selected_budget = request_budget.filter(|budget| budget.at <= deadline.into_std());
    let deadline = selected_budget.map_or(deadline, |budget| Instant::from_std(budget.at));
    tokio::select! {
        biased;
        _ = cancel.cancelled() => ControlWriteOutcome::Failed(NetError::from(crate::error::ErrorKind::Cancelled)),
        result = tokio::time::timeout_at(deadline, write.send(frame)) => match result {
            Ok(Ok(())) => ControlWriteOutcome::Complete,
            Ok(Err(WsError::WriteBufferFull(rejected)))
                if expected_ping.as_ref().is_some_and(|expected| {
                    matches!(&*rejected, Message::Ping(payload) if payload == expected)
                }) =>
            {
                ControlWriteOutcome::Retryable(
                    NetError::from(crate::error::ErrorKind::QueueFull)
                        .with_stage(crate::error::ErrorStage::Write),
                )
            }
            Ok(Err(WsError::WriteBufferFull(_))) => ControlWriteOutcome::Failed(
                NetError::from(crate::error::ErrorKind::DeliveryUnknown)
                    .with_stage(crate::error::ErrorStage::Write),
            ),
            Ok(Err(error)) => ControlWriteOutcome::Failed(error.into()),
            Err(_) => ControlWriteOutcome::Failed(selected_budget.map_or_else(
                || NetError::from(crate::error::ErrorKind::DeliveryUnknown),
                budget_delivery_unknown,
            )),
        },
    }
}

async fn send_control_frame<W>(
    write: &mut W,
    frame: Message,
    timeout: Duration,
    cancel: &CancellationToken,
) -> Result<(), NetError>
where
    W: Sink<Message, Error = WsError> + Unpin,
{
    crate::log_t!(LogType::WSC; "ws.write.send_control_frame", "arg_write_present|arg_frame_type|arg_frame_bytes|timeout|arg_cancel_present", true, match &frame { Message::Ping(_) => "ping", Message::Pong(_) => "pong", Message::Close(_) => "close", _ => "data" }, frame.len(), timeout, true);
    match send_control_frame_outcome(write, frame, timeout, cancel, None).await {
        ControlWriteOutcome::Complete => Ok(()),
        ControlWriteOutcome::Retryable(error) | ControlWriteOutcome::Failed(error) => Err(error),
    }
}

/// 控制消息处理后写循环应继续还是停止。
#[derive(Clone, Debug)]
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
        crate::log_t!(LogType::WSC; "new", "message_bytes|frame_size", message.len(), frame_size);
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
        crate::log_t!(LogType::WSC; "next");
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
    crate::log_t!(LogType::WSC; "utf8_payload", "payload_bytes", payload.len());
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
    kind: IoEndKind,
    cancel: &CancellationToken,
) {
    let error = if error.context().stage.is_none() {
        error.with_stage(crate::error::ErrorStage::Write)
    } else {
        error
    };
    crate::log_t!(LogType::WSC; "ws.write.send_write_end", "arg_io_event_tx_present|generation|error_kind|error_stage|kind|arg_cancel_present|cancelled", true, generation, format!("{:?}", error.kind()), format!("{:?}", error.context().stage), format!("{kind:?}"), true, cancel.is_cancelled());
    if matches!(
        error.kind(),
        crate::error::ErrorKind::Cancelled | crate::error::ErrorKind::Closed
    ) {
        crate::log_s!(LogType::WSC; "ws.write.send_write_end", "state|generation|error", "write_ended", generation, format!("{:?}/{:?}", error.kind(), error.context().stage));
    } else {
        crate::log_e!(LogType::WSC; "ws.write.send_write_end", "generation|error", generation, format!("{:?}/{:?}", error.kind(), error.context().stage));
    }
    tokio::select! {
        biased;
        _ = cancel.cancelled() => {
            crate::log_s!(LogType::WSC; "ws.write.send_write_end", "state|generation", "end_notification_cancelled", generation);
        }
        result = io_event_tx.send(IoEvent::WriteEnded { generation, error, kind }) => {
            if result.is_err() {
                crate::log_s!(LogType::WSC; "ws.write.send_write_end", "state|generation", "worker_event_receiver_closed", generation);
            }
        }
    }
}

/// 以同一个终态错误结束一批已从队列移出的请求。
///
/// 对每条请求先按 UUID 与令牌删除匹配的待响应记录，再释放其任务/字节许可，
/// 并通知原始发送调用方。令牌检查可避免迟到清理删除后来复用相同 UUID 的记录。
pub(crate) fn fail_requests(requests: Vec<QueuedRequest>, error: NetError) {
    for request in requests {
        request.complete(Err(error.clone()));
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
#[path = "ws_write/retry_budget_tests.rs"]
mod retry_budget_tests;

#[cfg(test)]
/// Frame-level tests retain the original programmable sinks and wire assertions.
mod tests {
    use super::*;
    use crate::module::ws_client::test_support::{
        check, check_eq, check_ne, test_error, TestResult,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};
    use tokio::sync::oneshot;
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
        fail_on_send: bool,
        injected_error: Option<WsError>,
    }

    impl NthFlushErrorSink {
        fn new(fail_on_flush: usize) -> Self {
            Self {
                messages: Arc::new(Mutex::new(Vec::new())),
                flush_count: 0,
                fail_on_flush,
                fail_on_send: false,
                injected_error: None,
            }
        }

        fn with_injected_error(error: WsError, fail_on_send: bool) -> Self {
            Self {
                fail_on_send,
                injected_error: Some(error),
                ..Self::new(1)
            }
        }

        fn recorded(&self) -> Arc<Mutex<Vec<Message>>> {
            Arc::clone(&self.messages)
        }

        fn failure(&mut self) -> WsError {
            self.injected_error.take().unwrap_or_else(|| {
                WsError::Io(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "injected flush failure",
                ))
            })
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
            let this = self.get_mut();
            this.messages
                .lock()
                .map_err(|error| {
                    WsError::Io(std::io::Error::other(format!(
                        "recorded messages lock: {error}"
                    )))
                })?
                .push(message);
            if this.fail_on_send {
                return Err(this.failure());
            }
            Ok(())
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            let this = self.get_mut();
            this.flush_count += 1;
            if this.flush_count == this.fail_on_flush {
                Poll::Ready(Err(this.failure()))
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

    #[tokio::test]
    async fn tls_alerts_during_start_send_and_flush_preserve_unknown_delivery_and_tls_origin(
    ) -> TestResult {
        use crate::error::{ErrorKind, ErrorStage};

        #[derive(Clone, Copy)]
        enum Source {
            ProxyIo,
            TargetIo,
            TargetTls,
        }
        for source in [Source::ProxyIo, Source::TargetIo, Source::TargetTls] {
            for fail_on_send in [false, true] {
                let tls_error =
                    rustls::Error::AlertReceived(rustls::AlertDescription::BadCertificate);
                let (source, stage) = match source {
                    Source::ProxyIo => (
                        WsError::Io(crate::module::transport::test_proxy_tls_error(
                            std::io::Error::new(std::io::ErrorKind::InvalidData, tls_error),
                        )),
                        ErrorStage::ProxyTls,
                    ),
                    Source::TargetIo => (
                        WsError::Io(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            tls_error,
                        )),
                        ErrorStage::Tls,
                    ),
                    Source::TargetTls => (WsError::Tls(tls_error.into()), ErrorStage::Tls),
                };
                let mut sink = NthFlushErrorSink::with_injected_error(source, fail_on_send);
                let failure = send_data_frame(
                    &mut sink,
                    Message::Binary(Bytes::from_static(b"TLS delivery boundary")),
                    false,
                    Instant::now() + Duration::from_secs(1),
                    Duration::from_secs(1),
                    &CancellationToken::new(),
                    &CancellationToken::new(),
                )
                .await
                .err()
                .ok_or_else(|| test_error("injected TLS writer failure unexpectedly succeeded"))?;
                check_eq!(failure.request_error.kind(), ErrorKind::DeliveryUnknown)?;
                check_eq!(failure.request_error.context().stage, Some(stage))?;
                let RequestAction::StopWithError(connection_error) = failure.connection_action
                else {
                    return Err(test_error(
                        "TLS writer failure did not retire its connection",
                    ));
                };
                check_eq!(connection_error.kind(), ErrorKind::DeliveryUnknown)?;
                check_eq!(connection_error.context().stage, Some(stage))?;
                check_eq!(failure.requeue, RequestRequeue::IdempotentRetry)?;
                check_eq!(sink.flush_count, usize::from(!fail_on_send))?;
                check!(sink.injected_error.is_none(), "TLS source was not consumed")?;
                check_eq!(
                    sink.recorded()
                        .lock()
                        .map_err(|error| test_error(format!("recorded messages lock: {error:?}")))?
                        .len(),
                    1
                )?;
            }
        }
        Ok(())
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

    struct RetryablePingSink {
        attempts: usize,
        messages: Arc<Mutex<Vec<Message>>>,
    }

    impl RetryablePingSink {
        fn new() -> Self {
            Self {
                attempts: 0,
                messages: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn recorded(&self) -> Arc<Mutex<Vec<Message>>> {
            Arc::clone(&self.messages)
        }
    }

    impl Sink<Message> for RetryablePingSink {
        type Error = WsError;

        fn poll_ready(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, message: Message) -> Result<(), Self::Error> {
            let this = self.get_mut();
            if this.attempts == 0 && matches!(message, Message::Ping(_)) {
                this.attempts = 1;
                return Err(WsError::WriteBufferFull(Box::new(message)));
            }
            this.messages
                .lock()
                .map_err(|error| {
                    WsError::Io(std::io::Error::other(format!("messages lock: {error}")))
                })?
                .push(message);
            Ok(())
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
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
            &mut HeartbeatSchedule::from_interval(heartbeat, Duration::from_secs(7_200)),
            &heartbeat_state,
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
        let heartbeat = tokio::time::interval_at(Instant::now() + period, period);
        let heartbeat_state = HeartbeatState::new(1);
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut heartbeat = HeartbeatSchedule::from_interval(heartbeat, period);
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
                NetError::from(crate::error::ErrorKind::Cancelled),
                if data_started {
                    RequestAction::StopWithError(NetError::from(
                        crate::error::ErrorKind::DeliveryUnknown,
                    ))
                } else {
                    RequestAction::Continue
                },
                RequestRequeue::Never,
            ),
            BoundaryInterruption::Deadline => (
                NetError::from(crate::error::ErrorKind::TimedOut),
                if data_started {
                    RequestAction::StopWithError(NetError::from(
                        crate::error::ErrorKind::DeliveryUnknown,
                    ))
                } else {
                    RequestAction::Continue
                },
                RequestRequeue::Never,
            ),
            BoundaryInterruption::ConnectionCancelled => (
                if data_started {
                    NetError::from(crate::error::ErrorKind::DeliveryUnknown)
                } else {
                    NetError::from(crate::error::ErrorKind::Cancelled)
                },
                RequestAction::Stop,
                if data_started {
                    RequestRequeue::IdempotentRetry
                } else {
                    RequestRequeue::WaitForReconnect
                },
            ),
        };
        check_eq!((failure.request_error).kind(), (expected_error).kind())?;
        check_eq!(
            request_action_key(&(failure.connection_action)),
            request_action_key(&(expected_action))
        )?;
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
        let heartbeat =
            tokio::time::interval_at(Instant::now() + heartbeat_interval, heartbeat_interval);
        let mut remaining_controls = MAX_READY_CONTROLS_PER_BOUNDARY;

        let action = drain_ready_controls(
            &mut sink,
            &mut control_rx,
            Duration::from_secs(1),
            &CancellationToken::new(),
            &mut HeartbeatSchedule::from_interval(heartbeat, Duration::from_secs(7_200)),
            &HeartbeatState::new(1),
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
            &mut HeartbeatSchedule::from_interval(heartbeat, Duration::from_secs(7_200)),
            &HeartbeatState::new(7),
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
            &mut HeartbeatSchedule::from_interval(heartbeat, Duration::from_secs(7_200)),
            &heartbeat_state,
            &CancellationToken::new(),
            &CancellationToken::new(),
        )
        .await;

        sink.verify()?;
        check_eq!(
            result.map_err(|failure| failure.request_error),
            Err(NetError::from(crate::error::ErrorKind::TimedOut))
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
            &mut HeartbeatSchedule::from_interval(heartbeat, Duration::from_secs(7_200)),
            &heartbeat_state,
            &cancel,
            &CancellationToken::new(),
        )
        .await;

        sink.finish_injection().await?;
        check_eq!(
            result.map_err(|failure| failure.request_error),
            Err(NetError::from(crate::error::ErrorKind::DeliveryUnknown))
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
            &mut HeartbeatSchedule::from_interval(heartbeat, Duration::from_secs(7_200)),
            &heartbeat_state,
            &CancellationToken::new(),
            &CancellationToken::new(),
        )
        .await;

        sink.finish_injection().await?;
        check_eq!(
            result.map_err(|failure| failure.request_error),
            Err(NetError::from(crate::error::ErrorKind::DeliveryUnknown))
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

        let mut heartbeat = HeartbeatSchedule::from_interval(heartbeat, Duration::from_secs(7_200));
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
            Err(NetError::from(crate::error::ErrorKind::DeliveryUnknown))
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
            Err(NetError::from(crate::error::ErrorKind::Cancelled))
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
            Err(NetError::from(crate::error::ErrorKind::DeliveryUnknown))
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
            &mut HeartbeatSchedule::from_interval(heartbeat, Duration::from_secs(7_200)),
            &heartbeat_state,
            &CancellationToken::new(),
            &CancellationToken::new(),
        )
        .await;

        check_eq!(
            result.map_err(|failure| failure.request_error),
            Err(NetError::from(crate::error::ErrorKind::DeliveryUnknown))
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
        check_eq!(
            error,
            Err(NetError::from(crate::error::ErrorKind::DeliveryUnknown))
        )?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn normal_ping_retries_only_a_provably_unaccepted_write() -> TestResult {
        let mut sink = RetryablePingSink::new();
        let recorded = sink.recorded();
        let retry = HeartbeatRetryConfig {
            ping_write_retries: 1,
            ping_write_retry_interval: Duration::from_millis(5),
            ..HeartbeatRetryConfig::default()
        };
        let cancel = CancellationToken::new();
        let heartbeat_state = HeartbeatState::new(79);
        let mut recovery = HeartbeatRecovery::new(retry);
        check_eq!(
            handle_heartbeat_tick_with_recovery(
                &mut sink,
                &heartbeat_state,
                &mut recovery,
                Duration::from_secs(1),
                Duration::from_secs(1),
                &cancel,
            )
            .await,
            Ok(())
        )?;
        tokio::time::advance(Duration::from_millis(5)).await;
        check_eq!(
            service_due_heartbeat_work(
                &mut sink,
                &heartbeat_state,
                &mut recovery,
                Duration::from_secs(1),
                None,
                &cancel,
            )
            .await,
            Ok(())
        )?;
        let messages = recorded
            .lock()
            .map_err(|error| test_error(format!("messages lock: {error:?}")))?;
        check_eq!(messages.len(), 1)?;
        check!(matches!(&messages[0], Message::Ping(payload) if !payload.is_empty()))?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn normal_ping_retry_does_not_repeat_an_unknown_delivery() -> TestResult {
        let mut sink = PendingSink::default();
        let state = HeartbeatState::new(80);
        let cancel = CancellationToken::new();
        let mut recovery = HeartbeatRecovery::new(HeartbeatRetryConfig {
            ping_write_retries: 3,
            ping_write_retry_interval: Duration::from_millis(1),
            ..HeartbeatRetryConfig::default()
        });
        let result = handle_heartbeat_tick_with_recovery(
            &mut sink,
            &state,
            &mut recovery,
            Duration::from_secs(1),
            Duration::from_millis(5),
            &cancel,
        )
        .await;
        check_eq!(
            result,
            Err(NetError::from(crate::error::ErrorKind::DeliveryUnknown))
        )?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn normal_ping_retry_is_cancelled_after_a_matching_pong_race() -> TestResult {
        let mut sink = RetryablePingSink::new();
        let recorded = sink.recorded();
        let heartbeat_state = HeartbeatState::new(82);
        let now = Instant::now();
        let HeartbeatTick::SendProbe(payload) =
            heartbeat_state.on_tick(now, Duration::from_secs(1))
        else {
            return Err(test_error("normal probe was not created"));
        };
        let mut recovery = HeartbeatRecovery::new(HeartbeatRetryConfig {
            ping_write_retries: 1,
            ping_write_retry_interval: Duration::from_millis(5),
            ..HeartbeatRetryConfig::default()
        });
        recovery.schedule_normal_retry(payload.clone(), now)?;
        check!(heartbeat_state.acknowledge_pong(payload.as_ref()))?;
        tokio::time::advance(Duration::from_millis(5)).await;
        service_due_heartbeat_work(
            &mut sink,
            &heartbeat_state,
            &mut recovery,
            Duration::from_secs(1),
            None,
            &CancellationToken::new(),
        )
        .await?;
        let messages = recorded
            .lock()
            .map_err(|error| test_error(format!("messages lock: {error:?}")))?;
        check!(messages.is_empty())?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn pong_timeout_uses_the_configured_bounded_recovery_rounds() -> TestResult {
        let heartbeat_state = HeartbeatState::new(81);
        let mut sink = RecordingSink::new(None);
        let recorded = sink.recorded();
        let mut recovery = HeartbeatRecovery::new(HeartbeatRetryConfig {
            pong_recovery_attempts: 2,
            pong_retry_interval: Duration::from_secs(1),
            ..HeartbeatRetryConfig::default()
        });
        let cancel = CancellationToken::new();
        check_eq!(
            handle_heartbeat_tick_with_recovery(
                &mut sink,
                &heartbeat_state,
                &mut recovery,
                Duration::from_secs(1),
                Duration::from_secs(1),
                &cancel,
            )
            .await,
            Ok(())
        )?;
        tokio::time::advance(Duration::from_secs(1)).await;
        check_eq!(
            handle_heartbeat_tick_with_recovery(
                &mut sink,
                &heartbeat_state,
                &mut recovery,
                Duration::from_secs(1),
                Duration::from_secs(1),
                &cancel,
            )
            .await,
            Ok(())
        )?;
        tokio::time::advance(Duration::from_secs(1)).await;
        check_eq!(
            service_due_heartbeat_work(
                &mut sink,
                &heartbeat_state,
                &mut recovery,
                Duration::from_secs(1),
                None,
                &cancel,
            )
            .await,
            Ok(())
        )?;
        tokio::time::advance(Duration::from_secs(1)).await;
        let result = service_due_heartbeat_work(
            &mut sink,
            &heartbeat_state,
            &mut recovery,
            Duration::from_secs(1),
            None,
            &cancel,
        )
        .await;
        let error = result
            .err()
            .ok_or_else(|| test_error("recovery unexpectedly succeeded"))?;
        check_eq!(error.kind(), crate::error::ErrorKind::TimedOut)?;
        check_eq!(
            error.context().stage,
            Some(crate::error::ErrorStage::Heartbeat)
        )?;
        let messages = recorded
            .lock()
            .map_err(|error| test_error(format!("messages lock: {error:?}")))?;
        check_eq!(messages.len(), 3)?;
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
            Err(NetError::from(crate::error::ErrorKind::TimedOut))
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
        check_eq!(
            result,
            Err(NetError::from(crate::error::ErrorKind::DeliveryUnknown))
        )?;
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

    #[tokio::test(start_paused = true)]
    async fn disabled_heartbeat_keeps_controls_working_without_sending_probes() -> TestResult {
        let sink = RecordingSink::new(None);
        let recorded = sink.recorded();
        let flushes = sink.flushes();
        let (control_tx, control_rx) = mpsc::channel(2);
        let (io_event_tx, mut io_event_rx) = mpsc::channel(1);
        let writer = tokio::spawn(run_write_loop(
            sink,
            WriteLoopContext {
                cancel_domain_gate: None,
                queue: PriorityWriteQueue::new(1, 64)?,
                urgent_queue: PriorityWriteQueue::new(1, 64)?,
                control_rx,
                pending_requests: crate::module::ws_client::v2_test_support::pending(1)?,
                io_event_tx,
                generation: 34,
                cancel: CancellationToken::new(),
                data_frame_payload_size: Some(1024),
                control_write_timeout: Duration::from_secs(1),
                data_frame_write_timeout: Duration::from_secs(1),
                heartbeat_config: None,
                heartbeat: Arc::new(HeartbeatState::new(34)),
            },
            CancellationToken::new(),
        ));
        // Confirm the writer has initialized its schedule before advancing time.
        control_tx.send(ControlMessage::FlushAutomatic).await?;
        tokio::time::timeout(Duration::from_secs(1), async {
            while flushes.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await?;
        tokio::time::advance(Duration::from_secs(86_400)).await;
        // The peer-Close acknowledgement is a barrier after both queued controls.
        control_tx.send(ControlMessage::FlushAutomatic).await?;
        let (closed_tx, closed_rx) = tokio::sync::oneshot::channel();
        control_tx
            .send(ControlMessage::PeerClose(closed_tx))
            .await?;
        closed_rx.await??;
        writer.await?;
        check!(recorded
            .lock()
            .map_err(|e| test_error(e.to_string()))?
            .is_empty())?;
        check!(flushes.load(Ordering::SeqCst) >= 3)?;
        check!(io_event_rx.recv().await.is_none())?;
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
                cancel_domain_gate: None,
                queue: business_queue,
                urgent_queue,
                control_rx,
                pending_requests: crate::module::ws_client::v2_test_support::pending(1)?,
                io_event_tx,
                generation: 33,
                cancel: CancellationToken::new(),
                data_frame_payload_size: Some(1024),
                control_write_timeout: Duration::from_secs(1),
                data_frame_write_timeout: Duration::from_secs(1),
                heartbeat_config: Some(crate::ws::HeartbeatConfig {
                    interval: Duration::from_secs(60),
                    pong_timeout: Duration::from_secs(5),
                    ..Default::default()
                }),
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
                error,
                ..
            } if matches!(error.kind(), crate::error::ErrorKind::TimedOut)))?;
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
                cancel_domain_gate: None,
                queue: business_queue,
                urgent_queue,
                control_rx,
                pending_requests: crate::module::ws_client::v2_test_support::pending(1)?,
                io_event_tx,
                generation: 35,
                cancel: CancellationToken::new(),
                data_frame_payload_size: Some(1024),
                control_write_timeout: Duration::from_secs(1),
                data_frame_write_timeout: Duration::from_secs(1),
                heartbeat_config: Some(crate::ws::HeartbeatConfig {
                    interval: Duration::from_secs(60),
                    pong_timeout: Duration::from_secs(5),
                    ..Default::default()
                }),
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
                error,
                ..
            }) if matches!(error.kind(), crate::error::ErrorKind::TimedOut)))?;
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
        let request = crate::module::ws_client::v2_test_support::queued(
            1,
            crate::ws::SendOptions::default(),
        )?;
        let business_result = request.dispatch_phase.clone();
        business_queue.push_existing(request).map_err(|(_, e)| e)?;

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
                cancel_domain_gate: None,
                queue: Arc::clone(&business_queue),
                urgent_queue,
                control_rx,
                pending_requests: crate::module::ws_client::v2_test_support::pending(1)?,
                io_event_tx,
                generation: 36,
                cancel: cancel.clone(),
                data_frame_payload_size: Some(1024),
                control_write_timeout: Duration::from_secs(1),
                data_frame_write_timeout: Duration::from_secs(1),
                heartbeat_config: Some(crate::ws::HeartbeatConfig {
                    interval: Duration::from_secs(3_600),
                    pong_timeout: Duration::from_secs(7_200),
                    ..Default::default()
                }),
                heartbeat: Arc::new(HeartbeatState::new(36)),
            },
            CancellationToken::new(),
        ));

        let business_result = business_result.written().await;
        let controls_before_data = flushes_before_first_data.load(Ordering::Relaxed);
        cancel.cancel();
        drop(control_tx);
        writer
            .await
            .map_err(|error| test_error(format!("writer task: {error:?}")))?;

        check_eq!(business_result, Ok(crate::ws::WriteOutcome::Written))?;
        check!(
            controls_before_data <= MAX_READY_CONTROLS_PER_BOUNDARY,
            "main-loop controls and the first frame boundary must share one total budget"
        )?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn before_first_frame_control_failure_preserves_delivery_and_reconnect_policy(
    ) -> TestResult {
        use crate::module::ws_client::v2_test_support as fixture;
        use crate::ws::{DeliveryEvidence, DisconnectedPolicy, OperationPhase, SendOptions};
        for wait in [false, true] {
            for cause in [0, 1, 2] {
                let source = PriorityWriteQueue::new(1, 32)?;
                let request = fixture::queued(
                    79,
                    SendOptions {
                        disconnected: if wait {
                            DisconnectedPolicy::WaitForReconnect
                        } else {
                            DisconnectedPolicy::Reject
                        },
                        ..Default::default()
                    },
                )?;
                let core = request.dispatch_phase.clone();
                let (control_tx, mut controls) = mpsc::channel(1);
                if cause == 2 {
                    control_tx.send(ControlMessage::FlushAutomatic).await?;
                }
                let _control_owner = if cause == 0 {
                    drop(control_tx);
                    None
                } else {
                    Some(control_tx)
                };
                let heartbeat = HeartbeatState::new(1);
                if cause == 1 {
                    let now = Instant::now();
                    let HeartbeatTick::SendProbe(payload) = heartbeat.on_tick(now, Duration::ZERO)
                    else {
                        return Err(test_error("probe missing"));
                    };
                    check!(heartbeat.mark_sent(&payload, now))?;
                }
                let mut sink = PendingSink::default();
                let period = Duration::from_secs(3600);
                let mut schedule = HeartbeatSchedule::from_interval(
                    tokio::time::interval_at(Instant::now() + period, period),
                    period,
                );
                let mut recovery = HeartbeatRecovery::new(HeartbeatRetryConfig::default());
                let action = handle_queued_request(
                    &mut sink,
                    request,
                    &source,
                    &fixture::pending(1)?,
                    1,
                    None,
                    MAX_READY_CONTROLS_PER_BOUNDARY,
                    &mut controls,
                    Duration::from_millis(25),
                    Duration::from_secs(1),
                    &mut schedule,
                    &heartbeat,
                    &mut recovery,
                    &CancellationToken::new(),
                )
                .await;
                sink.verify()?;
                match cause {
                    0 => check_eq!(action, RequestAction::Stop)?,
                    1 => {
                        let RequestAction::StopWithError(error) = action else {
                            return Err(test_error("heartbeat did not retire writer"));
                        };
                        check_eq!(error.kind(), crate::error::ErrorKind::TimedOut)?;
                        check_eq!(
                            error.context().stage,
                            Some(crate::error::ErrorStage::Heartbeat)
                        )?;
                    }
                    _ => check_eq!(
                        action,
                        RequestAction::StopWithError(NetError::from(
                            crate::error::ErrorKind::DeliveryUnknown
                        ))
                    )?,
                }
                check_eq!(core.snapshot()?.delivery, DeliveryEvidence::NotStarted)?;
                if wait {
                    check_eq!(core.snapshot()?.phase, OperationPhase::Queued)?;
                    check!(core.snapshot()?.result.is_none())?;
                    let retained = source
                        .try_next()
                        .ok_or_else(|| test_error("unsent request not retained"))?;
                    check_eq!(retained.sequence, 79)?;
                    check_eq!(retained.attempt, 0)?;
                    retained.complete(Err(NetError::from(crate::error::ErrorKind::Cancelled)));
                } else {
                    check!(source.try_next().is_none())?;
                    check!(core.written().await.is_err())?;
                }
            }
        }
        Ok(())
    }

    async fn drive_raw_write<W>(
        sink: &mut W,
        request: QueuedRequest,
        queue: &Arc<PriorityWriteQueue>,
        generation: u64,
    ) -> TestResult<RequestAction>
    where
        W: Sink<Message, Error = WsError> + Unpin,
    {
        drive_write_with_pending(
            sink,
            request,
            queue,
            generation,
            &crate::module::ws_client::v2_test_support::pending(1)?,
        )
        .await
    }

    async fn drive_write_with_pending<W>(
        sink: &mut W,
        request: QueuedRequest,
        queue: &Arc<PriorityWriteQueue>,
        generation: u64,
        pending: &Arc<NativePending>,
    ) -> TestResult<RequestAction>
    where
        W: Sink<Message, Error = WsError> + Unpin,
    {
        let (_controls, mut control_rx) = mpsc::channel(1);
        let period = Duration::from_secs(3_600);
        let mut schedule = HeartbeatSchedule::from_interval(
            tokio::time::interval_at(Instant::now() + period, period),
            period,
        );
        let heartbeat = HeartbeatState::new(generation);
        let mut recovery = HeartbeatRecovery::new(HeartbeatRetryConfig::default());
        Ok(handle_queued_request(
            sink,
            request,
            queue,
            pending,
            generation,
            None,
            MAX_READY_CONTROLS_PER_BOUNDARY,
            &mut control_rx,
            Duration::from_secs(1),
            Duration::from_secs(1),
            &mut schedule,
            &heartbeat,
            &mut recovery,
            &CancellationToken::new(),
        )
        .await)
    }

    #[tokio::test]
    async fn raw_retry_uses_delayed_queue_and_one_terminal_report() -> TestResult {
        use crate::api::retry::{
            Backoff, RetryEvent, RetrySchedule, RetrySetting, RetryStopReason,
        };
        use crate::ws::{SendOptions, WsReplaySafety, WsSendRetryOn, WsSendRetryPolicy};
        let events = Arc::new(Mutex::new(Vec::new()));
        let observed = events.clone();
        let policy = WsSendRetryPolicy::builder()
            .schedule(
                RetrySchedule::builder()
                    .max_attempts(2)
                    .backoff(Backoff::Constant(Duration::from_millis(30)))
                    .build()?,
            )
            .retry_on(WsSendRetryOn::none().with_write_errors(true))
            .safety(WsReplaySafety::IdempotentByContract)
            .observer(move |event: &RetryEvent| {
                if let Ok(mut events) = observed.lock() {
                    events.push(event.clone());
                }
            })
            .build()?;
        let request = crate::module::ws_client::v2_test_support::queued(
            42,
            SendOptions {
                retry_setting: RetrySetting::Policy(policy),
                ..Default::default()
            },
        )?;
        let control = request.dispatch_phase.clone();
        let operation_id = control.id();
        let queue = PriorityWriteQueue::new(1, 64)?;
        let mut first = RecordingSink::new(None);
        first.fail_on_flush = Some(1);
        check!(matches!(
            drive_raw_write(&mut first, request, &queue, 1).await?,
            RequestAction::StopWithError(_)
        ))?;
        check!(!control.is_finished())?;
        check_eq!(control.retry_report().attempts, 1)?;
        check!(queue.try_next().is_none())?;
        let request = crate::module::ws_client::v2_test_support::bounded(queue.next())
            .await?
            .ok_or_else(|| test_error("retry queue lost operation"))?;
        check_eq!(request.dispatch_phase.id(), operation_id)?;
        let mut second = RecordingSink::new(None);
        check_eq!(
            drive_raw_write(&mut second, request, &queue, 2).await?,
            RequestAction::Continue
        )?;
        check_eq!(control.written().await?, crate::ws::WriteOutcome::Written)?;
        let report = control.retry_report();
        check_eq!(report.attempts, 2)?;
        check_eq!(report.retry_count, 1)?;
        check_eq!(report.transport_sends, 2)?;
        check_eq!(report.stop_reason, Some(RetryStopReason::Completed))?;
        control.cancel()?;
        check_eq!(control.retry_report(), report)?;
        let events = events
            .lock()
            .map_err(|error| test_error(error.to_string()))?;
        check_eq!(events.len(), 6)?;
        check!(matches!(
            events.first(),
            Some(RetryEvent::AttemptStarted { attempt: 1 })
        ))?;
        check!(
            matches!(events.last(), Some(RetryEvent::Completed(final_report)) if final_report == &report)
        )?;
        Ok(())
    }

    #[tokio::test]
    async fn raw_retry_readiness_timeout_counts_attempt_without_transport_send() -> TestResult {
        use crate::api::retry::{RetrySchedule, RetrySetting, RetryStopReason};
        use crate::ws::{SendOptions, WsReplaySafety, WsSendRetryOn, WsSendRetryPolicy};
        let policy = WsSendRetryPolicy::builder()
            .schedule(RetrySchedule::builder().max_attempts(2).build()?)
            .retry_on(WsSendRetryOn::none().with_write_timeout(true))
            .safety(WsReplaySafety::IdempotentByContract)
            .build()?;
        let request = crate::module::ws_client::v2_test_support::queued(
            44,
            SendOptions {
                write_timeout: Duration::from_millis(10),
                retry_setting: RetrySetting::Policy(policy),
                ..Default::default()
            },
        )?;
        let control = request.dispatch_phase.clone();
        let queue = PriorityWriteQueue::new(1, 64)?;
        let mut blocked = PendingSink::default();
        check!(matches!(
            drive_raw_write(&mut blocked, request, &queue, 1).await?,
            RequestAction::StopWithError(_)
        ))?;
        blocked.verify()?;
        check_eq!(control.retry_report().attempts, 1)?;
        check_eq!(control.retry_report().transport_sends, 0)?;
        let request = queue
            .try_next()
            .ok_or_else(|| test_error("readiness timeout retry missing"))?;
        let mut ready = RecordingSink::new(None);
        check_eq!(
            drive_raw_write(&mut ready, request, &queue, 2).await?,
            RequestAction::Continue
        )?;
        check_eq!(control.written().await?, crate::ws::WriteOutcome::Written)?;
        let report = control.retry_report();
        check_eq!(report.attempts, 2)?;
        check_eq!(report.transport_sends, 1)?;
        check_eq!(report.stop_reason, Some(RetryStopReason::Completed))?;
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn raw_retry_slow_classifier_propagates_expired_total_deadline() -> TestResult {
        use crate::api::retry::{RetryDecision, RetrySchedule, RetrySetting, RetryStopReason};
        use crate::ws::{SendOptions, WsReplaySafety, WsSendRetryPolicy};
        let classified = Arc::new(AtomicUsize::new(0));
        let observed = classified.clone();
        let policy = WsSendRetryPolicy::builder()
            .schedule(RetrySchedule::builder().max_attempts(2).build()?)
            .safety(WsReplaySafety::IdempotentByContract)
            .classifier(move |context| {
                observed.fetch_add(1, Ordering::SeqCst);
                let remaining = context
                    .retry
                    .remaining
                    .map_or(Duration::ZERO, |remaining| remaining);
                std::thread::sleep(remaining.saturating_add(Duration::from_millis(2)));
                RetryDecision::Retry
            })
            .build()?;
        let request = crate::module::ws_client::v2_test_support::queued(
            45,
            SendOptions {
                deadline: Some(std::time::Instant::now() + Duration::from_millis(100)),
                retry_setting: RetrySetting::Policy(policy),
                ..Default::default()
            },
        )?;
        let control = request.dispatch_phase.clone();
        let queue = PriorityWriteQueue::new(1, 64)?;
        let mut sink = RecordingSink::new(None);
        sink.fail_on_flush = Some(1);
        drive_raw_write(&mut sink, request, &queue, 1).await?;
        check_eq!(classified.load(Ordering::SeqCst), 1)?;
        let error = control
            .written()
            .await
            .err()
            .ok_or_else(|| test_error("expired classifier returned success"))?;
        check_eq!(error.kind(), crate::error::ErrorKind::DeliveryUnknown)?;
        let cause = std::error::Error::source(&error)
            .and_then(|source| source.downcast_ref::<NetError>())
            .ok_or_else(|| test_error("delivery failure lost deadline cause"))?;
        check_eq!(cause.kind(), crate::error::ErrorKind::TimedOut)?;
        check_eq!(
            cause.ws_timeout_phase(),
            Some(crate::ws::WsTimeoutPhase::Total)
        )?;
        check_eq!(
            error.ws_timeout_phase(),
            Some(crate::ws::WsTimeoutPhase::Total)
        )?;
        let report = error
            .retry_report()
            .ok_or_else(|| test_error("deadline report missing"))?;
        check_eq!(report.stop_reason, Some(RetryStopReason::TimedOut))?;
        check_eq!(report.attempts, 1)?;
        check_eq!(report.transport_sends, 1)?;
        check!(queue.try_next().is_none())?;
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn raw_retry_observer_cancellation_stops_sends_and_scheduling() -> TestResult {
        use crate::api::retry::{RetryEvent, RetrySchedule, RetrySetting, RetryStopReason};
        use crate::ws::{
            CancellationGroup, SendOptions, WsReplaySafety, WsSendRetryOn, WsSendRetryPolicy,
        };
        for cancel_at_start in [true, false] {
            let cancellation = CancellationGroup::new();
            let observer_cancellation = cancellation.clone();
            let events = Arc::new(Mutex::new(Vec::new()));
            let observed = events.clone();
            let policy = WsSendRetryPolicy::builder()
                .schedule(RetrySchedule::builder().max_attempts(2).build()?)
                .retry_on(WsSendRetryOn::none().with_write_errors(true))
                .safety(WsReplaySafety::IdempotentByContract)
                .observer(move |event: &RetryEvent| {
                    if let Ok(mut events) = observed.lock() {
                        events.push(event.clone());
                    }
                    if (cancel_at_start && matches!(event, RetryEvent::AttemptStarted { .. }))
                        || (!cancel_at_start && matches!(event, RetryEvent::AttemptFinished { .. }))
                    {
                        observer_cancellation.cancel();
                    }
                })
                .build()?;
            let request = crate::module::ws_client::v2_test_support::queued(
                46,
                SendOptions {
                    cancellation: Some(cancellation),
                    retry_setting: RetrySetting::Policy(policy),
                    ..Default::default()
                },
            )?;
            let control = request.dispatch_phase.clone();
            control.bind_cancellation()?;
            let queue = PriorityWriteQueue::new(1, 64)?;
            let mut sink = RecordingSink::new(None);
            sink.fail_on_flush = Some(1);
            let recorded = sink.recorded();
            drive_raw_write(&mut sink, request, &queue, 1).await?;
            check!(control.written().await.is_err())?;
            let report = control.retry_report();
            check_eq!(report.stop_reason, Some(RetryStopReason::Cancelled))?;
            check_eq!(report.attempts, 1)?;
            check_eq!(report.transport_sends, u64::from(!cancel_at_start))?;
            check_eq!(
                recorded
                    .lock()
                    .map_err(|error| test_error(error.to_string()))?
                    .len(),
                usize::from(!cancel_at_start)
            )?;
            check!(queue.try_next().is_none())?;
            let events = events
                .lock()
                .map_err(|error| test_error(error.to_string()))?;
            check!(!events
                .iter()
                .any(|event| matches!(event, RetryEvent::RetryScheduled { .. })))?;
            check_eq!(
                events
                    .iter()
                    .filter(|event| matches!(event, RetryEvent::Completed(_)))
                    .count(),
                1
            )?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn raw_total_deadline_does_not_become_a_retryable_write_timeout() -> TestResult {
        use crate::api::retry::{RetryDecision, RetrySchedule, RetrySetting, RetryStopReason};
        use crate::ws::{
            SendOptions, WsReplaySafety, WsSendRetryOn, WsSendRetryPolicy, WsTimeoutPhase,
        };
        let policy = WsSendRetryPolicy::builder()
            .schedule(RetrySchedule::builder().max_attempts(2).build()?)
            .retry_on(
                WsSendRetryOn::none()
                    .with_write_timeout(true)
                    .with_errors([crate::error::ErrorKind::TimedOut]),
            )
            .safety(WsReplaySafety::IdempotentByContract)
            .classifier(|_| RetryDecision::Retry)
            .build()?;
        let request = crate::module::ws_client::v2_test_support::queued(
            47,
            SendOptions {
                write_timeout: Duration::from_secs(1),
                deadline: Some(std::time::Instant::now() + Duration::from_millis(20)),
                retry_setting: RetrySetting::Policy(policy),
                ..Default::default()
            },
        )?;
        let control = request.dispatch_phase.clone();
        let queue = PriorityWriteQueue::new(1, 64)?;
        let (started, started_rx) = oneshot::channel();
        let mut sink = FlushPendingSink::new(started);
        drive_raw_write(&mut sink, request, &queue, 1).await?;
        started_rx.await?;
        let error = control
            .written()
            .await
            .err()
            .ok_or_else(|| test_error("total deadline ignored"))?;
        check_eq!(error.kind(), crate::error::ErrorKind::DeliveryUnknown)?;
        check_eq!(error.ws_timeout_phase(), Some(WsTimeoutPhase::Total))?;
        let report = error
            .retry_report()
            .ok_or_else(|| test_error("total deadline report missing"))?;
        check_eq!(report.stop_reason, Some(RetryStopReason::TimedOut))?;
        check_eq!(report.attempts, 1)?;
        check_eq!(report.transport_sends, 1)?;
        check!(queue.try_next().is_none())?;
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn settled_control_timeout_is_not_reclassified_by_late_boundary_consumption() -> TestResult
    {
        let budget = TimeoutBudget::new(
            (Instant::now() + Duration::from_millis(50)).into_std(),
            crate::ws::WsTimeoutPhase::Attempt,
        );
        let mut sink = PendingSink::default();
        let action = handle_control_message_with_budget(
            &mut sink,
            ControlMessage::FlushAutomatic,
            Duration::from_millis(5),
            &CancellationToken::new(),
            Some(budget),
        )
        .await;
        let ControlAction::Failed { error, origin } = action else {
            return Err(test_error("control flush did not time out"));
        };
        check_eq!(error.kind(), crate::error::ErrorKind::DeliveryUnknown)?;
        check_eq!(error.ws_timeout_phase(), None)?;
        std::thread::sleep(
            budget
                .at
                .saturating_duration_since(std::time::Instant::now())
                .saturating_add(Duration::from_millis(2)),
        );
        let failure = classify_control_failure(error, origin, true, budget);
        check_eq!(
            failure.request_error.kind(),
            crate::error::ErrorKind::DeliveryUnknown
        )?;
        check_eq!(failure.request_error.ws_timeout_phase(), None)?;
        check_eq!(failure.requeue, RequestRequeue::IdempotentRetry)?;
        sink.verify()?;
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn settled_write_timeout_keeps_its_budget_when_writer_delivery_is_delayed() -> TestResult
    {
        use crate::api::retry::{RetrySchedule, RetrySetting, RetryStopReason};
        use crate::module::ws_client::v2_test_support as fixture;
        use crate::ws::{
            ResponseRouting, TaskEndCause, WebSocketClientConfig, WsReplaySafety,
            WsRequestOperationOptions, WsRequestRetryOn, WsRequestRetryPolicy, WsTimeoutPhase,
        };
        let runtime = fixture::runtime(
            WebSocketClientConfig::default(),
            ResponseRouting::Manual,
            true,
        )
        .await?;
        let policy = WsRequestRetryPolicy::builder()
            .schedule(RetrySchedule::builder().max_attempts(2).build()?)
            .retry_on(WsRequestRetryOn::none().with_write_timeout(true))
            .safety(WsReplaySafety::IdempotencyKey)
            .build()?;
        let receipt = fixture::requests(&runtime)
            .request_with_factory("delayed-writer", |context| {
                Ok(crate::ws::Message::text(context.request_id().as_str()))
            })?
            .options(WsRequestOperationOptions {
                write_timeout: Duration::from_millis(10),
                attempt_timeout: Some(Duration::from_millis(100)),
                total_timeout: Some(Duration::from_secs(2)),
                retry_setting: RetrySetting::Policy(policy),
                ..Default::default()
            })
            .enqueue()
            .await
            .map_err(|error| error.into_error())?;
        let request = fixture::bounded(runtime.queue.next())
            .await?
            .ok_or_else(|| test_error("first tracked write missing"))?;
        let core = request.dispatch_phase.clone();
        let absolute = core
            .absolute_budget()
            .ok_or_else(|| test_error("attempt budget missing"))?;
        check_eq!(absolute.phase, WsTimeoutPhase::Attempt)?;
        let registration = request
            .registration
            .as_ref()
            .ok_or_else(|| test_error("tracked registration missing"))?;
        let first_id = registration.request_id().clone();
        let connection = crate::ws::ConnectionId::from_allocated(1);
        check!(core.mark_writing(connection))?;
        check!(runtime.pending.bind_connection(registration, connection)?)?;
        let budget = TimeoutBudget::new(
            (Instant::now() + request.config.write_timeout).into_std(),
            WsTimeoutPhase::Write,
        );
        check!(budget.at < absolute.at)?;
        let (started, started_rx) = oneshot::channel();
        let mut sink = FlushPendingSink::new(started);
        let (_controls, mut control_rx) = mpsc::channel(1);
        let period = Duration::from_secs(3_600);
        let mut heartbeat = HeartbeatSchedule::from_interval(
            tokio::time::interval_at(Instant::now() + period, period),
            period,
        );
        let mut recovery = HeartbeatRecovery::new(HeartbeatRetryConfig::default());
        let failure = send_request_message_with_phase(
            &mut sink,
            request.message.clone(),
            None,
            MAX_READY_CONTROLS_PER_BOUNDARY,
            budget,
            &mut control_rx,
            Duration::from_secs(1),
            Duration::from_secs(1),
            &mut heartbeat,
            &HeartbeatState::new(1),
            &mut recovery,
            &CancellationToken::new(),
            &request.dispatch_cancel,
            &core,
        )
        .await
        .err()
        .ok_or_else(|| test_error("stalled writer unexpectedly succeeded"))?;
        started_rx.await?;
        check_eq!(
            failure.request_error.ws_timeout_phase(),
            Some(WsTimeoutPhase::Write)
        )?;
        // The transport timer has settled. Delay its writer-side delivery while
        // the later Attempt timer expires, without allowing the runner to poll.
        std::thread::sleep(
            absolute
                .at
                .saturating_duration_since(std::time::Instant::now())
                .saturating_add(Duration::from_millis(2)),
        );
        request.complete(Err(failure.request_error));
        let selected = core
            .selected_error()
            .ok_or_else(|| test_error("writer completion missing"))?;
        check_eq!(selected.ws_timeout_phase(), Some(WsTimeoutPhase::Write))?;
        let next = fixture::bounded(runtime.queue.next())
            .await?
            .ok_or_else(|| test_error("settled Write timeout was replaced by Attempt timeout"))?;
        let next_id = next
            .registration
            .as_ref()
            .ok_or_else(|| test_error("retry registration missing"))?
            .request_id();
        check!(*next_id != first_id)?;
        let (started, started_rx) = oneshot::channel();
        let mut sink = FlushPendingSink::new(started);
        drive_write_with_pending(&mut sink, next, &runtime.queue, 1, &runtime.pending).await?;
        started_rx.await?;
        let error = fixture::bounded(receipt.response())
            .await?
            .err()
            .ok_or_else(|| test_error("two stalled writes unexpectedly succeeded"))?;
        check_eq!(error.ws_timeout_phase(), Some(WsTimeoutPhase::Write))?;
        let report = error
            .retry_report()
            .ok_or_else(|| test_error("logical report missing"))?;
        check_eq!(report.stop_reason, Some(RetryStopReason::AttemptsExhausted))?;
        check_eq!(report.attempts, 2)?;
        check_eq!(report.transport_sends, 2)?;
        runtime.end(
            NetError::from(crate::error::ErrorKind::EngineDropped),
            TaskEndCause::Shutdown,
        );
        fixture::bounded(runtime.wait_requests()).await?;
        Ok(())
    }

    #[tokio::test]
    async fn tracked_write_timeout_retries_logical_request_with_fresh_wire_identity() -> TestResult
    {
        use crate::api::retry::{RetrySchedule, RetrySetting, RetryStopReason};
        use crate::module::ws_client::v2_test_support as fixture;
        use crate::ws::{
            DisconnectedPolicy, ResponseRouting, TaskEndCause, WebSocketClientConfig,
            WsReplaySafety, WsRequestOperationOptions, WsRequestRetryOn, WsRequestRetryPolicy,
            WsTimeoutPhase,
        };
        let mut config = WebSocketClientConfig::default();
        config.requests.max_pending = 1;
        let runtime = fixture::runtime(config, ResponseRouting::Manual, true).await?;
        let factory_calls = Arc::new(Mutex::new(Vec::new()));
        let observed = factory_calls.clone();
        let policy = WsRequestRetryPolicy::builder()
            .schedule(RetrySchedule::builder().max_attempts(2).build()?)
            .retry_on(WsRequestRetryOn::none().with_write_timeout(true))
            .safety(WsReplaySafety::IdempotencyKey)
            .build()?;
        let receipt = fixture::requests(&runtime)
            .request_with_factory("same-business-key", move |context| {
                observed.lock().map_err(NetError::from_poison)?.push((
                    context.attempt(),
                    context.request_id().clone(),
                    context.operation_id(),
                    context.idempotency_key().to_owned(),
                ));
                Ok(crate::ws::Message::text(format!(
                    "{}|{}",
                    context.request_id(),
                    context.idempotency_key()
                )))
            })?
            .options(WsRequestOperationOptions {
                write_timeout: Duration::from_millis(20),
                response_timeout: Duration::from_secs(1),
                total_timeout: Some(Duration::from_secs(2)),
                disconnected: DisconnectedPolicy::WaitForReconnect,
                retry_setting: RetrySetting::Policy(policy),
                ..Default::default()
            })
            .enqueue()
            .await
            .map_err(|error| error.into_error())?;
        let operation_id = receipt.handle().id();
        let mut wire_ids = Vec::new();
        for generation in 1..=2 {
            let request = fixture::bounded(runtime.queue.next())
                .await?
                .ok_or_else(|| test_error("logical write attempt was not queued"))?;
            let registration = request
                .registration
                .as_ref()
                .ok_or_else(|| test_error("logical attempt missing tracked registration"))?;
            wire_ids.push(registration.request_id().clone());
            let control = request.dispatch_phase.clone();
            runtime.activate_connection(crate::ws::ConnectionId::from_allocated(generation))?;
            let (started, started_rx) = oneshot::channel();
            let mut sink = FlushPendingSink::new(started);
            let recorded = sink.recorded();
            check!(matches!(
                drive_write_with_pending(
                    &mut sink,
                    request,
                    &runtime.queue,
                    generation,
                    &runtime.pending
                )
                .await?,
                RequestAction::StopWithError(_)
            ))?;
            fixture::bounded(started_rx).await??;
            check_eq!(
                recorded
                    .lock()
                    .map_err(|error| test_error(error.to_string()))?
                    .len(),
                1
            )?;
            let error = control
                .selected_error()
                .ok_or_else(|| test_error("write timeout not selected"))?;
            check_eq!(error.kind(), crate::error::ErrorKind::DeliveryUnknown)?;
            check_eq!(error.ws_timeout_phase(), Some(WsTimeoutPhase::Write))?;
            let timeout = std::error::Error::source(&error)
                .and_then(|source| source.downcast_ref::<NetError>())
                .ok_or_else(|| test_error("unknown delivery lost actual write timeout"))?;
            check_eq!(timeout.kind(), crate::error::ErrorKind::TimedOut)?;
            check_eq!(timeout.ws_timeout_phase(), Some(WsTimeoutPhase::Write))?;
            check_eq!(control.retry_report().transport_sends, 1)?;
        }
        let error = fixture::bounded(receipt.response())
            .await?
            .err()
            .ok_or_else(|| test_error("two timed out writes unexpectedly succeeded"))?;
        let report = error
            .retry_report()
            .ok_or_else(|| test_error("logical retry report missing"))?;
        check_eq!(report.stop_reason, Some(RetryStopReason::AttemptsExhausted))?;
        check_eq!(report.attempts, 2)?;
        check_eq!(report.retry_count, 1)?;
        check_eq!(report.transport_sends, 2)?;
        let calls = factory_calls
            .lock()
            .map_err(|error| test_error(error.to_string()))?;
        check_eq!(calls.len(), 2)?;
        check_eq!(wire_ids.len(), 2)?;
        check!(wire_ids.first() != wire_ids.get(1))?;
        for (index, (attempt, request_id, captured_operation, key)) in calls.iter().enumerate() {
            check_eq!(*attempt as usize, index + 1)?;
            check_eq!(wire_ids.get(index), Some(request_id))?;
            check_eq!(*captured_operation, operation_id)?;
            check_eq!(key.as_str(), "same-business-key")?;
        }
        drop(calls);
        check!(runtime.queue.try_next().is_none())?;
        check!(runtime.pending.snapshot()?.is_empty())?;
        runtime.end(
            NetError::from(crate::error::ErrorKind::EngineDropped),
            TaskEndCause::Shutdown,
        );
        fixture::bounded(runtime.wait_requests()).await?;
        Ok(())
    }

    #[tokio::test]
    async fn raw_retry_exhaustion_preserves_last_delivery_error_and_report() -> TestResult {
        use crate::api::retry::{RetrySchedule, RetrySetting, RetryStopReason};
        use crate::ws::{SendOptions, WsReplaySafety, WsSendRetryOn, WsSendRetryPolicy};
        let policy = WsSendRetryPolicy::builder()
            .schedule(RetrySchedule::builder().max_attempts(2).build()?)
            .retry_on(WsSendRetryOn::none().with_write_errors(true))
            .safety(WsReplaySafety::IdempotentByContract)
            .build()?;
        let request = crate::module::ws_client::v2_test_support::queued(
            43,
            SendOptions {
                retry_setting: RetrySetting::Policy(policy),
                ..Default::default()
            },
        )?;
        let control = request.dispatch_phase.clone();
        let queue = PriorityWriteQueue::new(1, 64)?;
        let mut first = RecordingSink::new(None);
        first.fail_on_flush = Some(1);
        drive_raw_write(&mut first, request, &queue, 1).await?;
        let request = queue
            .try_next()
            .ok_or_else(|| test_error("retry missing"))?;
        let mut second = RecordingSink::new(None);
        second.fail_on_flush = Some(1);
        drive_raw_write(&mut second, request, &queue, 2).await?;
        let error = control
            .written()
            .await
            .err()
            .ok_or_else(|| test_error("exhausted send succeeded"))?;
        check_eq!(error.kind(), crate::error::ErrorKind::DeliveryUnknown)?;
        let report = error
            .retry_report()
            .ok_or_else(|| test_error("error lost retry report"))?;
        check_eq!(report.attempts, 2)?;
        check_eq!(report.transport_sends, 2)?;
        check_eq!(report.stop_reason, Some(RetryStopReason::AttemptsExhausted))?;
        check!(queue.try_next().is_none())?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "ws_write/send_chain_component_tests.rs"]
mod send_chain_component_tests;

#[cfg(test)]
fn request_action_key(action: &RequestAction) -> (u8, Option<crate::error::ErrorKind>) {
    match action {
        RequestAction::Continue => (0, None),
        RequestAction::Stop => (1, None),
        RequestAction::StopWithError(error) => (2, Some(error.kind())),
    }
}

use crate::api::net_error::NetError;
use crate::api::wsc::wsc_response::WSCResponse;
use crate::common::log::log_def::LogType;
use crate::module::ws_client::callback_event::CallbackEvent;
use crate::module::ws_client::io_event::IoEvent;
use crate::module::ws_client::listener_store::ListenerStore;
use crate::module::ws_client::read::read_loop_context::ReadLoopContext;
use crate::module::ws_client::write::control_message::ControlMessage;
use futures::{Stream, StreamExt};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};

/// 向客户端 worker 报告指定连接世代的读循环已终止。
///
/// `error` 说明终止原因。若 worker 已停止并关闭事件通道，发送失败会被
/// 忽略，因为此时已没有可处理该终态的接收方。
/// 事件通道已满时，本函数会一直异步等待容量，没有额外的超时或取消分支。
async fn send_read_end(io_event_tx: &mpsc::Sender<IoEvent>, generation: u64, error: NetError) {
    crate::log_t!(LogType::WSC; "send_read_end", "generation|error", generation, format!("{error:?}"));
    if matches!(error, NetError::ConnectionClosed | NetError::Cancelled) {
        crate::log_s!(LogType::WSC; "send_read_end", "state|generation|error", "read_ended", generation, format!("{error:?}"));
    } else {
        crate::log_e!(LogType::WSC; "send_read_end", "generation|error", generation, format!("{error:?}"));
    }
    if io_event_tx
        .send(IoEvent::ReadEnded { generation, error })
        .await
        .is_err()
    {
        crate::log_s!(LogType::WSC; "send_read_end", "state|generation", "worker_event_receiver_closed", generation);
    }
}

/// 运行单个 WebSocket 连接的异步读循环。
///
/// 文本和二进制消息会被包装为响应事件并交给数据回调循环；读到 Ping/Close 后会
/// 要求写循环优先 flush Tungstenite 自动排好的协议回复；只有 payload 与当前主动
/// Ping 完全匹配的 Pong 才会确认心跳，Close reply flush 后连接结束。
/// Tungstenite 暴露的原始 `Frame` 在此不作业务处理。
///
/// `read` 必须是可在异步任务中持有的 WebSocket 消息流；`context` 提供本世代
/// 的路由通道、待处理请求视图、心跳状态和取消信号；`listeners` 用于在收到完整
/// 业务消息时捕获数据监听器快照。之后的监听器注销或替换只影响未来收到的消息。
///
/// # 终止行为
///
/// - 在等待下一条网络消息时观察到取消信号，会静默退出，不再上报读端错误。
/// - 消息流结束、收到 Close，或用于安排协议回复 flush 的控制通道关闭时，
///   上报连接已关闭。
/// - 底层读错误会转换为 [`NetError`] 后上报；回调通道关闭时上报任务中断。
/// - 数据或控制 lane 已满时立即上报对应 overflow，避免阻塞后续 Ping/Pong 读取。
///
/// 协议回复 flush 指令和业务回调使用 `try_send`；只有读终态事件可能短暂等待 worker
/// 事件通道容量。
pub(crate) async fn run_read_loop<S>(
    mut read: S,
    context: ReadLoopContext,
    listeners: Arc<ListenerStore>,
) where
    S: Stream<Item = Result<Message, WsError>> + Unpin + Send + 'static,
{
    crate::log_t!(LogType::WSC; "run_read_loop", "generation|max_callback_bytes", context.generation, context.data_callback_max_bytes);
    let ReadLoopContext {
        control_tx,
        data_callback_tx,
        data_callback_bytes,
        data_callback_max_bytes,
        pending_requests,
        io_event_tx,
        generation,
        cancel,
        heartbeat,
    } = context;
    loop {
        let next = tokio::select! {
            _ = cancel.cancelled() => {
                crate::log_s!(LogType::WSC; "run_read_loop", "state|generation", "read_cancelled", generation);
                return;
            },
            next = read.next() => next,
        };
        let Some(next) = next else {
            crate::log_s!(LogType::WSC; "run_read_loop", "state|generation", "stream_ended", generation);
            send_read_end(&io_event_tx, generation, NetError::ConnectionClosed).await;
            return;
        };
        let message = match next {
            Ok(message) => message,
            Err(error) => {
                crate::log_e!(LogType::WSC; "run_read_loop", "generation|error", generation, crate::common::log::summary::error(&error));
                send_read_end(&io_event_tx, generation, error.into()).await;
                return;
            }
        };

        crate::log_s!(LogType::WSC; "run_read_loop", "state|generation|message_type|message_bytes", "message_received", generation, match &message { Message::Text(_) => "text", Message::Binary(_) => "binary", Message::Ping(_) => "ping", Message::Pong(_) => "pong", Message::Close(_) => "close", Message::Frame(_) => "frame" }, message.len());
        match message {
            Message::Text(_) | Message::Binary(_) => {
                let payload_bytes = message.len().max(1);
                if payload_bytes > data_callback_max_bytes {
                    crate::log_e!(LogType::WSC; "run_read_loop", "stage|generation|payload_bytes|max_bytes", "callback_payload_too_large", generation, payload_bytes, data_callback_max_bytes);
                    send_read_end(&io_event_tx, generation, NetError::CallbackQueueOverflow).await;
                    return;
                }
                let byte_permit = match Arc::clone(&data_callback_bytes)
                    .try_acquire_many_owned(payload_bytes as u32)
                {
                    Ok(permit) => permit,
                    Err(_) => {
                        crate::log_e!(LogType::WSC; "run_read_loop", "stage|generation|payload_bytes", "callback_byte_budget_exhausted", generation, payload_bytes);
                        send_read_end(&io_event_tx, generation, NetError::CallbackQueueOverflow)
                            .await;
                        return;
                    }
                };
                let listener = listeners
                    .data
                    .read()
                    .map(|listener| listener.as_ref().map(Arc::clone))
                    .unwrap_or_else(|_| {
                        crate::log_e!(LogType::WSC; "run_read_loop", "error", "data_listener_lock_poisoned");
                        None
                    });
                let response = WSCResponse::new(message, pending_requests.clone(), generation);
                match data_callback_tx.try_send(CallbackEvent::Data {
                    listener,
                    response,
                    byte_permit,
                }) {
                    Ok(()) => {
                        crate::log_s!(LogType::WSC; "run_read_loop", "state|generation|message_bytes", "data_callback_queued", generation, payload_bytes);
                    }
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        crate::log_e!(LogType::WSC; "run_read_loop", "stage|generation", "data_callback_lane_full", generation);
                        send_read_end(&io_event_tx, generation, NetError::CallbackQueueOverflow)
                            .await;
                        return;
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        crate::log_e!(LogType::WSC; "run_read_loop", "stage|generation", "data_callback_lane_closed", generation);
                        send_read_end(&io_event_tx, generation, NetError::TaskInterruptionError)
                            .await;
                        return;
                    }
                }
            }
            Message::Ping(_) => match control_tx.try_send(ControlMessage::FlushAutomatic) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    crate::log_e!(LogType::WSC; "run_read_loop", "stage|generation", "automatic_pong_control_lane_full", generation);
                    send_read_end(&io_event_tx, generation, NetError::ControlQueueOverflow).await;
                    return;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    send_read_end(&io_event_tx, generation, NetError::ConnectionClosed).await;
                    return;
                }
            },
            Message::Pong(payload) => {
                let matched = heartbeat.acknowledge_pong(payload.as_ref());
                crate::log_s!(LogType::WSC; "run_read_loop", "state|generation|matched", "pong_received", generation, matched);
            }
            Message::Close(_) => {
                let (done_tx, done_rx) = tokio::sync::oneshot::channel();
                match control_tx.try_send(ControlMessage::PeerClose(done_tx)) {
                    Ok(()) => {
                        tokio::select! {
                                        _ = cancel.cancelled() => {
                            crate::log_s!(LogType::WSC; "run_read_loop", "state|generation", "read_cancelled", generation);
                            return;
                        },
                                        _ = done_rx => {}
                                    }
                    }
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        crate::log_e!(LogType::WSC; "run_read_loop", "stage|generation", "peer_close_control_lane_full", generation);
                        send_read_end(&io_event_tx, generation, NetError::ControlQueueOverflow)
                            .await;
                        return;
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        crate::log_s!(LogType::WSC; "run_read_loop", "state|generation", "peer_close_control_channel_closed", generation);
                    }
                }
                send_read_end(&io_event_tx, generation, NetError::ConnectionClosed).await;
                return;
            }
            Message::Frame(_) => {
                crate::log_s!(LogType::WSC; "run_read_loop", "state|generation", "raw_frame_ignored", generation);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::wsc::wsc_response::PendingRequestView;
    use crate::module::ws_client::heartbeat_state::{HeartbeatState, HeartbeatTick};
    use crate::module::ws_client::test_support::{
        check, check_eq, check_ne, test_error, TestResult,
    };
    use futures::stream;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Semaphore;
    use tokio::time::{Duration, Instant};
    use tokio_util::sync::CancellationToken;

    fn context(
        control_tx: mpsc::Sender<ControlMessage>,
        data_callback_tx: mpsc::Sender<CallbackEvent>,
        io_event_tx: mpsc::Sender<IoEvent>,
        heartbeat: Arc<HeartbeatState>,
    ) -> ReadLoopContext {
        ReadLoopContext {
            control_tx,
            data_callback_tx,
            data_callback_bytes: Arc::new(Semaphore::new(1024)),
            data_callback_max_bytes: 1024,
            pending_requests: Default::default(),
            io_event_tx,
            generation: 7,
            cancel: CancellationToken::new(),
            heartbeat,
        }
    }

    #[tokio::test]
    async fn data_event_captures_listener_registered_when_message_is_received() -> TestResult {
        let (control_tx, _control_rx) = mpsc::channel(1);
        let (data_callback_tx, mut data_callback_rx) = mpsc::channel(1);
        let (io_event_tx, _io_event_rx) = mpsc::channel(2);
        let listeners = Arc::new(ListenerStore::default());
        let original_calls = Arc::new(AtomicUsize::new(0));
        let original_calls_for_listener = Arc::clone(&original_calls);
        *listeners
            .data
            .write()
            .map_err(|error| test_error(format!("listener lock: {error:?}")))? =
            Some(Arc::new(move |_response| {
                original_calls_for_listener.fetch_add(1, Ordering::SeqCst);
            }));

        run_read_loop(
            stream::iter([Ok(Message::Text("response".into()))]),
            context(
                control_tx,
                data_callback_tx.clone(),
                io_event_tx.clone(),
                Arc::new(HeartbeatState::new(7)),
            ),
            Arc::clone(&listeners),
        )
        .await;

        let replacement_calls = Arc::new(AtomicUsize::new(0));
        let replacement_calls_for_listener = Arc::clone(&replacement_calls);
        *listeners
            .data
            .write()
            .map_err(|error| test_error(format!("listener lock: {error:?}")))? =
            Some(Arc::new(move |_response| {
                replacement_calls_for_listener.fetch_add(1, Ordering::SeqCst);
            }));
        let CallbackEvent::Data {
            listener: Some(listener),
            response,
            byte_permit: _byte_permit,
        } = data_callback_rx
            .recv()
            .await
            .ok_or_else(|| test_error("queued data callback"))?
        else {
            return Err(test_error(
                "expected data callback with a captured listener",
            ));
        };
        listener(response);

        let (later_control_tx, _later_control_rx) = mpsc::channel(1);
        run_read_loop(
            stream::iter([Ok(Message::Text("later response".into()))]),
            context(
                later_control_tx,
                data_callback_tx,
                io_event_tx,
                Arc::new(HeartbeatState::new(7)),
            ),
            listeners,
        )
        .await;
        let CallbackEvent::Data {
            listener: Some(listener),
            response,
            byte_permit: _byte_permit,
        } = data_callback_rx
            .recv()
            .await
            .ok_or_else(|| test_error("later data callback"))?
        else {
            return Err(test_error(
                "expected later callback with the replacement listener",
            ));
        };
        listener(response);

        check_eq!(original_calls.load(Ordering::SeqCst), 1)?;
        check_eq!(replacement_calls.load(Ordering::SeqCst), 1)?;
        Ok(())
    }

    #[tokio::test]
    async fn callback_overflow_fails_fast_instead_of_blocking_the_reader() -> TestResult {
        let (control_tx, _control_rx) = mpsc::channel(1);
        let (data_callback_tx, data_callback_rx) = mpsc::channel(1);
        let (io_event_tx, mut io_event_rx) = mpsc::channel(1);
        let context = context(
            control_tx,
            data_callback_tx.clone(),
            io_event_tx,
            Arc::new(HeartbeatState::new(7)),
        );
        let callback_bytes = Arc::clone(&context.data_callback_bytes);
        let pending = PendingRequestView::default();
        let byte_permit = Arc::clone(&callback_bytes)
            .try_acquire_owned()
            .map_err(|error| test_error(format!("callback byte permit: {error:?}")))?;
        check!(data_callback_tx
            .try_send(CallbackEvent::Data {
                listener: None,
                response: WSCResponse::new(Message::Binary(vec![1].into()), pending, 7),
                byte_permit,
            })
            .is_ok())?;

        tokio::time::timeout(
            Duration::from_secs(1),
            run_read_loop(
                stream::iter([Ok(Message::Text("overflow".into()))]),
                context,
                Arc::new(ListenerStore::default()),
            ),
        )
        .await
        .map_err(|error| {
            test_error(format!(
                "reader must not wait for callback capacity: {error:?}"
            ))
        })?;

        let event = io_event_rx.try_recv()?;
        check!(matches!(
            event,
            IoEvent::ReadEnded {
                generation: 7,
                error: NetError::CallbackQueueOverflow
            }
        ))?;
        check_eq!(callback_bytes.available_permits(), 1023)?;
        check_eq!(data_callback_rx.len(), 1)?;
        drop(data_callback_rx);
        check_eq!(callback_bytes.available_permits(), 1024)?;
        Ok(())
    }

    #[tokio::test]
    async fn callback_payload_byte_budget_fails_fast() -> TestResult {
        let (control_tx, _control_rx) = mpsc::channel(1);
        let (data_callback_tx, data_callback_rx) = mpsc::channel(2);
        let (io_event_tx, mut io_event_rx) = mpsc::channel(1);
        let mut context = context(
            control_tx,
            data_callback_tx,
            io_event_tx,
            Arc::new(HeartbeatState::new(7)),
        );
        context.data_callback_max_bytes = 4;
        context.data_callback_bytes = Arc::new(Semaphore::new(4));
        let callback_bytes = Arc::clone(&context.data_callback_bytes);

        tokio::time::timeout(
            Duration::from_secs(1),
            run_read_loop(
                stream::iter([Ok(Message::Binary(vec![0; 5].into()))]),
                context,
                Arc::new(ListenerStore::default()),
            ),
        )
        .await
        .map_err(|error| test_error(format!("oversized callback must fail fast: {error:?}")))?;

        check!(matches!(
            io_event_rx.try_recv()?,
            IoEvent::ReadEnded {
                generation: 7,
                error: NetError::CallbackQueueOverflow
            }
        ))?;
        check_eq!(data_callback_rx.len(), 0)?;
        check_eq!(callback_bytes.available_permits(), 4)?;
        Ok(())
    }

    #[tokio::test]
    async fn callback_accumulated_byte_budget_accepts_exact_limit_and_rejects_next_byte(
    ) -> TestResult {
        for extra_byte in [false, true] {
            let (control_tx, _control_rx) = mpsc::channel(1);
            let (data_callback_tx, data_callback_rx) = mpsc::channel(4);
            let (io_event_tx, mut io_event_rx) = mpsc::channel(1);
            let mut context = context(
                control_tx,
                data_callback_tx,
                io_event_tx,
                Arc::new(HeartbeatState::new(7)),
            );
            context.data_callback_max_bytes = 4;
            context.data_callback_bytes = Arc::new(Semaphore::new(4));
            let callback_bytes = Arc::clone(&context.data_callback_bytes);
            let mut messages = vec![
                Ok(Message::Text("ab".into())),
                Ok(Message::Binary(vec![3, 4].into())),
            ];
            if extra_byte {
                messages.push(Ok(Message::Binary(vec![5].into())));
            }

            tokio::time::timeout(
                Duration::from_secs(1),
                run_read_loop(
                    stream::iter(messages),
                    context,
                    Arc::new(ListenerStore::default()),
                ),
            )
            .await
            .map_err(|error| test_error(format!("byte budget reader must finish: {error:?}")))?;

            let expected_error = if extra_byte {
                NetError::CallbackQueueOverflow
            } else {
                NetError::ConnectionClosed
            };
            check!(matches!(
                io_event_rx.try_recv()?,
                IoEvent::ReadEnded { generation: 7, error } if error == expected_error
            ))?;
            check_eq!(data_callback_rx.len(), 2)?;
            check_eq!(callback_bytes.available_permits(), 0)?;
            drop(data_callback_rx);
            check_eq!(callback_bytes.available_permits(), 4)?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn callback_small_messages_hit_queue_limit_before_byte_budget() -> TestResult {
        let (control_tx, _control_rx) = mpsc::channel(1);
        let (data_callback_tx, data_callback_rx) = mpsc::channel(2);
        let (io_event_tx, mut io_event_rx) = mpsc::channel(1);
        let mut context = context(
            control_tx,
            data_callback_tx,
            io_event_tx,
            Arc::new(HeartbeatState::new(7)),
        );
        context.data_callback_max_bytes = 8;
        context.data_callback_bytes = Arc::new(Semaphore::new(8));
        let callback_bytes = Arc::clone(&context.data_callback_bytes);

        tokio::time::timeout(
            Duration::from_secs(1),
            run_read_loop(
                stream::iter([
                    Ok(Message::Text("a".into())),
                    Ok(Message::Binary(vec![2].into())),
                    Ok(Message::Text("c".into())),
                ]),
                context,
                Arc::new(ListenerStore::default()),
            ),
        )
        .await
        .map_err(|error| test_error(format!("small message overflow must fail fast: {error:?}")))?;

        check!(matches!(
            io_event_rx.try_recv()?,
            IoEvent::ReadEnded {
                generation: 7,
                error: NetError::CallbackQueueOverflow
            }
        ))?;
        check_eq!(data_callback_rx.len(), 2)?;
        check_eq!(callback_bytes.available_permits(), 6)?;
        drop(data_callback_rx);
        check_eq!(callback_bytes.available_permits(), 8)?;
        Ok(())
    }

    #[tokio::test]
    async fn callback_empty_messages_consume_one_byte_each() -> TestResult {
        let (control_tx, _control_rx) = mpsc::channel(1);
        let (data_callback_tx, data_callback_rx) = mpsc::channel(4);
        let (io_event_tx, mut io_event_rx) = mpsc::channel(1);
        let mut context = context(
            control_tx,
            data_callback_tx,
            io_event_tx,
            Arc::new(HeartbeatState::new(7)),
        );
        context.data_callback_max_bytes = 2;
        context.data_callback_bytes = Arc::new(Semaphore::new(2));
        let callback_bytes = Arc::clone(&context.data_callback_bytes);

        tokio::time::timeout(
            Duration::from_secs(1),
            run_read_loop(
                stream::iter([
                    Ok(Message::Text("".into())),
                    Ok(Message::Binary(Vec::new().into())),
                    Ok(Message::Text("".into())),
                ]),
                context,
                Arc::new(ListenerStore::default()),
            ),
        )
        .await
        .map_err(|error| test_error(format!("empty message overflow must fail fast: {error:?}")))?;

        check!(matches!(
            io_event_rx.try_recv()?,
            IoEvent::ReadEnded {
                generation: 7,
                error: NetError::CallbackQueueOverflow
            }
        ))?;
        check_eq!(data_callback_rx.len(), 2)?;
        check_eq!(callback_bytes.available_permits(), 0)?;
        drop(data_callback_rx);
        check_eq!(callback_bytes.available_permits(), 2)?;
        Ok(())
    }

    #[tokio::test]
    async fn closed_callback_channel_releases_rejected_payload_byte_permit() -> TestResult {
        let (control_tx, _control_rx) = mpsc::channel(1);
        let (data_callback_tx, data_callback_rx) = mpsc::channel(1);
        drop(data_callback_rx);
        let (io_event_tx, mut io_event_rx) = mpsc::channel(1);
        let mut context = context(
            control_tx,
            data_callback_tx,
            io_event_tx,
            Arc::new(HeartbeatState::new(7)),
        );
        context.data_callback_max_bytes = 4;
        context.data_callback_bytes = Arc::new(Semaphore::new(4));
        let callback_bytes = Arc::clone(&context.data_callback_bytes);

        tokio::time::timeout(
            Duration::from_secs(1),
            run_read_loop(
                stream::iter([Ok(Message::Binary(vec![1, 2, 3, 4].into()))]),
                context,
                Arc::new(ListenerStore::default()),
            ),
        )
        .await
        .map_err(|error| test_error(format!("closed callback lane must fail fast: {error:?}")))?;

        check!(matches!(
            io_event_rx.try_recv()?,
            IoEvent::ReadEnded {
                generation: 7,
                error: NetError::TaskInterruptionError
            }
        ))?;
        check_eq!(callback_bytes.available_permits(), 4)?;
        Ok(())
    }

    #[tokio::test]
    async fn control_overflow_fails_fast_instead_of_blocking_the_reader() -> TestResult {
        let (control_tx, _control_rx) = mpsc::channel(1);
        check!(control_tx.try_send(ControlMessage::FlushAutomatic).is_ok())?;
        let (data_callback_tx, _data_callback_rx) = mpsc::channel(1);
        let (io_event_tx, mut io_event_rx) = mpsc::channel(1);

        tokio::time::timeout(
            Duration::from_secs(1),
            run_read_loop(
                stream::iter([Ok(Message::Ping(vec![1].into()))]),
                context(
                    control_tx,
                    data_callback_tx,
                    io_event_tx,
                    Arc::new(HeartbeatState::new(7)),
                ),
                Arc::new(ListenerStore::default()),
            ),
        )
        .await
        .map_err(|error| {
            test_error(format!(
                "reader must not wait for control capacity: {error:?}"
            ))
        })?;

        let event = io_event_rx.try_recv()?;
        check!(matches!(
            event,
            IoEvent::ReadEnded {
                generation: 7,
                error: NetError::ControlQueueOverflow
            }
        ))?;
        Ok(())
    }

    #[tokio::test]
    async fn ping_requests_an_automatic_pong_flush() -> TestResult {
        let (control_tx, mut control_rx) = mpsc::channel(1);
        let (data_callback_tx, _data_callback_rx) = mpsc::channel(1);
        let (io_event_tx, _io_event_rx) = mpsc::channel(2);
        run_read_loop(
            stream::iter([Ok(Message::Ping(vec![1, 2, 3].into()))]),
            context(
                control_tx,
                data_callback_tx,
                io_event_tx,
                Arc::new(HeartbeatState::new(7)),
            ),
            Arc::new(ListenerStore::default()),
        )
        .await;
        check!(matches!(
            control_rx
                .recv()
                .await
                .ok_or_else(|| test_error("automatic Pong flush command"))?,
            ControlMessage::FlushAutomatic
        ))?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn only_a_matching_pong_acknowledges_the_outstanding_probe() -> TestResult {
        let heartbeat = Arc::new(HeartbeatState::new(7));
        let started_at = Instant::now();
        let HeartbeatTick::SendProbe(probe) = heartbeat.on_tick(started_at, Duration::from_secs(5))
        else {
            return Err(test_error("first heartbeat tick must create a probe"));
        };
        check!(heartbeat.mark_sent(probe.as_ref(), started_at))?;

        let (control_tx, _control_rx) = mpsc::channel(1);
        let (data_callback_tx, _data_callback_rx) = mpsc::channel(1);
        let (io_event_tx, _io_event_rx) = mpsc::channel(2);
        run_read_loop(
            stream::iter([
                Ok(Message::Pong(b"unrelated-pong".to_vec().into())),
                Ok(Message::Pong(probe.clone())),
            ]),
            context(
                control_tx,
                data_callback_tx,
                io_event_tx,
                Arc::clone(&heartbeat),
            ),
            Arc::new(ListenerStore::default()),
        )
        .await;

        let HeartbeatTick::SendProbe(next_probe) =
            heartbeat.on_tick(started_at + Duration::from_secs(1), Duration::from_secs(5))
        else {
            return Err(test_error("matching Pong must clear the outstanding probe"));
        };
        check_ne!(probe, next_probe)?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn an_unmatched_pong_does_not_prevent_timeout() -> TestResult {
        let heartbeat = Arc::new(HeartbeatState::new(7));
        let sent_at = Instant::now();
        let HeartbeatTick::SendProbe(probe) = heartbeat.on_tick(sent_at, Duration::from_secs(5))
        else {
            return Err(test_error("first heartbeat tick must create a probe"));
        };
        check!(heartbeat.mark_sent(probe.as_ref(), sent_at))?;

        let (control_tx, _control_rx) = mpsc::channel(1);
        let (data_callback_tx, _data_callback_rx) = mpsc::channel(1);
        let (io_event_tx, _io_event_rx) = mpsc::channel(2);
        run_read_loop(
            stream::iter([Ok(Message::Pong(b"unrelated-pong".to_vec().into()))]),
            context(
                control_tx,
                data_callback_tx,
                io_event_tx,
                Arc::clone(&heartbeat),
            ),
            Arc::new(ListenerStore::default()),
        )
        .await;

        check_eq!(
            heartbeat.on_tick(sent_at + Duration::from_secs(5), Duration::from_secs(5),),
            HeartbeatTick::TimedOut
        )?;
        Ok(())
    }
}

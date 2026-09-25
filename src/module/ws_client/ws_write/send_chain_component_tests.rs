//! Real WebSocket/SplitSink contracts over bounded programmable asynchronous I/O.
//! These are component measurements, not TCP syscall, allocator, or lock counts.

use super::*;
use crate::module::ws_client::network_io::{NetworkAwareSink, NetworkAwareStream};
use crate::module::ws_client::read::read_loop_context::ReadLoopContext;
use crate::module::ws_client::test_support::{check, check_eq, test_error, TestResult};
use crate::module::ws_client::ws_read::run_read_loop;
use futures::StreamExt;
use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::task::{Context, Poll, Wake, Waker};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_tungstenite::tungstenite::protocol::{Role, WebSocketConfig};
use tokio_tungstenite::WebSocketStream;

const LARGE: usize = 24 * 1024 * 1024 + 17;
const LIMIT: Duration = Duration::from_secs(30);
const PROBE: &[u8] = b"component-ping";

#[derive(Default)]
struct WakeCounter(AtomicUsize);

impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

fn poll_once<F: Future>(future: Pin<&mut F>, wakes: &Arc<WakeCounter>) -> Poll<F::Output> {
    let waker = Waker::from(wakes.clone());
    future.poll(&mut Context::from_waker(&waker))
}

#[derive(Clone, Copy, Default, Debug, PartialEq, Eq, serde::Serialize)]
struct IoCounts {
    accepted_bytes: usize,
    read_polls: usize,
    read_bytes: usize,
    write_polls: usize,
    write_calls: usize,
    write_pending: usize,
    flush_polls: usize,
    flush_pending: usize,
    shutdown_polls: usize,
}

struct IoState {
    wire: Vec<u8>,
    limit: usize,
    incoming: VecDeque<u8>,
    max_write: usize,
    pending_after_write: bool,
    pending_next: bool,
    gate_at: Option<usize>,
    gate_open: bool,
    flush_open: bool,
    read_waker: Option<Waker>,
    write_waker: Option<Waker>,
    counts: IoCounts,
}

#[derive(Clone)]
struct ScriptedIo(Arc<Mutex<IoState>>);

impl ScriptedIo {
    fn new(limit: usize, max_write: usize, gate_at: Option<usize>) -> Self {
        Self(Arc::new(Mutex::new(IoState {
            wire: Vec::new(),
            limit,
            incoming: VecDeque::new(),
            max_write,
            pending_after_write: false,
            pending_next: false,
            gate_at,
            gate_open: gate_at.is_none(),
            flush_open: true,
            read_waker: None,
            write_waker: None,
            counts: IoCounts::default(),
        })))
    }

    fn lock(&self) -> io::Result<MutexGuard<'_, IoState>> {
        self.0
            .lock()
            .map_err(|_| io::Error::other("component I/O state poisoned"))
    }

    fn counts(&self) -> TestResult<IoCounts> {
        Ok(self.lock()?.counts)
    }

    fn incoming_control(&self, opcode: u8, body: &[u8]) -> TestResult {
        check!(body.len() <= 125, "control payload exceeds RFC limit")?;
        let wake = {
            let mut state = self.lock()?;
            check!(
                state.incoming.len() + body.len() + 2 <= 256,
                "input script is bounded"
            )?;
            state
                .incoming
                .extend([0x80 | opcode, u8::try_from(body.len())?]);
            state.incoming.extend(body.iter().copied());
            state.read_waker.take()
        };
        if let Some(waker) = wake {
            waker.wake();
        }
        Ok(())
    }

    fn release(&self) -> TestResult {
        let wake = {
            let mut state = self.lock()?;
            state.gate_open = true;
            state.flush_open = true;
            state.write_waker.take()
        };
        if let Some(waker) = wake {
            waker.wake();
        }
        Ok(())
    }
}

impl AsyncRead for ScriptedIo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut state = self.lock()?;
        state.counts.read_polls += 1;
        if state.incoming.is_empty() {
            state.read_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        while output.remaining() > 0 {
            let Some(byte) = state.incoming.pop_front() else {
                break;
            };
            output.put_slice(&[byte]);
            state.counts.read_bytes += 1;
        }
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for ScriptedIo {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        input: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut state = self.lock()?;
        state.counts.write_polls += 1;
        let gate_remaining = if state.gate_open {
            usize::MAX
        } else {
            state
                .gate_at
                .map_or(usize::MAX, |at| at.saturating_sub(state.wire.len()))
        };
        if gate_remaining == 0 {
            state.counts.write_pending += 1;
            state.write_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        if state.pending_next {
            state.pending_next = false;
            state.counts.write_pending += 1;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        let accepted = input.len().min(state.max_write).min(gate_remaining);
        if state.wire.len().saturating_add(accepted) > state.limit {
            return Poll::Ready(Err(io::Error::other(
                "component wire buffer bound exceeded",
            )));
        }
        state.wire.try_reserve(accepted).map_err(io::Error::other)?;
        state.wire.extend_from_slice(&input[..accepted]);
        state.counts.accepted_bytes += accepted;
        state.counts.write_calls += 1;
        state.pending_next = state.pending_after_write;
        Poll::Ready(Ok(accepted))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut state = self.lock()?;
        state.counts.flush_polls += 1;
        if !state.flush_open
            || (!state.gate_open && state.gate_at.is_some_and(|at| state.wire.len() >= at))
        {
            state.counts.flush_pending += 1;
            state.write_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.lock()?.counts.shutdown_polls += 1;
        self.poll_flush(cx)
    }
}

type ClientWrite =
    NetworkAwareSink<futures::stream::SplitSink<WebSocketStream<ScriptedIo>, Message>>;
type ClientRead = NetworkAwareStream<futures::stream::SplitStream<WebSocketStream<ScriptedIo>>>;

async fn socket(io: &ScriptedIo, retired: &CancellationToken) -> (ClientWrite, ClientRead) {
    let config = WebSocketConfig::default().max_write_buffer_size(LARGE + 1024);
    let socket = WebSocketStream::from_raw_socket(io.clone(), Role::Client, Some(config)).await;
    let (write, read) = socket.split();
    (
        NetworkAwareSink::new(write, None, 0).with_write_retirement(retired),
        NetworkAwareStream::new(read, None, 0).with_write_retirement(retired),
    )
}

async fn reader(
    read: ClientRead,
    controls: mpsc::Sender<ControlMessage>,
    heartbeat: Arc<HeartbeatState>,
    cancel: CancellationToken,
) -> TestResult {
    let runtime = crate::module::ws_client::v2_test_support::runtime(
        crate::ws::WebSocketClientConfig::default(),
        crate::ws::ResponseRouting::Disabled,
        true,
    )
    .await?;
    let (event_tx, mut event_rx) = mpsc::channel(2);
    run_read_loop(
        read,
        ReadLoopContext {
            runtime,
            peer_close: Arc::new(Default::default()),
            control_tx: controls,
            io_event_tx: event_tx,
            generation: 901,
            cancel,
            heartbeat,
        },
    )
    .await;
    check!(
        event_rx.try_recv().is_err(),
        "reader reported an I/O end before cancellation"
    )?;
    Ok(())
}

fn idle_heartbeat() -> Interval {
    let period = Duration::from_secs(3600);
    let mut heartbeat = tokio::time::interval_at(Instant::now() + period, period);
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
    heartbeat
}

fn body(size: usize) -> TestResult<Bytes> {
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(size)?;
    bytes.extend((0..size).map(|offset| (offset % 251) as u8));
    Ok(Bytes::from(bytes))
}

fn header_size(length: usize) -> usize {
    match length {
        0..=125 => 6,
        126..=65535 => 8,
        _ => 14,
    }
}

#[derive(Debug)]
struct WireFrame<'a> {
    offset: usize,
    fin: bool,
    opcode: u8,
    mask: [u8; 4],
    body: &'a [u8],
}

/// Independent RFC wire parser: does not call Tungstenite decode or the production iterator.
fn wire_frames(wire: &[u8]) -> TestResult<Vec<WireFrame<'_>>> {
    let mut result = Vec::new();
    let mut offset = 0usize;
    while offset < wire.len() {
        let header = wire
            .get(offset..offset + 2)
            .ok_or_else(|| test_error("truncated frame header"))?;
        check_eq!(header[0] & 0x70, 0, "RSV bits must be clear")?;
        check!(header[1] & 0x80 != 0, "client output must be masked")?;
        let mut at = offset + 2;
        let marker = header[1] & 0x7f;
        let length = match marker {
            126 | 127 => {
                let width = if marker == 126 { 2 } else { 8 };
                let encoded = wire
                    .get(at..at + width)
                    .ok_or_else(|| test_error("truncated extended length"))?;
                let mut length = 0u64;
                for byte in encoded {
                    length = (length << 8) | u64::from(*byte);
                }
                check!(
                    marker != 127 || (65536..(1u64 << 63)).contains(&length),
                    "invalid 64-bit length"
                )?;
                check!(marker != 126 || length >= 126, "nonminimal 16-bit length")?;
                at += width;
                usize::try_from(length)?
            }
            value => usize::from(value),
        };
        let mask: [u8; 4] = wire
            .get(at..at + 4)
            .ok_or_else(|| test_error("truncated mask"))?
            .try_into()?;
        at += 4;
        let end = at
            .checked_add(length)
            .ok_or_else(|| test_error("frame length overflow"))?;
        let payload = wire
            .get(at..end)
            .ok_or_else(|| test_error("truncated payload"))?;
        let opcode = header[0] & 15;
        let fin = header[0] & 0x80 != 0;
        if opcode >= 8 {
            check!(fin && length <= 125, "invalid control frame")?;
        }
        result.push(WireFrame {
            offset,
            fin,
            opcode,
            mask,
            body: payload,
        });
        offset = end;
    }
    Ok(result)
}

fn decoded(frame: &WireFrame<'_>) -> Vec<u8> {
    frame
        .body
        .iter()
        .enumerate()
        .map(|(i, byte)| byte ^ frame.mask[i % 4])
        .collect()
}

fn validate_upload(
    wire: &[u8],
    payload: &[u8],
    frame_size: Option<usize>,
    pong: Option<&[u8]>,
) -> TestResult {
    let frames = wire_frames(wire)?;
    let maximum = frame_size.map_or(payload.len().max(1), |size| size);
    let expected_frames = payload.len().max(1).div_ceil(maximum);
    let mut data_frames = 0;
    let mut offset = 0;
    let mut pongs = 0;
    for frame in &frames {
        match frame.opcode {
            0 | 2 => {
                check_eq!(frame.opcode, if data_frames == 0 { 2 } else { 0 })?;
                let count = (payload.len() - offset).min(maximum);
                check_eq!(frame.body.len(), count)?;
                check_eq!(frame.fin, data_frames + 1 == expected_frames)?;
                for (index, byte) in frame.body.iter().enumerate() {
                    check_eq!(
                        byte ^ frame.mask[index % 4],
                        payload[offset + index],
                        "payload offset {}",
                        offset + index
                    )?;
                }
                offset += count;
                data_frames += 1;
            }
            10 => {
                let expected = pong.ok_or_else(|| test_error("unexpected Pong"))?;
                check_eq!(decoded(frame), expected)?;
                pongs += 1;
            }
            opcode => return Err(test_error(format!("unexpected opcode {opcode}"))),
        }
    }
    check_eq!(offset, payload.len())?;
    check_eq!(data_frames, expected_frames)?;
    check_eq!(pongs, usize::from(pong.is_some()))?;
    Ok(())
}

async fn send<W: Sink<Message, Error = WsError> + Unpin>(
    write: &mut W,
    payload: Bytes,
    frame: Option<usize>,
    controls: &mut mpsc::Receiver<ControlMessage>,
    state: &HeartbeatState,
    phase: &Arc<OperationControl>,
    cancel: &CancellationToken,
) -> Result<(), RequestWriteFailure> {
    send_request_message_with_phase(
        write,
        Message::Binary(payload),
        frame,
        MAX_READY_CONTROLS_PER_BOUNDARY,
        Instant::now() + LIMIT,
        controls,
        LIMIT,
        LIMIT,
        &mut HeartbeatSchedule::from_interval(idle_heartbeat(), LIMIT),
        state,
        &CancellationToken::new(),
        cancel,
        phase,
    )
    .await
}

#[tokio::test(start_paused = true)]
async fn half_frame_ping_is_flushed_by_real_reader_after_current_frame() -> TestResult {
    let payload = body(LARGE)?;
    let alias = payload.clone();
    for frame_size in [Some(4096), Some(32768), Some(1024 * 1024), None] {
        let first_size = frame_size.map_or(LARGE, |size| size);
        let head = header_size(first_size);
        for (name, body_offset) in [
            ("first-byte", 1),
            ("middle", first_size / 2),
            ("tail", first_size - 1),
            ("boundary", first_size),
        ] {
            let gate = head + body_offset;
            let io = ScriptedIo::new(LARGE + 100_000, 65533, Some(gate));
            let retired = CancellationToken::new();
            let (mut write, read) = socket(&io, &retired).await;
            let (control_tx, mut control_rx) = mpsc::channel(32);
            let state = Arc::new(HeartbeatState::new(901));
            let read_cancel = CancellationToken::new();
            let mut reading = Box::pin(reader(
                read,
                control_tx.clone(),
                state.clone(),
                read_cancel.clone(),
            ));
            let wakes = Arc::new(WakeCounter::default());
            check!(poll_once(reading.as_mut(), &wakes).is_pending())?;
            let phase = crate::module::ws_client::v2_test_support::operation(
                &crate::ws::SendOptions::default(),
                false,
                1,
            )?;
            phase.enqueue()?;
            check!(phase.mark_writing(crate::ws::ConnectionId::from_allocated(901)))?;
            phase.bind_write_retirement(retired.clone());
            let dispatch_cancel = CancellationToken::new();
            let mut sending = Box::pin(send(
                &mut write,
                payload.clone(),
                frame_size,
                &mut control_rx,
                &state,
                &phase,
                &dispatch_cancel,
            ));
            check!(poll_once(sending.as_mut(), &wakes).is_pending())?;
            check_eq!(io.counts()?.accepted_bytes, gate)?;
            let before_inject = wakes.0.load(Ordering::Relaxed);
            io.incoming_control(9, PROBE)?;
            check!(
                wakes.0.load(Ordering::Relaxed) > before_inject,
                "input must wake reader"
            )?;
            check!(poll_once(reading.as_mut(), &wakes).is_pending())?;
            check_eq!(io.counts()?.read_bytes, PROBE.len() + 2)?;
            check_eq!(
                control_tx.capacity(),
                31,
                "real read loop must enqueue automatic flush"
            )?;
            check_eq!(
                io.counts()?.accepted_bytes,
                gate,
                "Ping cannot interrupt current frame"
            )?;
            let before_release = wakes.0.load(Ordering::Relaxed);
            io.release()?;
            check!(
                wakes.0.load(Ordering::Relaxed) > before_release,
                "write readiness must wake registered task"
            )?;
            // Deliberately do not poll the writer. Only the real reader drives automatic Pong.
            check!(poll_once(reading.as_mut(), &wakes).is_pending())?;
            {
                let state = io.lock()?;
                let frames = wire_frames(&state.wire)?;
                check_eq!(
                    frames.len(),
                    2,
                    "reader must flush current data then one Pong"
                )?;
                check_eq!(frames[0].body.len(), first_size)?;
                check_eq!(frames[1].opcode, 10)?;
                check_eq!(frames[1].offset, head + first_size)?;
                check_eq!(decoded(&frames[1]), PROBE)?;
            }
            check_eq!(tokio::time::timeout(LIMIT, sending).await?, Ok(()))?;
            validate_upload(&io.lock()?.wire, &payload, frame_size, Some(PROBE))?;
            check!(
                alias
                    .iter()
                    .enumerate()
                    .all(|(offset, byte)| *byte == (offset % 251) as u8),
                "shared source bytes changed during masking"
            )?;
            println!(
                "SEND_CHAIN_RESULT {}",
                serde_json::json!({"case":"half-frame-ping","frame_size":frame_size,"position":name,"payload_bytes":LARGE,"counts":io.counts()?,"scope":"real WebSocketStream + SplitSink + writer + reader; no TCP or peer","passed":true})
            );
            read_cancel.cancel();
            tokio::time::timeout(LIMIT, reading).await??;
        }
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn partial_async_writes_and_final_flush_pending_do_not_complete_early() -> TestResult {
    for max_write in [1, 3, 257, 65533] {
        let payload = body(65537)?;
        let io = ScriptedIo::new(66000, max_write, None);
        {
            let mut state = io.lock()?;
            state.pending_after_write = true;
            state.flush_open = false;
        }
        let (write, _) = socket(&io, &CancellationToken::new()).await;
        let sink_counts = Arc::new(SinkCounts::default());
        let mut write = CountedSink {
            inner: write,
            counts: sink_counts.clone(),
        };
        let (_tx, mut rx) = mpsc::channel(1);
        let state = HeartbeatState::new(902);
        let phase = crate::module::ws_client::v2_test_support::operation(
            &crate::ws::SendOptions::default(),
            false,
            1,
        )?;
        phase.enqueue()?;
        check!(phase.mark_writing(crate::ws::ConnectionId::from_allocated(901)))?;
        let cancel = CancellationToken::new();
        let mut sending = Box::pin(send(
            &mut write,
            payload.clone(),
            None,
            &mut rx,
            &state,
            &phase,
            &cancel,
        ));
        let wakes = Arc::new(WakeCounter::default());
        let mut polls = 0;
        while io.counts()?.flush_pending == 0 {
            check!(
                polls <= 2 * 66000,
                "bounded script did not reach final flush"
            )?;
            check!(
                poll_once(sending.as_mut(), &wakes).is_pending(),
                "send completed before underlying flush"
            )?;
            polls += 1;
        }
        validate_upload(&io.lock()?.wire, &payload, None, None)?;
        let before = io.counts()?;
        check_eq!(
            sink_counts.starts.load(Ordering::Relaxed),
            1,
            "Pending must not resubmit an accepted frame"
        )?;
        check!(before.write_pending > 0)?;
        check!(wakes.0.load(Ordering::Relaxed) > 0)?;
        check!(poll_once(sending.as_mut(), &wakes).is_pending())?;
        check_eq!(
            io.counts()?.write_calls,
            before.write_calls,
            "flush retry must not resubmit an accepted frame"
        )?;
        let before_release = wakes.0.load(Ordering::Relaxed);
        io.release()?;
        check!(wakes.0.load(Ordering::Relaxed) > before_release)?;
        check_eq!(tokio::time::timeout(LIMIT, sending).await?, Ok(()))?;
        check_eq!(io.counts()?.accepted_bytes, before.accepted_bytes)?;
        check_eq!(sink_counts.starts.load(Ordering::Relaxed), 1)?;
        println!(
            "SEND_CHAIN_RESULT {}",
            serde_json::json!({"case":"partial-write-final-flush","max_write":max_write,"payload_bytes":payload.len(),"counts":io.counts()?,"passed":true})
        );
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn retirement_blocks_real_reader_from_flushing_partial_data_and_pong() -> TestResult {
    for frame_size in [Some(4096), Some(32768), Some(1024 * 1024), None] {
        let payload = body(LARGE)?;
        let first_size = frame_size.map_or(LARGE, |size| size);
        let gate = header_size(first_size) + first_size / 2;
        let io = ScriptedIo::new(LARGE + 100_000, 65533, Some(gate));
        let retired = CancellationToken::new();
        let (mut write, read) = socket(&io, &retired).await;
        let (tx, mut rx) = mpsc::channel(32);
        let heartbeat = Arc::new(HeartbeatState::new(903));
        let read_cancel = CancellationToken::new();
        let mut reading = Box::pin(reader(read, tx, heartbeat.clone(), read_cancel.clone()));
        let phase = crate::module::ws_client::v2_test_support::operation(
            &crate::ws::SendOptions::default(),
            false,
            1,
        )?;
        phase.enqueue()?;
        check!(phase.mark_writing(crate::ws::ConnectionId::from_allocated(901)))?;
        phase.bind_write_retirement(retired.clone());
        let dispatch_cancel = CancellationToken::new();
        let mut sending = Box::pin(send(
            &mut write,
            payload,
            frame_size,
            &mut rx,
            &heartbeat,
            &phase,
            &dispatch_cancel,
        ));
        let wakes = Arc::new(WakeCounter::default());
        check!(poll_once(sending.as_mut(), &wakes).is_pending())?;
        io.incoming_control(9, PROBE)?;
        check!(poll_once(reading.as_mut(), &wakes).is_pending())?;
        check_eq!(io.counts()?.accepted_bytes, gate)?;
        let _ = phase.cancel();
        check!(
            retired.is_cancelled(),
            "dispatch cancellation must retire the shared transport"
        )?;
        let frozen = io.counts()?;
        io.release()?;
        check!(poll_once(reading.as_mut(), &wakes).is_pending())?;
        check_eq!(
            io.counts()?,
            frozen,
            "retired reader must not enter the underlying socket"
        )?;
        dispatch_cancel.cancel();
        let result = tokio::time::timeout(LIMIT, sending).await?;
        let failure = result
            .err()
            .ok_or_else(|| test_error("partial cancellation incorrectly succeeded"))?;
        check_eq!(
            failure.request_error,
            NetError::from(crate::error::ErrorKind::Cancelled)
        )?;
        check_eq!(
            failure.connection_action,
            RequestAction::StopWithError(NetError::from(crate::error::ErrorKind::DeliveryUnknown))
        )?;
        check_eq!(
            phase.snapshot()?.delivery,
            crate::ws::DeliveryEvidence::Unknown
        )?;
        check_eq!(
            phase.selected_error().map(|e| e.kind()),
            Some(crate::error::ErrorKind::DeliveryUnknown)
        )?;
        check_eq!(failure.requeue, RequestRequeue::Never)?;
        check_eq!(
            io.counts()?,
            frozen,
            "retired writer must not resume the partial frame"
        )?;
        let automatic = handle_control_message(
            &mut write,
            ControlMessage::FlushAutomatic,
            LIMIT,
            &CancellationToken::new(),
        )
        .await;
        check!(matches!(automatic, ControlAction::Failed { .. }))?;
        check_eq!(io.counts()?, frozen)?;
        println!(
            "SEND_CHAIN_RESULT {}",
            serde_json::json!({"case":"retired-reader","frame_size":frame_size,"counts":frozen,"passed":true})
        );
        read_cancel.cancel();
        tokio::time::timeout(LIMIT, reading).await??;
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn real_reader_matching_pong_before_ping_flush_does_not_resurrect_probe() -> TestResult {
    let io = ScriptedIo::new(256, 3, None);
    io.lock()?.flush_open = false;
    let (mut write, read) = socket(&io, &CancellationToken::new()).await;
    let (tx, _rx) = mpsc::channel(4);
    let heartbeat = Arc::new(HeartbeatState::new(904));
    let cancel = CancellationToken::new();
    let mut reading = Box::pin(reader(read, tx, heartbeat.clone(), cancel.clone()));
    let write_cancel = CancellationToken::new();
    let mut ping = Box::pin(handle_heartbeat_tick(
        &mut write,
        &heartbeat,
        Duration::from_secs(5),
        LIMIT,
        &write_cancel,
    ));
    let wakes = Arc::new(WakeCounter::default());
    check!(poll_once(ping.as_mut(), &wakes).is_pending())?;
    check_eq!(heartbeat.pong_deadline(), None)?;
    let payload = {
        let state = io.lock()?;
        let frames = wire_frames(&state.wire)?;
        check_eq!(frames.len(), 1)?;
        check_eq!(frames[0].opcode, 9)?;
        decoded(&frames[0])
    };
    io.incoming_control(10, &payload)?;
    check!(poll_once(reading.as_mut(), &wakes).is_pending())?;
    io.release()?;
    check_eq!(tokio::time::timeout(LIMIT, ping).await?, Ok(()))?;
    check_eq!(
        heartbeat.pong_deadline(),
        None,
        "completed Ping must not restore an already matched probe"
    )?;
    check!(matches!(
        heartbeat.on_tick(Instant::now(), LIMIT),
        HeartbeatTick::SendProbe(_)
    ))?;
    cancel.cancel();
    tokio::time::timeout(LIMIT, reading).await??;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn request_and_frame_deadlines_keep_absolute_order_during_pending_flush() -> TestResult {
    for (request_ms, frame_ms) in [(7, 11), (11, 7), (7, 7)] {
        let io = ScriptedIo::new(1024, 3, None);
        io.lock()?.flush_open = false;
        let (mut write, _) = socket(&io, &CancellationToken::new()).await;
        let (_tx, mut rx) = mpsc::channel(1);
        let heartbeat = HeartbeatState::new(905);
        let mut interval = HeartbeatSchedule::from_interval(idle_heartbeat(), LIMIT);
        let connection_cancel = CancellationToken::new();
        let dispatch_cancel = CancellationToken::new();
        let phase = crate::module::ws_client::v2_test_support::operation(
            &crate::ws::SendOptions::default(),
            false,
            1,
        )?;
        phase.enqueue()?;
        check!(phase.mark_writing(crate::ws::ConnectionId::from_allocated(901)))?;
        let mut sending = Box::pin(send_request_message_with_phase(
            &mut write,
            Message::Binary(body(513)?),
            None,
            MAX_READY_CONTROLS_PER_BOUNDARY,
            Instant::now() + Duration::from_millis(request_ms),
            &mut rx,
            LIMIT,
            Duration::from_millis(frame_ms),
            &mut interval,
            &heartbeat,
            &connection_cancel,
            &dispatch_cancel,
            &phase,
        ));
        let wakes = Arc::new(WakeCounter::default());
        check!(poll_once(sending.as_mut(), &wakes).is_pending())?;
        let before = io.counts()?;
        let limit = request_ms.min(frame_ms);
        tokio::time::advance(Duration::from_millis(limit - 1)).await;
        check!(poll_once(sending.as_mut(), &wakes).is_pending())?;
        check_eq!(io.counts()?.accepted_bytes, before.accepted_bytes)?;
        tokio::time::advance(Duration::from_millis(1)).await;
        let result = match poll_once(sending.as_mut(), &wakes) {
            Poll::Ready(result) => result,
            Poll::Pending => {
                return Err(test_error(
                    "absolute deadline was postponed by a flush poll",
                ))
            }
        };
        let failure = result
            .err()
            .ok_or_else(|| test_error("deadline incorrectly committed success"))?;
        check_eq!(
            failure.request_error.kind(),
            if request_ms <= frame_ms {
                crate::error::ErrorKind::TimedOut
            } else {
                crate::error::ErrorKind::DeliveryUnknown
            }
        )?;
        check_eq!(
            phase.snapshot()?.delivery,
            crate::ws::DeliveryEvidence::Unknown
        )?;
        check_eq!(
            failure.connection_action,
            RequestAction::StopWithError(NetError::from(crate::error::ErrorKind::DeliveryUnknown))
        )?;
        check_eq!(
            failure.requeue,
            if request_ms <= frame_ms {
                RequestRequeue::Never
            } else {
                RequestRequeue::IdempotentRetry
            }
        )?;
        check_eq!(io.counts()?.accepted_bytes, before.accepted_bytes)?;
    }
    Ok(())
}

#[derive(Default)]
struct SinkCounts {
    ready_polls: AtomicUsize,
    starts: AtomicUsize,
    flush_polls: AtomicUsize,
    close_polls: AtomicUsize,
}

struct CountedSink<W> {
    inner: W,
    counts: Arc<SinkCounts>,
}

impl<W: Sink<Message, Error = WsError> + Unpin> Sink<Message> for CountedSink<W> {
    type Error = WsError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        this.counts.ready_polls.fetch_add(1, Ordering::Relaxed);
        Pin::new(&mut this.inner).poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, frame: Message) -> Result<(), Self::Error> {
        let this = self.get_mut();
        this.counts.starts.fetch_add(1, Ordering::Relaxed);
        Pin::new(&mut this.inner).start_send(frame)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        this.counts.flush_polls.fetch_add(1, Ordering::Relaxed);
        Pin::new(&mut this.inner).poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        this.counts.close_polls.fetch_add(1, Ordering::Relaxed);
        Pin::new(&mut this.inner).poll_close(cx)
    }
}

#[tokio::test(start_paused = true)]
async fn pure_send_chain_counts_separate_sink_operations_from_async_io_polls() -> TestResult {
    let payload = body(LARGE)?;
    for frame in [
        Some(1024),
        Some(4096),
        Some(32768),
        Some(256 * 1024),
        Some(1024 * 1024),
        None,
    ] {
        for partial in [false, true] {
            let maximum = if partial { 65533 } else { usize::MAX };
            let io = ScriptedIo::new(LARGE + 250_000, maximum, None);
            let (write, _) = socket(&io, &CancellationToken::new()).await;
            let counts = Arc::new(SinkCounts::default());
            let mut write = CountedSink {
                inner: write,
                counts: counts.clone(),
            };
            let (_tx, mut rx) = mpsc::channel(1);
            let heartbeat = HeartbeatState::new(906);
            let phase = crate::module::ws_client::v2_test_support::operation(
                &crate::ws::SendOptions::default(),
                false,
                1,
            )?;
            phase.enqueue()?;
            check!(phase.mark_writing(crate::ws::ConnectionId::from_allocated(901)))?;
            let started = std::time::Instant::now();
            let result = tokio::time::timeout(
                LIMIT,
                send(
                    &mut write,
                    payload.clone(),
                    frame,
                    &mut rx,
                    &heartbeat,
                    &phase,
                    &CancellationToken::new(),
                ),
            )
            .await?;
            let send_elapsed = started.elapsed();
            check_eq!(result, Ok(()))?;
            // The wire oracle and content checks are outside the measured send interval.
            validate_upload(&io.lock()?.wire, &payload, frame, None)?;
            check!(payload
                .iter()
                .enumerate()
                .all(|(offset, byte)| *byte == (offset % 251) as u8))?;
            let frames = LARGE.div_ceil(frame.map_or(LARGE, |value| value));
            check_eq!(counts.starts.load(Ordering::Relaxed), frames)?;
            check_eq!(counts.ready_polls.load(Ordering::Relaxed), frames)?;
            check_eq!(counts.flush_polls.load(Ordering::Relaxed), frames)?;
            check_eq!(counts.close_polls.load(Ordering::Relaxed), 0)?;
            check_eq!(io.counts()?.flush_polls, frames)?;
            let expected_wire = LARGE
                + (frames - 1) * header_size(frame.map_or(LARGE, |value| value))
                + header_size(LARGE - (frames - 1) * frame.map_or(LARGE, |value| value));
            check_eq!(io.counts()?.accepted_bytes, expected_wire)?;
            println!(
                "SEND_CHAIN_RESULT {}",
                serde_json::json!({
                    "case":"pure-send-chain","frame_size":frame,"partial_writes":partial,"payload_bytes":LARGE,
                    "sink":{"ready_polls":counts.ready_polls.load(Ordering::Relaxed),"starts":frames,"flush_polls":counts.flush_polls.load(Ordering::Relaxed)},
                    "io":io.counts()?,"elapsed_send_us":send_elapsed.as_micros(),"timing_scope":"diagnostic; includes bounded test recording and counters; excludes payload construction and wire validation",
                    "socket_syscalls":null,"split_lock_acquisitions":null,"scope":"no TCP, receiver, handshake, callbacks or active heartbeat","passed":true
                })
            );
        }
    }
    Ok(())
}

#[test]
fn independent_wire_oracle_rejects_missing_tail_duplicate_prefix_and_midframe_control() -> TestResult
{
    // Binary "abcde", client mask 01 02 03 04; fixed bytes independent of the encoder.
    let valid = [0x82, 0x85, 1, 2, 3, 4, 0x60, 0x60, 0x60, 0x60, 0x64];
    validate_upload(&valid, b"abcde", None, None)?;
    let mut missing_tail = valid.to_vec();
    missing_tail.pop();
    let mut repeated_prefix = valid.to_vec();
    repeated_prefix.extend_from_slice(&valid);
    let mut inside = valid[..8].to_vec();
    inside.extend_from_slice(&[0x8a, 0x80, 1, 2, 3, 4]);
    inside.extend_from_slice(&valid[8..]);
    for (name, mutant) in [
        ("missing-tail", missing_tail),
        ("repeated-prefix", repeated_prefix),
        ("control-inside-body", inside),
    ] {
        check!(
            validate_upload(&mutant, b"abcde", None, None).is_err(),
            "oracle accepted {name} mutation"
        )?;
    }
    Ok(())
}

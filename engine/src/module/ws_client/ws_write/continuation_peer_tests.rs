//! Real wire checks for retiring a writer with an unfinished fragmented message.
//! A second handshake is opened explicitly; these tests do not exercise worker reconnect policy.

use super::*;
use crate::api::traits::ws::ws_request_config::WSRequestConfig;
use crate::module::ws_client::test_support::{check, check_eq, test_error, TestResult};
use crate::module::ws_client::write::queued_request::DispatchPhase;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio_tungstenite::WebSocketStream;

const LIMIT: Duration = Duration::from_secs(4);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Only the test wrapper gates progress. The wrapped socket still performs the actual
/// client masking and network flush before the next frame boundary becomes observable.
struct GateSink<W> {
    inner: W,
    controls: mpsc::Sender<ControlMessage>,
    completed_flushes: usize,
    first_fragment_started: bool,
    first_fragment_flushed: bool,
    boundary_flushed: Arc<AtomicBool>,
    entered: Option<oneshot::Sender<()>>,
    release: Option<oneshot::Receiver<()>>,
}

impl<W> Sink<Message> for GateSink<W>
where
    W: Sink<Message, Error = WsError> + Unpin,
{
    type Error = WsError;

    fn poll_ready(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.get_mut().inner).poll_ready(context)
    }

    fn start_send(self: Pin<&mut Self>, message: Message) -> Result<(), Self::Error> {
        let this = self.get_mut();
        let first_fragment = matches!(
            &message,
            Message::Frame(frame)
                if frame.header().opcode == OpCode::Data(OpData::Binary)
                    && !frame.header().is_final
        );
        Pin::new(&mut this.inner).start_send(message)?;
        this.first_fragment_started |= first_fragment;
        Ok(())
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        // One data flush plus fifteen control flushes have completed. Hold the
        // final control, then let drain_ready_controls reach its fairness yield.
        if this.completed_flushes == MAX_READY_CONTROLS_PER_BOUNDARY {
            if let Some(entered) = this.entered.take() {
                if entered.send(()).is_err() {
                    return Poll::Ready(Err(WsError::Io(std::io::Error::other(
                        "wire boundary observer closed",
                    ))));
                }
            }
            if let Some(release) = this.release.as_mut() {
                match Pin::new(release).poll(context) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(())) => this.release = None,
                    Poll::Ready(Err(error)) => {
                        return Poll::Ready(Err(WsError::Io(std::io::Error::other(format!(
                            "wire boundary release closed: {error}"
                        )))));
                    }
                }
            }
        }
        match Pin::new(&mut this.inner).poll_flush(context) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Ready(Ok(())) => {}
        }
        let Some(completed) = this.completed_flushes.checked_add(1) else {
            return Poll::Ready(Err(WsError::Io(std::io::Error::other(
                "wire flush counter exhausted",
            ))));
        };
        this.completed_flushes = completed;
        if completed == MAX_READY_CONTROLS_PER_BOUNDARY + 1 {
            this.boundary_flushed.store(true, Ordering::Release);
        }
        if this.first_fragment_started && !this.first_fragment_flushed {
            this.first_fragment_flushed = true;
            for _ in 0..MAX_READY_CONTROLS_PER_BOUNDARY {
                if let Err(error) = this.controls.try_send(ControlMessage::FlushAutomatic) {
                    return Poll::Ready(Err(WsError::Io(std::io::Error::other(format!(
                        "inject wire boundary control: {error}"
                    )))));
                }
            }
        }
        Poll::Ready(Ok(()))
    }

    fn poll_close(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.get_mut().inner).poll_close(context)
    }
}

async fn socket_pair() -> TestResult<(WebSocketStream<TcpStream>, WebSocketStream<TcpStream>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let client = async {
        let stream = TcpStream::connect(address).await?;
        let (socket, _) =
            tokio_tungstenite::client_async(format!("ws://{address}/"), stream).await?;
        Ok::<_, crate::module::ws_client::test_support::TestError>(socket)
    };
    let peer = async {
        let (stream, _) = listener.accept().await?;
        let socket = tokio_tungstenite::accept_async(stream).await?;
        Ok::<_, crate::module::ws_client::test_support::TestError>(socket)
    };
    tokio::time::timeout(LIMIT, async { tokio::try_join!(client, peer) }).await?
}

/// The controlled payloads are short. Reject extensions rather than accepting
/// an unexpected frame shape or allocating according to arbitrary peer input.
async fn read_masked_frame(stream: &mut TcpStream) -> TestResult<(u8, Vec<u8>)> {
    tokio::time::timeout(LIMIT, async {
        let mut header = [0u8; 2];
        stream.read_exact(&mut header).await?;
        let [flags, encoded_length] = header;
        check!(encoded_length & 0x80 != 0, "client frame must be masked")?;
        let length = usize::from(encoded_length & 0x7f);
        check!(length < 126, "unexpected extended wire payload")?;
        let mut mask = [0u8; 4];
        stream.read_exact(&mut mask).await?;
        let mut payload = Vec::new();
        payload.try_reserve_exact(length)?;
        payload.resize(length, 0);
        stream.read_exact(&mut payload).await?;
        for (byte, key) in payload.iter_mut().zip(mask.iter().cycle()) {
            *byte ^= *key;
        }
        Ok((flags, payload))
    })
    .await?
}

async fn check_no_more_wire_bytes(stream: &mut TcpStream) -> TestResult {
    let mut byte = [0u8; 1];
    match tokio::time::timeout(LIMIT, stream.read(&mut byte)).await? {
        Ok(0) => Ok(()),
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted
            ) =>
        {
            Ok(())
        }
        result => Err(test_error(format!(
            "unfinished connection must end without a continuation or message B: {result:?}"
        ))),
    }
}

fn context(
    queue: Arc<PriorityWriteQueue>,
    urgent_queue: Arc<PriorityWriteQueue>,
    control_rx: mpsc::Receiver<ControlMessage>,
    io_event_tx: mpsc::Sender<IoEvent>,
    generation: u64,
) -> WriteLoopContext {
    WriteLoopContext {
        queue,
        urgent_queue,
        control_rx,
        pending_requests: PendingRequestView::default(),
        io_event_tx,
        generation,
        cancel: CancellationToken::new(),
        data_frame_payload_size: Some(3),
        control_write_timeout: REQUEST_TIMEOUT,
        data_frame_write_timeout: REQUEST_TIMEOUT,
        heartbeat_interval: Duration::from_secs(3_600),
        pong_timeout: Duration::from_secs(7_200),
        response_dispatch_grace: Duration::from_secs(1),
        heartbeat: Arc::new(HeartbeatState::new(generation)),
    }
}

async fn check_interrupted_prefix_on_wire(deadline: bool) -> TestResult {
    let (client, mut peer) = socket_pair().await?;
    let queue = PriorityWriteQueue::new(2, 64)
        .map_err(|error| test_error(format!("wire queue: {error:?}")))?;
    let urgent_queue = PriorityWriteQueue::new(1, 64)
        .map_err(|error| test_error(format!("wire urgent queue: {error:?}")))?;
    let shutdown = CancellationToken::new();
    let dispatch_cancel = CancellationToken::new();
    let config = WSRequestConfig {
        write_timeout: REQUEST_TIMEOUT,
        idempotent: true,
        send_retry_count: 1,
        ..WSRequestConfig::default()
    };
    let written_a = queue
        .enqueue(
            "wire-A".to_string(),
            None,
            Message::Binary(Bytes::from_static(b"abcdef")),
            6,
            config.clone(),
            &shutdown,
            dispatch_cancel.clone(),
            DispatchPhase::new(),
            CancellationToken::new(),
        )
        .await
        .map_err(|error| test_error(format!("enqueue wire A: {error:?}")))?;
    let mut written_b = queue
        .enqueue(
            "wire-B".to_string(),
            None,
            Message::Binary(Bytes::from_static(b"B")),
            1,
            config,
            &shutdown,
            CancellationToken::new(),
            DispatchPhase::new(),
            CancellationToken::new(),
        )
        .await
        .map_err(|error| test_error(format!("enqueue wire B: {error:?}")))?;
    let (control_tx, control_rx) = mpsc::channel(MAX_READY_CONTROLS_PER_BOUNDARY);
    let (entered, entered_rx) = oneshot::channel();
    let (release_tx, release) = oneshot::channel();
    let boundary_flushed = Arc::new(AtomicBool::new(false));
    let sink = GateSink {
        inner: client,
        controls: control_tx,
        completed_flushes: 0,
        first_fragment_started: false,
        first_fragment_flushed: false,
        boundary_flushed: Arc::clone(&boundary_flushed),
        entered: Some(entered),
        release: Some(release),
    };
    let (io_event_tx, mut io_event_rx) = mpsc::channel(2);
    // No task is spawned: every error path drops the local future and its socket.
    let mut writer = Box::pin(run_write_loop(
        sink,
        context(
            Arc::clone(&queue),
            Arc::clone(&urgent_queue),
            control_rx,
            io_event_tx,
            41,
        ),
        CancellationToken::new(),
    ));
    tokio::time::timeout(LIMIT, async {
        tokio::select! {
            _ = writer.as_mut() => Err(test_error("writer ended before the wire boundary")),
            entered = entered_rx => entered.map_err(|error| test_error(format!("wire gate: {error}"))),
        }
    })
    .await??;
    let (flags, payload) = read_masked_frame(peer.get_mut()).await?;
    check_eq!(flags, 0x02, "first frame must be non-FIN binary")?;
    check_eq!(payload.as_slice(), b"abc")?;

    release_tx
        .send(())
        .map_err(|_| test_error("wire gate release receiver closed"))?;
    check!(
        writer.as_mut().now_or_never().is_none(),
        "writer must yield after the final ready control"
    )?;
    check!(
        boundary_flushed.load(Ordering::Acquire),
        "the final real flush must finish before interrupting at the fairness yield"
    )?;
    if deadline {
        // Handshake and first-frame reads already used real time. No writer is
        // polled during this advance; resume real time before the next socket I/O.
        tokio::time::pause();
        tokio::time::advance(REQUEST_TIMEOUT + Duration::from_secs(1)).await;
        tokio::time::resume();
    } else {
        dispatch_cancel.cancel();
    }
    tokio::time::timeout(LIMIT, writer.as_mut()).await?;
    drop(writer);
    check_eq!(
        tokio::time::timeout(LIMIT, written_a).await??,
        Err(NetError::DeliveryUnknown)
    )?;
    let event = tokio::time::timeout(LIMIT, io_event_rx.recv())
        .await?
        .ok_or_else(|| test_error("retired writer did not publish WriteEnded"))?;
    check!(matches!(
        event,
        IoEvent::WriteEnded {
            generation: 41,
            error: NetError::DeliveryUnknown,
        }
    ))?;
    check!(matches!(
        written_b.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ))?;
    check_no_more_wire_bytes(peer.get_mut()).await?;
    drop(peer);

    // Feed the retained B to a separately established writer. Closing its queue
    // gives that writer a natural exit after B, without a cleanup cancellation.
    let (next_client, mut next_peer) = socket_pair().await?;
    let (_next_control_tx, next_control_rx) = mpsc::channel(1);
    let (next_events_tx, mut next_events_rx) = mpsc::channel(1);
    queue.close();
    let next_writer = run_write_loop(
        next_client,
        context(queue, urgent_queue, next_control_rx, next_events_tx, 42),
        CancellationToken::new(),
    );
    tokio::time::timeout(LIMIT, next_writer).await?;
    check_eq!(tokio::time::timeout(LIMIT, written_b).await??, Ok(()))?;
    let (flags, payload) = read_masked_frame(next_peer.get_mut()).await?;
    check_eq!(
        flags,
        0x82,
        "fresh connection must receive one FIN binary B"
    )?;
    check_eq!(payload.as_slice(), b"B")?;
    check_no_more_wire_bytes(next_peer.get_mut()).await?;
    check!(tokio::time::timeout(LIMIT, next_events_rx.recv())
        .await?
        .is_none())?;
    Ok(())
}

#[tokio::test]
async fn raw_peer_dispatch_cancel_retires_fragmented_writer_before_next_message() -> TestResult {
    check_interrupted_prefix_on_wire(false).await
}

#[tokio::test]
async fn raw_peer_total_deadline_retires_fragmented_writer_before_next_message() -> TestResult {
    check_interrupted_prefix_on_wire(true).await
}

use super::{run_budget, CancellationSignal, StreamBudget};
use crate::api::error::{ErrorStage, NetError};
use bytes::{BufMut, Bytes, BytesMut};
use futures_util::future::BoxFuture;
use futures_util::{stream, FutureExt};
use reqwest::{Request, Response};
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::{mpsc, oneshot};

const CHUNK_BYTES: usize = 64 * 1024;

pub(crate) struct UploadReader {
    reader: Pin<Box<dyn AsyncRead + Send>>,
    remaining: u64,
    buffer: BytesMut,
}

impl UploadReader {
    pub(crate) fn new(reader: impl AsyncRead + Send + 'static, length: u64) -> Self {
        Self {
            reader: Box::pin(reader),
            remaining: length,
            buffer: BytesMut::new(),
        }
    }

    async fn next_chunk(&mut self) -> Result<Option<Bytes>, NetError> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let limit = self.remaining.min(CHUNK_BYTES as u64) as usize;
        if self.buffer.capacity() == 0 {
            self.buffer.reserve(limit);
        }
        // Reuse the unsent portion of the allocation after a short read.
        let capacity = limit.min(self.buffer.capacity());
        let mut destination = (&mut self.buffer).limit(capacity);
        let read = self
            .reader
            .read_buf(&mut destination)
            .await
            .map_err(|error| NetError::from(error).with_stage(ErrorStage::Write))?;
        if read == 0 {
            return Err(NetError::from(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "upload reader ended before Content-Length",
            ))
            .with_stage(ErrorStage::Write));
        }
        self.remaining -= read as u64;
        Ok(Some(self.buffer.split().freeze()))
    }

    pub(super) fn attach(mut self, request: &mut Request) -> UploadTransfer {
        request
            .headers_mut()
            .insert(http::header::CONTENT_LENGTH, self.remaining.into());
        // Hyper can own the request body on a separate connection task. Keep
        // the actual reader in the request worker so cancelling that worker's
        // transfer releases even a pending reader under HTTP/2 backpressure.
        let (release, released) = oneshot::channel();
        let (pull, mut demand) =
            mpsc::channel::<oneshot::Sender<Result<(Bytes, bool), NetError>>>(1);
        let frame_state = (self.remaining != 0).then_some((pull, released));
        let frames = stream::unfold(frame_state, |state| async move {
            let (pull, released) = state?;
            let (reply, frame) = oneshot::channel();
            if pull.send(reply).await.is_ok() {
                match frame.await {
                    Ok(Ok((bytes, last))) => {
                        return Some((Ok(bytes), (!last).then_some((pull, released))))
                    }
                    Ok(Err(error)) => return Some((Err(error), None)),
                    Err(_) => {}
                }
            }
            // The reader can stop on an early rejection while the response is
            // still arriving. Signalling a short request EOF to HTTP/1 here
            // would close the connection and truncate that response.
            let _ = released.await;
            None
        });
        *request.body_mut() = Some(reqwest::Body::wrap_stream(frames));
        let pump = async move {
            while self.remaining > 0 {
                let Some(mut reply) = demand.recv().await else {
                    return Ok(SourceEnd::TransportClosed);
                };
                // Only one pull can be outstanding and no file read is spawned.
                let read = tokio::select! {
                    biased;
                    _ = reply.closed() => return Ok(SourceEnd::TransportClosed),
                    read = self.next_chunk() => read,
                };
                let bytes = match read {
                    Ok(Some(bytes)) => bytes,
                    Ok(None) => break,
                    Err(error) => {
                        let _ = reply.send(Err(error.clone()));
                        return Err(error);
                    }
                };
                if reply.send(Ok((bytes, self.remaining == 0))).is_err() {
                    return Ok(SourceEnd::TransportClosed);
                }
            }
            Ok(SourceEnd::Complete)
        };
        UploadTransfer {
            pump: Some(pump.boxed()),
            release: Some(release),
        }
    }
}

enum SourceEnd {
    Complete,
    TransportClosed,
}

impl SourceEnd {
    fn require_complete(self) -> Result<(), NetError> {
        match self {
            Self::Complete => Ok(()),
            Self::TransportClosed => Err(NetError::from(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "transport stopped consuming the upload body",
            ))
            .with_stage(ErrorStage::Write)),
        }
    }
}

#[derive(Default)]
pub(super) struct UploadTransfer {
    pump: Option<BoxFuture<'static, Result<SourceEnd, NetError>>>,
    release: Option<oneshot::Sender<()>>,
}

/// Drops with the rejected response when a redirect discards it, or after
/// collection if the response is returned to the caller.
#[derive(Clone)]
pub(super) struct RejectedUpload {
    _hold: Arc<oneshot::Sender<()>>,
}

impl UploadTransfer {
    /// Drive reads concurrently with transport. Report a reader error directly,
    /// before the HTTP stack can convert it to a generic connection failure.
    pub(super) async fn headers(
        &mut self,
        sending: impl Future<Output = Result<Response, NetError>>,
    ) -> Result<Response, NetError> {
        let Some(mut pump) = self.pump.take() else {
            return sending.await;
        };
        tokio::pin!(sending);
        let result = tokio::select! {
            biased;
            end = &mut pump => {
                let end = end?;
                let response = sending.await?;
                if response.status().is_success() {
                    end.require_complete()?;
                }
                Ok(response)
            }
            result = &mut sending => {
                self.pump = Some(pump);
                result
            }
        };
        match result {
            Ok(mut response) => {
                if !response.status().is_success() && self.pump.take().is_some() {
                    if let Some(release) = self.release.take() {
                        response.extensions_mut().insert(RejectedUpload {
                            _hold: Arc::new(release),
                        });
                    }
                }
                Ok(response)
            }
            Err(error) => {
                *self = Self::default();
                Err(error)
            }
        }
    }

    /// HTTP/2 can receive a 2xx head before the peer consumes the whole
    /// request. Drive both directions to avoid a flow-control deadlock.
    pub(super) async fn finish_with<T>(
        self,
        reading: impl Future<Output = Result<T, NetError>>,
        budget: StreamBudget,
        control: &CancellationSignal,
    ) -> Result<T, NetError> {
        let Some(pump) = self.pump else {
            return reading.await;
        };
        let writing = run_budget(budget, control, ErrorStage::Write, async {
            pump.await?.require_complete()
        });
        // The response collector owns its idle timer and received head. If
        // both directions reach the same deadline while that body is still
        // pending, it reports the same failure as a regular buffered request.
        let (response, ()) = tokio::try_join!(biased; reading, writing)?;
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::check;
    use super::*;
    use crate::api::error::ErrorKind;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::task::{Context, Poll};
    use tokio::io::ReadBuf;

    type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

    #[derive(Default)]
    struct Reads {
        bytes: AtomicUsize,
        polls: AtomicUsize,
    }

    struct Probe {
        source: &'static [u8],
        failure: Option<io::ErrorKind>,
        reads: Arc<Reads>,
    }

    impl AsyncRead for Probe {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            self.reads.polls.fetch_add(1, Ordering::Relaxed);
            if self.source.is_empty() {
                if let Some(kind) = self.failure {
                    return Poll::Ready(Err(kind.into()));
                }
            }
            let before = buf.filled().len();
            let result = Pin::new(&mut self.source).poll_read(cx, buf);
            self.reads
                .bytes
                .fetch_add(buf.filled().len() - before, Ordering::Relaxed);
            result
        }
    }

    #[tokio::test]
    async fn reads_only_the_declared_prefix_including_zero() -> TestResult {
        for (len, prefix) in [(0, None), (3, Some(&b"abc"[..]))] {
            let reads = Arc::new(Reads::default());
            let probe = Probe {
                source: b"abc-more",
                reads: reads.clone(),
                failure: None,
            };
            let mut upload = UploadReader::new(probe, len);
            check(
                upload.next_chunk().await?.as_deref() == prefix,
                "wrong prefix",
            )?;
            check(upload.next_chunk().await?.is_none(), "extra upload bytes")?;
            check(
                reads.bytes.load(Ordering::Relaxed) as u64 == len,
                "source overread",
            )?;
            check(
                reads.polls.load(Ordering::Relaxed) == usize::from(len != 0),
                "extra reader poll",
            )?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn short_eof_and_source_errors_preserve_the_io_category() -> TestResult {
        for (failure, expected) in [
            (None, io::ErrorKind::UnexpectedEof),
            (
                Some(io::ErrorKind::PermissionDenied),
                io::ErrorKind::PermissionDenied,
            ),
        ] {
            let probe = Probe {
                source: b"abc",
                reads: Arc::new(Reads::default()),
                failure,
            };
            let mut upload = UploadReader::new(probe, 8);
            check(
                upload.next_chunk().await?.as_deref() == Some(&b"abc"[..]),
                "lost initial bytes",
            )?;
            check(
                upload.next_chunk().await.is_err_and(|error| {
                    error.kind() == ErrorKind::Io
                        && error.context().stage == Some(ErrorStage::Write)
                        && error.io_kind() == Some(expected)
                }),
                "lost local reader error",
            )?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn huge_lengths_use_bounded_chunks() -> TestResult {
        let mut upload = UploadReader::new(tokio::io::repeat(1), u64::MAX);
        for _ in 0..2 {
            check(
                upload
                    .next_chunk()
                    .await?
                    .is_some_and(|bytes| bytes.len() == CHUNK_BYTES),
                "wrong read capacity for large Content-Length",
            )?;
        }
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn expired_attempt_after_headers_uses_the_pending_response_read() -> TestResult {
        use super::super::read_response;
        use crate::api::http::{HttpRequestId, TimeoutPhase};
        use std::time::Duration;
        use tokio::time::Instant;

        let duration = Duration::from_secs(1);
        let budget = StreamBudget::default().with_phase(
            Instant::now(),
            Some(duration),
            TimeoutPhase::Attempt,
        )?;
        let control = Arc::new(CancellationSignal::new());
        let response = http::Response::builder()
            .status(http::StatusCode::CREATED)
            .header("x-test-head", "ready")
            .body(reqwest::Body::wrap_stream(stream::pending::<
                Result<Bytes, io::Error>,
            >()))?;
        #[cfg(feature = "http-client-http2")]
        let http2 = crate::api::http::Http2Config::new();
        let reading = read_response(
            response.into(),
            1,
            HttpRequestId(1),
            1024,
            control.clone(),
            #[cfg(feature = "http-client-http2")]
            &http2,
            budget,
            None,
        );
        // The upload has delivered its last chunk. The response headers arrived
        // before expiry, but the body has not been collected when the timer wins.
        let transfer = UploadTransfer {
            pump: Some(async { Ok(SourceEnd::Complete) }.boxed()),
            ..UploadTransfer::default()
        };
        tokio::time::advance(duration).await;
        let error = transfer
            .finish_with(reading, budget, &control)
            .await
            .err()
            .ok_or("expired response body unexpectedly succeeded")?;
        check(
            error.timeout_phase() == Some(TimeoutPhase::Attempt)
                && error.context().stage == Some(ErrorStage::Receive)
                && error.http_response_head().is_some_and(|head| {
                    head.status == http::StatusCode::CREATED
                        && head
                            .headers
                            .get("x-test-head")
                            .is_some_and(|value| value == "ready")
                }),
            "upload replaced the response read timeout or its received headers",
        )
    }

    #[tokio::test(start_paused = true)]
    async fn response_collected_before_a_source_stall_keeps_the_write_timeout() -> TestResult {
        use crate::api::http::TimeoutPhase;
        use std::time::Duration;
        use tokio::time::Instant;

        let budget = StreamBudget::total(Instant::now(), Some(Duration::from_secs(1)))?;
        let transfer = UploadTransfer {
            pump: Some(std::future::pending().boxed()),
            ..UploadTransfer::default()
        };
        let error = transfer
            .finish_with(async { Ok(()) }, budget, &CancellationSignal::new())
            .await
            .err()
            .ok_or("stalled upload was reported as complete")?;
        check(
            error.kind() == ErrorKind::DeadlineExceeded
                && error.timeout_phase() == Some(TimeoutPhase::Total)
                && error.context().stage == Some(ErrorStage::Write)
                && error.http_response_head().is_none(),
            "a pending reader was misclassified as a response-body failure",
        )
    }
}

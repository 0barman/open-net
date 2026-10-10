use crate::api::error::NetError;
use crate::api::http::http_response::HttpRequestId;
use bytes::Bytes;
use futures_util::Stream;
use http::{HeaderMap, StatusCode};
use std::pin::Pin;
#[cfg(feature = "http-client-http2")]
use std::sync::{Arc, Mutex};

/// Owned, sendable stream of response body chunks.
///
/// Each item is an independently owned [`Bytes`] chunk. `None` marks clean EOF;
/// an error item terminates delivery for the associated response. The stream is
/// `'static` so it can outlive the worker future that opened the response. After
/// its first error it returns permanent EOF. For SDK transport responses, attempt,
/// total, and idle deadlines continue elapsing while consumption is paused. The
/// SDK releases expired transport resources independently of further polling;
/// the next poll returns the terminal error once, followed by permanent EOF.
pub type HttpByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, NetError>> + Send + 'static>>;

/// A response whose body is consumed incrementally.
///
/// Headers and status are available immediately; body bytes are pulled from
/// [`Self::stream`] with [`Self::next_chunk`]. The response does not buffer the
/// whole body and therefore is appropriate for large or long-lived payloads.
///
/// SDK transport streams retain admission capacity until EOF, error, drop, or
/// termination by the SDK supervisor. Timeouts, cancellation, and client shutdown
/// release network resources and admission capacity even if the stream is kept
/// without polling. A later poll receives the terminal error once and then EOF.
///
/// The idle budget includes time spent processing delivered bytes or otherwise
/// pausing consumption. Only delivery of a nonempty chunk resets it; the SDK does
/// not prefetch body bytes to extend this budget. Disable or increase idle timeout
/// when long consumption pauses are expected. With every timeout disabled, a
/// retained stream remains active until consumption, drop, cancellation, or
/// shutdown ends it, and can keep `HttpClient::drain` waiting in the meantime.
/// The opening runtime must remain alive for network I/O; deadline supervision
/// also requires the SDK runtime to remain alive and receive scheduling time.
/// Custom streams supplied to [`Self::new`] retain their own lifecycle semantics.
pub struct HttpStreamResponse {
    /// Response status code returned by the peer.
    pub status: StatusCode,
    /// Response headers returned by the peer.
    pub headers: HeaderMap,
    /// Identifier assigned to the originating request.
    pub request_id: HttpRequestId,
    /// Number of transport attempts used before the stream was opened.
    pub attempts: u32,
    /// Incremental body stream.
    pub stream: HttpByteStream,
    #[cfg(feature = "http-client-http2")]
    info: crate::api::http::HttpResponseInfo,
    #[cfg(feature = "http-client-http2")]
    flow_control: Arc<Mutex<crate::api::http::Http2FlowControlSnapshot>>,
}

impl HttpStreamResponse {
    /// Construct a streaming response with transport metadata omitted.
    ///
    /// This constructor remains useful to custom middleware and tests. A transport-created
    /// response is enriched internally with transport metadata.
    pub fn new(
        status: StatusCode,
        headers: HeaderMap,
        request_id: HttpRequestId,
        attempts: u32,
        stream: HttpByteStream,
    ) -> Self {
        #[cfg(feature = "http-client-http2")]
        let flow_control = Arc::new(Mutex::new(
            crate::api::http::Http2FlowControlSnapshot::default(),
        ));
        #[cfg(feature = "http-client-http2")]
        let stream = counted_stream(stream, Arc::clone(&flow_control));
        Self {
            status,
            headers,
            request_id,
            attempts,
            stream,
            #[cfg(feature = "http-client-http2")]
            info: crate::api::http::HttpResponseInfo::default(),
            #[cfg(feature = "http-client-http2")]
            flow_control,
        }
    }

    #[cfg(feature = "http-client-http2")]
    pub(crate) fn with_info(mut self, info: crate::api::http::HttpResponseInfo) -> Self {
        if let Some(snapshot) = info.flow_control {
            if let Ok(mut current) = self.flow_control.lock() {
                *current = snapshot;
            }
        }
        self.info = info;
        self
    }

    #[cfg(feature = "http-client-http2")]
    pub fn info(&self) -> &crate::api::http::HttpResponseInfo {
        &self.info
    }

    #[cfg(feature = "http-client-http2")]
    pub fn flow_control(&self) -> Option<crate::api::http::Http2FlowControlSnapshot> {
        self.info.flow_control.map(|_| {
            self.flow_control
                .lock()
                .map(|snapshot| *snapshot)
                .unwrap_or_default()
        })
    }

    /// Return the UTF-8 `Content-Type` header value, if present.
    pub fn content_type(&self) -> Option<&str> {
        self.headers
            .get(http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
    }

    /// Await and return the next body chunk.
    ///
    /// `None` indicates clean end of stream. A returned error should be treated
    /// as terminal for this response; the stream does not transparently replay
    /// bytes that have already been consumed.
    pub async fn next_chunk(&mut self) -> Option<Result<Bytes, NetError>> {
        use futures_util::StreamExt;
        self.stream.next().await
    }

    /// Check the response status against a caller-selected acceptance policy.
    pub fn check_status(&self, policy: crate::api::http::HttpStatusPolicy) -> Result<(), NetError> {
        if policy.accepts(self.status) {
            Ok(())
        } else {
            Err(NetError::from_http_status(self.status))
        }
    }
}

#[cfg(feature = "http-client-http2")]
struct CountedStream {
    inner: HttpByteStream,
    state: Arc<Mutex<crate::api::http::Http2FlowControlSnapshot>>,
}

#[cfg(feature = "http-client-http2")]
impl futures_util::Stream for CountedStream {
    type Item = Result<Bytes, NetError>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let result = this.inner.as_mut().poll_next(cx);
        if let std::task::Poll::Ready(Some(Ok(bytes))) = &result {
            if let Ok(mut snapshot) = this.state.lock() {
                snapshot.body_bytes_delivered = snapshot
                    .body_bytes_delivered
                    .saturating_add(bytes.len() as u64);
                snapshot.body_chunks_delivered = snapshot.body_chunks_delivered.saturating_add(1);
            }
        }
        result
    }
}

#[cfg(feature = "http-client-http2")]
fn counted_stream(
    stream: HttpByteStream,
    state: Arc<Mutex<crate::api::http::Http2FlowControlSnapshot>>,
) -> HttpByteStream {
    Box::pin(CountedStream {
        inner: stream,
        state,
    })
}

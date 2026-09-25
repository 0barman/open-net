use crate::api::error::NetError;
use crate::api::http::http_response::HttpRequestId;
use bytes::Bytes;
use futures_util::Stream;
use http::{HeaderMap, StatusCode};
use std::pin::Pin;

/// Owned, sendable stream of response body chunks.
///
/// Each item is an independently owned [`Bytes`] chunk. `None` marks clean EOF;
/// an error item terminates delivery for the associated response. The stream is
/// `'static` so it can outlive the worker future that opened the response.
pub type HttpByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, NetError>> + Send + 'static>>;

/// A response whose body is consumed incrementally.
///
/// Headers and status are available immediately; body bytes are pulled from
/// [`Self::stream`] with [`Self::next_chunk`]. The response does not buffer the
/// whole body and therefore is appropriate for large or long-lived payloads.
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
}

impl HttpStreamResponse {
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

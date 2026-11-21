use super::HttpStreamResponse;
use crate::api::error::{ErrorKind, ErrorStage, NetError};
use bytes::{Buf, Bytes};
use std::fmt;
use std::time::Duration;

const UTF8_BOM: &[u8] = b"\xef\xbb\xbf";

/// One dispatched server-sent event. Event data and identifiers may be sensitive.
#[derive(Clone, Eq, PartialEq)]
pub struct SseEvent {
    /// The value of the `event` field, or `message` when absent or empty.
    pub event: String,
    /// Consecutive `data` fields joined by a single LF, including empty fields.
    pub data: String,
    /// The effective event ID, inherited across events until set or cleared.
    pub last_event_id: String,
}

impl fmt::Debug for SseEvent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SseEvent")
            .field("event_bytes", &self.event.len())
            .field("data_bytes", &self.data.len())
            .field("id_bytes", &self.last_event_id.len())
            .finish()
    }
}

/// An incremental SSE decoder for a single open HTTP response.
///
/// Dropping the decoder drops the underlying response. Callers may save the
/// response's request ID before conversion to cancel from another task.
pub struct SseEvents {
    active: Option<ActiveStream>,
    last_event_id: String,
    retry_delay: Option<Duration>,
}

struct ActiveStream {
    response: HttpStreamResponse,
    chunk: Bytes,
    steps_left: usize,
    line: Vec<u8>,
    bom_remaining: &'static [u8],
    skip_lf: bool,
    max_event_bytes: usize,
    block_bytes: usize,
    data: Option<String>,
    event: String,
    pending_id: Option<String>,
}

// Count line scans and source polls, including empty chunks. A ready backlog
// of SSE comments still gives other tasks a chance to cancel or time out.
const STEPS_PER_YIELD: usize = 128;

impl SseEvents {
    pub(super) fn new(
        response: HttpStreamResponse,
        max_event_bytes: usize,
    ) -> Result<Self, NetError> {
        if max_event_bytes == 0 {
            return Err(NetError::input("max_event_bytes", "must be positive")
                .with_http_request_id(response.request_id));
        }
        let media_type = response
            .content_type()
            .and_then(|value| value.split(';').next());
        if !media_type.is_some_and(|value| value.trim().eq_ignore_ascii_case("text/event-stream")) {
            return Err(
                NetError::protocol("SSE requires Content-Type text/event-stream")
                    .with_stage(ErrorStage::Response)
                    .with_http_request_id(response.request_id)
                    .with_http_attempt(response.attempts),
            );
        }
        Ok(Self {
            active: Some(ActiveStream {
                response,
                chunk: Bytes::new(),
                steps_left: STEPS_PER_YIELD,
                line: Vec::new(),
                bom_remaining: UTF8_BOM,
                skip_lf: false,
                max_event_bytes,
                block_bytes: 0,
                data: None,
                event: String::new(),
                pending_id: None,
            }),
            last_event_id: String::new(),
            retry_delay: None,
        })
    }

    /// The last ID committed by a blank line, including an ID-only block.
    /// An empty ID clears the value. An unfinished block cannot change it.
    pub fn last_event_id(&self) -> &str {
        &self.last_event_id
    }

    /// The most recent server-suggested delay, in milliseconds on the wire.
    /// Invalid values and values exceeding a `u64` in milliseconds are ignored.
    pub fn retry_delay(&self) -> Option<Duration> {
        self.retry_delay
    }

    /// Yield one event, one terminal error, or permanent EOF. A clean EOF
    /// discards an incomplete event. A wire event named `error` is an ordinary
    /// event. Dropping this wait preserves the partial event for the next call.
    pub async fn next_event(&mut self) -> Option<Result<SseEvent, NetError>> {
        loop {
            let active = self.active.as_mut()?;
            if active.steps_left == 0 {
                active.steps_left = STEPS_PER_YIELD;
                tokio::task::yield_now().await;
            }
            if !active.chunk.is_empty() {
                match active.decode_chunk(&mut self.last_event_id, &mut self.retry_delay) {
                    Ok(Some(event)) => return Some(Ok(event)),
                    Err(error) => {
                        self.active = None;
                        return Some(Err(error));
                    }
                    Ok(None) => continue,
                }
            }
            active.steps_left -= 1;
            match active.response.next_chunk().await {
                Some(Ok(chunk)) => active.chunk = chunk,
                Some(Err(error)) => {
                    self.active = None;
                    return Some(Err(error));
                }
                None => {
                    let result = active.finish_bom_prefix();
                    self.active = None;
                    return result.err().map(Err);
                }
            }
        }
    }
}

impl ActiveStream {
    // A partial signature at EOF, or a mismatch after one or two bytes, is
    // ordinary input. Restore those bytes and apply the same line limit.
    fn finish_bom_prefix(&mut self) -> Result<(), NetError> {
        if !self.bom_remaining.is_empty() {
            let matched = UTF8_BOM.len() - self.bom_remaining.len();
            self.account(matched)?;
            self.line.extend_from_slice(&UTF8_BOM[..matched]);
            self.bom_remaining = &[];
        }
        Ok(())
    }

    fn account(&mut self, count: usize) -> Result<(), NetError> {
        self.block_bytes = self
            .block_bytes
            .checked_add(count)
            .filter(|total| *total <= self.max_event_bytes)
            .ok_or_else(|| {
                NetError::from(ErrorKind::ItemTooLarge)
                    .with_stage(ErrorStage::Receive)
                    .with_http_request_id(self.response.request_id)
                    .with_http_attempt(self.response.attempts)
            })?;
        Ok(())
    }

    fn decode_chunk(
        &mut self,
        last_event_id: &mut String,
        retry_delay: &mut Option<Duration>,
    ) -> Result<Option<SseEvent>, NetError> {
        // Only the start of the stream can contain a signature. Keep at most
        // two undecided bytes across polls and exclude a complete BOM from the limit.
        while let (Some((next, rest)), Some(byte)) =
            (self.bom_remaining.split_first(), self.chunk.first())
        {
            if next != byte {
                self.finish_bom_prefix()?;
                break;
            }
            self.chunk.advance(1);
            self.bom_remaining = rest;
        }
        while !self.chunk.is_empty() && self.steps_left > 0 {
            self.steps_left -= 1;
            // A CR terminates the line immediately, even if its optional LF
            // hasn't arrived. Otherwise a lone CR could stall an open stream.
            if std::mem::take(&mut self.skip_lf) && self.chunk.first() == Some(&b'\n') {
                self.chunk.advance(1);
                continue;
            }
            // One byte beyond the remaining limit is sufficient to reject an
            // oversized line, even if a custom source supplied a much larger chunk.
            let scan = self
                .chunk
                .len()
                .min((self.max_event_bytes - self.block_bytes).saturating_add(1));
            let length = self
                .chunk
                .iter()
                .take(scan)
                .position(|byte| matches!(byte, b'\r' | b'\n'))
                .map_or(scan, |offset| offset);
            self.account(length)?;
            self.line.extend_from_slice(&self.chunk.split_to(length));
            let Some(ending) = self.chunk.first().copied() else {
                break;
            };
            self.chunk.advance(1);
            self.skip_lf = ending == b'\r';
            if !self.line.is_empty() {
                self.account(1)?;
            }
            if self.line.is_empty() {
                self.block_bytes = 0;
                if let Some(id) = self.pending_id.take() {
                    *last_event_id = id;
                }
                let event = std::mem::take(&mut self.event);
                if let Some(data) = self.data.take() {
                    return Ok(Some(SseEvent {
                        event: if event.is_empty() {
                            "message".into()
                        } else {
                            event
                        },
                        data,
                        last_event_id: last_event_id.clone(),
                    }));
                }
                continue;
            }
            // Line endings are ASCII. A complete line contains all bytes of
            // each UTF-8 sequence, regardless of the transport chunk boundary.
            let line = String::from_utf8_lossy(&self.line);
            let (field, value) = match line.split_once(':') {
                Some((field, value)) => (field, value.strip_prefix(' ').map_or(value, |v| v)),
                None => (line.as_ref(), ""),
            };
            match field {
                "data" => match &mut self.data {
                    Some(data) => {
                        data.push('\n');
                        data.push_str(value);
                    }
                    None => self.data = Some(value.to_owned()),
                },
                "event" => value.clone_into(&mut self.event),
                "id" if !value.contains('\0') => self.pending_id = Some(value.to_owned()),
                "retry" if value.bytes().all(|byte| byte.is_ascii_digit()) => {
                    if let Ok(millis) = value.parse::<u64>() {
                        *retry_delay = Some(Duration::from_millis(millis));
                    }
                }
                _ => {}
            }
            self.line.clear();
        }
        Ok(None)
    }
}

#[cfg(test)]
#[path = "sse_tests.rs"]
mod tests;

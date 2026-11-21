use super::*;
use crate::api::http::{HttpByteStream, HttpRequestId};
use futures_util::{stream, FutureExt};
use http::{header::CONTENT_TYPE, HeaderMap, HeaderValue, StatusCode};
use tokio::sync::mpsc;

type TestResult = Result<(), crate::BoxError>;

fn check(condition: bool, message: &'static str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(message.into())
    }
}

fn response(media: Option<HeaderValue>, body: HttpByteStream) -> HttpStreamResponse {
    let mut headers = HeaderMap::new();
    if let Some(value) = media {
        headers.insert(CONTENT_TYPE, value);
    }
    HttpStreamResponse::new(StatusCode::OK, headers, HttpRequestId(42), 2, body)
}

fn sse_response(body: HttpByteStream) -> HttpStreamResponse {
    response(Some(HeaderValue::from_static("text/event-stream")), body)
}

#[test]
fn ready_heartbeats_yield_and_the_next_wait_resumes_in_order() -> TestResult {
    let heartbeat = b": heartbeat\n\n";
    for prefix in [
        vec![Bytes::from(heartbeat.repeat(1024))],
        vec![Bytes::from_static(heartbeat); 1024],
        vec![Bytes::new(); 1024],
    ] {
        let mut chunks = vec![Bytes::from_static(b"id: 42\n\n")];
        chunks.extend(prefix);
        chunks.push(Bytes::from_static(b"event: end\ndata:\n\n"));
        let mut events = from_chunks(chunks, 32)?;
        check(
            events.next_event().now_or_never().is_none(),
            "a single poll drained the whole control-only backlog",
        )?;
        // Dropping that wait may happen on any executor. No Tokio runtime is
        // needed to resume a decoder whose byte source is already ready.
        let event = futures::executor::block_on(next(&mut events))?;
        check(
            event.event == "end" && event.data.is_empty() && event.last_event_id == "42",
            "resuming after a cooperative yield lost the event or control state",
        )?;
    }
    Ok(())
}

fn from_chunks(chunks: Vec<Bytes>, limit: usize) -> Result<SseEvents, NetError> {
    sse_response(Box::pin(stream::iter(chunks.into_iter().map(Ok)))).into_sse_events(limit)
}

async fn next(events: &mut SseEvents) -> Result<SseEvent, crate::BoxError> {
    Ok(events
        .next_event()
        .await
        .ok_or("expected another event")??)
}

#[tokio::test]
async fn every_split_preserves_utf8_crlf_bom_and_event_boundaries() -> TestResult {
    let wire = "\u{feff}data: 你好\r\ndata: 第二行\r\n\r\nevent: end\rdata:\r\r".as_bytes();
    let verify = |chunks| async move {
        let mut events = from_chunks(chunks, 256)?;
        let first = next(&mut events).await?;
        check(
            first.event == "message" && first.data == "你好\n第二行",
            "broken multiline event",
        )?;
        let end = next(&mut events).await?;
        check(
            end.event == "end" && end.data.is_empty(),
            "broken named event",
        )?;
        check(events.next_event().await.is_none(), "extra event at EOF")
    };
    for split in 0..=wire.len() {
        let (head, tail) = wire.split_at(split);
        verify(vec![
            Bytes::copy_from_slice(head),
            Bytes::copy_from_slice(tail),
        ])
        .await?;
    }
    verify(wire.chunks(1).map(Bytes::copy_from_slice).collect()).await
}

#[tokio::test]
async fn stream_signature_does_not_consume_the_first_event_budget() -> TestResult {
    let wire = b"\xef\xbb\xbfdata:a\r\n\r\n";
    for split in 0..=wire.len() {
        let (head, tail) = wire.split_at(split);
        let mut events = from_chunks(
            vec![Bytes::copy_from_slice(head), Bytes::copy_from_slice(tail)],
            7,
        )?;
        check(
            next(&mut events).await?.data == "a",
            "split stream signature consumed event bytes",
        )?;
        check(
            events.next_event().await.is_none(),
            "extra event after initial signature",
        )?;
    }
    let mut empty = from_chunks(
        b"\xef\xbb\xbf\r\n"
            .chunks(1)
            .map(Bytes::copy_from_slice)
            .collect(),
        1,
    )?;
    check(
        empty.next_event().await.is_none(),
        "signature and blank line consumed the event limit",
    )?;
    for bytes in [&b"\xef\xbba\n\n"[..], &b"\xef\xbb\xbf\xef\xbb\xbf\r"[..]] {
        let mut events = from_chunks(bytes.chunks(1).map(Bytes::copy_from_slice).collect(), 2)?;
        check(
            matches!(events.next_event().await, Some(Err(error)) if error.kind() == ErrorKind::ItemTooLarge),
            "non-signature bytes were omitted from the limit",
        )?;
    }
    let mut partial = from_chunks(vec![Bytes::from_static(b"\xef\xbb")], 1)?;
    check(
        matches!(partial.next_event().await, Some(Err(error)) if error.kind() == ErrorKind::ItemTooLarge),
        "incomplete signature was treated as a BOM at EOF",
    )?;
    check(
        partial.next_event().await.is_none(),
        "signature error did not finish the stream",
    )?;
    Ok(())
}

#[tokio::test]
async fn control_blocks_update_id_and_retry_without_dispatching() -> TestResult {
    let wire =
        b": heartbeat\nid: 42\nretry: 1500\n\nevent: discarded\n\nevent: segment\ndata: one\n\n\
                 id: bad\0id\nretry: +20\nretry: 18446744073709551616\nretry: 20 \nretry:\n\
                 event:\ndata\ndata:  two: more\n\nid\n\nevent: end\ndata:\n\n";
    let mut events = from_chunks(vec![Bytes::from_static(wire)], 512)?;
    let segment = next(&mut events).await?;
    check(
        segment.event == "segment" && segment.last_event_id == "42",
        "ID-only block or event type lost",
    )?;
    check(
        events.retry_delay() == Some(Duration::from_millis(1500)),
        "retry hint lost",
    )?;
    let unnamed = next(&mut events).await?;
    check(
        unnamed.event == "message" && unnamed.data == "\n two: more",
        "field parsing changed data",
    )?;
    check(
        events.last_event_id() == "42" && unnamed.last_event_id == "42",
        "NUL ID overwrote state",
    )?;
    check(
        events.retry_delay() == Some(Duration::from_millis(1500)),
        "invalid retry hint overwrote state",
    )?;
    let end = next(&mut events).await?;
    check(
        end.event == "end" && end.data.is_empty() && end.last_event_id.is_empty(),
        "empty ID or data lost",
    )?;
    check(
        events.last_event_id().is_empty() && events.next_event().await.is_none(),
        "control block leaked an event",
    )
}

#[tokio::test]
async fn final_partial_event_is_discarded_and_utf8_replacement_is_local() -> TestResult {
    let wire = b"data: a\xc3z\n\ndata: \xef\xbb\xbftext\n\nDATA: ignored\nunrecognized\n\n\
                 id: published\n\nid: pending\nretry: 0\ndata: never dispatched\n";
    let mut events = from_chunks(vec![Bytes::from_static(wire)], 256)?;
    check(
        next(&mut events).await?.data == "a\u{fffd}z",
        "invalid UTF-8 did not preserve valid text",
    )?;
    check(
        next(&mut events).await?.data == "\u{feff}text",
        "non-leading BOM was removed",
    )?;
    check(
        events.next_event().await.is_none(),
        "EOF dispatched an incomplete event",
    )?;
    check(
        events.last_event_id() == "published",
        "incomplete block committed its ID",
    )?;
    check(
        events.retry_delay() == Some(Duration::ZERO),
        "retry line did not update before EOF",
    )?;
    check(events.next_event().await.is_none(), "EOF was not permanent")
}

#[test]
fn media_type_limit_and_http_status_are_independent() -> TestResult {
    for value in ["text/event-stream", "TEXT/Event-Stream; charset=UTF-8"] {
        let mut response = response(
            Some(HeaderValue::from_static(value)),
            Box::pin(stream::empty()),
        );
        response.status = StatusCode::FORBIDDEN;
        response.into_sse_events(8)?;
    }
    for value in [
        None,
        Some(HeaderValue::from_static("application/json")),
        Some(HeaderValue::from_static("text/event-streaming")),
    ] {
        let result = response(value, Box::pin(stream::empty())).into_sse_events(8);
        check(
            matches!(result, Err(error) if error.kind() == ErrorKind::Protocol && error.context().http_request_id == Some(HttpRequestId(42))),
            "unexpected media type accepted or misclassified",
        )?;
    }
    let zero = sse_response(Box::pin(stream::empty())).into_sse_events(0);
    check(
        matches!(zero, Err(error) if error.kind() == ErrorKind::InvalidInput),
        "zero limit accepted",
    )
}

#[tokio::test]
async fn limit_applies_to_one_block_and_to_incomplete_lines_before_eof() -> TestResult {
    let mut exact = from_chunks(vec![Bytes::from_static(b"data:a\r\n\r\ndata:b\n\n")], 7)?;
    check(
        next(&mut exact).await?.data == "a" && next(&mut exact).await?.data == "b",
        "limit leaked between events or charged CRLF twice",
    )?;
    for wire in [
        &b"data:ABCD"[..],      // unfinished line
        &b"data:AB\n"[..],      // normalized ending exceeds the limit
        &b"data:\ndata:\n"[..], // many short lines in one event
        &b":12345678"[..],      // comments are still bounded
    ] {
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let body = Box::pin(stream::poll_fn(move |cx| receiver.poll_recv(cx)));
        let mut events = sse_response(body).into_sse_events(7)?;
        for byte in wire {
            sender.send(Ok(Bytes::copy_from_slice(std::slice::from_ref(byte))))?;
        }
        let result = events.next_event().now_or_never();
        check(
            matches!(result, Some(Some(Err(error))) if error.kind() == ErrorKind::ItemTooLarge && error.context().stage == Some(ErrorStage::Receive)),
            "limit did not fail before waiting for more bytes",
        )?;
        check(
            sender.is_closed() && events.next_event().await.is_none(),
            "oversize decoder retained response or repeated its error",
        )?;
    }
    Ok(())
}

#[tokio::test]
async fn wire_error_event_and_transport_error_are_distinct_and_ordered() -> TestResult {
    for kind in [ErrorKind::TimedOut, ErrorKind::Cancelled] {
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let body = stream::poll_fn(move |cx| receiver.poll_recv(cx));
        let mut events = sse_response(Box::pin(body)).into_sse_events(64)?;
        sender.send(Ok(Bytes::from_static(
            b"data:first\n\nevent:error\ndata:busy\n\ndata:incomplete\n",
        )))?;
        check(next(&mut events).await?.data == "first", "first event lost")?;
        sender.send(Err(
            NetError::from(kind).with_http_request_id(HttpRequestId(73))
        ))?;
        sender.send(Ok(Bytes::from_static(b"data:later\n\n")))?;
        let message = next(&mut events).await?;
        check(
            message.event == "error" && message.data == "busy",
            "terminal error overtook an event in an already delivered chunk",
        )?;
        check(
            matches!(events.next_event().await, Some(Err(error)) if error.kind() == kind && error.context().http_request_id == Some(HttpRequestId(73))),
            "transport error was reclassified or lost",
        )?;
        check(
            events.next_event().await.is_none(),
            "decoder continued after terminal error",
        )?;
    }
    Ok(())
}

#[tokio::test]
async fn dropping_one_wait_keeps_partial_bytes_and_dropping_owner_releases_source() -> TestResult {
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let body = Box::pin(stream::poll_fn(move |cx| receiver.poll_recv(cx)));
    let mut events = sse_response(body).into_sse_events(128)?;
    sender.send(Ok(Bytes::from_static(
        b"id: waiting\nretry: 12\n\nevent: segment\ndata: \xe4",
    )))?;
    check(
        events.next_event().now_or_never().is_none(),
        "partial UTF-8 completed an event",
    )?;
    check(
        events.last_event_id() == "waiting"
            && events.retry_delay() == Some(Duration::from_millis(12)),
        "dropping a wait lost committed control state",
    )?;
    sender.send(Ok(Bytes::from_static(b"\xbd\xa0\r")))?;
    check(
        events.next_event().now_or_never().is_none(),
        "line ending dispatched without a blank line",
    )?;
    sender.send(Ok(Bytes::from_static(b"\n\r")))?;
    let event = next(&mut events).await?;
    check(
        event.data == "你" && event.event == "segment" && event.last_event_id == "waiting",
        "resumed wait lost partial state or waited for LF after CR",
    )?;
    drop(events);
    check(
        sender.is_closed(),
        "dropping the decoder retained its source",
    )
}

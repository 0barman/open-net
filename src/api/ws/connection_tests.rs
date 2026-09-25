use super::*;
use crate::error::{ErrorKind, ErrorStage};
use crate::module::ws_client::test_support::{check_eq, TestResult};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

fn attempt(metadata: crate::Metadata) -> HandshakeAttempt {
    HandshakeAttempt {
        client_id: ClientId::from_allocated(1),
        session_id: SessionId::from_allocated(2),
        cycle_id: CycleId::from_allocated(3),
        attempt_id: AttemptId::from_allocated(4),
        metadata: Arc::new(metadata),
    }
}

fn event(kind: ConnectionEventKind) -> ConnectionEvent {
    ConnectionEvent {
        sequence: 1,
        client_id: ClientId::from_allocated(1),
        session_id: SessionId::from_allocated(2),
        occurred_at: SystemTime::UNIX_EPOCH,
        kind,
    }
}

#[test]
fn journal_defaults_and_minimum_include_all_four_reserved_facts() -> TestResult {
    let defaults = JournalOptions::default();
    check_eq!(defaults.max_events, 32)?;
    check_eq!(defaults.max_bytes, 1024 * 1024)?;
    defaults.validate()?;
    check_eq!(JournalOptions::MIN_EVENTS, 4)?;
    JournalOptions {
        max_events: 4,
        max_bytes: 4 * MAX_CONNECTION_EVENT_BYTES,
    }
    .validate()?;
    Ok(())
}

#[test]
fn journal_rejects_insufficient_or_unrepresentable_capacity_with_exact_fields() -> TestResult {
    for (max_events, max_bytes, field) in [
        (0, 1024 * 1024, "journal.max_events"),
        (3, 1024 * 1024, "journal.max_events"),
        (4, 4 * MAX_CONNECTION_EVENT_BYTES - 1, "journal.max_bytes"),
        (usize::MAX, 1024 * 1024, "journal.max_events"),
        (4, usize::MAX, "journal.max_bytes"),
    ] {
        let options = JournalOptions {
            max_events,
            max_bytes,
        };
        let error = options
            .validate()
            .err()
            .ok_or("invalid journal capacity accepted")?;
        check_eq!(error.kind(), ErrorKind::InvalidConfig)?;
        check_eq!(
            error.config_error().ok_or("missing journal field")?.field(),
            field
        )?;
    }
    Ok(())
}

#[test]
fn event_measurement_counts_utf8_metadata_and_owned_credential_fields() -> TestResult {
    let empty = event(ConnectionEventKind::AttemptStarted {
        attempt: attempt(crate::Metadata::new()),
    });
    let metadata = event(ConnectionEventKind::AttemptStarted {
        attempt: attempt(crate::Metadata::from([("key".to_owned(), "值".to_owned())])),
    });
    check_eq!(metadata.measured_size()? - empty.measured_size()?, 6)?;
    let connection = ConnectionInfo {
        client_id: ClientId::from_allocated(1),
        session_id: SessionId::from_allocated(2),
        connection_id: ConnectionId::from_allocated(5),
        cycle_id: CycleId::from_allocated(3),
        attempt_id: AttemptId::from_allocated(4),
        connected_at: SystemTime::UNIX_EPOCH,
        credential_version: Some("版本".to_owned()),
    };
    let established = event(ConnectionEventKind::Established { connection });
    check_eq!(established.measured_size()? - empty.measured_size()?, 6)?;
    Ok(())
}

/// Test probe used to verify that event-size accounting does not inspect a
/// user-provided error source unexpectedly.
#[derive(Debug)]
struct SourceProbe(
    /// Counts formatting or source reads performed by the test probe.
    Arc<AtomicUsize>,
);
impl std::fmt::Display for SourceProbe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fetch_add(1, Ordering::SeqCst);
        f.write_str("source must never be formatted for event accounting")
    }
}
impl std::error::Error for SourceProbe {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.fetch_add(1, Ordering::SeqCst);
        None
    }
}

#[test]
fn error_accounting_ignores_arbitrary_user_source_and_preserves_owned_context() -> TestResult {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut context = crate::error::ErrorContext::default();
    context.peer_close = Some(PeerClose {
        code: Some(1000),
        reason: "关闭".to_owned(),
    });
    let source = crate::NetError::provider(SourceProbe(calls.clone())).with_context(context);
    let observed = event(ConnectionEventKind::Closed {
        result: Err(source),
    });
    let size = observed.measured_size()?;
    let cloned = observed.clone();
    drop(observed);
    check_eq!(cloned.measured_size()?, size)?;
    check_eq!(size, std::mem::size_of::<ConnectionEvent>() + 6)?;
    check_eq!(calls.load(Ordering::SeqCst), 0)?;
    Ok(())
}

#[test]
fn oversized_event_is_rejected_without_mutating_it() -> TestResult {
    let observed = event(ConnectionEventKind::Closed {
        result: Err(crate::NetError::config(
            "field",
            "x".repeat(MAX_CONNECTION_EVENT_BYTES),
        )),
    });
    let error = observed
        .measured_size()
        .err()
        .ok_or("oversized event accepted")?;
    check_eq!(error.kind(), ErrorKind::ItemTooLarge)?;
    check_eq!(error.context().stage, Some(ErrorStage::Dispatch))?;
    let ConnectionEventKind::Closed { result: Err(error) } = &observed.kind else {
        return Err("event mutated during measurement".into());
    };
    check_eq!(
        error
            .config_error()
            .ok_or("missing original detail")?
            .reason()
            .len(),
        MAX_CONNECTION_EVENT_BYTES
    )?;
    Ok(())
}

#[test]
fn terminal_value_owns_last_connection_details_after_observation_drop() -> TestResult {
    let observed = event(ConnectionEventKind::Closed {
        result: Ok(SessionEnd {
            reason: TerminationReason::PeerClose,
            last_connection: Some(ConnectionEnd {
                reason: TerminationReason::PeerClose,
                error: None,
                peer_close: Some(PeerClose {
                    code: Some(1000),
                    reason: "bye".to_owned(),
                }),
                io_end: Some(IoEndKind::PeerClose),
            }),
        }),
    });
    let size = observed.measured_size()?;
    let cloned = observed.clone();
    drop(observed);
    let ConnectionEventKind::Closed { result: Ok(end) } = cloned.kind else {
        return Err("terminal result changed".into());
    };
    check_eq!(end.reason, TerminationReason::PeerClose)?;
    let last = end.last_connection.ok_or("lost last physical connection")?;
    check_eq!(
        last.peer_close.ok_or("lost peer close")?.reason.as_str(),
        "bye"
    )?;
    check_eq!(size, std::mem::size_of::<ConnectionEvent>() + 3)?;
    Ok(())
}

#[test]
fn failed_attempt_counts_diagnostic_request_id_and_credential_without_reading_source() -> TestResult
{
    let body = br#"{"code":"PUBLIC"}"#;
    let length = body.len().to_string();
    let diagnostic = crate::ws::HandshakeDiagnostic::http(
        &crate::ws::HandshakeDiagnosticOptions {
            public_json_codes: vec!["PUBLIC".to_owned()],
        },
        [
            ("content-type", b"application/json".as_slice()),
            ("content-length", length.as_bytes()),
            ("retry-after", b"7".as_slice()),
        ]
        .into_iter(),
        Some(body),
    );
    let diagnostic_size = diagnostic
        .headers()
        .iter()
        .map(|(name, value)| name.as_str().len() + value.as_bytes().len())
        .sum::<usize>()
        + diagnostic
            .body_summary()
            .ok_or("missing captured public code")?
            .len();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut context = crate::error::ErrorContext::default();
    context.diagnostic = Some(diagnostic);
    context.request_id = Some(crate::ws::RequestId::new("request-id")?);
    let observed = event(ConnectionEventKind::AttemptFailed {
        attempt: attempt(crate::Metadata::from([(
            "key".to_owned(),
            "value".to_owned(),
        )])),
        credential_version: Some("version".to_owned()),
        error: crate::NetError::provider(SourceProbe(calls.clone())).with_context(context),
        retry: RetryDecision::Scheduled {
            after: Duration::from_millis(12),
        },
    });
    check_eq!(
        observed.measured_size()?,
        std::mem::size_of::<ConnectionEvent>() + diagnostic_size + 10 + 8 + 7
    )?;
    check_eq!(calls.load(Ordering::SeqCst), 0)?;
    Ok(())
}

#[test]
fn largest_valid_attempt_fields_fit_one_reserved_event_and_size_overflow_is_fallible() -> TestResult
{
    let metadata = crate::Metadata::from([("k".to_owned(), "v".repeat(16383))]);
    crate::ws::validate_metadata(&metadata)?;
    let mut context = crate::error::ErrorContext::default();
    context.request_id = Some(crate::ws::RequestId::new("r".repeat(1024))?);
    context.peer_close = Some(PeerClose {
        code: Some(1000),
        reason: "界".repeat(41),
    });
    let observed = event(ConnectionEventKind::AttemptFailed {
        attempt: attempt(metadata),
        credential_version: Some("c".repeat(256)),
        error: crate::NetError::from(ErrorKind::Io).with_context(context),
        retry: RetryDecision::Stop,
    });
    let size = observed.measured_size()?;
    if size > MAX_CONNECTION_EVENT_BYTES {
        return Err("valid bounded fields do not fit reserved event".into());
    }
    let mut meter = EventSize(1);
    let error = meter
        .add(usize::MAX)
        .err()
        .ok_or("byte size overflow accepted")?;
    check_eq!(error.kind(), ErrorKind::ResourceExhausted)?;
    check_eq!(meter.0, 1)?;
    Ok(())
}

#[test]
fn connection_debug_redacts_credentials_and_attempt_metadata_across_records() -> TestResult {
    let credential = "private-credential-revision-token";
    let metadata_value = "private-account-metadata";
    let connection = ConnectionInfo {
        client_id: ClientId::from_allocated(1),
        session_id: SessionId::from_allocated(2),
        connection_id: ConnectionId::from_allocated(5),
        cycle_id: CycleId::from_allocated(3),
        attempt_id: AttemptId::from_allocated(4),
        connected_at: SystemTime::UNIX_EPOCH,
        credential_version: Some(credential.to_owned()),
    };
    let failed = event(ConnectionEventKind::AttemptFailed {
        attempt: attempt(crate::Metadata::from([(
            "account".to_owned(),
            metadata_value.to_owned(),
        )])),
        credential_version: Some(credential.to_owned()),
        error: ErrorKind::HandshakeRejected.into(),
        retry: RetryDecision::Scheduled {
            after: Duration::from_millis(25),
        },
    });
    let established = event(ConnectionEventKind::Established {
        connection: connection.clone(),
    });
    let ended = event(ConnectionEventKind::Disconnected {
        connection: connection.clone(),
        end: ConnectionEnd {
            reason: TerminationReason::LocalClose,
            error: None,
            peer_close: None,
            io_end: None,
        },
    });
    for debug in [
        format!("{connection:?}"),
        format!("{failed:?}"),
        format!("{established:?}"),
        format!("{ended:?}"),
        format!("{:?}", ConnectionState::Connected(connection)),
    ] {
        check_eq!(debug.contains(credential), false)?;
        check_eq!(debug.contains(metadata_value), false)?;
        check_eq!(
            debug.contains(&format!(
                "credential_version_bytes: Some({})",
                credential.len()
            )),
            true
        )?;
        check_eq!(debug.contains("client_id: ClientId(1)"), true)?;
        check_eq!(debug.contains("session_id: SessionId(2)"), true)?;
        check_eq!(debug.contains("attempt_id: AttemptId(4)"), true)?;
    }
    let failed_debug = format!("{failed:?}");
    check_eq!(failed_debug.contains("HandshakeRejected"), true)?;
    check_eq!(failed_debug.contains("after: 25ms"), true)?;
    check_eq!(failed_debug.contains("metadata_count: 1"), true)?;
    Ok(())
}

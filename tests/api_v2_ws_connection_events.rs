#![cfg(feature = "ws-client")]

use open_net::error::ErrorKind;
use open_net::ws::{
    ConnectionEnd, ConnectionEvent, ConnectionEventKind, ConnectionInfo, JournalOptions,
    RetryDecision, SessionEnd, TerminationReason,
};

type TestResult = std::result::Result<(), open_net::BoxError>;

fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}

// Destructure the public records directly: no old getter or observation facade.
const _: fn(ConnectionEvent) = |event| {
    let ConnectionEvent {
        sequence,
        client_id,
        session_id,
        occurred_at,
        kind,
    } = event;
    let _: (u64, u64, u64, std::time::SystemTime) = (
        sequence,
        client_id.as_u64(),
        session_id.as_u64(),
        occurred_at,
    );
    match kind {
        ConnectionEventKind::AttemptStarted { attempt } => {
            let _ = attempt.metadata;
        }
        ConnectionEventKind::Established { connection } => {
            let _ = connection.connection_id;
        }
        ConnectionEventKind::AttemptFailed {
            attempt,
            credential_version,
            error,
            retry,
        } => {
            let _: (u64, Option<String>, open_net::NetError, RetryDecision) = (
                attempt.attempt_id.as_u64(),
                credential_version,
                error,
                retry,
            );
        }
        ConnectionEventKind::Disconnected { connection, end } => {
            let _ = (connection, end);
        }
        ConnectionEventKind::Closed { result } => {
            let _: open_net::Result<SessionEnd> = result;
        }
        _ => {}
    }
};

const _: fn(ConnectionInfo) = |connection| {
    let ConnectionInfo {
        client_id,
        session_id,
        connection_id,
        cycle_id,
        attempt_id,
        connected_at,
        credential_version,
    } = connection;
    let _: (
        u64,
        u64,
        u64,
        u64,
        u64,
        std::time::SystemTime,
        Option<String>,
    ) = (
        client_id.as_u64(),
        session_id.as_u64(),
        connection_id.as_u64(),
        cycle_id.as_u64(),
        attempt_id.as_u64(),
        connected_at,
        credential_version,
    );
};

#[test]
fn journal_defaults_and_minimum_reserve_are_valid() -> TestResult {
    let defaults = JournalOptions::default();
    defaults.validate()?;
    check(
        JournalOptions::MIN_EVENTS == 4
            && defaults.max_events == 32
            && defaults.max_bytes == 1024 * 1024,
        "journal defaults or terminal reservation minimum changed",
    )?;
    JournalOptions {
        max_events: 4,
        max_bytes: 128 * 1024,
    }
    .validate()?;
    Ok(())
}

#[test]
fn journal_rejects_insufficient_or_unrepresentable_capacity_with_field_details() -> TestResult {
    for (options, field) in [
        (
            JournalOptions {
                max_events: 3,
                ..Default::default()
            },
            "journal.max_events",
        ),
        (
            JournalOptions {
                max_events: usize::MAX,
                ..Default::default()
            },
            "journal.max_events",
        ),
        (
            JournalOptions {
                max_bytes: 128 * 1024 - 1,
                ..Default::default()
            },
            "journal.max_bytes",
        ),
        (
            JournalOptions {
                max_bytes: usize::MAX,
                ..Default::default()
            },
            "journal.max_bytes",
        ),
    ] {
        let error = options
            .validate()
            .err()
            .ok_or("invalid journal capacity was accepted")?;
        check(
            error.kind() == ErrorKind::InvalidConfig
                && error
                    .config_error()
                    .is_some_and(|detail| detail.field() == field),
            "journal validation omitted the exact failing public field",
        )?;
    }
    Ok(())
}

#[test]
fn session_end_carries_the_owned_physical_close_without_promoting_it_to_an_error() -> TestResult {
    let physical = ConnectionEnd {
        reason: TerminationReason::PeerClose,
        error: None,
        peer_close: Some(open_net::ws::PeerClose {
            code: Some(1000),
            reason: "peer-secret".to_owned(),
        }),
        io_end: Some(open_net::ws::IoEndKind::PeerClose),
    };
    let ended = SessionEnd {
        reason: TerminationReason::PeerClose,
        last_connection: Some(physical),
    };
    let cloned = ended.clone();
    drop(ended);
    let physical = cloned
        .last_connection
        .as_ref()
        .ok_or("session lost its physical end")?;
    check(
        cloned.reason == TerminationReason::PeerClose
            && physical.error.is_none()
            && physical
                .peer_close
                .as_ref()
                .is_some_and(|close| close.reason == "peer-secret")
            && !format!("{cloned:?}").contains("peer-secret"),
        "normal session end lost close ownership or exposed the peer reason",
    )?;
    let retry = RetryDecision::Scheduled {
        after: std::time::Duration::from_millis(250),
    };
    check(
        matches!(retry, RetryDecision::Scheduled { after } if after == std::time::Duration::from_millis(250)),
        "scheduled retry lost its sampled delay",
    )
}

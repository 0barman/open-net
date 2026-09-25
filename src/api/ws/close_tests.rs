use super::{CloseFrame, PeerClose};
use crate::error::{ErrorKind, ErrorStage};

type TestResult = Result<(), crate::BoxError>;
fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}

#[test]
fn close_frames_accept_defined_and_application_codes() -> TestResult {
    for code in [
        1000, 1001, 1002, 1003, 1007, 1008, 1009, 1010, 1011, 1012, 1013, 1014, 3000, 3999, 4000,
        4999,
    ] {
        let frame = CloseFrame::new(code, "done")?;
        check(
            frame.code() == code && frame.reason() == "done",
            "valid close frame changed input",
        )?;
        check(
            frame.clone().reason() == "done",
            "close frame clone changed reason",
        )?;
    }
    Ok(())
}

#[test]
fn close_frames_reject_reserved_and_out_of_range_wire_codes() -> TestResult {
    for code in [0, 999, 1004, 1005, 1006, 1015, 1016, 2999, 5000, u16::MAX] {
        let error = CloseFrame::new(code, "private-close-secret")
            .err()
            .ok_or("invalid wire close code was accepted")?;
        check(
            error.kind() == ErrorKind::InvalidInput
                && error.context().stage == Some(ErrorStage::Close),
            "invalid close code classification changed",
        )?;
        check(
            error
                .config_error()
                .is_some_and(|detail| detail.field() == "close.code"),
            "invalid close code lacks field information",
        )?;
        check(
            !format!("{error} {error:?}").contains("private-close-secret"),
            "close validation disclosed reason",
        )?;
    }
    Ok(())
}

#[test]
fn close_reason_limit_counts_utf8_bytes_without_truncation() -> TestResult {
    for reason in [String::new(), "x".repeat(123), "界".repeat(41)] {
        let frame = CloseFrame::new(1000, reason.clone())?;
        check(frame.reason() == reason, "valid close reason was altered")?;
    }
    for reason in ["x".repeat(124), "界".repeat(42)] {
        let error = CloseFrame::new(1000, reason)
            .err()
            .ok_or("oversized UTF-8 reason was accepted")?;
        check(
            error.kind() == ErrorKind::InvalidInput
                && error
                    .config_error()
                    .is_some_and(|detail| detail.field() == "close.reason"),
            "oversized reason lacks field information",
        )?;
    }
    Ok(())
}

#[test]
fn peer_close_exposes_observed_fields_but_redacts_debug() -> TestResult {
    let peer = PeerClose {
        code: Some(4001),
        reason: "peer-payload-secret".to_owned(),
    };
    let cloned = peer.clone();
    let output = format!("{peer:?}");
    check(
        cloned.code == Some(4001) && cloned.reason == "peer-payload-secret",
        "peer close fields were lost",
    )?;
    check(
        output.contains("4001")
            && output.contains(&peer.reason.len().to_string())
            && !output.contains("peer-payload-secret"),
        "peer close Debug must expose only code and length",
    )?;
    let absent = PeerClose {
        code: None,
        reason: String::new(),
    };
    check(
        absent.code.is_none() && absent.reason.is_empty(),
        "absent close code must remain distinct",
    )
}

#[test]
fn decoded_peer_close_owns_full_utf8_reason_after_frame_is_dropped() -> TestResult {
    use tokio_tungstenite::tungstenite::protocol::{
        frame::coding::CloseCode, CloseFrame as WireCloseFrame,
    };

    let absent = PeerClose::from_frame(None)?;
    check(
        absent.code.is_none() && absent.reason.is_empty(),
        "empty peer frame gained a synthetic code or reason",
    )?;
    for reason in [String::new(), "x".repeat(123), "界".repeat(41)] {
        let frame = WireCloseFrame {
            code: CloseCode::Library(4001),
            reason: reason.clone().into(),
        };
        let close = PeerClose::from_frame(Some(&frame))?;
        drop(frame);
        let copied = close.clone();
        drop(close);
        check(
            copied.code == Some(4001) && copied.reason == reason,
            "owned peer close lost its decoded code or UTF-8 reason",
        )?;
    }
    Ok(())
}

#[test]
fn decoded_peer_close_rejects_oversized_internal_frames_without_truncation() -> TestResult {
    use tokio_tungstenite::tungstenite::protocol::{
        frame::coding::CloseCode, CloseFrame as WireCloseFrame,
    };

    for reason in ["x".repeat(124), "界".repeat(42)] {
        let frame = WireCloseFrame {
            code: CloseCode::Normal,
            reason: reason.into(),
        };
        let error = PeerClose::from_frame(Some(&frame))
            .err()
            .ok_or("oversized internal peer close was accepted")?;
        check(
            error.kind() == ErrorKind::Protocol,
            "malformed peer frame must remain a protocol failure",
        )?;
    }
    Ok(())
}

use super::{IoEndKind, PeerClose};
use crate::module::ws_client::test_support::{check, check_eq, TestResult};
use tokio_tungstenite::tungstenite::protocol::{frame::coding::CloseCode, CloseFrame};
fn requires_copy<T: Copy>() {}
fn requires_clone<T: Clone>() {}

#[test]
fn peer_close_preserves_empty_frame_and_maximum_utf8_reason_without_debug_leak() -> TestResult {
    requires_clone::<PeerClose>();
    requires_clone::<crate::ws::ConnectionEvent>();
    requires_copy::<IoEndKind>();
    let empty = PeerClose::from_frame(None)?;
    check_eq!(empty.code, None)?;
    check_eq!(empty.reason, "")?;

    let reason = "界".repeat(41);
    let frame = CloseFrame {
        code: CloseCode::Library(4001),
        reason: reason.clone().into(),
    };
    let close = PeerClose::from_frame(Some(&frame))?;
    check_eq!(close.code, Some(4001))?;
    check_eq!(close.reason, reason)?;
    let debug = format!("{close:?}");
    check!(!debug.contains('界'))?;
    check!(debug.contains("4001") && debug.contains("123"))?;

    let no_reason = CloseFrame {
        code: CloseCode::Normal,
        reason: "".into(),
    };
    let close = PeerClose::from_frame(Some(&no_reason))?;
    check_eq!(close.code, Some(1000))?;
    check_eq!(close.reason, "")?;
    Ok(())
}

#[test]
fn peer_close_rejects_oversized_internal_frames_instead_of_truncating_utf8() -> TestResult {
    let frame = CloseFrame {
        code: CloseCode::Normal,
        reason: "界".repeat(42).into(),
    };
    check!(PeerClose::from_frame(Some(&frame)).is_err())?;
    Ok(())
}

use crate::error::{ErrorKind, ErrorStage};
use crate::{NetError, Result};
use std::fmt;

/// Validated outbound close frame with a UTF-8 reason of at most 123 bytes.
#[derive(Clone, Debug)]
pub struct CloseFrame {
    /// WebSocket wire close code validated by the constructor.
    code: u16,
    /// Reason text sent with the close frame; may be empty.
    reason: String,
}

impl CloseFrame {
    /// Validates a wire close code and UTF-8 reason.
    pub fn new(code: u16, reason: impl Into<String>) -> Result<Self> {
        // RFC 6455 §§5.5/7.4 and the IANA WebSocket Close Code registry.
        if !matches!(code, 1000..=1003 | 1007..=1014 | 3000..=4999) {
            return Err(NetError::input(
                "close.code",
                "must be an allowed WebSocket wire close code",
            )
            .with_stage(ErrorStage::Close));
        }
        let reason = reason.into();
        if reason.len() > 123 {
            return Err(
                NetError::input("close.reason", "must be at most 123 UTF-8 bytes")
                    .with_stage(ErrorStage::Close),
            );
        }
        Ok(Self { code, reason })
    }

    /// Returns the validated close code.
    pub fn code(&self) -> u16 {
        self.code
    }

    /// Borrows the close reason.
    pub fn reason(&self) -> &str {
        &self.reason
    }

    pub(crate) fn into_wire(self) -> tokio_tungstenite::tungstenite::protocol::CloseFrame {
        use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
        // Tungstenite 0.30 maps registered code 1014 to Bad. Preserve its valid
        // numeric wire value using the library's explicit IANA-code variant.
        let code = if self.code == 1014 {
            CloseCode::Iana(self.code)
        } else {
            CloseCode::from(self.code)
        };
        tokio_tungstenite::tungstenite::protocol::CloseFrame {
            code,
            reason: self.reason.into(),
        }
    }
}

/// Peer close information observed by the receiver, including code-less closes.
#[derive(Clone)]
pub struct PeerClose {
    /// Decoded close code; `None` for an empty close frame.
    pub code: Option<u16>,
    /// Complete decoded reason; empty for an empty close frame.
    pub reason: String,
}

impl PeerClose {
    /// Own the decoder's complete UTF-8 reason; an empty Close has no code.
    /// Tungstenite may already have replaced an invalid wire code with its
    /// protocol-error Close before this observation reaches the SDK.
    pub(crate) fn from_frame(
        frame: Option<&tokio_tungstenite::tungstenite::protocol::CloseFrame>,
    ) -> Result<Self> {
        let Some(frame) = frame else {
            return Ok(Self {
                code: None,
                reason: String::new(),
            });
        };
        if frame.reason.len() > 123 {
            // Real network frames are checked by the decoder. Reject malformed
            // internal streams as well, without truncating a UTF-8 sequence.
            return Err(NetError::from(ErrorKind::Protocol).with_stage(ErrorStage::Close));
        }
        let mut reason = String::new();
        reason
            .try_reserve_exact(frame.reason.len())
            .map_err(|error| {
                NetError::with_source(ErrorKind::ResourceExhausted, error)
                    .with_stage(ErrorStage::Close)
            })?;
        reason.push_str(frame.reason.as_str());
        Ok(Self {
            code: Some(u16::from(frame.code)),
            reason,
        })
    }
}

impl fmt::Debug for PeerClose {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PeerClose")
            .field("code", &self.code)
            .field("reason_len", &self.reason.len())
            .finish()
    }
}

/// I/O notification category that ended a physical connection.
/// An observed peer close is retained even if the close reply write fails.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IoEndKind {
    /// A WebSocket close frame was received from the peer.
    PeerClose,
    /// The stream ended before the expected close handshake completed.
    UnexpectedEof,
    /// The underlying connection was reset.
    ConnectionReset,
    /// A WebSocket protocol error occurred during I/O.
    ProtocolError,
    /// Another I/O, heartbeat, timeout, or internal-channel failure occurred.
    Other,
}

#[cfg(test)]
#[path = "close_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "peer_close_tests.rs"]
mod peer_tests;

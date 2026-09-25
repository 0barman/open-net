use super::{ClientId, ConnectionId, Message, PeerClose, SessionId};
use bytes::Bytes;
use std::sync::Arc;
use std::sync::Weak;
use std::time::SystemTime;

/// Received business message or protocol-control payload.
#[derive(Clone, Debug)]
pub enum IncomingPayload {
    /// Text or binary business message.
    Message(
        /// Owned business body.
        Message,
    ),
    /// Ping control frame sent by the peer.
    Ping(
        /// Raw bytes carried by the Ping frame.
        Bytes,
    ),
    /// Pong control frame sent by the peer.
    Pong(
        /// Raw bytes carried by the Pong frame.
        Bytes,
    ),
    /// Close control frame sent by the peer.
    Close(
        /// Decoded close code and reason.
        PeerClose,
    ),
}

/// Source identity assigned by the receiving connection; owns no session resources.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct IncomingOrigin {
    /// Client identity that received the item.
    client_id: ClientId,
    /// Logical session identity that received the item.
    session_id: SessionId,
    /// Physical connection that received the item.
    connection_id: ConnectionId,
}

impl IncomingOrigin {
    pub(crate) fn new(
        client_id: ClientId,
        session_id: SessionId,
        connection_id: ConnectionId,
    ) -> Self {
        Self {
            client_id,
            session_id,
            connection_id,
        }
    }
}

/// Immutable inbound item; clones share one payload allocation.
#[derive(Clone, Debug)]
pub struct IncomingMessage {
    /// Client, session, and physical connection that produced the item.
    origin: IncomingOrigin,
    /// Business or control payload shared by all clones.
    payload: Arc<IncomingPayload>,
    /// System time recorded when the SDK received the item.
    received_at: SystemTime,
    /// Response-dispatch identity, or `None` before request dispatch binding.
    dispatch: Option<IncomingDispatch>,
}

/// Response-dispatch identity attached to an inbound item for source and validity checks.
#[derive(Clone, Debug)]
pub(crate) struct IncomingDispatch {
    /// Weak reference to the request table; does not extend table or session lifetime.
    pub(crate) pending: Weak<crate::module::ws_client::native_pending::NativePending>,
    /// Identifier allocated by that table for this inbound dispatch.
    pub(crate) id: u64,
}

impl IncomingMessage {
    pub(crate) fn new(
        origin: IncomingOrigin,
        payload: IncomingPayload,
        received_at: SystemTime,
    ) -> Self {
        Self {
            origin,
            payload: Arc::new(payload),
            received_at,
            dispatch: None,
        }
    }

    pub(crate) fn bind_dispatch(
        &mut self,
        pending: Weak<crate::module::ws_client::native_pending::NativePending>,
        id: u64,
    ) {
        self.dispatch = Some(IncomingDispatch { pending, id });
    }

    pub(crate) fn dispatch(&self) -> Option<&IncomingDispatch> {
        self.dispatch.as_ref()
    }

    /// Borrows the complete inbound payload, including control frames.
    pub fn payload(&self) -> &IncomingPayload {
        self.payload.as_ref()
    }

    /// Returns the business message, or `None` for control frames.
    pub fn message(&self) -> Option<&Message> {
        match self.payload() {
            IncomingPayload::Message(message) => Some(message),
            IncomingPayload::Ping(_) | IncomingPayload::Pong(_) | IncomingPayload::Close(_) => None,
        }
    }

    /// Returns the SDK receive timestamp.
    pub fn received_at(&self) -> SystemTime {
        self.received_at
    }

    /// Returns the originating client identity.
    pub fn client_id(&self) -> ClientId {
        self.origin.client_id
    }

    /// Returns the originating logical session identity.
    pub fn session_id(&self) -> SessionId {
        self.origin.session_id
    }

    /// Returns the originating physical connection identity.
    pub fn connection_id(&self) -> ConnectionId {
        self.origin.connection_id
    }
}

#[cfg(test)]
#[path = "incoming_tests.rs"]
mod tests;

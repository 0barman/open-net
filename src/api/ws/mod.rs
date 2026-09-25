//! WebSocket session API and owned value types.
mod identity;
pub use crate::subscription::SubscriptionId;
pub use identity::{AttemptId, ClientId, ConnectionId, CycleId, OperationId, SessionId};
mod close;
mod incoming;
mod message;
mod request;
mod request_id;
mod response;
mod value_limits;
pub use close::{CloseFrame, IoEndKind, PeerClose};
pub(crate) use incoming::IncomingOrigin;
pub use incoming::{IncomingMessage, IncomingPayload};
pub use message::Message;
pub use request::Request;
pub use request_id::RequestId;
pub use response::Response;

mod config;
pub use config::{
    DispatchLimits, FrameConfig, HeartbeatConfig, QueueLimit, QueueLimits, RequestLimits,
    TcpConfig, TcpKeepaliveConfig, WebSocketClientConfig,
};

mod reconnect;
pub use reconnect::{BackoffConfig, ReconnectPolicy};

mod receive_options;
pub use receive_options::{EventOptions, ReceiveOptions, ReceiveOverflow, TaskEventOptions};

mod handshake_diagnostic;
pub use handshake_diagnostic::{
    HandshakeBodyCaptureState, HandshakeDiagnostic, HandshakeDiagnosticKind,
    HandshakeDiagnosticOptions,
};

mod handshake;
pub(crate) use handshake::validate_headers;
pub use handshake::{HandshakeAttempt, HandshakeHeaders, HandshakeProvider};
pub(crate) use value_limits::validate_metadata;

mod connection;
pub(crate) use connection::MAX_CONNECTION_EVENT_BYTES;
pub use connection::{
    ConnectionEnd, ConnectionEvent, ConnectionEventKind, ConnectionInfo, ConnectionSnapshot,
    ConnectionState, JournalOptions, RetryDecision, SessionEnd, TerminationReason,
};

/// Receiver for retained connection lifecycle events.
pub type ConnectionJournal = crate::subscription::EventReceiver<ConnectionEvent>;
/// Receiver for live connection lifecycle events.
pub type ConnectionEvents = crate::subscription::EventReceiver<ConnectionEvent>;

pub(crate) mod cancellation;
pub use cancellation::{CancellationGroup, CancellationGuard};

mod operation;
pub use operation::{
    DeliveryEvidence, OperationPhase, OperationSnapshot, TaskSuccess, TerminationOutcome,
    WriteOutcome,
};

mod options;
pub use options::{
    DisconnectedPolicy, MessageLane, Priority, RequestOptions, ResponseTimeoutOrigin, SendOptions,
    SendRetryPolicy,
};

mod task;
pub use task::{TaskEndCause, TaskEvent, TaskEvents, TaskSource};

pub(crate) mod request_control;
pub use request_control::{RequestHandle, RequestRegistration, RequestSnapshot};
mod routing;
pub use routing::{
    ResolveOutcome, ResponseProtocol, ResponseResolver, ResponseRoute, ResponseRouting,
};
mod connect_options;
pub use connect_options::{ConnectOptions, InitialMessages};

/// Receiver for incoming WebSocket messages.
pub type MessageReceiver = crate::subscription::EventReceiver<IncomingMessage>;

mod dispatch;
mod send;
pub use send::{MessageBuilder, MessageReceipt, PreparedMessage, Sender};
mod request_client;
pub use request_client::{PreparedRequest, RequestBuilder, RequestClient, RequestReceipt};
mod client;
pub use client::WebSocketClient;
mod session;
pub use session::Session;

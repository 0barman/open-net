use on_common::log::log_def::LogType;
pub mod pending_request_completion;
pub mod pending_request_entry;
pub mod pending_request_info;
pub mod pending_request_status;
pub mod pending_request_view;
pub mod prepared_request;
pub mod queued_request_completion;
pub mod reconnect_policy;
pub mod request_registration;
pub mod request_scope;
pub mod web_socket_client_config;
pub mod web_socket_connect_options;
pub mod web_socket_connection_event;
pub mod web_socket_connection_events;
pub mod web_socket_context_connect_options;
pub mod web_socket_handshake_context;
pub mod web_socket_request_options;
pub mod web_socket_task_event;
pub mod wsc_response;

use std::time::{Duration, Instant};

/// 判断相对时长能否表示为 Tokio 或标准库的绝对截止时间。
///
/// Tokio 的 [`tokio::time::Instant`] 封装了 [`std::time::Instant`]，因此本检查无需运行时，
/// 既适用于同步调用 `try_send*` 的场景，也适用于异步流程。
pub(crate) fn duration_fits_instant(duration: Duration) -> bool {
    on_common::log_t!(LogType::WSC; "duration_fits_instant", "duration", format!("{:?}", duration));
    let fits = Instant::now().checked_add(duration).is_some();
    if !fits {
        on_common::log_e!(LogType::WSC; "duration_fits_instant", "duration|error", format!("{:?}", duration), "unrepresentable_deadline");
    }
    fits
}

pub use pending_request_completion::PendingRequestCompletion;
pub use prepared_request::PreparedRequest;
pub use queued_request_completion::QueuedRequestCompletion;
pub use request_registration::{
    RequestRegistration, RequestRegistrationToken, RequestTerminationOutcome,
};
pub use request_scope::RequestScope;
pub use tokio_tungstenite::tungstenite::Message as WebSocketMessage;
pub use web_socket_connection_event::{
    WebSocketConnectStage, WebSocketConnectionEvent, WebSocketConnectionEventKind,
    WebSocketConnectionFailure, WebSocketTerminationReason,
};
pub use web_socket_connection_events::WebSocketConnectionEvents;
pub use web_socket_context_connect_options::WebSocketContextConnectOptions;
pub use web_socket_handshake_context::{
    WebSocketHandshakeAttempt, WebSocketHandshakeProvider, WebSocketHandshakeSnapshot,
};
pub use web_socket_request_options::{ResponseDeadlineOrigin, WebSocketRequestOptions};
pub use web_socket_task_event::{
    WebSocketTaskDelivery, WebSocketTaskEndCause, WebSocketTaskEvent, WebSocketTaskEventOptions,
    WebSocketTaskPhase, WebSocketTaskSource, WebSocketTaskSuccess,
};
pub use wsc_response::{PendingRequestInfo, PendingRequestStatus, PendingRequestView, WSCResponse};

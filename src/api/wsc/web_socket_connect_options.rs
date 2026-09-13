use crate::api::wsc::reconnect_policy::ReconnectPolicy;
use crate::WebSocketHeaderProvider;

#[derive(Clone, Default)]
pub struct WebSocketConnectOptions {
    pub headers: Vec<(String, String)>,
    pub header_provider: Option<WebSocketHeaderProvider>,
    pub reconnect: ReconnectPolicy,
}

/// Worker-local lifecycle state; public observation uses `ws::ConnectionState`.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum ConnectionStatus {
    Idle,
    Connecting,
    Connected,
    Reconnecting,
    Closing,
    Disconnected,
    Closed,
}

/// Origin of a log record. Existing numeric values are stable.
#[repr(i32)]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum LogType {
    /// No origin or an unknown origin.
    None = 0,
    /// Database subsystem.
    Database = 1,
    /// Engine lifecycle and scheduling.
    Engine = 2,
    /// WebSocket server subsystem.
    WSS = 3,
    /// WebSocket client subsystem.
    WSC = 4,
    /// Shared infrastructure.
    Common = 5,
    /// HTTP client subsystem.
    HTTP = 6,
}

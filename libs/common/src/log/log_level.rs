use serde::{Deserialize, Serialize};

#[repr(i32)]
#[derive(Copy, Clone, Debug, Deserialize, PartialEq, Serialize, Eq, PartialOrd, Default)]
/// Severity level for SDK log records.
pub enum LogLevel {
    /// Disable log output.
    None = 0,
    /// Error-level log entry.
    Error = 1,
    /// Warning-level log entry.
    Warn = 2,
    Key = 3,
    /// Informational log entry.
    #[default]
    Info = 4,
    /// Debug-level log entry.
    Debug = 5,
}

impl From<LogLevel> for i32 {
    fn from(val: LogLevel) -> Self {
        val as i32
    }
}

impl From<i32> for LogLevel {
    fn from(value: i32) -> Self {
        if value == 0 {
            LogLevel::None
        } else if value == 1 {
            LogLevel::Error
        } else if value == 2 {
            LogLevel::Warn
        } else if value == 3 {
            LogLevel::Key
        } else if value == 4 {
            LogLevel::Info
        } else if value == 5 {
            LogLevel::Debug
        } else {
            LogLevel::None
        }
    }
}

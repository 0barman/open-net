use super::LogLevel;
use super::LogType;

/// Structured log record delivered to logging listeners.
#[derive(Debug, Clone)]
pub struct LogInfo {
    /// Structured origin used to route records; never inferred from the tag.
    pub log_type: LogType,
    /// Source file, line and column. Synthetic overflow records use an empty location.
    pub location: String,
    /// Log severity.
    pub level: LogLevel,
    /// Operation label, kept exactly as supplied (for example, `start_session`).
    /// No origin, SDK prefix or severity suffix is added.
    pub tag: String,
    /// Log payload, often JSON formatted by the exported log macros.
    pub content: String,
    /// Creation timestamp in Unix milliseconds.
    pub create_time: i64,
}

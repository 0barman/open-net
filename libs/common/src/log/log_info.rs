use crate::log::log_def::LogType;
use crate::log::log_level::LogLevel;

#[derive(Debug, Clone)]
pub struct LogInfo {
    /// Structured origin used to route records; never inferred from the tag.
    pub log_type: LogType,
    /// Source file, line and column. Synthetic overflow records use an empty location.
    pub location: String,
    /// Log severity.
    pub level: LogLevel,
    /// Log tag identifying the subsystem or operation.
    pub tag: String,
    /// Log payload, often JSON formatted by the exported log macros.
    pub content: String,
    /// Creation timestamp in Unix milliseconds.
    pub create_time: i64,
}

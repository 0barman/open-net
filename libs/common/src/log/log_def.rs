use crate::log::log_info::LogInfo;
use crate::log::log_level::LogLevel;
use crate::log::logger::Logger;
use indexmap::IndexMap;
use serde_json::Value;

pub static LOG_PREFIX: &str = "ON";
pub static DESC: &str = "desc";

/// Origin of a log record. Existing numeric values are stable.
#[repr(i32)]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum LogType {
    None = 0,
    Database = 1,
    Engine = 2,
    WSS = 3,
    WSC = 4,
    Common = 5,
    HTTP = 6,
}

/// Builds JSON content from pipe-separated field names and serializable values.
/// Logging internals deliberately do not emit logs through their own pipeline.
pub fn create_log_content(first_field: Option<&str>, values: Option<Vec<Value>>) -> String {
    let Some(first_field) = first_field else {
        return "{}".to_string();
    };
    let map: IndexMap<_, _> = first_field
        .split('|')
        .zip(values.unwrap_or_default())
        .collect();
    serde_json::to_string(&map).unwrap_or_else(|_| "{}".to_string())
}

pub(crate) fn format_tag(log_type: LogType, tag: &str, suffix: &str) -> String {
    let prefix = match log_type {
        LogType::None | LogType::Engine => LOG_PREFIX,
        LogType::Database => "ON_DB",
        LogType::WSS => "ON_WSS",
        LogType::WSC => "ON_WSC",
        LogType::Common => "ON_COMMON",
        LogType::HTTP => "ON_HTTP",
    };
    format!("{prefix}-{tag}-{suffix}")
}

pub(crate) fn timestamp_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

/// Queues a structured record for matching subscriptions without writing stdout,
/// files, or a database. User callbacks execute on their subscription's thread.
#[allow(clippy::too_many_arguments)]
pub fn on_log(
    location: String,
    level: LogLevel,
    log_type: LogType,
    tag: &str,
    first_field: Option<&str>,
    values: Option<Vec<Value>>,
    suffix: &str,
) {
    if !Logger::is_enabled(log_type) {
        return;
    }
    Logger::dispatch(LogInfo {
        log_type,
        location,
        level,
        tag: format_tag(log_type, tag, suffix),
        content: create_log_content(first_field, values),
        create_time: timestamp_millis(),
    });
}

#[doc(hidden)]
#[macro_export]
macro_rules! location {
    () => {{
        format!("{}:{}:{}", file!(), line!(), column!())
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! internal_log {
    ($log_type:expr, $level:expr, $suffix:expr, $tag:expr, $first_field:expr $(, $value:expr)* $(,)?) => {{
        let log_type = $log_type;
        if $crate::log::logger::Logger::is_enabled(log_type) {
            let values = vec![$($crate::__serde_json::json!($value)),*];
            $crate::log::log_def::on_log(
                $crate::location!(), $level, log_type, $tag,
                Some($first_field), Some(values), $suffix,
            );
        }
    }};
    ($log_type:expr, $level:expr, $suffix:expr, $tag:expr $(,)?) => {{
        let log_type = $log_type;
        if $crate::log::logger::Logger::is_enabled(log_type) {
            $crate::log::log_def::on_log(
                $crate::location!(), $level, log_type, $tag, None, None, $suffix,
            );
        }
    }};
}

/// Emits a T record. Use `LogType::WSC; "function", "a|b", a, b` for a typed record.
/// The legacy form without a type uses `LogType::Engine`.
#[macro_export]
macro_rules! log_t {
    ($log_type:expr; $($arg:expr),+ $(,)?) => {
        $crate::internal_log!($log_type, $crate::log::log_level::LogLevel::Info, "T", $($arg),+)
    };
    ($($arg:expr),+ $(,)?) => {
        $crate::internal_log!($crate::log::log_def::LogType::Engine, $crate::log::log_level::LogLevel::Info, "T", $($arg),+)
    };
}

/// Emits a R record. Use `LogType::WSC; "function", "a|b", a, b` for a typed record.
/// The legacy form without a type uses `LogType::Engine`.
#[macro_export]
macro_rules! log_r {
    ($log_type:expr; $($arg:expr),+ $(,)?) => {
        $crate::internal_log!($log_type, $crate::log::log_level::LogLevel::Debug, "R", $($arg),+)
    };
    ($($arg:expr),+ $(,)?) => {
        $crate::internal_log!($crate::log::log_def::LogType::Engine, $crate::log::log_level::LogLevel::Debug, "R", $($arg),+)
    };
}

/// Emits a S record. Use `LogType::WSC; "function", "a|b", a, b` for a typed record.
/// The legacy form without a type uses `LogType::Engine`.
#[macro_export]
macro_rules! log_s {
    ($log_type:expr; $($arg:expr),+ $(,)?) => {
        $crate::internal_log!($log_type, $crate::log::log_level::LogLevel::Debug, "S", $($arg),+)
    };
    ($($arg:expr),+ $(,)?) => {
        $crate::internal_log!($crate::log::log_def::LogType::Engine, $crate::log::log_level::LogLevel::Debug, "S", $($arg),+)
    };
}

/// Emits a E record. Use `LogType::WSC; "function", "a|b", a, b` for a typed record.
/// The legacy form without a type uses `LogType::Engine`.
#[macro_export]
macro_rules! log_e {
    ($log_type:expr; $($arg:expr),+ $(,)?) => {
        $crate::internal_log!($log_type, $crate::log::log_level::LogLevel::Error, "E", $($arg),+)
    };
    ($($arg:expr),+ $(,)?) => {
        $crate::internal_log!($crate::log::log_def::LogType::Engine, $crate::log::log_level::LogLevel::Error, "E", $($arg),+)
    };
}

#[macro_export]
macro_rules! array_to_json_string {
    ($vec:expr) => {{
        let cloned_vec = $vec.clone();

        match $crate::__serde_json::to_string(&cloned_vec) {
            Ok(json) => json,
            Err(e) => format!("JSON serialization failed: {}", e),
        }
    }};
}

/// Converts displayable objects to a JSON-array-like string using `Display`.
#[macro_export]
macro_rules! obj_array_to_json_string {
    ($vec:expr) => {{
        format!(
            "[{}]",
            $vec.iter()
                .map(|x| x.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }};
}

/// Converts a string-keyed map of basic serializable values to JSON.
#[macro_export]
macro_rules! basic_type_map_to_json_string {
    ($map:expr) => {{
        use std::collections::HashMap;

        let cloned_map: HashMap<String, _> = $map.clone();

        (|| -> String {
            match $crate::__serde_json::to_string(&cloned_map) {
                Ok(json) => json,
                Err(e) => format!("JSON serialization failed: {}", e),
            }
        })()
    }};
}

/// Implements `Display` for serializable types by rendering JSON.
#[macro_export]
macro_rules! impl_display_json {
    ($($struct:ty),*) => {
        $(
            impl std::fmt::Display for $struct {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    match $crate::__serde_json::to_string(self) {
                        Ok(json) => write!(f, "{}", json),
                        Err(e) => write!(f, "Serialization error: {}", e),
                    }
                }
            }
        )*
    };
}

#[macro_export]
macro_rules! err {
    ($err:expr) => {{
        let error = $err;
        $crate::log_e!("err", "error", $crate::log::summary::error(&error));
        error
    }};
}

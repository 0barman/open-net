use crate::common::log::log_info::LogInfo;
use crate::common::log::log_level::LogLevel;
use crate::common::log::logger::OpenNetLogger;
use serde::ser::SerializeMap;
use serde::{Serialize, Serializer};
use serde_json::Value;
use std::collections::{hash_map::Entry, HashMap};

pub static DESC: &str = "desc";

pub(crate) use crate::api::log::LogType;

struct LogFields<'a>(Vec<(&'a str, Value)>);

impl Serialize for LogFields<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (field, value) in &self.0 {
            map.serialize_entry(field, value)?;
        }
        map.end()
    }
}

/// Builds JSON content from pipe-separated field names and serializable values.
/// Repeated names keep their first position and use their last paired value.
/// Logging internals deliberately do not emit logs through their own pipeline.
pub fn create_log_content(first_field: Option<&str>, values: Option<Vec<Value>>) -> String {
    let Some(first_field) = first_field else {
        return "{}".to_string();
    };
    let mut fields: Vec<(&str, Value)> = Vec::new();
    let mut positions: HashMap<&str, usize> = HashMap::new();
    for (field, value) in first_field.split('|').zip(values.unwrap_or_default()) {
        match positions.entry(field) {
            Entry::Occupied(position) => fields[*position.get()].1 = value,
            Entry::Vacant(position) => {
                position.insert(fields.len());
                fields.push((field, value));
            }
        }
    }
    serde_json::to_string(&LogFields(fields)).unwrap_or_else(|_| "{}".to_string())
}

#[cfg(test)]
#[path = "log_content_tests.rs"]
mod log_content_tests;

pub(crate) fn timestamp_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

/// Queues a structured record for matching subscriptions without writing stdout,
/// files, or a database. User callbacks execute on their subscription's thread.
pub fn on_log(
    location: String,
    level: LogLevel,
    log_type: LogType,
    tag: &str,
    first_field: Option<&str>,
    values: Option<Vec<Value>>,
) {
    if !OpenNetLogger::is_enabled_for(log_type, level) {
        return;
    }
    crate::common::log::logger::dispatch(LogInfo {
        log_type,
        location,
        level,
        tag: tag.to_owned(),
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
    ($log_type:expr, $level:expr, $tag:expr, $first_field:expr $(, $value:expr)* $(,)?) => {{
        let log_type = $log_type;
        if $crate::OpenNetLogger::is_enabled(log_type) {
            let level = $level;
            if $crate::OpenNetLogger::is_enabled_for(log_type, level) {
                let values = vec![$($crate::__serde_json::json!($value)),*];
                $crate::__log::on_log(
                    $crate::location!(), level, log_type, $tag,
                    Some($first_field), Some(values),
                );
            }
        }
    }};
    ($log_type:expr, $level:expr, $tag:expr $(,)?) => {{
        let log_type = $log_type;
        if $crate::OpenNetLogger::is_enabled(log_type) {
            let level = $level;
            if $crate::OpenNetLogger::is_enabled_for(log_type, level) {
                $crate::__log::on_log(
                    $crate::location!(), level, log_type, $tag, None, None,
                );
            }
        }
    }};
}

/// Emits an Info record. Use `LogType::WSC; "function", "a|b", a, b` for a typed record.
/// The legacy form without a type uses `LogType::Engine`.
#[macro_export]
macro_rules! log_t {
    ($log_type:expr; $($arg:expr),+ $(,)?) => {
        $crate::internal_log!($log_type, $crate::LogLevel::Info, $($arg),+)
    };
    ($($arg:expr),+ $(,)?) => {
        $crate::internal_log!($crate::LogType::Engine, $crate::LogLevel::Info, $($arg),+)
    };
}

/// Emits a Debug record. Use `LogType::WSC; "function", "a|b", a, b` for a typed record.
/// The legacy form without a type uses `LogType::Engine`.
#[macro_export]
macro_rules! log_r {
    ($log_type:expr; $($arg:expr),+ $(,)?) => {
        $crate::internal_log!($log_type, $crate::LogLevel::Debug, $($arg),+)
    };
    ($($arg:expr),+ $(,)?) => {
        $crate::internal_log!($crate::LogType::Engine, $crate::LogLevel::Debug, $($arg),+)
    };
}

/// Emits a Debug record. Use `LogType::WSC; "function", "a|b", a, b` for a typed record.
/// The legacy form without a type uses `LogType::Engine`.
/// Use `LogType::WSC, LogLevel::Key; "function"` for a key lifecycle record.
#[macro_export]
macro_rules! log_s {
    ($log_type:expr, $level:expr; $($arg:expr),+ $(,)?) => {
        $crate::internal_log!($log_type, $level, $($arg),+)
    };
    ($log_type:expr; $($arg:expr),+ $(,)?) => {
        $crate::internal_log!($log_type, $crate::LogLevel::Debug, $($arg),+)
    };
    ($($arg:expr),+ $(,)?) => {
        $crate::internal_log!($crate::LogType::Engine, $crate::LogLevel::Debug, $($arg),+)
    };
}

/// Emits an Error record. Use `LogType::WSC; "function", "a|b", a, b` for a typed record.
/// The legacy form without a type uses `LogType::Engine`.
#[macro_export]
macro_rules! log_e {
    ($log_type:expr; $($arg:expr),+ $(,)?) => {
        $crate::internal_log!($log_type, $crate::LogLevel::Error, $($arg),+)
    };
    ($($arg:expr),+ $(,)?) => {
        $crate::internal_log!($crate::LogType::Engine, $crate::LogLevel::Error, $($arg),+)
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
        $crate::log_e!("err", "error", $crate::__log::error_summary(&error));
        error
    }};
}

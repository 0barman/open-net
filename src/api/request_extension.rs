use crate::common::log::log_def::LogType;
use std::collections::HashMap;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RequestExtension {
    ext_map: HashMap<String, String>,
}

impl RequestExtension {
    pub fn new() -> Self {
        crate::log_t!(LogType::Engine; "new");
        Self::default()
    }

    pub fn insert(&mut self, key: impl Into<String>, value: impl Into<String>) -> Option<String> {
        crate::log_t!(LogType::Engine; "insert");
        self.ext_map.insert(key.into(), value.into())
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        crate::log_t!(LogType::Engine; "get", "key_bytes", key.len());
        self.ext_map.get(key).map(String::as_str)
    }

    pub fn as_map(&self) -> &HashMap<String, String> {
        crate::log_t!(LogType::Engine; "as_map");
        &self.ext_map
    }

    pub fn into_map(self) -> HashMap<String, String> {
        crate::log_t!(LogType::Engine; "into_map");
        self.ext_map
    }
}

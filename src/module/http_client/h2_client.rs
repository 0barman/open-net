use crate::common::log::log_def::LogType;
use crate::common::CommonEngine;

pub struct HttpClient {
    pub(crate) fewf: CommonEngine,
}

impl HttpClient {
    pub fn new() -> Self {
        crate::log_t!(LogType::HTTP; "new");
        HttpClient {
            fewf: CommonEngine::new(512, 128).expect("failed to create the HTTP client runtime"),
        }
    }

    pub fn task(&self) {
        crate::log_t!(LogType::HTTP; "task");
    }
}

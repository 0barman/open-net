use on_common::log::log_def::LogType;
use on_common::CommonEngine;

pub struct HttpClient {
    pub(crate) fewf: CommonEngine,
}

impl HttpClient {
    pub fn new() -> Self {
        on_common::log_t!(LogType::HTTP; "new");
        HttpClient {
            fewf: CommonEngine::new(512, 128).expect("failed to create the HTTP client runtime"),
        }
    }

    pub fn task(&self) {
        on_common::log_t!(LogType::HTTP; "task");
    }
}

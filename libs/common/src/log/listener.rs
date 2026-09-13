use crate::common::log::log_info::LogInfo;

pub type LogListener = Box<dyn Fn(LogInfo) + Send + Sync + 'static>;

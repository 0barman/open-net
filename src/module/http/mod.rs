pub(crate) mod http_client_inner;
mod network_observation;

#[cfg(test)]
mod http_worker_tests;
pub(crate) mod request_wait_guard;
pub(crate) mod result_request;
mod retry_after;

pub(crate) mod connect_timeout;
pub(crate) mod http_failure;
pub(crate) mod http_timeout;

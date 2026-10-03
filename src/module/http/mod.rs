pub(crate) mod http_client_inner;
mod network_observation;

#[cfg(test)]
mod http_worker_tests;
pub(crate) mod request_wait_guard;
pub(crate) mod result_request;

pub(crate) mod http_failure;

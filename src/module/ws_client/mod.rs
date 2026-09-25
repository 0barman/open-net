#![forbid(unsafe_code)]
#![deny(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::todo,
    clippy::unimplemented,
    clippy::unreachable
)]

mod active_io;
pub(crate) mod callback_event;
mod callback_executor;
mod client_command;
mod connect_target;
pub(crate) mod connection_history;
#[cfg(test)]
mod connection_history_tests;
pub(crate) mod connection_session;
pub(crate) mod connection_status;
pub(crate) mod data_subscription_executor;
mod heartbeat_schedule;
pub(crate) mod heartbeat_state;
pub(crate) mod io_diagnostics;
pub(crate) mod io_event;
pub(crate) mod listener_executor;
pub(crate) mod listener_store;
pub(crate) mod network_io;
pub mod read;
#[cfg(test)]
pub(crate) mod test_support;
pub mod write;
pub mod ws_client_inner;
pub(crate) mod ws_client_worker;
pub mod ws_read;
pub mod ws_write;

pub(crate) mod native_task_observer;

pub(crate) mod native_pending;
pub(crate) mod operation_control;

pub(crate) mod message_source;

pub(crate) mod session_runtime;

#[cfg(test)]
pub(crate) mod v2_test_support;

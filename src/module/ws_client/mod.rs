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
pub(crate) mod connection_session;
pub(crate) mod heartbeat_state;
pub(crate) mod io_event;
pub(crate) mod listener_store;
pub(crate) mod network_io;
pub mod read;
pub(crate) mod send_admission;
pub(crate) mod task_observer;
#[cfg(test)]
pub(crate) mod test_support;
pub mod write;
pub mod ws_client_inner;
pub(crate) mod ws_client_worker;
pub mod ws_read;
pub mod ws_write;

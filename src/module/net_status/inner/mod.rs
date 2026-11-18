// 网络状态内部实现：组织客户端生命周期、监控运行状态、观测快照及平台刷新提示。
pub(crate) mod inner_net_status_client;
mod monitor_runtime;
mod monitor_state;
pub(crate) mod names;
pub(crate) mod network_status_snapshot;
mod platform;
mod recoverable;
pub(crate) mod refresh;
mod refresh_trigger;
#[cfg(any(target_os = "windows", test))]
mod sample_diagnostics;

mod coordinated_view;
mod network_view;
pub(crate) mod shared;

pub(crate) mod facade;

// 网络状态内部实现：组织客户端生命周期、监控运行状态、观测快照及平台刷新提示。
pub(crate) mod inner_net_status_client;
mod monitor_runtime;
mod monitor_state;
pub(crate) mod network_status_snapshot;
mod platform;
mod refresh_trigger;

mod network_view;
pub(crate) mod shared;

pub(crate) mod facade;

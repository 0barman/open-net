//! 由 [`crate::OpenNet`] 创建并持有的网络状态客户端。

pub use crate::module::net_status::{
    IpStack, NetStatusClient, NetworkStatus, NetworkStatusListener, NetworkStatusListenerHandle,
};

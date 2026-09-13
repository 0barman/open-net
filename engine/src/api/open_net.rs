use crate::api::net_error::NetError;
use crate::api::net_status_client::NetStatusClient;
#[cfg(feature = "ws-client")]
use crate::api::web_socket_client::WebSocketClient;
#[cfg(feature = "ws-client")]
use crate::api::wsc::web_socket_client_config::WebSocketClientConfig;
use crate::inner::net_impl::OpenNetInner;
use on_common::log::log_def::LogType;

pub struct OpenNet {
    #[allow(dead_code)]
    pub(crate) inner: OpenNetInner,
}

impl OpenNet {
    /// 创建引擎，并为其 WebSocket 客户端设置不可变的默认网络配置。
    ///
    /// 创建任何工作任务前会先校验代理和 TLS 配置。该配置目前仅适用于 WebSocket
    /// 客户端，此接口不提供 HTTP 请求执行器。
    /// 可通过 [`Self::create_ws_client_with_network_config`] 为单个客户端覆盖默认配置。
    #[cfg(feature = "ws-client")]
    pub fn new_with_network_config(
        config: crate::api::network_config::NetworkConfig,
    ) -> Result<Self, NetError> {
        Ok(Self {
            inner: OpenNetInner::new_with_network_config(config)?,
        })
    }

    pub fn new() -> Result<Self, NetError> {
        on_common::log_t!(LogType::Engine; "new");
        let result: Result<Self, NetError> = (|| {
            Ok(Self {
                inner: OpenNetInner::new()?,
            })
        })();
        result.inspect_err(|error| {
            on_common::log_e!(LogType::Engine; "new", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    /// 使用默认 WebSocket 参数和引擎默认网络配置创建客户端，等待工作任务就绪后返回。
    ///
    /// 返回的客户端尚未连接服务器，需要继续调用 `connect` 或 `connect_with_options`。
    #[cfg(feature = "ws-client")]
    pub async fn create_ws_client(&self, thread_name: &str) -> Result<WebSocketClient, NetError> {
        on_common::log_t!(LogType::WSC; "create_ws_client", "thread_name", thread_name);
        let result: Result<WebSocketClient, NetError> = async {
            self.create_ws_client_with_config(thread_name, WebSocketClientConfig::default())
                .await
        }
        .await;
        result.inspect_err(|error| {
            on_common::log_e!(LogType::WSC; "create_ws_client", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    /// 使用指定 WebSocket 参数和引擎默认网络配置创建客户端，等待工作任务就绪后返回。
    ///
    /// 取消等待不会取消已经提交的创建任务；创建成功后仍可通过 `get_ws_client` 获取，
    /// 并通过 `destroy_ws_client` 销毁客户端。
    #[cfg(feature = "ws-client")]
    pub async fn create_ws_client_with_config(
        &self,
        thread_name: &str,
        config: WebSocketClientConfig,
    ) -> Result<WebSocketClient, NetError> {
        on_common::log_t!(LogType::WSC; "create_ws_client_with_config", "thread_name|config", thread_name, format!("{:?}", config));
        let result: Result<WebSocketClient, NetError> = async {
            let thread_name = thread_name.trim();
            if thread_name.is_empty() {
                return Err(NetError::ParameterEmpty);
            }
            self.inner
                .create_ws_client(thread_name.to_string(), config, None)
                .await
        }
        .await;
        result.inspect_err(|error| {
            on_common::log_e!(LogType::WSC; "create_ws_client_with_config", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    /// 创建 WebSocket 客户端，并为其单独设置不可变的代理和 TLS 策略。
    ///
    /// 传入的网络策略会完整替换引擎默认配置。传入 `NetworkConfig::default()`
    /// 表示明确使用直连和 WebPKI 根证书。
    /// 两种连接接口、重试和重连均使用该客户端选定的策略。
    /// 如需仅修改代理并保留默认 TLS 设置，应克隆原始网络配置，修改代理后再传入。
    ///
    /// 此方法等待工作任务就绪，返回时尚未建立网络连接。创建任务提交后，丢弃返回的
    /// 异步任务不会取消创建：仍可通过 [`Self::get_ws_client`] 获取客户端，
    /// 并且需要通过 [`Self::destroy_ws_client`] 释放客户端。
    /// 配置校验失败时会释放预留名称，允许后续重试。
    ///
    /// ```no_run
    /// use open_net::{NetworkConfig, NetError, OpenNet, ProxyConfig, WebSocketClientConfig};
    /// # async fn example() -> Result<(), NetError> {
    /// let defaults = NetworkConfig::default()
    ///     .with_proxy(ProxyConfig::http_connect("http://127.0.0.1:8080", None)?);
    /// let net = OpenNet::new_with_network_config(defaults.clone())?;
    /// let inherited = net.create_ws_client("inherited").await?;
    /// let direct = net.create_ws_client_with_network_config(
    ///     "direct", WebSocketClientConfig::default(), NetworkConfig::default(),
    /// ).await?;
    /// let other = net.create_ws_client_with_network_config(
    ///     "other", WebSocketClientConfig::default(),
    ///     defaults.with_proxy(ProxyConfig::http_connect("http://127.0.0.1:8081", None)?),
    /// ).await?;
    /// net.destroy_ws_client("inherited").await?;
    /// net.destroy_ws_client("direct").await?;
    /// net.destroy_ws_client("other").await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "ws-client")]
    pub async fn create_ws_client_with_network_config(
        &self,
        thread_name: &str,
        config: WebSocketClientConfig,
        network_config: crate::api::network_config::NetworkConfig,
    ) -> Result<WebSocketClient, NetError> {
        on_common::log_t!(LogType::WSC; "create_ws_client_with_network_config", "thread_name|config", thread_name, format!("{config:?}"));
        let result: Result<WebSocketClient, NetError> = async {
            let thread_name = thread_name.trim();
            if thread_name.is_empty() {
                return Err(NetError::ParameterEmpty);
            }
            self.inner
                .create_ws_client(thread_name.to_string(), config, Some(network_config))
                .await
        }
        .await;
        result.inspect_err(|error| {
            on_common::log_e!(LogType::WSC; "create_ws_client_with_network_config", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    #[cfg(feature = "ws-client")]
    pub fn get_ws_client(&self, thread_name: &str) -> Result<WebSocketClient, NetError> {
        on_common::log_t!(LogType::WSC; "get_ws_client", "thread_name", thread_name);
        let result: Result<WebSocketClient, NetError> = (|| {
            if thread_name.trim().is_empty() {
                return Err(NetError::ParameterEmpty);
            }
            self.inner.get_ws_client(thread_name.trim())
        })();
        result.inspect_err(|error| {
            on_common::log_e!(LogType::WSC; "get_ws_client", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }

    #[cfg(feature = "ws-client")]
    pub async fn destroy_ws_client(&self, thread_name: &str) -> Result<(), NetError> {
        on_common::log_t!(LogType::WSC; "destroy_ws_client", "thread_name", thread_name);
        let result: Result<(), NetError> = async {
            if thread_name.trim().is_empty() {
                return Err(NetError::ParameterEmpty);
            }
            self.inner.destroy_ws_client(thread_name.trim()).await
        }
        .await;
        result.inspect_err(|error| {
            on_common::log_e!(LogType::WSC; "destroy_ws_client", "error|error_code", format!("{error:?}"), *error as i32);
        })
    }
}

impl OpenNet {
    /// 创建网络状态客户端；返回时尚未监控，需要继续调用 `start().await`。
    ///
    /// 名称会去除首尾空白，同一引擎内网络状态客户端不能重名。
    /// 本功能始终可用，不需要启用任何可选功能。
    ///
    /// ```no_run
    /// use open_net::{NetError, OpenNet};
    /// # async fn example() -> Result<(), NetError> {
    /// let net = OpenNet::new()?;
    /// let client = net.create_net_status_client("network-status").await?;
    /// client.start().await?;
    /// let status = client.local_network_reachability()?;
    /// let ip_stack = client.ip_stack()?;
    /// net.destroy_net_status_client("network-status").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_net_status_client(
        &self,
        thread_name: &str,
    ) -> Result<NetStatusClient, NetError> {
        let thread_name = thread_name.trim();
        if thread_name.is_empty() {
            return Err(NetError::ParameterEmpty);
        }
        self.inner.create_net_status_client(thread_name)
    }

    /// 获取同名客户端的共享句柄。
    pub fn get_net_status_client(&self, thread_name: &str) -> Result<NetStatusClient, NetError> {
        let thread_name = thread_name.trim();
        if thread_name.is_empty() {
            return Err(NetError::ParameterEmpty);
        }
        self.inner.get_net_status_client(thread_name)
    }

    /// 永久停止客户端，等待监控退出后释放名称。已有克隆不能再次启动。
    ///
    /// 取消等待不会取消已经提交的销毁；清理完成前名称仍保留。
    /// 仅需暂停并保留客户端时，使用 `NetStatusClient::shutdown`。
    pub async fn destroy_net_status_client(&self, thread_name: &str) -> Result<(), NetError> {
        let thread_name = thread_name.trim();
        if thread_name.is_empty() {
            return Err(NetError::ParameterEmpty);
        }
        self.inner.destroy_net_status_client(thread_name).await
    }
}

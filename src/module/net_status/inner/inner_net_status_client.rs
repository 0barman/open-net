use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use crate::common::log::log_def::LogType;
use crate::common::CommonEngine;
use n0_watcher::Watcher as _;
use tokio::sync::{oneshot, watch};
use tokio::time::{self, MissedTickBehavior};

use super::monitor_runtime::{MonitorCompletion, MonitorInitialization, MonitorRuntime};
use super::monitor_state::MonitorState;
use super::network_status_snapshot::{
    NetworkPublication, NetworkStatusPublisher, NetworkStatusSnapshot, NetworkStatusSource,
};
use super::platform::PlatformNetworkMonitor;
use super::refresh_trigger::{request_refresh_or_stop, RefreshWorkOutcome};
use crate::error::NetError;
use crate::module::net_status::{IpStack, NetworkStatus};
use crate::net_status::{MonitorState as PublicMonitorState, NetworkSnapshot};
use crate::subscription::common_executor::CommonCallbackExecutor;
use crate::subscription::StateReceiver;

#[cfg(test)]
#[path = "network_observation_tests.rs"]
mod network_observation_tests;

#[cfg(test)]
#[path = "initialization_tests.rs"]
mod initialization_tests;

#[derive(Default)]
// 客户端生命周期状态；短时同步锁串行化启动、停止和永久销毁。
struct Lifecycle {
    // 当前可接受更新的监控代；停止时先移出，再异步等待其完成。
    monitor: Option<MonitorRuntime>,
    /// Keep completion receivers after signaling stop, including when a caller
    /// cancels shutdown. A later destroy must still await these native resources.
    // 已请求停止或自行退休但尚未完成内部资源清理的监控代，供后续 shutdown/destroy 等待。
    stopping: Vec<watch::Receiver<bool>>,
    // 永久销毁标志；一旦置位，后续 start 不再允许创建监控任务。
    destroyed: bool,
}

// 在首次轮询之前即持有的一代退出守卫；清理不依赖 start 的等待者仍然存在。
struct MonitorTaskCompletion {
    // 弱引用避免后台任务与客户端形成所有权环，仅退休身份匹配的当前监控代。
    lifecycle: Weak<Mutex<Lifecycle>>,
    // 该代唯一有效性标志，其 Arc 身份同时用于防止旧代清理误删新代。
    active: Arc<AtomicBool>,
    // 监控代的退休屏障；退出时完成内部状态清理后再通知等待者。
    state: Arc<Mutex<MonitorState>>,
    // 失败或取消时在资源清理完成后唤醒所有同代初始化等待者。
    initial_state: watch::Sender<MonitorInitialization>,
    // 正常构造或首轮发布返回的初始化错误，取消未轮询任务时使用 RuntimeError。
    initialization_error: Option<NetError>,
    // 退出时重置本代内部观测；旧发布器不会覆盖新代观测。
    observation: NetworkStatusPublisher,
    // 内部资源清理后发布完成；任意用户捕获析构不包含在等待范围内，避免重入销毁自等。
    completion: Option<MonitorCompletion>,
}

impl Drop for MonitorTaskCompletion {
    // 原子地退休句柄并保留完成接收端；内部资源就绪后发布结果，再锁外释放用户捕获。
    fn drop(&mut self) {
        let stopped = !self.active.swap(false, Ordering::AcqRel);
        let retired = self.lifecycle.upgrade().and_then(|lifecycle| {
            let mut lifecycle = match lifecycle.lock() {
                Ok(lifecycle) => lifecycle,
                Err(poisoned) => {
                    crate::log_e!(LogType::Engine; "network_status_monitor_exit", "error", "lifecycle_lock_poisoned_recovered");
                    poisoned.into_inner()
                }
            };
            if lifecycle
                .monitor
                .as_ref()
                .is_some_and(|monitor| Arc::ptr_eq(&monitor.active, &self.active))
            {
                let monitor = lifecycle.monitor.take();
                if let Some(monitor) = &monitor {
                    lifecycle.stopping.push(monitor.finished.clone());
                }
                monitor
            } else {
                None
            }
        });
        // Preserve the original retirement barrier before signalling native completion.
        {
            let _state = match self.state.lock() {
                Ok(state) => state,
                Err(poisoned) => {
                    crate::log_e!(LogType::Engine; "network_status_monitor_exit", "error", "state_lock_poisoned_recovered");
                    poisoned.into_inner()
                }
            };
        }
        let failure = self.initialization_error.clone().or_else(|| {
            (!stopped).then(|| NetError::from(crate::error::ErrorKind::RuntimeUnavailable))
        });
        let terminal = failure
            .clone()
            .map_or(PublicMonitorState::Stopped, PublicMonitorState::Failed);
        let publication = self.observation.prepare_finish(terminal);
        drop(retired);
        drop(self.completion.take());
        self.initial_state.send_if_modified(|initial| {
            if !matches!(initial, MonitorInitialization::Pending) {
                return false;
            }
            *initial = match failure {
                Some(error) => MonitorInitialization::Failed(error),
                None if stopped => MonitorInitialization::Stopped,
                None => MonitorInitialization::Failed(NetError::from(
                    crate::error::ErrorKind::RuntimeUnavailable,
                )),
            };
            true
        });
        // User wakes and captures may synchronously wait for destroy. Signal completion first.
        if let Err(error) = publication.and_then(NetworkPublication::dispatch) {
            crate::log_e!(LogType::Engine; "network_status_monitor_exit", "error", crate::common::log::summary::error(&error));
        }
    }
}

// 网络状态客户端内部实现，连接引擎运行时、单代监控生命周期与跨代观测源。
pub(crate) struct InnerNetStatusClient {
    // 提供 Tokio 运行时与公开监听器回调线程池的共享引擎。
    engine: Arc<CommonEngine>,
    // 保护监控任务及销毁状态；异步等待完成信号时不持有该锁。
    lifecycle: Arc<Mutex<Lifecycle>>,
    // 统一快照源；公开订阅复用引擎回调池，私有 watch 维持 WS 的网络门控时序。
    observations: NetworkStatusSource,
    // 测试专用的安全初始化工厂，用于精确控制失败与并发时机，不改变生产构建。
    #[cfg(test)]
    monitor_factory: Mutex<Option<initialization_tests::MonitorFactory>>,
}

impl InnerNetStatusClient {
    // 创建未启动的客户端，持有共享引擎并将内部观测初始化为未知。
    pub(crate) fn new(engine: Arc<CommonEngine>) -> Result<Self, NetError> {
        let executor = Arc::new(CommonCallbackExecutor::new(&engine));
        let observations = NetworkStatusSource::new(executor, 1024)?;
        Ok(Self {
            engine,
            lifecycle: Arc::new(Mutex::new(Lifecycle::default())),
            observations,
            #[cfg(test)]
            monitor_factory: Mutex::new(None),
        })
    }

    /// Observe ordered network facts without involving the public callback pool.
    /// Before initialization and after monitoring stops, the status is Unknown.
    #[cfg_attr(not(any(feature = "ws-client", test)), allow(dead_code))]
    // 订阅内部快照而不启动监控；尚无观测或停止后的状态为 None，表示未知。
    pub(crate) fn subscribe(&self) -> watch::Receiver<NetworkStatusSnapshot> {
        self.observations.subscribe()
    }

    // 幂等启动并等待同代初始化结果；失败代先完成退休，再将错误交付所有等待者。
    // stop 取消初始化时返回当前快照；永久销毁优先返回 Closed。
    pub(crate) async fn start(&self) -> Result<NetworkSnapshot, NetError> {
        let (mut initial_state, task, publication) = {
            let mut lifecycle = self.lifecycle.lock().map_err(NetError::from_poison)?;
            if lifecycle.destroyed {
                return Err(NetError::from(crate::error::ErrorKind::Closed));
            }
            lifecycle
                .stopping
                .retain(|finished| !*::tokio::sync::watch::Receiver::borrow(finished));
            let (task, publication) = if lifecycle.monitor.is_none() {
                let (monitor, task, publication) = self.prepare_monitor_task()?;
                lifecycle.monitor = Some(monitor);
                (Some(task), Some(publication))
            } else {
                (None, None)
            };
            let monitor = lifecycle
                .monitor
                .as_ref()
                .ok_or(NetError::from(crate::error::ErrorKind::Internal))?;
            (monitor.initial_state.clone(), task, publication)
        };
        // A failed preparation owns deferred cleanup. Retire its unpolled task first;
        // normal notifications must follow submission because a user waker may await shutdown.
        if let Err(error) = publication
            .as_ref()
            .map(NetworkPublication::result)
            .transpose()
        {
            drop(task);
            if let Some(publication) = publication {
                if let Err(dispatch_error) = publication.dispatch() {
                    crate::log_e!(LogType::Engine; "network_status_start", "error", crate::common::log::summary::error(&dispatch_error));
                }
            }
            return Err(error);
        }
        // 退出守卫也会获取生命周期锁；在锁外提交，覆盖运行时拒绝未轮询任务的路径。
        if let Some(task) = task {
            self.engine.runtime_handle().spawn(task);
        }
        if let Some(publication) = publication {
            publication.dispatch()?;
        }
        while matches!(
            *initial_state.borrow_and_update(),
            MonitorInitialization::Pending
        ) {
            if initial_state.changed().await.is_err() {
                break;
            }
        }
        if self
            .lifecycle
            .lock()
            .map_err(NetError::from_poison)?
            .destroyed
        {
            return Err(NetError::from(crate::error::ErrorKind::Closed));
        }
        let result = ::tokio::sync::watch::Receiver::borrow(&initial_state).clone();
        match result {
            MonitorInitialization::Ready | MonitorInitialization::Stopped => self.snapshot(),
            MonitorInitialization::Failed(error) => Err(error),
            MonitorInitialization::Pending => {
                Err(NetError::from(crate::error::ErrorKind::RuntimeUnavailable))
            }
        }
    }

    // 创建独立监控代、信号与尚未提交的 Future，由 start 安装句柄后在生命周期锁外启动。
    // 退出守卫在首次轮询前就由 Future 持有，覆盖取消等待、未轮询释放及异常退出。
    fn prepare_monitor_task(
        &self,
    ) -> Result<
        (
            MonitorRuntime,
            impl std::future::Future<Output = ()> + Send + 'static,
            NetworkPublication,
        ),
        NetError,
    > {
        #[cfg(test)]
        let factory = self
            .monitor_factory
            .lock()
            .map_err(NetError::from_poison)?
            .clone();
        let (stop_sender, stop_receiver) = oneshot::channel();
        let (initial_sender, initial_state) = watch::channel(MonitorInitialization::Pending);
        let (finished_sender, finished) = watch::channel(false);
        let (publisher, publication) = self.observations.begin_generation()?;
        let state = MonitorState {
            observation: Some(publisher.clone()),
            ..MonitorState::default()
        };
        let active = Arc::clone(&state.active);
        let state = Arc::new(Mutex::new(state));
        let shared_state = Arc::clone(&state);
        let completion = MonitorTaskCompletion {
            lifecycle: Arc::downgrade(&self.lifecycle),
            active: Arc::clone(&active),
            state: Arc::clone(&state),
            initial_state: initial_sender.clone(),
            initialization_error: publication.result().err(),
            observation: publisher,
            completion: Some(MonitorCompletion(finished_sender)),
        };
        // 长期任务直接交给共享运行时，不进入逐项等待的引擎工作队列。
        let task = async move {
            let mut completion = completion;
            if let Err(error) = Self::monitor_until_stopped(
                shared_state,
                stop_receiver,
                initial_sender,
                #[cfg(test)]
                factory,
            )
            .await
            {
                completion.initialization_error = Some(error);
            }
        };
        let monitor = MonitorRuntime {
            stop_sender: Some(stop_sender),
            initial_state,
            finished,
            state,
            active,
        };
        Ok((monitor, task, publication))
    }

    // Commit the stop while holding the lifecycle lock; notify public observers afterwards.
    fn request_stop(&self, permanent: bool) -> (Vec<watch::Receiver<bool>>, Option<NetError>) {
        let mut error = None;
        let (finished, retired, publication) = {
            let mut lifecycle = match self.lifecycle.lock() {
                Ok(lifecycle) => lifecycle,
                Err(poisoned) => {
                    error = Some(NetError::from(crate::error::ErrorKind::Internal));
                    poisoned.into_inner()
                }
            };
            lifecycle.destroyed |= permanent;
            let mut retired = lifecycle.monitor.take();
            if let Some(monitor) = &mut retired {
                monitor.active.store(false, Ordering::Release);
                if let Some(stop) = monitor.stop_sender.take() {
                    let _ = stop.send(());
                }
                lifecycle.stopping.push(monitor.finished.clone());
            }
            let publication = if permanent {
                self.observations.prepare_closed()
            } else {
                self.observations.prepare_stopped()
            };
            lifecycle
                .stopping
                .retain(|finished| !*::tokio::sync::watch::Receiver::borrow(finished));
            (lifecycle.stopping.clone(), retired, publication)
        };
        drop(retired);
        if let Err(publication_error) = publication.and_then(NetworkPublication::dispatch) {
            crate::log_e!(LogType::Engine; "network_status_stop", "error", crate::common::log::summary::error(&publication_error));
            error = Some(publication_error);
        }
        (finished, error)
    }

    // 逐一等待已退役监控完成；发送端关闭也结束对应等待，全程不占用客户端生命周期锁。
    async fn wait_until_finished(finished: Vec<watch::Receiver<bool>>) {
        for mut receiver in finished {
            while !*receiver.borrow_and_update() {
                if receiver.changed().await.is_err() {
                    break;
                }
            }
        }
    }

    // 停止当前监控并等待所有退役监控释放资源，之后允许重新启动；透传同步清理阶段的错误。
    pub(crate) async fn stop(&self) -> Result<(), NetError> {
        let (finished, error) = self.request_stop(false);
        Self::wait_until_finished(finished).await;
        error.map_or(Ok(()), Err)
    }

    // 发出永久销毁请求并禁止后续启动；此同步入口不等待异步监控资源清理完成。
    pub(crate) fn request_destroy(&self) {
        self.request_stop(true);
    }

    // 永久销毁客户端并等待已有监控清理完成，返回停止过程中记录的错误。
    pub(crate) async fn destroy(&self) -> Result<(), NetError> {
        let (finished, error) = self.request_stop(true);
        Self::wait_until_finished(finished).await;
        error.map_or(Ok(()), Err)
    }

    pub(crate) async fn shutdown(&self) -> Result<(), NetError> {
        self.destroy().await
    }

    pub(crate) fn snapshot(&self) -> Result<NetworkSnapshot, NetError> {
        self.observations.snapshot()
    }

    pub(crate) fn subscribe_state(&self) -> Result<StateReceiver<NetworkSnapshot>, NetError> {
        self.observations.observe()
    }

    #[cfg(test)]
    fn current_state(&self) -> Result<Option<Arc<Mutex<MonitorState>>>, NetError> {
        Ok(self
            .lifecycle
            .lock()
            .map_err(NetError::from_poison)?
            .monitor
            .as_ref()
            .map(|monitor| Arc::clone(&monitor.state)))
    }

    #[cfg(test)]
    fn is_started(&self) -> bool {
        matches!(self.snapshot(), Ok(snapshot) if matches!(snapshot.state, PublicMonitorState::Running))
    }

    // 按默认路由与 IP 能力推导可达性：同时具备默认路由及至少一种 IP 能力时为 Available。
    fn reachability_from_flags(
        has_default_route: bool,
        have_v4: bool,
        have_v6: bool,
    ) -> NetworkStatus {
        if has_default_route && (have_v4 || have_v6) {
            NetworkStatus::Available
        } else {
            NetworkStatus::Unavailable
        }
    }

    // 从 netwatch 接口快照提取默认路由与 IPv4/IPv6 标志，转换为本库的可达性枚举。
    fn reachability_from_state(state: &netwatch::netmon::State) -> NetworkStatus {
        Self::reachability_from_flags(
            state.default_route_interface.is_some(),
            state.have_v4,
            state.have_v6,
        )
    }

    /// Derive the IP-stack capability from the `netwatch` interface state. This
    /// uses the same `have_v4` / `have_v6` flags on every platform, so the
    /// reported value has consistent cross-platform semantics.
    // 将 netwatch 的 IPv4/IPv6 能力标志映射为 IP 栈枚举，各平台使用同一套映射规则。
    fn ip_stack_from_state(state: &netwatch::netmon::State) -> IpStack {
        IpStack::from_flags(state.have_v4, state.have_v6)
    }

    // 获取当前可达性；Windows 优先使用系统连接状态，查询无结果或其他平台则使用 netwatch 快照。
    fn current_reachability(state: &netwatch::netmon::State) -> NetworkStatus {
        #[cfg(target_os = "windows")]
        if let Some(reachability) = Self::windows_network_reachability() {
            return reachability;
        }

        Self::reachability_from_state(state)
    }

    // One authoritative snapshot contains reachability and IP capability. Public wakeups,
    // user captures and callback scheduling are all released outside the monitor lock.
    fn update_state_inner(
        state: &Arc<Mutex<MonitorState>>,
        reachability: NetworkStatus,
        ip_stack: IpStack,
    ) -> Result<(), NetError> {
        #[cfg(target_os = "windows")]
        let network_name = Self::query_current_network();
        #[cfg(not(target_os = "windows"))]
        let network_name = None;
        let publication = {
            let guard = state.lock().map_err(NetError::from_poison)?;
            if !guard.active.load(Ordering::Acquire) {
                return Ok(());
            }
            match &guard.observation {
                Some(observation) => Some(observation.prepare_observation(
                    reachability,
                    Some(ip_stack),
                    network_name,
                )?),
                None => None,
            }
        };
        if let Some(publication) = publication {
            publication.dispatch()?;
        }
        Ok(())
    }

    /// Monitor task body: ported from the reference project's
    /// `monitor_until_stopped`, but the state comes from the instance rather
    /// than a global.
    ///
    /// Initialization errors reach every caller waiting on this generation's
    /// start result. Later observation failures end the task; its completion
    /// guard retires the generation and resets its state.
    // 初始化 netwatch 并发布首轮观测，然后处理接口更新、原生刷新提示、定时复查与优先停止信号。
    // 原生事件仅请求 netwatch 刷新，不能直接作为可达性事实；初始化失败保留内部未知状态。
    // 退出时释放平台监控并重置公开状态，内部未知状态及完成通知由任务所持守卫负责发布。
    async fn monitor_until_stopped(
        state: Arc<Mutex<MonitorState>>,
        mut stop_receiver: oneshot::Receiver<()>,
        initial_state: watch::Sender<MonitorInitialization>,
        #[cfg(test)] factory: Option<initialization_tests::MonitorFactory>,
    ) -> Result<(), NetError> {
        let monitor = tokio::select! {
            biased;
            _ = &mut stop_receiver => return Ok(()),
            result = Self::create_monitor(
                #[cfg(test)]
                factory,
            ) => result,
        };
        let monitor = match monitor {
            Ok(monitor) => monitor,
            Err(error) => {
                // Detector failure is not evidence of an unavailable network.
                // Completion publishes Failed with no current observation.
                return Err(error);
            }
        };
        let mut interface_state = monitor.interface_state();

        let initial = interface_state.get();
        let current = Self::current_reachability(&initial);
        let ip_stack = Self::ip_stack_from_state(&initial);
        // If the initial state update fails (lock poisoned), end the task.
        if let Err(error) = Self::update_state_inner(&state, current, ip_stack) {
            crate::log_e!(LogType::Engine; "network_status_initial_observation", "error", crate::common::log::summary::error(&error));
            return Err(error);
        }
        initial_state.send_replace(MonitorInitialization::Ready);

        // The native macOS source is an optional post-initialization hint. A
        // yield after publishing the authoritative netwatch state ensures its
        // creation is not part of `NetStatusClient::start`'s initial-state barrier.
        #[cfg(target_os = "macos")]
        tokio::task::yield_now().await;
        let mut platform_monitor = PlatformNetworkMonitor::start();

        let mut refresh_interval = time::interval(Duration::from_secs(2));
        refresh_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                biased;
                _ = &mut stop_receiver => break,
                update = interface_state.updated() => {
                    match update {
                        Ok(new_state) => {
                            let reachability = Self::current_reachability(&new_state);
                            let ip_stack = Self::ip_stack_from_state(&new_state);
                            // Exit the monitor loop if the lock is poisoned.
                            Self::update_state_inner(&state, reachability, ip_stack)?;
                        }
                        Err(_) => break,
                    }
                }
                native_changed = platform_monitor.changed() => {
                    if native_changed {
                        match request_refresh_or_stop(
                            &mut stop_receiver,
                            || monitor.network_change(),
                        ).await {
                            RefreshWorkOutcome::Stopped => break,
                            RefreshWorkOutcome::Requested | RefreshWorkOutcome::RequestFailed => {}
                        }
                    }
                }
                _ = refresh_interval.tick() => {
                    let snapshot = interface_state.get();
                    let reachability = Self::current_reachability(&snapshot);
                    let ip_stack = Self::ip_stack_from_state(&snapshot);
                    Self::update_state_inner(&state, reachability, ip_stack)?;
                }
            }
        }

        // On macOS the native source waits for cancellation completion and
        // drains the serial callback queue before releasing the
        // callback sender, preventing a late native hint from reaching state
        // reset or a later client generation.
        #[cfg(target_os = "macos")]
        drop(platform_monitor);

        Ok(())
    }

    // 创建真实检测器并保留底层诊断；测试可替换构造 Future，以安全方式注入错误与时序。
    async fn create_monitor(
        #[cfg(test)] factory: Option<initialization_tests::MonitorFactory>,
    ) -> Result<netwatch::netmon::Monitor, NetError> {
        #[cfg(test)]
        if let Some(factory) = factory {
            return factory().await;
        }
        netwatch::netmon::Monitor::new().await.map_err(|error| {
            crate::log_e!(LogType::Engine; "network_status_monitor_start", "error", format!("{error:?}"));
            NetError::with_source(crate::error::ErrorKind::RuntimeUnavailable, error)
                .with_stage(crate::error::ErrorStage::NetworkMonitor)
        })
    }

    // ------------------------------------------------------------------
    // Windows network reachability (based on the NetworkListManager COM API)
    // ------------------------------------------------------------------

    #[cfg(target_os = "windows")]
    // 将 Windows 连接标志转为可达性，只认可 IPv4 或 IPv6 的 INTERNET 位。
    fn reachability_from_windows_connectivity(
        connectivity: windows::Win32::Networking::NetworkListManager::NLM_CONNECTIVITY,
    ) -> NetworkStatus {
        use windows::Win32::Networking::NetworkListManager::{
            NLM_CONNECTIVITY_IPV4_INTERNET, NLM_CONNECTIVITY_IPV6_INTERNET,
        };

        if connectivity.0 & NLM_CONNECTIVITY_IPV4_INTERNET.0 != 0
            || connectivity.0 & NLM_CONNECTIVITY_IPV6_INTERNET.0 != 0
        {
            NetworkStatus::Available
        } else {
            NetworkStatus::Unavailable
        }
    }

    #[cfg(target_os = "windows")]
    // 同步查询 Windows NetworkListManager 的连接状态，系统调用失败返回 None 以便上层回退。
    // 仅当本次成功初始化 COM 时进行配对反初始化，不释放其他调用方已有的 COM 初始化计数。
    fn windows_network_reachability() -> Option<NetworkStatus> {
        use windows::Win32::{
            Networking::NetworkListManager::{INetworkListManager, NetworkListManager},
            System::Com::{
                CoCreateInstance, CoInitializeEx, CoUninitialize, CLSCTX_ALL, COINIT_MULTITHREADED,
            },
        };

        unsafe {
            let com_initialized = CoInitializeEx(None, COINIT_MULTITHREADED).is_ok();
            let reachability = (|| {
                let manager: INetworkListManager =
                    CoCreateInstance(&NetworkListManager, None, CLSCTX_ALL).ok()?;
                let connectivity = manager.GetConnectivity().ok()?;
                Some(Self::reachability_from_windows_connectivity(connectivity))
            })();

            if com_initialized {
                CoUninitialize();
            }

            reachability
        }
    }

    // ------------------------------------------------------------------
    // Windows connected-network name (Wi-Fi SSID + NetworkListManager)
    // ------------------------------------------------------------------

    /// Resolve the name of the currently connected network on Windows.
    ///
    /// Prefers the active Wi-Fi SSID; if that is unavailable, falls back to the
    /// `NetworkListManager` connected-network name. Returns `None` when no
    /// connected network can be resolved.
    #[cfg(target_os = "windows")]
    // 优先解析活动 Wi-Fi 的 SSID，无结果再查询首个已连接 Windows 网络的名称。
    fn query_current_network() -> Option<String> {
        Self::query_wifi_network().or_else(Self::query_windows_connected_network)
    }

    /// Query the active Wi-Fi SSID via `netsh wlan show interfaces`.
    #[cfg(target_os = "windows")]
    // 无可见控制台地执行 netsh 并解析 Wi-Fi 名称；命令启动失败、退出失败或无 SSID 时返回 None。
    fn query_wifi_network() -> Option<String> {
        use std::os::windows::process::CommandExt;
        use std::process::Command;

        /// Avoid spawning a visible console window for the `netsh` child process.
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;

        let output = Command::new("netsh")
            .args(["wlan", "show", "interfaces"])
            .creation_flags(CREATE_NO_WINDOW)
            .output()
            .ok()?;

        if !output.status.success() {
            return None;
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        Self::parse_netsh_wifi_ssid(&stdout)
    }

    /// Extract the `SSID` value from `netsh wlan show interfaces` output,
    /// ignoring the `BSSID` line and any empty value.
    #[cfg(target_os = "windows")]
    // 从 netsh 文本中提取首个非空 SSID 值，忽略 BSSID 等其他键，找不到时返回 None。
    fn parse_netsh_wifi_ssid(output: &str) -> Option<String> {
        output.lines().find_map(|line| {
            let (key, value) = line.split_once(':')?;
            let key = key.trim();
            if key.eq_ignore_ascii_case("SSID") && !key.eq_ignore_ascii_case("BSSID") {
                let name = value.trim();
                if !name.is_empty() {
                    return Some(name.to_string());
                }
            }
            None
        })
    }

    /// Query the first connected network's name via the `NetworkListManager`
    /// COM API. COM is uninitialized on every exit path via an RAII guard, even
    /// on early returns.
    #[cfg(target_os = "windows")]
    // 通过 COM 获取首个已连接网络的非空名称；查询失败返回 None，并由守卫配对释放本次 COM 初始化。
    fn query_windows_connected_network() -> Option<String> {
        use windows::Win32::Foundation::RPC_E_CHANGED_MODE;
        use windows::Win32::Networking::NetworkListManager::{
            INetworkListManager, NetworkListManager, NLM_ENUM_NETWORK_CONNECTED,
        };
        use windows::Win32::System::Com::{
            CoCreateInstance, CoInitializeEx, CoUninitialize, CLSCTX_ALL, COINIT_MULTITHREADED,
        };

        // 当前线程 COM 初始化的局部释放守卫，覆盖查询中所有提前返回路径。
        struct CoUninit(bool); // 唯一字段：本次调用是否成功初始化 COM，决定析构时是否需要反初始化。
        impl Drop for CoUninit {
            // 仅释放本次成功取得的 COM 初始化引用，保留其他线程模型或调用者已有的初始化状态。
            fn drop(&mut self) {
                if self.0 {
                    unsafe { CoUninitialize() };
                }
            }
        }

        let hr = unsafe { CoInitializeEx(None, COINIT_MULTITHREADED) };
        let should_uninit = hr.is_ok();
        // `RPC_E_CHANGED_MODE` means COM is already initialized with a different
        // model on this thread; the existing apartment is reused and must not be
        // uninitialized here. Any other failure is fatal to this query.
        if hr.is_err() && hr != RPC_E_CHANGED_MODE {
            return None;
        }
        let _co_guard = CoUninit(should_uninit);

        unsafe {
            let nlm: INetworkListManager =
                CoCreateInstance(&NetworkListManager, None, CLSCTX_ALL).ok()?;
            let networks = nlm.GetNetworks(NLM_ENUM_NETWORK_CONNECTED).ok()?;
            let mut fetched = 0;
            let mut items = [None];
            networks.Next(&mut items, Some(&mut fetched)).ok()?;
            if fetched == 0 {
                return None;
            }

            let name = items[0].as_ref()?.GetName().ok()?.to_string();
            if name.trim().is_empty() {
                return None;
            }

            Some(name)
        }
    }
}

impl Drop for InnerNetStatusClient {
    // 客户端析构时发出永久停止请求；Drop 不执行异步等待，资源回收由监控任务自行完成。
    fn drop(&mut self) {
        self.request_destroy();
    }
}

#[cfg(test)]
#[path = "lifecycle_tests.rs"]
mod tests;

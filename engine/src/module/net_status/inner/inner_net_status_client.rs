use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use n0_watcher::Watcher as _;
use on_common::log::listener::LogListener;
use on_common::log::log_def::LogType;
use on_common::log::logger::{LogSubscription, Logger};
use on_common::CommonEngine;
use tokio::runtime::Handle;
use tokio::sync::{oneshot, watch};
use tokio::time::{self, MissedTickBehavior};

use super::monitor_runtime::{MonitorCompletion, MonitorRuntime};
use super::monitor_state::{Dispatcher, MonitorState, SharedListener};
use super::network_status_snapshot::{
    NetworkStatusMonitorGuard, NetworkStatusSnapshot, NetworkStatusSource,
};
use super::platform::PlatformNetworkMonitor;
use super::refresh_trigger::{request_refresh_or_stop, RefreshWorkOutcome};
use crate::api::net_error::NetError;
use crate::module::net_status::{
    IpStack, NetworkStatus, NetworkStatusListener, NetworkStatusListenerHandle,
};

#[cfg(test)]
#[path = "network_observation_tests.rs"]
mod network_observation_tests;

#[derive(Default)]
struct Lifecycle {
    monitor: Option<MonitorRuntime>,
    /// Keep completion receivers after signaling stop, including when a caller
    /// cancels shutdown. A later destroy must still await these native resources.
    stopping: Vec<watch::Receiver<bool>>,
    destroyed: bool,
    log_subscription: Option<LogSubscription>,
}

pub(crate) struct InnerNetStatusClient {
    engine: Arc<CommonEngine>,
    lifecycle: Mutex<Lifecycle>,
    observations: NetworkStatusSource,
}

impl InnerNetStatusClient {
    pub(crate) fn new(engine: Arc<CommonEngine>) -> Self {
        Self {
            engine,
            lifecycle: Mutex::new(Lifecycle::default()),
            observations: NetworkStatusSource::new(),
        }
    }

    /// Observe ordered network facts without involving the public callback pool.
    /// Before initialization and after monitoring stops, the status is Unknown.
    #[cfg_attr(not(any(feature = "ws-client", test)), allow(dead_code))]
    pub(crate) fn subscribe(&self) -> watch::Receiver<NetworkStatusSnapshot> {
        self.observations.subscribe()
    }

    pub(crate) async fn start(&self) -> Result<(), NetError> {
        let (mut initial_state, active) = {
            let mut lifecycle = self.lifecycle.lock().map_err(NetError::from_poison)?;
            if lifecycle.destroyed {
                return Err(NetError::EngineDropped);
            }
            lifecycle.stopping.retain(|finished| !*finished.borrow());
            if lifecycle.monitor.is_none() {
                lifecycle.monitor = Some(self.spawn_monitor_task()?);
            }
            let monitor = lifecycle.monitor.as_ref().ok_or(NetError::InternalError)?;
            (monitor.initial_state.clone(), Arc::clone(&monitor.active))
        };
        while !*initial_state.borrow_and_update() {
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
            return Err(NetError::EngineDropped);
        }
        // A concurrent ordinary shutdown may have already retired this start.
        // Preserve the idempotent lifecycle contract in that race.
        if !active.load(Ordering::Acquire) {
            return Ok(());
        }
        if !*initial_state.borrow() {
            return Err(NetError::RuntimeError);
        }
        Ok(())
    }

    fn spawn_monitor_task(&self) -> Result<MonitorRuntime, NetError> {
        let (stop_sender, stop_receiver) = oneshot::channel();
        let (initial_sender, initial_state) = watch::channel(false);
        let (finished_sender, finished) = watch::channel(false);
        let publisher = self.observations.begin_generation()?;
        let observation_completion = NetworkStatusMonitorGuard(publisher.clone());
        let state = MonitorState {
            observation: Some(publisher),
            ..MonitorState::default()
        };
        let active = Arc::clone(&state.active);
        let state = Arc::new(Mutex::new(state));
        let shared_state = Arc::clone(&state);
        let dispatcher = self.dispatcher(Arc::clone(&active));
        let completion = MonitorCompletion(finished_sender);
        // CommonEngine's post queue awaits one task at a time. A long-lived
        // monitor must be spawned directly or it would starve unrelated work.
        self.engine.runtime_handle().spawn(async move {
            let _completion = completion;
            let _observation_completion = observation_completion;
            Self::monitor_until_stopped(shared_state, dispatcher, stop_receiver, initial_sender)
                .await;
        });
        Ok(MonitorRuntime {
            stop_sender: Some(stop_sender),
            initial_state,
            finished,
            state,
            active,
        })
    }

    fn dispatcher(&self, active: Arc<AtomicBool>) -> Dispatcher {
        let runtime_handle = self.engine.runtime_handle();
        Arc::from(
            self.engine
                .cb_pool_fn2_boxed(move |listener: SharedListener, status| {
                    if active.load(Ordering::Acquire) {
                        Self::invoke_listener(Some(&runtime_handle), &listener, status);
                    }
                }),
        )
    }

    fn invoke_listener(
        runtime_handle: Option<&Handle>,
        listener: &SharedListener,
        status: NetworkStatus,
    ) {
        let _runtime_guard = runtime_handle.map(Handle::enter);
        if catch_unwind(AssertUnwindSafe(|| listener(status))).is_err() {
            on_common::log_s!(LogType::Engine; "network_status_listener", "listener_panic", status.name());
        }
    }

    /// Linearize stopping under a short synchronous lock. Every start receives
    /// an independent state; it can safely overlap a retiring task's cleanup.
    fn request_stop(&self, permanent: bool) -> (Vec<watch::Receiver<bool>>, Option<NetError>) {
        let mut error = None;
        let (finished, listeners, subscription) = {
            let mut lifecycle = self.lifecycle.lock().unwrap_or_else(|poisoned| {
                error = Some(NetError::InternalError);
                poisoned.into_inner()
            });
            lifecycle.destroyed |= permanent;
            let mut listeners = None;
            if let Some(mut monitor) = lifecycle.monitor.take() {
                monitor.active.store(false, Ordering::Release);
                if let Some(stop) = monitor.stop_sender.take() {
                    let _ = stop.send(());
                }
                let mut state = monitor.state.lock().unwrap_or_else(|poisoned| {
                    error = Some(NetError::InternalError);
                    poisoned.into_inner()
                });
                state.reachability = NetworkStatus::Unavailable;
                state.ip_stack = IpStack::None;
                if let Some(observation) = &state.observation {
                    if let Err(publication_error) = observation.publish(None) {
                        on_common::log_e!(LogType::Engine; "network_status_observation_stop", "error", on_common::log::summary::error(&publication_error));
                        error = Some(publication_error);
                    }
                }
                listeners = Some(std::mem::take(&mut state.listeners));
                lifecycle.stopping.push(monitor.finished);
            }
            lifecycle.stopping.retain(|finished| !*finished.borrow());
            let subscription = if permanent {
                lifecycle.log_subscription.take()
            } else {
                None
            };
            (lifecycle.stopping.clone(), listeners, subscription)
        };
        // User callback captures can have reentrant destructors. Never release
        // them while holding either lifecycle or monitor locks.
        drop(listeners);
        drop(subscription);
        (finished, error)
    }

    async fn wait_until_finished(finished: Vec<watch::Receiver<bool>>) {
        for mut receiver in finished {
            while !*receiver.borrow_and_update() {
                if receiver.changed().await.is_err() {
                    break;
                }
            }
        }
    }

    pub(crate) async fn shutdown(&self) -> Result<(), NetError> {
        let (finished, error) = self.request_stop(false);
        Self::wait_until_finished(finished).await;
        error.map_or(Ok(()), Err)
    }

    pub(crate) fn request_destroy(&self) {
        self.request_stop(true);
    }

    pub(crate) async fn destroy(&self) -> Result<(), NetError> {
        let (finished, error) = self.request_stop(true);
        Self::wait_until_finished(finished).await;
        error.map_or(Ok(()), Err)
    }

    fn current_state(&self) -> Result<Option<Arc<Mutex<MonitorState>>>, NetError> {
        Ok(self
            .lifecycle
            .lock()
            .map_err(NetError::from_poison)?
            .monitor
            .as_ref()
            .map(|monitor| Arc::clone(&monitor.state)))
    }

    pub(crate) fn is_started(&self) -> bool {
        self.lifecycle
            .lock()
            .map(|lifecycle| lifecycle.monitor.is_some())
            .unwrap_or(false)
    }

    pub(crate) fn local_network_reachability(&self) -> Result<NetworkStatus, NetError> {
        let Some(state) = self.current_state()? else {
            return Ok(NetworkStatus::Unavailable);
        };
        #[cfg(target_os = "windows")]
        if let Some(reachability) = Self::windows_network_reachability() {
            let active = Arc::clone(&state.lock().map_err(NetError::from_poison)?.active);
            Self::update_reachability_inner(&state, &self.dispatcher(active), reachability)?;
        }
        let value = state.lock().map_err(NetError::from_poison)?.reachability;
        Ok(value)
    }

    pub(crate) fn ip_stack(&self) -> Result<IpStack, NetError> {
        let Some(state) = self.current_state()? else {
            return Ok(IpStack::None);
        };
        let value = state.lock().map_err(NetError::from_poison)?.ip_stack;
        Ok(value)
    }

    pub(crate) fn register(
        &self,
        listener: NetworkStatusListener,
    ) -> Result<Option<NetworkStatusListenerHandle>, NetError> {
        // Keep the caller's reference alive until after all lock guards drop.
        let listener: SharedListener = Arc::from(listener);
        let Some(state) = self.current_state()? else {
            return Ok(None);
        };
        let mut state = state.lock().map_err(NetError::from_poison)?;
        if !state.active.load(Ordering::Acquire) {
            return Ok(None);
        }
        let handle = state.next_listener_handle();
        state.listeners.insert(handle, Arc::clone(&listener));
        Ok(Some(handle))
    }

    pub(crate) fn unregister(&self, handle: NetworkStatusListenerHandle) -> Result<bool, NetError> {
        let Some(state) = self.current_state()? else {
            return Ok(false);
        };
        let listener = state
            .lock()
            .map_err(NetError::from_poison)?
            .listeners
            .remove(&handle);
        Ok(listener.is_some())
    }

    pub(crate) fn clear_all_listener(&self) -> Result<(), NetError> {
        let state = self.current_state()?.ok_or(NetError::NotStarted)?;
        let listeners = {
            let mut state = state.lock().map_err(NetError::from_poison)?;
            if !state.active.load(Ordering::Acquire) {
                return Err(NetError::NotStarted);
            }
            std::mem::take(&mut state.listeners)
        };
        drop(listeners);
        Ok(())
    }

    pub(crate) fn get_current_network_name(&self) -> Result<Option<String>, NetError> {
        if self.current_state()?.is_none() {
            return Ok(None);
        }
        #[cfg(target_os = "windows")]
        {
            Ok(Self::query_current_network())
        }
        #[cfg(not(target_os = "windows"))]
        {
            Ok(None)
        }
    }

    pub(crate) fn set_log_listener(&self, listener: Option<LogListener>) {
        if let Err(error) = self.try_set_log_listener(listener) {
            on_common::log_e!(LogType::Engine; "net_status_set_log_listener", "error", on_common::log::summary::error(&error));
        }
    }

    pub(crate) fn try_set_log_listener(
        &self,
        listener: Option<LogListener>,
    ) -> std::io::Result<()> {
        let listener = listener.map(Arc::new);
        let previous = {
            let mut lifecycle = self
                .lifecycle
                .lock()
                .map_err(|_| std::io::Error::other("net status lifecycle lock poisoned"))?;
            let replacement = if lifecycle.destroyed {
                None
            } else {
                listener
                    .as_ref()
                    .map(|listener| {
                        let listener = Arc::clone(listener);
                        let handle = self.engine.runtime_handle();
                        Logger::register_log_listener(
                            Box::new(move |info| {
                                let _guard = handle.enter();
                                let _ = catch_unwind(AssertUnwindSafe(|| listener(info)));
                            }),
                            &[LogType::Engine, LogType::Common],
                        )
                    })
                    .transpose()?
            };
            std::mem::replace(&mut lifecycle.log_subscription, replacement)
        };
        drop(previous);
        Ok(())
    }

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
    fn ip_stack_from_state(state: &netwatch::netmon::State) -> IpStack {
        IpStack::from_flags(state.have_v4, state.have_v6)
    }

    fn current_reachability(state: &netwatch::netmon::State) -> NetworkStatus {
        #[cfg(target_os = "windows")]
        if let Some(reachability) = Self::windows_network_reachability() {
            return reachability;
        }

        Self::reachability_from_state(state)
    }

    /// Static version used by the monitor task (which only holds an
    /// `Arc<Mutex<MonitorState>>` and the dispatcher).
    ///
    /// Returns [`NetError::InternalError`] on poison, leaving handling to the caller;
    /// never panics.
    #[cfg(any(target_os = "windows", test))]
    fn update_reachability_inner(
        state: &Arc<Mutex<MonitorState>>,
        dispatcher: &Dispatcher,
        reachability: NetworkStatus,
    ) -> Result<(), NetError> {
        let listeners = {
            let mut guard = state.lock().map_err(NetError::from_poison)?;
            if !guard.active.load(Ordering::Acquire) {
                return Ok(());
            }
            if let Some(observation) = &guard.observation {
                observation.publish(Some(reachability))?;
            }
            if guard.reachability == reachability {
                return Ok(());
            }
            guard.reachability = reachability;
            guard.listeners.values().cloned().collect::<Vec<_>>()
        };

        on_common::log_s!(LogType::Engine;
            "network_status_listener",
            "network_status",
            reachability.name()
        );

        for listener in listeners {
            dispatcher(listener, reachability);
        }
        Ok(())
    }

    /// Update both reachability and IP-stack capability under a single lock.
    ///
    /// The IP-stack value is always refreshed (it has no listeners and fires no
    /// callbacks). Reachability is only updated, and its listeners only
    /// dispatched, when it actually changes — preserving the existing
    /// change-detection contract. Used by the monitor task, which holds the full
    /// `netwatch` state needed to compute both values at once.
    ///
    /// Returns [`NetError::InternalError`] on poison, leaving handling to the caller;
    /// never panics.
    fn update_state_inner(
        state: &Arc<Mutex<MonitorState>>,
        dispatcher: &Dispatcher,
        reachability: NetworkStatus,
        ip_stack: IpStack,
    ) -> Result<(), NetError> {
        let listeners = {
            let mut guard = state.lock().map_err(NetError::from_poison)?;
            if !guard.active.load(Ordering::Acquire) {
                return Ok(());
            }
            if let Some(observation) = &guard.observation {
                observation.publish(Some(reachability))?;
            }
            guard.ip_stack = ip_stack;
            if guard.reachability == reachability {
                return Ok(());
            }
            guard.reachability = reachability;
            guard.listeners.values().cloned().collect::<Vec<_>>()
        };

        on_common::log_s!(LogType::Engine;
            "network_status_listener",
            "network_status",
            reachability.name()
        );

        for listener in listeners {
            dispatcher(listener, reachability);
        }
        Ok(())
    }

    /// Monitor task body: ported from the reference project's
    /// `monitor_until_stopped`, but the state comes from the instance rather
    /// than a global.
    ///
    /// This task runs directly on the shared engine runtime and cannot return errors to
    /// the developer; if an internal lock becomes poisoned
    /// (`update_reachability_inner` returns `Err`), the task gracefully exits
    /// the loop and resets the state, and never panics.
    async fn monitor_until_stopped(
        state: Arc<Mutex<MonitorState>>,
        dispatcher: Dispatcher,
        mut stop_receiver: oneshot::Receiver<()>,
        initial_state: watch::Sender<bool>,
    ) {
        let monitor = tokio::select! {
            biased;
            _ = &mut stop_receiver => return,
            result = netwatch::netmon::Monitor::new() => result,
        };
        let monitor = match monitor {
            Ok(monitor) => monitor,
            Err(error) => {
                // A detector failure does not establish that the network is
                // unavailable. The completion guard preserves Unknown internally;
                // the public facade retains its previous default Unavailable value.
                on_common::log_e!(LogType::Engine; "network_status_monitor_start", "error", on_common::log::summary::error(&error));
                let _ = initial_state.send(true);
                return;
            }
        };
        let mut interface_state = monitor.interface_state();

        let initial = interface_state.get();
        let current = Self::current_reachability(&initial);
        let ip_stack = Self::ip_stack_from_state(&initial);
        // If the initial state update fails (lock poisoned), end the task.
        if Self::update_state_inner(&state, &dispatcher, current, ip_stack).is_err() {
            let _ = initial_state.send(true);
            return;
        }
        let _ = initial_state.send(true);

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
                            if Self::update_state_inner(&state, &dispatcher, reachability, ip_stack).is_err() {
                                break;
                            }
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
                    if Self::update_state_inner(&state, &dispatcher, reachability, ip_stack).is_err() {
                        break;
                    }
                }
            }
        }

        // On macOS this drops PathMonitor first. Its binding cancels the
        // monitor and drains the serial callback queue before releasing the
        // callback sender, preventing a late native hint from reaching state
        // reset or a later client generation.
        #[cfg(target_os = "macos")]
        drop(platform_monitor);

        // The task is exiting; silently reset the state. If the lock is
        // poisoned it cannot be reset, so just give up (without panicking).
        if let Ok(mut guard) = state.lock() {
            guard.reachability = NetworkStatus::Unavailable;
            guard.ip_stack = IpStack::None;
        }
    }

    // ------------------------------------------------------------------
    // Windows network reachability (based on the NetworkListManager COM API)
    // ------------------------------------------------------------------

    #[cfg(target_os = "windows")]
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
    fn query_current_network() -> Option<String> {
        Self::query_wifi_network().or_else(Self::query_windows_connected_network)
    }

    /// Query the active Wi-Fi SSID via `netsh wlan show interfaces`.
    #[cfg(target_os = "windows")]
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
    fn query_windows_connected_network() -> Option<String> {
        use windows::Win32::Foundation::RPC_E_CHANGED_MODE;
        use windows::Win32::Networking::NetworkListManager::{
            INetworkListManager, NetworkListManager, NLM_ENUM_NETWORK_CONNECTED,
        };
        use windows::Win32::System::Com::{
            CoCreateInstance, CoInitializeEx, CoUninitialize, CLSCTX_ALL, COINIT_MULTITHREADED,
        };

        struct CoUninit(bool);
        impl Drop for CoUninit {
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
    fn drop(&mut self) {
        self.request_destroy();
    }
}

#[cfg(test)]
mod tests {
    //! Regression coverage for the reported production crash:
    //!
    //! ```text
    //! there is no reactor running, must be called from the context of a Tokio 1.x runtime
    //! ```
    //!
    //! It happened because a registered listener called `tokio::spawn`, and
    //! `net_status` dispatched that listener on CommonEngine's callback thread pool —
    //! bare OS workers with no ambient Tokio runtime. These tests exercise
    //! [`InnerNetStatusClient::invoke_listener`], the single choke point every listener call
    //! now flows through, on a non-runtime thread (exactly like a callback
    //! worker).
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Instant;

    /// Build a single-threaded multi-thread runtime to hand to `invoke_listener`.
    fn test_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("build test runtime")
    }

    /// Spin-wait (off any runtime) until `flag` is set or the deadline passes.
    fn wait_for(flag: &AtomicBool) -> bool {
        let deadline = Instant::now() + Duration::from_secs(2);
        while !flag.load(Ordering::SeqCst) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        flag.load(Ordering::SeqCst)
    }

    #[test]
    fn listener_calling_tokio_spawn_runs_instead_of_panicking_with_handle() {
        // The exact pattern that crashed: the listener calls `tokio::spawn`.
        // With a runtime handle entered, it must spawn successfully — no panic.
        let runtime = test_runtime();
        let handle = runtime.handle().clone();

        let ran = Arc::new(AtomicBool::new(false));
        let ran_in_task = Arc::clone(&ran);
        let listener: SharedListener = Arc::new(move |_status| {
            let ran = Arc::clone(&ran_in_task);
            tokio::spawn(async move {
                ran.store(true, Ordering::SeqCst);
            });
        });

        // Runs on the current (non-runtime) thread, just like a callback worker.
        InnerNetStatusClient::invoke_listener(Some(&handle), &listener, NetworkStatus::Available);

        assert!(
            wait_for(&ran),
            "listener's tokio::spawn should have executed on the entered runtime"
        );
    }

    #[test]
    fn listener_calling_tokio_spawn_without_handle_is_contained() {
        // No runtime handle available: the listener's `tokio::spawn` panics with
        // the reported "there is no reactor running" message. `invoke_listener`
        // must swallow it so the callback worker (and the process) survives.
        let body_ran = Arc::new(AtomicBool::new(false));
        let body_ran_in_listener = Arc::clone(&body_ran);
        let listener: SharedListener = Arc::new(move |_status| {
            body_ran_in_listener.store(true, Ordering::SeqCst);
            // Panics: no ambient runtime on this thread.
            tokio::spawn(async {});
        });

        // Must return normally despite the listener panicking internally.
        InnerNetStatusClient::invoke_listener(None, &listener, NetworkStatus::Available);

        assert!(
            body_ran.load(Ordering::SeqCst),
            "listener body must have been entered before the contained panic"
        );
    }

    #[test]
    fn arbitrary_listener_panic_is_contained() {
        // Any panic from third-party listener code — not just a missing runtime
        // — must be contained rather than unwinding through the callback worker.
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_in_listener = Arc::clone(&calls);
        let listener: SharedListener = Arc::new(move |_status| {
            calls_in_listener.fetch_add(1, Ordering::SeqCst);
            panic!("listener blew up");
        });

        InnerNetStatusClient::invoke_listener(None, &listener, NetworkStatus::Available);
        InnerNetStatusClient::invoke_listener(None, &listener, NetworkStatus::Unavailable);

        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "both panicking invocations must have run and been contained"
        );
    }

    #[test]
    fn non_windows_reachability_truth_table_remains_netwatch_only() {
        let cases = [
            (false, false, false, NetworkStatus::Unavailable),
            (false, true, false, NetworkStatus::Unavailable),
            (false, false, true, NetworkStatus::Unavailable),
            (false, true, true, NetworkStatus::Unavailable),
            (true, false, false, NetworkStatus::Unavailable),
            (true, true, false, NetworkStatus::Available),
            (true, false, true, NetworkStatus::Available),
            (true, true, true, NetworkStatus::Available),
        ];

        for (has_default_route, have_v4, have_v6, expected) in cases {
            assert_eq!(
                InnerNetStatusClient::reachability_from_flags(has_default_route, have_v4, have_v6),
                expected,
                "unexpected result for default_route={has_default_route}, v4={have_v4}, v6={have_v6}"
            );
        }
    }

    #[test]
    fn unchanged_reachability_refreshes_ip_stack_without_dispatching() {
        let state = Arc::new(Mutex::new(MonitorState::default()));
        let dispatches = Arc::new(AtomicUsize::new(0));
        let dispatches_in_callback = Arc::clone(&dispatches);
        let dispatcher: Dispatcher = Arc::new(move |_listener, _status| {
            dispatches_in_callback.fetch_add(1, Ordering::SeqCst);
        });

        InnerNetStatusClient::update_state_inner(
            &state,
            &dispatcher,
            NetworkStatus::Unavailable,
            IpStack::V6Only,
        )
        .expect("state update should succeed");

        let guard = state.lock().expect("test state lock should remain healthy");
        assert_eq!(guard.reachability, NetworkStatus::Unavailable);
        assert_eq!(guard.ip_stack, IpStack::V6Only);
        assert_eq!(dispatches.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn repeated_reachability_is_deduplicated_while_ip_stack_keeps_refreshing() {
        let state = Arc::new(Mutex::new(MonitorState::default()));
        let observed = Arc::new(Mutex::new(Vec::new()));
        let observed_in_listener = Arc::clone(&observed);
        let listener: SharedListener = Arc::new(move |status| {
            observed_in_listener
                .lock()
                .expect("observed status lock should remain healthy")
                .push(status);
        });
        {
            let mut guard = state.lock().expect("test state lock should remain healthy");
            let handle = guard.next_listener_handle();
            guard.listeners.insert(handle, listener);
        }
        let dispatcher: Dispatcher = Arc::new(move |listener, status| listener(status));

        InnerNetStatusClient::update_state_inner(
            &state,
            &dispatcher,
            NetworkStatus::Available,
            IpStack::V4Only,
        )
        .expect("first state update should succeed");
        InnerNetStatusClient::update_state_inner(
            &state,
            &dispatcher,
            NetworkStatus::Available,
            IpStack::DualStack,
        )
        .expect("repeated state update should succeed");

        assert_eq!(
            *observed
                .lock()
                .expect("observed status lock should remain healthy"),
            vec![NetworkStatus::Available]
        );
        let guard = state.lock().expect("test state lock should remain healthy");
        assert_eq!(guard.ip_stack, IpStack::DualStack);
    }

    /// Install controllable completion channels instead of a native monitor so
    /// lifecycle races can be tested without depending on a real NIC change.
    fn install_pending_monitor(
        client: &InnerNetStatusClient,
    ) -> (
        Arc<Mutex<MonitorState>>,
        oneshot::Receiver<()>,
        watch::Sender<bool>,
    ) {
        let state = Arc::new(Mutex::new(MonitorState::default()));
        let active = Arc::clone(&state.lock().unwrap().active);
        let (stop_sender, stop_receiver) = oneshot::channel();
        let (_initial_sender, initial_state) = watch::channel(true);
        let (finished_sender, finished) = watch::channel(false);
        client.lifecycle.lock().unwrap().monitor = Some(MonitorRuntime {
            stop_sender: Some(stop_sender),
            initial_state,
            finished,
            state: Arc::clone(&state),
            active,
        });
        (state, stop_receiver, finished_sender)
    }

    #[tokio::test]
    async fn cancelled_shutdown_keeps_retiring_monitor_for_destroy_to_await() {
        let engine = Arc::new(CommonEngine::new(16, 16).unwrap());
        let client = InnerNetStatusClient::new(engine);
        let (_state, mut stopped, finished) = install_pending_monitor(&client);

        let mut shutdown = Box::pin(client.shutdown());
        tokio::select! {
            biased;
            _ = &mut shutdown => panic!("shutdown must await the monitor completion"),
            _ = std::future::ready(()) => {}
        }
        assert_eq!(stopped.try_recv(), Ok(()));
        drop(shutdown);
        assert_eq!(client.lifecycle.lock().unwrap().stopping.len(), 1);

        let mut destroy = Box::pin(client.destroy());
        tokio::select! {
            biased;
            _ = &mut destroy => panic!("destroy must also await the cancelled shutdown's monitor"),
            _ = std::future::ready(()) => {}
        }
        assert_eq!(client.start().await, Err(NetError::EngineDropped));
        finished.send_replace(true);
        tokio::time::timeout(Duration::from_secs(2), destroy)
            .await
            .unwrap()
            .unwrap();
    }

    #[test]
    fn stopped_monitor_cannot_overwrite_state_or_dispatch_after_restart() {
        let engine = Arc::new(CommonEngine::new(16, 16).unwrap());
        let client = InnerNetStatusClient::new(engine);
        let (old_state, _old_stopped, old_finished) = install_pending_monitor(&client);
        let listener: SharedListener = Arc::new(|_| {});
        {
            let mut old = old_state.lock().unwrap();
            let handle = old.next_listener_handle();
            old.listeners.insert(handle, listener);
            old.reachability = NetworkStatus::Available;
            old.ip_stack = IpStack::DualStack;
        }
        client.request_stop(false);
        let (new_state, _new_stopped, new_finished) = install_pending_monitor(&client);
        let dispatches = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&dispatches);
        let dispatcher: Dispatcher = Arc::new(move |_, _| {
            observed.fetch_add(1, Ordering::SeqCst);
        });
        InnerNetStatusClient::update_state_inner(
            &new_state,
            &dispatcher,
            NetworkStatus::Available,
            IpStack::V6Only,
        )
        .unwrap();
        InnerNetStatusClient::update_state_inner(
            &old_state,
            &dispatcher,
            NetworkStatus::Available,
            IpStack::V4Only,
        )
        .unwrap();
        InnerNetStatusClient::update_reachability_inner(
            &old_state,
            &dispatcher,
            NetworkStatus::Available,
        )
        .unwrap();
        let old = old_state.lock().unwrap();
        assert_eq!(old.reachability, NetworkStatus::Unavailable);
        assert_eq!(old.ip_stack, IpStack::None);
        assert!(old.listeners.is_empty());
        drop(old);
        assert_eq!(
            new_state.lock().unwrap().reachability,
            NetworkStatus::Available
        );
        assert_eq!(client.ip_stack(), Ok(IpStack::V6Only));
        assert_eq!(dispatches.load(Ordering::SeqCst), 0);
        old_finished.send_replace(true);
        new_finished.send_replace(true);
    }

    #[test]
    fn stop_drops_listener_captures_outside_lifecycle_and_state_locks() {
        struct ReenterOnDrop {
            client: std::sync::Weak<InnerNetStatusClient>,
            dropped: Arc<AtomicBool>,
        }
        impl Drop for ReenterOnDrop {
            fn drop(&mut self) {
                if let Some(client) = self.client.upgrade() {
                    assert!(!client.is_started());
                    assert_eq!(client.ip_stack(), Ok(IpStack::None));
                }
                self.dropped.store(true, Ordering::SeqCst);
            }
        }
        let engine = Arc::new(CommonEngine::new(16, 16).unwrap());
        let client = Arc::new(InnerNetStatusClient::new(engine));
        let (_state, _stopped, finished) = install_pending_monitor(&client);
        let dropped = Arc::new(AtomicBool::new(false));
        let capture = ReenterOnDrop {
            client: Arc::downgrade(&client),
            dropped: Arc::clone(&dropped),
        };
        client
            .register(Box::new(move |_| {
                let _ = &capture;
            }))
            .unwrap();
        client.request_destroy();
        assert!(dropped.load(Ordering::SeqCst));
        finished.send_replace(true);
    }

    #[test]
    fn completion_guard_notifies_even_when_unpolled_task_is_dropped() {
        let (sender, receiver) = watch::channel(false);
        let completion = MonitorCompletion(sender);
        let future = async move {
            let _completion = completion;
            std::future::pending::<()>().await;
        };
        drop(future);
        assert!(*receiver.borrow());
    }
}

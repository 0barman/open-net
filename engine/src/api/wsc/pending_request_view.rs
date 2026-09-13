use crate::api::net_error::NetError;
use crate::api::traits::ws::ws_request_config::WSRequestConfig;
use crate::api::traits::ws::ws_request_trait::WSRequestTrait;
use crate::api::wsc::pending_request_completion::PendingRequestCompletion;
use crate::api::wsc::pending_request_entry::PendingRequestEntry;
use crate::api::wsc::pending_request_info::PendingRequestInfo;
use crate::api::wsc::pending_request_status::PendingRequestStatus;
use crate::api::wsc::request_registration::{
    RegistrationControl, RegistrationState, RequestRegistration, RequestRegistrationToken,
    RequestTerminationOutcome,
};
use crate::api::wsc::request_scope::RequestScope;
use crate::module::ws_client::task_observer::TaskObservation;
use on_common::log::log_def::LogType;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

#[path = "pending_request_view/registration_deadlines.rs"]
mod registration_deadlines;

#[derive(Clone)]
pub struct PendingRequestView {
    entries: Arc<RwLock<HashMap<String, PendingRequestEntry>>>,
    response_dispatches: Arc<Mutex<HashMap<u64, ResponseDispatchState>>>,
    response_dispatch_changed: Arc<Notify>,
    /// 写任务尚在等待 `Sink::send` 结果时，响应已被认领的请求标记。
    /// 写任务消费这些短期标记，使已成功收到的业务响应优先于随后投递结果不明确的
    /// 写缓冲刷新错误，同时避免永久保留已完成请求的占位记录。
    write_response_claims: Arc<Mutex<HashSet<u64>>>,
    max_entries: usize,
    pub(crate) deadline_changed: Arc<Notify>,
}

#[derive(Default)]
struct ResponseDispatchState {
    in_flight: usize,
    terminal_error: Option<NetError>,
    force_scheduled: bool,
}

/// 让连接代次清理等待回调通道已接收的响应先完成处理。
pub(crate) struct PendingResponseGuard {
    pending_requests: PendingRequestView,
    generation: u64,
}

impl Drop for PendingResponseGuard {
    fn drop(&mut self) {
        on_common::log_t!(LogType::WSC; "drop");
        self.pending_requests
            .finish_response_dispatch(self.generation);
    }
}

impl Default for PendingRequestView {
    fn default() -> Self {
        on_common::log_t!(LogType::WSC; "default");
        Self::with_capacity(usize::MAX)
    }
}

impl PendingRequestView {
    pub(crate) fn with_capacity(max_entries: usize) -> Self {
        on_common::log_t!(LogType::WSC; "with_capacity", "max_entries", max_entries);
        Self {
            entries: Arc::new(RwLock::new(HashMap::new())),
            response_dispatches: Arc::new(Mutex::new(HashMap::new())),
            response_dispatch_changed: Arc::new(Notify::new()),
            write_response_claims: Arc::new(Mutex::new(HashSet::new())),
            max_entries,
            deadline_changed: Arc::new(Notify::new()),
        }
    }

    pub(in crate::api::wsc) fn same_table(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.entries, &other.entries)
    }

    /// 仅在请求属于指定物理连接代次且绑定的业务 scope 未撤销时返回该请求。
    pub fn get_request(&self, uuid: &str, generation: u64) -> Option<Arc<dyn WSRequestTrait>> {
        on_common::log_t!(LogType::WSC; "get_request", "uuid|generation", uuid, generation);
        self.entries
            .read()
            .map_err(|_| {
                on_common::log_e!(LogType::WSC; "get_request", "error", "pending_lock_poisoned");
            })
            .ok()
            .and_then(|entries| {
                entries.get(uuid).and_then(|entry| {
                    (entry.info.connection_generation == Some(generation)
                        && !entry.scope.as_ref().is_some_and(RequestScope::is_cancelled))
                    .then(|| Arc::clone(&entry.request))
                })
            })
    }

    /// 仅在请求属于指定物理连接代次且绑定的业务 scope 未撤销时原子取出。
    ///
    /// 调用方应传入当前 [`crate::WSCResponse::connection_generation`]；代次校验可防止
    /// 重连前迟到的响应认领新连接上复用同一 UUID 的请求。返回 `None` 时，消息可按
    /// 服务器通知或无法关联的迟到响应处理。
    pub fn take_request(&self, uuid: &str, generation: u64) -> Option<Arc<dyn WSRequestTrait>> {
        on_common::log_t!(LogType::WSC; "take_request", "uuid|generation", uuid, generation);
        let mut entries = self
            .entries
            .write()
            .map_err(|_| {
                on_common::log_e!(LogType::WSC; "take_request", "error", "pending_lock_poisoned");
            })
            .ok()?;
        let matches_generation = entries.get(uuid).is_some_and(|entry| {
            entry.info.connection_generation == Some(generation)
                && !entry.scope.as_ref().is_some_and(RequestScope::is_cancelled)
        });
        let entry = matches_generation.then(|| entries.remove(uuid)).flatten();
        if let Some(entry) = &entry {
            if entry.info.status == PendingRequestStatus::Writing && entry.deferred_write.is_none()
            {
                self.write_response_claims
                    .lock()
                    .unwrap_or_else(|poisoned| { on_common::log_e!(LogType::WSC; "take_request", "error", "lock_poisoned_recovered"); poisoned.into_inner() })
                    .insert(entry.token);
            }
        }
        drop(entries);
        let request = entry.map(|entry| entry.complete(Ok(())));
        if request.is_some() {
            self.response_dispatch_changed.notify_waiters();
        }
        on_common::log_s!(LogType::WSC; "take_request", "uuid|generation|claimed", uuid, generation, request.is_some());
        request
    }

    pub(in crate::api::wsc) fn take_registered_request(
        &self,
        registration: &RequestRegistration,
        generation: u64,
    ) -> Result<Option<Arc<dyn WSRequestTrait>>, NetError> {
        if !registration.belongs_to(self) {
            return Ok(None);
        }
        let mut entries = self.entries.write().map_err(|_| {
            on_common::log_e!(LogType::WSC; "take_registered_request", "error", "pending_lock_poisoned");
            NetError::InternalError
        })?;
        let matches = entries.get(registration.request_id()).is_some_and(|entry| {
            entry.token == registration.raw_token()
                && entry.info.connection_generation == Some(generation)
                && !entry.scope.as_ref().is_some_and(RequestScope::is_cancelled)
        });
        if !matches {
            return Ok(None);
        }
        // Acquire the short-lived write-claim marker before removing the entry. A poisoned
        // marker lock must return an error without losing the pending completion authority.
        let writing = entries.get(registration.request_id()).is_some_and(|entry| {
            entry.info.status == PendingRequestStatus::Writing && entry.deferred_write.is_none()
        });
        if writing {
            self.write_response_claims.lock().map_err(|_| {
                on_common::log_e!(LogType::WSC; "take_registered_request", "error", "write_claim_lock_poisoned");
                NetError::InternalError
            })?.insert(registration.raw_token());
        }
        let entry = entries.remove(registration.request_id());
        drop(entries);
        let request = entry.map(|entry| entry.complete(Ok(())));
        if request.is_some() {
            self.response_dispatch_changed.notify_waiters();
        }
        Ok(request)
    }

    pub(in crate::api::wsc) fn terminate_registration(
        &self,
        registration: &RequestRegistration,
        requested: NetError,
    ) -> Result<RequestTerminationOutcome, NetError> {
        if !registration.belongs_to(self) {
            return Ok(RequestTerminationOutcome::StaleRegistration);
        }
        let mut entries = self.entries.write().map_err(|_| {
            on_common::log_e!(LogType::WSC; "terminate_registration", "error", "pending_lock_poisoned");
            NetError::InternalError
        })?;
        let Some(entry) = entries.get(registration.request_id()) else {
            return Ok(RequestTerminationOutcome::AlreadyClaimedOrFinished);
        };
        if entry.token != registration.raw_token() {
            return Ok(RequestTerminationOutcome::StaleRegistration);
        }
        // Compete with writer before releasing the pending lock. Cancelling the queue is
        // deliberately deferred: queue completion may call back into the pending table.
        let requested = entry.first_deferred_error.unwrap_or(requested);
        let selected = entry
            .registration_control
            .as_ref()
            .map_or(requested, |control| control.select_cancellation(requested));
        let entry = entries
            .remove(registration.request_id())
            .ok_or(NetError::InternalError)?;
        drop(entries);
        if let Some(control) = &entry.registration_control {
            control.finish_cancellation(selected);
        }
        let observation = entry.observation.clone();
        entry.complete(Err(selected));
        let selected = match observation.and_then(|observation| observation.selected_result()) {
            Some(Err(error)) => error,
            _ => selected,
        };
        self.response_dispatch_changed.notify_waiters();
        Ok(RequestTerminationOutcome::Terminated { error: selected })
    }

    /// 消费响应在 `Sink::send` 完成之前被认领时留下的标记。
    pub(crate) fn take_write_response_claim(&self, token: u64) -> bool {
        on_common::log_t!(LogType::WSC; "take_write_response_claim", "token", token);
        self.write_response_claims
            .lock()
            .unwrap_or_else(|poisoned| { on_common::log_e!(LogType::WSC; "take_write_response_claim", "error", "lock_poisoned_recovered"); poisoned.into_inner() })
            .remove(&token)
    }

    pub fn snapshot(&self) -> Vec<PendingRequestInfo> {
        on_common::log_t!(LogType::WSC; "snapshot");
        self.entries
            .read()
            .map(|entries| entries.values().map(|entry| entry.info.clone()).collect())
            .unwrap_or_else(|_| {
                on_common::log_e!(LogType::WSC; "snapshot", "error", "pending_lock_poisoned");
                Vec::new()
            })
    }

    pub fn len(&self) -> usize {
        on_common::log_t!(LogType::WSC; "len");
        self.entries
            .read()
            .map(|entries| entries.len())
            .unwrap_or_else(|_| {
                on_common::log_e!(LogType::WSC; "len", "error", "pending_lock_poisoned");
                0
            })
    }

    pub fn is_empty(&self) -> bool {
        on_common::log_t!(LogType::WSC; "is_empty");
        self.len() == 0
    }

    #[cfg(test)]
    pub(crate) fn reserve(
        &self,
        request: Arc<dyn WSRequestTrait>,
        config: &WSRequestConfig,
    ) -> Result<(u64, PendingRequestCompletion), NetError> {
        self.reserve_observed(request, config, None, None)
    }

    #[cfg(test)]
    pub(crate) fn reserve_observed(
        &self,
        request: Arc<dyn WSRequestTrait>,
        config: &WSRequestConfig,
        observation: Option<Arc<TaskObservation>>,
        admission: Option<&CancellationToken>,
    ) -> Result<(u64, PendingRequestCompletion), NetError> {
        self.reserve_uuid_observed(request.uuid(), request, config, observation, admission)
    }

    pub(crate) fn reserve_uuid_observed(
        &self,
        uuid: String,
        request: Arc<dyn WSRequestTrait>,
        config: &WSRequestConfig,
        observation: Option<Arc<TaskObservation>>,
        admission: Option<&CancellationToken>,
    ) -> Result<(u64, PendingRequestCompletion), NetError> {
        self.reserve_snapshot_observed(uuid, request, config, observation, admission, None, None)
            .map(|(registration, completion)| (registration.raw_token(), completion))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn reserve_snapshot_observed(
        &self,
        uuid: String,
        request: Arc<dyn WSRequestTrait>,
        config: &WSRequestConfig,
        observation: Option<Arc<TaskObservation>>,
        admission: Option<&CancellationToken>,
        registration_control: Option<RegistrationControl>,
        scope: Option<RequestScope>,
    ) -> Result<(RequestRegistration, PendingRequestCompletion), NetError> {
        self.reserve_timed_snapshot_observed(
            uuid,
            request,
            config,
            observation,
            admission,
            registration_control,
            scope,
            None,
            crate::ResponseDeadlineOrigin::AfterWritten,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn reserve_timed_snapshot_observed(
        &self,
        uuid: String,
        request: Arc<dyn WSRequestTrait>,
        config: &WSRequestConfig,
        observation: Option<Arc<TaskObservation>>,
        admission: Option<&CancellationToken>,
        registration_control: Option<RegistrationControl>,
        scope: Option<RequestScope>,
        registration_deadline: Option<Instant>,
        origin: crate::ResponseDeadlineOrigin,
    ) -> Result<(RequestRegistration, PendingRequestCompletion), NetError> {
        on_common::log_t!(LogType::WSC; "reserve", "request_type|config", "WSRequestTrait", format!("{:?}", config));
        if uuid.trim().is_empty() {
            on_common::log_e!(LogType::WSC; "reserve", "error", "ParameterEmpty");
            return Err(NetError::ParameterEmpty);
        }
        // 持有请求表锁时绝不能执行用户的特征实现。即使实现缓慢或发生恐慌，
        // 也不能阻塞响应认领及清理，或使相关锁中毒。
        let extension = request.request_extension();
        let mut entries = self.entries.write().map_err(|_| {
            on_common::log_e!(LogType::WSC; "reserve", "error", "pending_lock_poisoned");
            NetError::InternalError
        })?;
        // 与生命周期清空操作串行执行：已取消的准入凭证不能在清理后注册请求。
        if scope.as_ref().is_some_and(RequestScope::is_cancelled) {
            return Err(NetError::Cancelled);
        }
        if admission.is_some_and(CancellationToken::is_cancelled) {
            return Err(NetError::ConnectionClosed);
        }
        if entries.contains_key(uuid.as_str()) {
            on_common::log_e!(LogType::WSC; "reserve", "uuid|error", uuid, "DuplicateRequestId");
            return Err(NetError::DuplicateRequestId);
        }
        if entries.len() >= self.max_entries {
            on_common::log_e!(LogType::WSC; "reserve", "uuid|capacity|error", uuid, self.max_entries, "PendingRequestLimitReached");
            return Err(NetError::PendingRequestLimitReached);
        }
        let registered_at = tokio::time::Instant::now().into_std();
        if registration_deadline.is_some_and(|deadline| registered_at >= deadline) {
            return Err(NetError::TimeoutError);
        }
        let registration_state = Arc::new(RegistrationState::new(
            registered_at,
            config.response_timeout,
            origin,
        )?);
        let registration_token = RequestRegistrationToken::allocate()?;
        let token = registration_token.raw();
        let (completion_tx, completion_rx) = tokio::sync::oneshot::channel();
        let response_timeout_cancel = CancellationToken::new();
        let registration = RequestRegistration::new(
            self.clone(),
            uuid.clone(),
            registration_token,
            response_timeout_cancel.clone(),
            Arc::clone(&registration_state),
            registration_control.clone(),
        );
        entries.insert(
            uuid.clone(),
            PendingRequestEntry {
                info: PendingRequestInfo {
                    uuid: uuid.clone(),
                    extension,
                    priority: config.priority,
                    status: PendingRequestStatus::Queued,
                    queued_at: registered_at,
                    sent_at: None,
                    send_attempt: 0,
                    connection_generation: None,
                },
                request,
                token,
                completion_tx: Some(completion_tx),
                first_deferred_error: None,
                response_timeout_cancel,
                observation,
                registration_control,
                scope,
                registration_state,
                registration_grace_deadline: None,
                deferred_write: None,
            },
        );
        let count = entries.len();
        drop(entries);
        if origin == crate::ResponseDeadlineOrigin::AtRegistration {
            self.deadline_changed.notify_one();
        }
        on_common::log_s!(LogType::WSC; "reserve", "uuid|token|state|pending_count", uuid, token, "Queued", count);
        Ok((
            registration,
            PendingRequestCompletion::new(uuid, completion_rx),
        ))
    }

    pub(crate) fn mark_writing(
        &self,
        uuid: &str,
        token: u64,
        attempt: u32,
        generation: u64,
    ) -> bool {
        on_common::log_t!(LogType::WSC; "mark_writing", "uuid|token|attempt|generation", uuid, token, attempt, generation);
        let Ok(mut entries) = self.entries.write() else {
            on_common::log_e!(LogType::WSC; "mark_writing", "error", "pending_lock_poisoned");
            return false;
        };
        let Some(entry) = entries.get_mut(uuid) else {
            on_common::log_s!(LogType::WSC; "mark_writing", "uuid|state", uuid, "request_missing");
            return false;
        };
        if entry.token != token {
            on_common::log_s!(LogType::WSC; "mark_writing", "uuid|token|state", uuid, token, "stale_token");
            return false;
        }
        entry.info.status = PendingRequestStatus::Writing;
        entry.info.send_attempt = attempt;
        entry.info.connection_generation = Some(generation);
        drop(entries);
        on_common::log_s!(LogType::WSC; "mark_writing", "uuid|token|attempt|generation|state", uuid, token, attempt, generation, "Writing");
        true
    }

    pub(crate) fn mark_queued(&self, uuid: &str, token: u64) -> bool {
        on_common::log_t!(LogType::WSC; "mark_queued", "uuid|token", uuid, token);
        let Ok(mut entries) = self.entries.write() else {
            on_common::log_e!(LogType::WSC; "mark_queued", "error", "pending_lock_poisoned");
            return false;
        };
        let Some(entry) = entries.get_mut(uuid) else {
            on_common::log_s!(LogType::WSC; "mark_queued", "uuid|state", uuid, "request_missing");
            return false;
        };
        if entry.token != token {
            on_common::log_s!(LogType::WSC; "mark_queued", "uuid|token|state", uuid, token, "stale_token");
            return false;
        }
        entry.info.status = PendingRequestStatus::Queued;
        entry.info.connection_generation = None;
        drop(entries);
        on_common::log_s!(LogType::WSC; "mark_queued", "uuid|token|state", uuid, token, "Queued");
        true
    }

    pub(crate) fn mark_sent(&self, uuid: &str, token: u64) -> Option<CancellationToken> {
        on_common::log_t!(LogType::WSC; "mark_sent", "uuid|token", uuid, token);
        let Ok(mut entries) = self.entries.write() else {
            on_common::log_e!(LogType::WSC; "mark_sent", "error", "pending_lock_poisoned");
            return None;
        };
        let entry = entries.get_mut(uuid)?;
        if entry.token != token {
            on_common::log_s!(LogType::WSC; "mark_sent", "uuid|token|state", uuid, token, "stale_token");
            return None;
        }
        let written_at = tokio::time::Instant::now().into_std();
        if let Err(error) = entry.registration_state.mark_written(written_at) {
            on_common::log_e!(LogType::WSC; "mark_sent", "error", format!("{error:?}"));
            let entry = entries.remove(uuid);
            drop(entries);
            if let Some(entry) = entry {
                entry.complete(Err(error));
            }
            return None;
        }
        entry.info.status = PendingRequestStatus::AwaitingResponse;
        entry.info.sent_at = Some(written_at);
        let starts_after_write =
            entry.registration_state.origin() == crate::ResponseDeadlineOrigin::AfterWritten;
        let cancel = entry.response_timeout_cancel.clone();
        drop(entries);
        on_common::log_s!(LogType::WSC; "mark_sent", "uuid|token|state", uuid, token, "AwaitingResponse");
        starts_after_write.then_some(cancel)
    }

    pub(crate) fn response_deadline(
        &self,
        uuid: &str,
        token: u64,
    ) -> Result<Option<Instant>, NetError> {
        let entries = self.entries.read().map_err(|_| NetError::InternalError)?;
        match entries.get(uuid).filter(|entry| entry.token == token) {
            Some(entry) => entry.registration_state.deadline(),
            None => Ok(None),
        }
    }

    pub(crate) fn remove_if_token(
        &self,
        uuid: &str,
        token: u64,
        error: NetError,
    ) -> Option<Arc<dyn WSRequestTrait>> {
        on_common::log_t!(LogType::WSC; "remove_if_token", "uuid|token|error", uuid, token, format!("{:?}", error));
        let mut entries = self.entries.write().map_err(|_| {
            on_common::log_e!(LogType::WSC; "remove_if_token", "error", "pending_lock_poisoned");
        }).ok()?;
        let token_matches = entries.get(uuid).is_some_and(|entry| entry.token == token);
        let entry = token_matches.then(|| entries.remove(uuid)).flatten();
        drop(entries);
        let request = entry.map(|entry| entry.complete(Err(error)));
        if request.is_some() {
            self.response_dispatch_changed.notify_waiters();
        }
        on_common::log_s!(LogType::WSC; "remove_if_token", "uuid|token|removed", uuid, token, request.is_some());
        request
    }

    /// 判断一条仍存在的请求所属连接代是否有已进入回调通道的响应。
    ///
    /// 通用传输层尚未解析业务 UUID，因此这是按连接代次进行的保守判断：同代任意响应
    /// 正在排队或执行，均可为本请求触发有界宽限。读取请求记录后会先释放其锁，再读取
    /// 分发状态，避免与终态清理路径形成嵌套锁顺序。
    pub(crate) fn response_dispatch_in_flight(&self, uuid: &str, token: u64) -> bool {
        on_common::log_t!(LogType::WSC; "response_dispatch_in_flight", "uuid|token", uuid, token);
        let generation = self.entries.read().inspect_err(|_| { on_common::log_e!(LogType::WSC; "response_dispatch_in_flight", "error", "lock_poisoned"); }).ok().and_then(|entries| {
            entries.get(uuid).and_then(|entry| {
                (entry.token == token)
                    .then_some(entry.info.connection_generation)
                    .flatten()
            })
        });
        generation.is_some_and(|generation| {
            self.response_dispatches
                .lock().inspect_err(|_| { on_common::log_e!(LogType::WSC; "response_dispatch_in_flight", "error", "lock_poisoned"); })
                .ok()
                .and_then(|dispatches| {
                    dispatches
                        .get(&generation)
                        .map(|dispatch| dispatch.in_flight > 0)
                })
                .unwrap_or(false)
        })
    }

    /// 为仍匹配请求标记的请求记录进入宽限时最先观察到的错误。
    pub(crate) fn record_deferred_error(&self, uuid: &str, token: u64, error: NetError) -> bool {
        on_common::log_t!(LogType::WSC; "record_deferred_error", "uuid|token|error", uuid, token, format!("{:?}", error));
        let Ok(mut entries) = self.entries.write() else {
            on_common::log_e!(LogType::WSC; "record_deferred_error", "error", "pending_lock_poisoned");
            return false;
        };
        let Some(entry) = entries.get_mut(uuid) else {
            return false;
        };
        if entry.token != token {
            on_common::log_s!(LogType::WSC; "record_deferred_error", "uuid|token|state", uuid, token, "stale_token");
            return false;
        }
        let first_error = entry.record_deferred_error(error);
        drop(entries);
        on_common::log_s!(LogType::WSC; "record_deferred_error", "uuid|token|first_error|state", uuid, token, format!("{:?}", first_error), "response_grace");
        true
    }

    /// 结束所有待响应请求，但给已进入回调通道的响应一个有界认领窗口。
    pub(crate) fn fail_all_with_response_grace(
        &self,
        error: NetError,
        response_dispatch_grace: Duration,
    ) {
        on_common::log_t!(LogType::WSC; "fail_all_with_response_grace", "error|response_dispatch_grace", format!("{:?}", error), format!("{:?}", response_dispatch_grace));
        if let Ok(mut entries) = self.entries.write().inspect_err(|_| { on_common::log_e!(LogType::WSC; "fail_all_with_response_grace", "error", "lock_poisoned"); }) {
            for entry in entries.values_mut() {
                entry.record_deferred_error(error);
            }
        }
        let (deferred, newly_scheduled) =
            if let Ok(mut dispatches) = self.response_dispatches.lock().inspect_err(|_| { on_common::log_e!(LogType::WSC; "fail_all_with_response_grace", "error", "lock_poisoned"); }) {
                let mut deferred = HashSet::new();
                let mut newly_scheduled = Vec::new();
                for (generation, dispatch) in dispatches.iter_mut() {
                    if dispatch.in_flight == 0 {
                        continue;
                    }
                    deferred.insert(*generation);
                    dispatch.terminal_error.get_or_insert(error);
                    if !dispatch.force_scheduled {
                        dispatch.force_scheduled = true;
                        newly_scheduled.push(*generation);
                    }
                }
                (deferred, newly_scheduled)
            } else {
                (HashSet::new(), Vec::new())
            };
        let drained = self
            .entries
            .write().inspect_err(|_| { on_common::log_e!(LogType::WSC; "fail_all_with_response_grace", "error", "lock_poisoned"); })
            .map(|mut entries| {
                let immediate = entries
                    .iter()
                    .filter_map(|(uuid, entry)| {
                        (!entry
                            .info
                            .connection_generation
                            .is_some_and(|generation| deferred.contains(&generation)))
                        .then_some(uuid.clone())
                    })
                    .collect::<Vec<_>>();
                immediate
                    .into_iter()
                    .filter_map(|uuid| entries.remove(uuid.as_str()))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        on_common::log_s!(LogType::WSC; "fail_all_with_response_grace", "immediate_count|deferred_generations|new_timers", drained.len(), deferred.len(), newly_scheduled.len());
        for entry in drained {
            entry.complete(Err(error));
        }
        for generation in newly_scheduled {
            self.schedule_force_finish_response_dispatch(
                generation,
                error,
                response_dispatch_grace,
            );
        }
    }

    /// 结束属于某一物理连接代次的请求，保留尚未绑定连接的排队请求。
    pub(crate) fn fail_for_generation(
        &self,
        generation: u64,
        error: NetError,
        response_dispatch_grace: Duration,
    ) {
        on_common::log_t!(LogType::WSC; "fail_for_generation", "generation|error|response_dispatch_grace", generation, format!("{:?}", error), format!("{:?}", response_dispatch_grace));
        if let Ok(mut entries) = self.entries.write().inspect_err(|_| {
            on_common::log_e!(LogType::WSC; "fail_for_generation", "error", "lock_poisoned");
        }) {
            for entry in entries
                .values_mut()
                .filter(|entry| entry.info.connection_generation == Some(generation))
            {
                entry.record_deferred_error(error);
            }
        }
        let (defer, schedule) = if let Ok(mut dispatches) =
            self.response_dispatches.lock().inspect_err(|_| {
                on_common::log_e!(LogType::WSC; "fail_for_generation", "error", "lock_poisoned");
            }) {
            match dispatches.get_mut(&generation) {
                Some(dispatch) if dispatch.in_flight > 0 => {
                    dispatch.terminal_error.get_or_insert(error);
                    if dispatch.force_scheduled {
                        (true, false)
                    } else {
                        dispatch.force_scheduled = true;
                        (true, true)
                    }
                }
                _ => {
                    dispatches.remove(&generation);
                    (false, false)
                }
            }
        } else {
            (false, false)
        };
        if schedule {
            self.schedule_force_finish_response_dispatch(
                generation,
                error,
                response_dispatch_grace,
            );
        }
        if defer {
            on_common::log_s!(LogType::WSC; "fail_for_generation", "generation|state|timer_scheduled", generation, "response_grace", schedule);
            return;
        }
        self.fail_generation_now(generation, error);
    }

    /// 注册读取任务已成功接收、准备交给回调处理的响应。
    pub(crate) fn begin_response_dispatch(&self, generation: u64) -> PendingResponseGuard {
        on_common::log_t!(LogType::WSC; "begin_response_dispatch", "generation", generation);
        let mut dispatches = self
            .response_dispatches
            .lock()
            .unwrap_or_else(|poisoned| { on_common::log_e!(LogType::WSC; "begin_response_dispatch", "error", "lock_poisoned_recovered"); poisoned.into_inner() });
        let dispatch = dispatches.entry(generation).or_default();
        dispatch.in_flight = dispatch.in_flight.saturating_add(1);
        let in_flight = dispatch.in_flight;
        drop(dispatches);
        on_common::log_s!(LogType::WSC; "begin_response_dispatch", "generation|in_flight", generation, in_flight);
        PendingResponseGuard {
            pending_requests: self.clone(),
            generation,
        }
    }

    fn finish_response_dispatch(&self, generation: u64) {
        on_common::log_t!(LogType::WSC; "finish_response_dispatch", "generation", generation);
        let terminal_error = if let Ok(mut dispatches) = self.response_dispatches.lock().inspect_err(|_| { on_common::log_e!(LogType::WSC; "finish_response_dispatch", "error", "lock_poisoned"); }) {
            let Some(dispatch) = dispatches.get_mut(&generation) else {
                return;
            };
            dispatch.in_flight = dispatch.in_flight.saturating_sub(1);
            if dispatch.in_flight == 0 {
                dispatches
                    .remove(&generation)
                    .and_then(|dispatch| dispatch.terminal_error)
            } else {
                None
            }
        } else {
            None
        };
        if let Some(error) = terminal_error {
            on_common::log_s!(LogType::WSC; "finish_response_dispatch", "generation|state", generation, "grace_completed");
            self.fail_generation_now(generation, error);
        }
        self.response_dispatch_changed.notify_waiters();
    }

    fn force_finish_response_dispatch(&self, generation: u64, fallback_error: NetError) {
        on_common::log_t!(LogType::WSC; "force_finish_response_dispatch", "generation|fallback_error", generation, format!("{:?}", fallback_error));
        let error = self
            .response_dispatches
            .lock().inspect_err(|_| { on_common::log_e!(LogType::WSC; "force_finish_response_dispatch", "error", "lock_poisoned"); })
            .ok()
            .and_then(|mut dispatches| dispatches.remove(&generation))
            .and_then(|dispatch| dispatch.terminal_error)
            .unwrap_or(fallback_error);
        on_common::log_s!(LogType::WSC; "force_finish_response_dispatch", "generation|state|error", generation, "grace_expired", format!("{:?}", error));
        self.fail_generation_now(generation, error);
    }

    fn schedule_force_finish_response_dispatch(
        &self,
        generation: u64,
        error: NetError,
        grace: Duration,
    ) {
        on_common::log_t!(LogType::WSC; "schedule_force_finish_response_dispatch", "generation|error|grace", generation, format!("{:?}", error), format!("{:?}", grace));
        if grace.is_zero() {
            on_common::log_s!(LogType::WSC; "schedule_force_finish_response_dispatch", "generation|state", generation, "grace_disabled");
            self.force_finish_response_dispatch(generation, error);
            return;
        }
        // 在创建操作系统线程前计算截止时间。调度延迟必须消耗宽限时间，
        // 不能在线程真正运行后重新等待完整时长，导致宽限被延长。
        let deadline = Instant::now().checked_add(grace);
        let pending_requests = self.clone();
        if let Err(spawn_error) = std::thread::Builder::new()
            .name("open-net-ws-response-grace".to_string())
            .spawn(move || {
                if let Some(deadline) = deadline {
                    std::thread::sleep(deadline.saturating_duration_since(Instant::now()));
                }
                pending_requests.force_finish_response_dispatch(generation, error);
            })
        {
            on_common::log_e!(LogType::WSC; "schedule_force_finish_response_dispatch", "generation|error", generation, spawn_error.to_string());
            self.force_finish_response_dispatch(generation, error);
        }
    }

    /// 立即结束终态宽限开始后仍然保留的所有待响应请求记录。
    ///
    /// 客户端永久关闭时，在回调排空截止时间之后、发布关闭完成通知之前调用本方法。
    /// 本方法独立于 `response_dispatches` 清空 `entries`：响应守卫或独立的强制清理定时器
    /// 可能已经移除分发状态，但对应请求记录的清理仍在进行。各连接代次的定时器之后
    /// 可能被唤醒，但由于找不到状态或记录，不会再执行操作。`PendingRequestEntry::complete`
    /// 优先保留各记录最先延后的终态错误，而不使用 `fallback_error` 覆盖它。
    pub(crate) fn force_finish_all_response_dispatches(&self, fallback_error: NetError) {
        on_common::log_t!(LogType::WSC; "force_finish_all_response_dispatches", "fallback_error", format!("{:?}", fallback_error));
        if let Ok(mut dispatches) = self.response_dispatches.lock().inspect_err(|_| { on_common::log_e!(LogType::WSC; "force_finish_all_response_dispatches", "error", "lock_poisoned"); }) {
            dispatches.clear();
        }
        let failed = self
            .entries
            .write().inspect_err(|_| { on_common::log_e!(LogType::WSC; "force_finish_all_response_dispatches", "error", "lock_poisoned"); })
            .map(|mut entries| entries.drain().map(|(_, entry)| entry).collect::<Vec<_>>())
            .unwrap_or_default();
        on_common::log_s!(LogType::WSC; "force_finish_all_response_dispatches", "failed_count", failed.len());
        for entry in failed {
            entry.complete(Err(fallback_error));
        }
        self.response_dispatch_changed.notify_waiters();
    }

    /// 等待已接收响应不再能够认领任何待响应请求，或等待至截止时间。
    ///
    /// 监听器可能将 `WSCResponse` 移交给应用运行时后立即返回，因此仅凭回调通道排空屏障
    /// 不能证明响应分发已经完成。终态处理调用 `fail_all_with_response_grace` 后，
    /// 所有未延期的记录均已移除，因此以 `entries` 是否为空作为清理完成的可靠判断依据。
    /// 直接检查请求记录还覆盖了响应守卫已移除分发表状态、但尚未移除对应请求记录的时间窗口。
    /// 这一第二阶段保留了异步移交响应的机会，且不会延长唯一的终态宽限截止时间。
    pub(crate) async fn wait_for_deferred_responses_until(
        &self,
        deadline: Option<tokio::time::Instant>,
    ) {
        on_common::log_t!(LogType::WSC; "wait_for_deferred_responses_until", "deadline", format!("{:?}", deadline));
        let Some(deadline) = deadline else {
            return;
        };
        loop {
            let notified = self.response_dispatch_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !self.has_pending_entries() {
                on_common::log_s!(LogType::WSC; "wait_for_deferred_responses_until", "state", "all_responses_finished");
                return;
            }
            if tokio::time::Instant::now() >= deadline {
                on_common::log_s!(LogType::WSC; "wait_for_deferred_responses_until", "state", "grace_deadline_reached");
                return;
            }
            tokio::select! {
                biased;
                _ = tokio::time::sleep_until(deadline) => {
                    on_common::log_s!(LogType::WSC; "wait_for_deferred_responses_until", "state", "grace_deadline_reached");
                    return;
                },
                _ = &mut notified => {}
            }
        }
    }

    fn has_pending_entries(&self) -> bool {
        on_common::log_t!(LogType::WSC; "has_pending_entries");
        self.entries
            .read()
            .map(|entries| !entries.is_empty())
            .unwrap_or_else(|_| {
                on_common::log_e!(LogType::WSC; "has_pending_entries", "error", "pending_lock_poisoned");
                true
            })
    }

    fn fail_generation_now(&self, generation: u64, error: NetError) {
        on_common::log_t!(LogType::WSC; "fail_generation_now", "generation|error", generation, format!("{:?}", error));
        let failed = if let Ok(mut entries) = self.entries.write().inspect_err(|_| {
            on_common::log_e!(LogType::WSC; "fail_generation_now", "error", "lock_poisoned");
        }) {
            let failed = entries
                .iter()
                .filter_map(|(uuid, entry)| {
                    (entry.info.connection_generation == Some(generation)).then_some(uuid.clone())
                })
                .collect::<Vec<_>>();
            failed
                .into_iter()
                .filter_map(|uuid| entries.remove(uuid.as_str()))
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        on_common::log_s!(LogType::WSC; "fail_generation_now", "generation|failed_count", generation, failed.len());
        for entry in failed {
            entry.complete(Err(error));
        }
        self.response_dispatch_changed.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::traits::ws::ws_body::WsBody;
    use crate::module::ws_client::test_support::{check, check_eq, test_error, TestResult};
    use std::sync::atomic::Ordering;

    #[tokio::test]
    async fn typed_lifecycle_logs_do_not_reinvoke_request_traits_or_expose_extensions() -> TestResult
    {
        use on_common::log::logger::Logger;
        use std::sync::atomic::AtomicUsize;

        struct CountedRequest {
            uuid_calls: Arc<AtomicUsize>,
            extension_calls: Arc<AtomicUsize>,
            body_calls: Arc<AtomicUsize>,
        }
        impl WSRequestTrait for CountedRequest {
            fn uuid(&self) -> String {
                self.uuid_calls.fetch_add(1, Ordering::SeqCst);
                "typed-log-pending-lifecycle".to_string()
            }

            fn request_extension(&self) -> HashMap<String, String> {
                self.extension_calls.fetch_add(1, Ordering::SeqCst);
                HashMap::from([(
                    "private".to_string(),
                    "never-log-extension-secret".to_string(),
                )])
            }

            fn body(&self) -> Result<WsBody, NetError> {
                self.body_calls.fetch_add(1, Ordering::SeqCst);
                Ok(WsBody::Text("never-log-body-secret".to_string()))
            }
        }

        let (tx, rx) = std::sync::mpsc::channel();
        let subscription = Logger::register_log_listener_with_capacity(
            Box::new(move |record| {
                if record.content.contains("typed-log-pending-lifecycle") {
                    let _ = tx.send(record);
                }
            }),
            &[LogType::WSC],
            16_384,
        )
        .map_err(|error| test_error(format!("register log listener: {error:?}")))?;
        let uuid_calls = Arc::new(AtomicUsize::new(0));
        let extension_calls = Arc::new(AtomicUsize::new(0));
        let body_calls = Arc::new(AtomicUsize::new(0));
        let pending = PendingRequestView::with_capacity(1);
        let (token, completion) = pending
            .reserve(
                Arc::new(CountedRequest {
                    uuid_calls: Arc::clone(&uuid_calls),
                    extension_calls: Arc::clone(&extension_calls),
                    body_calls: Arc::clone(&body_calls),
                }),
                &WSRequestConfig::default(),
            )
            .map_err(|error| test_error(format!("reserve request: {error:?}")))?;
        check!(pending.mark_writing("typed-log-pending-lifecycle", token, 0, 73))?;
        check!(pending
            .mark_sent("typed-log-pending-lifecycle", token)
            .is_some())?;
        check!(pending
            .take_request("typed-log-pending-lifecycle", 73)
            .is_some())?;
        check_eq!(completion.wait().await, Ok(()))?;

        let mut states = Vec::new();
        loop {
            let record = rx
                .recv_timeout(Duration::from_secs(5))
                .map_err(|error| test_error(format!("lifecycle log: {error:?}")))?;
            check_eq!(record.log_type, LogType::WSC)?;
            check!(!record.content.contains("never-log-extension-secret"))?;
            check!(!record.content.contains("never-log-body-secret"))?;
            let fields: serde_json::Value = serde_json::from_str(&record.content)?;
            if let Some(state) = fields["state"].as_str() {
                states.push(state.to_string());
            }
            if record.tag.ends_with("-wait-S") {
                break;
            }
        }
        check!(states.iter().any(|state| state == "Queued"))?;
        check!(states.iter().any(|state| state == "Writing"))?;
        check!(states.iter().any(|state| state == "AwaitingResponse"))?;
        check!(states.iter().any(|state| state == "response_claimed"))?;
        check_eq!(uuid_calls.load(Ordering::SeqCst), 1)?;
        check_eq!(extension_calls.load(Ordering::SeqCst), 1)?;
        check_eq!(body_calls.load(Ordering::SeqCst), 0)?;
        drop(subscription);
        Ok(())
    }

    struct TestRequest(&'static str);

    impl WSRequestTrait for TestRequest {
        fn uuid(&self) -> String {
            self.0.to_string()
        }

        fn body(&self) -> Result<WsBody, NetError> {
            Ok(WsBody::Text(self.0.to_string()))
        }
    }

    fn register_identity(
        pending: &PendingRequestView,
        uuid: &'static str,
    ) -> TestResult<(
        crate::api::wsc::request_registration::RequestRegistration,
        PendingRequestCompletion,
    )> {
        pending
            .reserve_snapshot_observed(
                uuid.to_string(),
                Arc::new(TestRequest(uuid)),
                &WSRequestConfig::default(),
                None,
                None,
                None,
                None,
            )
            .map_err(|error| test_error(format!("reserve identity: {error:?}")))
    }

    #[tokio::test]
    async fn registration_old_token_cannot_cancel_or_claim_replacement() -> TestResult {
        use crate::api::wsc::request_registration::RequestTerminationOutcome;
        let pending = PendingRequestView::with_capacity(1);
        let (original, completed) = register_identity(&pending, "registration-reused")?;
        check_eq!(
            original.expire(),
            Ok(RequestTerminationOutcome::Terminated {
                error: NetError::TimeoutError
            })
        )?;
        check_eq!(completed.wait().await, Err(NetError::TimeoutError))?;
        let (replacement, replacement_completion) =
            register_identity(&pending, "registration-reused")?;
        check!(original.token() != replacement.token())?;
        check!(pending.mark_writing("registration-reused", replacement.raw_token(), 0, 11))?;
        check!(!pending.mark_writing("registration-reused", original.raw_token(), 99, 12))?;
        check!(!pending.mark_queued("registration-reused", original.raw_token()))?;
        check!(pending
            .mark_sent("registration-reused", original.raw_token())
            .is_none())?;
        check!(!pending.record_deferred_error(
            "registration-reused",
            original.raw_token(),
            NetError::TimeoutError
        ))?;
        check!(pending
            .remove_if_token(
                "registration-reused",
                original.raw_token(),
                NetError::TimeoutError
            )
            .is_none())?;
        let retained = pending
            .snapshot()
            .pop()
            .ok_or_else(|| test_error("replacement disappeared"))?;
        check_eq!(retained.status, PendingRequestStatus::Writing)?;
        check_eq!(retained.send_attempt, 0)?;
        check_eq!(retained.connection_generation, Some(11))?;
        check!(retained.sent_at.is_none())?;
        check_eq!(
            original.cancel(),
            Ok(RequestTerminationOutcome::StaleRegistration)
        )?;
        let response = crate::WSCResponse::new(
            crate::WebSocketMessage::Text("old ack".into()),
            pending.clone(),
            11,
        );
        check!(response.take_request_if_registered(&original)?.is_none())?;
        check_eq!(pending.len(), 1)?;
        check!(response.take_request_if_registered(&replacement)?.is_some())?;
        check_eq!(replacement_completion.wait().await, Ok(()))?;
        check_eq!(
            replacement.expire(),
            Ok(RequestTerminationOutcome::AlreadyClaimedOrFinished)
        )?;
        Ok(())
    }

    #[tokio::test]
    async fn registration_claim_requires_original_table_and_response_generation() -> TestResult {
        let first = PendingRequestView::with_capacity(1);
        let second = PendingRequestView::with_capacity(1);
        let (registration, completion) = register_identity(&first, "registration-origin")?;
        let (other, other_completion) = register_identity(&second, "registration-origin")?;
        check!(first.mark_writing("registration-origin", registration.raw_token(), 0, 13))?;
        check!(second.mark_writing("registration-origin", other.raw_token(), 0, 13))?;
        let foreign = crate::WSCResponse::new(
            crate::WebSocketMessage::Text("ack".into()),
            second.clone(),
            13,
        );
        check!(foreign.take_request_if_registered(&registration)?.is_none())?;
        let stale = crate::WSCResponse::new(
            crate::WebSocketMessage::Text("ack".into()),
            first.clone(),
            12,
        );
        check!(stale.take_request_if_registered(&registration)?.is_none())?;
        let immediate = crate::WSCResponse::new(
            crate::WebSocketMessage::Text("ack".into()),
            first.clone(),
            13,
        );
        check!(immediate
            .take_request_if_registered(&registration)?
            .is_some())?;
        check!(first.take_write_response_claim(registration.raw_token()))?;
        check_eq!(completion.wait().await, Ok(()))?;
        check_eq!(registration.cancel(), Ok(crate::api::wsc::request_registration::RequestTerminationOutcome::AlreadyClaimedOrFinished))?;
        check_eq!(second.len(), 1)?;
        other.cancel()?;
        check_eq!(other_completion.wait().await, Err(NetError::Cancelled))?;
        Ok(())
    }

    #[tokio::test]
    async fn registration_cancellation_selects_dispatch_phase_and_finishes_once() -> TestResult {
        use crate::api::wsc::request_registration::RequestTerminationOutcome;
        use crate::module::ws_client::write::queued_request::DispatchPhase;
        for (phase, expected) in [
            (PendingRequestStatus::Queued, NetError::TimeoutError),
            (PendingRequestStatus::Writing, NetError::DeliveryUnknown),
            (
                PendingRequestStatus::AwaitingResponse,
                NetError::TimeoutError,
            ),
        ] {
            let pending = PendingRequestView::with_capacity(1);
            let dispatch = DispatchPhase::new();
            let cancel = CancellationToken::new();
            let control =
                RegistrationControl::new(dispatch.clone(), cancel.clone(), std::sync::Weak::new());
            let (registration, completion) = pending.reserve_snapshot_observed(
                "registration-phase".to_string(),
                Arc::new(TestRequest("registration-phase")),
                &WSRequestConfig::default(),
                None,
                None,
                Some(control),
                None,
            )?;
            if phase != PendingRequestStatus::Queued {
                check!(dispatch.start_writing())?;
                check!(pending.mark_writing(
                    "registration-phase",
                    registration.raw_token(),
                    0,
                    23
                ))?;
                // This row models a frame admitted to the sink, not readiness alone.
                check!(dispatch.start_data_write())?;
            }
            if phase == PendingRequestStatus::AwaitingResponse {
                check!(pending
                    .mark_sent("registration-phase", registration.raw_token())
                    .is_some())?;
                check!(dispatch.commit())?;
            }
            let finished = registration.cancellation_finished_token();
            check!(!finished.is_cancelled())?;
            check_eq!(
                registration.expire(),
                Ok(RequestTerminationOutcome::Terminated { error: expected })
            )?;
            check!(cancel.is_cancelled())?;
            check!(finished.is_cancelled())?;
            check!(pending.is_empty())?;
            check_eq!(completion.wait().await, Err(expected))?;
            check_eq!(
                registration.cancel(),
                Ok(RequestTerminationOutcome::AlreadyClaimedOrFinished)
            )?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn registration_cancelled_scope_cannot_claim_through_any_response_entry() -> TestResult {
        let pending = PendingRequestView::with_capacity(1);
        let scope = RequestScope::new();
        let (registration, completion) = pending.reserve_snapshot_observed(
            "registration-scope".to_string(),
            Arc::new(TestRequest("registration-scope")),
            &WSRequestConfig::default(),
            None,
            None,
            None,
            Some(scope.clone()),
        )?;
        check!(pending.mark_writing("registration-scope", registration.raw_token(), 0, 29))?;
        let response = crate::WSCResponse::new(
            crate::WebSocketMessage::Text("ack".into()),
            pending.clone(),
            29,
        );
        scope.cancel();
        check!(response.get_request("registration-scope").is_none())?;
        check!(response.take_request("registration-scope").is_none())?;
        check!(response
            .take_request_if_registered(&registration)?
            .is_none())?;
        check_eq!(pending.len(), 1)?;
        registration.cancel()?;
        check_eq!(completion.wait().await, Err(NetError::Cancelled))?;
        check!(pending
            .reserve_snapshot_observed(
                "registration-scope".to_string(),
                Arc::new(TestRequest("registration-scope")),
                &WSRequestConfig::default(),
                None,
                None,
                None,
                Some(scope),
            )
            .is_err())?;
        check!(pending.is_empty())?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn registration_concurrent_cancel_and_expire_have_one_terminal_owner() -> TestResult {
        use crate::api::wsc::request_registration::RequestTerminationOutcome;
        let pending = PendingRequestView::with_capacity(1);
        let (registration, completion) = register_identity(&pending, "registration-concurrent")?;
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let cancel_registration = registration.clone();
        let cancel_barrier = Arc::clone(&barrier);
        let cancel_task = tokio::spawn(async move {
            cancel_barrier.wait().await;
            cancel_registration.cancel()
        });
        let expire_task = tokio::spawn(async move {
            barrier.wait().await;
            registration.expire()
        });
        let (cancelled, expired) = tokio::time::timeout(Duration::from_secs(2), async {
            Ok::<_, crate::module::ws_client::test_support::TestError>((
                cancel_task.await??,
                expire_task.await??,
            ))
        })
        .await
        .map_err(|_| test_error("registration competition timed out"))??;
        let mut selected = None;
        for outcome in [cancelled, expired] {
            match outcome {
                RequestTerminationOutcome::Terminated { error } => {
                    check!(selected.is_none(), "two callers won the terminal claim")?;
                    selected = Some(error);
                }
                RequestTerminationOutcome::AlreadyClaimedOrFinished => {}
                RequestTerminationOutcome::StaleRegistration => {
                    return Err(test_error("same registration became stale"))
                }
            }
        }
        let selected =
            selected.ok_or_else(|| test_error("neither caller finished registration"))?;
        check!(matches!(
            selected,
            NetError::Cancelled | NetError::TimeoutError
        ))?;
        check_eq!(completion.wait().await, Err(selected))?;
        check!(pending.is_empty())?;
        Ok(())
    }

    #[test]
    fn registration_dispatch_cleanup_does_not_create_a_pending_ownership_cycle() -> TestResult {
        use crate::module::ws_client::write::queued_request::DispatchPhase;
        let pending = PendingRequestView::with_capacity(1);
        let request: Arc<dyn WSRequestTrait> = Arc::new(TestRequest("registration-drop"));
        let retained = Arc::downgrade(&request);
        let phase = DispatchPhase::new();
        let control = RegistrationControl::new(
            phase.clone(),
            CancellationToken::new(),
            std::sync::Weak::new(),
        );
        let (registration, completion) = pending.reserve_snapshot_observed(
            "registration-drop".to_string(),
            request,
            &WSRequestConfig::default(),
            None,
            None,
            Some(control),
            None,
        )?;
        phase.set_pending_cleanup(
            pending.clone(),
            "registration-drop".to_string(),
            registration.raw_token(),
        )?;
        drop(registration);
        drop(completion);
        drop(pending);
        check!(
            retained.upgrade().is_some(),
            "dispatch cleanup lost its pending ownership too early"
        )?;
        drop(phase);
        check!(
            retained.upgrade().is_none(),
            "registration control formed a strong pending/dispatch cycle"
        )?;
        Ok(())
    }

    #[tokio::test]
    async fn capacity_is_bounded_and_generation_guards_response_claims() -> TestResult {
        let pending = PendingRequestView::with_capacity(1);
        let (token, completion) = pending
            .reserve(Arc::new(TestRequest("first")), &WSRequestConfig::default())
            .map_err(|error| test_error(format!("reserve first request: {error:?}")))?;
        check!(matches!(
            pending.reserve(Arc::new(TestRequest("second")), &WSRequestConfig::default()),
            Err(NetError::PendingRequestLimitReached)
        ))?;

        check!(pending.mark_writing("first", token, 0, 7))?;
        check!(pending.take_request("first", 8).is_none())?;
        check!(pending.take_request("first", 7).is_some())?;
        check_eq!(completion.wait().await, Ok(()))?;
        check!(pending.is_empty())?;
        Ok(())
    }

    #[tokio::test]
    async fn timeout_or_disconnect_completion_is_delivered_once() -> TestResult {
        let pending = PendingRequestView::with_capacity(1);
        let (token, completion) = pending
            .reserve(Arc::new(TestRequest("first")), &WSRequestConfig::default())
            .map_err(|error| test_error(format!("reserve request: {error:?}")))?;
        check!(pending.mark_writing("first", token, 0, 9))?;
        pending.fail_for_generation(9, NetError::ConnectionClosed, Duration::from_secs(1));
        pending.fail_all_with_response_grace(NetError::Cancelled, Duration::ZERO);

        check_eq!(completion.wait().await, Err(NetError::ConnectionClosed))?;
        check!(pending.is_empty())?;
        Ok(())
    }

    #[tokio::test]
    async fn accepted_response_can_claim_before_deferred_disconnect_cleanup() -> TestResult {
        let pending = PendingRequestView::with_capacity(1);
        let (token, completion) = pending
            .reserve(
                Arc::new(TestRequest("response-before-close")),
                &WSRequestConfig::default(),
            )
            .map_err(|error| test_error(format!("reserve request: {error:?}")))?;
        check!(pending.mark_writing("response-before-close", token, 0, 17))?;
        let response_guard = pending.begin_response_dispatch(17);

        pending.fail_for_generation(17, NetError::ConnectionClosed, Duration::from_secs(1));
        check_eq!(
            pending.len(),
            1,
            "accepted response must hold correlation open"
        )?;
        check!(pending.take_request("response-before-close", 17).is_some())?;
        drop(response_guard);

        check_eq!(completion.wait().await, Ok(()))?;
        check!(pending.is_empty())?;
        Ok(())
    }

    #[tokio::test]
    async fn blocked_response_dispatch_only_defers_disconnect_for_a_bounded_grace() -> TestResult {
        let pending = PendingRequestView::with_capacity(1);
        let (token, completion) = pending
            .reserve(
                Arc::new(TestRequest("blocked-response")),
                &WSRequestConfig::default(),
            )
            .map_err(|error| test_error(format!("reserve request: {error:?}")))?;
        check!(pending.mark_writing("blocked-response", token, 0, 19))?;
        let _response_guard = pending.begin_response_dispatch(19);

        pending.fail_for_generation(19, NetError::ConnectionClosed, Duration::from_millis(50));
        tokio::time::sleep(Duration::from_millis(80)).await;

        check_eq!(completion.wait().await, Err(NetError::ConnectionClosed))?;
        check!(pending.is_empty())?;
        Ok(())
    }

    #[tokio::test]
    async fn terminal_fail_all_preserves_already_accepted_response_until_grace() -> TestResult {
        let pending = PendingRequestView::with_capacity(1);
        let (token, completion) = pending
            .reserve(
                Arc::new(TestRequest("accepted-before-shutdown")),
                &WSRequestConfig::default(),
            )
            .map_err(|error| test_error(format!("reserve request: {error:?}")))?;
        check!(pending.mark_writing("accepted-before-shutdown", token, 0, 23))?;
        let response_guard = pending.begin_response_dispatch(23);

        pending.fail_all_with_response_grace(NetError::Cancelled, Duration::from_millis(100));
        check_eq!(pending.len(), 1)?;
        check!(pending
            .take_request("accepted-before-shutdown", 23)
            .is_some())?;
        drop(response_guard);

        check_eq!(completion.wait().await, Ok(()))?;
        check!(pending.is_empty())?;
        Ok(())
    }

    #[tokio::test]
    async fn terminal_force_finishes_deferred_pending_before_shutdown_completion() -> TestResult {
        let pending = PendingRequestView::with_capacity(1);
        let (token, completion) = pending
            .reserve(
                Arc::new(TestRequest("terminal-force")),
                &WSRequestConfig::default(),
            )
            .map_err(|error| test_error(format!("reserve request: {error:?}")))?;
        check!(pending.mark_writing("terminal-force", token, 0, 41))?;
        let response_guard = pending.begin_response_dispatch(41);

        pending.fail_all_with_response_grace(NetError::Cancelled, Duration::from_secs(3_600));
        check_eq!(pending.len(), 1)?;
        pending.force_finish_all_response_dispatches(NetError::Cancelled);

        check!(pending.is_empty())?;
        check_eq!(completion.wait().await, Err(NetError::Cancelled))?;
        drop(response_guard);
        Ok(())
    }

    #[tokio::test]
    async fn terminal_wait_and_force_do_not_trust_a_removed_dispatch_state() -> TestResult {
        let pending = PendingRequestView::with_capacity(1);
        let (token, completion) = pending
            .reserve(
                Arc::new(TestRequest("dispatch-cleanup-window")),
                &WSRequestConfig::default(),
            )
            .map_err(|error| test_error(format!("reserve request: {error:?}")))?;
        check!(pending.mark_writing("dispatch-cleanup-window", token, 0, 43))?;
        let response_guard = pending.begin_response_dispatch(43);
        check!(pending.record_deferred_error(
            "dispatch-cleanup-window",
            token,
            NetError::ConnectionClosed,
        ))?;

        // 模拟响应守卫或强制清理定时器已移除分发状态，
        // 但尚未取得请求表锁以完成待响应请求清理的时间窗口。
        pending
            .response_dispatches
            .lock()
            .map_err(|error| test_error(format!("response dispatch lock: {error:?}")))?
            .clear();
        let mut wait = Box::pin(pending.wait_for_deferred_responses_until(Some(
            tokio::time::Instant::now() + Duration::from_secs(1),
        )));
        check!(
            tokio::time::timeout(Duration::from_millis(20), wait.as_mut())
                .await
                .is_err(),
            "an existing entry must keep terminal cleanup waiting even without dispatch state"
        )?;

        pending.force_finish_all_response_dispatches(NetError::Cancelled);
        tokio::time::timeout(Duration::from_secs(1), wait.as_mut())
            .await
            .map_err(|error| test_error(format!("entry drain wakes terminal waiter: {error:?}")))?;
        check!(pending.is_empty())?;
        check_eq!(
            completion.wait().await,
            Err(NetError::ConnectionClosed),
            "the first deferred terminal error must survive final force cleanup"
        )?;
        drop(response_guard);
        Ok(())
    }

    #[tokio::test]
    async fn first_deferred_error_wins_but_response_claim_can_still_succeed() -> TestResult {
        let pending = PendingRequestView::with_capacity(2);
        let (first_token, first_completion) = pending
            .reserve(
                Arc::new(TestRequest("timeout-first")),
                &WSRequestConfig::default(),
            )
            .map_err(|error| test_error(format!("reserve first request: {error:?}")))?;
        check!(pending.mark_writing("timeout-first", first_token, 0, 29))?;
        check!(pending.record_deferred_error(
            "timeout-first",
            first_token,
            NetError::TimeoutError,
        ))?;
        pending.fail_for_generation(29, NetError::ConnectionClosed, Duration::ZERO);
        check_eq!(first_completion.wait().await, Err(NetError::TimeoutError))?;

        let (claim_token, claim_completion) = pending
            .reserve(
                Arc::new(TestRequest("claim-during-grace")),
                &WSRequestConfig::default(),
            )
            .map_err(|error| test_error(format!("reserve claim request: {error:?}")))?;
        check!(pending.mark_writing("claim-during-grace", claim_token, 0, 31))?;
        check!(pending.record_deferred_error(
            "claim-during-grace",
            claim_token,
            NetError::TimeoutError,
        ))?;
        check!(pending.take_request("claim-during-grace", 31).is_some())?;
        check_eq!(claim_completion.wait().await, Ok(()))?;

        let (lifecycle_token, lifecycle_completion) = pending
            .reserve(
                Arc::new(TestRequest("lifecycle-first")),
                &WSRequestConfig::default(),
            )
            .map_err(|error| test_error(format!("reserve lifecycle request: {error:?}")))?;
        check!(pending.mark_writing("lifecycle-first", lifecycle_token, 0, 37))?;
        let lifecycle_guard = pending.begin_response_dispatch(37);
        pending.fail_for_generation(37, NetError::ConnectionClosed, Duration::from_millis(50));
        check!(pending.record_deferred_error(
            "lifecycle-first",
            lifecycle_token,
            NetError::TimeoutError,
        ))?;
        drop(lifecycle_guard);
        check_eq!(
            lifecycle_completion.wait().await,
            Err(NetError::ConnectionClosed)
        )?;
        Ok(())
    }
}

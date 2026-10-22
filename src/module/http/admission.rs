//! One admission transaction for buffered jobs and stream supervisors.
//!
//! The registration guard and owned lane slot travel together until publication.
//! Dropping an unpublished transaction releases both, including after acceptance.
use super::*;
use crate::api::http::HttpAdmissionPolicy;

pub(super) struct Admission<T> {
    // Release lane capacity before retiring the registration or running observers.
    pub(super) slot: mpsc::OwnedPermit<T>,
    pub(super) request: RequestAdmission,
}

struct PendingAdmission {
    registered: RegisteredRequest,
    registration: RegistrationGuard,
    budget: StreamBudget,
    operation_start: Instant,
}

impl HttpClientInner {
    fn begin_admission(
        &self,
        requested: Option<HttpRequestId>,
        resolved: ResolvedTimeouts,
        operation_start: Instant,
    ) -> Result<PendingAdmission, NetError> {
        let budget = StreamBudget::total(operation_start, resolved.total)?.with_phase(
            operation_start,
            resolved.admission,
            TimeoutPhase::Admission,
        )?;
        let (registered, registration) = self.reserve_request(requested)?;
        Ok(PendingAdmission {
            registered,
            registration,
            budget,
            operation_start,
        })
    }

    /// This is the only acceptance point. No await or user code runs under the gate.
    fn accept_admission<T>(
        &self,
        pending: PendingAdmission,
        permit: OwnedSemaphorePermit,
        slot: mpsc::OwnedPermit<T>,
        lane: &mpsc::Sender<T>,
    ) -> Result<Admission<T>, NetError> {
        let id = pending.registered.id;
        let accepted = (|| {
            let _gate = self.admission_gate.lock().map_err(NetError::from_poison)?;
            check_budget(
                pending.budget,
                &pending.registered.control,
                ErrorStage::Admission,
            )?;
            if self.shutdown_sent.load(Ordering::Acquire) || lane.is_closed() {
                return Err(NetError::from(ErrorKind::QueueClosed));
            }
            self.registry.activate(id, &pending.registered.control)
        })();
        accepted.map_err(|error| admission_error(error, Some(id)))?;
        Ok(Admission {
            slot,
            request: RequestAdmission {
                registered: pending.registered,
                registration: pending.registration,
                permit: Arc::new(permit),
                operation_start: pending.operation_start,
            },
        })
    }

    fn try_admission<T>(
        &self,
        lane: &mpsc::Sender<T>,
        requested: Option<HttpRequestId>,
        resolved: ResolvedTimeouts,
        operation_start: Instant,
    ) -> Result<Admission<T>, NetError> {
        let pending = self
            .begin_admission(requested, resolved, operation_start)
            .map_err(|error| admission_error(error, requested))?;
        let id = pending.registered.id;
        let resources = (|| {
            check_budget(
                pending.budget,
                &pending.registered.control,
                ErrorStage::Admission,
            )?;
            let permit = self.try_acquire_permit()?;
            let slot = lane.clone().try_reserve_owned().map_err(|error| {
                NetError::from(match error {
                    mpsc::error::TrySendError::Full(_) => ErrorKind::QueueFull,
                    mpsc::error::TrySendError::Closed(_) => ErrorKind::QueueClosed,
                })
            })?;
            Ok((permit, slot))
        })();
        let (permit, slot) = resources.map_err(|error| admission_error(error, Some(id)))?;
        self.accept_admission(pending, permit, slot, lane)
    }

    async fn wait_admission_resource<T>(
        &self,
        pending: &PendingAdmission,
        resource: impl std::future::Future<Output = Result<T, NetError>>,
    ) -> Result<T, NetError> {
        run_budget(
            pending.budget,
            &pending.registered.control,
            ErrorStage::Admission,
            async {
                let closing = self.shutdown_notify.notified();
                tokio::pin!(closing);
                closing.as_mut().enable();
                if self.shutdown_sent.load(Ordering::Acquire) {
                    return Err(NetError::from(ErrorKind::QueueClosed));
                }
                tokio::select! {
                    biased;
                    _ = closing => Err(NetError::from(ErrorKind::QueueClosed)),
                    result = resource => result,
                }
            },
        )
        .await
        .map_err(|error| admission_error(error, Some(pending.registered.id)))
    }

    async fn admit<T>(
        &self,
        lane: &mpsc::Sender<T>,
        policy: HttpAdmissionPolicy,
        requested: Option<HttpRequestId>,
        resolved: ResolvedTimeouts,
        operation_start: Instant,
    ) -> Result<Admission<T>, NetError> {
        if policy == HttpAdmissionPolicy::FailFast {
            return self.try_admission(lane, requested, resolved, operation_start);
        }
        let pending = self
            .begin_admission(requested, resolved, operation_start)
            .map_err(|error| admission_error(error, requested))?;
        let permit = self
            .wait_admission_resource(&pending, self.acquire_permit())
            .await?;
        let slot = self
            .wait_admission_resource(&pending, async {
                lane.clone()
                    .reserve_owned()
                    .await
                    .map_err(|_| NetError::from(ErrorKind::QueueClosed))
            })
            .await?;
        self.accept_admission(pending, permit, slot, lane)
    }

    pub(super) fn future_admission_policy(&self, request: &HttpRequest) -> HttpAdmissionPolicy {
        match request.admission_policy_override() {
            Some(policy) => policy,
            None => self.config.admission_policy(),
        }
    }

    #[cfg(test)]
    pub(super) async fn admit_buffered_future(
        &self,
        request: &HttpRequest,
    ) -> Result<(Admission<SendCommand>, ResolvedTimeouts), NetError> {
        self.admit_buffered_future_at(request, Instant::now()).await
    }
    pub(super) async fn admit_buffered_future_at(
        &self,
        request: &HttpRequest,
        start: Instant,
    ) -> Result<(Admission<SendCommand>, ResolvedTimeouts), NetError> {
        let policy = self.future_admission_policy(request);
        let resolved = self
            .config
            .timeouts()
            .resolve(request.timeouts(), self.config.timeout)
            .map_err(|error| admission_report(admission_error(error, None), start))?;
        let admission = self
            .admit(&self.request_tx, policy, None, resolved, start)
            .await
            .map_err(|error| admission_report(error, start))?;
        Ok((admission, resolved))
    }

    #[cfg(test)]
    pub(crate) fn submit_request<R: HttpRequestTrait>(
        &self,
        request: R,
        options: HttpRequestOptions,
    ) -> Result<HttpRequestHandle, EnqueueError<R>> {
        self.submit_request_at(request, options, Instant::now())
    }
    pub(crate) fn submit_request_at<R: HttpRequestTrait>(
        &self,
        request: R,
        mut options: HttpRequestOptions,
        start: Instant,
    ) -> Result<HttpRequestHandle, EnqueueError<R>> {
        let prepared = (|| {
            let resolved = self.resolve_timeouts(&options)?;
            options = options.clone().with_timeouts(resolved.as_overrides())?;
            self.try_admission(&self.request_tx, options.request_id, resolved, start)
        })();
        match prepared {
            Ok(admission) => Ok(self.publish_buffered(request, options, admission)),
            Err(error) => Err(EnqueueError::new(
                request,
                admission_report(admission_error(error, options.request_id), start),
            )),
        }
    }

    #[cfg(test)]
    pub(crate) async fn submit_request_wait<R: HttpRequestTrait>(
        &self,
        request: R,
        options: HttpRequestOptions,
    ) -> Result<HttpRequestHandle, EnqueueError<R>> {
        self.submit_request_wait_at(request, options, Instant::now())
            .await
    }
    pub(crate) async fn submit_request_wait_at<R: HttpRequestTrait>(
        &self,
        request: R,
        mut options: HttpRequestOptions,
        start: Instant,
    ) -> Result<HttpRequestHandle, EnqueueError<R>> {
        let prepared = async {
            let resolved = self.resolve_timeouts(&options)?;
            options = options.clone().with_timeouts(resolved.as_overrides())?;
            self.admit(
                &self.request_tx,
                HttpAdmissionPolicy::Wait,
                options.request_id,
                resolved,
                start,
            )
            .await
        }
        .await;
        match prepared {
            Ok(admission) => Ok(self.publish_buffered(request, options, admission)),
            Err(error) => Err(EnqueueError::new(
                request,
                admission_report(admission_error(error, options.request_id), start),
            )),
        }
    }

    /// Acceptance already reserved this slot; shutdown cannot revoke publication.
    pub(super) fn publish_buffered<R: HttpRequestTrait>(
        &self,
        request: R,
        options: HttpRequestOptions,
        admission: Admission<SendCommand>,
    ) -> HttpRequestHandle {
        let Admission {
            slot,
            request: admission,
        } = admission;
        let (registered, mut job, registration) = admission.into_job(request, options);
        let handle = HttpRequestHandle::new(registered.id, Arc::clone(&registered.control));
        job.completion = Some(CompletionLease::new(
            self.registry(),
            registered.id,
            registered.control,
            Arc::clone(&job.permit),
        ));
        registration.commit();
        drop(slot.send(SendCommand::Job(job)));
        handle
    }

    pub(super) async fn admit_stream(
        &self,
        policy: HttpAdmissionPolicy,
        resolved: ResolvedTimeouts,
        operation_start: Instant,
        owner: &mut StreamOwner,
    ) -> Result<(HttpRequestId, Arc<CancellationSignal>), NetError> {
        let Admission {
            slot,
            request: admission,
        } = self
            .admit(&self.supervisor_tx, policy, None, resolved, operation_start)
            .await?;
        let RequestAdmission {
            registered,
            registration,
            permit,
            ..
        } = admission;
        let request_id = registered.id;
        let control = registered.control;
        control.streaming.store(true, Ordering::Release);
        if let Some(lifecycle) = owner.lifecycle.as_mut() {
            lifecycle.control = Some(Arc::clone(&control));
        }
        owner.supervisor_slot = Some(slot);
        owner.lease = Some(StreamRegistrationLease {
            registry: self.registry(),
            request_id,
            control: Arc::clone(&control),
            permit: Some(permit),
        });
        registration.commit();
        Ok((request_id, control))
    }
}

fn admission_report(error: NetError, start: Instant) -> NetError {
    let mut report = match error.retry_report() {
        Some(report) => report.clone(),
        None => RetryReport::new(),
    };
    report.elapsed = start.elapsed();
    report.stop_reason = Some(stop_for_error(&error));
    error.with_retry_report(report)
}

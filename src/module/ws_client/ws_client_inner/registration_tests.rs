use super::*;
use crate::module::ws_client::test_support::{check, check_eq, test_error, TestResult};
use std::sync::atomic::AtomicUsize;

struct ChangingId(AtomicUsize);

impl WSRequestTrait for ChangingId {
    fn uuid(&self) -> String {
        format!("captured-{}", self.0.fetch_add(1, Ordering::Relaxed))
    }

    fn body(&self) -> Result<WsBody, NetError> {
        Ok(WsBody::Text("body".into()))
    }
}

fn connected() -> TestResult<Arc<WSClientInner>> {
    connected_with_config(WebSocketClientConfig::default())
}

fn connected_with_config(config: WebSocketClientConfig) -> TestResult<Arc<WSClientInner>> {
    let (inner, _worker) = WSClientInner::new(config)?;
    *inner.state.write().map_err(|_| test_error("state lock"))? = ConnectionStatus::Connected;
    inner.send_admission.begin_session();
    inner.send_admission.connection_succeeded();
    Ok(inner)
}

#[test]
fn try_prepare_commit_and_cleanup_work_without_a_runtime() -> TestResult {
    check!(tokio::runtime::Handle::try_current().is_err())?;
    let inner = connected()?;
    let prepared = inner.try_prepare_registered(
        Arc::new(ChangingId(AtomicUsize::new(0))),
        WebSocketRequestOptions::default(),
    )?;
    let registration = prepared.registration().clone();
    let receipt = prepared.commit()?;
    check_eq!(
        registration.cancel()?,
        crate::RequestTerminationOutcome::Terminated {
            error: NetError::Cancelled
        }
    )?;
    drop(receipt);
    check!(inner.pending_requests.is_empty())?;
    check!(inner.queue.drain().is_empty())?;
    Ok(())
}

#[tokio::test]
async fn each_submission_captures_uuid_once_for_all_cleanup_paths() -> TestResult {
    for synchronous in [true, false] {
        let inner = connected()?;
        let request = Arc::new(ChangingId(AtomicUsize::new(0)));
        if synchronous {
            inner.try_send(request.clone(), WSRequestConfig::default())?;
        } else {
            let mut sending = Box::pin(inner.send(request.clone(), WSRequestConfig::default()));
            check!(futures::poll!(&mut sending).is_pending())?;
            drop(sending);
        }
        let calls = request.0.load(Ordering::Relaxed);
        for request in inner.queue.drain() {
            request.complete(Err(NetError::Cancelled));
        }
        check_eq!(calls, 1, "uuid must be captured only once per submission")?;
        check!(
            inner.pending_requests.is_empty(),
            "cleanup used a different UUID"
        )?;
    }
    Ok(())
}

#[tokio::test]
async fn prepared_registration_is_bounded_cancellable_and_invisible_to_writer() -> TestResult {
    let inner = connected()?;
    let prepared = inner.try_prepare_registered(
        Arc::new(ChangingId(AtomicUsize::new(0))),
        WebSocketRequestOptions::default(),
    )?;
    let registration = prepared.registration().clone();
    check!(inner.queue.try_next().is_none())?;
    check_eq!(inner.pending_requests.len(), 1)?;
    drop(prepared);
    check!(inner.pending_requests.is_empty())?;
    check!(inner.queue.drain().is_empty())?;
    check_eq!(
        registration.cancel()?,
        crate::RequestTerminationOutcome::AlreadyClaimedOrFinished
    )?;
    let prepared = inner
        .prepare_registered(
            Arc::new(ChangingId(AtomicUsize::new(0))),
            WebSocketRequestOptions::default(),
        )
        .await?;
    let registration = prepared.registration().clone();
    let receipt = prepared.commit()?;
    let request = inner
        .queue
        .try_next()
        .ok_or_else(|| test_error("committed request"))?;
    check_eq!(request.pending_token, Some(registration.raw_token()))?;
    check!(inner
        .pending_requests
        .mark_writing("captured-0", registration.raw_token(), 0, 5))?;
    request.complete(Ok(()));
    let completion = receipt.wait_until_written().await?;
    check!(inner
        .pending_requests
        .take_request("captured-0", 5)
        .is_some())?;
    completion.wait().await?;
    Ok(())
}

#[tokio::test]
async fn scoped_submission_never_recaptures_a_new_connection_owner() -> TestResult {
    let inner = connected()?;
    let old = RequestScope::new();
    let current = RequestScope::new();
    inner
        .send_admission
        .begin_session_with_scope(Some(7), Some(current.clone()));
    inner.send_admission.connection_succeeded();
    for scope in [None, Some(old.clone())] {
        let mut options = WebSocketRequestOptions::default();
        if let Some(scope) = scope {
            options = options.with_scope(scope);
        }
        check!(matches!(
            inner.try_prepare_registered(Arc::new(ChangingId(AtomicUsize::new(0))), options),
            Err(NetError::ConfigError)
        ))?;
    }
    check!(matches!(
        inner.try_send(
            Arc::new(ChangingId(AtomicUsize::new(0))),
            WSRequestConfig::default()
        ),
        Err(NetError::ConfigError)
    ))?;
    let prepared = inner.try_prepare_registered(
        Arc::new(ChangingId(AtomicUsize::new(0))),
        WebSocketRequestOptions::default().with_scope(current.clone()),
    )?;
    current.cancel();
    check!(matches!(prepared.commit(), Err(NetError::Cancelled)))?;
    check!(inner.pending_requests.is_empty())?;
    check!(inner.queue.try_next().is_none())?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn cancelling_prepare_during_capacity_wait_removes_registration_without_free_capacity(
) -> TestResult {
    for drop_future in [false, true] {
        let inner = connected_with_config(WebSocketClientConfig {
            business_queue_capacity: 1,
            business_queue_max_bytes: 4,
            ..WebSocketClientConfig::default()
        })?;
        let scope = RequestScope::new();
        inner
            .send_admission
            .begin_session_with_scope(Some(1), Some(scope.clone()));
        inner.send_admission.connection_succeeded();
        inner.try_send_untracked_scoped(
            WsBody::Text("held".into()),
            WSRequestConfig::default(),
            false,
            Some(scope.clone()),
        )?;
        let mut preparing = Box::pin(inner.prepare_registered(
            Arc::new(ChangingId(AtomicUsize::new(0))),
            WebSocketRequestOptions::default().with_scope(scope.clone()),
        ));
        check!(futures::poll!(&mut preparing).is_pending())?;
        check_eq!(inner.pending_requests.len(), 1)?;
        if !drop_future {
            scope.cancel();
            check!(matches!(
                futures::poll!(&mut preparing),
                std::task::Poll::Ready(Err(NetError::Cancelled))
            ))?;
        }
        drop(preparing);
        check!(inner.pending_requests.is_empty())?;
        for request in inner.queue.drain() {
            request.complete(Err(NetError::Cancelled));
        }
    }
    Ok(())
}

#[tokio::test]
async fn expired_preparation_releases_capacity_and_never_activates_a_reused_uuid() -> TestResult {
    let inner = connected_with_config(WebSocketClientConfig {
        business_queue_capacity: 1,
        business_queue_max_bytes: 4,
        ..WebSocketClientConfig::default()
    })?;
    let prepare = || {
        inner.try_prepare_registered(
            Arc::new(ChangingId(AtomicUsize::new(0))),
            WebSocketRequestOptions::default(),
        )
    };
    let first = prepare()?;
    let old = first.registration().clone();
    check!(matches!(prepare(), Err(NetError::DuplicateRequestId)))?;
    check_eq!(
        old.expire()?,
        crate::RequestTerminationOutcome::Terminated {
            error: NetError::TimeoutError
        }
    )?;
    let replacement = prepare()?;
    check_eq!(
        old.cancel()?,
        crate::RequestTerminationOutcome::StaleRegistration
    )?;
    check!(matches!(first.commit(), Err(NetError::TimeoutError)))?;
    check_eq!(inner.pending_requests.len(), 1)?;
    drop(replacement);
    check!(inner.pending_requests.is_empty())?;
    Ok(())
}

#[tokio::test]
async fn prepared_commit_and_cancellation_race_has_one_terminal_owner() -> TestResult {
    for _ in 0..32 {
        let inner = connected()?;
        let prepared = inner.try_prepare_registered(
            Arc::new(ChangingId(AtomicUsize::new(0))),
            WebSocketRequestOptions::default(),
        )?;
        let registration = prepared.registration().clone();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let commit_barrier = barrier.clone();
        let commit = std::thread::spawn(move || {
            commit_barrier.wait();
            prepared.commit()
        });
        barrier.wait();
        check_eq!(
            registration.cancel()?,
            crate::RequestTerminationOutcome::Terminated {
                error: NetError::Cancelled
            }
        )?;
        let committed = commit
            .join()
            .map_err(|_| test_error("commit thread failed"))?;
        match committed {
            Ok(receipt) => check!(matches!(
                receipt.wait_until_written().await,
                Err(NetError::Cancelled)
            ))?,
            Err(error) => check_eq!(error, NetError::Cancelled)?,
        }
        check!(inner.pending_requests.is_empty())?;
        check!(inner.queue.drain().is_empty())?;
    }
    Ok(())
}

#[tokio::test]
async fn all_scoped_untracked_lanes_reject_foreign_owners_and_keep_current_owner() -> TestResult {
    for urgent in [false, true] {
        for synchronous in [false, true] {
            let inner = connected()?;
            let scope = RequestScope::new();
            inner
                .send_admission
                .begin_session_with_scope(Some(1), Some(scope.clone()));
            inner.send_admission.connection_succeeded();
            for owner in [None, Some(RequestScope::new())] {
                let result = if synchronous {
                    inner.try_send_untracked_scoped(
                        WsBody::Text("old".into()),
                        WSRequestConfig::default(),
                        urgent,
                        owner,
                    )
                } else {
                    inner
                        .send_untracked_scoped(
                            WsBody::Text("old".into()),
                            WSRequestConfig::default(),
                            urgent,
                            owner,
                        )
                        .await
                };
                check_eq!(result, Err(NetError::ConfigError))?;
            }
            let queue = if urgent {
                &inner.urgent_queue
            } else {
                &inner.queue
            };
            if synchronous {
                inner.try_send_untracked_scoped(
                    WsBody::Text("current".into()),
                    WSRequestConfig::default(),
                    urgent,
                    Some(scope),
                )?;
                queue
                    .try_next()
                    .ok_or_else(|| test_error("scoped lane missing"))?
                    .complete(Ok(()));
            } else {
                let mut sending = Box::pin(inner.send_untracked_scoped(
                    WsBody::Text("current".into()),
                    WSRequestConfig::default(),
                    urgent,
                    Some(scope),
                ));
                check!(futures::poll!(&mut sending).is_pending())?;
                queue
                    .try_next()
                    .ok_or_else(|| test_error("scoped lane missing"))?
                    .complete(Ok(()));
                sending.await?;
            }
            check!(inner.pending_requests.is_empty())?;
        }
    }
    Ok(())
}

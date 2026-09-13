use super::*;
use crate::api::wsc::request_registration::RegistrationControl;
use crate::api::wsc::PendingRequestCompletion;
use crate::module::ws_client::test_support::{check, check_eq, test_error, TestResult};
use crate::{WSRequestConfig, WSRequestTrait, WsBody};

const UUID: &str = "fixed-response-deadline";
const GENERATION: u64 = 609;
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);
const GRACE: Duration = Duration::from_secs(3);

struct Request;

impl WSRequestTrait for Request {
    fn uuid(&self) -> String {
        UUID.to_owned()
    }
    fn body(&self) -> Result<WsBody, NetError> {
        Ok(WsBody::Text("body".to_owned()))
    }
}

struct WrittenRegistration {
    pending: PendingRequestView,
    token: u64,
    completion: PendingRequestCompletion,
    timeout_cancel: CancellationToken,
    _phase: DispatchPhase,
}

fn written_registration() -> TestResult<WrittenRegistration> {
    let pending = PendingRequestView::with_capacity(1);
    let phase = DispatchPhase::new();
    let config = WSRequestConfig {
        response_timeout: RESPONSE_TIMEOUT,
        ..WSRequestConfig::default()
    };
    let control = RegistrationControl::new(
        phase.clone(),
        CancellationToken::new(),
        std::sync::Weak::new(),
    );
    let (registration, completion) = pending.reserve_snapshot_observed(
        UUID.to_owned(),
        Arc::new(Request),
        &config,
        None,
        None,
        Some(control),
        None,
    )?;
    let token = registration.raw_token();
    check!(phase.start_writing())?;
    check!(pending.mark_writing(UUID, token, 0, GENERATION))?;
    check!(phase.start_data_write())?;
    check!(phase.commit())?;
    let timeout_cancel = pending
        .mark_sent(UUID, token)
        .ok_or_else(|| test_error("could not record completed write"))?;
    Ok(WrittenRegistration {
        pending,
        token,
        completion,
        timeout_cancel,
        _phase: phase,
    })
}

#[tokio::test(start_paused = true)]
async fn response_timeout_uses_recorded_write_time_when_its_first_poll_is_late() -> TestResult {
    let written = written_registration()?;
    tokio::time::advance(RESPONSE_TIMEOUT + Duration::from_secs(1)).await;
    let timer = expire_pending_after_response_timeout(
        written.pending.clone(),
        UUID.to_owned(),
        written.token,
        written.timeout_cancel,
        RESPONSE_TIMEOUT,
        GRACE,
    );
    check!(
        timer.now_or_never().is_some(),
        "late first timer poll restarted the response budget"
    )?;
    check!(written.pending.is_empty())?;
    check_eq!(written.completion.wait().await, Err(NetError::TimeoutError))?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn response_dispatch_grace_cannot_restart_when_timeout_processing_is_late() -> TestResult {
    let written = written_registration()?;
    let _accepted_response = written.pending.begin_response_dispatch(GENERATION);
    let mut timer = Box::pin(expire_pending_after_response_timeout(
        written.pending.clone(),
        UUID.to_owned(),
        written.token,
        written.timeout_cancel,
        RESPONSE_TIMEOUT,
        GRACE,
    ));
    check!(futures::poll!(&mut timer).is_pending())?;
    // The local future is not spawned; its timeout branch cannot execute during this advance.
    tokio::time::advance(RESPONSE_TIMEOUT + GRACE + Duration::from_secs(1)).await;
    check!(
        futures::poll!(&mut timer).is_ready(),
        "late timeout processing granted a new response grace window"
    )?;
    check!(written.pending.is_empty())?;
    check_eq!(written.completion.wait().await, Err(NetError::TimeoutError))?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn response_dispatch_grace_only_uses_time_remaining_before_the_fixed_limit() -> TestResult {
    let written = written_registration()?;
    let _accepted_response = written.pending.begin_response_dispatch(GENERATION);
    let mut timer = Box::pin(expire_pending_after_response_timeout(
        written.pending.clone(),
        UUID.to_owned(),
        written.token,
        written.timeout_cancel,
        RESPONSE_TIMEOUT,
        GRACE,
    ));
    check!(futures::poll!(&mut timer).is_pending())?;
    tokio::time::advance(RESPONSE_TIMEOUT + Duration::from_secs(1)).await;
    check!(futures::poll!(&mut timer).is_pending())?;
    check_eq!(written.pending.len(), 1)?;
    tokio::time::advance(GRACE - Duration::from_secs(1)).await;
    check!(
        futures::poll!(&mut timer).is_ready(),
        "grace extended beyond write time plus response budget plus one grace"
    )?;
    check_eq!(written.completion.wait().await, Err(NetError::TimeoutError))?;
    Ok(())
}

use crate::module::ws_client::{
    test_support::{check, check_eq, TestResult},
    v2_test_support as fixture,
};
use crate::ws::*;
use crate::{error::ErrorKind, NetError};

#[test]
fn operation_phase_and_delivery_are_independent_until_the_unique_terminal() -> TestResult {
    let core = fixture::operation(&SendOptions::default(), false, 1)?;
    check_eq!(core.snapshot()?.phase, OperationPhase::WaitingForCapacity)?;
    core.prepare()?;
    core.enqueue()?;
    check!(core.mark_writing(ConnectionId::from_allocated(1)))?;
    check_eq!(core.snapshot()?.delivery, DeliveryEvidence::NotStarted)?;
    check!(core.start_data_write(false)?)?;
    check_eq!(core.snapshot()?.delivery, DeliveryEvidence::Unknown)?;
    check!(core.requeue())?;
    check_eq!(core.snapshot()?.delivery, DeliveryEvidence::Unknown)?;
    check!(core.mark_writing(ConnectionId::from_allocated(2)))?;
    let (won, publication) = core.select_written();
    publication.dispatch();
    check!(won)?;
    check_eq!(core.snapshot()?.delivery, DeliveryEvidence::Written)?;
    check_eq!(core.cancel()?, TerminationOutcome::AlreadyFinished)?;
    check_eq!(core.snapshot()?.result, Some(Ok(TaskSuccess::Written)))?;
    Ok(())
}
#[test]
fn cancellation_before_and_during_write_selects_correct_delivery_and_retirement() -> TestResult {
    for started in [false, true] {
        let core = fixture::operation(&SendOptions::default(), false, 1)?;
        core.enqueue()?;
        check!(core.mark_writing(ConnectionId::from_allocated(1)))?;
        let retirement = tokio_util::sync::CancellationToken::new();
        core.bind_write_retirement(retirement.clone());
        if started {
            check!(core.start_data_write(false)?)?;
        }
        check_eq!(
            core.cancel()?,
            if started {
                TerminationOutcome::DeliveryUnknown
            } else {
                TerminationOutcome::TerminatedBeforeWrite
            }
        )?;
        check_eq!(retirement.is_cancelled(), started)?;
        check!(!core.start_data_write(false)?)?;
        check_eq!(core.cancel()?, TerminationOutcome::AlreadyFinished)?;
    }
    Ok(())
}
#[test]
fn response_confirmation_survives_later_failure_and_finishes_continuation_framing() -> TestResult {
    let core = fixture::operation(&SendOptions::default(), true, 1)?;
    core.enqueue()?;
    check!(core.mark_writing(ConnectionId::from_allocated(1)))?;
    check!(core.start_data_write(false)?)?;
    let (won, publication) = core.select_response();
    publication.dispatch();
    check!(won)?;
    check!(core.start_data_write(true)?)?;
    core.terminate(
        NetError::from(ErrorKind::DeliveryUnknown),
        TaskEndCause::Failed,
    )?;
    check_eq!(
        core.snapshot()?.result,
        Some(Ok(TaskSuccess::ResponseReceived))
    )?;
    check_eq!(
        core.snapshot()?.delivery,
        DeliveryEvidence::ResponseConfirmed
    )?;
    Ok(())
}

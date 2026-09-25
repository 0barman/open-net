use super::*;
use crate::module::ws_client::{
    test_support::{check, check_eq, test_error, TestResult},
    v2_test_support as fixture,
};
use crate::ws::{RequestOptions, ResponseRouting, WebSocketClientConfig};

const RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);
const GRACE: Duration = Duration::from_secs(3);

#[tokio::test(start_paused = true)]
async fn recorded_write_deadline_and_manual_grace_never_restart_on_late_processing() -> TestResult {
    for (accepted, late) in [(false, true), (true, true), (true, false)] {
        let mut config = WebSocketClientConfig::default();
        config.requests.manual_response_grace = GRACE;
        let runtime = fixture::runtime(config, ResponseRouting::Manual, true).await?;
        let receipt = fixture::requests(&runtime)
            .request(fixture::request("fixed-deadline")?)
            .options(RequestOptions {
                response_timeout: RESPONSE_TIMEOUT,
                ..Default::default()
            })
            .try_enqueue()
            .map_err(|e| e.into_error())?;
        let handle = receipt.handle().clone();
        let queued = runtime
            .queue
            .try_next()
            .ok_or_else(|| test_error("request absent"))?;
        check!(queued
            .dispatch_phase
            .mark_writing(crate::ws::ConnectionId::from_allocated(1)))?;
        check!(runtime.pending.bind_connection(
            handle.registration(),
            crate::ws::ConnectionId::from_allocated(1)
        )?)?;
        check!(queued.dispatch_phase.start_data_write(false)?)?;
        let written = Instant::now();
        check!(runtime
            .pending
            .mark_written(handle.registration(), written.into_std())?)?;
        let mut response = fixture::incoming(1, "accepted reply");
        if accepted {
            runtime.pending.register_dispatch(&mut response)?;
        }
        tokio::time::advance(
            RESPONSE_TIMEOUT
                + if late {
                    GRACE + Duration::from_secs(1)
                } else {
                    Duration::from_secs(1)
                },
        )
        .await;
        runtime
            .pending
            .refresh_response_deadline(handle.request_id(), handle.registration().token())?;
        if accepted && !late {
            check_eq!(runtime.pending.snapshot()?.len(), 1)?;
            tokio::time::advance(GRACE - Duration::from_secs(1)).await;
            runtime
                .pending
                .refresh_response_deadline(handle.request_id(), handle.registration().token())?;
        }
        check!(runtime.pending.snapshot()?.is_empty())?;
        check_eq!(
            receipt.response().await.err().map(|e| e.kind()),
            Some(crate::error::ErrorKind::TimedOut)
        )?;
        check_eq!(
            handle.state()?.delivery,
            crate::ws::DeliveryEvidence::Written
        )?;
        check!(Instant::now() >= written + RESPONSE_TIMEOUT)?;
        drop(queued);
    }
    Ok(())
}

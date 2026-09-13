use super::*;
use crate::module::ws_client::test_support::{check, check_eq, test_error, TestResult};

fn observed_inner(
    loss_epoch: u64,
) -> TestResult<(Arc<WSClientInner>, watch::Sender<NetworkStatusSnapshot>)> {
    let (mut inner, _worker) = WSClientInner::new(WebSocketClientConfig::default())?;
    let (source, receiver) = watch::channel(NetworkStatusSnapshot {
        revision: loss_epoch,
        loss_epoch,
        status: Some(NetworkStatus::Available),
    });
    Arc::get_mut(&mut inner)
        .ok_or_else(|| test_error("new client unexpectedly shared"))?
        .network_status = Some(receiver);
    inner.send_admission.begin_session();
    inner
        .send_admission
        .connection_succeeded_with_network_epoch(loss_epoch);
    *inner
        .state
        .write()
        .map_err(|_| test_error("state lock poisoned"))? = ConnectionStatus::Connected;
    Ok((inner, source))
}

#[test]
fn observed_unavailable_rejects_send_before_worker_changes_connected_state() -> TestResult {
    let (inner, source) = observed_inner(7)?;
    source.send_replace(NetworkStatusSnapshot {
        revision: 8,
        loss_epoch: 8,
        status: Some(NetworkStatus::Unavailable),
    });
    check_eq!(inner.connection_status(), ConnectionStatus::Connected)?;
    check!(matches!(
        inner.validate_send_admission(&WSRequestConfig::default()),
        Err(NetError::NetworkError)
    ))?;
    Ok(())
}

#[test]
fn available_after_coalesced_loss_does_not_admit_into_old_connection() -> TestResult {
    let (inner, source) = observed_inner(7)?;
    source.send_replace(NetworkStatusSnapshot {
        revision: 8,
        loss_epoch: 8,
        status: Some(NetworkStatus::Unavailable),
    });
    source.send_replace(NetworkStatusSnapshot {
        revision: 9,
        loss_epoch: 8,
        status: Some(NetworkStatus::Available),
    });
    check!(matches!(
        inner.validate_send_admission(&WSRequestConfig::default()),
        Err(NetError::NetworkError)
    ))?;
    inner
        .send_admission
        .connection_succeeded_with_network_epoch(8);
    check!(inner
        .validate_send_admission(&WSRequestConfig::default())
        .is_ok())?;
    Ok(())
}

#[test]
fn waiting_requests_retain_session_and_unknown_health_does_not_block_send() -> TestResult {
    let (inner, source) = observed_inner(7)?;
    source.send_replace(NetworkStatusSnapshot {
        revision: 8,
        loss_epoch: 7,
        status: None,
    });
    check!(inner
        .validate_send_admission(&WSRequestConfig::default())
        .is_ok())?;
    source.send_replace(NetworkStatusSnapshot {
        revision: 9,
        loss_epoch: 8,
        status: Some(NetworkStatus::Unavailable),
    });
    let lease = inner.validate_send_admission(&WSRequestConfig {
        disconnected_policy: DisconnectedTaskPolicy::WaitForReconnect,
        ..WSRequestConfig::default()
    })?;
    inner.send_admission.connection_ended();
    check!(!lease.cancel.is_cancelled())?;
    inner.request_shutdown();
    check!(lease.cancel.is_cancelled())?;
    Ok(())
}

#[test]
fn send_lease_captures_connection_epoch_before_reconnect_replaces_it() -> TestResult {
    let (inner, _source) = observed_inner(7)?;
    let old = inner
        .send_admission
        .lease_observed(DisconnectedTaskPolicy::Reject);
    inner
        .send_admission
        .connection_succeeded_with_network_epoch(8);
    let current = inner
        .send_admission
        .lease_observed(DisconnectedTaskPolicy::Reject);
    check_eq!(old.network_loss_epoch, 7)?;
    check!(old.cancel.is_cancelled())?;
    check_eq!(current.network_loss_epoch, 8)?;
    check!(!current.cancel.is_cancelled())?;
    Ok(())
}

#[test]
fn saturated_control_queue_cannot_hide_network_loss_from_send_admission() -> TestResult {
    let (mut inner, _worker) = WSClientInner::new(WebSocketClientConfig::default())?;
    let (source, receiver) = watch::channel(NetworkStatusSnapshot {
        revision: 0,
        loss_epoch: 0,
        status: Some(NetworkStatus::Available),
    });
    Arc::get_mut(&mut inner)
        .ok_or_else(|| test_error("new client shared"))?
        .network_status = Some(receiver);
    inner.send_admission.begin_session();
    inner.send_admission.connection_succeeded();
    *inner
        .state
        .write()
        .map_err(|_| test_error("state lock poisoned"))? = ConnectionStatus::Connected;
    for _ in 0..64 {
        inner
            .command_tx
            .try_send(ClientCommand::NetworkAvailable)
            .map_err(|_| test_error("command queue filled too early"))?;
    }
    check_eq!(inner.command_tx.capacity(), 0)?;
    source.send_replace(NetworkStatusSnapshot {
        revision: 2,
        loss_epoch: 1,
        status: Some(NetworkStatus::Available),
    });
    inner.notify_network_available();
    check!(matches!(
        inner.validate_send_admission(&WSRequestConfig::default()),
        Err(NetError::NetworkError)
    ))?;
    check!(inner.pending_requests.is_empty())?;
    Ok(())
}

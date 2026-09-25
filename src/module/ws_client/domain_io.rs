//! One physical connection's currently buffered business message.
//!
//! Lock order: I/O slot -> domain -> dispatch metadata. Cancellation releases the
//! domain before cleanup enters pending/queue locks. No guard crosses an await.
use crate::common::log::log_def::LogType;
use crate::module::ws_client::operation_control::OperationControl;
use crate::NetError;
use std::sync::{Arc, Mutex};

#[derive(Default)]
pub(crate) struct DomainIoGate {
    active: Mutex<Option<Arc<OperationControl>>>,
}

pub(crate) struct ActiveDomainIo {
    gate: Arc<DomainIoGate>,
}

impl DomainIoGate {
    pub(crate) fn activate(
        self: &Arc<Self>,
        phase: Arc<OperationControl>,
    ) -> Result<ActiveDomainIo, NetError> {
        let mut active = self
            .active
            .lock()
            .map_err(|_| NetError::from(crate::error::ErrorKind::Internal))?;
        if active.is_some() {
            return Err(NetError::from(crate::error::ErrorKind::Internal));
        }
        *active = Some(phase);
        Ok(ActiveDomainIo { gate: self.clone() })
    }

    pub(crate) fn run<T>(&self, io: impl FnOnce() -> T) -> Result<T, NetError> {
        let active = self
            .active
            .lock()
            .map_err(|_| NetError::from(crate::error::ErrorKind::Internal))?
            .clone();
        if let Some(phase) = active.as_ref().filter(|phase| phase.has_in_flight_data()) {
            if let Some(domain) = phase.cancel_domain() {
                return match domain.inner.lock_if_active() {
                    Ok(_guard) => Ok(io()),
                    // A concurrent successful commit can finish the message
                    // between our phase snapshot and acquiring the domain gate.
                    Err(_) if !phase.has_in_flight_data() => Ok(io()),
                    Err(error) => {
                        // Freeze both halves before writer cleanup can run.
                        phase.retire_write();
                        Err(error)
                    }
                };
            }
        }
        Ok(io())
    }
}

impl Drop for ActiveDomainIo {
    fn drop(&mut self) {
        let mut active = match self.gate.active.lock() {
            Ok(active) => active,
            Err(poisoned) => {
                crate::log_e!(LogType::WSC; "domain_io_drop", "error", "lock_poisoned_recovered");
                poisoned.into_inner()
            }
        };
        let retired = active.take();
        drop(active);
        if let Some(phase) = retired {
            if phase.has_in_flight_data() {
                phase.retire_write();
            }
        }
    }
}

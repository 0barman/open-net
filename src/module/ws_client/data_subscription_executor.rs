//! Generic subscriptions submitted to the client's existing data callback lane.

use super::callback_event::CallbackEvent;
use super::callback_executor::DataCallbackPool;
use super::listener_executor::CallbackJob;
use crate::error::{ErrorKind, NetError};
use crate::subscription::CallbackExecutor;
use crate::ws::SessionId;
use crate::Result;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

/// Uses the original bounded queue, in-flight admission and lazily grown data pool.
/// Holding this sender neither creates a dispatcher nor starts a callback thread.
pub(crate) struct DataSubscriptionExecutor {
    sender: mpsc::Sender<CallbackEvent>,
    session_id: SessionId,
    initial_slot: Mutex<Option<mpsc::OwnedPermit<CallbackEvent>>>,
    pool: Arc<DataCallbackPool>,
}

impl DataSubscriptionExecutor {
    pub(super) fn for_session(
        sender: mpsc::Sender<CallbackEvent>,
        session_id: SessionId,
        pool: Arc<DataCallbackPool>,
    ) -> Self {
        Self {
            sender,
            session_id,
            initial_slot: Mutex::new(None),
            pool,
        }
    }

    /// Each registration has its own first-submit reservation.
    pub(crate) fn fork(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            session_id: self.session_id,
            initial_slot: Mutex::new(None),
            pool: Arc::clone(&self.pool),
        }
    }
}

impl CallbackExecutor for DataSubscriptionExecutor {
    fn ensure_ready(&self) -> Result<()> {
        if self.sender.is_closed() {
            return Err(NetError::from(ErrorKind::QueueClosed));
        }
        self.pool.ensure_ready().map_err(|error| {
            NetError::with_source(ErrorKind::RuntimeUnavailable, error)
                .with_stage(crate::error::ErrorStage::Runtime)
        })?;
        let mut slot = self.initial_slot.lock().map_err(NetError::from_poison)?;
        if slot.is_none() {
            *slot = Some(
                self.sender
                    .clone()
                    .try_reserve_owned()
                    .map_err(|error| match error {
                        mpsc::error::TrySendError::Full(_) => NetError::from(ErrorKind::QueueFull),
                        mpsc::error::TrySendError::Closed(_) => {
                            NetError::from(ErrorKind::QueueClosed)
                        }
                    })?,
            );
        }
        Ok(())
    }

    fn submit(&self, job: CallbackJob) -> Result<()> {
        let permit = self
            .initial_slot
            .lock()
            .map_err(NetError::from_poison)?
            .take();
        let event = CallbackEvent::SessionJob {
            session_id: self.session_id,
            job,
        };
        if let Some(permit) = permit {
            let sender = permit.send(event);
            drop(sender);
            return Ok(());
        }
        self.sender.try_send(event).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => NetError::from(ErrorKind::QueueFull),
            mpsc::error::TrySendError::Closed(_) => NetError::from(ErrorKind::QueueClosed),
        })
    }
}

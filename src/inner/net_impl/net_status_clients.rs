use super::OpenNetInner;
use crate::{NetError, NetStatusClient};
use std::sync::Arc;

pub(super) enum NetStatusClientSlot {
    Ready(NetStatusClient),
    Closing(NetStatusClient),
}

impl OpenNetInner {
    pub(crate) fn create_net_status_client(
        &self,
        thread_name: &str,
    ) -> Result<NetStatusClient, NetError> {
        if thread_name.contains('\0') {
            return Err(NetError::from(crate::error::ErrorKind::InvalidConfig));
        }
        let mut clients = self
            .net_status_clients
            .lock()
            .map_err(NetError::from_poison)?;
        if clients.contains_key(thread_name) {
            return Err(NetError::from(crate::error::ErrorKind::ClientAlreadyExists));
        }
        // Construction only allocates the facade; monitoring starts explicitly.
        // Admission and publication are atomic and have no cancellation point.
        let client = NetStatusClient::new(self.network_status.context())?;
        clients.insert(
            thread_name.to_owned(),
            NetStatusClientSlot::Ready(client.clone()),
        );
        Ok(client)
    }

    pub(crate) fn get_net_status_client(
        &self,
        thread_name: &str,
    ) -> Result<NetStatusClient, NetError> {
        let clients = self
            .net_status_clients
            .lock()
            .map_err(NetError::from_poison)?;
        match clients.get(thread_name) {
            Some(NetStatusClientSlot::Ready(client)) => Ok(client.clone()),
            Some(NetStatusClientSlot::Closing(_)) => {
                Err(NetError::from(crate::error::ErrorKind::ConnectionClosing))
            }
            None => Err(NetError::from(crate::error::ErrorKind::ClientNotFound)),
        }
    }

    pub(crate) async fn destroy_net_status_client(
        &self,
        thread_name: &str,
    ) -> Result<(), NetError> {
        let client = {
            let mut clients = self
                .net_status_clients
                .lock()
                .map_err(NetError::from_poison)?;
            let client = match clients.get(thread_name) {
                Some(NetStatusClientSlot::Ready(client)) => client.clone(),
                Some(NetStatusClientSlot::Closing(_)) => {
                    return Err(NetError::from(crate::error::ErrorKind::ConnectionClosing))
                }
                None => return Err(NetError::from(crate::error::ErrorKind::ClientNotFound)),
            };
            clients.insert(
                thread_name.to_owned(),
                NetStatusClientSlot::Closing(client.clone()),
            );
            client
        };
        client.request_destroy();
        let clients = Arc::clone(&self.net_status_clients);
        let name = thread_name.to_owned();
        let (completed, completion) = tokio::sync::oneshot::channel();
        // Cleanup belongs to the engine once submitted. Dropping the caller's
        // future cannot strand a Closing reservation or revive the old client.
        self.common_engine.runtime_handle().spawn(async move {
            let result = client.destroy().await.and_then(|()| {
                let mut clients = clients.lock().map_err(NetError::from_poison)?;
                clients.remove(&name);
                Ok(())
            });
            // An error is not proof that internal cleanup completed. Keep the
            // Closing reservation and report failure, even if its waiter left.
            if let Err(error) = &result {
                crate::log_e!(crate::common::log::log_def::LogType::Engine; "net_status_destroy_cleanup", "error", crate::common::log::summary::error(error));
            }
            let _ = completed.send(result);
        });
        completion
            .await
            .map_err(|_| NetError::from(crate::error::ErrorKind::Internal))?
    }

    pub(super) fn stop_net_status_clients(&self) {
        let entries = {
            let mut clients = match self.net_status_clients.lock() {
                Ok(clients) => clients,
                Err(poisoned) => {
                    crate::log_e!(crate::common::log::log_def::LogType::Engine; "net_status_registry_drop", "error", "poisoned_registry_recovered");
                    poisoned.into_inner()
                }
            };
            clients.drain().map(|(_, slot)| slot).collect::<Vec<_>>()
        };
        for slot in entries {
            match slot {
                NetStatusClientSlot::Ready(client) | NetStatusClientSlot::Closing(client) => {
                    client.request_destroy();
                }
            }
        }
    }
}

#[cfg(test)]
mod cleanup_error_tests {
    use crate::{error::ErrorKind, OpenNet};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn failed_named_cleanup_retains_closing_name_reservation() -> Result<(), crate::BoxError>
    {
        let net = OpenNet::new()?;
        let (entered, entry) = oneshot::channel();
        let entered = Mutex::new(Some(entered));
        net.inner
            .network_status
            .inner_for_test()
            .set_monitor_factory_for_test(Arc::new(move || {
                if let Ok(mut entered) = entered.lock() {
                    if let Some(entered) = entered.take() {
                        let _ = entered.send(());
                    }
                }
                Box::pin(std::future::pending())
            }))?;
        let client = net.create_net_status_client("cleanup-error-name").await?;
        let waiting = client.clone();
        let start = tokio::spawn(async move { waiting.start().await });
        tokio::time::timeout(Duration::from_secs(5), entry).await??;
        net.inner
            .network_status
            .inner_for_test()
            .exhaust_source_revision_for_test()?;
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            net.destroy_net_status_client("cleanup-error-name"),
        )
        .await?;
        if !matches!(result, Err(error) if error.kind() == ErrorKind::Internal) {
            return Err("cleanup did not report its source publication failure".into());
        }
        if tokio::time::timeout(Duration::from_secs(5), start)
            .await??
            .is_ok()
        {
            return Err("closed facade kept a successful pending start".into());
        }
        if !matches!(net.get_net_status_client("cleanup-error-name"), Err(error) if error.kind() == ErrorKind::ConnectionClosing)
        {
            return Err("failed cleanup prematurely released the Closing name".into());
        }
        if !matches!(net.create_net_status_client("cleanup-error-name").await, Err(error) if error.kind() == ErrorKind::ClientAlreadyExists)
        {
            return Err("unconfirmed cleanup allowed name reuse".into());
        }
        if net.inner.network_status.active_consumers_for_test()? != 0 {
            return Err("failed cleanup retained monitoring demand".into());
        }
        Ok(())
    }
}

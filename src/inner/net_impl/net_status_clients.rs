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
            return Err(NetError::ConfigError);
        }
        let mut clients = self
            .net_status_clients
            .lock()
            .map_err(NetError::from_poison)?;
        if clients.contains_key(thread_name) {
            return Err(NetError::ClientAlreadyExists);
        }
        // Construction only allocates the facade; monitoring starts explicitly.
        // Admission and publication are atomic and have no cancellation point.
        let client = NetStatusClient::new(Arc::clone(&self.common_engine));
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
            Some(NetStatusClientSlot::Closing(_)) => Err(NetError::ConnectionClosing),
            None => Err(NetError::ClientNotFound),
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
                Some(NetStatusClientSlot::Closing(_)) => return Err(NetError::ConnectionClosing),
                None => return Err(NetError::ClientNotFound),
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
            let result = client.destroy().await;
            if let Ok(mut clients) = clients.lock() {
                clients.remove(&name);
            }
            let _ = completed.send(result);
        });
        completion
            .await
            .map_err(|_| NetError::TaskInterruptionError)?
    }

    pub(super) fn stop_net_status_clients(&self) {
        let entries = {
            let mut clients = self
                .net_status_clients
                .lock()
                .unwrap_or_else(|error| error.into_inner());
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

//! Test-owned V2 sessions. The Session, not an event receiver, owns the connection.
#![allow(dead_code)]
use open_net::ws::{
    ConnectOptions, ConnectionEvent, ConnectionJournal, JournalOptions, ReconnectPolicy, Session,
    WebSocketClient,
};
use open_net::{error::ReceiveError, NetError};

pub fn session_options(url: impl Into<String>, reconnect: ReconnectPolicy) -> ConnectOptions {
    let mut options = ConnectOptions::new(url);
    options.reconnect = reconnect;
    options.routing = open_net::ws::ResponseRouting::Manual;
    options
}

#[derive(Debug)]
pub struct ObservedSession {
    pub session: Session,
    pub journal: ConnectionJournal,
}

pub async fn observe(
    client: &WebSocketClient,
    options: ConnectOptions,
) -> Result<ObservedSession, NetError> {
    observe_with(client, options, JournalOptions::default()).await
}

pub async fn observe_with(
    client: &WebSocketClient,
    options: ConnectOptions,
    journal: JournalOptions,
) -> Result<ObservedSession, NetError> {
    let mut session = client.start_session(options, Some(journal)).await?;
    let journal = session
        .take_journal()
        .ok_or_else(|| NetError::from(open_net::error::ErrorKind::Internal))?;
    Ok(ObservedSession { session, journal })
}

impl ObservedSession {
    pub async fn recv(&mut self) -> Result<Option<ConnectionEvent>, ReceiveError> {
        self.journal.recv().await
    }
}

#[derive(Debug)]
pub struct SessionGuard {
    pub session: Session,
}
impl SessionGuard {
    pub async fn establish(
        client: &WebSocketClient,
        options: ConnectOptions,
    ) -> Result<Self, NetError> {
        Ok(Self {
            session: client.connect(options).await?,
        })
    }
    pub async fn finish(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let _terminal =
            tokio::time::timeout(std::time::Duration::from_secs(5), self.session.closed()).await?;
        Ok(())
    }
}

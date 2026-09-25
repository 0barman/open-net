#![cfg(feature = "ws-client")]

use futures::Stream;
use open_net::error::{ReceiveError, TryReceiveError};
use open_net::subscription::EventReceiver;
use open_net::ws::{ConnectionEvent, ConnectionEvents, ConnectionJournal};

#[test]
fn connection_receiver_names_expose_the_native_stream_and_fallible_receive_contract() {
    fn require_stream<T: Stream<Item = Result<ConnectionEvent, ReceiveError>> + Send>() {}
    require_stream::<ConnectionJournal>();
    require_stream::<ConnectionEvents>();
    let _journal: fn(EventReceiver<ConnectionEvent>) -> ConnectionJournal = |receiver| receiver;
    let _events: fn(EventReceiver<ConnectionEvent>) -> ConnectionEvents = |receiver| receiver;
    let _receive: fn(&mut ConnectionJournal) -> Result<ConnectionEvent, TryReceiveError> =
        ConnectionJournal::try_recv;
    let _unsubscribe: fn(&ConnectionJournal) -> bool = ConnectionJournal::unsubscribe;
}

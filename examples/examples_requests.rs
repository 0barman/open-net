//! Echo protocol example: wire messages contain "request-id|payload".
//! A real application's ResponseProtocol should decode its actual wire format.
use open_net::{
    ws::{
        ConnectOptions, IncomingMessage, InitialMessages, Message, Request, RequestId,
        ResponseProtocol, ResponseRoute, ResponseRouting,
    },
    BoxError, OpenNet,
};

struct EchoProtocol;
impl ResponseProtocol for EchoProtocol {
    fn route(&self, incoming: &IncomingMessage) -> Result<ResponseRoute, BoxError> {
        match incoming
            .message()
            .and_then(Message::as_text)
            .and_then(|text| text.split_once('|'))
        {
            Some((id, _)) => Ok(ResponseRoute::Final {
                request_id: RequestId::new(id)?,
            }),
            None => Ok(ResponseRoute::Unmatched),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let url = std::env::args()
        .nth(1)
        .ok_or("provide a WebSocket echo URL")?;
    let net = OpenNet::new()?;
    let client = net.create_ws_client("requests").await?;
    let mut options = ConnectOptions::new(url);
    options.routing = ResponseRouting::protocol(EchoProtocol);
    options.initial_messages = InitialMessages::DiscardUnmatched;
    let session = client.connect(options).await?;

    let id = RequestId::random()?;
    // RequestId is local association metadata; encode it into your wire protocol explicitly.
    let body = Message::text(format!("{}|hello", id.as_str()));
    let response = session.requests()?.execute(Request::new(id, body)).await?;
    println!("{:?}", response.message());

    session.close().await?;
    net.destroy_ws_client("requests").await?;
    Ok(())
}

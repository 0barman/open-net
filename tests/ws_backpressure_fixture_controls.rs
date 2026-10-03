#![cfg(feature = "ws-client")]

#[path = "support/backpressure.rs"]
mod backpressure;

use backpressure::{release_receive_window, AbortOnDrop};
use socket2::SockRef;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
async fn native_tcp_confirms_paused_reader_pressure_then_full_transfer() -> TestResult {
    const BYTES: usize = 32 * 1024 * 1024;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (prefix_tx, prefix_rx) = oneshot::channel();
    let (resume_tx, resume_rx) = oneshot::channel();
    let mut peer = AbortOnDrop(tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let mut prefix = [0_u8; 64];
        stream.read_exact(&mut prefix).await?;
        prefix_tx.send(()).map_err(|_| "prefix receiver closed")?;
        resume_rx.await?;
        release_receive_window(&stream)?;
        let mut received = prefix.len();
        let mut buffer = [0_u8; 32 * 1024];
        while received < BYTES {
            let limit = (BYTES - received).min(buffer.len());
            let count = stream.read(&mut buffer[..limit]).await?;
            if count == 0 {
                return Err("native sender ended before complete payload".into());
            }
            received = received
                .checked_add(count)
                .ok_or("native byte count overflow")?;
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(received)
    }));
    let mut sender = TcpStream::connect(address).await?;
    SockRef::from(&sender).set_send_buffer_size(4096)?;
    sender.set_nodelay(true)?;
    let mut writing = Box::pin(async move {
        let block = [0x5a_u8; 32 * 1024];
        for _ in 0..BYTES / block.len() {
            sender.write_all(&block).await?;
        }
        Ok::<_, std::io::Error>(())
    });
    if futures::poll!(writing.as_mut()).is_ready() {
        return Err("native transfer bypassed the paused peer".into());
    }
    tokio::time::timeout(Duration::from_secs(10), prefix_rx).await??;
    if futures::poll!(writing.as_mut()).is_ready() {
        return Err("native transfer completed before releasing pressure".into());
    }
    resume_tx
        .send(())
        .map_err(|_| "native peer ended before release")?;
    tokio::time::timeout(Duration::from_secs(10), writing).await??;
    let received = tokio::time::timeout(Duration::from_secs(10), &mut peer.0).await???;
    if received != BYTES {
        return Err("native transfer lost bytes".into());
    }
    Ok(())
}

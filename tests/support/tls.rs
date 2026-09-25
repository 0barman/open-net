use std::io::{self, ErrorKind};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

/// Let the peer receive the fatal TLS alert before releasing the TCP socket.
pub async fn close_rejected_connection(socket: &mut TcpStream) -> io::Result<()> {
    // With TLS 1.3, the client can already be sending its HTTP Upgrade when the
    // server rejects its certificate. Dropping a socket with unread input can
    // reset the connection on Windows and hide the TLS alert. Half-close the
    // write side, then drain input until the client closes after reading it.
    let closed = tokio::time::timeout(Duration::from_secs(2), async {
        socket.shutdown().await?;
        tokio::io::copy(socket, &mut tokio::io::sink()).await?;
        io::Result::Ok(())
    })
    .await
    .map_err(|_| io::Error::new(ErrorKind::TimedOut, "rejected TLS peer did not close"))?;
    match closed {
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::ConnectionReset | ErrorKind::ConnectionAborted | ErrorKind::NotConnected
            ) =>
        {
            // A peer that has already closed needs no further draining.
            Ok(())
        }
        result => result,
    }
}

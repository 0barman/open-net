//! Private transport composition. Proxy TLS owns its TCP socket, while the
//! CONNECT reader (including unread tunnel bytes) belongs to the target stream.

use std::fmt;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, BufReader, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

pub(crate) type TunnelIo = BufReader<ProxyIo>;
pub(crate) type NativeWebSocketStream = WebSocketStream<MaybeTlsStream<TunnelIo>>;

/// A bounded read buffer is retained through CONNECT, target TLS and Upgrade.
pub(crate) fn tunnel_io(io: ProxyIo) -> TunnelIo {
    BufReader::with_capacity(1024, io)
}

pub(crate) fn tcp_ref(stream: &NativeWebSocketStream) -> &TcpStream {
    stream.get_ref().get_ref().get_ref().tcp_ref()
}

pub(crate) enum ProxyIo {
    Plain(TcpStream),
    // Avoid making every plain connection/future carry an inline TLS session.
    Tls(Box<TlsStream<TcpStream>>),
}

impl ProxyIo {
    fn tcp_ref(&self) -> &TcpStream {
        match self {
            Self::Plain(socket) => socket,
            Self::Tls(stream) => stream.get_ref().0,
        }
    }
}

impl AsyncRead for ProxyIo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(socket) => Pin::new(socket).poll_read(cx, buf),
            Self::Tls(stream) => Pin::new(stream)
                .poll_read(cx, buf)
                .map_err(mark_proxy_tls_error),
        }
    }
}

impl AsyncWrite for ProxyIo {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(socket) => Pin::new(socket).poll_write(cx, buf),
            Self::Tls(stream) => Pin::new(stream)
                .poll_write(cx, buf)
                .map_err(mark_proxy_tls_error),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(socket) => Pin::new(socket).poll_flush(cx),
            Self::Tls(stream) => Pin::new(stream)
                .poll_flush(cx)
                .map_err(mark_proxy_tls_error),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(socket) => Pin::new(socket).poll_shutdown(cx),
            Self::Tls(stream) => Pin::new(stream)
                .poll_shutdown(cx)
                .map_err(mark_proxy_tls_error),
        }
    }
}

/// TLS alerts can arrive after connect() completes, including while the inner
/// TLS handshake is running. Preserve the outer layer at its I/O boundary.
struct ProxyTlsError(io::Error);

impl fmt::Debug for ProxyTlsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProxyTlsError")
            .field("kind", &self.0.kind())
            .finish_non_exhaustive()
    }
}

impl fmt::Display for ProxyTlsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("proxy TLS failed")
    }
}

impl std::error::Error for ProxyTlsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

pub(super) fn mark_proxy_tls_error(error: io::Error) -> io::Error {
    if error
        .get_ref()
        .is_some_and(|source| source.is::<rustls::Error>())
        || error.kind() == io::ErrorKind::InvalidData
    {
        io::Error::new(error.kind(), ProxyTlsError(error))
    } else {
        // Ordinary TCP errors keep the phase where they were observed.
        error
    }
}

pub(crate) fn proxy_tls_io(error: &io::Error) -> Option<&io::Error> {
    error
        .get_ref()
        .and_then(|source| source.downcast_ref::<ProxyTlsError>())
        .map(|source| &source.0)
}

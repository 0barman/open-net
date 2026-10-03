//! Keep the pressure-establishment phase separate from the recovery measurement.
use socket2::SockRef;
use tokio::net::TcpStream;

pub fn release_receive_window(stream: &TcpStream) -> std::io::Result<()> {
    let socket = SockRef::from(stream);
    socket.set_recv_buffer_size(2 * 1024 * 1024)?;
    if socket.recv_buffer_size()? < 64 * 1024 {
        return Err(std::io::Error::other(
            "peer receive window remained too small for recovery",
        ));
    }
    Ok(())
}

pub struct AbortOnDrop<T>(pub tokio::task::JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

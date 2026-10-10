#![cfg(feature = "http-client-http2")]

use bytes::Bytes;
use h2::server;
use http::{Response, StatusCode};
use open_net::api::error::{ErrorKind, ErrorStage};
use open_net::api::http::{
    Http2Config, HttpClient, HttpClientConfig, HttpProtocol, HttpTlsConfig, HttpVersionPolicy,
};
use rustls::pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;
const CA: &[u8] = include_bytes!("fixtures/network/ca.pem");
const SERVER: &[u8] = include_bytes!("fixtures/network/server.pem");
const SERVER_KEY: &[u8] = include_bytes!("fixtures/network/server-key.pem");

struct Peer {
    address: SocketAddr,
    task: tokio::task::JoinHandle<TestResult>,
}

impl Drop for Peer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Peer {
    async fn finish(mut self) -> TestResult {
        tokio::time::timeout(Duration::from_secs(5), &mut self.task).await???;
        Ok(())
    }
}

async fn tls_peer(alpn_protocols: Vec<Vec<u8>>) -> TestResult<Peer> {
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let mut config = rustls::ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_no_client_auth()
        .with_single_cert(
            CertificateDer::pem_slice_iter(SERVER).collect::<Result<Vec<_>, _>>()?,
            PrivateKeyDer::from_pem_slice(SERVER_KEY)?,
        )?;
    config.alpn_protocols = alpn_protocols;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await?;
        let accept = TlsAcceptor::from(Arc::new(config)).accept(socket).await;
        let mut stream = match accept {
            Ok(stream) => stream,
            Err(_) => return Ok(()),
        };
        match stream.get_ref().1.alpn_protocol() {
            Some(b"h2") => serve_h2(stream).await,
            Some(b"http/1.1") => serve_h1(&mut stream).await,
            other => Err(format!("unexpected ALPN: {other:?}").into()),
        }
    });
    Ok(Peer { address, task })
}

async fn serve_h2<S>(stream: S) -> TestResult
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let mut connection = server::handshake(stream).await?;
    while let Some(result) = connection.accept().await {
        let (request, mut respond) = match result {
            Ok(result) => result,
            Err(error)
                if error.get_io().is_some_and(|io| {
                    matches!(
                        io.kind(),
                        std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset
                    )
                }) =>
            {
                // 客户端完成读取后关闭连接时，h2 可能把写端关闭报告为 BrokenPipe。
                return Ok(());
            }
            Err(error) => return Err(error.into()),
        };
        assert_eq!(request.uri().path(), "/h2");
        let mut send = respond.send_response(Response::new(()), false)?;
        send.send_data(Bytes::from_static(b"h2-"), false)?;
        send.send_data(Bytes::from_static(b"ok"), true)?;
        // 响应已经结束后发送 GOAWAY，避免测试服务一直等待连接池关闭。
        connection.graceful_shutdown();
    }
    Ok(())
}

async fn serve_h1<S>(stream: &mut S) -> TestResult
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let mut request = Vec::new();
    let mut byte = [0_u8; 1];
    while !request.ends_with(b"\r\n\r\n") {
        if stream.read(&mut byte).await? == 0 {
            return Err("HTTP/1.1 peer closed before request".into());
        }
        request.push(byte[0]);
        if request.len() > 16 * 1024 {
            return Err("HTTP/1.1 request headers exceeded fixture limit".into());
        }
    }
    stream
        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\nh1-ok")
        .await?;
    stream.shutdown().await?;
    Ok(())
}

fn http2_config() -> Http2Config {
    Http2Config::new()
        .adaptive_window(true)
        .initial_stream_window(1024 * 1024)
        .initial_connection_window(4 * 1024 * 1024)
        .max_frame_size(16 * 1024)
        .max_header_list_size(64 * 1024)
        .keep_alive(Duration::from_secs(10), Duration::from_secs(5), true)
}

async fn client_for(peer: &Peer, policy: HttpVersionPolicy) -> TestResult<HttpClient> {
    let tls = HttpTlsConfig::default().with_root_certificates(CA, true)?;
    let config = HttpClientConfig::new(format!("https://{}", peer.address))?
        .with_tls(tls)
        .with_environment_proxy(false)
        .with_timeout(Duration::from_secs(3))?
        .with_http_version_policy(policy)
        .with_http2(http2_config())?;
    Ok(HttpClient::new(config, "http2-integration")?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auto_negotiates_h2_and_reports_delivery_snapshot() -> TestResult {
    let peer = tls_peer(vec![b"h2".to_vec(), b"http/1.1".to_vec()]).await?;
    let client = client_for(&peer, HttpVersionPolicy::Auto).await?;
    let response = client.get("/h2").await?;
    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(response.body, Bytes::from_static(b"h2-ok"));
    assert_eq!(response.protocol(), HttpProtocol::Http2);
    let flow = response.info().flow_control.expect("h2 flow snapshot");
    assert_eq!(flow.configured_stream_window, Some(1024 * 1024));
    assert_eq!(flow.configured_connection_window, Some(4 * 1024 * 1024));
    assert!(flow.adaptive_window);
    assert_eq!(flow.body_bytes_delivered, 5);
    assert!(flow.body_chunks_delivered >= 1);
    // 释放响应持有的底层字节流，让连接池可以在客户端关闭时回收连接。
    drop(response);
    client.shutdown().await?;
    peer.finish().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auto_falls_back_to_http1_and_http1_only_is_observable() -> TestResult {
    let peer = tls_peer(vec![b"http/1.1".to_vec()]).await?;
    let client = client_for(&peer, HttpVersionPolicy::Auto).await?;
    let response = client.get("/h2").await?;
    assert_eq!(response.protocol(), HttpProtocol::Http11);
    assert!(response.info().flow_control.is_none());
    client.shutdown().await?;
    peer.finish().await?;

    let peer = tls_peer(vec![b"h2".to_vec(), b"http/1.1".to_vec()]).await?;
    let client = client_for(&peer, HttpVersionPolicy::Http1Only).await?;
    let response = client.get("/h2").await?;
    assert_eq!(response.protocol(), HttpProtocol::Http11);
    client.shutdown().await?;
    peer.finish().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http2_only_rejects_cleartext_and_non_h2_peer() -> TestResult {
    let error = HttpClientConfig::new("http://127.0.0.1:1")?
        .with_http_version_policy(HttpVersionPolicy::Http2Only)
        .validate()
        .expect_err("cleartext h2-only must be rejected");
    assert_eq!(error.kind(), ErrorKind::InvalidConfig);
    assert_eq!(error.context().stage, Some(ErrorStage::Configuration));

    let peer = tls_peer(vec![b"http/1.1".to_vec()]).await?;
    let client = client_for(&peer, HttpVersionPolicy::Http2Only).await?;
    let error = client
        .get("/h2")
        .await
        .expect_err("h2-only must not accept an h1 peer");
    assert!(matches!(error.kind(), ErrorKind::Tls | ErrorKind::Protocol));
    client.shutdown().await?;
    peer.finish().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn streaming_snapshot_updates_without_affecting_existing_lifecycle() -> TestResult {
    let peer = tls_peer(vec![b"h2".to_vec()]).await?;
    let client = client_for(&peer, HttpVersionPolicy::Http2Only).await?;
    let request = open_net::api::http::HttpRequest::builder()
        .path("/h2")
        .build()?;
    let mut response = client.stream(request).await?;
    assert_eq!(response.info().protocol, HttpProtocol::Http2);
    assert_eq!(response.flow_control().unwrap().body_bytes_delivered, 0);
    while response.next_chunk().await.transpose()?.is_some() {}
    let flow = response.flow_control().unwrap();
    assert_eq!(flow.body_bytes_delivered, 5);
    assert!(flow.body_chunks_delivered >= 1);
    // 释放响应持有的底层字节流，让连接池可以在客户端关闭时回收连接。
    drop(response);
    client.shutdown().await?;
    peer.finish().await?;
    Ok(())
}

#![allow(clippy::expect_used, clippy::unwrap_used)]

use super::*;
use codex_http_client::OutboundProxyPolicy;
use codex_websocket_client::WebSocketTlsMode;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::accept_async;

async fn connection_pair() -> (ResponsesWebsocketConnection, WebSocketStream<TcpStream>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let request = format!("ws://{}/responses", listener.local_addr().unwrap())
        .into_client_request()
        .unwrap();
    let factory = HttpClientFactory::new(OutboundProxyPolicy::ReqwestDefault);
    let connector =
        WebSocketConnector::new_with_tls_mode(&factory, WebSocketTlsMode::TungsteniteDefault)
            .unwrap();
    let accept = async {
        let (socket, _) = listener.accept().await.unwrap();
        accept_async(socket).await.unwrap()
    };
    let (client, server) = tokio::join!(
        connector.connect_loopback_direct(request, WebSocketConfig::default()),
        accept
    );
    let (client, _) = client.unwrap();
    (
        ResponsesWebsocketConnection::new(
            WsStream::new(client),
            Duration::from_secs(300),
            false,
            None,
            None,
        ),
        server,
    )
}

#[tokio::test]
async fn idle_unresponsive_socket_is_rejected_before_reuse() {
    let (connection, _unresponsive_server) = connection_pair().await;
    // Keep TCP established while the peer stops reading WebSocket frames.
    // Wall-clock idle is intentional: it must include time spent suspended.
    tokio::time::sleep(Duration::from_secs(16)).await;
    assert!(!connection.is_closed().await);
    let reusable = tokio::time::timeout(Duration::from_secs(3), connection.probe_if_idle())
        .await
        .expect("liveness detection must finish before the model's 300-second timeout");
    assert!(
        !reusable,
        "an unresponsive established socket must not be reused"
    );
    assert!(connection.is_closed().await);
}

async fn mark_idle(connection: &ResponsesWebsocketConnection) {
    let guard = connection.stream.lock().await;
    let stream = guard.as_ref().unwrap();
    *stream.last_activity.lock().unwrap() = SystemTime::now() - Duration::from_secs(16);
}

#[test]
fn liveness_gate_includes_idle_boundary_and_backward_clock_changes() {
    let start = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
    assert!(!websocket_needs_probe(start, start));
    assert!(!websocket_needs_probe(
        start,
        start + Duration::from_secs(14)
    ));
    assert!(websocket_needs_probe(
        start,
        start + Duration::from_secs(15)
    ));
    assert!(websocket_needs_probe(start, start - Duration::from_secs(1)));
}

#[tokio::test]
async fn recently_active_connection_does_not_wait_for_a_probe() {
    let (connection, _server) = connection_pair().await;
    // The peer does not read. An unnecessary ping would time out and retire it.
    assert!(connection.probe_if_idle().await);
    assert!(!connection.is_closed().await);
}

#[tokio::test]
async fn matching_pong_preserves_idle_connection_and_refreshes_activity() {
    let (connection, mut server) = connection_pair().await;
    mark_idle(&connection).await;
    let (reusable, _server) = tokio::join!(connection.probe_if_idle(), async {
        let Message::Ping(payload) = server.next().await.unwrap().unwrap() else {
            panic!("expected liveness ping");
        };
        server.send(Message::Pong(payload)).await.unwrap();
        server
    });
    assert!(reusable);
    assert!(!connection.is_closed().await);
    // A second call must use the fresh activity, even though the peer stops reading.
    assert!(connection.probe_if_idle().await);
}

#[tokio::test]
async fn unrelated_pong_does_not_validate_idle_connection() {
    let (connection, mut server) = connection_pair().await;
    mark_idle(&connection).await;
    let (reusable, _server) = tokio::join!(connection.probe_if_idle(), async {
        assert!(matches!(
            server.next().await.unwrap().unwrap(),
            Message::Ping(_)
        ));
        server
            .send(Message::Pong(Bytes::from_static(b"unrelated")))
            .await
            .unwrap();
        server
    });
    assert!(!reusable);
    assert!(connection.is_closed().await);
}

#[tokio::test]
async fn canceled_probe_retires_connection() {
    let (connection, mut server) = connection_pair().await;
    mark_idle(&connection).await;
    {
        let probe = connection.probe_if_idle();
        tokio::pin!(probe);
        tokio::select! {
            message = server.next() => assert!(matches!(message.unwrap().unwrap(), Message::Ping(_))),
            _ = &mut probe => panic!("probe completed before the peer answered"),
        }
        // Drop the pending probe while the peer has received, but not answered, its ping.
    }
    assert!(connection.is_closed().await);
    assert!(!connection.probe_if_idle().await);
}

#[tokio::test]
async fn queued_response_data_survives_successful_probe() {
    let (connection, mut server) = connection_pair().await;
    server
        .send(Message::Text("queued response".into()))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if !connection
                .stream
                .lock()
                .await
                .as_ref()
                .unwrap()
                .rx_message
                .is_empty()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    mark_idle(&connection).await;
    let (reusable, _server) = tokio::join!(connection.probe_if_idle(), async {
        let Message::Ping(payload) = server.next().await.unwrap().unwrap() else {
            panic!("expected liveness ping");
        };
        server.send(Message::Pong(payload)).await.unwrap();
        server
    });
    assert!(reusable);
    let mut guard = connection.stream.lock().await;
    let message = guard
        .as_mut()
        .unwrap()
        .rx_message
        .try_recv()
        .unwrap()
        .unwrap();
    assert_eq!(message, Message::Text("queued response".into()));
}

#[tokio::test]
async fn server_ping_is_answered_while_client_waits_for_matching_pong() {
    let (connection, mut server) = connection_pair().await;
    mark_idle(&connection).await;
    let (reusable, _server) = tokio::join!(connection.probe_if_idle(), async {
        let Message::Ping(payload) = server.next().await.unwrap().unwrap() else {
            panic!("expected liveness ping");
        };
        server
            .send(Message::Ping(Bytes::from_static(b"server-ping")))
            .await
            .unwrap();
        let pong = server.next().await.unwrap().unwrap();
        assert_eq!(pong, Message::Pong(Bytes::from_static(b"server-ping")));
        server.send(Message::Pong(payload)).await.unwrap();
        server
    });
    assert!(reusable);
}

#[tokio::test]
async fn probe_deadline_includes_a_blocked_command_queue() {
    let (tx_command, _blocked_reader) = mpsc::channel(1);
    let (tx_pong, _rx_pong) = oneshot::channel();
    tx_command
        .send(WsCommand::Probe {
            payload: Bytes::new(),
            tx_pong,
        })
        .await
        .unwrap();
    let (_tx_message, rx_message) = mpsc::unbounded_channel();
    let stream = WsStream {
        tx_command,
        rx_message,
        pump_task: tokio::spawn(std::future::pending()),
        last_activity: Arc::new(StdMutex::new(SystemTime::now() - Duration::from_secs(16))),
    };
    let connection =
        ResponsesWebsocketConnection::new(stream, Duration::from_secs(300), false, None, None);
    assert!(
        !tokio::time::timeout(Duration::from_secs(3), connection.probe_if_idle())
            .await
            .unwrap()
    );
    assert!(connection.is_closed().await);
}

#[tokio::test]
async fn closed_connection_is_not_reused() {
    let (connection, server) = connection_pair().await;
    drop(server);
    tokio::time::timeout(Duration::from_secs(1), async {
        while !connection.is_closed().await {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!connection.probe_if_idle().await);
}

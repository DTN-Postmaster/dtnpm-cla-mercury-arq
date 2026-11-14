use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

use dtnpm_cla_mercury_arq::{Config, MercuryCla, PeerEntry};
use hardy_bpa::cla::{Acceptance, Cla, ClaAddress, ForwardBundleResult, Segment, Sink};
use hardy_bpv7::bundle::Id;
use hardy_bpv7::creation_timestamp::CreationTimestamp;
use hardy_bpv7::eid::NodeId;

struct MockSink {
    received_bundles: Arc<Mutex<Vec<Bytes>>>,
    active_peers: Arc<Mutex<Vec<ClaAddress>>>,
}

#[hardy_bpa::async_trait]
impl Sink for MockSink {
    async fn unregister(&self) {}

    async fn add_peer(
        &self,
        address: ClaAddress,
        _node_ids: &[NodeId],
    ) -> hardy_bpa::cla::Result<bool> {
        self.active_peers.lock().await.push(address);
        Ok(true)
    }

    async fn remove_peer(&self, address: &ClaAddress) -> hardy_bpa::cla::Result<bool> {
        self.active_peers.lock().await.retain(|a| a != address);
        Ok(true)
    }

    async fn dispatch(
        &self,
        _source_node: Option<&NodeId>,
        _cla_address: Option<&ClaAddress>,
        bundle: &mut dyn hardy_bpa::stream::Receiver<Segment>,
    ) -> hardy_bpa::cla::Result<Acceptance> {
        let mut data = Vec::new();
        while let Ok(segment) = bundle.recv().await {
            match segment {
                Segment::Next(b) => data.extend_from_slice(&b),
                Segment::Final(b) => {
                    data.extend_from_slice(&b);
                    break;
                }
            }
        }
        self.received_bundles.lock().await.push(Bytes::from(data));
        Ok(Acceptance::Accepted)
    }

    async fn transfer_outcome(
        &self,
        _bundle: &Id,
        _outcome: hardy_bpa::cla::TransferOutcome,
    ) -> hardy_bpa::cla::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn test_mock_mercury_session_lifecycle() {
    // Bind mock Mercury control and data ports
    let ctl_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let data_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let ctl_addr = ctl_listener.local_addr().unwrap();
    let data_addr = data_listener.local_addr().unwrap();

    // Spawn mock Mercury server task
    tokio::spawn(async move {
        let (ctl_sock, _) = ctl_listener.accept().await.unwrap();
        let (mut data_sock, _) = data_listener.accept().await.unwrap();

        let (ctl_r, mut ctl_w) = ctl_sock.into_split();
        let mut reader = tokio::io::BufReader::new(ctl_r);

        async fn read_ctl_line<R: tokio::io::AsyncBufRead + Unpin>(reader: &mut R) -> String {
            let mut buf = Vec::new();
            let mut byte = [0u8; 1];
            while reader.read_exact(&mut byte).await.is_ok() {
                if byte[0] == b'\r' || byte[0] == b'\n' {
                    let s = String::from_utf8_lossy(&buf).trim().to_string();
                    if !s.is_empty() {
                        return s;
                    }
                    buf.clear();
                } else {
                    buf.push(byte[0]);
                }
            }
            String::from_utf8_lossy(&buf).trim().to_string()
        }

        // Read startup commands
        let mut mycall = false;
        let mut listen = false;
        while !mycall || !listen {
            let line = read_ctl_line(&mut reader).await;
            if line.contains("MYCALL STATA") {
                mycall = true;
                ctl_w.write_all(b"OK\r").await.unwrap();
            } else if line.contains("LISTEN ON") {
                listen = true;
                ctl_w.write_all(b"OK\r").await.unwrap();
            } else if line.starts_with("BW") {
                ctl_w.write_all(b"OK\r").await.unwrap();
            }
        }

        // Wait for CONNECT STATA STATB
        let conn_cmd = read_ctl_line(&mut reader).await;
        assert!(conn_cmd.contains("CONNECT STATA STATB"), "got {conn_cmd}");
        ctl_w.write_all(b"OK\r").await.unwrap();
        ctl_w
            .write_all(b"CONNECTED STATA STATB 2300\r")
            .await
            .unwrap();

        // Read framed bundle from data socket: 4 bytes len + payload
        let mut len_buf = [0u8; 4];
        data_sock.read_exact(&mut len_buf).await.unwrap();
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut payload = vec![0u8; len];
        data_sock.read_exact(&mut payload).await.unwrap();
        assert_eq!(&payload, b"Hello from STATA bundle!");

        // Send reverse bundle from mock STATB: 4 bytes len + payload
        let reply_payload = b"Hello from STATB response bundle!";
        let mut reply_frame = Vec::new();
        reply_frame.extend_from_slice(&(reply_payload.len() as u32).to_be_bytes());
        reply_frame.extend_from_slice(reply_payload);
        data_sock.write_all(&reply_frame).await.unwrap();

        // Wait for DISCONNECT
        let disc_cmd = read_ctl_line(&mut reader).await;
        assert!(disc_cmd.contains("DISCONNECT"), "got {disc_cmd}");
        ctl_w.write_all(b"OK\r").await.unwrap();
        ctl_w.write_all(b"DISCONNECTED\r").await.unwrap();

        // Keep sockets open until client disconnects
        let mut dummy = [0u8; 1];
        let _ = reader.read(&mut dummy).await;
    });

    let config = Config {
        my_callsign: "STATA".into(),
        require_callsign: false,
        control_addr: ctl_addr,
        data_addr,
        bandwidth: 2300,
        connect_timeout: Some(Duration::from_secs(5)),
        grace_period: Some(Duration::from_secs(5)),
        idle_timeout: None,
        peers: vec![PeerEntry {
            callsign: "STATB".into(),
            node_id: Some("dtn://statb/".parse().unwrap()),
        }],
    };

    let cla = MercuryCla::new(config);
    let received_bundles = Arc::new(Mutex::new(Vec::new()));
    let active_peers = Arc::new(Mutex::new(Vec::new()));

    let sink = Box::new(MockSink {
        received_bundles: received_bundles.clone(),
        active_peers: active_peers.clone(),
    });

    cla.on_register(sink, &[], None).await;

    // Connect to peer
    tokio::time::sleep(Duration::from_millis(100)).await;
    cla.connect_peer("STATB", None)
        .await
        .expect("connect succeeds");
    assert!(cla.is_connected().await);

    // Verify peer was registered with sink
    {
        let peers = active_peers.lock().await;
        assert_eq!(peers.len(), 1);
    }

    // Forward bundle
    let bundle_data = Bytes::from_static(b"Hello from STATA bundle!");
    let bundle_len = bundle_data.len() as u64;
    let mut stream = bundle_data;
    let peer_addr = dtnpm_cla_mercury_arq::callsign_to_cla_address("STATB");
    let bundle_id = Id {
        source: "dtn://stata/".parse().unwrap(),
        timestamp: CreationTimestamp::now(),
        fragment_info: None,
    };

    let res = cla
        .forward(None, &peer_addr, &bundle_id, bundle_len, &mut stream)
        .await
        .unwrap();
    assert!(matches!(res, ForwardBundleResult::Sent));

    // Wait for reverse bundle to be received
    tokio::time::sleep(Duration::from_millis(150)).await;
    {
        let rx = received_bundles.lock().await;
        assert_eq!(rx.len(), 1);
        assert_eq!(&rx[0][..], b"Hello from STATB response bundle!");
    }

    // Disconnect
    cla.disconnect().await.expect("disconnect succeeds");
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!cla.is_connected().await);

    // Verify peer removed from sink
    {
        let peers = active_peers.lock().await;
        assert_eq!(peers.len(), 0);
    }

    cla.on_unregister().await;
}

#[tokio::test]
async fn test_forward_verifies_destination_and_max_frame_size() {
    let ctl_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ctl_addr = ctl_listener.local_addr().unwrap();
    let data_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let data_addr = data_listener.local_addr().unwrap();

    tokio::spawn(async move {
        let (ctl_sock, _) = ctl_listener.accept().await.unwrap();
        let (data_sock, _) = data_listener.accept().await.unwrap();
        let (mut ctl_r, mut ctl_w) = ctl_sock.into_split();
        let (mut data_r, _data_w) = data_sock.into_split();

        let mut buf = [0u8; 512];
        let mut cmd_buf = String::new();
        loop {
            let n = ctl_r.read(&mut buf).await.unwrap();
            if n == 0 {
                return;
            }
            cmd_buf.push_str(&String::from_utf8_lossy(&buf[..n]));
            if cmd_buf.contains("CONNECT STATA STATB") {
                cmd_buf.clear();
                ctl_w.write_all(b"CONNECTED STATA STATB\r").await.unwrap();
                break;
            }
        }
        let mut drain = [0u8; 64];
        let _ = data_r.read(&mut drain).await;
    });

    let config = Config {
        my_callsign: "STATA".into(),
        require_callsign: false,
        control_addr: ctl_addr,
        data_addr,
        bandwidth: 2300,
        connect_timeout: Some(Duration::from_secs(5)),
        grace_period: Some(Duration::from_secs(5)),
        idle_timeout: None,
        peers: Vec::new(),
    };
    let cla = MercuryCla::new(config);
    let sink = Box::new(MockSink {
        received_bundles: Arc::new(Mutex::new(Vec::new())),
        active_peers: Arc::new(Mutex::new(Vec::new())),
    });
    cla.on_register(sink, &[], None).await;
    tokio::time::sleep(Duration::from_millis(80)).await;

    cla.connect_peer("STATB", None).await.unwrap();

    // Forwarding to a mismatched callsign must return NoNeighbour
    let wrong_peer = dtnpm_cla_mercury_arq::callsign_to_cla_address("STATC");
    let bundle_id = Id {
        source: "dtn://stata/".parse().unwrap(),
        timestamp: CreationTimestamp::now(),
        fragment_info: None,
    };
    let payload = Bytes::from_static(b"wrong destination");
    let mut stream = payload.clone();
    let res = cla
        .forward(
            None,
            &wrong_peer,
            &bundle_id,
            payload.len() as u64,
            &mut stream,
        )
        .await
        .unwrap();
    assert!(matches!(res, ForwardBundleResult::NoNeighbour));

    // Forwarding larger than MAX_MERCURY_FRAME must fail with PayloadTooLarge before buffering
    let peer_b = dtnpm_cla_mercury_arq::callsign_to_cla_address("STATB");
    let mut empty = Bytes::new();
    let too_big = (dtnpm_cla_mercury_arq::MAX_MERCURY_FRAME as u64) + 1;
    let Err(err) = cla
        .forward(None, &peer_b, &bundle_id, too_big, &mut empty)
        .await
    else {
        panic!("expected PayloadTooLarge error for oversized frame");
    };
    assert!(matches!(
        err,
        hardy_bpa::cla::Error::PayloadTooLarge { size, max }
            if size == too_big && max == dtnpm_cla_mercury_arq::MAX_MERCURY_FRAME as u64
    ));

    cla.on_unregister().await;
}

#[tokio::test]
async fn test_wrong_and_connect_timeout_reset_state_and_tcp_drop_cleans_up() {
    let ctl_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ctl_addr = ctl_listener.local_addr().unwrap();
    let data_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let data_addr = data_listener.local_addr().unwrap();

    tokio::spawn(async move {
        let (ctl_sock, _) = ctl_listener.accept().await.unwrap();
        let (_data_sock, _) = data_listener.accept().await.unwrap();
        let (mut ctl_r, mut ctl_w) = ctl_sock.into_split();

        let mut buf = [0u8; 512];
        let mut cmd_buf = String::new();
        let mut connect_attempt = 0u8;
        loop {
            let n = ctl_r.read(&mut buf).await.unwrap();
            if n == 0 {
                return;
            }
            cmd_buf.push_str(&String::from_utf8_lossy(&buf[..n]));
            while let Some(pos) = cmd_buf.find('\r') {
                let line = cmd_buf[..pos].trim().to_string();
                cmd_buf.drain(..=pos);
                if line.starts_with("CONNECT ") {
                    connect_attempt += 1;
                    match connect_attempt {
                        1 => {
                            // 1st attempt: reply WRONG
                            ctl_w.write_all(b"WRONG\r").await.unwrap();
                        }
                        2 => {
                            // 2nd attempt: hang so connect_timeout fires and resets Connecting
                        }
                        _ => {
                            // 3rd attempt: succeed, then drop TCP connection shortly after
                            ctl_w.write_all(b"CONNECTED STATA STATB\r").await.unwrap();
                            tokio::time::sleep(Duration::from_millis(120)).await;
                            return;
                        }
                    }
                }
            }
        }
    });

    let config = Config {
        my_callsign: "STATA".into(),
        require_callsign: false,
        control_addr: ctl_addr,
        data_addr,
        bandwidth: 2300,
        connect_timeout: Some(Duration::from_millis(600)),
        grace_period: Some(Duration::from_secs(2)),
        idle_timeout: None,
        peers: Vec::new(),
    };

    let cla = MercuryCla::new(config);
    let active_peers = Arc::new(Mutex::new(Vec::new()));
    let sink = Box::new(MockSink {
        received_bundles: Arc::new(Mutex::new(Vec::new())),
        active_peers: active_peers.clone(),
    });
    cla.on_register(sink, &[], None).await;
    tokio::time::sleep(Duration::from_millis(80)).await;

    // 1st attempt: WRONG must reject and reset Connecting back to Disconnected
    let err1 = cla.connect_peer("STATB", None).await.unwrap_err();
    assert!(matches!(err1, dtnpm_cla_mercury_arq::Error::Control(_)));

    // 2nd attempt: hang until connect_timeout + idle tick resets Connecting back to Disconnected
    let err2 = cla.connect_peer("STATB", None).await.unwrap_err();
    assert!(matches!(err2, dtnpm_cla_mercury_arq::Error::Timeout));
    tokio::time::sleep(Duration::from_millis(550)).await;

    // 3rd attempt: not stuck in Busy; connects cleanly!
    cla.connect_peer("STATB", None).await.unwrap();
    assert!(cla.is_connected().await);
    assert_eq!(active_peers.lock().await.len(), 1);

    // Wait for TNC TCP drop -> session resets to Disconnected and active_peers is cleared
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(!cla.is_connected().await);
    assert_eq!(active_peers.lock().await.len(), 0);

    cla.on_unregister().await;
}

#[tokio::test]
async fn test_disconnect_times_out_within_five_seconds_on_stalled_tnc() {
    let ctl_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ctl_addr = ctl_listener.local_addr().unwrap();
    let data_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let data_addr = data_listener.local_addr().unwrap();

    let _server = tokio::spawn(async move {
        let (ctl_sock, _) = ctl_listener.accept().await.unwrap();
        let (_data_sock, _) = data_listener.accept().await.unwrap();
        let (mut ctl_r, mut ctl_w) = ctl_sock.into_split();

        let mut buf = [0u8; 512];
        let mut cmd_buf = String::new();
        loop {
            let n = ctl_r.read(&mut buf).await.unwrap();
            if n == 0 {
                return;
            }
            cmd_buf.push_str(&String::from_utf8_lossy(&buf[..n]));
            if cmd_buf.contains("CONNECT STATA STATB") {
                cmd_buf.clear();
                ctl_w.write_all(b"CONNECTED STATA STATB\r").await.unwrap();
                // Intentionally keep sockets open and ignore DISCONNECT forever
                while let Ok(n) = ctl_r.read(&mut buf).await {
                    if n == 0 {
                        break;
                    }
                }
                return;
            }
        }
    });

    let config = Config {
        my_callsign: "STATA".into(),
        require_callsign: false,
        control_addr: ctl_addr,
        data_addr,
        bandwidth: 2300,
        connect_timeout: Some(Duration::from_secs(2)),
        grace_period: Some(Duration::from_secs(2)),
        idle_timeout: None,
        peers: Vec::new(),
    };

    let cla = MercuryCla::new(config);
    let sink = Box::new(MockSink {
        received_bundles: Arc::new(Mutex::new(Vec::new())),
        active_peers: Arc::new(Mutex::new(Vec::new())),
    });
    cla.on_register(sink, &[], None).await;
    tokio::time::sleep(Duration::from_millis(80)).await;
    cla.connect_peer("STATB", None).await.unwrap();

    let start = tokio::time::Instant::now();
    let err = cla.disconnect().await.unwrap_err();
    let elapsed = start.elapsed();
    assert!(matches!(err, dtnpm_cla_mercury_arq::Error::Timeout));
    assert!(elapsed >= Duration::from_secs(4) && elapsed < Duration::from_secs(7));

    cla.on_unregister().await;
}

//! Mercury TNC ARQ driver managing TCP control and data sockets.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Instant, sleep};
use tracing::{debug, error, info, trace, warn};

use hardy_async::TaskPool;
use hardy_bpa::cla::{Acceptance, ClaAddress, ForwardBundleResult, Sink};
use hardy_bpv7::eid::NodeId;

use crate::config::{Config, PeerEntry, is_valid_callsign, normalize_callsign};
use crate::error::{Error, Result};

/// Maximum bundle frame length permitted over the Mercury ARQ data socket (16 MiB).
pub const MAX_MERCURY_FRAME: usize = 16 * 1024 * 1024;

/// Maximum number of dynamic peer callsign entries retained in memory.
const MAX_PEERS: usize = 256;

const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const DISCONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Converts a callsign into a canonical normalized Mercury [`ClaAddress`].
#[must_use]
pub fn callsign_to_cla_address(callsign: &str) -> ClaAddress {
    ClaAddress::Private(Bytes::from(normalize_callsign(callsign).into_bytes()))
}

/// Extracts a normalized callsign string from a Mercury [`ClaAddress`], if applicable.
#[must_use]
pub fn cla_address_to_callsign(addr: &ClaAddress) -> Option<String> {
    match addr {
        ClaAddress::Private(bytes) => std::str::from_utf8(bytes)
            .ok()
            .map(normalize_callsign)
            .filter(|s| !s.is_empty()),
        _ => None,
    }
}

/// Commands sent to the background driver task.
pub enum DriverCommand {
    /// Initiate an ARQ connection to the remote callsign.
    Connect {
        callsign: String,
        node_id: NodeId,
        reply: oneshot::Sender<Result<()>>,
    },
    /// Disconnect any active ARQ session.
    Disconnect { reply: oneshot::Sender<Result<()>> },
    /// Forward a bundle across the active data socket to a specific callsign.
    ForwardBundle {
        target_callsign: String,
        data: Bytes,
        reply: oneshot::Sender<Result<ForwardBundleResult>>,
    },
    /// Register a known peer mapping.
    RegisterPeer { peer: PeerEntry },
    /// Query if the CLA currently has an active ARQ connection.
    IsConnected { reply: oneshot::Sender<bool> },
    /// Terminate the driver.
    Shutdown,
}

#[derive(Debug, PartialEq, Eq)]
enum SessionState {
    Disconnected,
    Connecting {
        peer_callsign: String,
        peer_node_id: NodeId,
        entered_at: Instant,
    },
    Connected {
        peer_callsign: String,
        peer_node_id: NodeId,
        connected_at: Instant,
        last_activity: Instant,
    },
    Disconnecting {
        entered_at: Instant,
    },
}

pub struct Driver {
    config: Config,
    sink: Arc<dyn Sink>,
    tasks: Arc<TaskPool>,
    cmd_rx: mpsc::Receiver<DriverCommand>,
    peers: HashMap<String, NodeId>,
}

impl Driver {
    pub fn spawn(
        mut config: Config,
        sink: Arc<dyn Sink>,
        tasks: Arc<TaskPool>,
        cmd_rx: mpsc::Receiver<DriverCommand>,
    ) -> tokio::task::JoinHandle<()> {
        if let Err(e) = config.validate_callsign() {
            error!("Mercury CLA callsign validation failed: {e}");
            return tokio::spawn(async {});
        }
        config.my_callsign = normalize_callsign(&config.my_callsign);

        let mut peers = HashMap::new();
        for p in &config.peers {
            let norm = normalize_callsign(&p.callsign);
            if !is_valid_callsign(&norm) {
                warn!(
                    "ignoring invalid preconfigured Mercury peer callsign {:?}",
                    p.callsign
                );
                continue;
            }
            if peers.len() >= MAX_PEERS && !peers.contains_key(&norm) {
                warn!(
                    "Mercury peer table reached cap of {MAX_PEERS}; ignoring extra configured peer {norm}"
                );
                break;
            }
            peers.insert(norm, p.effective_node_id());
        }
        let driver = Self {
            config,
            sink,
            tasks,
            cmd_rx,
            peers,
        };
        tokio::spawn(driver.run())
    }

    fn cache_peer(&mut self, callsign: &str, node_id: Option<NodeId>) -> Option<NodeId> {
        let norm = normalize_callsign(callsign);
        if !is_valid_callsign(&norm) {
            warn!("rejecting invalid Mercury peer callsign {:?}", callsign);
            return None;
        }
        if let Some(nid) = node_id {
            if self.peers.contains_key(&norm) || self.peers.len() < MAX_PEERS {
                self.peers.insert(norm, nid.clone());
                return Some(nid);
            }
            warn!("Mercury peer table reached cap of {MAX_PEERS}; ignoring new peer {norm}");
            return None;
        }
        if let Some(existing) = self.peers.get(&norm) {
            return Some(existing.clone());
        }
        if self.peers.len() >= MAX_PEERS {
            warn!("Mercury peer table reached cap of {MAX_PEERS}; rejecting uncached peer {norm}");
            return None;
        }
        let lower = norm.to_lowercase();
        let derived = format!("dtn://{lower}/").parse().unwrap_or_else(|_| {
            NodeId::Dtn(hardy_bpv7::eid::DtnNodeId {
                node_name: lower.into_boxed_str(),
            })
        });
        self.peers.insert(norm, derived.clone());
        Some(derived)
    }

    async fn remove_active_session_peer(&self, session: &SessionState) {
        if let SessionState::Connected { peer_callsign, .. }
        | SessionState::Connecting { peer_callsign, .. } = session
        {
            let cla_addr = callsign_to_cla_address(peer_callsign);
            let _ = self.sink.remove_peer(&cla_addr).await;
        }
    }

    async fn run(mut self) {
        info!(
            "starting Mercury ARQ CLA driver (mycall={}, ctl={}, data={})",
            self.config.my_callsign, self.config.control_addr, self.config.data_addr
        );

        let mut connect_retry_delay = Duration::from_millis(500);

        'outer: loop {
            // Connect to Mercury control and data ports
            let (mut ctl_stream, data_stream) = match self.connect_mercury_ports().await {
                Ok(streams) => {
                    connect_retry_delay = Duration::from_millis(500);
                    streams
                }
                Err(err) => {
                    warn!(
                        "failed to connect to Mercury TNC: {err}. Retrying in {:?}",
                        connect_retry_delay
                    );
                    tokio::select! {
                        _ = sleep(connect_retry_delay) => {
                            connect_retry_delay = (connect_retry_delay * 2).min(Duration::from_secs(5));
                            continue 'outer;
                        }
                        cmd = self.cmd_rx.recv() => {
                            match cmd {
                                Some(DriverCommand::Shutdown) | None => break 'outer,
                                Some(DriverCommand::Connect { reply, .. }) => {
                                    let _ = reply.send(Err(Error::Control("Mercury TNC offline".into())));
                                }
                                Some(DriverCommand::Disconnect { reply }) => {
                                    let _ = reply.send(Ok(()));
                                }
                                Some(DriverCommand::ForwardBundle { reply, .. }) => {
                                    let _ = reply.send(Err(Error::Control("Mercury TNC offline".into())));
                                }
                                Some(DriverCommand::IsConnected { reply }) => {
                                    let _ = reply.send(false);
                                }
                                Some(DriverCommand::RegisterPeer { peer }) => {
                                    let _ = self.cache_peer(&peer.callsign, Some(peer.effective_node_id()));
                                }
                            }
                            continue 'outer;
                        }
                    }
                }
            };

            // Initialize Mercury TNC mode
            if let Err(err) = self.init_mercury_tnc(&mut ctl_stream).await {
                warn!(
                    "failed to initialize Mercury TNC via control port: {err}. Retrying in {:?}",
                    connect_retry_delay
                );
                tokio::select! {
                    _ = sleep(connect_retry_delay) => {
                        connect_retry_delay = (connect_retry_delay * 2).min(Duration::from_secs(5));
                        continue 'outer;
                    }
                    cmd = self.cmd_rx.recv() => {
                        match cmd {
                            Some(DriverCommand::Shutdown) | None => break 'outer,
                            Some(DriverCommand::Connect { reply, .. }) => {
                                let _ = reply.send(Err(Error::Control("Mercury TNC init failed".into())));
                            }
                            Some(DriverCommand::Disconnect { reply }) => {
                                let _ = reply.send(Ok(()));
                            }
                            Some(DriverCommand::ForwardBundle { reply, .. }) => {
                                let _ = reply.send(Err(Error::Control("Mercury TNC init failed".into())));
                            }
                            Some(DriverCommand::IsConnected { reply }) => {
                                let _ = reply.send(false);
                            }
                            Some(DriverCommand::RegisterPeer { peer }) => {
                                let _ = self.cache_peer(&peer.callsign, Some(peer.effective_node_id()));
                            }
                        }
                        continue 'outer;
                    }
                }
            }

            info!("connected and initialized Mercury TNC control and data interfaces");

            let (ctl_reader, mut ctl_writer) = ctl_stream.into_split();
            let (data_reader, mut data_writer) = data_stream.into_split();

            let mut session = SessionState::Disconnected;
            let mut pending_connect_reply: Option<oneshot::Sender<Result<()>>> = None;
            let mut pending_disconnect_reply: Option<oneshot::Sender<Result<()>>> = None;

            let (data_rx_tx, mut data_rx_rx) = mpsc::channel::<Bytes>(64);
            let (data_tx_tx, mut data_tx_rx) =
                mpsc::channel::<(Bytes, oneshot::Sender<Result<ForwardBundleResult>>)>(64);
            let (ctl_line_tx, mut ctl_line_rx) = mpsc::channel::<String>(64);

            // Spawn background task to read CRLF lines from control socket
            let ctl_reader_task = tokio::spawn(Self::control_read_loop(ctl_reader, ctl_line_tx));
            // Spawn background task to read length-prefixed bundles from data socket
            let data_reader_task = tokio::spawn(Self::data_read_loop(data_reader, data_rx_tx));
            // Spawn background task to write framed bundles to data socket
            let data_writer_task = tokio::spawn(async move {
                while let Some((bundle, reply)) = data_tx_rx.recv().await {
                    let len = bundle.len() as u32;
                    let mut frame = BytesMut::with_capacity(4 + bundle.len());
                    frame.extend_from_slice(&len.to_be_bytes());
                    frame.extend_from_slice(&bundle);
                    match data_writer.write_all(&frame).await {
                        Ok(()) => {
                            let _ = reply.send(Ok(ForwardBundleResult::Sent));
                        }
                        Err(e) => {
                            let _ = reply.send(Err(Error::Io(e)));
                            break;
                        }
                    }
                }
            });

            let mut idle_interval = tokio::time::interval(Duration::from_millis(500));

            'session_loop: loop {
                tokio::select! {
                    _ = idle_interval.tick() => {
                        match &session {
                            SessionState::Connected { peer_callsign, last_activity, .. }
                                if self.config.idle_timeout.is_some_and(|idle_to| last_activity.elapsed() >= idle_to) =>
                            {
                                info!("idle timeout reached for peer {peer_callsign}, issuing disconnect");
                                self.remove_active_session_peer(&session).await;
                                let _ = ctl_writer.write_all(b"DISCONNECT\r").await;
                                session = SessionState::Disconnecting { entered_at: Instant::now() };
                            }
                            SessionState::Connecting { peer_callsign, entered_at, .. } => {
                                let connect_to = self.config.connect_timeout.unwrap_or(DEFAULT_CONNECT_TIMEOUT);
                                if entered_at.elapsed() >= connect_to {
                                    warn!("Mercury connect handshake to {peer_callsign} timed out after {connect_to:?}; resetting to Disconnected");
                                    let _ = ctl_writer.write_all(b"DISCONNECT\r").await;
                                    session = SessionState::Disconnected;
                                    if let Some(reply) = pending_connect_reply.take() {
                                        let _ = reply.send(Err(Error::Timeout));
                                    }
                                }
                            }
                            SessionState::Disconnecting { entered_at }
                                if entered_at.elapsed() >= DISCONNECT_TIMEOUT =>
                            {
                                warn!("Mercury disconnect handshake timed out after {DISCONNECT_TIMEOUT:?}; forcing Disconnected");
                                session = SessionState::Disconnected;
                                if let Some(reply) = pending_disconnect_reply.take() {
                                    let _ = reply.send(Err(Error::Timeout));
                                }
                            }
                            _ => {}
                        }
                    }

                    // Line received from Mercury control socket
                    line = ctl_line_rx.recv() => {
                        let Some(line) = line else {
                            warn!("Mercury control connection closed");
                            break 'session_loop;
                        };
                        trace!("Mercury CTL << {line}");
                        self.handle_ctl_line(
                            &line,
                            &mut session,
                            &mut pending_connect_reply,
                            &mut pending_disconnect_reply,
                        ).await;
                    }

                    // Inbound bundle read from data socket
                    bundle = data_rx_rx.recv() => {
                        let Some(mut bundle) = bundle else {
                            warn!("Mercury data connection closed");
                            break 'session_loop;
                        };

                        if let SessionState::Connected { ref peer_callsign, ref peer_node_id, ref mut last_activity, .. } = session {
                            *last_activity = Instant::now();
                            let peer_addr = callsign_to_cla_address(peer_callsign);
                            let node_id = peer_node_id.clone();
                            let sink = self.sink.clone();
                            self.tasks.spawn(async move {
                                debug!("delivering received bundle ({} bytes) from peer {node_id}", bundle.len());
                                match sink.dispatch(Some(&node_id), Some(&peer_addr), &mut bundle).await {
                                    Ok(Acceptance::Accepted) => {}
                                    Ok(Acceptance::Refused) => {
                                        warn!("bundle from {node_id} refused by BPA");
                                    }
                                    Err(err) => {
                                        error!("dispatch to BPA failed: {err}");
                                    }
                                }
                            });
                        } else {
                            warn!("received data packet while session not connected; dropping");
                        }
                    }

                    // Command from BPA / Coordinator
                    cmd = self.cmd_rx.recv() => {
                        let Some(cmd) = cmd else {
                            break 'outer;
                        };

                        match cmd {
                            DriverCommand::Connect { callsign, node_id, reply } => {
                                let upper_call = normalize_callsign(&callsign);
                                let mycall = normalize_callsign(&self.config.my_callsign);
                                if !is_valid_callsign(&upper_call) || !is_valid_callsign(&mycall) {
                                    let _ = reply.send(Err(Error::InvalidCallsign(format!(
                                        "Invalid callsign format (my={}, remote={})",
                                        mycall, upper_call
                                    ))));
                                    continue;
                                }
                                match session {
                                    SessionState::Disconnected => {
                                        info!("dialing Mercury ARQ: CONNECT {} {}", mycall, upper_call);
                                        let connect_cmd = format!("CONNECT {mycall} {upper_call}\r");
                                        if let Err(err) = ctl_writer.write_all(connect_cmd.as_bytes()).await {
                                            let _ = reply.send(Err(Error::Io(err)));
                                        } else {
                                            let _ = self.cache_peer(&upper_call, Some(node_id.clone()));
                                            session = SessionState::Connecting {
                                                peer_callsign: upper_call,
                                                peer_node_id: node_id,
                                                entered_at: Instant::now(),
                                            };
                                            pending_connect_reply = Some(reply);
                                        }
                                    }
                                    SessionState::Connected { ref peer_callsign, .. } if peer_callsign == &upper_call => {
                                        let _ = reply.send(Ok(()));
                                    }
                                    _ => {
                                        let _ = reply.send(Err(Error::Busy));
                                    }
                                }
                            }

                            DriverCommand::Disconnect { reply } => {
                                match session {
                                    SessionState::Connected { .. } | SessionState::Connecting { .. } => {
                                        self.remove_active_session_peer(&session).await;
                                        if let Err(err) = ctl_writer.write_all(b"DISCONNECT\r").await {
                                            session = SessionState::Disconnected;
                                            let _ = reply.send(Err(Error::Io(err)));
                                        } else {
                                            session = SessionState::Disconnecting { entered_at: Instant::now() };
                                            pending_disconnect_reply = Some(reply);
                                        }
                                    }
                                    _ => {
                                        let _ = reply.send(Ok(()));
                                    }
                                }
                            }

                            DriverCommand::ForwardBundle { target_callsign, data, reply } => {
                                if data.len() > MAX_MERCURY_FRAME {
                                    let _ = reply.send(Err(Error::PayloadTooLarge {
                                        size: data.len() as u64,
                                        max: MAX_MERCURY_FRAME as u64,
                                    }));
                                    continue;
                                }
                                match session {
                                    SessionState::Connected { ref peer_callsign, ref mut last_activity, .. } => {
                                        let target = normalize_callsign(&target_callsign);
                                        if target != *peer_callsign {
                                            warn!(
                                                "dropping bundle addressed to {target}: active Mercury ARQ session is with {peer_callsign}"
                                            );
                                            let _ = reply.send(Ok(ForwardBundleResult::NoNeighbour));
                                            continue;
                                        }
                                        *last_activity = Instant::now();
                                        if data_tx_tx.send((data, reply)).await.is_err() {
                                            break 'session_loop;
                                        }
                                    }

                                    _ => {
                                        let _ = reply.send(Err(Error::ConnectionFailed));
                                    }
                                }
                            }

                            DriverCommand::IsConnected { reply } => {
                                let is_conn = matches!(session, SessionState::Connected { .. });
                                let _ = reply.send(is_conn);
                            }

                            DriverCommand::RegisterPeer { peer } => {
                                let _ = self.cache_peer(&peer.callsign, Some(peer.effective_node_id()));
                            }

                            DriverCommand::Shutdown => {
                                self.remove_active_session_peer(&session).await;
                                if matches!(session, SessionState::Connected { .. } | SessionState::Connecting { .. }) {
                                    let _ = ctl_writer.write_all(b"DISCONNECT\r").await;
                                }
                                ctl_reader_task.abort();
                                data_reader_task.abort();
                                data_writer_task.abort();
                                break 'outer;
                            }
                        }
                    }
                }
            }

            // Reset session state and withdraw any BPA peer registration before reconnecting
            self.remove_active_session_peer(&session).await;
            session = SessionState::Disconnected;
            let _ = &session;
            if let Some(reply) = pending_connect_reply.take() {
                let _ = reply.send(Err(Error::ConnectionFailed));
            }
            if let Some(reply) = pending_disconnect_reply.take() {
                let _ = reply.send(Ok(()));
            }

            ctl_reader_task.abort();
            data_reader_task.abort();
            data_writer_task.abort();
        }

        info!("Mercury ARQ CLA driver shutdown");
    }

    async fn connect_mercury_ports(&self) -> Result<(TcpStream, TcpStream)> {
        let ctl = TcpStream::connect(self.config.control_addr).await?;
        let data = TcpStream::connect(self.config.data_addr).await?;
        Ok((ctl, data))
    }

    async fn init_mercury_tnc(&self, ctl: &mut TcpStream) -> Result<()> {
        let mycall = normalize_callsign(&self.config.my_callsign);
        if !is_valid_callsign(&mycall) {
            return Err(Error::InvalidCallsign(format!(
                "Invalid configured my_callsign: {mycall}"
            )));
        }
        let setup = format!(
            "MYCALL {}\rBW{}\rLISTEN ON\r",
            mycall, self.config.bandwidth
        );
        ctl.write_all(setup.as_bytes()).await?;
        ctl.flush().await?;
        Ok(())
    }

    async fn control_read_loop(
        mut reader: tokio::net::tcp::OwnedReadHalf,
        tx: mpsc::Sender<String>,
    ) {
        let mut buf = Vec::with_capacity(1024);
        let mut temp = [0u8; 256];
        const MAX_CTL_BUF: usize = 4096;

        loop {
            match reader.read(&mut temp).await {
                Ok(0) => break,
                Ok(n) => {
                    buf.extend_from_slice(&temp[..n]);
                    while let Some(pos) = buf.iter().position(|&b| b == b'\r' || b == b'\n') {
                        let line_bytes: Vec<u8> = buf.drain(..=pos).collect();
                        let line = String::from_utf8_lossy(&line_bytes).trim().to_string();
                        if !line.is_empty() && tx.send(line).await.is_err() {
                            return;
                        }
                    }
                    if buf.len() > MAX_CTL_BUF {
                        warn!(
                            "Mercury TNC control buffer exceeded {} bytes without newline, dropping",
                            MAX_CTL_BUF
                        );
                        buf.clear();
                    }
                }
                Err(_) => break,
            }
        }
    }

    async fn data_read_loop(mut reader: tokio::net::tcp::OwnedReadHalf, tx: mpsc::Sender<Bytes>) {
        let mut len_buf = [0u8; 4];

        loop {
            if reader.read_exact(&mut len_buf).await.is_err() {
                break;
            }
            let length = u32::from_be_bytes(len_buf) as usize;
            if length == 0 {
                continue;
            }
            if length > MAX_MERCURY_FRAME {
                warn!(
                    "Mercury TNC frame size {} exceeds maximum {}; aborting connection",
                    length, MAX_MERCURY_FRAME
                );
                break;
            }

            let mut payload = vec![0u8; length];
            if reader.read_exact(&mut payload).await.is_err() {
                break;
            }

            if tx.send(Bytes::from(payload)).await.is_err() {
                break;
            }
        }
    }

    async fn handle_ctl_line(
        &mut self,
        line: &str,
        session: &mut SessionState,
        pending_connect_reply: &mut Option<oneshot::Sender<Result<()>>>,
        pending_disconnect_reply: &mut Option<oneshot::Sender<Result<()>>>,
    ) {
        if line.starts_with("CONNECTED") {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() < 3 {
                warn!("ignoring malformed CONNECTED control frame: {line}");
                return;
            }
            let src = normalize_callsign(parts[1]);
            let dst = normalize_callsign(parts[2]);
            if !is_valid_callsign(&src) || !is_valid_callsign(&dst) {
                warn!("ignoring CONNECTED control frame with invalid callsign: {line}");
                return;
            }

            // Determine peer callsign
            let peer = if src == normalize_callsign(&self.config.my_callsign) {
                dst
            } else {
                src
            };

            let Some(peer_node) = self.cache_peer(&peer, None) else {
                warn!("ignoring CONNECTED frame for {peer}: callsign rejected or peer table full");
                return;
            };

            info!("Mercury ARQ CONNECTED with peer {peer} ({peer_node})");

            let cla_addr = callsign_to_cla_address(&peer);
            let _ = self
                .sink
                .add_peer(cla_addr, std::slice::from_ref(&peer_node))
                .await;

            *session = SessionState::Connected {
                peer_callsign: peer,
                peer_node_id: peer_node,
                connected_at: Instant::now(),
                last_activity: Instant::now(),
            };

            if let Some(reply) = pending_connect_reply.take() {
                let _ = reply.send(Ok(()));
            }
        } else if line == "DISCONNECTED" {
            info!("Mercury ARQ DISCONNECTED");
            self.remove_active_session_peer(session).await;
            *session = SessionState::Disconnected;

            if let Some(reply) = pending_connect_reply.take() {
                let _ = reply.send(Err(Error::ConnectionFailed));
            }
            if let Some(reply) = pending_disconnect_reply.take() {
                let _ = reply.send(Ok(()));
            }
        } else if line == "WRONG" {
            warn!("Mercury TNC returned WRONG while in state {:?}", *session);
            self.remove_active_session_peer(session).await;
            *session = SessionState::Disconnected;
            if let Some(reply) = pending_connect_reply.take() {
                let _ = reply.send(Err(Error::Control("Mercury rejected command".into())));
            }
            if let Some(reply) = pending_disconnect_reply.take() {
                let _ = reply.send(Err(Error::Control(
                    "Mercury rejected disconnect command".into(),
                )));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hardy_bpa::async_trait;
    use hardy_bpa::cla::{Result as ClaResult, Segment, Sink, TransferOutcome};
    use hardy_bpa::stream::Receiver;
    use hardy_bpv7::bundle::Id;

    struct NoopSink;

    #[async_trait]
    impl Sink for NoopSink {
        async fn unregister(&self) {}

        async fn dispatch(
            &self,
            _peer: Option<&NodeId>,
            _peer_addr: Option<&ClaAddress>,
            _bundle: &mut dyn Receiver<Segment>,
        ) -> ClaResult<Acceptance> {
            Ok(Acceptance::Accepted)
        }

        async fn add_peer(&self, _cla_addr: ClaAddress, _node_ids: &[NodeId]) -> ClaResult<bool> {
            Ok(true)
        }

        async fn remove_peer(&self, _cla_addr: &ClaAddress) -> ClaResult<bool> {
            Ok(true)
        }

        async fn transfer_outcome(&self, _bundle: &Id, _outcome: TransferOutcome) -> ClaResult<()> {
            Ok(())
        }
    }

    fn make_test_driver() -> Driver {
        let (_tx, rx) = mpsc::channel(8);
        let config = Config {
            my_callsign: "F4JXQ".into(),
            ..Default::default()
        };
        Driver {
            config,
            sink: Arc::new(NoopSink),
            tasks: Arc::new(TaskPool::new()),
            cmd_rx: rx,
            peers: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn test_wrong_frame_resets_connecting_and_disconnecting_to_disconnected() {
        let mut driver = make_test_driver();
        let node: NodeId = "dtn://on4kst/".parse().unwrap();

        let (conn_tx, conn_rx) = oneshot::channel();
        let mut pending_connect = Some(conn_tx);
        let mut pending_disconnect = None;
        let mut session = SessionState::Connecting {
            peer_callsign: "ON4KST".into(),
            peer_node_id: node,
            entered_at: Instant::now(),
        };

        driver
            .handle_ctl_line(
                "WRONG",
                &mut session,
                &mut pending_connect,
                &mut pending_disconnect,
            )
            .await;

        assert_eq!(session, SessionState::Disconnected);
        assert!(matches!(conn_rx.await.unwrap(), Err(Error::Control(_))));

        // Also resets Disconnecting state when WRONG arrives without pending connect reply
        let (disc_tx, disc_rx) = oneshot::channel();
        pending_disconnect = Some(disc_tx);
        session = SessionState::Disconnecting {
            entered_at: Instant::now(),
        };
        driver
            .handle_ctl_line(
                "WRONG",
                &mut session,
                &mut pending_connect,
                &mut pending_disconnect,
            )
            .await;
        assert_eq!(session, SessionState::Disconnected);
        assert!(matches!(disc_rx.await.unwrap(), Err(Error::Control(_))));
    }

    #[tokio::test]
    async fn test_connected_validates_callsigns_and_caps_peers_at_256() {
        let mut driver = make_test_driver();
        let mut session = SessionState::Disconnected;
        let mut pending_conn = None;
        let mut pending_disc = None;

        // Reject invalid callsigns from forged/corrupted CONNECTED lines
        for bad_line in [
            "CONNECTED",
            "CONNECTED F4JXQ",
            "CONNECTED F4JXQ AB",
            "CONNECTED F4JXQ 123456",
            "CONNECTED F4JXQ BAD-99",
            "CONNECTED INVALID! F4JXQ",
        ] {
            driver
                .handle_ctl_line(bad_line, &mut session, &mut pending_conn, &mut pending_disc)
                .await;
            assert_eq!(session, SessionState::Disconnected);
            assert!(driver.peers.is_empty());
        }

        // Insert 300 distinct valid remote stations -> peer cache must cap strictly at 256
        for i in 0..300usize {
            let call = format!("ST{:04X}", i);
            let line = format!("CONNECTED F4JXQ {call}");
            driver
                .handle_ctl_line(&line, &mut session, &mut pending_conn, &mut pending_disc)
                .await;
        }
        assert_eq!(driver.peers.len(), MAX_PEERS);
        assert_eq!(MAX_PEERS, 256);
    }

    #[test]
    fn test_callsign_address_roundtrip_normalizes_whitespace_and_case() {
        let addr = callsign_to_cla_address("  on4kst-1 ");
        assert_eq!(cla_address_to_callsign(&addr).as_deref(), Some("ON4KST-1"));
    }
}

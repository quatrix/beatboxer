use anyhow::Result;
use kanal::SendError;
use postcard::from_bytes;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::async_trait;

use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::RwLock,
    time::timeout,
};
use tracing::{debug, error, info};

use crate::storage::Storage;

const SOCKET_WRITE_TIMEOUT: Duration = Duration::from_millis(1000);
const SOCKET_WRITE_LONG_TIMEOUT: Duration = Duration::from_millis(10000);
const SOCKET_READ_TIMEOUT: Duration = Duration::from_millis(1000);
const SOCKET_READ_LONG_TIMEOUT: Duration = Duration::from_millis(10000);

#[derive(Clone, Debug)]
struct KeepAliveUpdate {
    id: String,
    ts: i64,
}

enum Message {
    Ping,
    KeepAliveUpdate(KeepAliveUpdate),
}

pub struct KeepAlive {
    server_addr: String,
    nodes: Vec<String>,
    keep_alives: Arc<dyn Storage + Send + Sync>,
    txs: Arc<RwLock<Vec<(String, kanal::AsyncSender<Message>)>>>,
}

#[async_trait]
pub trait KeepAliveTrait {
    async fn pulse(&self, id: &str);
    async fn get(&self, id: &str) -> Option<i64>;
}

impl KeepAlive {
    pub fn new(
        server_addr: String,
        nodes: Vec<String>,
        storage: Arc<dyn Storage + Send + Sync>,
    ) -> KeepAlive {
        KeepAlive {
            server_addr,
            nodes,
            keep_alives: storage,
            txs: Arc::new(RwLock::new(Vec::new())),
        }
    }

    fn health_checker(&self) {
        info!("starting health_checker");

        let txsc = Arc::clone(&self.txs);

        tokio::spawn(async move {
            loop {
                let mut gc_senders = false;
                {
                    let txs = txsc.read().await;

                    for (addr, sender) in txs.iter() {
                        metrics::gauge!("channel_pressure", sender.len() as f64, "addr" => addr.to_string());

                        if sender.is_closed() || sender.is_disconnected() {
                            gc_senders = true;
                        } else {
                            match sender.try_send(Message::Ping) {
                                Ok(sent) => {
                                    if sent {
                                        metrics::increment_counter!("channel_sends_success_total", "type" => "ping", "addr" => addr.to_string());
                                    } else {
                                        error!("failed sending ping to channel, to addr: {}", addr);
                                        metrics::increment_counter!("channel_sends_errors_total", "type" => "ping", "addr" => addr.to_string());
                                    }
                                }
                                Err(SendError::Closed) => error!("socket closed, addr: {}", addr),
                                Err(SendError::ReceiveClosed) => {
                                    error!("receive closed, addr: {}", addr);
                                    sender.close();
                                }
                            };
                        }
                    }
                }

                if gc_senders {
                    info!("cleaninig dead senders");
                    let mut txs = txsc.write().await;
                    txs.retain(|(_, tx)| !tx.is_closed() && !tx.is_disconnected());
                }

                // FIXME: make keep alive interval configurable
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }

    pub async fn listen(&self) -> Result<()> {
        let listener = TcpListener::bind(&self.server_addr).await?;
        info!("Listening on: {}", self.server_addr);

        self.health_checker();

        loop {
            let (mut socket, addr) = listener.accept().await?;
            info!("connected client: {:?} addr: {}", socket, addr);

            let (tx, rx) = kanal::bounded_async(102400);
            let mut txs = self.txs.write().await;
            let kac = Arc::clone(&self.keep_alives);

            txs.push((addr.to_string(), tx));

            tokio::spawn(async move {
                let mut buf = vec![0; 1024];
                let mut already_synched = false;
                let mut last_pong = Instant::now();

                loop {
                    // first wait for a SYNC command
                    // to send a full sync of the current state
                    if !already_synched {
                        let n = match socket.read(&mut buf).await {
                            Ok(0) => {
                                info!("socket closed {:?}", socket);
                                break;
                            }
                            Ok(n) => n,
                            Err(e) => {
                                error!("Error reading from socket: {}", e);
                                break;
                            }
                        };

                        if n == 5 && buf[0..n] == *b"SYNC\n" {
                            let ka = kac.serialize().await.unwrap();
                            let ka_len = format!("{}\n", ka.len());
                            info!("Sending SYNC: {}", ka.len());
                            if let Err(e) =
                                timeout(SOCKET_WRITE_TIMEOUT, socket.write_all(ka_len.as_bytes()))
                                    .await
                            {
                                error!("Error writing SYNC to socket: {}", e);
                                break;
                            }

                            match timeout(SOCKET_WRITE_LONG_TIMEOUT, socket.write_all(&ka)).await {
                                Ok(_) => {
                                    already_synched = true;
                                }
                                Err(e) => {
                                    error!("Error writing STATE to socket: {}", e);
                                    break;
                                }
                            }
                        } else {
                            error!("Invalid command: {:?}", &buf[0..n]);
                            continue;
                        }
                    }

                    tokio::select! {
                        Ok(n) = socket.read(&mut buf) => {
                            if n == 0 {
                                info!("zero size read from socket, client closed socket");
                                break;
                            } else {
                                // FIXME: for now we're not parsing the message
                                // because it can only be PONG.
                                last_pong = Instant::now();
                            }
                        },

                        v = rx.recv() => match v {
                            Ok(message) => match message {
                                Message::Ping => {
                                    debug!("Sending Ping");

                                    // FIXME: make pong threshold configurable
                                    if last_pong.elapsed().as_secs() > 3 {
                                        error!("didn't see pong for a while, node is probably dead, closing socket: {:?}", socket);
                                        break;
                                    }

                                    match timeout(SOCKET_WRITE_TIMEOUT, socket.write_all("PING\n".as_bytes())).await {
                                        Ok(_) => {
                                            debug!("Sent to node")
                                        }
                                        Err(e) => {
                                            error!("Error writing PING to socket: {}", e);
                                            break;
                                        }
                                    }
                                }
                                Message::KeepAliveUpdate(ka) => {
                                    let line = format!("KA {} {}\n", ka.id, ka.ts);
                                    debug!("Sending: {} - to addr {}", line, addr);
                                    match timeout(SOCKET_WRITE_TIMEOUT, socket.write_all(line.as_bytes())).await {
                                        Ok(_) => {
                                            debug!("Sent to node")
                                        }
                                        Err(e) => {
                                            error!("Error writing KA to socket: {}", e);
                                            break;
                                        }
                                    }
                                }
                            },
                            Err(e) => {
                                error!("Error receiving from channel: {}", e);
                                break;
                            }
                        }
                    }
                }

                info!("Client disconnected: {:?}", socket);
            });
        }
    }

    pub fn connect_to_nodes(&self) {
        for node in self.nodes.clone() {
            let kac = Arc::clone(&self.keep_alives);
            tokio::spawn(async move {
                loop {
                    info!("Connecting to {}", node);

                    match TcpStream::connect(&node).await {
                        Ok(socket) => {
                            let mut socket = BufReader::new(socket);

                            let addr = match socket.get_ref().peer_addr() {
                                Ok(ip) => ip,
                                Err(e) => {
                                    error!("Error getting peer addr: {}", e);
                                    continue;
                                }
                            };

                            // send a SYNC command
                            match timeout(SOCKET_WRITE_TIMEOUT, socket.write_all(b"SYNC\n")).await {
                                Ok(_) => {}
                                Err(e) => {
                                    error!("Error writing SYNC to socket: {} (addr: {})", e, addr);
                                    continue;
                                }
                            }

                            let mut buf = String::new();

                            if let Err(e) =
                                timeout(SOCKET_READ_TIMEOUT, socket.read_line(&mut buf)).await
                            {
                                error!(
                                    "error reading ka_len line from socket {:?} (addr: {})",
                                    e, addr
                                );
                                continue;
                            }

                            let ka_len = match buf.trim().parse::<usize>() {
                                Ok(ka_len) => ka_len,
                                Err(e) => {
                                    error!("Invalid ka_len: {} error: {} (addr: {})", buf, e, addr);
                                    continue;
                                }
                            };

                            let mut ka = vec![0; ka_len];
                            if let Err(e) =
                                timeout(SOCKET_READ_LONG_TIMEOUT, socket.read_exact(&mut ka)).await
                            {
                                error!("failed getting STATE: {:?} (addr: {})", e, addr);
                                continue;
                            }

                            let ka = match from_bytes::<HashMap<String, i64>>(&ka) {
                                Ok(ka) => ka,
                                Err(e) => {
                                    error!("Invalid ka: {} error: {} (addr: {})", buf, e, addr);
                                    continue;
                                }
                            };

                            kac.bulk_set(ka).await;

                            loop {
                                let mut line = String::new();

                                if let Err(e) = socket.read_line(&mut line).await {
                                    error!("error while read_line: {:?} (addr: {})", e, addr);
                                    break;
                                }

                                if line.is_empty() {
                                    break;
                                }

                                if line.starts_with("KA ") {
                                    let line = line.trim();
                                    let mut parts = line.split(' ');

                                    // the KA part
                                    parts.next();

                                    match (parts.next(), parts.next()) {
                                        (Some(id), Some(ts)) => {
                                            debug!(
                                                "Got KA from another node: {} {} (addr: {})",
                                                id, ts, addr
                                            );
                                            let ts = match ts.parse::<i64>() {
                                                Ok(ts) => ts,
                                                Err(e) => {
                                                    error!(
                                                        "Invalid ts '{}' error: {} (addr: {})",
                                                        ts, e, addr
                                                    );
                                                    continue;
                                                }
                                            };

                                            let latency = std::time::SystemTime::now()
                                                .duration_since(std::time::UNIX_EPOCH)
                                                .unwrap()
                                                .as_millis()
                                                as i64
                                                - ts;

                                            metrics::histogram!(
                                                "message_sync_latency_seconds",
                                                latency as f64 / 1000.0,
                                                "addr" => addr.to_string(),
                                            );

                                            kac.set(id, ts).await;
                                        }
                                        _ => {
                                            error!("Invalid KA line: {} (addr: {})", line, addr);
                                        }
                                    }
                                } else if line.starts_with("PING") {
                                    if let Err(e) = timeout(
                                        SOCKET_WRITE_TIMEOUT,
                                        socket.write_all("PONG\n".as_bytes()),
                                    )
                                    .await
                                    {
                                        error!(
                                            "Error writing PONG to socket: {} (addr: {})",
                                            e, addr
                                        );
                                        break;
                                    }
                                } else {
                                    println!("Got a line: {:?} (addr: {})", line, addr);
                                }
                            }
                        }
                        Err(e) => {
                            error!("Error connecting to {}: {}", node, e);
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
            });
        }
    }
}

#[async_trait]
impl KeepAliveTrait for KeepAlive {
    async fn get(&self, id: &str) -> Option<i64> {
        self.keep_alives.get(id).await
    }

    async fn pulse(&self, id: &str) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        self.keep_alives.set(id, now).await;

        let ka = KeepAliveUpdate {
            id: id.to_string(),
            ts: now,
        };

        let txs = self.txs.read().await;

        for (addr, tx) in txs
            .iter()
            .filter(|(_, tx)| !tx.is_closed() && !tx.is_disconnected())
        {
            debug!("sending KA update to channel {} -> {:?}", addr, ka);
            match tx.try_send(Message::KeepAliveUpdate(ka.clone())) {
                Ok(sent) => {
                    if sent {
                        // FIXME: can we know wha node is giving us issues?
                        metrics::increment_counter!("channel_sends_success_total", "type" => "ka", "addr" => addr.to_string());
                    } else {
                        error!("failed sending ping to channel, for addr: {}", addr);
                        metrics::increment_counter!("channel_sends_errors_total", "type" => "ka", "addr" => addr.to_string());
                    }
                }
                Err(SendError::Closed) => error!("socket closed, addr: {}", addr),
                Err(SendError::ReceiveClosed) => {
                    error!("receive closed, addr: {}", addr);
                    tx.close();
                }
            }
        }
    }
}

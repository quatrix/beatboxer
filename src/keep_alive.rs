use anyhow::Result;
use kanal::SendError;
use postcard::from_bytes;
use std::{collections::HashMap, sync::Arc, time::Duration};

use axum::async_trait;

use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::RwLock,
};
use tracing::{debug, error, info};

use crate::storage::Storage;

#[derive(Clone, Debug)]
struct KeepAliveUpdate {
    id: String,
    ts: i64,
}

pub struct KeepAlive {
    server_addr: String,
    nodes: Vec<String>,
    keep_alives: Arc<dyn Storage + Send + Sync>,
    txs: Arc<RwLock<Vec<Arc<kanal::AsyncSender<KeepAliveUpdate>>>>>,
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

    pub async fn listen(&self) -> Result<()> {
        let listener = TcpListener::bind(&self.server_addr).await?;
        info!("Listening on: {}", self.server_addr);

        loop {
            let (mut socket, _) = listener.accept().await?;
            info!("connected client: {:?}", socket);

            let (tx, rx) = kanal::bounded_async(102400);
            let mut txs = self.txs.write().await;
            let kac = Arc::clone(&self.keep_alives);
            let txsc = Arc::clone(&self.txs);
            txs.push(Arc::new(tx));

            tokio::spawn(async move {
                let mut buf = vec![0; 1024];
                let mut already_synched = false;

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
                            match socket.write_all(ka_len.as_bytes()).await {
                                Ok(_) => {}
                                Err(e) => {
                                    error!("Error writing to socket: {}", e);
                                    break;
                                }
                            }
                            match socket.write_all(&ka).await {
                                Ok(_) => {
                                    already_synched = true;
                                }
                                Err(e) => {
                                    error!("Error writing to socket: {}", e);
                                    break;
                                }
                            }
                        } else {
                            error!("Invalid command: {:?}", &buf[0..n]);
                            continue;
                        }
                    }

                    match rx.recv().await {
                        Ok(ka) => {
                            let line = format!("KA {} {}\n", ka.id, ka.ts);
                            debug!("Sending: {}", line);
                            match socket.write_all(line.as_bytes()).await {
                                Ok(_) => {
                                    debug!("Sent to node")
                                }
                                Err(e) => {
                                    error!("Error writing to socket: {}", e);
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            error!("Error receiving from channel: {}", e);
                            break;
                        }
                    }
                }

                info!("Client disconnected: {:?}", socket);
                let mut txs = txsc.write().await;
                txs.retain(|tx| !tx.is_closed());
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

                            let node_ip = match socket.get_ref().peer_addr() {
                                Ok(ip) => ip,
                                Err(e) => {
                                    error!("Error getting peer addr: {}", e);
                                    continue;
                                }
                            };

                            // send a SYNC command
                            match socket.write_all(b"SYNC\n").await {
                                Ok(_) => {}
                                Err(e) => {
                                    error!("Error writing to socket: {}", e);
                                    continue;
                                }
                            }

                            let mut buf = String::new();
                            socket.read_line(&mut buf).await.unwrap();
                            let ka_len = match buf.trim().parse::<usize>() {
                                Ok(ka_len) => ka_len,
                                Err(e) => {
                                    error!("Invalid ka_len: {} error: {}", buf, e);
                                    continue;
                                }
                            };

                            let mut ka = vec![0; ka_len];
                            socket.read_exact(&mut ka).await.unwrap();

                            let ka = match from_bytes::<HashMap<String, i64>>(&ka) {
                                Ok(ka) => ka,
                                Err(e) => {
                                    error!("Invalid ka: {} error: {}", buf, e);
                                    continue;
                                }
                            };

                            kac.bulk_set(ka).await;

                            loop {
                                let mut line = String::new();
                                socket.read_line(&mut line).await.unwrap();

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
                                            debug!("Got KA from another node: {} {}", id, ts);
                                            let ts = match ts.parse::<i64>() {
                                                Ok(ts) => ts,
                                                Err(e) => {
                                                    error!("Invalid ts '{}' error: {}", ts, e);
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
                                                "node_ip" => format!("{}", node_ip),
                                            );

                                            kac.set(id, ts).await;
                                        }
                                        _ => {
                                            error!("Invalid KA line: {}", line);
                                        }
                                    }
                                } else {
                                    println!("Got a line: {:?}", line);
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

        // FIXME: not sure it's a good idead to always
        // lock write, as it's serializes writes to the channels
        // maybe the disconnected sockets should be
        // handled elsewhere
        let mut txs = self.txs.write().await;

        let ka = KeepAliveUpdate {
            id: id.to_string(),
            ts: now,
        };

        txs.retain(|sender| {
            match sender.try_send(ka.clone()) {
                Ok(_) => true,
                Err(SendError::Closed) => false,
                Err(SendError::ReceiveClosed) => true, // handle full case as per your use case
            }
        });
    }
}

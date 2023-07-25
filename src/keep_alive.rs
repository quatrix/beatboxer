use anyhow::Result;
use std::{collections::HashMap, sync::Arc, time::Duration};

use axum::async_trait;

use dashmap::DashMap;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::RwLock,
};
use tracing::{debug, error, info};

#[derive(Clone, Debug)]
struct KeepAliveUpdate {
    id: String,
    ts: i64,
}

pub struct KeepAlive {
    server_addr: String,
    nodes: Vec<String>,
    keep_alives: Arc<RwLock<HashMap<String, i64>>>,
    txs: RwLock<Vec<kanal::AsyncSender<KeepAliveUpdate>>>,
}

#[async_trait]
pub trait KeepAliveTrait {
    async fn pulse(&self, id: &str);
    async fn get(&self, id: &str) -> Option<i64>;
}

// let txs = DashMap::new();
// for node in nodes.clone() {
//     let (tx, rx) = kanal::bounded_async(1024);
//     txs.insert(node, (tx, rx));
// }

impl KeepAlive {
    pub fn new(server_addr: String, nodes: Vec<String>) -> KeepAlive {
        KeepAlive {
            server_addr,
            nodes,
            keep_alives: Arc::new(RwLock::new(HashMap::new())),
            txs: RwLock::new(Vec::new()),
        }
    }

    pub async fn listen(&self) -> Result<()> {
        let listener = TcpListener::bind(&self.server_addr).await?;
        info!("Listening on: {}", self.server_addr);

        loop {
            let (mut socket, _) = listener.accept().await?;
            info!("connected client: {:?}", socket);

            let (tx, rx) = kanal::bounded_async(1024);
            let mut txs = self.txs.write().await;
            txs.push(tx);

            tokio::spawn(async move {
                loop {
                    match rx.recv().await {
                        Ok(ka) => {
                            let line = format!("KA {} {}\n", ka.id, ka.ts);
                            debug!("Sending: {}", line);
                            match socket.write_all(line.as_bytes()).await {
                                Ok(_) => {}
                                Err(e) => {
                                    error!("Error writing to socket: {}", e);
                                    return;
                                }
                            }
                        }
                        Err(e) => {
                            error!("Error receiving from channel: {}", e);
                            return;
                        }
                    }
                }

                // FIXME: handle client disconection
                // remove tx from txs
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

                                            let mut ka = kac.write().await;

                                            match ka.get(id) {
                                                Some(current) => {
                                                    if *current < ts {
                                                        ka.insert(id.to_string(), ts);
                                                    }
                                                }
                                                None => {
                                                    ka.insert(id.to_string(), ts);
                                                }
                                            }
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
        let ka = self.keep_alives.read().await;
        ka.get(id).copied()
    }

    async fn pulse(&self, id: &str) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let mut ka = self.keep_alives.write().await;
        ka.insert(id.to_string(), now);

        for tx in self.txs.read().await.iter() {
            let ka = KeepAliveUpdate {
                id: id.to_string(),
                ts: now,
            };
            match tx.send(ka).await {
                Ok(_) => {}
                Err(e) => {
                    error!("Error sending to channel: {}", e);
                    std::process::exit(1);
                }
            }
        }
    }
}

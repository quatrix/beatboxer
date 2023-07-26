use anyhow::Result;
use postcard::{from_bytes, to_allocvec};
use std::{collections::HashMap, sync::Arc, time::Duration};

use axum::async_trait;

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
    txs: Arc<RwLock<Vec<kanal::AsyncSender<KeepAliveUpdate>>>>,
}

#[async_trait]
pub trait KeepAliveTrait {
    async fn pulse(&self, id: &str);
    async fn get(&self, id: &str) -> Option<i64>;
}

impl KeepAlive {
    pub fn new(server_addr: String, nodes: Vec<String>) -> KeepAlive {
        KeepAlive {
            server_addr,
            nodes,
            keep_alives: Arc::new(RwLock::new(HashMap::new())),
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
            txs.push(tx);

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
                            let ka = kac.read().await;
                            let ka = to_allocvec(&*ka).unwrap();
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

                            // send a SYNC command
                            match socket.write_all(b"SYNC\n").await {
                                Ok(_) => {}
                                Err(e) => {
                                    error!("Error writing to socket: {}", e);
                                    return;
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

                            {
                                let mut kac = kac.write().await;

                                for (id, ts) in ka {
                                    match kac.get(&id) {
                                        Some(current) => {
                                            if *current < ts {
                                                kac.insert(id, ts);
                                            }
                                        }
                                        None => {
                                            kac.insert(id, ts);
                                        }
                                    }
                                }
                            }

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
                                                "" => ""
                                            );

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

        let txs = self.txs.read().await;

        for tx in txs.iter() {
            let ka = KeepAliveUpdate {
                id: id.to_string(),
                ts: now,
            };
            match tx.send(ka).await {
                Ok(_) => {
                    debug!("Sent to channel")
                }
                Err(e) => {
                    error!("Error sending to channel: {}", e);
                }
            }
        }

        //future::join_all(sends).await;
    }
}

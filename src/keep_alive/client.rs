use crate::keep_alive::constants::{SOCKET_READ_LONG_TIMEOUT, SOCKET_WRITE_TIMEOUT};

use super::KeepAlive;

use postcard::from_bytes;
use std::{collections::HashMap, sync::Arc, time::Duration};

use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    time::timeout,
};
use tracing::{debug, error, info};

impl KeepAlive {
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
                            info!("sending SYNC to {}", node);
                            match timeout(SOCKET_WRITE_TIMEOUT, socket.write_all(b"SYNC\n")).await {
                                Ok(_) => {}
                                Err(e) => {
                                    error!("Error writing SYNC to socket: {} (addr: {})", e, addr);
                                    continue;
                                }
                            }

                            let mut buf = String::new();

                            if let Err(e) =
                                timeout(SOCKET_READ_LONG_TIMEOUT, socket.read_line(&mut buf)).await
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

                            let t0 = std::time::Instant::now();
                            info!("getting state from {}... (size: {})", node, ka_len);

                            if let Err(e) =
                                timeout(SOCKET_READ_LONG_TIMEOUT, socket.read_exact(&mut ka)).await
                            {
                                error!("failed getting STATE: {:?} (addr: {})", e, addr);
                                continue;
                            }

                            info!(
                                "got state from {}. (size: {}) took: {}",
                                node,
                                ka_len,
                                t0.elapsed().as_secs_f32()
                            );

                            let t0 = std::time::Instant::now();
                            let ka = match from_bytes::<HashMap<String, i64>>(&ka) {
                                Ok(ka) => ka,
                                Err(e) => {
                                    error!("Invalid ka: {} error: {} (addr: {})", buf, e, addr);
                                    continue;
                                }
                            };

                            info!(
                                "deserializing state from {}. took: {}",
                                node,
                                t0.elapsed().as_secs_f32()
                            );

                            let t0 = std::time::Instant::now();
                            kac.bulk_set(ka).await;

                            info!(
                                "storing state from {}. took: {}",
                                node,
                                t0.elapsed().as_secs_f32()
                            );

                            info!("ready. listening on updates from {}...", node);

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

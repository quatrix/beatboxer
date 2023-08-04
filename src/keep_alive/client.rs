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
        for addr in self.nodes.clone() {
            let kac = Arc::clone(&self.keep_alives);
            tokio::spawn(async move {
                loop {
                    info!("Connecting to {}", addr);

                    match TcpStream::connect(&addr).await {
                        Ok(socket) => {
                            let mut socket = BufReader::new(socket);

                            // send a SYNC command
                            info!("[{}] Sending SYNC reques", addr);
                            match timeout(SOCKET_WRITE_TIMEOUT, socket.write_all(b"SYNC\n")).await {
                                Ok(_) => {}
                                Err(e) => {
                                    error!("[{}] Error sending SYNC request: {:?}", addr, e);
                                    continue;
                                }
                            }

                            let mut buf = String::new();

                            if let Err(e) =
                                timeout(SOCKET_READ_LONG_TIMEOUT, socket.read_line(&mut buf)).await
                            {
                                error!("[{}] Error reading ka_len line from socket {:?}", addr, e);
                                continue;
                            }

                            let ka_len = match buf.trim().parse::<usize>() {
                                Ok(ka_len) => ka_len,
                                Err(e) => {
                                    error!("[{}] Invalid ka_len: {} error: {}", addr, buf, e);
                                    continue;
                                }
                            };

                            let mut ka = vec![0; ka_len];

                            let t0 = std::time::Instant::now();
                            info!("[{}] Getting state... (size: {})", addr, ka_len);

                            if let Err(e) =
                                timeout(SOCKET_READ_LONG_TIMEOUT, socket.read_exact(&mut ka)).await
                            {
                                error!("[{}] Failed getting STATE: {:?}", addr, e);
                                continue;
                            }

                            info!(
                                "[{}] Got state from. (size: {}) took: {:.2} secs",
                                addr,
                                ka_len,
                                t0.elapsed().as_secs_f32()
                            );

                            let t0 = std::time::Instant::now();
                            let ka = match from_bytes::<HashMap<String, i64>>(&ka) {
                                Ok(ka) => ka,
                                Err(e) => {
                                    error!("[{}] Invalid ka: {} error: {}", addr, buf, e);
                                    continue;
                                }
                            };

                            info!(
                                "[{}] Deserializing state took: {:.2} secs ({} keys)",
                                addr,
                                ka.keys().len(),
                                t0.elapsed().as_secs_f32()
                            );

                            let t0 = std::time::Instant::now();
                            kac.bulk_set(ka).await;

                            info!(
                                "[{}] Storing state took: {:.2} secs",
                                addr,
                                t0.elapsed().as_secs_f32()
                            );

                            info!("[{}] Synched. listening on updates...", addr);

                            loop {
                                let mut line = String::new();

                                if let Err(e) = socket.read_line(&mut line).await {
                                    error!("[{}] Error while read_line: {:?}", addr, e);
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
                                            debug!("[{}] Got KA {} -> {}", addr, id, ts);
                                            let ts = match ts.parse::<i64>() {
                                                Ok(ts) => ts,
                                                Err(e) => {
                                                    error!(
                                                        "[{}] Invalid ts '{}' error: {}",
                                                        addr, ts, e
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
                                            error!("[{}] Invalid KA line: {}", addr, line);
                                        }
                                    }
                                } else if line.starts_with("PING") {
                                    if let Err(e) = timeout(
                                        SOCKET_WRITE_TIMEOUT,
                                        socket.write_all("PONG\n".as_bytes()),
                                    )
                                    .await
                                    {
                                        error!("[{}] Error writing PONG to socket: {}", addr, e);
                                        break;
                                    }
                                } else {
                                    error!("[{}] Got unexpected command: {})", addr, line);
                                }
                            }
                        }
                        Err(e) => {
                            error!("[{}] Error connecting: {}, trying again...", addr, e);
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
            });
        }
    }
}

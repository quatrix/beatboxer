use crate::keep_alive::{
    constants::{SOCKET_READ_LONG_TIMEOUT, SOCKET_WRITE_TIMEOUT},
    types::Event,
};

use super::KeepAlive;
use anyhow::Result;

use postcard::from_bytes;
use serde::{de::DeserializeOwned, Deserialize};
use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    time::timeout,
};
use tracing::{debug, error, info};

async fn read_blob<T: Clone + DeserializeOwned>(
    tag: &str,
    socket: &mut BufReader<TcpStream>,
    addr: &str,
) -> Result<T> {
    let t0 = std::time::Instant::now();
    let mut buf = String::new();

    let _ = timeout(SOCKET_READ_LONG_TIMEOUT, socket.read_line(&mut buf)).await?;

    let blob_len = buf.trim().parse::<usize>()?;

    let mut blob = vec![0; blob_len];

    info!("[{}] Getting {}... (size: {})", addr, tag, blob_len);

    let _ = timeout(SOCKET_READ_LONG_TIMEOUT, socket.read_exact(&mut blob)).await?;

    info!(
        "[{}] Got {} from. (size: {}) took: {:.2} secs",
        addr,
        tag,
        blob_len,
        t0.elapsed().as_secs_f32()
    );

    let t0 = std::time::Instant::now();
    let blob: T = from_bytes(&blob)?;

    info!(
        "[{}] Deserialized {} took: {:.2} secs",
        addr,
        tag,
        t0.elapsed().as_secs_f32()
    );

    Ok(blob)
}

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

                            let ka = match read_blob::<HashMap<String, i64>>(
                                "STATE",
                                &mut socket,
                                &addr,
                            )
                            .await
                            {
                                Ok(ka) => ka,
                                Err(e) => {
                                    error!("error while reading STATE blob: {:?}", e);
                                    continue;
                                }
                            };

                            let t0 = std::time::Instant::now();
                            kac.bulk_set(ka).await;

                            info!(
                                "[{}] Storing state took: {:.2} secs",
                                addr,
                                t0.elapsed().as_secs_f32()
                            );

                            let events =
                                match read_blob::<VecDeque<Event>>("EVENTS", &mut socket, &addr)
                                    .await
                                {
                                    Ok(events) => events,
                                    Err(e) => {
                                        error!("error while reading EVENTS blob: {:?}", e);
                                        continue;
                                    }
                                };

                            kac.merge_events(events).await;

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

                                    match (parts.next(), parts.next(), parts.next()) {
                                        (Some(id), Some(ts), Some(ce)) => {
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

                                            let is_connection_event = match ce.parse::<u8>() {
                                                Ok(ce) => ce == 1,
                                                Err(e) => {
                                                    error!(
                                                            "[{}] Invalid connection_event '{}' error: {}",
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

                                            kac.set(id, ts, is_connection_event).await;
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

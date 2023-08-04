use crate::keep_alive::constants::{SOCKET_WRITE_LONG_TIMEOUT, SOCKET_WRITE_TIMEOUT};
use crate::keep_alive::types::Message;

use super::KeepAlive;

use anyhow::Result;
use std::{sync::Arc, time::Instant};
use tokio::sync::mpsc;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::timeout,
};
use tracing::{debug, error, info};

impl KeepAlive {
    pub async fn listen(&self) -> Result<()> {
        let listener = TcpListener::bind(&self.server_addr).await?;
        info!("Listening on: {}", self.server_addr);

        self.health_checker();

        loop {
            let (mut socket, addr) = listener.accept().await?;
            info!("connected client: {:?} addr: {}", socket, addr);

            let (tx, mut rx) = mpsc::channel(102400);
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
                            let t0 = std::time::Instant::now();
                            info!("Sending STATE... (size {}) to {}", ka.len(), addr);
                            if let Err(e) =
                                timeout(SOCKET_WRITE_TIMEOUT, socket.write_all(ka_len.as_bytes()))
                                    .await
                            {
                                error!("Error writing SYNC to socket: {}", e);
                                break;
                            }
                            info!(
                                "Sent STATE. (size {}) to {} (took: {})",
                                ka.len(),
                                addr,
                                t0.elapsed().as_secs_f32()
                            );

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
                            Some(message) => match message {
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
                            None => {
                                error!("Error receiving from channel");
                                break;
                            }
                        }
                    }
                }

                info!("Client disconnected: {:?}", socket);
            });
        }
    }
}

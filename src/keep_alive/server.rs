use crate::keep_alive::constants::{
    LAST_PONG_TIMEOUT, SOCKET_WRITE_LONG_TIMEOUT, SOCKET_WRITE_TIMEOUT,
};
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
            info!("[{}] Connected client!", addr);

            let (tx, mut rx) = mpsc::channel(102400);
            let mut txs = self.txs.write().await;
            let kac = Arc::clone(&self.keep_alives);

            txs.push((addr.to_string(), tx));

            tokio::spawn(async move {
                let mut buf = vec![0; 1024];
                let mut already_synched = false;
                let mut last_pong = Instant::now();

                loop {
                    // first wait for a SYNC request
                    // to send a full sync of the current state
                    if !already_synched {
                        let n = match socket.read(&mut buf).await {
                            Ok(0) => {
                                info!("[{}] Socket closed.", addr);
                                break;
                            }
                            Ok(n) => n,
                            Err(e) => {
                                error!("[{}] Error reading from socket: {}", addr, e);
                                break;
                            }
                        };

                        if n == 5 && buf[0..n] == *b"SYNC\n" {
                            let t0 = std::time::Instant::now();
                            let ka = kac.serialize().await.unwrap();
                            info!(
                                "[{}] Serialized STATE took {:.2} sec",
                                addr,
                                t0.elapsed().as_secs_f32()
                            );

                            let ka_len = format!("{}\n", ka.len());
                            let t0 = std::time::Instant::now();
                            info!("[{}] Sending STATE... (size {})", addr, ka.len());
                            if let Err(e) =
                                timeout(SOCKET_WRITE_TIMEOUT, socket.write_all(ka_len.as_bytes()))
                                    .await
                            {
                                error!("[{}] Error writing SYNC to socket: {}", addr, e);
                                break;
                            }
                            info!(
                                "[{}] Sent STATE. size {} took: {:.2}) secs",
                                addr,
                                ka.len(),
                                t0.elapsed().as_secs_f32()
                            );

                            match timeout(SOCKET_WRITE_LONG_TIMEOUT, socket.write_all(&ka)).await {
                                Ok(_) => {
                                    already_synched = true;

                                    // XXX: we want to start checking keep alive only after sync
                                    last_pong = Instant::now();
                                }
                                Err(e) => {
                                    error!("[{}] Error writing STATE to socket: {}", addr, e);
                                    break;
                                }
                            }
                        } else {
                            error!("[{}] Invalid command: {:?}", addr, &buf[0..n]);
                            continue;
                        }
                    }

                    tokio::select! {
                        Ok(n) = socket.read(&mut buf) => {
                            if n == 0 {
                                info!("[{}] Client closed socket", addr);
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
                                    debug!("[{}] Sending Ping", addr);
                                    let elapsed_since_last_pong = last_pong.elapsed();

                                    if elapsed_since_last_pong > LAST_PONG_TIMEOUT {
                                        error!("[{}] Didn't see pong for a while ({:.2} secs), node is probably dead, closing socket.", addr, elapsed_since_last_pong.as_secs_f32());
                                        break;
                                    }

                                    match timeout(SOCKET_WRITE_TIMEOUT, socket.write_all("PING\n".as_bytes())).await {
                                        Ok(_) => {
                                            debug!("[{}] Sent PING to node", addr)
                                        }
                                        Err(e) => {
                                            error!("[{}] Error writing PING to socket: {}", addr, e);
                                            break;
                                        }
                                    }
                                }
                                Message::KeepAliveUpdate(ka) => {
                                    let line = format!("KA {} {}\n", ka.id, ka.ts);
                                    debug!("[{}] Sending KA '{}'", addr, line);
                                    match timeout(SOCKET_WRITE_TIMEOUT, socket.write_all(line.as_bytes())).await {
                                        Ok(_) => {
                                            debug!("[{}] Sent KA to node", addr);
                                        }
                                        Err(e) => {
                                            error!("[{}] Error writing KA to socket: {}", addr, e);
                                            break;
                                        }
                                    }
                                }
                            },
                            None => {
                                error!("[{}] Error receiving from channel", addr);
                                break;
                            }
                        }
                    }
                }

                info!("[{}] Client disconnected", addr);
            });
        }
    }
}

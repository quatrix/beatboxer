use std::fmt::Write as _;

use crate::keep_alive::{
    constants::{LAST_PONG_TIMEOUT, SOCKET_WRITE_LONG_TIMEOUT, SOCKET_WRITE_TIMEOUT, SYNC_TIMEOUT},
    types::Message,
};
use crate::storage::Storage;

use super::KeepAlive;

use anyhow::{anyhow, Result};
use arrayvec::ArrayString;
use std::net::SocketAddr;
use std::{sync::Arc, time::Instant};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{self, Receiver};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::timeout,
};
use tracing::{debug, error, info};

async fn send_blob(
    tag: &str,
    socket: &mut TcpStream,
    addr: &SocketAddr,
    blob: &Vec<u8>,
) -> Result<()> {
    let t0 = std::time::Instant::now();

    let blob_len = format!("{}\n", blob.len());

    let _ = timeout(*SOCKET_WRITE_TIMEOUT, socket.write_all(blob_len.as_bytes())).await?;
    let _ = timeout(*SOCKET_WRITE_LONG_TIMEOUT, socket.write_all(blob)).await?;

    info!(
        "[{}] send blob {} took {:.2} sec",
        addr,
        tag,
        t0.elapsed().as_secs_f32()
    );

    Ok(())
}

impl KeepAlive {
    pub async fn subscribe_to_commands(&self, addr: SocketAddr) -> Receiver<Message> {
        let mut txs = self.txs.write().await;
        let (tx, rx) = mpsc::channel(3_000_000);
        txs.push((addr.to_string(), tx));

        rx
    }

    pub async fn listen(&self) -> Result<()> {
        let server_addr = format!("{}:{}", self.listen_addr, self.listen_port);
        let listener = TcpListener::bind(&server_addr).await?;
        info!("Listening on: {}", server_addr);

        self.health_checker();

        loop {
            let (mut socket, addr) = listener.accept().await?;
            info!("[{}] Connected client!", addr);

            let mut rx = self.subscribe_to_commands(addr).await;
            let kac = Arc::clone(&self.keep_alives);

            tokio::spawn(async move {
                let mut already_synched = false;
                let mut last_pong = Instant::now();

                loop {
                    // first wait for a SYNC request
                    // to send a full sync of the current state
                    if !already_synched {
                        match sync_with_client(&mut socket, &addr, &kac).await {
                            Ok(_) => {
                                last_pong = Instant::now();
                                already_synched = true;
                            }
                            Err(e) => {
                                error!("[{}] got error while synching: {:?}", addr, e);
                                break;
                            }
                        }
                    }

                    if let Err(e) =
                        handle_client_commands(&mut socket, &addr, &mut last_pong, &mut rx).await
                    {
                        error!("[{}] got error while handling commands: {:?}", addr, e);
                        break;
                    }
                }

                info!("[{}] Client disconnected", addr);
            });
        }
    }
}

async fn handle_client_commands(
    socket: &mut TcpStream,
    addr: &SocketAddr,
    last_pong: &mut Instant,
    rx: &mut Receiver<Message>,
) -> Result<()> {
    let mut buf = vec![0; 100];

    tokio::select! {
        Ok(n) = socket.read(&mut buf) => {
            if n == 0 {
                info!("[{}] Client closed socket", addr);
                Err(anyhow!("Client closed socket"))
            } else {
                // FIXME: for now we're not parsing the message
                // because it can only be PONG.
                *last_pong = Instant::now();
                Ok(())
            }
        },

        v = rx.recv() => match v {
            Some(message) => match message {
                Message::Ping => {
                    debug!("[{}] Sending Ping", addr);
                    let elapsed_since_last_pong = last_pong.elapsed();

                    if elapsed_since_last_pong > *LAST_PONG_TIMEOUT {
                        return Err(anyhow!("[{}] Didn't see pong for a while ({:.2} secs), node is probably dead, closing socket.", addr, elapsed_since_last_pong.as_secs_f32()));
                    }

                    let _ = timeout(*SOCKET_WRITE_TIMEOUT, socket.write_all("PING\n".as_bytes())).await?;
                    Ok(())
                }
                Message::KeepAliveUpdate(ka) => {
                    let mut write_buf = ArrayString::<64>::new();
                    writeln!(write_buf, "KA {} {} {}", ka.id, ka.ts, ka.is_connection_event as u8)?;
                    debug!("[{}] Sending KA '{}'...", addr, write_buf);
                    let _ = timeout(*SOCKET_WRITE_TIMEOUT, socket.write_all(write_buf.as_bytes())).await?;
                    debug!("[{}] Sent KA '{}'.", addr, write_buf);
                    Ok(())
                }
                Message::DeadUpdate(dd) => {
                    let mut write_buf = ArrayString::<64>::new();
                    writeln!(write_buf, "DD {} {} {}", dd.id, dd.last_ka, dd.ts_of_death)?;
                    debug!("[{}] Sending DD '{}'...", addr, write_buf);
                    let _ = timeout(*SOCKET_WRITE_TIMEOUT, socket.write_all(write_buf.as_bytes())).await?;
                    debug!("[{}] Sent DD '{}'.", addr, write_buf);
                    Ok(())
                }
            },
            None => {
                Err(anyhow!("[{}] Error receiving from channel", addr))
            }
        }
    }
}

async fn sync_with_client(
    socket: &mut TcpStream,
    addr: &SocketAddr,
    kac: &Arc<dyn Storage + Sync + Send>,
) -> Result<()> {
    let mut buf = vec![0; 100];
    let n = socket.read(&mut buf).await?;

    if n == 0 {
        return Err(anyhow!("Client closed socket"));
    }

    if n == 5 && buf[0..n] == *b"SYNC\n" {
        let t0 = std::time::Instant::now();
        let state = kac.serialize_state().await?;

        match kac.last_event_ts().await {
            Some(last_event_ts) => info!("[{}] sending events up to {}", addr, last_event_ts),
            None => info!("[{}] events is empty", addr),
        };

        let events = kac.serialize_events().await?;

        info!(
            "[{}] serializing state + events took {:.2} sec",
            addr,
            t0.elapsed().as_secs_f32()
        );

        send_blob("STATE", socket, addr, &state).await?;
        send_blob("EVENTS", socket, addr, &events).await?;

        let n = timeout(*SYNC_TIMEOUT, socket.read(&mut buf)).await?;
        let n = match n {
            Ok(n) => n,
            Err(e) => return Err(e.into()),
        };

        if n == 0 {
            return Err(anyhow!("Client closed socket"));
        }

        if n == 8 && buf[0..n] == *b"SYNCHED\n" {
            info!(
                "[✅] [{}] SYNC end-to-end time {:.2} sec",
                addr,
                t0.elapsed().as_secs_f32()
            );

            Ok(())
        } else {
            Err(anyhow!(
                "[{}] Expecting to get SYNCHED but got something else {:?}",
                addr,
                &buf[0..n],
            ))
        }
    } else {
        Err(anyhow!(
            "[{}] Invalid command at this stage, expecting SYNC: {:?}",
            addr,
            &buf[0..n]
        ))
    }
}

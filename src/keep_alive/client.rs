use crate::{
    keep_alive::{
        cluster_status::NodeStatus,
        constants::{SOCKET_READ_LONG_TIMEOUT, SOCKET_WRITE_TIMEOUT},
        types::Event,
    },
    storage::Storage,
};

use super::KeepAlive;
use anyhow::Result;

use nom::{
    bytes::complete::tag,
    character::complete::{alpha1, alphanumeric1, digit1, space1},
    combinator::{map_res, recognize},
    sequence::Tuple,
    IResult,
};

use postcard::from_bytes;
use serde::de::DeserializeOwned;
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::Duration,
};

use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    time::timeout,
};
use tracing::{error, info};

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

async fn write(socket: &mut BufReader<TcpStream>, msg: &[u8]) -> Result<()> {
    let _ = timeout(SOCKET_WRITE_TIMEOUT, socket.write_all(msg)).await?;
    Ok(())
}

async fn do_sync(
    addr: &str,
    socket: &mut BufReader<TcpStream>,
    kac: &Arc<dyn Storage + Send + Sync>,
) -> Result<()> {
    info!("[{}] Sending SYNC reques", addr);

    write(socket, b"SYNC\n").await?;
    let ka = read_blob::<HashMap<String, i64>>("STATE", socket, addr).await?;

    let t0 = std::time::Instant::now();
    kac.bulk_set(ka).await;

    info!(
        "[{}] Storing state took: {:.2} secs",
        addr,
        t0.elapsed().as_secs_f32()
    );

    let events = read_blob::<VecDeque<Event>>("EVENTS", socket, addr).await?;
    kac.merge_events(events).await;
    Ok(())
}

struct KaCommand {
    id: String,
    ts: i64,
    is_connection_event: bool,
}

enum Command {
    KA(KaCommand),
    Ping,
    Closed,
    ParseError(String),
    Unknown(String),
}

fn parse_i64(input: &str) -> IResult<&str, i64> {
    map_res(recognize(digit1), str::parse)(input)
}

fn parse_u8(input: &str) -> IResult<&str, u8> {
    map_res(recognize(digit1), str::parse)(input)
}

fn ka_command(input: &str) -> IResult<&str, KaCommand> {
    let (input, _) = tag("KA ")(input)?;
    let (input, (id, _, ts, _, is_connection_event)) =
        (alphanumeric1, space1, parse_i64, space1, parse_u8).parse(input)?;
    Ok((
        input,
        KaCommand {
            id: id.to_string(),
            ts,
            is_connection_event: is_connection_event == 1,
        },
    ))
}

fn parse_command(line: &str) -> Command {
    if line.is_empty() {
        Command::Closed
    } else if line.starts_with("KA ") {
        match ka_command(line) {
            Ok((_, ka)) => Command::KA(ka),
            Err(e) => Command::ParseError(e.to_string()),
        }
    } else if line.starts_with("PING") {
        Command::Ping
    } else {
        Command::Unknown(line.to_string())
    }
}

async fn get_command(socket: &mut BufReader<TcpStream>) -> Result<Command> {
    let mut line = String::new();
    socket.read_line(&mut line).await?;

    Ok(parse_command(&line))
}

impl KeepAlive {
    pub fn connect_to_nodes(&self) {
        for addr in self.nodes.clone() {
            let kac = Arc::clone(&self.keep_alives);
            let cluster_status = Arc::clone(&self.cluster_status);

            tokio::spawn(async move {
                loop {
                    info!("Connecting to {}", addr);

                    match TcpStream::connect(&addr).await {
                        Ok(socket) => {
                            let mut socket = BufReader::new(socket);

                            match do_sync(&addr, &mut socket, &kac).await {
                                Ok(_) => {
                                    cluster_status.set_node_status(&addr, NodeStatus::Synched);
                                    cluster_status.update_last_sync(&addr);
                                }
                                Err(e) => {
                                    error!("[{}] Failed to sync: {:?}", addr, e);
                                    cluster_status
                                        .set_node_status(&addr, NodeStatus::FailedSynched);
                                    continue;
                                }
                            }

                            info!("[{}] Synched. listening on updates...", addr);

                            loop {
                                let command = match get_command(&mut socket).await {
                                    Ok(c) => c,
                                    Err(e) => {
                                        error!("[{}] error reading command: {:?}", addr, e);
                                        break;
                                    }
                                };

                                match command {
                                    Command::Closed => {
                                        info!("[{}] closed connection", addr);
                                        break;
                                    }

                                    Command::KA(ka) => {
                                        let latency = std::time::SystemTime::now()
                                            .duration_since(std::time::UNIX_EPOCH)
                                            .unwrap()
                                            .as_millis()
                                            as i64
                                            - ka.ts;

                                        metrics::histogram!(
                                            "message_sync_latency_seconds",
                                            latency as f64 / 1000.0,
                                            "addr" => addr.to_string(),
                                        );

                                        kac.set(&ka.id, ka.ts, ka.is_connection_event).await;
                                    }
                                    Command::Ping => {
                                        if let Err(e) = write(&mut socket, b"PONG\n").await {
                                            error!(
                                                "[{}] Error writing PONG to socket: {}",
                                                addr, e
                                            );
                                            break;
                                        }
                                        cluster_status.update_last_ping(&addr);
                                    }
                                    Command::ParseError(e) => {
                                        error!("[{}] Error parsing command: {})", addr, e);
                                    }
                                    Command::Unknown(line) => {
                                        error!("[{}] Got unexpected command: {}", addr, line);
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            cluster_status.set_node_status(&addr, NodeStatus::Dead);
                            error!("[{}] Error connecting: {}, trying again...", addr, e);
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
            });
        }
    }
}

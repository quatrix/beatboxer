use crate::{
    keep_alive::{
        cluster_status::NodeStatus,
        constants::{CONSOLIDATION_WINDOW, SOCKET_READ_LONG_TIMEOUT, SOCKET_WRITE_TIMEOUT},
        types::Event,
    },
    storage::{memory::zset::DeviceState, Storage},
};

use super::KeepAlive;
use anyhow::Result;

use nom::{
    branch::alt,
    bytes::complete::tag,
    character::complete::{alphanumeric1, digit1, multispace1, one_of},
    combinator::eof,
    sequence::tuple,
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
use tracing::{debug, error, info};

async fn read_blob<T: Clone + DeserializeOwned + Send + Sync + 'static>(
    tag: &str,
    socket: &mut BufReader<TcpStream>,
    addr: &str,
) -> Result<T> {
    let t0 = std::time::Instant::now();
    let mut buf = String::new();

    let _ = timeout(*SOCKET_READ_LONG_TIMEOUT, socket.read_line(&mut buf)).await?;

    let blob_len = buf.trim().parse::<usize>()?;

    let mut blob = vec![0; blob_len];

    info!("[{}] Getting {}... (size: {})", addr, tag, blob_len);

    let _ = timeout(*SOCKET_READ_LONG_TIMEOUT, socket.read_exact(&mut blob)).await?;

    info!(
        "[{}] Got {} from. (size: {}) took: {:.2} secs",
        addr,
        tag,
        blob_len,
        t0.elapsed().as_secs_f32()
    );

    let t0 = std::time::Instant::now();

    let f = tokio::task::spawn_blocking(move || -> Result<T> {
        let blob: T = from_bytes(&blob)?;
        Ok(blob)
    });

    let blob = f.await?;

    info!(
        "[{}] Deserialized {} took: {:.2} secs",
        addr,
        tag,
        t0.elapsed().as_secs_f32()
    );

    blob
}

async fn write(socket: &mut BufReader<TcpStream>, msg: &[u8]) -> Result<()> {
    let _ = timeout(*SOCKET_WRITE_TIMEOUT, socket.write_all(msg)).await?;
    Ok(())
}

async fn do_sync(
    addr: &str,
    socket: &mut BufReader<TcpStream>,
    kac: &Arc<dyn Storage + Send + Sync>,
) -> Result<()> {
    info!("[🔄] Sending SYNC request to {}...", addr);
    let t_e2e_0 = std::time::Instant::now();

    write(socket, b"SYNC\n").await?;
    let ka = read_blob::<HashMap<String, DeviceState>>("STATE", socket, addr).await?;
    let events = read_blob::<VecDeque<Event>>("EVENTS", socket, addr).await?;

    info!("[🔄] Got state + events from {}...", addr);

    let t0 = std::time::Instant::now();
    kac.bulk_set(ka).await;
    info!(
        "[💾] stored STATE. took {:.2} secs. from {}",
        t0.elapsed().as_secs_f32(),
        addr
    );

    let t0 = std::time::Instant::now();
    kac.merge_events(events).await;
    info!(
        "[💾] stored EVENTS. took {:.2} secs. from {}",
        t0.elapsed().as_secs_f32(),
        addr
    );

    write(socket, b"SYNCHED\n").await?;

    info!(
        "[🔄✅] SYNC with {} done. (took {:.2})",
        addr,
        t_e2e_0.elapsed().as_secs_f32()
    );
    Ok(())
}

#[derive(Debug, PartialEq)]
struct KaCommand {
    id: String,
    ts: i64,
    is_connection_event: bool,
}

#[derive(Debug, PartialEq)]
struct DeadCommand {
    id: String,
    last_ka: i64,
    ts_of_death: i64,
}

#[derive(Debug, PartialEq)]
enum Command {
    KA(KaCommand),
    Dead(DeadCommand),
    ParseError(String),
    Unknown(String),
    SyncMarker,
    Ping,
    Closed,
}

fn parse_ka(input: &str) -> IResult<&str, Command> {
    let mut parser = tuple((
        tag("KA"),
        multispace1,
        alphanumeric1,
        multispace1,
        digit1,
        multispace1,
        one_of("01"),
    ));

    let (input, (_, _, id_str, _, ts_str, _, is_connected_char)) = parser(input)?;

    let id = id_str.to_string();
    let ts = match ts_str.parse::<i64>() {
        Ok(ts) => ts,
        Err(e) => return Ok((input, Command::ParseError(e.to_string()))),
    };

    let is_connection_event = match is_connected_char {
        '1' => true,
        '0' => false,
        _ => unreachable!("this shouldn't happen"),
    };

    Ok((
        input,
        Command::KA(KaCommand {
            id,
            ts,
            is_connection_event,
        }),
    ))
}

fn parse_dead(input: &str) -> IResult<&str, Command> {
    let mut parser = tuple((
        tag("DD"),
        multispace1,
        alphanumeric1,
        multispace1,
        digit1,
        multispace1,
        digit1,
    ));

    let (input, (_, _, id_str, _, last_ka_str, _, ts_of_death_str)) = parser(input)?;

    let id = id_str.to_string();
    let last_ka = match last_ka_str.parse::<i64>() {
        Ok(ts) => ts,
        Err(e) => return Ok((input, Command::ParseError(e.to_string()))),
    };

    let ts_of_death = match ts_of_death_str.parse::<i64>() {
        Ok(ts) => ts,
        Err(e) => return Ok((input, Command::ParseError(e.to_string()))),
    };

    Ok((
        input,
        Command::Dead(DeadCommand {
            id,
            last_ka,
            ts_of_death,
        }),
    ))
}

fn parse_ping(input: &str) -> IResult<&str, Command> {
    let (input, _) = tag("PING")(input)?;
    Ok((input, Command::Ping))
}

fn parse_sync_marker(input: &str) -> IResult<&str, Command> {
    let (input, _) = tag("S_MARK")(input)?;
    Ok((input, Command::SyncMarker))
}

fn parse_closed(input: &str) -> IResult<&str, Command> {
    let (input, _) = eof(input)?; // check if the input is empty
    Ok((input, Command::Closed))
}

fn parse_command(input: &str) -> Command {
    let (rest, cmd) = alt((
        parse_closed,
        parse_ka,
        parse_dead,
        parse_ping,
        parse_sync_marker,
    ))(input)
    .unwrap_or((input, Command::Unknown(input.to_string())));
    if rest.trim().is_empty() {
        cmd
    } else {
        Command::Unknown(rest.to_string())
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
                let mut printed_connecting = false;
                let mut printed_conn_error = false;
                let max_connect_attempts = 5;
                let mut connect_attempts = 0;
                loop {
                    if !printed_connecting {
                        info!("Connecting to {}", addr);
                        printed_connecting = true;
                    }

                    connect_attempts += 1;

                    match TcpStream::connect(&addr).await {
                        Ok(socket) => {
                            printed_connecting = false;
                            printed_conn_error = false;

                            let mut socket = BufReader::new(socket);

                            if let Err(e) = do_sync(&addr, &mut socket, &kac).await {
                                error!("[{}] Failed to sync: {:?}", addr, e);
                                cluster_status.set_node_status(&addr, NodeStatus::SyncFailed);
                                continue;
                            }

                            info!(
                                "[{}] Mostly Synched. listening on updates...[waiting for marker]",
                                addr
                            );

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

                                        if ka.is_connection_event {
                                            debug!("got KA from {} - event: {:?}", addr, ka);
                                        }
                                        kac.set(&ka.id, ka.ts, ka.is_connection_event).await;
                                    }
                                    Command::Dead(dd) => {
                                        //info!("[{}] got DD update: {:?}", addr, dd);
                                        kac.dead(&dd.id, dd.last_ka, dd.ts_of_death).await;
                                    }
                                    Command::SyncMarker => {
                                        info!("[{}] Got Sync Marker!", addr);
                                        cluster_status.set_node_status(&addr, NodeStatus::Synched);
                                        cluster_status.update_last_sync(&addr);
                                        connect_attempts = 0;
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
                            if connect_attempts > max_connect_attempts {
                                // don't be so fast to declare a node dead
                                cluster_status.set_node_status(&addr, NodeStatus::Dead);
                            }
                            if !printed_conn_error {
                                error!("[{}] Error connecting: {}, trying again...", addr, e);
                                printed_conn_error = true;
                            }
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
            });
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_cmd_ka_connected_event() {
        let cmd = "KA foo123 1337 1";
        assert_eq!(
            parse_command(cmd),
            Command::KA(KaCommand {
                id: "foo123".to_string(),
                ts: 1337,
                is_connection_event: true,
            })
        );
    }

    #[test]
    fn test_cmd_ka_not_connected_event() {
        let cmd = "KA foo123 1337 0";
        assert_eq!(
            parse_command(cmd),
            Command::KA(KaCommand {
                id: "foo123".to_string(),
                ts: 1337,
                is_connection_event: false,
            })
        );
    }

    #[test]
    fn test_cmd_ka_malformed() {
        let cmd = "KA foo123 hey 0";
        assert_eq!(
            parse_command(cmd),
            Command::Unknown("KA foo123 hey 0".to_string())
        );
    }

    #[test]
    fn test_cmd_ka_very_big_ts() {
        let cmd = "KA foo123 9999999999999999999999999999999999999999 0";
        assert_eq!(
            parse_command(cmd),
            Command::ParseError("number too large to fit in target type".to_string())
        );
    }

    #[test]
    fn test_ping() {
        let cmd = "PING";
        assert_eq!(parse_command(cmd), Command::Ping);
    }

    #[test]
    fn test_unknown_command() {
        let cmd = "VOVA666";
        assert_eq!(parse_command(cmd), Command::Unknown("VOVA666".to_string()));
    }
}

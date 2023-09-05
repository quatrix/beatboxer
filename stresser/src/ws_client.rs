use std::{collections::HashSet, time::Duration};

use futures::StreamExt;
use reqwest::StatusCode;
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::connect_async;
use tracing::{debug, error, info};

use crate::{
    event::iso8601,
    strategies::{ledger::millis_to_systemtime, Strategy},
};

pub async fn ws_client(
    mut rx: oneshot::Receiver<()>,
    node: String,
    latch: mpsc::Sender<()>,
    mut strategy: impl Strategy,
) -> impl Strategy {
    let mut done = false;
    let mut seen = HashSet::new();
    let mut offset = None;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(300))
        .build()
        .unwrap();

    let mut printed_offset_msg = false;
    let mut printed_waiting_msg = false;

    while !done {
        let ws_uri = match offset {
            None => format!("ws://{node}/updates"),
            Some(n) => {
                let asked_offset = n - 30_000;
                if !printed_offset_msg {
                    info!(
                        "[{}] ts of last update: {} (asking for offset {})",
                        node,
                        iso8601(&millis_to_systemtime(n)),
                        iso8601(&millis_to_systemtime(asked_offset))
                    );
                    printed_offset_msg = true;
                }

                format!("ws://{node}/updates?offset={}", asked_offset)
            }
        };

        if !printed_waiting_msg {
            info!("waiting for node {} to become ready...", node);
            printed_waiting_msg = true;
        }

        let url = format!("http://{node}/ready");

        match client.get(&url).send().await {
            Ok(result) => match result.status() {
                StatusCode::OK => {}
                _ => {
                    tokio::time::sleep(Duration::from_millis(1000)).await;
                    continue;
                }
            },
            Err(e) => {
                debug!("[{}] got error while checking readiness: {:?}", node, e);
                tokio::time::sleep(Duration::from_millis(1000)).await;
                continue;
            }
        }

        info!("connecting to {}...", ws_uri);

        let ws_stream = match connect_async(&ws_uri).await {
            Ok((stream, _)) => {
                if !latch.is_closed() {
                    let _ = latch.send(()).await;
                }
                stream
            }
            Err(e) => {
                error!("WebSocket handshake for client {node} failed with {e}!");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };

        info!("connected to {}!", ws_uri);
        printed_waiting_msg = false;
        printed_offset_msg = false;

        let (_, mut receiver) = ws_stream.split();

        loop {
            tokio::select! {
                message = receiver.next() => {
                    match message {
                        Some(Ok(msg)) => {
                            let msg = msg.to_text().unwrap();

                            let mut parts = msg.split(',');
                            let ts = parts.next().unwrap().parse::<i64>().unwrap();
                            let id = parts.next().unwrap();
                            let state = parts.next().unwrap();

                            let msg_uniq_id = format!("{},{},{}", ts, id, state);

                            if seen.contains(&msg_uniq_id) {
                                // ignoring seen messages, since a message might come more than
                                // once
                                debug!("[{}] skipping msg, already seen: {:?}", node, msg);
                                continue
                            } else {
                                seen.insert(msg_uniq_id);
                            }

                            strategy.add_event(msg.to_string());

                            offset = Some(ts);

                            if state == "CONNECTED" {
                                strategy.on_connect(id.to_string(), ts);
                            } else if state == "DEAD" {
                                strategy.on_dead(id.to_string(), ts);

                            } else {
                                panic!("[{}] unknown state {}", node, state);
                            }
                        }
                        Some(Err(e)) => {
                            error!("[{}] got an error while reading from ws: {:?}", node, e);
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            break;
                        }
                        None => {
                            error!("[{}] got an None while reading from ws", node);
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            break;
                        }
                    }
                }
                _ = &mut rx => {
                    info!("closing {} ws!", node);
                    done = true;
                    break;
                }
            }
        }
    }

    strategy
}

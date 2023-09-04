use std::{sync::Arc, time::Duration};

use anyhow::{anyhow, Result};
use histogram::Histogram;
use rand::Rng;
use reqwest::{Client, StatusCode};
use tokio::sync::RwLock;
use tracing::{debug, error};

use crate::{config::Config, pulse::Pulse};
pub async fn checker(
    rx: kanal::AsyncReceiver<Pulse>,
    hist: Arc<RwLock<Histogram>>,
    config: Arc<Config>,
) -> i32 {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(300))
        .build()
        .unwrap();

    let mut checked = 0;

    loop {
        match rx.recv().await {
            Ok(pulse) => match get_ka(&client, &pulse, &config).await {
                Ok(ts) => {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as i64;
                    let delta = now - ts;

                    let hist_r = hist.write().await;
                    if let Err(e) = hist_r.increment(delta as u64, 1) {
                        error!("error incrementing hist: {:?} delta: {:?}", e, delta);
                    }

                    checked += 1;
                }

                Err(e) => error!("got error: {:?}", e),
            },
            Err(kanal::ReceiveError::SendClosed) => {
                return checked;
            }
            Err(_e) => {
                return checked;
            }
        }
    }
}

async fn get_ka(client: &Client, pulse: &Pulse, config: &Arc<Config>) -> Result<i64> {
    let nodes: Vec<String> = config
        .nodes
        .clone()
        .into_iter()
        .filter(|p| p != &pulse.node)
        .collect();

    let mut server_ts = -1;

    for _attempt in 0..100 {
        // pick a random node each attempt
        let node = &nodes[rand::thread_rng().gen_range(0..nodes.len())];
        let url = format!("http://{node}/ka/{}", pulse.id);

        match client.get(&url).send().await {
            Ok(result) => match result.status() {
                StatusCode::OK => match result.text().await {
                    Ok(ts) => match ts.parse::<i64>() {
                        Ok(ts) => {
                            if ts >= pulse.ts {
                                return Ok(ts);
                            } else {
                                server_ts = ts;
                                tokio::time::sleep(Duration::from_millis(10)).await;
                                continue;
                            }
                        }
                        Err(e) => error!("error parsing i64: {:?}", e),
                    },
                    Err(e) => error!("error getting text response: {:?}", e),
                },
                StatusCode::NOT_FOUND => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                }
                _ => error!("unexpected"),
            },
            Err(e) => {
                debug!("error doing get request: {:?}", e)
            }
        };
    }

    Err(anyhow!(
        "ka not found {} - sent to {} (server ts: {} pulse.ts: {} delta: {})",
        pulse.id,
        pulse.node,
        server_ts,
        pulse.ts,
        server_ts - pulse.ts,
    ))
}

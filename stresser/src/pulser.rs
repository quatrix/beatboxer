use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{anyhow, Result};
use kanal::AsyncReceiver;
use rand::Rng;
use tokio::task::JoinHandle;
use tracing::{debug, error};

use crate::{config::Config, pulse::Pulse};

pub async fn pulser(
    tx: Option<kanal::AsyncSender<Pulse>>,
    config: Arc<Config>,
    ids_rx: AsyncReceiver<String>,
) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(5000))
        .build()
        .unwrap();

    while let Ok(id) = ids_rx.recv().await {
        if let Ok((node, server_ts)) = send_pulse_to_random_node(&client, &id, &config.nodes).await
        {
            debug!("id: {} to node {} - ts: {}", id, node, server_ts);
            if let Some(tx) = &tx {
                if let Err(e) = tx
                    .send(Pulse {
                        id: id.to_string(),
                        node: node.to_string(),
                        ts: server_ts,
                    })
                    .await
                {
                    error!("error sending to channel: {:?}", e);
                }
            }
        } else {
            error!("failed sending pulse, id: {}", id);
        }
    }
}

pub fn another_pulser(
    ids: Vec<String>,
    interval: Duration,
    rounds: u32,
    nodes: Vec<String>,
) -> Vec<JoinHandle<()>> {
    let mut futures = Vec::new();

    for id in ids {
        let nodes = nodes.clone();
        let f = tokio::spawn(async move {
            let client = reqwest::Client::builder()
                .timeout(Duration::from_millis(300))
                .build()
                .unwrap();

            for _ in 0..rounds {
                let t0 = Instant::now();
                let _ = send_pulse_to_random_node(&client, &id, &nodes).await;
                let td = t0.elapsed();
                tokio::time::sleep(interval - td).await;
            }
        });

        futures.push(f);
    }

    futures
}

async fn send_pulse_to_random_node<'a>(
    client: &'a reqwest::Client,
    id: &'a str,
    nodes: &'a Vec<String>,
) -> Result<(&'a str, i64)> {
    for _ in 0..100 {
        let node = &nodes[rand::thread_rng().gen_range(0..nodes.len())];

        match client
            .post(format!("http://{node}/pulse/{id}"))
            .send()
            .await
        {
            Ok(response) => match response.text().await {
                Ok(text) => match text.parse::<i64>() {
                    Ok(ts) => {
                        return Ok((node, ts));
                    }
                    Err(e) => {
                        debug!("error doing post request: {:?}", e);
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                },
                Err(e) => {
                    debug!("error doing post request: {:?}", e);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            },
            Err(e) => {
                debug!("error doing post request: {:?}", e);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }

    Err(anyhow!("unable to send pulse"))
}

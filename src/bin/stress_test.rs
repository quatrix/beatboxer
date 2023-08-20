use clap::Parser;
use tracing::{error, info, warn};
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::{prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt};

use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    fs,
    sync::Arc,
    time::Duration,
};

use adjacent_pair_iterator::AdjacentPairIterator;
use anyhow::{anyhow, Result};
use futures::StreamExt;
use histogram::Histogram;
use rand::Rng;
use reqwest::{Client, StatusCode};
use tokio::{
    sync::{oneshot, RwLock},
    task::JoinError,
};
use tokio_tungstenite::connect_async;

#[derive(Parser, Debug)]
struct Config {
    #[arg(short, long, required = true)]
    nodes: Vec<String>,

    #[arg(long, default_value = "100")]
    pulse_workers: i32,

    #[arg(long, default_value = "100")]
    check_workers: i32,

    #[arg(long, default_value = "30000")]
    pulses_per_worker: i32,
}

const CHARSET: &str = "01234567890abcdef";

struct Pulse {
    pub id: String,
    pub node: String,
}

async fn pulser(tx: kanal::AsyncSender<Pulse>, config: Arc<Config>) {
    let client = reqwest::Client::new();

    for _i in 0..config.pulses_per_worker {
        let node = &config.nodes[rand::thread_rng().gen_range(0..config.nodes.len())];

        let id = random_string::generate(14, CHARSET);

        if let Err(e) = client
            .post(format!("http://{node}/pulse/{id}"))
            .send()
            .await
        {
            error!("error doing post request: {:?}", e);
        }

        if let Err(e) = tx
            .send(Pulse {
                id,
                node: node.to_string(),
            })
            .await
        {
            error!("error sending to channel: {:?}", e);
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

    let node = &nodes[rand::thread_rng().gen_range(0..nodes.len())];

    let url = format!("http://{node}/ka/{}", pulse.id);

    for _attempt in 0..200 {
        match client.get(&url).send().await {
            Ok(result) => match result.status() {
                StatusCode::OK => match result.text().await {
                    Ok(ts) => match ts.parse::<i64>() {
                        Ok(ts) => {
                            let now = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_millis() as i64;

                            let delta = now - ts;
                            if delta < 10000 {
                                return Ok(ts);
                            } else {
                                error!(
                                    "delta too large, it's probably stale: {} ({})",
                                    delta, pulse.id
                                );
                                tokio::time::sleep(Duration::from_millis(5)).await;
                                continue;
                            }
                        }
                        Err(e) => error!("error parsing i64: {:?}", e),
                    },
                    Err(e) => error!("error getting text response: {:?}", e),
                },
                StatusCode::NOT_FOUND => {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    continue;
                }
                _ => error!("unexpected"),
            },
            Err(e) => {
                error!("error doing get request: {:?}", e)
            }
        };
    }

    Err(anyhow!(
        "ka not found {} - sent to {}",
        pulse.id,
        pulse.node,
    ))
}

async fn checker(
    rx: kanal::AsyncReceiver<Pulse>,
    hist: Arc<RwLock<Histogram>>,
    config: Arc<Config>,
) {
    let client = reqwest::Client::new();

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
                }

                Err(e) => error!("got error: {:?}", e),
            },
            Err(kanal::ReceiveError::SendClosed) => {
                return;
            }
            Err(_e) => {
                return;
            }
        }
    }
}

async fn ws_client(
    mut rx: oneshot::Receiver<()>,
    ws_uri: String,
    expected_pairs: i32,
) -> Vec<String> {
    let mut waiting_death = HashMap::new();
    let mut connected_counter = HashMap::new();
    let mut dead_counter = HashMap::new();
    let mut issues = 0;
    let mut pairs = 0;
    let mut events = Vec::new();
    let mut ids = HashSet::new();

    let ws_stream = match connect_async(&ws_uri).await {
        Ok((stream, _)) => stream,
        Err(e) => {
            panic!("WebSocket handshake for client {ws_uri} failed with {e}!");
        }
    };

    let (_, mut receiver) = ws_stream.split();

    loop {
        tokio::select! {
            Some(Ok(msg)) = receiver.next() => {
                let msg = msg.to_text().unwrap();
                events.push(msg.to_string());

                let mut parts = msg.split(',');
                let _ts = parts.next().unwrap();
                let id = parts.next().unwrap();
                let state = parts.next().unwrap();

                ids.insert(id.to_string());

                if state == "CONNECTED" {
                    *waiting_death.entry(id.to_string()).or_insert(0) += 1;
                    *connected_counter.entry(id.to_string()).or_insert(0) += 1;
                } else if state == "DEAD" {
                    *dead_counter.entry(id.to_string()).or_insert(0) += 1;
                    // if got dead, there should be exactly one connected.
                    match waiting_death.entry(id.to_string()) {
                        Entry::Occupied(mut e) => {
                            if e.get() > &1 {
                                warn!("[{}] got DEAD but current connected count is {}", id, e.get());
                                issues += 1;
                            } else {
                                pairs += 1;
                                *e.get_mut() -= 1;
                            }
                        }
                        Entry::Vacant(_) =>  {
                            error!("[{}] got DEAD before CONNECTED", id);
                        }
                    }
                } else {
                    panic!("unknown state {}", state);
                }
            }
            _ = &mut rx => {
                info!("closing {} ws!", ws_uri);
                break;
            }
        }
    }

    for id in ids {
        let connected = match connected_counter.get(&id) {
            Some(c) => c,
            None => {
                issues += 1;
                warn!("missing connected! {}", id);
                continue;
            }
        };

        let dead = match dead_counter.get(&id) {
            Some(c) => c,
            None => {
                issues += 1;
                warn!("missing dead! {}", id);
                continue;
            }
        };

        if connected != dead {
            issues += 1;
            warn!("connected != dead {}", id);
            continue;
        }
    }

    if issues == 0 {
        info!("{} - for each connect there's a dead.", ws_uri);
    } else {
        error!("{} - oh no, {} issues.", ws_uri, issues);
    }

    if pairs == expected_pairs {
        info!("{} - got expected number of pairs {}", ws_uri, pairs)
    } else {
        error!(
            "{} - oh no, got {} pairs, expected {}",
            ws_uri, pairs, expected_pairs
        );
    }

    events
}

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "example_error_handling_and_dependency_injection=debug".into())
                .add_directive(LevelFilter::INFO.into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let config = Arc::new(Config::parse());
    let total_msgs = config.pulses_per_worker * config.pulse_workers;

    info!("starting stress test. config: {:?}", config);

    let mut futures = Vec::new();
    let mut ws_futures = Vec::new();
    let mut stop_ws = Vec::new();
    let (tx, rx) = kanal::bounded_async(100);
    let hist = Arc::new(RwLock::new(Histogram::new(0, 5, 20).unwrap()));

    let t0 = std::time::Instant::now();

    for node in &config.nodes {
        let (tx, rx) = oneshot::channel();

        ws_futures.push(tokio::spawn(ws_client(
            rx,
            format!("ws://{node}/updates"),
            total_msgs,
        )));

        stop_ws.push(tx);
    }

    for _ in 0..config.pulse_workers {
        let txc = tx.clone();
        let config = Arc::clone(&config);
        futures.push(tokio::spawn(pulser(txc, config)));
    }

    for _ in 0..config.check_workers {
        let rxc = rx.clone();
        let config = Arc::clone(&config);
        futures.push(tokio::spawn(checker(rxc, Arc::clone(&hist), config)));
    }

    drop(tx);
    drop(rx);

    futures::future::join_all(futures).await;
    let td = t0.elapsed().as_secs_f64();

    info!(
        "request rate: {} (total messages: {})",
        (total_msgs as f64) / td,
        total_msgs
    );

    let hist_r = hist.read().await;
    for p in hist_r.percentiles(&[25.0, 50.0, 90.0, 99.0, 99.9]).unwrap() {
        let b = p.bucket();
        info!(
            "p({:?}): low: {:?} high: {:?} count: {:?}",
            p.percentile(),
            b.low(),
            b.high(),
            b.count()
        );
    }

    tokio::time::sleep(Duration::from_secs(30)).await;

    for tx in stop_ws {
        let _ = tx.send(());
    }

    info!("waiting for ws clients to finish");
    let res = futures::future::join_all(ws_futures)
        .await
        .into_iter()
        .collect::<Result<Vec<Vec<String>>, JoinError>>()
        .unwrap();

    for (i, events) in res.iter().enumerate() {
        let filename = format!("/tmp/ws_{i}.events");
        match fs::write(&filename, events.join("\n")) {
            Ok(_) => info!("wrote events to {filename}"),
            Err(e) => error!("can't write events to {filename}: {:?}", e),
        };
    }

    let iterator = res.adjacent_pairs();

    info!("comparing the events from ws clients");

    for (a, b) in iterator {
        if a != b {
            error!("diff events");
        } else {
            info!("events are equal");
        }
    }
}

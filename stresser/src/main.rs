use clap::Parser;
use stresser::checker::checker;
use stresser::config::Config;
use stresser::id_generation::generate_ids;
use stresser::pulser::pulser;
use stresser::strategies::original::Original;
use stresser::strategies::Strategy;
use stresser::ws_client::ws_client;
use tokio::sync::mpsc;
use tracing::{error, info};
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::{prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt};

use std::{fs, sync::Arc, time::Duration};

use adjacent_pair_iterator::AdjacentPairIterator;
use anyhow::Result;
use histogram::Histogram;
use tokio::{
    sync::{oneshot, RwLock},
    task::JoinError,
};

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

    info!("starting stress test. config: {:?}", config);

    let mut pulses_futures = Vec::new();
    let mut checkers_futures = Vec::new();
    let mut ws_futures = Vec::new();
    let mut stop_ws = Vec::new();
    let mut latch_ws = Vec::new();
    let (tx, rx) = kanal::bounded_async(1000);
    let hist = Arc::new(RwLock::new(Histogram::new(0, 5, 20).unwrap()));

    let t0 = std::time::Instant::now();

    for node in &config.nodes {
        let (tx, rx) = oneshot::channel();
        let (latch_tx, latch_rx) = mpsc::channel(10000); // FIXME: close channel after initial sync
        let strategy = Original::new(node.to_string(), Arc::clone(&config));
        ws_futures.push(tokio::spawn(ws_client(
            rx,
            node.to_string(),
            latch_tx,
            strategy,
        )));

        stop_ws.push(tx);
        latch_ws.push(latch_rx);
    }

    // waiting for clients to connect
    for mut rx in latch_ws {
        let _ = rx.recv().await;
    }

    info!("all ws ready!");

    let (ids_rx, _) = generate_ids(&config);

    for _ in 0..config.pulse_workers {
        let txc = tx.clone();
        let config = Arc::clone(&config);
        pulses_futures.push(tokio::spawn(pulser(Some(txc), config, ids_rx.clone())));
    }

    for _ in 0..config.check_workers {
        let rxc = rx.clone();
        let config = Arc::clone(&config);
        checkers_futures.push(tokio::spawn(checker(rxc, Arc::clone(&hist), config)));
    }

    drop(tx);
    drop(rx);

    futures::future::join_all(pulses_futures).await;
    let checked = futures::future::join_all(checkers_futures).await;

    let checked: i32 = checked
        .into_iter()
        .collect::<Result<Vec<i32>, JoinError>>()
        .unwrap()
        .iter()
        .sum();

    if (checked as usize) == config.total_ids {
        info!("checked ids count: {checked} as expected");
    } else {
        error!(
            "checked ids count: {} != {} omg!",
            checked, config.total_ids
        );
    }

    let td = t0.elapsed().as_secs_f64();

    info!(
        "request rate: {} (total messages: {})",
        (checked as f64) / td,
        checked,
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

    // waiting for death
    tokio::time::sleep(Duration::from_secs(40)).await;

    for tx in stop_ws {
        let _ = tx.send(());
    }

    info!("waiting for ws clients to finish");
    let res = futures::future::join_all(ws_futures)
        .await
        .into_iter()
        .map(|s| s.unwrap().get_events())
        .collect::<Vec<Vec<String>>>();

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
            info!("events are equal ✅");
        }
    }
}

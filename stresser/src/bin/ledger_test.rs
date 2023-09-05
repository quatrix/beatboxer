use clap::Parser;
use itertools::Itertools;
use stresser::config::Config;
use stresser::event::{iso8601, Event, EventType};
use stresser::id_generation::generate_ids;
use stresser::pulser::pulser;
use stresser::scheduler::group_ledgers_by_id;
use stresser::strategies::ledger::Ledger;
use stresser::strategies::Strategy;
use stresser::ws_client::ws_client;
use tokio::sync::mpsc;
use tracing::{error, info, warn};
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::{prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt};

use std::collections::HashMap;
use std::fs;
use std::iter::zip;
use std::{sync::Arc, time::Duration};
use tokio::sync::oneshot;

#[tokio::main(flavor = "multi_thread", worker_threads = 20)]
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
    let mut ws_futures = Vec::new();
    let mut stop_ws = Vec::new();
    let mut latch_ws = Vec::new();

    for node in &config.nodes {
        let (tx, rx) = oneshot::channel();
        let (latch_tx, latch_rx) = mpsc::channel(10000); // FIXME: close channel after initial sync
        let strategy = Ledger::new(node);
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

    let (ids_rx, ledger_rx) = generate_ids(&config);

    for _ in 0..config.pulse_workers {
        let config = Arc::clone(&config);
        pulses_futures.push(tokio::spawn(pulser(None, config, ids_rx.clone())));
    }

    /*
    let ids = (0..config.total_ids)
        .map(|i| format!("{:08x}", i))
        .collect();
    let interval = Duration::from_millis(config.time_between_beats_ms);
    let rounds = config.rounds;
    let nodes = config.nodes.clone();
    let pulses_futures = another_pulser(ids, interval, rounds, nodes);
    */

    futures::future::join_all(pulses_futures).await;

    // waiting for death
    info!("waiting for death...");
    tokio::time::sleep(Duration::from_secs(120)).await;

    for tx in stop_ws {
        let _ = tx.send(());
    }

    let mut actual = HashMap::new();

    info!("waiting for ws clients to finish");
    for r in futures::future::join_all(ws_futures).await.into_iter() {
        match r {
            Ok(r) => {
                actual.insert(r.get_name(), r.get_grouped_events());
                let filename = format!("/tmp/ws_{}.log", r.get_name());
                info!("writing ws msgs into {:?}", filename);
                fs::write(&filename, r.get_events().join("\n")).unwrap();
            }
            Err(e) => error!("couldn't get results!: {:?}", e),
        }
    }

    let expected = group_ledgers_by_id(&ledger_rx).await;

    if compare_results(expected, actual) {
        info!("all good!");
    } else {
        warn!("not so goood.")
    }
}

fn compare_results(
    expected: HashMap<String, Vec<Event>>,
    actual: HashMap<String, HashMap<String, Vec<Event>>>,
) -> bool {
    let mut same = 0;
    let mut diff = 0;
    let mut not_found = 0;
    let mut results: HashMap<String, HashMap<DiffResult, u32>> = HashMap::new();

    for (id, ledger) in &expected {
        for (name, actual) in &actual {
            if let Some(actual_ledger) = actual.get(id) {
                let r = is_same(name, ledger, actual_ledger);

                if r == DiffResult::Ok {
                    same += 1;
                } else {
                    diff += 1;
                }

                *results
                    .entry(name.to_string())
                    .or_insert(HashMap::new())
                    .entry(r)
                    .or_insert(0) += 1;
            } else {
                error!("[{}] - {} not found!", name, id);
                not_found += 1;
            }
        }
    }

    info!("----- REPORT ----");

    info!("same: {} diff: {} not_found: {}", same, diff, not_found);

    for (name, results) in results {
        info!("results for {}", name);

        for (r_key, counts) in results {
            info!("\t{:?} -> {}", r_key, counts);
        }
    }

    diff == 0 && not_found == 0
}

fn print_expected_and_actual(name: &str, expected: &Vec<&Event>, actual: &Vec<Event>) {
    error!("[{}] - expected:", name);

    for e in expected {
        error!("[{}] - \t{:?} 🐠", name, e)
    }

    error!("[{}] - actual:", name);
    for e in actual {
        error!("[{}] - \t{:?} 🍣", name, e)
    }
}

#[derive(Eq, Hash, PartialEq, Debug)]
enum DiffResult {
    ConnectTooLate,
    ConnectTooSoon,
    DeadTooSoon,
    DeadBeforeExpected,
    DeadTooLate,
    MissingEvents,
    ExtraEvents,
    UnexpectedEvent,
    TwoConnects,
    TwoDeads,
    Wat,
    Ok,
}

fn is_same(name: &str, expected: &[Event], actual: &Vec<Event>) -> DiffResult {
    let expected = expected
        .iter()
        .filter(|e| e.event != EventType::Beat && e.event != EventType::Skip)
        .collect::<Vec<&Event>>();

    if expected.len() != actual.len() {
        error!("[{}] - not the same events!", name);

        //print_expected_and_actual(name, &expected, actual);

        while let Some((a_1, a_2)) = actual.iter().next_tuple() {
            if a_1.event == EventType::Connect && a_1.event == a_2.event {
                return DiffResult::TwoConnects;
            }

            if a_1.event == EventType::Dead && a_1.event == a_2.event {
                return DiffResult::TwoDeads;
            }
        }

        if expected.len() > actual.len() {
            return DiffResult::MissingEvents;
        } else {
            return DiffResult::ExtraEvents;
        }
    }

    for (e, a) in zip(&expected, actual) {
        if e.event != a.event {
            error!("[{}] - expected {:?} got {:?}", name, e.event, a.event);

            //print_expected_and_actual(name, &expected, actual);
            return DiffResult::UnexpectedEvent;
        }

        if e.event == EventType::Connect {
            // if this is a connection event, soon after we should get an actual
            // connection event
            match a.ts.duration_since(e.ts) {
                Ok(since_connect) => {
                    if since_connect > Duration::from_secs(11) {
                        error!(
                            "[{}] - connection event came too late [{:?}], expected {:?} actual {:?}",
                            name, since_connect, iso8601(&e.ts), iso8601(&a.ts)
                        );

                        //print_expected_and_actual(name, &expected, actual);
                        return DiffResult::ConnectTooLate;
                    }
                }
                Err(d) => {
                    if d.duration() > Duration::from_millis(100) {
                        error!("[{}] - actual connect came before expected?? {:?}", name, d);
                        //print_expected_and_actual(name, &expected, actual);
                        return DiffResult::ConnectTooSoon;
                    }
                }
            }
        } else if e.event == EventType::Dead {
            // if device died, we expect to get a dead event no sooner than 30s,
            // but not later than 50s
            match a.ts.duration_since(e.ts) {
                Ok(since_dead) => {
                    if since_dead > Duration::from_secs(40) {
                        error!(
                            "[{}] - got dead event too late [{:?}], device died at {} got dead event at {}",
                            name, since_dead, iso8601(&e.ts), iso8601(&a.ts)
                        );
                        //print_expected_and_actual(name, &expected, actual);

                        return DiffResult::DeadTooLate;
                    }

                    if since_dead < Duration::from_secs(19) {
                        error!(
                            "[{}] - got dead event too soon [{:?}], device died at {} got dead event at {}",
                            name, since_dead, iso8601(&e.ts), iso8601(&a.ts)
                        );

                        //print_expected_and_actual(name, &expected, actual);
                        return DiffResult::DeadTooSoon;
                    }
                }
                Err(d) => {
                    error!("[{}] - actual dead came before expected?? {:?}", name, d);
                    return DiffResult::DeadBeforeExpected;
                }
            }
        } else {
            error!("[{}] - got unexpected event {:?}", name, e.event);
            return DiffResult::Wat;
        }
    }

    DiffResult::Ok
}

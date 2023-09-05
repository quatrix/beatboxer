use anyhow::{anyhow, Result};
use rand::Rng;
use std::{
    collections::{hash_map::Entry, HashMap},
    time::{Duration, SystemTime},
};

use chrono::{DateTime, Utc};
use reqwest::StatusCode;
use tracing::{debug, error, info, warn};

#[derive(Debug, Eq, PartialEq)]
struct Node {
    name: String,
    url: String,
}

pub async fn start_the_chaos(nodes: Vec<String>, death_probability: f32, chaos_interval: Duration) {
    let nodes = nodes
        .iter()
        .map(|n| Node {
            name: n.to_string(),
            url: format!("http://{}", n),
        })
        .collect::<Vec<Node>>();

    let mut node_alive: HashMap<&str, bool> = HashMap::new();

    info!("initializing...");
    for node in &nodes {
        node_alive.insert(
            &node.name,
            if is_alive(node).await {
                info!("node {} is alive!", node.name);
                true
            } else {
                warn!("node {} is dead!", node.name);
                false
            },
        );
    }

    info!("starting the chaos...");
    loop {
        let now = SystemTime::now();

        let now_millis = now
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let time_str = format!("[{}] {}", now_millis, iso8601(&now));

        for node in &nodes {
            if is_alive(node).await {
                if let Entry::Occupied(mut s) = node_alive.entry(&node.name) {
                    if !s.get() {
                        info!(
                            "{} node {} was dead, now it's alive 😊",
                            time_str, node.name
                        );
                        s.insert(true);
                    }
                }
                if should_kill(death_probability) && can_kill(node, &nodes).await {
                    match kill(node).await {
                        Ok(_) => info!("{} killed node: {} 🔫", time_str, node.name),
                        Err(e) => error!("{} failed to kill node {}: {:?}", time_str, node.name, e),
                    }
                }
            } else if let Entry::Occupied(mut s) = node_alive.entry(&node.name) {
                if *s.get() {
                    info!(
                        "{} node {} was alive, now it's dead 💀",
                        time_str, node.name
                    );
                    s.insert(false);
                }
            }
        }

        tokio::time::sleep(chaos_interval).await;
    }
}

fn iso8601(st: &std::time::SystemTime) -> String {
    let dt: DateTime<Utc> = (*st).into();
    format!("{}", dt.format("%+"))
}

async fn is_alive(node: &Node) -> bool {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(5000))
        .build()
        .unwrap();

    let url = format!("{}/ready", node.url);
    match client.get(&url).send().await {
        Ok(result) => matches!(result.status(), StatusCode::OK),
        Err(e) => {
            debug!("got error while checking health: {:?}", e);
            false
        }
    }
}

fn rand() -> f32 {
    let mut rng = rand::thread_rng();
    rng.gen()
}

fn should_kill(probability: f32) -> bool {
    rand() < probability
}

async fn can_kill(node: &Node, nodes: &[Node]) -> bool {
    let rest = nodes.iter().filter(|n| *n != node).collect::<Vec<&Node>>();

    for node in rest {
        if is_alive(node).await {
            return true;
        }
    }

    false
}

async fn kill(node: &Node) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(5000))
        .build()
        .unwrap();

    let url = format!("{}/internal/die", node.url);
    let _ = client.get(&url).send().await;

    if !is_alive(node).await {
        Ok(())
    } else {
        Err(anyhow!(
            "tried killing node {} but it's still alive??",
            node.name
        ))
    }
}

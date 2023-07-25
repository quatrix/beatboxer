use axum::async_trait;

use dashmap::DashMap;
use keep_alive_sync::keep_alive_sync_client::KeepAliveSyncClient;
use keep_alive_sync::ForwardRequest;
use tokio::sync::broadcast;
use tracing::{debug, error, info};

#[derive(Clone, Debug)]
struct KeepAliveUpdate {
    id: String,
    ts: i64,
}

pub mod keep_alive_sync {
    tonic::include_proto!("keep_alive_sync");
}

pub struct KeepAlive {
    nodes: Vec<String>,
    txs: DashMap<
        String,
        (
            kanal::AsyncSender<KeepAliveUpdate>,
            kanal::AsyncReceiver<KeepAliveUpdate>,
        ),
    >,
    keep_alives: DashMap<String, i64>,
}

#[async_trait]
pub trait KeepAliveTrait {
    async fn pulse(&self, id: &str);
    async fn get(&self, id: &str) -> Option<i64>;
}

impl KeepAlive {
    pub fn new(nodes: Vec<String>) -> KeepAlive {
        let txs = DashMap::new();
        for node in nodes.clone() {
            let (tx, rx) = kanal::bounded_async(1024);
            txs.insert(node, (tx, rx));
        }

        KeepAlive {
            nodes,
            txs,
            keep_alives: DashMap::new(),
        }
    }

    pub fn update(&self, id: String, ts: i64) {
        debug!("got an update from node: {} {}", id, ts);
        self.keep_alives.insert(id, ts);
    }

    pub fn connect_to_nodes(&self) {
        for node in self.nodes.clone() {
            let rx = self.txs.get(&node).unwrap().1.clone();
            tokio::spawn(async move {
                loop {
                    info!("Connecting to {}", node);
                    match KeepAliveSyncClient::connect(format!("http://{}", node)).await {
                        Ok(mut client) => {
                            info!("Connected to {}", node);
                            loop {
                                match rx.recv().await {
                                    Ok(ka) => {
                                        let request = tonic::Request::new(ForwardRequest {
                                            id: ka.id,
                                            ts: ka.ts,
                                        });

                                        match client.forward_ka(request).await {
                                            Ok(response) => {
                                                debug!("Response: {:?}", response);
                                            }
                                            Err(e) => {
                                                error!("Error forwarding: {:?}", e);
                                                break;
                                            }
                                        };
                                    }
                                    Err(e) => {
                                        error!("Error recv from channel: {:?}", e);
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            error!("Error connecting: {:?}", e);
                            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                        }
                    }
                }
            });
        }
    }
}

#[async_trait]
impl KeepAliveTrait for KeepAlive {
    async fn get(&self, id: &str) -> Option<i64> {
        self.keep_alives.get(id).map(|ts| *ts)
    }

    async fn pulse(&self, id: &str) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        self.keep_alives.insert(id.to_string(), now);

        for node in self.nodes.clone() {
            let tx = self.txs.get(&node).unwrap().0.clone();
            let ka = KeepAliveUpdate {
                id: id.to_string(),
                ts: now,
            };
            tx.send(ka).await.unwrap();
        }
    }
}

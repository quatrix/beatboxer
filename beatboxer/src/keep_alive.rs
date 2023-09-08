pub mod client;
pub mod cluster_status;
pub mod constants;
pub mod server;
pub mod types;

use anyhow::Result;
use serde::Serialize;
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc::{error::TrySendError, Receiver, Sender};

use axum::async_trait;

use tokio::sync::RwLock;
use tracing::{debug, error, info};

use crate::storage::Storage;
use crate::{keep_alive::cluster_status::ClusterStatus, storage::memory::zset::DeviceState};

use self::types::{Event, EventType, KeepAliveUpdate, Message};

pub type SenderChannels = Arc<RwLock<Vec<(String, Sender<Message>)>>>;

pub struct KeepAlive {
    listen_addr: String,
    listen_port: u16,
    nodes: Vec<String>,
    keep_alives: Arc<dyn Storage + Send + Sync>,
    txs: SenderChannels,
    cluster_status: Arc<ClusterStatus>,
}

#[async_trait]
pub trait KeepAliveTrait {
    async fn pulse(&self, id: &str) -> i64;
    async fn get(&self, id: &str) -> Option<DeviceState>;
    async fn subscribe(&self, offset: Option<i64>) -> Result<Receiver<Event>>;
    async fn is_ready(&self) -> bool;
    async fn cluster_status(&self) -> ClusterStatusResponse;
}

#[derive(Serialize)]
pub struct CurrentNode {
    keys: usize,
}

#[derive(Serialize)]
pub struct ClusterStatusResponse<'a> {
    cluster_status: &'a ClusterStatus,
    current_node: CurrentNode,
}

fn is_dev() -> bool {
    // FIXME: this is a bit of a hack,
    // if env variable IS_DEV is set, it means
    // we're running on one machine, so the policy
    // to remove myself is to look for hostname:port
    //
    // but if it's not IS_DEV then we remove nodes
    // that starts with the hostname.
    std::env::var_os("IS_DEV").is_some()
}

fn remove_myself(nodes: Vec<String>, listen_port: u16) -> Vec<String> {
    let mut nodes = nodes.clone();
    let hostname = gethostname::gethostname().into_string().unwrap();
    info!("node hostname: {}", hostname);

    if is_dev() {
        let myself = format!("{}:{}", hostname, listen_port);

        info!("is_dev: filtering out {}", myself);
        nodes.retain(|n| *n != myself);
    } else {
        info!(
            "isn't dev: filtering out node that starts with {}",
            hostname
        );
        nodes.retain(|n| !n.starts_with(&hostname));
    }

    info!("peers: {:?}", nodes);
    nodes
}

impl KeepAlive {
    pub fn new(
        listen_addr: &str,
        listen_port: u16,
        nodes: Vec<String>,
        storage: Arc<dyn Storage + Send + Sync>,
        txs: SenderChannels,
    ) -> KeepAlive {
        let nodes = remove_myself(nodes, listen_port);

        KeepAlive {
            listen_addr: listen_addr.to_string(),
            listen_port,
            nodes: nodes.clone(),
            keep_alives: storage,
            txs,
            cluster_status: Arc::new(ClusterStatus::new(nodes)),
        }
    }

    fn health_checker(&self) {
        info!("starting health_checker");

        let txsc = Arc::clone(&self.txs);

        tokio::spawn(async move {
            loop {
                let mut gc_senders = false;
                {
                    let txs = txsc.read().await;

                    for (addr, sender) in txs.iter() {
                        let pressure = (sender.max_capacity() - sender.capacity()) as f64;
                        metrics::gauge!("channel_pressure", pressure, "channel_type" => "server", "addr" => addr.to_string());

                        match sender.try_send(Message::Ping) {
                            Ok(_) => {}
                            Err(TrySendError::Closed(_)) => {
                                error!("[{}] (ping) Channel closed.", addr);
                                gc_senders = true;
                            }
                            Err(TrySendError::Full(_)) => {
                                error!("[{}] (ping) Channel full.", addr);
                                metrics::increment_counter!("channel_full", "op" => "ping", "addr" => addr.clone());
                            }
                        };
                    }
                }

                if gc_senders {
                    info!("cleaninig dead senders");
                    let mut txs = txsc.write().await;
                    txs.retain(|(_, tx)| !tx.is_closed());
                }

                // FIXME: make keep alive interval configurable
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }

    pub fn start_background_tasks(&self) {
        self.keep_alives
            .start_background_tasks(Arc::clone(&self.cluster_status));
    }
}

#[async_trait]
impl KeepAliveTrait for KeepAlive {
    async fn get(&self, id: &str) -> Option<DeviceState> {
        self.keep_alives.get(id).await
    }

    async fn pulse(&self, id: &str) -> i64 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let is_connection_event = match self.keep_alives.get(id).await {
            None => {
                debug!(
                    "[pulse] this is a CONNECTED event. id: {} first time KA (ts: {})",
                    id, now
                );
                true
            }
            Some(ds) => {
                if ds.state == EventType::Dead {
                    debug!(
                        "[pulse] this is a CONNECTED event. because previous state is Dead. id: {} prev state: {:?} (ts: {})",
                        id, ds, now,
                    );
                    true
                } else {
                    false
                }
            }
        };

        self.keep_alives.set(id, now, is_connection_event).await;

        let ka = KeepAliveUpdate {
            id: id.to_string(),
            ts: now,
            is_connection_event,
        };

        let txs = self.txs.read().await;

        for (addr, tx) in txs.iter().filter(|(_, tx)| !tx.is_closed()) {
            debug!("sending KA update to channel {} -> {:?}", addr, ka);
            match tx.try_send(Message::KeepAliveUpdate(ka.clone())) {
                Ok(_) => {}
                Err(TrySendError::Closed(_)) => error!("[{}] (KA) Channel closed.", addr),
                Err(TrySendError::Full(_)) => {
                    error!("[{}] (KA) Channel full.", addr);
                    metrics::increment_counter!("channel_full", "op" => "ka", "addr" => addr.clone());
                }
            }
        }

        now
    }

    async fn subscribe(&self, offset: Option<i64>) -> Result<Receiver<Event>> {
        self.keep_alives.subscribe(offset).await
    }

    async fn is_ready(&self) -> bool {
        self.cluster_status.is_ready()
    }

    async fn cluster_status(&self) -> ClusterStatusResponse {
        ClusterStatusResponse {
            cluster_status: &self.cluster_status,
            current_node: CurrentNode {
                keys: self.keep_alives.len().await,
            },
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{keep_alive::types::EventType, storage::memory::InMemoryStorage};

    use super::*;

    #[tokio::test]
    async fn test_pulse_stores_ka() {
        let storage = Arc::new(InMemoryStorage::new(10));
        let ka = KeepAlive::new("127.0.0.1", 6666, vec![], storage);

        let id = "vova666";

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        assert!(ka.get(id).await.is_none());
        ka.pulse("vova666").await;

        let actual = ka.get(id).await.unwrap();

        assert!(actual >= now);
    }

    #[tokio::test]
    async fn test_pulse_publishes_to_all_txs() {
        let storage = Arc::new(InMemoryStorage::new(10));
        let ka = KeepAlive::new("127.0.0.1", 6666, vec![], storage);
        let mut rxs = Vec::new();

        rxs.push(ka.subscribe_to_commands("1.0.0.0:0".parse().unwrap()).await);
        rxs.push(ka.subscribe_to_commands("2.0.0.0:0".parse().unwrap()).await);

        let id = "vova666";

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        ka.pulse("vova666").await;

        for mut rx in rxs {
            let v = rx.recv().await.unwrap();
            match v {
                Message::KeepAliveUpdate(v) => {
                    assert_eq!(v.id, id);
                    assert!(v.ts >= now);
                    assert!(v.is_connection_event);
                }
                _ => panic!("shouldn't get here"),
            }
        }
    }

    #[tokio::test]
    async fn test_pulse_publishes_events() {
        let storage = Arc::new(InMemoryStorage::new(10));
        let cluster_status = ClusterStatus::new(vec![]);
        storage.start_background_tasks(Arc::new(cluster_status));

        let ka = KeepAlive::new("127.0.0.1", 6666, vec![], storage);
        let mut events_rxs = Vec::new();

        events_rxs.push(ka.subscribe(None).await.unwrap());
        events_rxs.push(ka.subscribe(None).await.unwrap());

        let id = "vova666";

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        ka.pulse("vova666").await;

        for mut rx in events_rxs {
            let event = rx.recv().await.unwrap();
            assert_eq!(event.id, id);
            assert!(event.ts >= now);
            assert_eq!(event.typ, EventType::Connected);
        }
    }

    #[tokio::test]
    async fn test_health_checks_sending_pings() {
        let storage = Arc::new(InMemoryStorage::new(10));
        let ka = KeepAlive::new("127.0.0.1", 6666, vec![], storage);
        ka.health_checker();

        let mut rxs = Vec::new();
        rxs.push(ka.subscribe_to_commands("1.0.0.0:0".parse().unwrap()).await);
        rxs.push(ka.subscribe_to_commands("2.0.0.0:0".parse().unwrap()).await);

        for mut rx in rxs {
            let v = rx.recv().await.unwrap();
            match v {
                Message::Ping => {}
                _ => panic!("shouldn't get here"),
            }
        }
    }
}

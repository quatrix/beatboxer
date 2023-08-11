pub mod client;
pub mod constants;
pub mod server;
pub mod types;

use anyhow::Result;
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc::{error::TrySendError, Receiver, Sender};

use axum::async_trait;

use tokio::sync::RwLock;
use tracing::{debug, error, info};

use crate::storage::Storage;

use self::{
    constants::DEAD_DEVICE_TIMEOUT,
    types::{Event, KeepAliveUpdate, Message},
};

type SenderChannels = Arc<RwLock<Vec<(String, Sender<Message>)>>>;

pub struct KeepAlive {
    server_addr: String,
    nodes: Vec<String>,
    keep_alives: Arc<dyn Storage + Send + Sync>,
    txs: SenderChannels,
}

#[async_trait]
pub trait KeepAliveTrait {
    async fn pulse(&self, id: &str);
    async fn get(&self, id: &str) -> Option<i64>;
    async fn subscribe(&self, offset: Option<i64>) -> Result<Receiver<Event>>;
}

impl KeepAlive {
    pub fn new(
        server_addr: String,
        nodes: Vec<String>,
        storage: Arc<dyn Storage + Send + Sync>,
    ) -> KeepAlive {
        KeepAlive {
            server_addr,
            nodes,
            keep_alives: storage,
            txs: Arc::new(RwLock::new(Vec::new())),
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
                        metrics::gauge!("channel_pressure", (sender.max_capacity()-sender.capacity()) as f64, "addr" => addr.to_string());

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
}

#[async_trait]
impl KeepAliveTrait for KeepAlive {
    async fn get(&self, id: &str) -> Option<i64> {
        self.keep_alives.get(id).await
    }

    async fn pulse(&self, id: &str) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let is_connection_event = match self.keep_alives.get(id).await {
            None => true,
            Some(ts) => now - ts > (DEAD_DEVICE_TIMEOUT.as_millis() as i64),
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
    }

    async fn subscribe(&self, offset: Option<i64>) -> Result<Receiver<Event>> {
        self.keep_alives.subscribe(offset).await
    }
}

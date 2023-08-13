use anyhow::Result;
use std::sync::Arc;

use tokio::sync::{
    mpsc::{self, error::TrySendError, Receiver, Sender},
    RwLock,
};
use tracing::{error, info};

use crate::keep_alive::types::Event;

pub struct NotificationDispatcher {
    txs: Arc<RwLock<Vec<Sender<Event>>>>,
}

impl NotificationDispatcher {
    pub fn new() -> Self {
        NotificationDispatcher {
            txs: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub async fn add_subscriber(
        &self,
        buffer_size: usize,
        initial_payload: Option<Vec<Event>>,
    ) -> Result<Receiver<Event>> {
        let (tx, rx) = mpsc::channel(buffer_size);

        if let Some(events) = initial_payload {
            for event in events {
                match tx.try_send(event) {
                    Ok(_) => {}
                    Err(TrySendError::Closed(_)) => {
                        error!("(subscriber-init) Channel closed.");
                        return Err(anyhow::Error::msg(
                            "channel closed while doing initial send",
                        ));
                    }
                    Err(TrySendError::Full(_)) => {
                        error!("(subscriber-init) Channel full.");
                        metrics::increment_counter!("channel_full", "op" => "notify-init");
                        return Err(anyhow::Error::msg(
                            "channel got full while doing initial send",
                        ));
                    }
                }
            }
        }
        {
            let mut txs = self.txs.write().await;
            txs.push(tx);
        }

        Ok(rx)
    }

    pub async fn notify(&self, event: &Event) {
        let mut gc_senders = false;

        {
            let txs = self.txs.read().await;
            for tx in txs.iter() {
                match tx.try_send(event.clone()) {
                    Ok(_) => {}
                    Err(TrySendError::Closed(_)) => {
                        error!("(subscriber) Channel closed.");
                        gc_senders = true;
                    }
                    Err(TrySendError::Full(e)) => {
                        error!("(subscriber) Channel full. dropping event {:?}", e);
                        metrics::increment_counter!("channel_full", "op" => "notify");
                    }
                }
            }
        }

        if gc_senders {
            info!("cleaninig dead subscribers");
            let mut txs = self.txs.write().await;
            txs.retain(|tx| !tx.is_closed());
        }
    }
}

impl Default for NotificationDispatcher {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod test {
    use crate::keep_alive::types::EventType;

    use super::*;

    #[tokio::test]
    async fn test_basics() {
        let event = Event {
            id: "vova666".to_string(),
            ts: 1337,
            typ: EventType::Connected,
        };

        let nd = NotificationDispatcher::new();

        let mut rx0 = nd.add_subscriber(5, None).await.unwrap();
        let mut rx1 = nd.add_subscriber(5, None).await.unwrap();

        nd.notify(&event).await;

        let actual0 = rx0.recv().await.unwrap();
        let actual1 = rx1.recv().await.unwrap();

        assert_eq!(actual0, event);
        assert_eq!(actual1, event);
    }
}

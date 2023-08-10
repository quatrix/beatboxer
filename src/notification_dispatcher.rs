use std::sync::Arc;

use tokio::sync::{
    mpsc::{self, error::SendError, Receiver, Sender},
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
    ) -> Receiver<Event> {
        let (tx, rx) = mpsc::channel(buffer_size);

        if let Some(events) = initial_payload {
            for event in events {
                tx.send(event).await.expect("can't event to tx");
            }
        }
        {
            let mut txs = self.txs.write().await;
            txs.push(tx);
        }

        rx
    }

    pub async fn notify(&self, event: &Event) {
        let mut gc_senders = false;

        {
            let txs = self.txs.read().await;
            for tx in txs.iter() {
                match tx.send(event.clone()).await {
                    Ok(_) => {}
                    Err(SendError(e)) => {
                        error!("(subscriber) error sending: {:?}", e);
                        gc_senders = true;
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

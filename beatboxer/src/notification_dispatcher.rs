use anyhow::Result;
use lru::LruCache;
use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use tokio::sync::{
    mpsc::{self, error::TrySendError, Receiver, Sender},
    RwLock,
};
use tracing::{error, info};

use crate::keep_alive::types::{Event, EventType};
use crate::storage::memory::events::Events;

pub struct NotificationDispatcher {
    txs: Arc<RwLock<Vec<Sender<Event>>>>,
    events_history: Arc<Events>,
    sent_events: RwLock<LruCache<Event, ()>>,
}

impl NotificationDispatcher {
    pub fn new(events_history: Arc<Events>) -> Self {
        NotificationDispatcher {
            txs: Arc::new(RwLock::new(Vec::new())),
            events_history,
            sent_events: RwLock::new(LruCache::new(NonZeroUsize::new(10_000).unwrap())),
        }
    }

    pub async fn add_subscriber(
        &self,
        buffer_size: usize,
        offset: Option<i64>,
    ) -> Result<Receiver<Event>> {
        let (tx, rx) = mpsc::channel(buffer_size);

        let last_offset = if let Some(offset) = offset {
            write_events_since_offset_to_channel(&self.events_history, offset, &tx).await?
        } else {
            None
        };

        {
            let mut txs = self.txs.write().await;

            // some new events might have been added while we were synching
            // sending this delta while holding a write lock makes sure no events
            // get lost, but some these events might get published more than once.
            if let Some(last_offset) = last_offset {
                write_events_since_offset_to_channel(&self.events_history, last_offset, &tx)
                    .await?;
            }

            txs.push(tx);
        }

        Ok(rx)
    }

    pub async fn notify(&self, event: &Event) {
        if event.typ == EventType::Dead {
            // since we get Dead events from all nodes
            // we don't want to notify for each of those,
            // but only for the first we see.
            {
                let mut sent_events = self.sent_events.write().await;
                if sent_events.contains(&event) {
                    return;
                } else {
                    sent_events.put(event.clone(), ());
                }
            }
        }

        let mut gc_senders = false;

        {
            let txs = self.txs.read().await;
            for tx in txs.iter() {
                match tx.try_send(event.clone()) {
                    Ok(_) => {
                        //info!("sending notification: {:?} to {:?}", event, tx);
                    }
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

    pub fn monitor(&self) {
        let txs = Arc::clone(&self.txs);

        tokio::spawn(async move {
            loop {
                {
                    let txs = txs.read().await;
                    for tx in txs.iter() {
                        let pressure = (tx.max_capacity() - tx.capacity()) as f64;
                        metrics::gauge!("channel_pressure", pressure, "channel_type" => "ws");
                    }
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }
}

async fn write_events_since_offset_to_channel(
    events_history: &Arc<Events>,
    offset: i64,
    tx: &Sender<Event>,
) -> Result<Option<i64>> {
    let events = events_history.events_since_ts(offset).await;
    let mut last_offset = offset;

    for event in events {
        last_offset = event.ts;
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

    Ok(Some(last_offset))
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

        let events_history = Arc::new(Events::new(1));

        let nd = NotificationDispatcher::new(events_history);

        let mut rx0 = nd.add_subscriber(5, None).await.unwrap();
        let mut rx1 = nd.add_subscriber(5, None).await.unwrap();

        nd.notify(&event).await;

        let actual0 = rx0.recv().await.unwrap();
        let actual1 = rx1.recv().await.unwrap();

        assert_eq!(actual0, event);
        assert_eq!(actual1, event);
    }

    #[tokio::test]
    async fn test_with_offset() {
        let events = vec![
            Event {
                id: "hey".to_string(),
                ts: 10,
                typ: EventType::Connected,
            },
            Event {
                id: "ho".to_string(),
                ts: 20,
                typ: EventType::Connected,
            },
            Event {
                id: "lets".to_string(),
                ts: 20,
                typ: EventType::Connected,
            },
            Event {
                id: "go".to_string(),
                ts: 30,
                typ: EventType::Connected,
            },
        ];

        let events_history = Arc::new(Events::new(10));

        for e in events {
            events_history.store_event(e).await;
        }

        let nd = NotificationDispatcher::new(events_history);

        let mut rx0 = nd.add_subscriber(5, Some(20)).await.unwrap();

        assert_eq!(rx0.recv().await.unwrap().id, "ho");
        assert_eq!(rx0.recv().await.unwrap().id, "lets");
        assert_eq!(rx0.recv().await.unwrap().id, "go");
    }
}

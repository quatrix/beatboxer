use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::Duration,
};

use crate::keep_alive::{
    constants::DEAD_DEVICE_TIMEOUT,
    types::{Event, EventType},
};

use self::{events::Events, zset::ZSet};

use super::Storage;
use anyhow::Result;
use axum::async_trait;
use postcard::to_allocvec;
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    RwLock,
};
use tracing::{error, info};

mod events;
mod zset;

pub struct InMemoryStorage {
    keep_alives: Arc<ZSet>,
    events: Arc<RwLock<Events>>,
    txs: Arc<RwLock<Vec<Sender<Event>>>>,
    max_history_size: usize,
}

impl InMemoryStorage {
    pub fn new(max_history_size: usize) -> Self {
        Self {
            keep_alives: Arc::new(ZSet::new()),
            events: Arc::new(RwLock::new(Events::new(max_history_size))),
            txs: Arc::new(RwLock::new(Vec::new())),
            max_history_size,
        }
    }

    async fn set_ka(&self, id: &str, ts: i64) {
        self.keep_alives.update(id, ts);
    }

    async fn events_since_ts(&self, ts: i64) -> Vec<Event> {
        let events = self.events.read().await;
        events.events_since_ts(ts)
    }
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self::new(500_000)
    }
}

#[async_trait]
impl Storage for InMemoryStorage {
    async fn get(&self, id: &str) -> Option<i64> {
        self.keep_alives.get(id)
    }

    async fn set(&self, id: &str, ts: i64, is_connection_event: bool) {
        self.set_ka(id, ts).await;

        if is_connection_event {
            let mut events = self.events.write().await;
            let event = Event {
                ts,
                id: id.to_string(),
                typ: EventType::Connected,
            };

            events.store_event(event.clone());

            let txs = self.txs.read().await;
            for tx in txs.iter() {
                if let Err(e) = tx.send(event.clone()).await {
                    error!("unable to send update: {:?}", e);
                }
            }
        }
    }

    async fn bulk_set(&self, new_data: HashMap<String, i64>) {
        // FIXME: unoptimized, but used only on SYNC
        for (id, ts) in new_data {
            self.set_ka(&id, ts).await;
        }
    }

    async fn merge_events(&self, new_data: VecDeque<Event>) {
        let mut events = self.events.write().await;
        events.merge(&new_data);
    }

    async fn serialize_state(&self) -> Result<Vec<u8>> {
        let t0 = std::time::Instant::now();

        let bin = to_allocvec(&self.keep_alives.scores)?;
        info!(
            "Serialized state in {:.2} secs ({} keys)",
            t0.elapsed().as_secs_f32(),
            self.keep_alives.scores.len()
        );
        Ok(bin)
    }

    async fn serialize_events(&self) -> Result<Vec<u8>> {
        let t0 = std::time::Instant::now();
        let events = self.events.read().await;

        let bin = to_allocvec(&events.events)?;
        info!(
            "Serialized state in {:.2} secs ({} keys)",
            t0.elapsed().as_secs_f32(),
            self.keep_alives.scores.len()
        );
        Ok(bin)
    }

    async fn subscribe(&self, offset: Option<i64>) -> Option<Receiver<Event>> {
        let (tx, rx) = mpsc::channel(self.max_history_size + 10000);

        if let Some(offset) = offset {
            for event in self.events_since_ts(offset).await {
                tx.send(event).await.expect("can't event to tx");
            }
        }

        {
            let mut txs = self.txs.write().await;
            txs.push(tx);
        }

        Some(rx)
    }

    fn watch_for_updates(&self) {
        let keep_alives_c = Arc::clone(&self.keep_alives);
        let events_c = Arc::clone(&self.events);
        let txs_c = Arc::clone(&self.txs);

        tokio::spawn(async move {
            let mut oldest_ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64
                - (120 * 1000); // older than 2m ago, we don't care

            loop {
                {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as i64;

                    let dead_ids = keep_alives_c
                        .range(oldest_ts, now - DEAD_DEVICE_TIMEOUT.as_millis() as i64);

                    for (id, ts) in dead_ids {
                        if ts > oldest_ts {
                            {
                                let mut events = events_c.write().await;
                                let txs = txs_c.read().await;

                                let event = Event {
                                    ts: ts + DEAD_DEVICE_TIMEOUT.as_millis() as i64,
                                    id: id.to_string(),
                                    typ: EventType::Dead,
                                };

                                events.store_event(event.clone());

                                for tx in txs.iter() {
                                    if let Err(e) = tx.send(event.clone()).await {
                                        error!("unable to send update: {:?}", e);
                                    }
                                }
                            }
                            oldest_ts = ts;
                        }
                    }
                }

                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }
}

#[cfg(test)]
mod test {
    use super::{Event, EventType, Events, InMemoryStorage};
    use crate::storage::Storage;

    #[tokio::test]
    async fn test_inserting_stores_only_newer_timestamps() {
        let storage = InMemoryStorage::new(0);
        storage.set("hey", 10, false).await;
        assert_eq!(storage.get("hey").await.unwrap(), 10);

        // 5 is older than 10, so should keep 10
        storage.set("hey", 5, false).await;
        assert_eq!(storage.get("hey").await.unwrap(), 10);

        // 15 is newer, so it should get set
        storage.set("hey", 15, false).await;
        assert_eq!(storage.get("hey").await.unwrap(), 15);
    }

    #[tokio::test]
    async fn test_getting_events_since_some_ts() {
        let storage = InMemoryStorage::new(5);
        storage.set("hey", 10, true).await;
        storage.set("ho", 20, true).await;
        storage.set("lets", 30, true).await;
        storage.set("go", 40, true).await;

        let events = storage.events_since_ts(20).await;
        let expected_events = [
            Event {
                ts: 20,
                id: "ho".to_string(),
                typ: EventType::Connected,
            },
            Event {
                ts: 30,
                id: "lets".to_string(),
                typ: EventType::Connected,
            },
            Event {
                ts: 40,
                id: "go".to_string(),
                typ: EventType::Connected,
            },
        ];

        assert_eq!(events, expected_events);
    }

    #[tokio::test]
    async fn test_getting_events_since_0_should_return_all_events() {
        let storage = InMemoryStorage::new(5);

        let expected_events = [
            Event {
                ts: 10,
                id: "hey".to_string(),
                typ: EventType::Connected,
            },
            Event {
                ts: 20,
                id: "ho".to_string(),
                typ: EventType::Connected,
            },
            Event {
                ts: 30,
                id: "lets".to_string(),
                typ: EventType::Connected,
            },
            Event {
                ts: 40,
                id: "go".to_string(),
                typ: EventType::Connected,
            },
        ];

        for event in &expected_events {
            storage.set(&event.id, event.ts, true).await;
        }

        let events = storage.events_since_ts(0).await;
        assert_eq!(events, expected_events);
    }
}

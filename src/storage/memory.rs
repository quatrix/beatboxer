use std::{
    cmp::max,
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::Duration,
};

use crate::{
    keep_alive::{
        constants::DEAD_DEVICE_TIMEOUT,
        types::{Event, EventType},
    },
    notification_dispatcher::NotificationDispatcher,
};

use self::{events::Events, zset::ZSet};

use super::Storage;
use anyhow::Result;
use axum::async_trait;
use postcard::to_allocvec;
use tokio::sync::mpsc::Receiver;
use tracing::info;

mod events;
mod zset;

pub struct InMemoryStorage {
    keep_alives: Arc<ZSet>,
    events: Arc<Events>,
    notification_dispatcher: Arc<NotificationDispatcher>,
    max_history_size: usize,
}

impl InMemoryStorage {
    pub fn new(max_history_size: usize) -> Self {
        Self {
            keep_alives: Arc::new(ZSet::new()),
            events: Arc::new(Events::new(max_history_size)),
            notification_dispatcher: Arc::new(NotificationDispatcher::new()),
            max_history_size,
        }
    }

    async fn set_ka(&self, id: &str, ts: i64) {
        self.keep_alives.update(id, ts);
    }

    async fn events_since_ts(&self, ts: i64) -> Vec<Event> {
        self.events.events_since_ts(ts).await
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
            let event = Event {
                ts,
                id: id.to_string(),
                typ: EventType::Connected,
            };

            self.events.store_event(event.clone()).await;
        }
    }

    async fn bulk_set(&self, new_data: HashMap<String, i64>) {
        // FIXME: unoptimized, but used only on SYNC
        for (id, ts) in new_data {
            self.set_ka(&id, ts).await;
        }
    }

    async fn merge_events(&self, new_data: VecDeque<Event>) {
        self.events.merge(&new_data).await;
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
        self.events.serialize().await
    }

    async fn subscribe(&self, offset: Option<i64>) -> Option<Receiver<Event>> {
        let buffer_size = self.max_history_size + 500_000;

        let rx = match offset {
            Some(offset) => {
                let events = self.events_since_ts(offset).await;

                self.notification_dispatcher
                    .add_subscriber(buffer_size, Some(events))
                    .await
            }
            None => {
                self.notification_dispatcher
                    .add_subscriber(buffer_size, None)
                    .await
            }
        };

        Some(rx)
    }

    fn watch_for_updates(&self) {
        let keep_alives_c = Arc::clone(&self.keep_alives);
        let events_c = Arc::clone(&self.events);
        let notification_dispatcher = Arc::clone(&self.notification_dispatcher);

        tokio::spawn(async move {
            let mut oldest_ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;

            let dead_ms = DEAD_DEVICE_TIMEOUT.as_millis() as i64;

            loop {
                {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as i64;

                    let end = max(oldest_ts, now - dead_ms);

                    let dead_ids = keep_alives_c.range(oldest_ts, end);

                    for (id, ts) in dead_ids {
                        let event = Event {
                            ts: ts + dead_ms,
                            id: id.to_string(),
                            typ: EventType::Dead,
                        };

                        events_c.store_event(event.clone()).await;
                        notification_dispatcher.notify(&event).await;
                    }
                    oldest_ts = end;
                }

                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }
}

#[cfg(test)]
mod test {
    use super::{Event, EventType, InMemoryStorage};
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

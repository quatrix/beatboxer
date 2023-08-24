use std::{
    cmp::max,
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::Duration,
};

use crate::{
    keep_alive::{
        constants::{CONSOLIDATION_WINDOW, DEAD_DEVICE_TIMEOUT},
        types::{Event, EventType},
    },
    notification_dispatcher::NotificationDispatcher,
};

use self::{events::Events, events_buffer::EventsBuffer, zset::ZSet};

use super::Storage;
use anyhow::Result;
use axum::async_trait;
use postcard::to_allocvec;
use tokio::sync::mpsc::Receiver;
use tracing::info;

mod events;
mod events_buffer;
mod zset;

pub struct InMemoryStorage {
    keep_alives: Arc<ZSet>,
    events_buffer: Arc<EventsBuffer>,
    events_history: Arc<Events>,
    notification_dispatcher: Arc<NotificationDispatcher>,
    max_history_size: usize,
}

impl InMemoryStorage {
    pub fn new(max_history_size: usize) -> Self {
        let notification_dispatcher = Arc::new(NotificationDispatcher::new());
        let events_history = Arc::new(Events::new(max_history_size));

        Self {
            keep_alives: Arc::new(ZSet::new()),
            events_buffer: Arc::new(EventsBuffer::new()),
            events_history: Arc::clone(&events_history),
            notification_dispatcher: Arc::clone(&notification_dispatcher),
            max_history_size,
        }
    }

    async fn set_ka(&self, id: &str, ts: i64) {
        self.keep_alives.update(id, ts);
    }

    async fn events_since_ts(&self, ts: i64) -> Vec<Event> {
        self.events_history.events_since_ts(ts).await
    }

    fn start_consolidator(&self) {
        let events_buffer = Arc::clone(&self.events_buffer);
        let events_history = Arc::clone(&self.events_history);
        let notification_dispatcher = Arc::clone(&self.notification_dispatcher);

        tokio::spawn(async move {
            let consolidation_ms = CONSOLIDATION_WINDOW.as_millis() as i64;

            loop {
                {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as i64;

                    let buffer_until = now - consolidation_ms;
                    let consolidated_events = events_buffer.consolidate(buffer_until).await;

                    for event in consolidated_events {
                        events_history.store_event(event.clone()).await;
                        notification_dispatcher.notify(&event).await;
                    }
                }

                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }

    fn watch_for_updates(&self) {
        let keep_alives_c = Arc::clone(&self.keep_alives);
        let events_buffer = Arc::clone(&self.events_buffer);

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

                    let dead_ids = keep_alives_c.pop_lower_than_score(end);

                    for (id, ts) in dead_ids {
                        let event = Event {
                            ts: ts + dead_ms,
                            id: id.to_string(),
                            typ: EventType::Dead,
                        };

                        events_buffer.store_event(event.clone()).await;
                    }
                    oldest_ts = end;
                }

                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self::new(500_000)
    }
}

#[async_trait]
impl Storage for InMemoryStorage {
    async fn len(&self) -> usize {
        self.keep_alives.len()
    }

    async fn is_empty(&self) -> bool {
        self.keep_alives.len() == 0
    }

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

            self.events_buffer.store_event(event.clone()).await;
        }
    }

    async fn bulk_set(&self, new_data: HashMap<String, i64>) {
        let mut counter = 0;

        for (id, ts) in new_data {
            counter += 1;
            self.set_ka(&id, ts).await;

            if counter % 10000 == 0 {
                tokio::task::yield_now().await;
            }
        }
    }

    async fn merge_events(&self, new_data: VecDeque<Event>) {
        self.events_history.merge(&new_data).await;
    }

    async fn serialize_state(&self) -> Result<Vec<u8>> {
        let t0 = std::time::Instant::now();

        let mut scores: HashMap<String, i64> = HashMap::new();

        // FIXME: converting the u128 state to i64 for serialization
        // not really optimized. find better way.
        for element in self.keep_alives.scores.iter() {
            scores.insert(element.key().to_string(), (element.value() >> 64) as i64);
        }

        let bin = to_allocvec(&scores)?;
        info!(
            "Serialized state in {:.2} secs ({} keys)",
            t0.elapsed().as_secs_f32(),
            self.keep_alives.scores.len()
        );
        Ok(bin)
    }

    async fn serialize_events(&self) -> Result<Vec<u8>> {
        self.events_history.serialize().await
    }

    async fn subscribe(&self, offset: Option<i64>) -> Result<Receiver<Event>> {
        let buffer_size = self.max_history_size + 500_000;

        match offset {
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
        }
    }

    fn start_background_tasks(&self) {
        self.start_consolidator();
        self.watch_for_updates();
        self.notification_dispatcher.monitor();
    }
}

#[cfg(test)]
mod test {
    use super::InMemoryStorage;
    use crate::storage::Storage;

    #[tokio::test]
    async fn test_inserting_stores_only_newer_timestamps() {
        let storage = InMemoryStorage::new(1);
        storage.set("hey", 10, false).await;
        assert_eq!(storage.get("hey").await.unwrap(), 10);

        // 5 is older than 10, so should keep 10
        storage.set("hey", 5, false).await;
        assert_eq!(storage.get("hey").await.unwrap(), 10);

        // 15 is newer, so it should get set
        storage.set("hey", 15, false).await;
        assert_eq!(storage.get("hey").await.unwrap(), 15);
    }
}

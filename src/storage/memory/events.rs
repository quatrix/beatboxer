use anyhow::Result;
use postcard::to_allocvec;
use std::collections::{HashSet, VecDeque};
use tokio::sync::RwLock;
use tracing::info;

use crate::keep_alive::types::Event;

pub struct Events {
    pub max_history_size: usize,
    pub events: RwLock<VecDeque<Event>>,
}

impl Events {
    pub fn new(max_history_size: usize) -> Self {
        Self {
            max_history_size,
            events: RwLock::new(VecDeque::new()),
        }
    }

    pub async fn store_event(&self, event: Event) {
        if self.max_history_size == 0 {
            return;
        }

        let mut events = self.events.write().await;
        events.push_back(event);

        if events.len() > self.max_history_size {
            let _ = events.pop_front();
        }
    }

    pub async fn events_since_ts(&self, ts: i64) -> Vec<Event> {
        let events = self.events.read().await;

        let index = match events.binary_search_by_key(&ts, |event| event.ts) {
            Ok(index) => index,
            Err(index) => index,
        };

        let events: Vec<Event> = events.range(index..).cloned().collect();

        info!(
            "getting events since {} (index: {}) number of events: {}",
            ts,
            index,
            events.len()
        );

        events
    }

    pub async fn merge(&self, other: &VecDeque<Event>) {
        let mut events = self.events.write().await;
        *events = merge_without_duplicates(&events, other);

        if events.len() > self.max_history_size {
            let overflow = events.len() - self.max_history_size;
            let _ = events.drain(0..overflow);
        }
    }

    pub async fn serialize(&self) -> Result<Vec<u8>> {
        let t0 = std::time::Instant::now();
        let events = self.events.read().await;

        let bin = to_allocvec(&*events)?;
        info!(
            "Serialized events in {:.2} secs",
            t0.elapsed().as_secs_f32(),
        );
        Ok(bin)
    }
}

fn merge_without_duplicates(a: &VecDeque<Event>, b: &VecDeque<Event>) -> VecDeque<Event> {
    let mut result = VecDeque::new();
    let mut seen = HashSet::new();

    for item in a.iter().chain(b.iter()) {
        if !seen.contains(&item) {
            result.push_back(item.clone());
            seen.insert(item);
        }
    }

    result.make_contiguous().sort_by(|a, b| a.ts.cmp(&b.ts));

    result
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::keep_alive::types::{Event, EventType};

    #[tokio::test]
    async fn test_merging_two_identical_events_lists() {
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
        let e0 = Events::new(3);
        let e1 = Events::new(3);

        for event in expected_events.iter() {
            e0.store_event(event.clone()).await;
            e1.store_event(event.clone()).await;
        }

        e0.merge(&*e1.events.read().await).await;

        assert_eq!(*e0.events.read().await, expected_events);
    }

    #[tokio::test]
    async fn test_merging_two_different_events_lists() {
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

        let e0 = Events::new(3);
        let e1 = Events::new(3);

        e0.store_event(Event {
            ts: 20,
            id: "ho".to_string(),
            typ: EventType::Connected,
        })
        .await;

        e0.store_event(Event {
            ts: 30,
            id: "lets".to_string(),
            typ: EventType::Connected,
        })
        .await;

        e1.store_event(Event {
            ts: 40,
            id: "go".to_string(),
            typ: EventType::Connected,
        })
        .await;

        e0.merge(&*e1.events.read().await).await;

        assert_eq!(*e0.events.read().await, expected_events);
    }

    #[tokio::test]
    async fn test_merging_should_remove_duplicates() {
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
            Event {
                ts: 50,
                id: "vova".to_string(),
                typ: EventType::Dead,
            },
        ];

        let e0 = Events::new(4);
        let e1 = Events::new(4);

        e0.store_event(Event {
            ts: 20,
            id: "ho".to_string(),
            typ: EventType::Connected,
        })
        .await;

        e0.store_event(Event {
            ts: 30,
            id: "lets".to_string(),
            typ: EventType::Connected,
        })
        .await;

        e1.store_event(Event {
            ts: 40,
            id: "go".to_string(),
            typ: EventType::Connected,
        })
        .await;

        e1.store_event(Event {
            ts: 20,
            id: "ho".to_string(),
            typ: EventType::Connected,
        })
        .await;

        e1.store_event(Event {
            ts: 50,
            id: "vova".to_string(),
            typ: EventType::Dead,
        })
        .await;

        e0.merge(&*e1.events.read().await).await;

        assert_eq!(*e0.events.read().await, expected_events);
    }

    #[tokio::test]
    async fn test_merging_keeps_the_max_list_size() {
        let expected_events = [
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

        let e0 = Events::new(2);
        let e1 = Events::new(3);

        e0.store_event(Event {
            ts: 30,
            id: "lets".to_string(),
            typ: EventType::Connected,
        })
        .await;

        e1.store_event(Event {
            ts: 20,
            id: "ho".to_string(),
            typ: EventType::Connected,
        })
        .await;

        e1.store_event(Event {
            ts: 40,
            id: "go".to_string(),
            typ: EventType::Connected,
        })
        .await;

        e0.merge(&*e1.events.read().await).await;

        assert_eq!(*e0.events.read().await, expected_events);
    }
}

use anyhow::{anyhow, Result};
use postcard::to_allocvec;
use std::{
    collections::{HashSet, VecDeque},
    sync::Arc,
};
use tokio::sync::RwLock;
use tracing::{error, info};

use crate::keep_alive::types::{Event, EventType};

pub struct Events {
    max_history_size: usize,
    events: Arc<RwLock<VecDeque<Event>>>,
}

impl Events {
    pub fn new(max_history_size: usize) -> Self {
        if max_history_size == 0 {
            panic!("max_history_size can't be 0")
        }

        Self {
            max_history_size,
            events: Arc::new(RwLock::new(VecDeque::new())),
        }
    }

    pub async fn last_ts(&self) -> Option<i64> {
        let events = self.events.read().await;
        events.back().map(|e| e.ts)
    }

    pub async fn store_event(&self, event: Event) -> Result<()> {
        // FIXME: this should make sure that events
        // from the same id are in order, order between all events
        // isn't guaranteed but for the same device it should.
        //
        let mut events = self.events.write().await;
        events.push_back(event.clone());

        if events.len() > self.max_history_size {
            let overflow = events.len() - self.max_history_size;
            let _ = events.drain(0..overflow);
        }

        Ok(())
    }

    pub async fn get_all_events_from_id(&self, id: &str) -> Vec<Event> {
        //FIXME slow, just using for debugging
        let events = self.events.read().await;
        events
            .iter()
            .filter(|e| e.id == id)
            .map(|e| e.clone())
            .collect::<Vec<Event>>()
    }

    pub async fn events_since_ts(&self, ts: i64) -> Vec<Event> {
        {
            // FIXME: is this cheating?
            let mut events = self.events.write().await;
            events.make_contiguous().sort()
        }

        let events = self.events.read().await;

        let index = match events.binary_search_by_key(&ts, |event| event.ts) {
            Ok(index) => index,
            Err(index) => index,
        };

        let mut first_index_with_ts = index;

        // find the first one
        for i in (0..index).rev() {
            if events[i].ts == ts {
                first_index_with_ts = i;
            } else {
                break;
            }
        }

        let events: Vec<Event> = events.range(first_index_with_ts..).cloned().collect();

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
    let mut joined = VecDeque::new();
    let mut seen = HashSet::new();

    for item in a.iter().chain(b.iter()) {
        if !seen.contains(&item) {
            joined.push_back(item.clone());
            seen.insert(item);
        }
    }

    joined.make_contiguous().sort();

    let mut result = VecDeque::new();
    let mut connected: HashSet<String> = HashSet::new();

    for event in joined {
        if event.typ == EventType::Connected {
            // if we already seen a connected event for this id, drop it
            if connected.contains(&event.id) {
                continue;
            } else {
                connected.insert(event.id.clone());
            }
        } else if event.typ == EventType::Dead {
            // if we see dead event, we can clear the connected set
            connected.remove(&event.id);
        } else {
            error!("wat, got unexpected event {:?}", event);
        }

        result.push_back(event);
    }

    result
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::keep_alive::types::{Event, EventType};

    #[tokio::test]
    async fn test_getting_events_since_some_ts() {
        let e0 = Events::new(10);

        let all_events = vec![
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

        for event in &all_events {
            let _ = e0.store_event(event.clone()).await;
        }

        assert_eq!(e0.events_since_ts(20).await, all_events[1..]);
        assert_eq!(e0.events_since_ts(0).await, all_events);
    }

    #[tokio::test]
    async fn test_storing_multiple_events_with_the_same_ts() {
        let e0 = Events::new(5);

        let all_events = vec![
            Event {
                ts: 10,
                id: "hey".to_string(),
                typ: EventType::Connected,
            },
            Event {
                ts: 10,
                id: "ho".to_string(),
                typ: EventType::Connected,
            },
            Event {
                ts: 10,
                id: "lets".to_string(),
                typ: EventType::Connected,
            },
        ];

        for event in &all_events {
            e0.store_event(event.clone()).await;
        }

        assert_eq!(e0.events_since_ts(0).await, all_events);
    }

    #[tokio::test]
    async fn test_fetching_from_offset_with_duplicate_ts() {
        let e0 = Events::new(5);

        let all_events = vec![
            Event {
                ts: 5,
                id: "heh".to_string(),
                typ: EventType::Connected,
            },
            Event {
                ts: 10,
                id: "hey".to_string(),
                typ: EventType::Connected,
            },
            Event {
                ts: 10,
                id: "ho".to_string(),
                typ: EventType::Connected,
            },
            Event {
                ts: 20,
                id: "lets".to_string(),
                typ: EventType::Connected,
            },
        ];

        for event in &all_events {
            e0.store_event(event.clone()).await;
        }

        assert_eq!(e0.events_since_ts(10).await, all_events[1..]);
    }

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

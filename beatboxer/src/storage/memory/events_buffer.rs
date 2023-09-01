use crossbeam_skiplist::SkipMap;
use std::{collections::HashSet, sync::Arc};
use tokio::sync::RwLock;
use tracing::error;

use crate::keep_alive::types::{Event, EventType};

type EventsBufferSkipMap = SkipMap<i64, RwLock<HashSet<Event>>>;

pub struct EventsBuffer {
    events_buffer: Arc<EventsBufferSkipMap>,
}

impl EventsBuffer {
    pub fn new() -> Self {
        EventsBuffer {
            events_buffer: Arc::new(SkipMap::new()),
        }
    }

    pub async fn store_event(&self, event: Event) {
        // before actully storing the event
        // and sending notifications, we want to
        // store it in a buffer to allow multiple
        // events coming from other masters that might
        // be out of order a channce to arrive.
        //
        // then there's a process that sorts them and take
        // older events and actually storing them.
        //
        // basically we're creating some delay in notification
        // so we can have more consistency and order.

        let hs = self
            .events_buffer
            .get_or_insert(event.ts, RwLock::new(HashSet::new()));

        let mut hs = hs.value().write().await;
        hs.insert(event.clone());
    }

    pub async fn consolidate(&self, buffer_until: i64) -> Vec<Event> {
        // FIXME: this could probably just be an iterator
        // instead of building and returning a vector.

        let mut e = Vec::new();
        let mut connected: HashSet<String> = HashSet::new();

        while self.events_buffer.front().map(|f| *f.key() < buffer_until) == Some(true) {
            if let Some(entry) = self.events_buffer.pop_front() {
                let events_set = entry.value().read().await;
                let mut events_set = events_set.iter().collect::<Vec<&Event>>();
                events_set.sort();

                for event in events_set {
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
                        error!("wat");
                    }
                    e.push(event.clone());
                }
            }
        }

        e
    }
}

impl Default for EventsBuffer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::keep_alive::types::{Event, EventType};

    #[tokio::test]
    async fn test_consolidating_events() {
        let e0 = EventsBuffer::new();

        let all_events = vec![
            Event {
                ts: 40,
                id: "go".to_string(),
                typ: EventType::Connected,
            },
            Event {
                ts: 10,
                id: "hey".to_string(),
                typ: EventType::Connected,
            },
            Event {
                ts: 10,
                id: "hello".to_string(),
                typ: EventType::Connected,
            },
            Event {
                ts: 30,
                id: "lets".to_string(),
                typ: EventType::Connected,
            },
            Event {
                ts: 15,
                id: "ho".to_string(),
                typ: EventType::Connected,
            },
        ];

        for event in &all_events {
            e0.store_event(event.clone()).await;
        }

        // events older than 20, should return
        let consolidated = e0.consolidate(20).await;

        assert_eq!(
            consolidated
                .iter()
                .map(|c| c.id.clone())
                .collect::<Vec<String>>(),
            vec!["hello", "hey", "ho"]
        );

        // doing it again, at later time, shouldn't return
        // the same events from before
        let consolidated = e0.consolidate(50).await;

        assert_eq!(
            consolidated
                .iter()
                .map(|c| c.id.clone())
                .collect::<Vec<String>>(),
            vec!["lets", "go"]
        );
    }

    #[tokio::test]
    async fn test_double_connect_should_be_eliminated_and_take_the_first_one() {
        let e0 = EventsBuffer::new();

        let all_events = vec![
            Event {
                ts: 40,
                id: "foo".to_string(),
                typ: EventType::Connected,
            },
            Event {
                ts: 20,
                id: "bar".to_string(),
                typ: EventType::Connected,
            },
            Event {
                ts: 10,
                id: "foo".to_string(),
                typ: EventType::Connected,
            },
            Event {
                ts: 45,
                id: "baz".to_string(),
                typ: EventType::Connected,
            },
        ];

        for event in &all_events {
            e0.store_event(event.clone()).await;
        }

        // events older than 20, should return
        let consolidated = e0.consolidate(50).await;

        let expected_events = vec![
            Event {
                ts: 10,
                id: "foo".to_string(),
                typ: EventType::Connected,
            },
            Event {
                ts: 20,
                id: "bar".to_string(),
                typ: EventType::Connected,
            },
            Event {
                ts: 45,
                id: "baz".to_string(),
                typ: EventType::Connected,
            },
        ];

        assert_eq!(consolidated, expected_events,);
    }

    #[tokio::test]
    async fn test_double_connect_with_dead_in_between() {
        let e0 = EventsBuffer::new();

        let all_events = vec![
            Event {
                ts: 40,
                id: "foo".to_string(),
                typ: EventType::Connected,
            },
            Event {
                ts: 20,
                id: "bar".to_string(),
                typ: EventType::Connected,
            },
            Event {
                ts: 15,
                id: "foo".to_string(),
                typ: EventType::Dead,
            },
            Event {
                ts: 10,
                id: "foo".to_string(),
                typ: EventType::Connected,
            },
            Event {
                ts: 45,
                id: "baz".to_string(),
                typ: EventType::Connected,
            },
        ];

        for event in &all_events {
            e0.store_event(event.clone()).await;
        }

        // events older than 20, should return
        let consolidated = e0.consolidate(50).await;

        let expected_events = vec![
            Event {
                ts: 10,
                id: "foo".to_string(),
                typ: EventType::Connected,
            },
            Event {
                ts: 15,
                id: "foo".to_string(),
                typ: EventType::Dead,
            },
            Event {
                ts: 20,
                id: "bar".to_string(),
                typ: EventType::Connected,
            },
            Event {
                ts: 40,
                id: "foo".to_string(),
                typ: EventType::Connected,
            },
            Event {
                ts: 45,
                id: "baz".to_string(),
                typ: EventType::Connected,
            },
        ];

        assert_eq!(consolidated, expected_events,);
    }
}

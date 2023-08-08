use std::collections::{HashSet, VecDeque};
use tracing::info;

use crate::keep_alive::types::Event;

pub struct Events {
    pub max_history_size: usize,
    pub events: VecDeque<Event>,
}

impl Events {
    pub fn new(max_history_size: usize) -> Self {
        Self {
            max_history_size,
            events: VecDeque::new(),
        }
    }

    pub fn store_event(&mut self, event: Event) {
        if self.max_history_size == 0 {
            return;
        }

        self.events.push_back(event);

        if self.events.len() > self.max_history_size {
            let _ = self.events.pop_front();
        }
    }

    pub fn events_since_ts(&self, ts: i64) -> Vec<Event> {
        let index = match self.events.binary_search_by_key(&ts, |event| event.ts) {
            Ok(index) => index,
            Err(index) => index,
        };

        let events: Vec<Event> = self.events.range(index..).cloned().collect();

        info!(
            "getting events since {} (index: {}) number of events: {}",
            ts,
            index,
            events.len()
        );

        events
    }

    pub fn merge(&mut self, other: &VecDeque<Event>) {
        self.events = merge_without_duplicates(&self.events, other);

        if self.events.len() > self.max_history_size {
            let overflow = self.events.len() - self.max_history_size;
            let _ = self.events.drain(0..overflow);
        }
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

    #[test]
    fn test_merging_two_identical_events_lists() {
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
        let mut e0 = Events::new(3);
        let mut e1 = Events::new(3);

        for event in expected_events.iter() {
            e0.store_event(event.clone());
            e1.store_event(event.clone());
        }

        e0.merge(&e1.events);

        assert_eq!(e0.events, expected_events);
    }

    #[test]
    fn test_merging_two_different_events_lists() {
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

        let mut e0 = Events::new(3);
        let mut e1 = Events::new(3);

        e0.store_event(Event {
            ts: 20,
            id: "ho".to_string(),
            typ: EventType::Connected,
        });

        e0.store_event(Event {
            ts: 30,
            id: "lets".to_string(),
            typ: EventType::Connected,
        });

        e1.store_event(Event {
            ts: 40,
            id: "go".to_string(),
            typ: EventType::Connected,
        });

        e0.merge(&e1.events);

        assert_eq!(e0.events, expected_events);
    }

    #[test]
    fn test_merging_should_remove_duplicates() {
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

        let mut e0 = Events::new(4);
        let mut e1 = Events::new(4);

        e0.store_event(Event {
            ts: 20,
            id: "ho".to_string(),
            typ: EventType::Connected,
        });

        e0.store_event(Event {
            ts: 30,
            id: "lets".to_string(),
            typ: EventType::Connected,
        });

        e1.store_event(Event {
            ts: 40,
            id: "go".to_string(),
            typ: EventType::Connected,
        });

        e1.store_event(Event {
            ts: 20,
            id: "ho".to_string(),
            typ: EventType::Connected,
        });

        e1.store_event(Event {
            ts: 50,
            id: "vova".to_string(),
            typ: EventType::Dead,
        });

        e0.merge(&e1.events);

        assert_eq!(e0.events, expected_events);
    }

    #[test]
    fn test_merging_keeps_the_max_list_size() {
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

        let mut e0 = Events::new(2);
        let mut e1 = Events::new(3);

        e0.store_event(Event {
            ts: 30,
            id: "lets".to_string(),
            typ: EventType::Connected,
        });

        e1.store_event(Event {
            ts: 20,
            id: "ho".to_string(),
            typ: EventType::Connected,
        });

        e1.store_event(Event {
            ts: 40,
            id: "go".to_string(),
            typ: EventType::Connected,
        });

        e0.merge(&e1.events);

        assert_eq!(e0.events, expected_events);
    }
}

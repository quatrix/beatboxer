use std::{
    collections::HashMap,
    time::{Duration, SystemTime},
};

use crate::event::{Event, EventType};

use super::Strategy;

pub struct Ledger {
    name: String,
    logs: HashMap<String, Vec<Event>>,
    ws_msgs: Vec<String>,
}

impl Ledger {
    pub fn new(name: &str) -> Self {
        Ledger {
            name: name.to_string(),
            logs: HashMap::new(),
            ws_msgs: Vec::new(),
        }
    }

    fn insert(&mut self, id: String, ts: i64, event: EventType) {
        self.logs
            .entry(id.to_string())
            .or_insert(Vec::new())
            .push(Event {
                id: id.clone(),
                ts: millis_to_systemtime(ts),
                event,
            });
    }
}

pub fn millis_to_systemtime(millis: i64) -> SystemTime {
    let duration_since_epoch = Duration::from_millis(millis as u64);
    SystemTime::UNIX_EPOCH + duration_since_epoch
}

impl Strategy for Ledger {
    fn get_name(&self) -> String {
        self.name.clone()
    }

    fn add_event(&mut self, msg: String) {
        self.ws_msgs.push(msg)
    }

    fn on_connect(&mut self, id: String, ts: i64) {
        self.insert(id, ts, EventType::Connect);
    }

    fn on_dead(&mut self, id: String, ts: i64) {
        self.insert(id, ts, EventType::Dead);
    }

    fn get_events(&self) -> Vec<String> {
        self.ws_msgs.clone()
    }

    fn get_grouped_events(&self) -> HashMap<String, Vec<Event>> {
        self.logs.clone()
    }
}

use std::time::SystemTime;

use chrono::{DateTime, Utc};

#[derive(Debug, Clone, PartialEq)]
pub enum EventType {
    Connect,
    Beat,
    Skip,
    Dead,
}

#[derive(Clone)]
pub struct Event {
    pub id: String,
    pub ts: SystemTime,
    pub event: EventType,
}

pub fn iso8601(st: &std::time::SystemTime) -> String {
    let dt: DateTime<Utc> = (*st).into();
    format!("{}", dt.format("%+"))
}

impl std::fmt::Debug for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let event = match self.event {
            EventType::Connect => "C",
            EventType::Beat => "B",
            EventType::Skip => "S",
            EventType::Dead => "D",
        };

        let ts = iso8601(&self.ts);

        write!(f, "{}-{}-{}", self.id, event, ts)
    }
}

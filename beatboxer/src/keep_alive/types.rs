use std::cmp::Ordering;

use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug)]
pub struct KeepAliveUpdate {
    pub id: String,
    pub ts: i64,
    pub is_connection_event: bool,
}

#[derive(Clone, Debug)]
pub struct DeadUpdate {
    pub id: String,
    pub last_ka: i64,
    pub ts_of_death: i64,
}

#[derive(Clone, Debug)]
pub enum Message {
    Ping,
    KeepAliveUpdate(KeepAliveUpdate),
    DeadUpdate(DeadUpdate),
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialOrd, PartialEq, Deserialize, Serialize)]
pub enum EventType {
    Connected,
    Dead,
    Unknown,
}

impl std::fmt::Display for EventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventType::Connected => write!(f, "CONNECTED"),
            EventType::Dead => write!(f, "DEAD"),
            EventType::Unknown => write!(f, "UNKNOWN"),
        }
    }
}

#[derive(Clone, Eq, Hash, PartialEq, Deserialize, Serialize)]
pub struct Event {
    pub ts: i64,
    pub id: String,
    pub typ: EventType,
}

fn millis_to_dt(ts_millis: i64) -> DateTime<Utc> {
    let ts_secs = ts_millis / 1000;
    let ts_ns = (ts_millis % 1000) * 1_000_000;

    DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(ts_secs, ts_ns as u32), Utc)
}

impl std::fmt::Debug for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ts = millis_to_dt(self.ts);
        let typ = match self.typ {
            EventType::Connected => "C",
            EventType::Dead => "D",
            EventType::Unknown => "U",
        };

        write!(f, "{}-{}-{}", typ, self.id, ts.time())
    }
}

impl Ord for Event {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.ts, &self.id).cmp(&(other.ts, &other.id))
    }
}

impl PartialOrd for Event {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

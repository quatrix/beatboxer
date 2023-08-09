use serde::{Deserialize, Serialize};

#[derive(Clone, Debug)]
pub struct KeepAliveUpdate {
    pub id: String,
    pub ts: i64,
    pub is_connection_event: bool,
}

pub enum Message {
    Ping,
    KeepAliveUpdate(KeepAliveUpdate),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Deserialize, Serialize)]
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

#[derive(Clone, Debug, Eq, Hash, PartialEq, Deserialize, Serialize)]
pub struct Event {
    pub ts: i64,
    pub id: String,
    pub typ: EventType,
}

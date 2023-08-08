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
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Deserialize, Serialize)]
pub struct Event {
    pub ts: i64,
    pub id: String,
    pub typ: EventType,
}

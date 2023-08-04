#[derive(Clone, Debug)]
pub struct KeepAliveUpdate {
    pub id: String,
    pub ts: i64,
}

pub enum Message {
    Ping,
    KeepAliveUpdate(KeepAliveUpdate),
}

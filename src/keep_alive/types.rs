#[derive(Clone, Debug)]
pub struct KeepAliveUpdate {
    pub id: String,
    pub ts: i64,
}

pub enum Message {
    Ping,
    KeepAliveUpdate(KeepAliveUpdate),
}

#[derive(Debug)]
pub enum NotificationStatus {
    Connected,
    Dead,
}

#[derive(Debug)]
pub struct Notification {
    pub id: String,
    pub status: NotificationStatus,
}

impl std::fmt::Display for Notification {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "id: {}, status: {:?}", self.id, self.status)
    }
}

use std::time::Duration;

pub(crate) const SOCKET_WRITE_TIMEOUT: Duration = Duration::from_millis(1000);
pub(crate) const SOCKET_WRITE_LONG_TIMEOUT: Duration = Duration::from_millis(10000);
//pub(crate) const SOCKET_READ_TIMEOUT: Duration = Duration::from_millis(1000);
pub(crate) const SOCKET_READ_LONG_TIMEOUT: Duration = Duration::from_millis(10000);
pub(crate) const LAST_PONG_TIMEOUT: Duration = Duration::from_millis(10000);

pub(crate) const DEAD_DEVICE_TIMEOUT: Duration = Duration::from_secs(20);

pub fn is_dead(ts: i64) -> bool {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    ts < (now - DEAD_DEVICE_TIMEOUT.as_millis() as i64)
}

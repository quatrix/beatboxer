use std::time::Duration;

pub(crate) const SOCKET_WRITE_TIMEOUT: Duration = Duration::from_millis(1000);
pub(crate) const SOCKET_WRITE_LONG_TIMEOUT: Duration = Duration::from_millis(10000);
//pub(crate) const SOCKET_READ_TIMEOUT: Duration = Duration::from_millis(1000);
pub(crate) const SOCKET_READ_LONG_TIMEOUT: Duration = Duration::from_millis(10000);
pub(crate) const LAST_PONG_TIMEOUT: Duration = Duration::from_millis(10000);

pub(crate) const DEAD_DEVICE_TIMEOUT: Duration = Duration::from_secs(20);

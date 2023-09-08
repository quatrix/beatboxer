use lazy_static::lazy_static;
use std::{env, time::Duration};

fn from_env(name: &str, default: Duration) -> Duration {
    match env::var_os(name) {
        Some(v) => Duration::from_millis(
            v.into_string()
                .unwrap()
                .parse::<u64>()
                .expect("{name} must be valid number"),
        ),
        None => default,
    }
}

lazy_static! {
    pub static ref SOCKET_WRITE_TIMEOUT: Duration =
        from_env("SOCKET_WRITE_TIMEOUT_MS", Duration::from_secs(5));
    pub static ref SOCKET_WRITE_LONG_TIMEOUT: Duration =
        from_env("SOCKET_WRITE_LONG_TIMEOUT_MS", Duration::from_secs(30));
    pub static ref SOCKET_READ_LONG_TIMEOUT: Duration =
        from_env("SOCKET_READ_LONG_TIMEOUT_MS", Duration::from_secs(30));
    pub static ref SYNC_TIMEOUT: Duration = from_env("SYNC_TIMEOUT_MS", Duration::from_secs(240));
    pub static ref LAST_PONG_TIMEOUT: Duration =
        from_env("LAST_PONG_TIMEOUT_MS", Duration::from_secs(10));
    pub static ref DEAD_DEVICE_TIMEOUT: Duration =
        from_env("DEAD_DEVICE_TIMEOUT_MS", Duration::from_secs(30));
    pub static ref CONSOLIDATION_WINDOW: Duration =
        from_env("CONSOLIDATION_WINDOW_MS", Duration::from_secs(2));
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_now_isnt_dead() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        assert!(!is_dead(now));
    }

    #[test]
    fn test_past_is_dead() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let past = now - (DEAD_DEVICE_TIMEOUT.as_millis() as i64);

        assert!(is_dead(past));
    }
}

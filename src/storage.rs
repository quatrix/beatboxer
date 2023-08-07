use anyhow::Result;
use axum::async_trait;
use std::collections::HashMap;
use tokio::sync::mpsc::Receiver;

use crate::keep_alive::types::Event;
pub mod memory;
pub mod persistent;
pub mod zset;

#[async_trait]
pub trait Storage {
    async fn get(&self, id: &str) -> Option<i64>;
    async fn set(&self, id: &str, ts: i64, is_connection_event: bool);
    async fn bulk_set(&self, new_data: HashMap<String, i64>);
    async fn serialize(&self) -> Result<Vec<u8>>;
    async fn subscribe(&self, _offset: Option<i64>) -> Option<Receiver<Event>> {
        None
    }

    fn watch_for_updates(&self) {}
}

use anyhow::Result;
use axum::async_trait;
use std::collections::{HashMap, VecDeque};
use tokio::sync::mpsc::Receiver;

use crate::keep_alive::types::Event;
pub mod memory;
pub mod persistent;

#[async_trait]
pub trait Storage {
    async fn get(&self, id: &str) -> Option<i64>;
    async fn set(&self, id: &str, ts: i64, is_connection_event: bool);
    async fn bulk_set(&self, new_data: HashMap<String, i64>);
    async fn serialize_state(&self) -> Result<Vec<u8>>;
    async fn serialize_events(&self) -> Result<Vec<u8>>;
    async fn merge_events(&self, _new_data: VecDeque<Event>);
    async fn subscribe(&self, _offset: Option<i64>) -> Result<Receiver<Event>>;
    fn start_background_tasks(&self) {}
}

use anyhow::Result;
use axum::async_trait;
use std::collections::HashMap;
pub mod memory;
pub mod persistent;

#[async_trait]
pub trait Storage {
    async fn get(&self, id: &str) -> Option<i64>;
    async fn set(&self, id: &str, ts: i64);
    async fn bulk_set(&self, new_data: HashMap<String, i64>);
    async fn serialize(&self) -> Result<Vec<u8>>;
    //fn clone(&self) -> Self;
}

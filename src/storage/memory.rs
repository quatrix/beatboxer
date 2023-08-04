use std::{any::Any, collections::HashMap};

use axum::async_trait;
use dashmap::{mapref::entry::Entry, DashMap};
use postcard::to_allocvec;

use anyhow::Result;
use tracing::info;

use super::Storage;
pub struct InMemoryStorage {
    data: DashMap<String, i64>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self {
            data: DashMap::new(),
        }
    }
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Storage for InMemoryStorage {
    async fn get(&self, id: &str) -> Option<i64> {
        self.data.get(id).map(|v| *v)
    }

    async fn set(&self, id: &str, ts: i64) {
        match self.data.entry(id.to_string()) {
            Entry::Occupied(mut occupied) => {
                let current = occupied.get();

                if *current < ts {
                    occupied.insert(ts);
                }
            }
            Entry::Vacant(vacant) => {
                vacant.insert(ts);
            }
        };
    }

    async fn bulk_set(&self, new_data: HashMap<String, i64>) {
        // FIXME: unoptimized, but used only on SYNC
        for (id, ts) in new_data {
            self.set(&id, ts).await;
        }
    }

    async fn serialize(&self) -> Result<Vec<u8>> {
        let t0 = std::time::Instant::now();
        let bin = to_allocvec(&self.data)?;
        info!(
            "Serialized state in {:.2} secs ({} keys)",
            t0.elapsed().as_secs_f32(),
            self.data.len()
        );
        Ok(bin)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod test {
    use super::InMemoryStorage;
    use crate::storage::Storage;

    #[tokio::test]
    async fn test_inserting_stores_only_newer_timestamps() {
        let storage = InMemoryStorage::new();
        storage.set("hey", 10).await;
        assert_eq!(storage.get("hey").await.unwrap(), 10);

        // 5 is older than 10, so should keep 10
        storage.set("hey", 5).await;
        assert_eq!(storage.get("hey").await.unwrap(), 10);

        // 15 is newer, so it should get set
        storage.set("hey", 15).await;
        assert_eq!(storage.get("hey").await.unwrap(), 15);
    }
}

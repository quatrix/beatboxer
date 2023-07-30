use std::{collections::HashMap, sync::Arc};

use axum::async_trait;
use postcard::to_allocvec;
use tokio::sync::RwLock;

use anyhow::Result;

use super::Storage;
pub struct InMemoryStorage {
    data: Arc<RwLock<HashMap<String, i64>>>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
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
        let data = self.data.read().await;
        data.get(id).cloned()
    }

    async fn set(&self, id: &str, ts: i64) {
        let mut data = self.data.write().await;

        match data.get(id) {
            Some(current) => {
                if *current < ts {
                    data.insert(id.to_owned(), ts);
                }
            }
            None => {
                data.insert(id.to_owned(), ts);
            }
        }
    }

    async fn bulk_set(&self, new_data: HashMap<String, i64>) {
        let mut data = self.data.write().await;

        for (id, ts) in new_data {
            match data.get(&id) {
                Some(current) => {
                    if *current < ts {
                        data.insert(id, ts);
                    }
                }
                None => {
                    data.insert(id, ts);
                }
            }
        }
    }

    async fn serialize(&self) -> Result<Vec<u8>> {
        let data = self.data.read().await;
        let bin = to_allocvec(&*data)?;
        Ok(bin)
    }
}

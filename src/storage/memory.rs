use std::{any::Any, collections::HashMap};

use axum::async_trait;
use dashmap::{mapref::entry::Entry, DashMap};
use postcard::to_allocvec;

use anyhow::Result;

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
        let bin = to_allocvec(&self.data)?;
        Ok(bin)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

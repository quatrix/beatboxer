use std::{collections::HashMap, sync::Arc};

use axum::async_trait;
use postcard::to_allocvec;
use rocksdb::DB;

use anyhow::Result;

use super::Storage;
pub struct PersistentStorage {
    db: Arc<DB>,
}

impl PersistentStorage {
    pub fn new(filename: &str) -> Self {
        let db = DB::open_default(filename).unwrap();
        Self { db: Arc::new(db) }
    }
}

#[async_trait]
impl Storage for PersistentStorage {
    async fn get(&self, id: &str) -> Option<i64> {
        match self.db.get(id) {
            Ok(Some(value)) => Some(i64::from_be_bytes(value.to_vec().try_into().unwrap())),
            Ok(None) => None,
            Err(e) => panic!("operational problem encountered: {}", e),
        }
    }

    async fn set(&self, id: &str, ts: i64) {
        match self.get(id).await {
            Some(current) => {
                if current < ts {
                    self.db.put(id, ts.to_be_bytes()).unwrap();
                }
            }
            None => {
                self.db.put(id, ts.to_be_bytes()).unwrap();
            }
        }
    }

    async fn bulk_set(&self, new_data: HashMap<String, i64>) {
        for (id, ts) in new_data {
            self.set(&id, ts).await;
        }
    }

    async fn serialize(&self) -> Result<Vec<u8>> {
        let mut h: HashMap<String, i64> = HashMap::new();
        for r in self.db.iterator(rocksdb::IteratorMode::Start) {
            let (key, value) = r.unwrap();
            h.insert(
                String::from_utf8(key.to_vec()).unwrap(),
                i64::from_be_bytes(value.to_vec().try_into().unwrap()),
            );
        }
        let bin = to_allocvec(&h)?;
        Ok(bin)
    }
}

use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use axum::async_trait;
use postcard::to_allocvec;
use rocksdb::DB;

use anyhow::Result;
use tokio::sync::mpsc::Receiver;

use crate::keep_alive::{
    cluster_status::ClusterStatus,
    types::{Event, EventType},
};

use super::{memory::zset::DeviceState, Storage};
pub struct PersistentStorage {
    db: Arc<DB>,
}

impl PersistentStorage {
    pub fn new(filename: &str) -> Self {
        let db = DB::open_default(filename).unwrap();
        Self { db: Arc::new(db) }
    }

    async fn set_ts(&self, id: &str, ts: i64) {
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
}

#[async_trait]
impl Storage for PersistentStorage {
    fn start_background_tasks(&self, _cluster_status: Arc<ClusterStatus>) {}

    async fn len(&self) -> usize {
        todo!("not implemented")
    }

    async fn is_empty(&self) -> bool {
        todo!("not implemented")
    }

    async fn last_event_ts(&self) -> Option<i64> {
        todo!("not implemented")
    }

    async fn get(&self, id: &str) -> Option<DeviceState> {
        /*
        match self.db.get(id) {
            Ok(Some(value)) => Some(i64::from_be_bytes(value.to_vec().try_into().unwrap())),
            Ok(None) => None,
            Err(e) => panic!("operational problem encountered: {}", e),
        }
        */
        todo!("not implemented")
    }

    async fn set(&self, id: &str, ts: i64, _is_connection_event: bool, _from_node_msg: bool) {
        self.set_ts(id, ts).await;
    }

    async fn dead(&self, id: &str, last_ka: i64, ts_of_death: i64) {
        todo!("not implemented")
    }

    async fn bulk_set(&self, new_data: HashMap<String, DeviceState>) {
        /*
        for (id, ts) in new_data {
            self.set_ts(&id, ts).await;
        }
        */
        todo!("not implemented")
    }

    async fn serialize_state(&self) -> Result<Vec<u8>> {
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

    async fn serialize_events(&self) -> Result<Vec<u8>> {
        let e: Vec<Event> = vec![];
        let bin = to_allocvec(&e)?;
        Ok(bin)
    }

    async fn merge_events(&self, _new_data: VecDeque<Event>) {}
    async fn subscribe(&self, _offset: Option<i64>) -> Result<Receiver<Event>> {
        todo!("not implemented")
    }
}

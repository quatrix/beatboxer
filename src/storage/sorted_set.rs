use anyhow::Result;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use tracing::info;

pub struct ZSet {
    scores: HashMap<String, i64>,
    elements: BTreeMap<i64, HashSet<String>>,
}

impl ZSet {
    pub fn new() -> Self {
        ZSet {
            scores: HashMap::new(),
            elements: BTreeMap::new(),
        }
    }

    pub fn update(&mut self, value: &str, score: i64) {
        if let Some(old_score) = self.scores.get(value) {
            if old_score > &score {
                return;
            }

            if let Some(set) = self.elements.get_mut(old_score) {
                set.remove(value);
                if set.is_empty() {
                    self.elements.remove(old_score);
                }
            }
        }

        // Insert the new score for the element in the scores map
        self.scores.insert(value.to_string(), score);

        // Add the element to the new score set
        self.elements.entry(score).or_insert_with(HashSet::new);
        if let Some(set) = self.elements.get_mut(&score) {
            set.insert(value.to_string());
        }
    }

    pub fn range(&self, start: i64, end: i64) -> Vec<(&String, i64)> {
        let mut res = vec![];
        for (&score, set) in self.elements.range(start..=end) {
            for element in set {
                res.push((element, score));
            }
        }
        res
    }
}

impl Default for ZSet {
    fn default() -> Self {
        Self::new()
    }
}

use axum::async_trait;
use tokio::sync::RwLock;

use super::Storage;

pub struct NotifyingStorage {
    data: Arc<RwLock<ZSet>>,
}

impl NotifyingStorage {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(ZSet::new())),
        }
    }
}

impl Default for NotifyingStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Storage for NotifyingStorage {
    fn init(&self) {
        info!("starting notifier");
        let data_c = Arc::clone(&self.data);

        tokio::spawn(async move {
            let mut oldest_ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64
                - (120 * 1000); // older than 2m ago, we don't care

            loop {
                {
                    let data = data_c.read().await;
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as i64;

                    let dead_ids = data.range(oldest_ts, now - 20000);

                    for (id, ts) in dead_ids {
                        if ts > oldest_ts {
                            info!("dead id: {:?}", id);
                            oldest_ts = ts;
                        }
                    }
                }

                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }

    async fn get(&self, id: &str) -> Option<i64> {
        None
    }

    async fn set(&self, id: &str, ts: i64) {
        let mut data = self.data.write().await;
        data.update(id, ts);
    }

    async fn bulk_set(&self, new_data: HashMap<String, i64>) {
        // FIXME: unoptimized, but used only on SYNC
        let mut data = self.data.write().await;

        for (id, ts) in new_data {
            data.update(&id, ts);
        }
    }

    async fn serialize(&self) -> Result<Vec<u8>> {
        todo!("shouldn't be called");
    }
}

#[cfg(test)]
mod test {
    use super::ZSet;

    #[test]
    fn basics() {
        let mut zset = ZSet::new();

        zset.update("hey", 10);
        zset.update("ho", 20);
        zset.update("lets", 30);
        zset.update("go", 40);

        assert_eq!(
            zset.range(30, 50),
            vec![(&"lets".to_string(), 30), (&"go".to_string(), 40)]
        );
    }

    #[test]
    fn should_only_update_if_score_is_higher() {
        let mut zset = ZSet::new();

        zset.update("hey", 20);
        zset.update("hey", 10);

        assert_eq!(zset.range(0, 50), vec![(&"hey".to_string(), 20)]);
    }
}

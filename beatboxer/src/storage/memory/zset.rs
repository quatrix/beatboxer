use atomic_counter::{AtomicCounter, RelaxedCounter};
use crossbeam_skiplist::SkipMap;
use dashmap::DashMap;
use tracing::{debug, error};

pub struct ZSet {
    pub scores: DashMap<String, u128>,
    elements: SkipMap<u128, String>,
    counter: RelaxedCounter,
}

impl Default for ZSet {
    fn default() -> Self {
        Self::new()
    }
}

impl ZSet {
    pub fn new() -> Self {
        ZSet {
            scores: DashMap::new(),
            elements: SkipMap::new(),
            counter: RelaxedCounter::new(0),
        }
    }

    pub fn len(&self) -> usize {
        self.scores.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get(&self, value: &str) -> Option<i64> {
        self.scores.get(value).map(|v| (*v >> 64) as i64)
    }

    pub fn update(&self, value: &str, score: i64) {
        // the score is actually a timestamp in millis
        // there could be multiple updates for the same millisecond
        // the skip_list stores ts -> device_id, so to enable
        // multple devies in the same milli, we add a counter
        // for each update at the end of the millisecond.
        let original_score = score;
        let score = score as u128;
        let score: u128 = score << 64;
        let counter_lsb = self.counter.inc();
        let counter_lsb = counter_lsb as u128;
        let score = score | counter_lsb;

        match self.scores.entry(value.to_string()) {
            dashmap::mapref::entry::Entry::Occupied(mut occupied) => {
                let old_score = occupied.get();

                if old_score > &score {
                    return;
                }

                self.elements.remove(old_score);
                occupied.insert(score);
            }
            dashmap::mapref::entry::Entry::Vacant(vacant) => {
                vacant.insert(score);
            }
        }

        debug!(
            "[ZSET] storing id {} score: {} ts: {}",
            value, score, original_score
        );
        self.elements.insert(score, value.to_string());
    }

    pub fn pop_lower_than_score(&self, max_score: i64) -> Vec<(String, i64)> {
        let mut res = vec![];

        debug!("[ZSET] poping max_score: {}", max_score);
        let max_score = max_score as u128;
        let max_score = max_score << 64;

        while let Some(element) = self.elements.front() {
            let lowest_score = element.key();

            if lowest_score > &max_score {
                break;
            }

            match self.elements.pop_front() {
                Some(p_element) => {
                    let original_score = p_element.key();
                    let original_score = (original_score >> 64) as i64;

                    debug!(
                        "[ZSET] returning {} - {}",
                        p_element.value(),
                        original_score
                    );

                    res.push((p_element.value().to_string(), original_score));
                }
                None => {
                    error!("this shouldn't happen!");
                    break;
                }
            }
        }

        res
    }
}

#[cfg(test)]
mod test {
    use super::ZSet;

    #[test]
    fn basics() {
        let zset = ZSet::new();

        zset.update("hey", 10);
        zset.update("ho", 20);
        zset.update("lets", 30);
        zset.update("go", 40);

        assert_eq!(
            zset.pop_lower_than_score(30),
            vec![("hey".to_string(), 10), ("ho".to_string(), 20)]
        );
    }

    #[test]
    fn test_different_ids_with_same_timestamp() {
        let zset = ZSet::new();

        zset.update("hey", 10);
        zset.update("ho", 20);
        zset.update("lets", 20);
        zset.update("go", 40);

        let mut actual = zset.pop_lower_than_score(50);
        actual.sort_by(|a, b| (a.1, &a.0).cmp(&(b.1, &b.0)));

        assert_eq!(
            actual,
            vec![
                ("hey".to_string(), 10),
                ("ho".to_string(), 20),
                ("lets".to_string(), 20),
                ("go".to_string(), 40)
            ]
        );
    }

    #[test]
    fn should_only_update_if_score_is_higher() {
        let zset = ZSet::new();

        zset.update("hey", 20);
        zset.update("hey", 10);

        assert_eq!(zset.pop_lower_than_score(50), vec![("hey".to_string(), 20)]);
    }
}

use crossbeam_skiplist::SkipMap;
use dashmap::DashMap;
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::cell::RefCell;

thread_local! {
    static RNG: RefCell<StdRng> = RefCell::new(StdRng::from_entropy());
}

pub struct ZSet {
    pub scores: DashMap<String, u128>,
    elements: SkipMap<u128, String>,
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
        }
    }

    pub fn len(&self) -> usize {
        self.scores.len()
    }

    pub fn get(&self, value: &str) -> Option<i64> {
        self.scores.get(value).map(|v| (*v >> 64) as i64)
    }

    pub fn update(&self, value: &str, score: i64) {
        let score = score as u128;
        let score: u128 = score << 64;
        let random_lsb = RNG.with(|rng| rng.borrow_mut().gen::<u64>()) as u128;
        let score = score | random_lsb;

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

        self.elements.insert(score, value.to_string());
    }

    pub fn range(&self, start: i64, end: i64) -> Vec<(String, i64)> {
        let mut res = vec![];
        let start = start as u128;
        let end = end as u128;

        let start = start << 64;
        let end = end << 64;

        let max_u64 = u64::MAX as u128;
        let end = end | max_u64;

        for entry in self.elements.range(start..end) {
            let ts = (*entry.key() >> 64) as i64;
            let value = entry.value();

            res.push((value.to_string(), ts));
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
            zset.range(30, 50),
            vec![("lets".to_string(), 30), ("go".to_string(), 40)]
        );
    }

    #[test]
    fn test_different_ids_with_same_timestamp() {
        let zset = ZSet::new();

        zset.update("hey", 10);
        zset.update("ho", 20);
        zset.update("lets", 20);
        zset.update("go", 40);

        let mut actual = zset.range(10, 50);
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

        assert_eq!(zset.range(0, 50), vec![("hey".to_string(), 20)]);
    }
}

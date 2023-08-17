use crossbeam_skiplist::SkipMap;
use dashmap::{DashMap, DashSet};

pub struct ZSet {
    pub scores: DashMap<String, i64>,
    elements: SkipMap<i64, DashSet<String>>,
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
        self.scores.get(value).map(|v| *v)
    }

    pub fn update(&self, value: &str, score: i64) {
        match self.scores.entry(value.to_string()) {
            dashmap::mapref::entry::Entry::Occupied(mut occupied) => {
                // if there's a score already, get it
                let old_score = occupied.get();

                if old_score > &score {
                    // if current score is older than prev score, ignore it, it's a laggy event
                    return;
                }

                // get the set based on the old score
                if let Some(set) = self.elements.get(old_score) {
                    // remove the old value from the set, and if the set is empty remove the whole
                    // set.
                    let set_l = set.value();
                    set_l.remove(value);
                    if set_l.is_empty() {
                        self.elements.remove(old_score);
                    }
                }
                occupied.insert(score);
            }
            dashmap::mapref::entry::Entry::Vacant(vacant) => {
                vacant.insert(score);
            }
        };

        let entry = self.elements.get_or_insert(score, DashSet::new());

        let set_l = entry.value();
        set_l.insert(value.to_string());
    }

    pub fn range(&self, start: i64, end: i64) -> Vec<(String, i64)> {
        let mut res = vec![];

        for entry in self.elements.range(start..end) {
            let score = *entry.key();
            let set_l = &*entry.value();

            for element in set_l.iter() {
                res.push((element.to_string(), score));
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
            zset.range(30, 50),
            vec![("lets".to_string(), 30), ("go".to_string(), 40)]
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

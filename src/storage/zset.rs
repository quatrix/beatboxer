use std::collections::{BTreeMap, HashMap, HashSet};

pub struct ZSet {
    pub scores: HashMap<String, i64>,
    elements: BTreeMap<i64, HashSet<String>>,
}

impl Default for ZSet {
    fn default() -> Self {
        Self::new()
    }
}

impl ZSet {
    pub fn new() -> Self {
        ZSet {
            scores: HashMap::new(),
            elements: BTreeMap::new(),
        }
    }

    pub fn get(&self, value: &str) -> Option<&i64> {
        self.scores.get(value)
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

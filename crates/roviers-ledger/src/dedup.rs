use std::collections::{HashSet, VecDeque};
use std::sync::RwLock;

/// Thread-safe deduplication cache with sliding window capacity
pub struct DedupLog {
    max_entries: usize,
    entries: RwLock<DedupState>,
}

struct DedupState {
    set: HashSet<String>,
    queue: VecDeque<String>,
}

impl DedupLog {
    pub fn new(max_entries: usize) -> Self {
        Self {
            max_entries: max_entries.max(1),
            entries: RwLock::new(DedupState {
                set: HashSet::with_capacity(max_entries),
                queue: VecDeque::with_capacity(max_entries),
            }),
        }
    }

    /// Returns `true` if the key was already present (duplicate).
    /// If not present, records it and returns `false`.
    pub fn check_and_record(&self, key: &str) -> bool {
        let mut state = self.entries.write().unwrap();
        if state.set.contains(key) {
            return true;
        }

        while state.queue.len() >= self.max_entries {
            if let Some(oldest) = state.queue.pop_front() {
                state.set.remove(&oldest);
            }
        }

        let key_str = key.to_string();
        state.set.insert(key_str.clone());
        state.queue.push_back(key_str);
        false
    }

    pub fn len(&self) -> usize {
        self.entries.read().unwrap().set.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dedup_log() {
        let dedup = DedupLog::new(3);
        assert!(!dedup.check_and_record("req_1"));
        assert!(dedup.check_and_record("req_1")); // duplicate

        assert!(!dedup.check_and_record("req_2"));
        assert!(!dedup.check_and_record("req_3"));

        // Evicts req_1
        assert!(!dedup.check_and_record("req_4"));
        // req_1 should be treated as fresh now
        assert!(!dedup.check_and_record("req_1"));
    }
}

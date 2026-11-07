use std::collections::HashMap;
use std::sync::RwLock;
use tokio::sync::broadcast;

/// Topic routing trie supporting dot-separated wildcards (e.g. `service.identity.*`)
pub struct TopicTrie {
    root: RwLock<TrieNode>,
}

struct TrieNode {
    children: HashMap<String, TrieNode>,
    wildcard_child: Option<Box<TrieNode>>,
    sender: Option<broadcast::Sender<u64>>,
}

impl TrieNode {
    fn new() -> Self {
        Self {
            children: HashMap::new(),
            wildcard_child: None,
            sender: None,
        }
    }
}

impl TopicTrie {
    pub fn new() -> Self {
        Self {
            root: RwLock::new(TrieNode::new()),
        }
    }

    /// Subscribe to a topic path. Returns a broadcast receiver of ledger record offsets.
    pub fn subscribe(&self, topic_pattern: &str) -> broadcast::Receiver<u64> {
        let mut current = self.root.write().unwrap();
        let segments: Vec<&str> = topic_pattern.split('.').collect();

        let mut node = &mut *current;
        for segment in segments {
            if segment == "*" {
                node = node
                    .wildcard_child
                    .get_or_insert_with(|| Box::new(TrieNode::new()));
            } else {
                node = node
                    .children
                    .entry(segment.to_string())
                    .or_insert_with(TrieNode::new);
            }
        }

        if let Some(ref tx) = node.sender {
            tx.subscribe()
        } else {
            let (tx, rx) = broadcast::channel(10000);
            node.sender = Some(tx);
            rx
        }
    }

    /// Publish a ledger record offset to all matching topic subscribers
    pub fn publish(&self, topic: &str, record_offset: u64) {
        let root = self.root.read().unwrap();
        let segments: Vec<&str> = topic.split('.').collect();
        Self::dispatch(&root, &segments, 0, record_offset);
    }

    fn dispatch(node: &TrieNode, segments: &[&str], index: usize, offset: u64) {
        if index == segments.len() {
            if let Some(ref tx) = node.sender {
                let _ = tx.send(offset);
            }
            return;
        }

        let seg = segments[index];

        // Exact match
        if let Some(child) = node.children.get(seg) {
            Self::dispatch(child, segments, index + 1, offset);
        }

        // Wildcard match
        if let Some(ref wildcard) = node.wildcard_child {
            Self::dispatch(wildcard, segments, index + 1, offset);
        }
    }
}

impl Default for TopicTrie {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_topic_routing() {
        let trie = TopicTrie::new();
        let mut rx_exact = trie.subscribe("service.identity.passport");
        let mut rx_wild = trie.subscribe("service.identity.*");

        trie.publish("service.identity.passport", 42);

        assert_eq!(rx_exact.recv().await.unwrap(), 42);
        assert_eq!(rx_wild.recv().await.unwrap(), 42);
    }
}

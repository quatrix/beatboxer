use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::Serialize;

#[derive(Clone, Debug, Serialize, Eq, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum NodeStatus {
    Initializing,
    Synching,
    Synched,
    SyncFailed,
    Dead,
}

#[derive(Clone, Debug, Serialize)]
pub struct Node {
    status: NodeStatus,
    status_since: Option<DateTime<Utc>>,
    last_ping: Option<DateTime<Utc>>,
    last_sync: Option<DateTime<Utc>>,
}

#[derive(Serialize)]
pub struct ClusterStatus {
    up_since: DateTime<Utc>,
    nodes: DashMap<String, Node>,
}

impl ClusterStatus {
    pub fn new(nodes: Vec<String>) -> Self {
        let initial = DashMap::new();

        for node in nodes {
            initial.insert(
                node,
                Node {
                    status: NodeStatus::Initializing,
                    status_since: None,
                    last_ping: None,
                    last_sync: None,
                },
            );
        }

        ClusterStatus {
            up_since: Utc::now(),
            nodes: initial,
        }
    }

    pub fn is_ready(&self) -> bool {
        // naive logic: we're ready when the connection
        // to the other nodes isn't Initializing, meaning
        // if we're synched with all of them, or all are dead
        // we can start serving.

        for entry in self.nodes.iter() {
            match entry.value().status {
                NodeStatus::Initializing | NodeStatus::Synching | NodeStatus::SyncFailed => {
                    return false;
                }
                NodeStatus::Dead | NodeStatus::Synched => {}
            }
        }

        true
    }

    pub fn set_node_status(&self, node: &str, new_status: NodeStatus) {
        let mut m = self.nodes.get_mut(&node.to_string()).unwrap();
        let node = m.value_mut();
        node.status = new_status;
        node.status_since = Some(Utc::now());
    }

    pub fn update_last_ping(&self, node: &str) {
        let mut m = self.nodes.get_mut(&node.to_string()).unwrap();
        let node = m.value_mut();
        node.last_ping = Some(Utc::now());
    }

    pub fn update_last_sync(&self, node: &str) {
        let mut m = self.nodes.get_mut(&node.to_string()).unwrap();
        let node = m.value_mut();
        node.last_sync = Some(Utc::now());
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_is_ready_when_all_nodes_are_synched() {
        let cs = ClusterStatus::new(vec!["node_a".to_string(), "node_b".to_string()]);

        // initially all nodes are Initializing, so cluster isn't ready
        assert!(!cs.is_ready());

        cs.set_node_status("node_a", NodeStatus::Synched);
        cs.set_node_status("node_b", NodeStatus::Synched);

        // when all nodes are sync, cluster is ready
        assert!(cs.is_ready());
    }

    #[test]
    fn test_is_ready_when_all_nodes_are_dead() {
        let cs = ClusterStatus::new(vec!["node_a".to_string(), "node_b".to_string()]);

        // initially all nodes are Initializing, so cluster isn't ready
        assert!(!cs.is_ready());

        cs.set_node_status("node_a", NodeStatus::Dead);
        cs.set_node_status("node_b", NodeStatus::Dead);

        // when all nodes are dead, cluster is ready
        // because we're probably the only node
        assert!(cs.is_ready());
    }

    #[test]
    fn test_is_not_ready_when_some_nodes_fail_to_sync() {
        let cs = ClusterStatus::new(vec!["node_a".to_string(), "node_b".to_string()]);

        // initially all nodes are Initializing, so cluster isn't ready
        assert!(!cs.is_ready());

        cs.set_node_status("node_a", NodeStatus::Dead);
        cs.set_node_status("node_b", NodeStatus::SyncFailed);

        // when some nodes fail sync, it means they're alive
        // but we can't for some reason sync with them
        // it means we won't be getting this masters updates
        // so our data will be partial, better to not be ready at all in this case.
        assert!(!cs.is_ready());
    }
}

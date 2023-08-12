use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::Serialize;

#[derive(Clone, Debug, Serialize, Eq, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum NodeStatus {
    Initializing,
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
            if entry.value().status == NodeStatus::Initializing {
                return false;
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

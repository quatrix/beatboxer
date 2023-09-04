use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    sync::Arc,
};

use crate::{config::Config, event::Event};
use tracing::{debug, error, info, warn};

use super::Strategy;

pub struct Original {
    node: String,
    config: Arc<Config>,
    waiting_death: HashMap<String, i32>,
    connected_counter: HashMap<String, i32>,
    dead_counter: HashMap<String, i32>,
    connected_ts: HashMap<String, i64>,
    issues: i32,
    extra_connected: i32,
    dead_before_connected: i32,
    pairs: i32,
    events: Vec<String>,
    ids: HashSet<String>,
}

impl Original {
    pub fn new(node: String, config: Arc<Config>) -> Self {
        Original {
            node,
            config,
            waiting_death: HashMap::new(),
            connected_counter: HashMap::new(),
            dead_counter: HashMap::new(),
            connected_ts: HashMap::new(),
            issues: 0,
            extra_connected: 0,
            dead_before_connected: 0,
            pairs: 0,
            events: Vec::new(),
            ids: HashSet::new(),
        }
    }
}

impl Strategy for Original {
    fn get_name(&self) -> String {
        self.node.clone()
    }

    fn add_event(&mut self, event: String) {
        self.events.push(event);
    }

    fn on_connect(&mut self, id: String, ts: i64) {
        self.ids.insert(id.clone());
        *self.waiting_death.entry(id.to_string()).or_insert(0) += 1;
        *self.connected_counter.entry(id.to_string()).or_insert(0) += 1;
        self.connected_ts.insert(id.to_string(), ts);
    }

    fn on_dead(&mut self, id: String, ts: i64) {
        self.ids.insert(id.clone());
        *self.dead_counter.entry(id.to_string()).or_insert(0) += 1;

        // if got dead, there should be exactly one connected.
        match self.waiting_death.entry(id.to_string()) {
            Entry::Occupied(mut e) => {
                match *e.get() {
                    0 => {
                        // got DEAD but wasn't waiting for it
                        info!("[{}] - [{}] got DEAD before CONNECTED.", self.node, id,);
                        self.dead_before_connected += 1;
                    }
                    1 => {
                        // got DEAD and was waiting for it
                        match self.connected_ts.get(&id) {
                            Some(c_ts) => {
                                let delta = ts - c_ts;

                                if delta < 5000 {
                                    error!(
                                        "[{}] - [{}] got dead event too soon! (delta: {})",
                                        self.node, id, delta
                                    );
                                    self.issues += 1;
                                } else if delta
                                    > (((self.config.rounds) as i64
                                        * self.config.time_between_beats_ms as i64)
                                        + 30000
                                        + 5000)
                                {
                                    error!(
                                        "[{}] - [{}] got dead event too late! (delta: {})",
                                        self.node, id, delta
                                    );
                                    self.issues += 1;
                                }
                            }
                            None => {
                                error!("[{}] - [{}] wat, no ts??", self.node, id);
                                self.issues += 1;
                            }
                        }

                        self.pairs += 1;
                        *e.get_mut() -= 1;
                    }
                    n => {
                        // got DEAD but there was multiple connects before
                        info!(
                            "[{}] - [{}] got DEAD but waiting dead count is > 1: {}",
                            self.node, id, n
                        );
                        self.extra_connected += 1;
                    }
                }
            }
            Entry::Vacant(_) => {
                debug!("[{}] - [{}] got DEAD before CONNECTED", self.node, id);
                self.dead_before_connected += 1;
            }
        }
    }

    fn get_events(&self) -> Vec<String> {
        self.events.clone()
    }

    fn get_grouped_events(&self) -> HashMap<String, Vec<Event>> {
        HashMap::new()
    }
}

impl Original {
    fn print_report(&self) {
        let mut total_connected = 0;
        let mut total_dead = 0;
        let mut missing_connected = 0;
        let mut missing_dead = 0;

        for id in &self.ids {
            let connected = match self.connected_counter.get(id) {
                Some(c) => c,
                None => {
                    missing_connected += 1;
                    debug!("[{}] - missing connected! {}", self.node, id);
                    continue;
                }
            };

            let dead = match self.dead_counter.get(id) {
                Some(c) => c,
                None => {
                    missing_dead += 1;
                    warn!("[{}] missing dead! {}", self.node, id);
                    continue;
                }
            };

            total_connected += connected;
            total_dead += dead;
        }

        if missing_connected == 0 {
            info!("{} - no missing connected. ✅", self.node);
        } else {
            error!(
                "{} - oh no, {} missing connected. ❌",
                self.node, missing_connected
            );
        }

        if missing_dead == 0 {
            info!("{} - no missing dead. ✅", self.node);
        } else {
            error!("{} - oh no, {} missing dead. ❌", self.node, missing_dead);
        }

        if self.extra_connected == 0 {
            info!("{} - no extra connected. ✅", self.node);
        } else {
            error!(
                "{} - oh no, {} extra connected. ❌",
                self.node, self.extra_connected
            );
        }

        if total_connected == total_dead {
            info!(
                "{} - total connected == total dead == {}. ✅",
                self.node, total_connected
            );
        } else {
            match total_connected > total_dead {
                true => {
                    error!(
                        "{} - got {} more connected than dead. (connected: {} dead: {}). ❌",
                        self.node,
                        total_connected - total_dead,
                        total_connected,
                        total_dead
                    );
                }
                false => {
                    error!(
                        "{} - got {} more dead than connected. (connected: {} dead: {}). ❌",
                        self.node,
                        total_dead - total_connected,
                        total_connected,
                        total_dead
                    );
                }
            }
        }

        if self.dead_before_connected == 0 {
            info!("{} - no dead before connected. ✅", self.node);
        } else {
            error!(
                "{} - oh no, {} dead before connected ❌",
                self.node, self.dead_before_connected
            );
        }

        if self.issues == 0 {
            info!("{} - for each connect there's a dead. ✅", self.node);
        } else {
            error!("{} - oh no, {} issues. ❌", self.node, self.issues);
        }

        if (self.pairs as usize) == self.config.total_ids {
            info!(
                "{} - got expected number of pairs {} ✅",
                self.node, self.pairs
            )
        } else {
            error!(
                "{} - oh no, got {} pairs, expected {} ❌",
                self.node, self.pairs, self.config.total_ids
            );
        }

        if self.ids.len() == self.config.total_ids {
            info!(
                "{} - got the expected number of ids {} ✅",
                self.node,
                self.ids.len()
            );
        } else {
            error!(
                "{} - number of ids {} isn't what's expected {} ❌",
                self.node,
                self.ids.len(),
                self.config.total_ids,
            );
        }
    }
}

use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::Duration,
};

use crate::{
    keep_alive::{
        cluster_status::ClusterStatus,
        constants::{CONSOLIDATION_WINDOW, DEAD_DEVICE_TIMEOUT},
        types::{DeadUpdate, Event, EventType, Message},
        SenderChannels,
    },
    notification_dispatcher::NotificationDispatcher,
    storage::memory::zset::State,
};

use self::{
    events::Events,
    events_buffer::EventsBuffer,
    zset::{DeviceState, ZSet},
};

use super::Storage;
use anyhow::Result;
use axum::async_trait;
use postcard::to_allocvec;
use tokio::sync::{
    mpsc::{error::TrySendError, Receiver},
    RwLock,
};
use tracing::{debug, error, info, warn};

pub mod events;
pub mod events_buffer;
pub mod zset;

pub struct InMemoryStorage {
    txs: SenderChannels,
    keep_alives: Arc<ZSet>,
    events_buffer: Arc<EventsBuffer>,
    events_history: Arc<Events>,
    notification_dispatcher: Arc<NotificationDispatcher>,
    max_history_size: usize,
    dead_lock: Arc<RwLock<()>>,
}

impl InMemoryStorage {
    pub fn new(max_history_size: usize, txs: SenderChannels) -> Self {
        let events_history = Arc::new(Events::new(max_history_size));
        let notification_dispatcher =
            Arc::new(NotificationDispatcher::new(Arc::clone(&events_history)));

        Self {
            txs,
            keep_alives: Arc::new(ZSet::new()),
            events_buffer: Arc::new(EventsBuffer::new()),
            events_history: Arc::clone(&events_history),
            notification_dispatcher: Arc::clone(&notification_dispatcher),
            max_history_size,
            dead_lock: Arc::new(RwLock::new(())),
        }
    }

    async fn set_ka(&self, id: &str, ts: i64, state: Option<State>) {
        self.keep_alives.update(id, ts, state);
    }

    fn start_consolidator(&self, cluster_status: Arc<ClusterStatus>) {
        let events_buffer = Arc::clone(&self.events_buffer);
        let events_history = Arc::clone(&self.events_history);
        let notification_dispatcher = Arc::clone(&self.notification_dispatcher);

        tokio::spawn(async move {
            let consolidation_ms = CONSOLIDATION_WINDOW.as_millis() as i64;

            loop {
                if cluster_status.is_ready() {
                    info!("[🧠] cluster is ready! starting consolidator");
                    break;
                } else {
                    info!("[🧠] cluster isn't ready yet...");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }

            loop {
                {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as i64;

                    let buffer_until = now - consolidation_ms;
                    let consolidated_events = events_buffer.consolidate(buffer_until).await;

                    for event in consolidated_events {
                        debug!("sending event: {:?}", event);
                        match events_history.store_event(event.clone()).await {
                            Ok(_) => notification_dispatcher.notify(&event).await,
                            Err(e) => error!("error while inserting to history: {:?}", e),
                        }
                    }
                }

                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }

    fn watch_for_dead(&self, cluster_status: Arc<ClusterStatus>) {
        let keep_alives_c = Arc::clone(&self.keep_alives);
        //let events_buffer = Arc::clone(&self.events_buffer);
        let events_history = Arc::clone(&self.events_history);
        let notification_dispatcher = Arc::clone(&self.notification_dispatcher);
        let txs = Arc::clone(&self.txs);
        let dead_lock = Arc::clone(&self.dead_lock);

        tokio::spawn(async move {
            loop {
                if cluster_status.is_ready() {
                    info!("[💀] cluster is ready! starting death watcher");
                    break;
                } else {
                    info!("[💀] cluster isn't ready yet...");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }

            let dead_ms = DEAD_DEVICE_TIMEOUT.as_millis() as i64;

            /*
            let end_of_history = match events_history.last_ts().await {
                Some(ts) => ts,
                None => start_of_watch,
            };
            */

            loop {
                {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as i64;

                    let end = now - dead_ms;
                    let dead_ids = keep_alives_c.pop_lower_than_score(end);
                    let start_of_watch = (std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        - *DEAD_DEVICE_TIMEOUT)
                        .as_millis() as i64;

                    for (id, ts) in dead_ids {
                        let deadline = ts + dead_ms;

                        // XXX: ignoring all events that would have happened
                        // before the last event in history
                        // this is needed when a node syncs with a peer and
                        // and the zset get's populated with ts already in the history
                        //if deadline < start_of_watch {
                        //    debug!("skipping id: {} ts: {}", id, ts);
                        //    continue;
                        //}

                        match keep_alives_c.get(&id) {
                            Some(ds) => {
                                //let _lock = dead_lock.write().await;
                                //info!("detected dead for {} (ds: {:?})", id, ds);

                                let event = Event {
                                    ts: deadline,
                                    id: id.to_string(),
                                    typ: EventType::Dead,
                                };

                                if ds.state == EventType::Connected {
                                    //events_buffer.store_event(event.clone()).await;
                                    notification_dispatcher.notify(&event).await;
                                    let _ = events_history.store_event(event.clone()).await;

                                    keep_alives_c.update_state(
                                        &id,
                                        State {
                                            state: EventType::Dead,
                                            state_ts: deadline,
                                        },
                                    );

                                    let txs = txs.read().await;

                                    let dead_update = DeadUpdate {
                                        id: id.clone(),
                                        last_ka: ts,
                                        ts_of_death: deadline,
                                    };

                                    for (addr, tx) in txs.iter().filter(|(_, tx)| !tx.is_closed()) {
                                        match tx.try_send(Message::DeadUpdate(dead_update.clone()))
                                        {
                                            Ok(_) => {}
                                            Err(TrySendError::Closed(_)) => {
                                                error!("[{}] (KA) Channel closed.", addr)
                                            }
                                            Err(TrySendError::Full(_)) => {
                                                error!("[{}] (KA) Channel full.", addr);
                                                metrics::increment_counter!("channel_full", "op" => "dd", "addr" => addr.clone());
                                            }
                                        }
                                    }
                                } else {
                                    //let events = events_history.get_all_events_from_id(&id).await;
                                    //info!(
                                    //    "ignoring dead for {}, because of previous state: {:?} [events: {:?}]",
                                    //    id, ds, events
                                    //);
                                    //
                                    //
                                    //
                                    //if ds.state == EventType::Dead && ds.state_ts == ts {
                                    //    warn!(
                                    //        "resending dead event id: {:?} ts: {:?} ds: : {:?}",
                                    //        id, ts, ds
                                    //    );

                                    //    notification_dispatcher.notify(&event).await;
                                    //}
                                }
                            }
                            None => {
                                //info!(
                                //    "logic error: dead but without a previous state?? id: {}",
                                //    id
                                //)
                            }
                        }
                    }
                }

                tokio::time::sleep(Duration::from_millis(1000)).await;
            }
        });
    }
}

#[async_trait]
impl Storage for InMemoryStorage {
    async fn len(&self) -> usize {
        self.keep_alives.len()
    }

    async fn last_event_ts(&self) -> Option<i64> {
        self.events_history.last_ts().await
    }

    async fn is_empty(&self) -> bool {
        self.keep_alives.len() == 0
    }

    async fn get(&self, id: &str) -> Option<DeviceState> {
        self.keep_alives.get(id)
    }

    async fn dead(&self, id: &str, last_ka: i64, ts_of_death: i64) {
        // FIXME: just testing if this helps
        // in reality we need a lock per id, not a global one lock.
        //let _lock = self.dead_lock.write().await;

        let dead = State {
            state: EventType::Dead,
            state_ts: ts_of_death,
        };

        let event = Event {
            ts: ts_of_death,
            id: id.to_string(),
            typ: EventType::Dead,
        };

        match self.keep_alives.get(id) {
            Some(ds) => {
                if ds.state == EventType::Dead {
                    //let events = self.events_history.get_all_events_from_id(id).await;
                    //error!("it's a dead event, but last event is already a dead event! ignoring. id: {:?} ds: {:?} [events: {:?}]", id, ds, events);
                    //

                    //if ds.state_ts == ts_of_death {
                    //    warn!(
                    //        "resending dead event id: {:?} ts: {:?} ds: : {:?}",
                    //        id, ts_of_death, ds
                    //    );

                    //    self.notification_dispatcher.notify(&event).await;
                    //}
                } else {
                    //info!("storing dead for {} (ds: {:?})", id, ds);
                    self.notification_dispatcher.notify(&event).await;
                    let _ = self.events_history.store_event(event.clone()).await;
                    self.keep_alives.update_state(id, dead);
                }
            }
            None => {
                warn!("ignoring dead for {} because of no state", id);
                /*
                self.set_ka(id, last_ka, Some(dead)).await;

                debug!("events_buffer.store_event({:?})", event);
                let _ = self.events_history.store_event(event.clone()).await;
                self.notification_dispatcher.notify(&event).await;
                */
            }
        }
    }

    async fn set(&self, id: &str, ts: i64, is_connection_event: bool) {
        if is_connection_event {
            let connected = State {
                state: EventType::Connected,
                state_ts: ts,
            };

            let event = Event {
                ts,
                id: id.to_string(),
                typ: EventType::Connected,
            };

            match self.keep_alives.get(id) {
                Some(ds) => {
                    // only store if previous state is dead
                    // FIXME: maybe we need to check the udpate time of that event... maybe it's
                    // old

                    if ds.state == EventType::Dead {
                        debug!("events_buffer.store_event({:?})", event);
                        //self.events_buffer.store_event(event.clone()).await;
                        self.notification_dispatcher.notify(&event).await;
                        let _ = self.events_history.store_event(event.clone()).await;
                        self.set_ka(id, ts, Some(connected)).await;
                    } else {
                        //let events = self.events_history.get_all_events_from_id(id).await;
                        //error!("connected event, but last connected event! ignoring. id: {:?} ts: {:?} ds: : {:?} [events: {:?}]", id, ts, ds, events);

                        // if it's the same event, send it again, for good measure
                        // if ds.state == EventType::Connected && ds.state_ts == ts {
                        //     warn!(
                        //         "resending connect event id: {:?} ts: {:?} ds: : {:?}",
                        //         id, ts, ds
                        //     );
                        //     self.notification_dispatcher.notify(&event).await;
                        // }
                    }
                }
                None => {
                    let event = Event {
                        ts,
                        id: id.to_string(),
                        typ: EventType::Connected,
                    };

                    debug!("events_buffer.store_event({:?})", event);
                    //self.events_buffer.store_event(event.clone()).await;
                    self.notification_dispatcher.notify(&event).await;
                    let _ = self.events_history.store_event(event.clone()).await;
                    self.set_ka(id, ts, Some(connected)).await;
                }
            }
        } else {
            self.set_ka(id, ts, None).await;
        }
    }

    async fn bulk_set(&self, new_data: HashMap<String, DeviceState>) {
        let mut counter = 0;

        for (id, ds) in new_data {
            counter += 1;

            let ts = ds.ts;

            let state = State {
                state: ds.state,
                state_ts: ds.state_ts,
            };

            self.set_ka(&id, ts, Some(state)).await;

            if counter % 10000 == 0 {
                tokio::task::yield_now().await;
            }
        }
    }

    async fn merge_events(&self, new_data: VecDeque<Event>) {
        self.events_history.merge(&new_data).await;
    }

    async fn serialize_state(&self) -> Result<Vec<u8>> {
        let t0 = std::time::Instant::now();

        let mut scores: HashMap<String, DeviceState> = HashMap::new();

        for element in self.keep_alives.scores.iter() {
            scores.insert(element.key().to_string(), element.value().clone());
        }

        let bin = to_allocvec(&scores)?;
        info!(
            "Serialized state in {:.2} secs ({} keys)",
            t0.elapsed().as_secs_f32(),
            self.keep_alives.scores.len()
        );
        Ok(bin)
    }

    async fn serialize_events(&self) -> Result<Vec<u8>> {
        self.events_history.serialize().await
    }

    async fn subscribe(&self, offset: Option<i64>) -> Result<Receiver<Event>> {
        let buffer_size = self.max_history_size + 500_000;

        self.notification_dispatcher
            .add_subscriber(buffer_size, offset)
            .await
    }

    fn start_background_tasks(&self, cluster_status: Arc<ClusterStatus>) {
        //self.start_consolidator(Arc::clone(&cluster_status));
        self.watch_for_dead(cluster_status);
        self.notification_dispatcher.monitor();
    }
}

#[cfg(test)]
mod test {
    use super::InMemoryStorage;
    use crate::storage::Storage;

    #[tokio::test]
    async fn test_inserting_stores_only_newer_timestamps() {
        let storage = InMemoryStorage::new(1);
        storage.set("hey", 10, false).await;
        assert_eq!(storage.get("hey").await.unwrap(), 10);

        // 5 is older than 10, so should keep 10
        storage.set("hey", 5, false).await;
        assert_eq!(storage.get("hey").await.unwrap(), 10);

        // 15 is newer, so it should get set
        storage.set("hey", 15, false).await;
        assert_eq!(storage.get("hey").await.unwrap(), 15);
    }
}

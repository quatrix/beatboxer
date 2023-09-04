use std::{
    cmp::Ordering,
    collections::{hash_map::DefaultHasher, BinaryHeap, HashMap, HashSet},
    hash::{Hash, Hasher},
    time::{Duration, SystemTime},
};

use kanal::{AsyncReceiver, AsyncSender};
use rand::Rng;
use tracing::error;

use crate::event::{Event, EventType};

#[derive(Debug)]
pub struct Settings {
    pub interval: Duration,
    pub rounds: u32,
    pub skip_probability: f32,
    pub death_probability: f32,
    pub death_rounds: u32,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Task {
    pub id: String,
    pub next_tick: SystemTime,
    pub ticks: u32,
}

impl Ord for Task {
    fn cmp(&self, other: &Self) -> Ordering {
        (other.next_tick).cmp(&(self.next_tick))
    }
}

impl PartialOrd for Task {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

fn initial_tasks(ids: Vec<String>, interval: Duration) -> Vec<Task> {
    let millis = interval.as_millis() as u64;
    let mut tasks = Vec::new();
    let now = SystemTime::now();

    for id in ids {
        let ts = calculate_hash(&id) % millis;
        let next_tick = now + Duration::from_millis(ts);
        tasks.push(Task {
            id: id.clone(),
            next_tick,
            ticks: 0,
        });
    }

    tasks
}

fn rand() -> f32 {
    let mut rng = rand::thread_rng();
    rng.gen()
}

async fn next_task(task: &Task, settings: &Settings) -> (Task, EventType) {
    let now = SystemTime::now();
    let event;

    let next_tick = if rand() < settings.skip_probability {
        event = EventType::Skip;
        now + (settings.interval * 2)
    } else if rand() < settings.death_probability {
        event = EventType::Dead;
        now + (settings.interval * settings.death_rounds)
    } else {
        event = EventType::Beat;
        now + settings.interval
    };

    let new_task = Task {
        id: task.id.clone(),
        next_tick,
        ticks: task.ticks + 1,
    };

    (new_task, event)
}

async fn write_ledger(
    id: &str,
    ts: SystemTime,
    event_type: EventType,
    ledger_tx: &AsyncSender<Event>,
) {
    let _ = ledger_tx
        .send(Event {
            id: id.to_string(),
            ts,
            event: event_type,
        })
        .await;
}

pub fn schedule(
    ids: Vec<String>,
    settings: Settings,
) -> (AsyncReceiver<String>, AsyncReceiver<Event>) {
    let (tx, rx) = kanal::unbounded_async();
    let (ledger_tx, ledger_rx) = kanal::unbounded_async();
    let tasks = initial_tasks(ids, settings.interval);
    let mut ring = BinaryHeap::with_capacity(tasks.len());

    for task in &tasks {
        ring.push(task.clone())
    }

    tokio::spawn(async move {
        let mut seen = HashSet::new();
        let mut last_event = HashMap::new();

        while let Some(task) = ring.pop() {
            let now = SystemTime::now();

            let time_until_deadline = match task.next_tick.duration_since(now) {
                Ok(d) => d,
                Err(_) => Duration::from_millis(0),
            };

            if time_until_deadline > Duration::from_millis(0) {
                //debug!("sleeping for {:?}", time_until_deadline);
                tokio::time::sleep(time_until_deadline).await;
            }

            // sending the beat
            if let Err(e) = tx.send(task.id.clone()).await {
                error!("got error while tx.send from scheduler: {:?}", e);
                break;
            }

            // if this is the first time we're sending a beat
            // from this id, this means it's a CONNECT event
            // if it's not the first time, it's just a BEAT event
            if !seen.contains(&task.id) {
                last_event.insert(task.id.clone(), EventType::Connect);
                write_ledger(&task.id, task.next_tick, EventType::Connect, &ledger_tx).await;
                seen.insert(task.id.clone());
            } else {
                last_event.insert(task.id.clone(), EventType::Beat);
                write_ledger(&task.id, task.next_tick, EventType::Beat, &ledger_tx).await;
            }

            // getting the new task
            let (new_task, event) = next_task(&task, &settings).await;

            if new_task.ticks <= settings.rounds {
                ring.push(new_task);

                if event == EventType::Dead {
                    seen.remove(&task.id);

                    if let Some(le) = last_event.get(&task.id) {
                        if *le != EventType::Dead {
                            last_event.insert(task.id.clone(), EventType::Dead);
                            write_ledger(&task.id, SystemTime::now(), EventType::Dead, &ledger_tx)
                                .await;
                        }
                    }
                }
            } else {
                // no more beats for this id

                if let Some(le) = last_event.get(&task.id) {
                    if *le != EventType::Dead {
                        last_event.insert(task.id.clone(), EventType::Dead);
                        write_ledger(&task.id, SystemTime::now(), EventType::Dead, &ledger_tx)
                            .await;
                    }
                }
            }
        }
    });

    (rx, ledger_rx)
}

pub async fn group_ledgers_by_id(ledger_rx: &AsyncReceiver<Event>) -> HashMap<String, Vec<Event>> {
    let mut res = HashMap::new();

    while let Ok(event) = ledger_rx.recv().await {
        res.entry(event.id.to_string())
            .or_insert(Vec::new())
            .push(event.clone())
    }

    res
}

#[cfg(test)]
mod test {
    use std::collections::{hash_map::Entry, HashMap};

    use super::*;

    #[tokio::test]
    async fn test_basics() {
        let ids = (0..2000).map(|i| format!("{:08x}", i)).collect();
        let settings = Settings {
            interval: Duration::from_millis(100),
            rounds: 20000,
            death_probability: 0.05,
            skip_probability: 0.1,
            death_rounds: 2,
        };
        let (rx, ledger_rx) = schedule(ids, settings);
        let mut tracker = HashMap::new();

        loop {
            match rx.recv().await {
                Ok(id) => {
                    match tracker.entry(id.clone()) {
                        Entry::Occupied(mut e) => {
                            eprintln!(
                                "id: {:?} delta: {:?}",
                                e.key(),
                                SystemTime::now().duration_since(*e.get()).unwrap()
                            );
                            e.insert(SystemTime::now());
                        }
                        Entry::Vacant(e) => {
                            eprintln!("id: {:?}", e.key());
                            e.insert(SystemTime::now());
                        }
                    };
                }
                Err(_) => {
                    eprintln!("bye");
                    break;
                }
            }
        }

        let grouped = group_ledgers_by_id(&ledger_rx).await;

        for (id, log) in grouped {
            eprintln!("{}", id);

            for e in log {
                eprintln!("\t{:?}", e);
            }

            //eprintln!("\t{:?}", log.iter().last());
        }
    }
}

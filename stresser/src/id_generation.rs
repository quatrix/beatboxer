use std::time::Duration;

use crate::{
    config::Config,
    event::Event,
    scheduler::{self, schedule},
};

pub fn generate_ids(
    config: &Config,
) -> (kanal::AsyncReceiver<String>, kanal::AsyncReceiver<Event>) {
    let total_ids = config.total_ids;
    let rounds = config.rounds;

    let ids = (0..total_ids).map(|i| format!("{:08x}", i)).collect();

    schedule(
        ids,
        scheduler::Settings {
            interval: Duration::from_millis(config.time_between_beats_ms),
            rounds,
            skip_probability: config.skip_probability,
            death_probability: config.death_probability,
            death_rounds: config.death_rounds,
        },
    )
}

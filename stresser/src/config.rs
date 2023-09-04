use clap::Parser;

#[derive(Parser, Debug)]
pub struct Config {
    #[arg(short, long, required = true)]
    pub nodes: Vec<String>,

    #[arg(long, default_value = "300")]
    pub pulse_workers: i32,

    #[arg(long, default_value = "100")]
    pub check_workers: i32,

    #[arg(long, default_value = "100000")]
    pub total_ids: usize,

    #[arg(long, default_value = "20")]
    pub rounds: u32,

    #[arg(long, default_value = "10000")]
    pub time_between_beats_ms: u64,

    #[arg(long, default_value = "0.1")]
    pub skip_probability: f32,

    #[arg(long, default_value = "0.05")]
    pub death_probability: f32,

    #[arg(long, default_value = "5")]
    pub death_rounds: u32,
}

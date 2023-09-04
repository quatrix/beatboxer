use std::collections::HashMap;

use crate::event::Event;

pub mod ledger;
pub mod original;

pub trait Strategy {
    fn add_event(&mut self, event: String);
    fn on_connect(&mut self, id: String, ts: i64);
    fn on_dead(&mut self, id: String, ts: i64);
    fn get_events(&self) -> Vec<String>;
    fn get_grouped_events(&self) -> HashMap<String, Vec<Event>>;
    fn get_name(&self) -> String;
}

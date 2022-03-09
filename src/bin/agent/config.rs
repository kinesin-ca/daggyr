use daggyr::prelude::*;
pub use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use tokio::sync::mpsc;

#[derive(Deserialize, Debug, Clone)]
pub struct GlobalConfigSpec {
    pub ip: String,
    pub port: u32,
    pub workers: usize,
}

impl Default for GlobalConfigSpec {
    fn default() -> Self {
        GlobalConfigSpec {
            ip: String::from("127.0.0.1"),
            port: 2503,
            workers: 10,
        }
    }
}

#[derive(Clone)]
pub struct GlobalConfig {
    pub ip: String,
    pub port: u32,
    pub workers: usize,
    pub tracker: mpsc::UnboundedSender<TrackerMessage>,
    pub executor: mpsc::UnboundedSender<ExecutorMessage>,
}

impl GlobalConfig {
    pub fn new(spec: &GlobalConfigSpec) -> Self {
        let workers = spec.workers;

        let (executor, exe_rx) = mpsc::unbounded_channel();
        local_executor::start(workers, exe_rx);

        // Tracker
        let (tracker, trx) = mpsc::unbounded_channel();
        noop_tracker::start(trx);

        GlobalConfig {
            ip: spec.ip.clone(),
            port: spec.port,
            workers,
            tracker,
            executor,
        }
    }

    pub fn listen_spec(&self) -> String {
        format!("{}:{}", self.ip, self.port)
    }
}

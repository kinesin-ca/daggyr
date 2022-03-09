use daggyr::prelude::*;
pub use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use sysinfo::{RefreshKind, System, SystemExt};
use tokio::sync::mpsc;

fn default_workers() -> usize {
    let system = System::new_with_specifics(RefreshKind::new().with_cpu());
    let workers = system.processors().len() - 2;
    if workers <= 0 {
        1
    } else {
        workers
    }
}

fn default_ip() -> String {
    "127.0.0.1".to_owned()
}

fn default_port() -> u32 {
    2504
}

#[derive(Deserialize, Debug, Clone)]
pub struct GlobalConfigSpec {
    #[serde(default = "default_ip")]
    pub ip: String,

    #[serde(default = "default_port")]
    pub port: u32,

    #[serde(default = "default_workers")]
    pub workers: usize,
}

impl Default for GlobalConfigSpec {
    fn default() -> Self {
        GlobalConfigSpec {
            ip: String::from("127.0.0.1"),
            port: 2503,
            workers: default_workers(),
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

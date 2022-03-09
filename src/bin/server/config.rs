use daggyr::prelude::*;
pub use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::mpsc;

#[derive(Clone, Deserialize, Debug)]
pub struct ServerConfig {
    pub ip: String,
    pub port: u32,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            ip: String::from("127.0.0.1"),
            port: 2503,
        }
    }
}

#[derive(Deserialize, Debug)]
#[serde(tag = "executor")]
pub enum PoolConfig {
    Local {
        workers: usize,
    },
    SSH {
        targets: Vec<daggyr::executors::ssh_executor::SSHTarget>,
    },

    #[cfg(feature = "slurm")]
    Slurm {
        base_url: String,
    },
}

fn default_pools() -> HashMap<String, PoolConfig> {
    HashMap::from([("default".to_owned(), PoolConfig::Local { workers: 10 })])
}

#[derive(Deserialize, Debug)]
#[serde(tag = "tracker")]
pub enum TrackerConfig {
    Memory,
}

impl Default for TrackerConfig {
    fn default() -> Self {
        TrackerConfig::Memory
    }
}

#[derive(Deserialize, Debug)]
pub struct GlobalConfigSpec {
    #[serde(default)]
    pub server: ServerConfig,

    #[serde(default = "default_pools")]
    pub pools: HashMap<String, PoolConfig>,

    #[serde(default)]
    pub tracker: TrackerConfig,

    #[serde(default)]
    pub default_pool: String,
}

#[derive(Clone, Debug)]
pub struct GlobalConfig {
    pub server: ServerConfig,
    pub pools: HashMap<String, mpsc::UnboundedSender<ExecutorMessage>>,
    pub tracker: mpsc::UnboundedSender<TrackerMessage>,
    pub runner: mpsc::UnboundedSender<RunnerMessage>,
    pub default_pool: String,
}

impl GlobalConfig {
    pub fn new(spec: &GlobalConfigSpec) -> Self {
        let pools = HashMap::new();

        use PoolConfig::*;
        for (pool, pool_spec) in spec.pools.iter() {
            let (tx, rx) = mpsc::unbounded_channel();
            match pool_spec {
                Local { workers } => {
                    local_executor::start(*workers, rx);
                }
                SSH { targets } => {
                    ssh_executor::start(*targets.clone(), rx);
                }

                #[cfg(feature = "slurm")]
                Slurm { base_url } => {
                    slurm_executor::start(base_url);
                }
            }
            pools.insert(*pool, tx);
        }

        // Tracker
        let (tracker, trx) = mpsc::unbounded_channel();
        use TrackerConfig::*;
        match spec.tracker {
            Memory => memory_tracker::start(trx),
        }

        // Runner
        let (runner, rrx) = mpsc::unbounded_channel();
        let rtx = runner.clone();
        runner::start(rtx, rrx);

        let default_pool = if spec.default_pool.is_empty() {
            pools.keys().next().unwrap().clone()
        } else {
            spec.default_pool
        };

        GlobalConfig {
            server: spec.server,
            pools,
            tracker,
            runner,
            default_pool,
        }
    }

    pub fn listen_spec(&self) -> String {
        format!("{}:{}", self.server.ip, self.server.port)
    }
}

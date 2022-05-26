use daggyr::prelude::*;
pub use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use sysinfo::{RefreshKind, System, SystemExt};
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

fn default_workers() -> usize {
    let system = System::new_with_specifics(RefreshKind::new().with_cpu());
    let workers = system.processors().len();
    if workers > 2 {
        workers - 2
    } else {
        workers
    }
}

#[derive(Deserialize, Debug, Clone)]
#[serde(tag = "executor", rename_all = "lowercase")]
pub enum PoolConfig {
    Local {
        #[serde(default = "default_workers")]
        workers: usize,
    },

    Ssh {
        targets: Vec<daggyr::executors::ssh_executor::SSHTarget>,
    },

    Agent {
        targets: Vec<daggyr::executors::agent_executor::AgentTarget>,
    },

    #[cfg(feature = "slurm")]
    Slurm { base_url: String },
}

fn default_pools() -> HashMap<String, PoolConfig> {
    HashMap::from([(
        "default".to_owned(),
        PoolConfig::Local {
            workers: default_workers(),
        },
    )])
}

#[derive(Deserialize, Debug, Clone)]
#[serde(tag = "tracker")]
pub enum TrackerConfig {
    Memory,
}

impl Default for TrackerConfig {
    fn default() -> Self {
        TrackerConfig::Memory
    }
}

#[derive(Deserialize, Debug, Clone)]
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

#[derive(Clone)]
pub struct GlobalConfig {
    pub server: ServerConfig,
    pub pools: HashMap<String, mpsc::UnboundedSender<ExecutorMessage>>,
    pub tracker: mpsc::UnboundedSender<TrackerMessage>,
    pub runner: mpsc::UnboundedSender<RunnerMessage>,
    pub default_pool: String,
    pub spec: GlobalConfigSpec,
}

impl GlobalConfig {
    pub fn new(spec: &GlobalConfigSpec) -> Self {
        let mut pools = HashMap::new();

        use PoolConfig::*;
        for (pool, pool_spec) in spec.pools.iter() {
            let (tx, rx) = mpsc::unbounded_channel();
            match pool_spec {
                Local { workers } => {
                    local_executor::start(*workers, rx);
                }

                Ssh { targets } => {
                    ssh_executor::start(targets.clone(), rx);
                }

                Agent { targets } => {
                    agent_executor::start(targets.clone(), rx);
                }

                #[cfg(feature = "slurm")]
                Slurm { base_url } => {
                    slurm_executor::start(base_url.clone(), rx);
                }
            }
            pools.insert(pool.clone(), tx);
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
            spec.default_pool.clone()
        };

        GlobalConfig {
            server: spec.server.clone(),
            pools,
            tracker,
            runner,
            default_pool,
            spec: spec.clone(),
        }
    }

    pub fn listen_spec(&self) -> String {
        format!("{}:{}", self.server.ip, self.server.port)
    }
}

impl Debug for GlobalConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GlobalConfig")
            .field("spec", &self.spec)
            .field("default_pool", &self.default_pool)
            .finish()
    }
}

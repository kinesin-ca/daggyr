//! The SSH executor is essentially a wrapped version of the local executor.
//! It dispatches tasks to remote hosts

extern crate serde_json;

use super::*;
use crate::structs::*;
use futures::stream::futures_unordered::FuturesUnordered;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot};

use futures::StreamExt;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ResourceCapacity {
    cores: u64,
    memory_mb: u64,
}

impl ResourceCapacity {
    fn can_satisfy(&self, resources: &ResourceCapacity) -> bool {
        resources.cores <= self.cores && resources.memory_mb <= self.memory_mb
    }

    fn sub(&mut self, resources: &ResourceCapacity) -> bool {
        self.cores -= resources.cores;
        self.memory_mb -= resources.memory_mb;
    }

    fn add(&mut self, resources: &ResourceCapacity) -> bool {
        self.cores += resources.cores;
        self.memory_mb += resources.memory_mb;
    }
}

pub struct SSHTarget {
    host: String,
    port: Option<u16>,
    private_key_file: Option<String>,
    user: Option<String>,
    resources: ResourceCapacity,
}

impl SSHTarget {
    fn new(host: String, resources: ResourceCapacity) -> Self {
        SSHTarget {
            host,
            port: None,
            private_key_file: None,
            user: None,
            resources: resources,
        }
    }
}

/// Contains specifics on how to run a local task
#[derive(Serialize, Deserialize, Clone, Debug)]
struct SSHTaskDetail {
    /// The command and all arguments to run
    #[serde(default)]
    command: Vec<String>,

    /// Environment variables to set
    #[serde(default)]
    environment: HashMap<String, String>,

    /// Timeout in seconds
    #[serde(default)]
    timeout: i64,

    /// Cores required by the task
    resources: ResourceCapacity,
}

fn get_task_details(task: &Task) -> Result<SSHTaskDetail, serde_json::Error> {
    serde_json::from_value::<SSHTaskDetail>(task.details.clone())
}

fn shell_escape_char(ch: char) -> Option<&'static str> {
    match ch {
        '"' => Some("\\\""),
        '\'' => Some("\\'"),
        ';' => Some("\\;"),
        _ => None,
    }
}

fn shell_escape(input: &str) -> String {
    let mut output = String::with_capacity(input.len() + 2);
    output.push('"');
    for ch in input.chars() {
        match shell_escape_char(ch) {
            Some(replacement) => output.push_str(replacement),
            None => output.push(ch),
        };
    }
    output.push('"');
    output
}

fn sshify_task(mut task: Task, target: &SSHTarget) -> Result<Task> {
    let mut new_command = vec!["ssh".to_owned()];
    let details = get_task_details(&task)?;

    // Handle the user and host
    if let Some(user) = target.user {
        new_command.push(format!("{}@{}", user, target.host));
    } else {
        new_command.push(target.host);
    }

    // Port
    if let Some(port) = target.port {
        new_command.push("-p".to_owned());
        new_command.push(format!("{}", port));
    }

    // private key
    if let Some(key) = target.private_key_file {
        new_command.push("-i".to_owned());
        new_command.push(key);
    }

    // Add the environment
    for (k, v) in details.environment.iter() {
        new_command.push("export".to_owned());

        // TODO -- this needs to be quoted properly
        new_command.push(format!("{}={}", shell_escape(k), shell_escape(v)));
    }

    // Copy in the remaining
    new_command.extend(details.command);

    *task.details.get_mut("command").unwrap() = json!(new_command);

    Ok(task)
}

async fn validate_tasks(
    tasks: Vec<Task>,
    max_capacities: Vec<ResourceCapacity>,
) -> Result<(), Vec<String>> {
    let mut errors = Vec::<String>::new();
    for (i, task) in tasks.iter().enumerate() {
        match get_task_details(&task) {
            Ok(details) => {
                if !max_capacities
                    .iter()
                    .any(|x| x.can_satisfy(&details.resources))
                {
                    errors.push(format!(
                        "[Task {}]: No SSH target satisfies the required resources\n",
                        i
                    ));
                }
            }
            Err(err) => errors.push(format!("[Task {}]: {}\n", i, err)),
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

struct RunningTask {
    resources: ResourceCapacity,
    target_id: usize,
}

/// The mpsc channel can be sized to fit max parallelism
async fn start_ssh_executor(
    mut targets: Vec<SSHTarget>,
    mut exe_msgs: mpsc::UnboundedReceiver<ExecutorMessage>,
) {
    // Start up a local executor capable of sending
    let max_caps: Vec<ResourceCapacity> = targets.iter().map(|x| x.resources.clone()).collect();
    let total_cores = targets.iter().map(|x| x.total_resources.cores).sum();
    let cur_caps = Arc::new(Mutex::new(max_caps));

    // Set up the
    let (le_tx, le_rx) = mpsc::unbounded_channel();
    local_executor::start(max_cores as usize, le_rx);

    while let Some(msg) = exe_msgs.recv().await {
        use ExecutorMessage::*;
        match msg {
            ValidateTasks { tasks, response } => {
                let ltx = le_tx.clone();
                let caps = capacities.clone();
                tokio::spawn(async move {
                    let result = validate_tasks(tasks, caps).await;
                    if result.is_err() {
                        response.send(result).unwrap_or(());
                    } else {
                        ltx.send(ValidateTasks { tasks, response }).unwrap_or(());
                    }
                });
            }
            ExpandTasks {
                tasks,
                parameters,
                response,
            } => {
                let ltx = le_tx.clone();
                tokio::spawn(async move {
                    ltx.send(ExecutorMessage::ExpandTasks {
                        tasks,
                        parameters,
                        response,
                    })
                    .unwrap_or(());
                });
            }
            ExecuteTask {
                run_id,
                task_id,
                task,
                response,
                tracker,
            } => {
                loop {
                    if let Some(tid, target) = targets
                        .iter()
                        .enumerate()
                        .find(|(i, x)| x.available_resources.can_satisfy(details.resources))
                    {
                        target.available_resources.sub(&details.resources);

                        let resources = details.resources.clone();

                        // TODO: Handle errors more gracefully
                        let ssh_task = sshify_task(task, target).unwrap();

                        let ltx = le_tx.clone();
                        tokio::spawn(async move {
                            let (rtx, rrx) = mpsc::unbounded_channel();

                            le.send(ExecuteTask {
                                run_id,
                                task_id,
                                ssh_task,
                                rtx,
                                tracker,
                            })
                            .unwrap_or(());

                            let Some(msg) = rrx.recv().await;
                            match msg {
                                exe @ RunnerMessage::ExecutionReport { .. } => {
                                    response.send(exe).unwrap_or(());
                                }
                                _ => {
                                    panic!("Unexpected message");
                                }
                            }
                        });

                        break;
                    }
                    sleep(Duration::from_millis(100)).await;
                }

                task_channels.insert((run_id, task_id), tx);
                if running.len() == max_parallel {
                    running.next().await;
                }
                running.push(tokio::spawn(async move {
                    let attempt = run_task(task, rx).await;
                    response
                        .send(RunnerMessage::ExecutionReport {
                            run_id,
                            task_id,
                            attempt,
                        })
                        .unwrap();
                }));
            }
            StopTask {
                run_id,
                task_id,
                response,
            } => {
                if let Some(tx) = task_channels.remove(&(run_id, task_id)) {
                    tx.send(()).unwrap_or(());
                }
                response.send(()).unwrap_or(());
            }
            Stop {} => {
                break;
            }
        }
    }
}

pub fn start(targets: Vec<SSHTarget>, msgs: mpsc::UnboundedReceiver<ExecutorMessage>) {
    tokio::spawn(async move {
        start_ssh_executor(targets, msgs).await;
    });
}

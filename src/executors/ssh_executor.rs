//! The SSH executor is essentially a wrapped version of the local executor.
//! It dispatches tasks to remote hosts

extern crate serde_json;

use super::{local_executor, ExecutorMessage, Result, RunnerMessage};
use crate::structs::{HashMap, TaskResources, TaskDetails};
use futures::stream::futures_unordered::FuturesUnordered;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::mpsc;

use futures::StreamExt;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SSHTarget {
    pub host: String,

    #[serde(default)]
    pub port: Option<u16>,

    #[serde(default)]
    pub private_key_file: Option<String>,

    #[serde(default)]
    pub user: Option<String>,

    pub resources: TaskResources,
}

impl SSHTarget {
    fn new(host: String, resources: TaskResources) -> Self {
        SSHTarget {
            host,
            port: None,
            private_key_file: None,
            user: None,
            resources,
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
    resources: TaskResources,
}

fn extract_details(details: &TaskDetails) -> Result<SSHTaskDetail, serde_json::Error> {
    serde_json::from_value::<SSHTaskDetail>(details.clone())
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

fn sshify_task(mut details: TaskDetails, target: &SSHTarget) -> Result<TaskDetails> {
    let mut new_command = vec!["ssh".to_owned()];
    let parsed = extract_details(&details)?;

    // Handle the user and host
    if let Some(user) = &target.user {
        new_command.push(format!("{}@{}", user, target.host));
    } else {
        new_command.push(target.host.clone());
    }

    // Port
    if let Some(port) = target.port {
        new_command.push("-p".to_owned());
        new_command.push(format!("{}", port));
    }

    // private key
    if let Some(key) = &target.private_key_file {
        new_command.push("-i".to_owned());
        new_command.push(key.clone());
    }

    // Add the environment
    for (k, v) in &parsed.environment {
        new_command.push("export".to_owned());

        // TODO -- this needs to be quoted properly
        new_command.push(format!("{}={}", shell_escape(k), shell_escape(v)));
    }

    // Copy in the remaining
    new_command.extend(parsed.command);

    *details.get_mut("command").unwrap() = json!(new_command);

    Ok(details)
}

fn validate_task(details: &TaskDetails, max_capacities: &[TaskResources]) -> Result<()> {
    match extract_details(details) {
        Ok(details) => {
            if !max_capacities
                .iter()
                .any(|x| x.can_satisfy(&details.resources))
            {
                Err(anyhow!( "No SSH target satisfies the required resources"))
            } else {
                Ok(())
            }
        }
        Err(err) => Err(anyhow!(err)),
    }
}

struct RunningTask {
    resources: TaskResources,
    target_id: usize,
}

/// The mpsc channel can be sized to fit max parallelism
async fn start_ssh_executor(
    targets: Vec<SSHTarget>,
    mut exe_msgs: mpsc::UnboundedReceiver<ExecutorMessage>,
) {
    assert!(
        targets.iter().all(|x| x.resources.contains_key("cores")),
        "Not all SSH targets have the required resource 'cores' defined"
    );

    // Start up a local executor capable of sending
    let max_caps: Vec<TaskResources> = targets.iter().map(|x| x.resources.clone()).collect();
    let total_cores: i64 = targets.iter().map(|x| x.resources["cores"]).sum();
    assert!(total_cores > 0, "No cores available to run tasks");
    let mut cur_caps = max_caps.clone();

    // Set up the local executor
    let (le_tx, le_rx) = mpsc::unbounded_channel();
    local_executor::start(usize::try_from(total_cores).unwrap_or(1usize), le_rx);

    // Tasks waiting to release resources
    let mut running = FuturesUnordered::new();

    while let Some(msg) = exe_msgs.recv().await {
        use ExecutorMessage::{ExecuteTask, ExpandTaskDetails, Stop, StopTask, ValidateTask};
        match msg {
            ValidateTask { details, response } => {
                let ltx = le_tx.clone();
                let caps = max_caps.clone();
                tokio::spawn(async move {
                    let result = validate_task(&details, &caps);
                    if result.is_err() {
                        response.send(result).unwrap_or(());
                    } else {
                        ltx.send(ValidateTask { details, response }).unwrap_or(());
                    }
                });
            }
            ExpandTaskDetails {
                details,
                parameters,
                response,
            } => {
                let ltx = le_tx.clone();
                tokio::spawn(async move {
                    ltx.send(ExecutorMessage::ExpandTaskDetails {
                        details,
                        parameters,
                        response,
                    })
                    .unwrap_or(());
                });
            }
            ExecuteTask {
                run_id,
                task_id,
                details,
                response,
                tracker,
            } => {
                let parsed = extract_details(&details).unwrap();
                let resources = parsed.resources.clone();

                // Wait until a target is available
                while !cur_caps.iter().any(|x| x.can_satisfy(&resources)) {
                    let result: Result<(usize, TaskResources), tokio::task::JoinError> =
                        running.next().await.unwrap();

                    let (tid, resources) = result.unwrap();
                    cur_caps[tid].add(&resources);
                }

                let (tid, capacity) = cur_caps
                    .iter_mut()
                    .enumerate()
                    .find(|(_, x)| x.can_satisfy(&parsed.resources))
                    .unwrap();
                capacity.sub(&resources).unwrap();
                let ssh_task = sshify_task(details, &targets[tid]).unwrap();
                let ltx = le_tx.clone();
                running.push(tokio::spawn(async move {
                    let (rtx, mut rrx) = mpsc::unbounded_channel();
                    ltx.send(ExecuteTask {
                        run_id,
                        task_id,
                        details: ssh_task,
                        response: rtx,
                        tracker,
                    })
                    .expect("Unable to submit task to local executor");

                    let msg = rrx.recv().await.unwrap();
                    match msg {
                        exe @ RunnerMessage::ExecutionReport { .. } => {
                            response.send(exe).unwrap_or(());
                        }
                        _ => {
                            panic!("Unexpected message");
                        }
                    }

                    (tid, resources)
                }));
            }
            msg @ StopTask { .. } => {
                le_tx.send(msg).unwrap_or(());
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

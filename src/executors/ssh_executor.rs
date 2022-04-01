//! The SSH executor is essentially a wrapped version of the local executor.
//! It dispatches tasks to remote hosts

extern crate serde_json;

use super::*;
use crate::structs::*;
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
    tasks: &Vec<Task>,
    max_capacities: &Vec<TaskResources>,
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
    resources: TaskResources,
    target_id: usize,
}

/// The mpsc channel can be sized to fit max parallelism
async fn start_ssh_executor(
    targets: Vec<SSHTarget>,
    mut exe_msgs: mpsc::UnboundedReceiver<ExecutorMessage>,
) {
    if !targets.iter().all(|x| x.resources.contains_key("cores")) {
        panic!("Not all SSH targets have the required resource 'cores' defined");
    }

    // Start up a local executor capable of sending
    let max_caps: Vec<TaskResources> = targets.iter().map(|x| x.resources.clone()).collect();
    let total_cores: i64 = targets.iter().map(|x| x.resources["cores"]).sum();
    let mut cur_caps = max_caps.clone();

    // Set up the local executor
    let (le_tx, le_rx) = mpsc::unbounded_channel();
    local_executor::start(total_cores as usize, le_rx);

    // Tasks waiting to release resources
    let mut running = FuturesUnordered::new();

    while let Some(msg) = exe_msgs.recv().await {
        use ExecutorMessage::*;
        match msg {
            ValidateTasks { tasks, response } => {
                let ltx = le_tx.clone();
                let caps = max_caps.clone();
                tokio::spawn(async move {
                    let result = validate_tasks(&tasks, &caps).await;
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
                task_id,
                task,
                response,
                tracker,
            } => {
                match validate_tasks(&vec![task.clone()], &max_caps).await {
                    Err(e) => {
                        let mut attempt = TaskAttempt::new();
                        attempt.succeeded = false;
                        attempt.executor.extend(e);
                        response
                            .send(RunnerMessage::ExecutionReport { task_id, attempt })
                            .expect("Unable to send response");
                    }
                    Ok(()) => {
                        let details = get_task_details(&task).unwrap();
                        let resources = details.resources.clone();

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
                            .find(|(_, x)| x.can_satisfy(&details.resources))
                            .unwrap();
                        capacity.sub(&resources).unwrap();
                        let ssh_task = sshify_task(task, &targets[tid]).unwrap();
                        let ltx = le_tx.clone();
                        running.push(tokio::spawn(async move {
                            let (rtx, mut rrx) = mpsc::unbounded_channel();
                            ltx.send(ExecuteTask {
                                task_id,
                                task: ssh_task,
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
                }
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

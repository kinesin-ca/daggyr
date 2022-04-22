//! The Agent executor is essentially a wrapped version of the local executor.
//! It dispatches tasks to remote hosts

extern crate serde_json;

use super::*;
use crate::structs::*;
use futures::stream::futures_unordered::FuturesUnordered;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

use futures::StreamExt;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AgentTarget {
    pub base_url: String,

    #[serde(default)]
    pub resources: TaskResources,
}

impl AgentTarget {
    fn new(base_url: String, resources: TaskResources) -> Self {
        AgentTarget {
            base_url,
            resources,
        }
    }
}

/// Contains specifics on how to run a local task
#[derive(Serialize, Deserialize, Clone, Debug)]
struct AgentTaskDetail {
    /// The command and all arguments to run
    #[serde(default)]
    command: Vec<String>,

    /// Environment variables to set
    #[serde(default)]
    environment: HashMap<String, String>,

    /// Timeout in seconds
    #[serde(default)]
    timeout: i64,

    /// resources required by the task
    resources: TaskResources,
}

fn get_task_details(task: &Task) -> Result<AgentTaskDetail, serde_json::Error> {
    serde_json::from_value::<AgentTaskDetail>(task.details.clone())
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
                        "[Task {}]: No Agent target satisfies the required resources\n",
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
async fn start_agent_executor(
    mut targets: Vec<AgentTarget>,
    mut exe_msgs: mpsc::UnboundedReceiver<ExecutorMessage>,
) {
    let client = reqwest::Client::new();

    // pre-calculate the largest task we can dispatch
    for target in targets.iter_mut() {
        let resource_url = format!("{}/resources", target.base_url);
        let result = client
            .get(resource_url)
            .send()
            .await
            .expect(&format!("Unable to query {}", target.base_url));
        match result.status() {
            reqwest::StatusCode::OK => {
                target.resources = result.json().await.unwrap();
            }
            _ => {
                panic!("Unable to query {}", target.base_url);
            }
        }
    }

    let max_caps: Vec<TaskResources> = targets.iter().map(|x| x.resources.clone()).collect();
    let mut cur_caps = max_caps.clone();

    // Set up the local executor
    let (le_tx, le_rx) = mpsc::unbounded_channel();
    local_executor::start(1, le_rx);

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
                        let base_url = targets[tid].base_url.clone();
                        let submit_client = client.clone();
                        running.push(tokio::spawn(async move {
                            let (tx, rx) = oneshot::channel();
                            tracker
                                .send(TrackerMessage::UpdateTaskState {
                                    task_id: task_id.clone(),
                                    state: State::Running,
                                    response: tx,
                                })
                                .unwrap_or(());
                            rx.await.unwrap().expect("Unable to update task state");

                            let submit_url = format!("{}/{}", base_url, task_id);
                            // TODO Handle the case where an agent stops responding
                            let result = submit_client
                                .post(submit_url)
                                .json(&task)
                                .send()
                                .await
                                .unwrap();

                            match result.status() {
                                reqwest::StatusCode::OK => {
                                    let mut attempt: TaskAttempt = result.json().await.unwrap();
                                    attempt
                                        .executor
                                        .push(format!("Executed on agent at {}", base_url));
                                    response
                                        .send(RunnerMessage::ExecutionReport { task_id, attempt })
                                        .expect("Unable to send message to runner");
                                }
                                _ => {
                                    let mut attempt = TaskAttempt::new();
                                    attempt.succeeded = false;
                                    attempt
                                        .executor
                                        .push(format!("Unable to dispatch task to {}", base_url));
                                    response
                                        .send(RunnerMessage::ExecutionReport { task_id, attempt })
                                        .expect("Unable to send response");
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

pub fn start(targets: Vec<AgentTarget>, msgs: mpsc::UnboundedReceiver<ExecutorMessage>) {
    tokio::spawn(async move {
        start_agent_executor(targets, msgs).await;
    });
}

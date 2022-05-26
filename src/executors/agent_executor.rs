//! The Agent executor is essentially a wrapped version of the local executor.
//! It dispatches tasks to remote hosts

extern crate serde_json;

use super::{local_executor, ExecutorMessage, Result, RunnerMessage, TrackerMessage};
use crate::structs::{HashMap, RunID, State, Task, TaskAttempt, TaskID, TaskResources};
use futures::stream::futures_unordered::FuturesUnordered;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

use futures::StreamExt;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AgentTarget {
    pub base_url: String,

    #[serde(default)]
    pub resources: TaskResources,

    #[serde(default)]
    pub enabled: bool,
}

impl AgentTarget {
    fn new(base_url: String, resources: TaskResources) -> Self {
        AgentTarget {
            base_url,
            resources,
            enabled: true,
        }
    }

    async fn refresh_resources(&mut self, client: &reqwest::Client) -> Result<()> {
        let resource_url = format!("{}/resources", self.base_url);
        let result = client.get(resource_url).send().await?;
        if result.status() == reqwest::StatusCode::OK {
            self.resources = result.json().await.unwrap();
            Ok(())
        } else {
            self.enabled = false;
            Err(anyhow!("Unable to query {}", self.base_url))
        }
    }

    async fn ping(&mut self, client: &reqwest::Client) -> Result<()> {
        let resource_url = format!("{}/ready", self.base_url);
        let result = client.get(resource_url).send().await?;
        self.enabled = result.status() == reqwest::StatusCode::OK;
        Ok(())
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

fn validate_tasks(tasks: &[Task], max_capacities: &[TaskResources]) -> Result<(), Vec<String>> {
    let mut errors = Vec::<String>::new();
    for (i, task) in tasks.iter().enumerate() {
        match get_task_details(task) {
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

async fn submit_task(
    run_id: RunID,
    task_id: TaskID,
    task: Task,
    tracker: mpsc::UnboundedSender<TrackerMessage>,
    base_url: String,
    client: reqwest::Client,
    response: mpsc::UnboundedSender<RunnerMessage>,
) {
    let (tx, rx) = oneshot::channel();
    tracker
        .send(TrackerMessage::UpdateTaskState {
            run_id,
            task_id: task_id.clone(),
            state: State::Running,
            response: tx,
        })
        .unwrap_or(());
    rx.await.unwrap().expect("Unable to update task state");

    let submit_url = format!("{}/{}/{}", base_url, run_id, task_id);
    // TODO Handle the case where an agent stops responding
    let result = client.post(submit_url).json(&task).send().await.unwrap();

    if result.status() == reqwest::StatusCode::OK {
        let mut attempt: TaskAttempt = result.json().await.unwrap();
        attempt
            .executor
            .push(format!("Executed on agent at {}", base_url));
        response
            .send(RunnerMessage::ExecutionReport {
                run_id,
                task_id,
                attempt,
            })
            .expect("Unable to send message to runner");
    } else {
        let mut attempt = TaskAttempt::new();
        attempt.succeeded = false;
        attempt
            .executor
            .push(format!("Unable to dispatch task to {}", base_url));
        response
            .send(RunnerMessage::ExecutionReport {
                run_id,
                task_id,
                attempt,
            })
            .expect("Unable to send response");
    }
}

// async fn select_target() -> Option<usize> {}

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

    for target in &mut targets {
        target.refresh_resources(&client).await.unwrap();
    }

    let max_caps: Vec<TaskResources> = targets.iter().map(|x| x.resources.clone()).collect();
    let mut cur_caps = max_caps.clone();

    // Set up the local executor
    let (le_tx, le_rx) = mpsc::unbounded_channel();
    local_executor::start(1, le_rx);

    // Tasks waiting to release resources
    let mut running = FuturesUnordered::new();

    while let Some(msg) = exe_msgs.recv().await {
        use ExecutorMessage::{ExecuteTask, ExpandTaskDetails, Stop, StopTask, ValidateTasks};
        match msg {
            ValidateTasks { tasks, response } => {
                let ltx = le_tx.clone();
                let caps = max_caps.clone();
                tokio::spawn(async move {
                    let result = validate_tasks(&tasks, &caps);
                    if result.is_err() {
                        response.send(result).unwrap_or(());
                    } else {
                        ltx.send(ValidateTasks { tasks, response }).unwrap_or(());
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
                task,
                response,
                tracker,
            } => {
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
                    submit_task(
                        run_id,
                        task_id,
                        task,
                        tracker,
                        base_url,
                        submit_client,
                        response,
                    )
                    .await;
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

pub fn start(targets: Vec<AgentTarget>, msgs: mpsc::UnboundedReceiver<ExecutorMessage>) {
    tokio::spawn(async move {
        start_agent_executor(targets, msgs).await;
    });
}

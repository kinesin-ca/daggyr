use super::*;
use crate::prelude::*;
use crate::utilities::{apply_vars, find_applicable_vars};
use chrono::{DateTime, Utc};
use futures::stream::futures_unordered::FuturesUnordered;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{sleep, Duration};

// Traits
use futures::StreamExt;

fn default_cpus() -> usize {
    1usize
}

fn default_min_memory_mb() -> usize {
    200usize
}
fn default_min_tmp_disk_mb() -> usize {
    0usize
}
fn default_time_limit_seconds() -> usize {
    3600usize
}

fn default_priority() -> usize {
    1usize
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SlurmTaskDetail {
    pub user: String,

    pub jwt_token: String,

    #[serde(default = "default_cpus")]
    pub min_cpus: usize,

    #[serde(default = "default_min_memory_mb")]
    pub min_memory_mb: usize,

    #[serde(default = "default_min_tmp_disk_mb")]
    pub min_tmp_disk_mb: usize,

    #[serde(default = "default_priority")]
    pub priority: usize,

    #[serde(default)]
    pub time_limit_seconds: usize,

    /// The command and all arguments to run
    pub command: Vec<String>,

    /// Environment variables to set
    #[serde(default)]
    pub environment: HashMap<String, String>,

    /// Log directory. If this is readable from the server daggyr runs on,
    /// output will be read and stored in the output field of any TaskAttempts
    pub logdir: PathBuf,
}

#[derive(Serialize, Clone, Debug)]
struct SlurmSubmitJobDetails {
    name: String,
    nodes: usize,
    environment: HashMap<String, String>,
    standard_output: String,
    standard_error: String,
}

#[derive(Serialize, Clone, Debug)]
struct SlurmSubmitJob {
    script: String,
    job: SlurmSubmitJobDetails,
}

impl SlurmSubmitJob {
    fn new(task_name: String, detail: SlurmTaskDetail) -> Self {
        let script = format!("#!/bin/bash\n{}\n", detail.command.join(" "));

        // ENV always has to have at least one value in it, so might as
        // well give it some helpful defaults
        let mut env = detail.environment.clone();
        env.insert("DAGGY_TASK_NAME".to_owned(), task_name.clone());

        let mut stdout = detail.logdir.clone();
        stdout.push(format!("{}.stdout", task_name));
        let mut stderr = detail.logdir.clone();
        stderr.push(format!("{}.stderr", task_name));

        SlurmSubmitJob {
            script: script,
            job: SlurmSubmitJobDetails {
                nodes: 1,
                environment: env,
                name: task_name,
                standard_output: stdout.into_os_string().into_string().unwrap(),
                standard_error: stderr.into_os_string().into_string().unwrap(),
            },
        }
    }
}

fn get_task_details(task: &Task) -> Result<SlurmTaskDetail, serde_json::Error> {
    serde_json::from_value::<SlurmTaskDetail>(task.details.clone())
}

/// Contains the information required to monitor and resubmit failed
/// tasks. Resubmission only happens if there was a failure in the
/// cluster.
#[derive(Clone, Debug)]
struct SlurmJob {
    start_time: DateTime<Utc>,
    slurm_id: u64,
    user: String,
    jwt_token: String,
    response: mpsc::UnboundedSender<RunnerMessage>,
    run_id: RunID,
    task: Task,
    task_name: String,
    killed: bool,
}

/// Submit a task to slurmrestd, and extract the slurm job_id
async fn submit_slurm_job(
    base_url: &String,
    client: &reqwest::Client,
    task_id: &TaskID,
    task: &Task,
) -> Result<u64> {
    let details = get_task_details(&task).unwrap();

    let job = SlurmSubmitJob::new(task_id.to_string(), details.clone());

    let result = client
        .post(base_url.clone() + "/job/submit")
        .header("X-SLURM-USER-NAME", details.user.clone())
        .header("X-SLURM-USER-TOKEN", details.jwt_token.clone())
        .json(&job)
        .send()
        .await?;

    match result.status() {
        reqwest::StatusCode::OK => {
            let payload: serde_json::Value = result.json().await.unwrap();
            Ok(payload["job_id"].as_u64().unwrap())
        }
        _ => {
            let payload: serde_json::Value = result.json().await.unwrap();
            let errors: Vec<String> = payload["errors"]
                .as_array()
                .unwrap()
                .iter()
                .map(|x| x.as_str().unwrap().to_string())
                .collect();
            Err(anyhow!(errors.join("\n")))
        }
    }
}

fn slurp_if_exists(filename: String) -> String {
    let pth = std::path::Path::new(&filename);
    if pth.exists() {
        std::fs::read_to_string(pth).unwrap()
    } else {
        filename
    }
}

fn expand_tasks(tasks: TaskSet, parameters: Parameters) -> Result<TaskSet> {
    let mut expanded_tasks = HashMap::new();
    for (task_id, task) in tasks {
        let template = get_task_details(&task)?;

        let all_vars: Vec<String> = parameters.keys().into_iter().cloned().collect();

        // Need to decompose the environment to apply the expansion
        let env_keys: Vec<String> = template.environment.keys().into_iter().cloned().collect();
        let env_values: Vec<String> = env_keys
            .iter()
            .map(|x| template.environment[x].clone())
            .collect();

        // The expansion set will include both environment
        let vars: HashSet<_> = find_applicable_vars(&template.command, &all_vars)
            .union(&find_applicable_vars(&env_values, &all_vars))
            .cloned()
            .collect();

        if vars.is_empty() {
            expanded_tasks.insert(task_id, task);
        } else {
            let new_cmds = apply_vars(&template.command, &parameters, &vars);
            let new_envs = apply_vars(&env_values, &parameters, &vars);
            for (i, (new_cmd, new_env_vals)) in new_cmds.iter().zip(new_envs.iter()).enumerate() {
                let mut new_task = task.clone();
                new_task.details["command"] = serde_json::json!(new_cmd);
                new_task.details["environment"] = env_keys
                    .iter()
                    .cloned()
                    .zip(new_env_vals.iter().cloned())
                    .collect();
                let mut new_task_id = task_id.clone();
                new_task_id.set_instance(i);
                expanded_tasks.insert(new_task_id, new_task);
            }
        }
    }

    Ok(expanded_tasks)
}

enum JobEvent {
    Kill,
    Timeout,
}

async fn watch_job(
    slurm_id: u64,
    task_id: TaskID,
    task: Task,
    base_url: String,
    response: mpsc::UnboundedSender<RunnerMessage>,
    kill_signal: oneshot::Receiver<JobEvent>,
) {
    let start_time = Utc::now();
    let client = reqwest::Client::new();
    let details = get_task_details(&task).unwrap();
    let mut signals = FuturesUnordered::new();
    signals.push(kill_signal);
    let mut killed = false;

    loop {
        // Generate a timeout for the next poll
        let (timeout_tx, timeout_rx) = oneshot::channel();
        tokio::spawn(async move {
            sleep(Duration::from_secs(1)).await;
            timeout_tx.send(JobEvent::Timeout).unwrap_or(());
        });

        signals.push(timeout_rx);

        if let Some(Ok(event)) = signals.next().await {
            match event {
                JobEvent::Kill => {
                    let url = format!("{}/job/{}", base_url, slurm_id);
                    let response = client
                        .delete(url)
                        .header("X-SLURM-USER-NAME", details.user.clone())
                        .header("X-SLURM-USER-TOKEN", details.jwt_token.clone())
                        .send()
                        .await
                        .unwrap();
                    if response.status() == 200 {
                        killed = true;
                    }
                }
                JobEvent::Timeout => {
                    let url = format!("{}/job/{}", base_url, slurm_id);
                    let result = client
                        .get(url)
                        .header("X-SLURM-USER-NAME", details.user.clone())
                        .header("X-SLURM-USER-TOKEN", details.jwt_token.clone())
                        .send()
                        .await
                        .unwrap();

                    if result.status() != 200 {
                        let mut attempt = TaskAttempt::new();
                        attempt.executor.push(format!(
                                    "Unable to query job status, assuming critical failure. Investigate job id {}, task name {} in slurm for more details"
                                    , slurm_id, task_id
                                ));
                        response
                            .send(RunnerMessage::ExecutionReport { task_id, attempt })
                            .unwrap();
                        return;
                    }

                    let payload: serde_json::Value = result.json().await.unwrap();
                    let job = &payload["jobs"].as_array().unwrap()[0];
                    let state = job["job_state"].as_str().unwrap();
                    match state {
                        // Waiting for progress
                        "PENDING" | "SUSPENDED" | "RUNNING" => {}

                        // Completed
                        "COMPLETED" | "FAILED" | "CANCELLED" | "TIMEOUT" | "OOM" => {
                            // Attempt to read the standard out / error
                            let stderr = slurp_if_exists(
                                job["standard_error"].as_str().unwrap().to_string(),
                            );
                            let stdout = slurp_if_exists(
                                job["standard_output"].as_str().unwrap().to_string(),
                            );

                            response
                                .send(RunnerMessage::ExecutionReport {
                                    task_id: task_id,
                                    attempt: TaskAttempt {
                                        succeeded: state == "COMPLETED",
                                        output: stdout,
                                        error: stderr,
                                        start_time: start_time,
                                        stop_time: Utc::now(),
                                        executor: Vec::new(),
                                        exit_code: job["exit_code"].as_i64().unwrap() as i32,
                                        killed: killed,
                                        max_cpu: 0,
                                        max_rss: 0,
                                    },
                                })
                                .unwrap();
                            break;
                        }
                        // Retry
                        "NODE_FAIL" | "PREEMPTED" | "BOOT_FAIL" | "DEADLINE" => {
                            let stderr = slurp_if_exists(
                                job["standard_error"].as_str().unwrap().to_string(),
                            );
                            let stdout = slurp_if_exists(
                                job["standard_output"].as_str().unwrap().to_string(),
                            );
                            response
                                .send(RunnerMessage::ExecutionReport {
                                    task_id: task_id,
                                    attempt: TaskAttempt {
                                        succeeded: false,
                                        output: stdout,
                                        error: stderr,
                                        start_time: start_time,
                                        stop_time: Utc::now(),
                                        executor: vec![format!(
                                            "Job failed due to potential cluster issue: {}",
                                            state
                                        )],
                                        exit_code: job["exit_code"].as_i64().unwrap() as i32,
                                        killed: false,
                                        max_cpu: 0,
                                        max_rss: 0,
                                    },
                                })
                                .unwrap();
                            return;
                        }
                        _ => {}
                    }
                }
            }
        }
    }
}

pub async fn start_slurm_executor(
    base_url: String,
    mut msgs: mpsc::UnboundedReceiver<ExecutorMessage>,
) {
    let mut running_tasks = HashMap::<TaskID, oneshot::Sender<JobEvent>>::new();

    let client = reqwest::Client::new();

    while let Some(msg) = msgs.recv().await {
        use ExecutorMessage::*;
        match msg {
            ValidateTasks { tasks, response } => {
                let mut errors = Vec::new();
                for (i, task) in tasks.iter().enumerate() {
                    if let Err(err) = get_task_details(&task) {
                        errors.push(format!("[Task {}]: {}\n", i, err));
                    }
                }
                response
                    .send(if errors.len() == 0 {
                        Ok(())
                    } else {
                        Err(errors)
                    })
                    .unwrap_or(());
            }
            ExpandTasks {
                tasks,
                parameters,
                response,
            } => {
                response.send(expand_tasks(tasks, parameters)).unwrap_or(());
            }
            ExecuteTask {
                task_id,
                task,
                response,
                tracker,
            } => {
                let url = base_url.clone();
                match submit_slurm_job(&base_url, &client, &task_id, &task).await {
                    Ok(slurm_id) => {
                        let (kill_tx, kill_rx) = oneshot::channel();
                        let tid = task_id.clone();
                        tokio::spawn(async move {
                            watch_job(slurm_id, tid, task, url, response, kill_rx).await
                        });
                        let (tx, _) = oneshot::channel();
                        tracker
                            .send(TrackerMessage::UpdateTaskState {
                                task_id: task_id.clone(),
                                state: State::Running,
                                response: tx,
                            })
                            .unwrap_or(());
                        running_tasks.insert(task_id, kill_tx);
                    }
                    Err(e) => {
                        let mut attempt = TaskAttempt::new();
                        attempt.executor.push(format!("{:?}", e));
                        response
                            .send(RunnerMessage::ExecutionReport {
                                task_id: task_id,
                                attempt: attempt,
                            })
                            .unwrap_or(());
                    }
                }
            }
            StopTask { task_id, response } => {
                if let Some(channel) = running_tasks.remove(&task_id) {
                    channel.send(JobEvent::Kill).unwrap_or(());
                }
                response.send(()).unwrap_or(());
            }
            Stop {} => {
                break;
            }
        }
    }
}

pub fn start(base_url: String, msgs: mpsc::UnboundedReceiver<ExecutorMessage>) {
    tokio::spawn(async move {
        start_slurm_executor(base_url, msgs).await;
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    use tokio::process::Command;
    use users::get_current_username;

    async fn get_userinfo() -> (String, String) {
        let osuser = get_current_username().unwrap();
        let user = osuser.to_string_lossy().clone();

        let output = Command::new("scontrol")
            .arg("token")
            .output()
            .await
            .expect("Failed to execute scontrol to obtain token");

        let result = String::from_utf8_lossy(&output.stdout);
        let token = result
            .split("=")
            .nth(1)
            .expect("Unable to get token for slurm")
            .trim();

        (user.to_string(), token.to_string())
    }

    #[tokio::test]
    async fn test_basic_submission() {
        let (user, token) = get_userinfo().await;
        let base_url = "http://localhost:6820/slurm/v0.0.36".to_owned();

        let (exe_tx, exe_rx) = mpsc::unbounded_channel();
        super::start(base_url, exe_rx);

        let task_spec = format!(
            r#"
                {{
                    "details": {{
                        "command": [ "/usr/bin/echo", "hello", "$MYVAR" ],
                        "user": "{}",
                        "jwt_token": "{}",
                        "environment": {{
                            "MYVAR": "fancy_pants"
                        }},
                        "logdir": "/tmp"
                    }}
                }}"#,
            user, token
        );

        let task: Task = serde_json::from_str(task_spec.as_str()).unwrap();
        let task_id = TaskID::new(0, &"fancy_pants".to_owned(), 0);

        let (tx, mut rx) = mpsc::unbounded_channel();
        let (log_tx, _) = mpsc::unbounded_channel();
        exe_tx
            .send(ExecutorMessage::ExecuteTask {
                task_id,
                task: task,
                response: tx,
                tracker: log_tx,
            })
            .unwrap();

        match rx.recv().await.unwrap() {
            RunnerMessage::ExecutionReport { attempt, .. } => {
                assert!(attempt.succeeded);
                assert_eq!(attempt.output, "hello fancy_pants\n");
            }
            _ => {
                assert!("Unexpected Message" == "");
            }
        }

        // Read the output

        exe_tx.send(ExecutorMessage::Stop {}).unwrap();
    }

    #[tokio::test]
    async fn test_stop_job() {
        let (user, token) = get_userinfo().await;
        let base_url = "http://localhost:6820/slurm/v0.0.36".to_owned();

        let (exe_tx, exe_rx) = mpsc::unbounded_channel();
        super::start(base_url, exe_rx);

        let task_spec = format!(
            r#"
                {{
                    "details": {{
                        "command": [ "sleep", "1800" ],
                        "user": "{}",
                        "jwt_token": "{}",
                        "logdir": "/tmp"
                    }}
                }}"#,
            user, token
        );

        let task: Task = serde_json::from_str(task_spec.as_str()).unwrap();
        let task_id = TaskID::new(0, &"fancy_pants".to_owned(), 0);

        let (tx, mut rx) = mpsc::unbounded_channel();
        let (log_tx, _) = mpsc::unbounded_channel();
        exe_tx
            .send(ExecutorMessage::ExecuteTask {
                task_id: task_id.clone(),
                task: task.clone(),
                response: tx,
                tracker: log_tx,
            })
            .unwrap();

        // Sleep for a bit
        sleep(Duration::from_secs(2)).await;

        // Cancel
        let (cancel_tx, cancel_rx) = oneshot::channel();
        exe_tx
            .send(ExecutorMessage::StopTask {
                task_id,
                response: cancel_tx,
            })
            .unwrap();

        cancel_rx.await.unwrap();

        match rx.recv().await.unwrap() {
            RunnerMessage::ExecutionReport { attempt, .. } => {
                assert!(attempt.killed);
            }
            _ => {
                panic!("Unexpected Message");
            }
        }

        exe_tx.send(ExecutorMessage::Stop {}).unwrap();
    }
}

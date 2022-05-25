use super::*;
use crate::structs::*;
use crate::utilities::*;
use chrono::prelude::*;
use futures::stream::futures_unordered::FuturesUnordered;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::process::Stdio;
use tokio::process::Command;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{sleep, Duration};

use futures::StreamExt;
use tokio::io::AsyncReadExt;

/// Contains specifics on how to run a local task
#[derive(Serialize, Deserialize, Clone, Debug)]
struct LocalTaskDetail {
    /// The command and all arguments to run
    #[serde(default)]
    command: Vec<String>,

    /// Environment variables to set
    #[serde(default)]
    environment: HashMap<String, String>,

    /// Timeout in seconds
    #[serde(default)]
    timeout: i64,
}

fn get_details(details: serde_json::Value) -> Result<LocalTaskDetail, serde_json::Error> {
    serde_json::from_value::<LocalTaskDetail>(details)
}

fn get_task_details(task: &Task) -> Result<LocalTaskDetail, serde_json::Error> {
    get_details(task.details.clone())
}

async fn validate_tasks(tasks: Vec<Task>) -> Result<(), Vec<String>> {
    let mut errors = Vec::<String>::new();
    for (i, task) in tasks.iter().enumerate() {
        if let Err(err) = get_task_details(&task) {
            errors.push(format!("[Task {}]: {}\n", i, err))
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

async fn expand_task_details(
    details: serde_json::Value,
    parameters: Parameters,
) -> Result<Vec<(serde_json::Value, Vec<(String, String)>)>> {
    let mut expanded_tasks = Vec::new();

    let template = get_details(details.clone())?;

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
        expanded_tasks.push((details, Vec::new()));
    } else {
        // Build out the interpolation sets
        let interpolation_sets = generate_interpolation_sets(&parameters, &vars);

        let new_cmds = apply_vars(&template.command, &interpolation_sets);
        let new_envs = apply_vars(&env_values, &interpolation_sets);
        for ((new_cmd, new_env_vals), int_set) in new_cmds
            .iter()
            .zip(new_envs.iter())
            .zip(interpolation_sets.into_iter())
        {
            let mut new_details = details.clone();
            new_details["command"] = serde_json::json!(new_cmd);
            new_details["environment"] = env_keys
                .iter()
                .cloned()
                .zip(new_env_vals.iter().cloned())
                .collect();
            expanded_tasks.push((new_details, int_set))
        }
    }

    Ok(expanded_tasks)
}

async fn run_task(task: Task, mut stop_rx: oneshot::Receiver<()>) -> TaskAttempt {
    let details = get_task_details(&task).unwrap();
    let mut attempt = TaskAttempt::new();
    attempt.executor.push(format!("{:?}\n", details));
    let (program, args) = details.command.split_first().unwrap();
    let mut command = Command::new(program);
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());
    command.args(args);
    command.envs(details.environment);

    attempt.start_time = Utc::now();
    let mut child = command.spawn().unwrap();

    let mut stdout_handle = child.stdout.take().unwrap();
    let stdout_reader = tokio::spawn(async move {
        let mut data = Vec::new();
        stdout_handle.read_to_end(&mut data).await.unwrap();
        data
    });

    let mut stderr_handle = child.stderr.take().unwrap();
    let stderr_reader = tokio::spawn(async move {
        let mut data = Vec::new();
        stderr_handle.read_to_end(&mut data).await.unwrap();
        data
    });

    // Generate a timeout message, if needed
    let (timeout_tx, mut timeout_rx) = oneshot::channel();
    if details.timeout > 0 {
        let timeout = details.timeout as u64;
        tokio::spawn(async move {
            sleep(Duration::from_millis(1000 * timeout)).await;
            timeout_tx.send(()).unwrap_or(());
        });
    }

    tokio::select! {
        _ = child.wait() => {},
        _ = (&mut stop_rx) => {
            attempt.killed = true;
            child.kill().await.unwrap_or(());
            attempt.executor.push("Task was killed by request".to_owned());
        }
        _ = (&mut timeout_rx) => {
            child.kill().await.unwrap_or(());
            attempt.killed = true;
            attempt.executor.push("Task exceeded the timeout interval and was killed".to_owned());
        }
    }

    // Get any output
    let output = child.wait_with_output().await.unwrap();
    attempt.succeeded = output.status.success();
    attempt.output = String::from_utf8_lossy(&stdout_reader.await.unwrap()).to_string();
    attempt.error = String::from_utf8_lossy(&stderr_reader.await.unwrap()).to_string();
    attempt.exit_code = match output.status.code() {
        Some(code) => code,
        None => -1i32,
    };

    attempt.stop_time = Utc::now();
    attempt
}

/// The mpsc channel can be sized to fit max parallelism
async fn start_local_executor(
    max_parallel: usize,
    mut exe_msgs: mpsc::UnboundedReceiver<ExecutorMessage>,
) {
    let mut task_channels = HashMap::<(RunID, TaskID), oneshot::Sender<()>>::new();

    let mut running = FuturesUnordered::new();

    while let Some(msg) = exe_msgs.recv().await {
        use ExecutorMessage::*;
        match msg {
            ValidateTasks { tasks, response } => {
                tokio::spawn(async move {
                    let result = validate_tasks(tasks).await;
                    response.send(result).unwrap_or(());
                });
            }
            ExpandTaskDetails {
                details,
                parameters,
                response,
            } => {
                tokio::spawn(async move {
                    let result = expand_task_details(details, parameters).await;
                    response.send(result).unwrap_or(());
                });
            }
            ExecuteTask {
                run_id,
                task_id,
                task,
                response,
                tracker,
            } => {
                let (tx, rx) = oneshot::channel();
                task_channels.insert((run_id, task_id.clone()), tx);
                if running.len() == max_parallel {
                    running.next().await;
                }
                let (upd, _) = oneshot::channel();
                tracker
                    .send(TrackerMessage::UpdateTaskState {
                        run_id,
                        task_id: task_id.clone(),
                        state: State::Running,
                        response: upd,
                    })
                    .unwrap_or(());
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

pub fn start(max_parallel: usize, msgs: mpsc::UnboundedReceiver<ExecutorMessage>) {
    tokio::spawn(async move {
        start_local_executor(max_parallel, msgs).await;
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trackers::noop_tracker;

    #[tokio::test(flavor = "multi_thread", worker_threads = 5)]
    async fn test_basic_execution() {
        let task: Task = serde_json::from_str(
            r#"
            {
                "details": {
                    "command": [ "/bin/echo", "hello", "world" ]
                }
            }"#,
        )
        .unwrap();

        let run_id: RunID = 0;
        let task_id = "task_a".to_owned();

        let (log_tx, log_rx) = mpsc::unbounded_channel();
        noop_tracker::start(log_rx);

        let (tx, rx) = mpsc::unbounded_channel();
        super::start(10, rx);

        // Submit the task
        let (run_tx, mut run_rx) = mpsc::unbounded_channel();
        tx.send(ExecutorMessage::ExecuteTask {
            run_id,
            task_id: task_id.clone(),
            task: task,
            response: run_tx,
            tracker: log_tx,
        })
        .expect("Unable to spawn task");

        match run_rx
            .recv()
            .await
            .expect("Unable to receive data from result")
        {
            RunnerMessage::ExecutionReport {
                task_id: rtid,
                attempt,
                ..
            } => {
                assert!(attempt.succeeded);
                assert_eq!(attempt.output, "hello world\n");
                assert_eq!(task_id, rtid);
            }
            _ => {
                panic!("Unexpected message")
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 5)]
    async fn test_stop_execution() {
        let task: Task = serde_json::from_str(
            r#"
            {
                "details": {
                    "command": [ "/bin/sleep", "60" ]
                }
            }"#,
        )
        .unwrap();

        let run_id: RunID = 0;
        let task_id = "task_a".to_owned();

        let (log_tx, log_rx) = mpsc::unbounded_channel();
        noop_tracker::start(log_rx);

        let (tx, rx) = mpsc::unbounded_channel();
        super::start(10, rx);

        // Submit the task
        let (run_tx, mut run_rx) = mpsc::unbounded_channel();
        tx.send(ExecutorMessage::ExecuteTask {
            run_id,
            task_id: task_id.clone(),
            task,
            response: run_tx,
            tracker: log_tx,
        })
        .expect("Unable to spawn task");

        let (response, cancel_rx) = oneshot::channel();
        tx.send(ExecutorMessage::StopTask {
            run_id,
            task_id: task_id.clone(),
            response,
        })
        .expect("Unable to stop task");
        cancel_rx.await.unwrap();

        match run_rx
            .recv()
            .await
            .expect("Unable to receive data from result")
        {
            RunnerMessage::ExecutionReport {
                task_id: rtid,
                attempt,
                ..
            } => {
                assert!(attempt.killed);
                assert!(attempt.stop_time - attempt.start_time < chrono::Duration::seconds(5));
                assert_eq!(task_id, rtid);
            }
            _ => {
                panic!("Unexpected message")
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn test_max_parallel_execution() {
        let task: Task = serde_json::from_str(
            r#"
            {
                "details": {
                    "command": [ "/bin/sleep", "2" ]
                }
            }"#,
        )
        .unwrap();

        let run_id: RunID = 0;
        let task_id = "task_a".to_owned();

        let max_parallel = 5;

        let (log_tx, log_rx) = mpsc::unbounded_channel();
        noop_tracker::start(log_rx);

        let (tx, rx) = mpsc::unbounded_channel();
        super::start(max_parallel, rx);

        let mut chans = Vec::new();
        for i in 0..10 {
            // Submit the task
            let ntid = format!("{}_{}", task_id, i);
            let (run_tx, run_rx) = mpsc::unbounded_channel();
            tx.send(ExecutorMessage::ExecuteTask {
                run_id,
                task_id: ntid,
                task: task.clone(),
                response: run_tx,
                tracker: log_tx.clone(),
            })
            .expect("Unable to spawn task");
            chans.push(run_rx);
        }

        let mut sequence = Vec::new();
        for mut chan in chans {
            let report = chan.recv().await.expect("Unable to recv");
            match report {
                RunnerMessage::ExecutionReport { attempt, .. } => {
                    sequence.push((attempt.start_time, "start"));
                    sequence.push((attempt.stop_time, "stop"));
                }
                _ => {
                    panic!("Unexpected message")
                }
            }
        }

        sequence.sort();

        let mut n_running = 0;
        let mut max_running = 0;
        for (_, event) in sequence {
            if event == "start" {
                n_running += 1;
            } else {
                n_running -= 1;
            }
            if n_running > max_running {
                max_running = n_running;
            }
        }

        assert!(max_running <= max_parallel);
        assert!(max_running >= max_parallel - 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn test_large_ouput() {
        let task: Task = serde_json::from_str(
            r#"
            {
                "details": {
                    "command": [ "/bin/dd", "if=/dev/urandom", "count=10", "bs=1024k" ]
                }
            }"#,
        )
        .unwrap();

        let run_id: RunID = 0;
        let task_id = "task_a".to_owned();

        let (log_tx, log_rx) = mpsc::unbounded_channel();
        noop_tracker::start(log_rx);

        let (exe_tx, exe_rx) = mpsc::unbounded_channel();
        local_executor::start(10, exe_rx);

        // Submit the task
        let (run_tx, mut run_rx) = mpsc::unbounded_channel();
        exe_tx
            .send(ExecutorMessage::ExecuteTask {
                run_id,
                task_id: task_id.clone(),
                task: task.clone(),
                response: run_tx,
                tracker: log_tx,
            })
            .expect("Unable to spawn task");

        let report = run_rx.recv().await.expect("Unable to recv");
        match report {
            RunnerMessage::ExecutionReport {
                task_id: rtid,
                attempt,
                ..
            } => {
                assert!(attempt.succeeded);
                assert!(attempt.output.len() >= 1024 * 1024 * 10);
                assert_eq!(task_id, rtid);
            }
            _ => {
                panic!("Unexpected message")
            }
        }
    }
}

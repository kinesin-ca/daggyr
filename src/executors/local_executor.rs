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

fn get_task_details(task: &Task) -> Result<LocalTaskDetail, serde_json::Error> {
    serde_json::from_value::<LocalTaskDetail>(task.details.clone())
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

async fn expand_tasks(tasks: Vec<Task>, parameters: Parameters) -> Result<Vec<Task>> {
    let mut expanded_tasks = Vec::new();

    for task in tasks {
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
            expanded_tasks.push(task);
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
                new_task.instance = i;
                expanded_tasks.push(new_task);
            }
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
pub async fn start_local_executor(
    max_parallel: usize,
    mut exe_msgs: mpsc::Receiver<ExecutorMessage>,
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
            ExpandTasks {
                tasks,
                parameters,
                response,
            } => {
                tokio::spawn(async move {
                    let result = expand_tasks(tasks, parameters).await;
                    response.send(result).unwrap_or(());
                });
            }
            ExecuteTask {
                run_id,
                task_id,
                task,
                response,
                // logger,
            } => {
                let (tx, rx) = oneshot::channel();
                task_channels.insert((run_id, task_id), tx);
                if running.len() == max_parallel {
                    running.next().await;
                }
                running.push(tokio::spawn(async move {
                    let attempt = run_task(task, rx).await;
                    response.send(attempt).unwrap_or(());
                }));
            }
            StopTask { run_id, task_id } => {
                if let Some(tx) = task_channels.remove(&(run_id, task_id)) {
                    tx.send(()).unwrap_or(());
                }
            }
            Stop {} => {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 5)]
    async fn test_basic_execution() {
        let task: Task = serde_json::from_str(
            r#"
            {
                "class": "simple_task",
                "details": {
                    "command": [ "/bin/echo", "hello", "world" ]
                }
            }"#,
        )
        .unwrap();

        let (tx, rx) = mpsc::channel(10);

        tokio::spawn(async move {
            start_local_executor(10, rx).await;
        });

        // Submit the task
        let (run_tx, run_rx) = oneshot::channel();
        tx.send(ExecutorMessage::ExecuteTask {
            run_id: 0,
            task_id: 0,
            task: task,
            response: run_tx,
        })
        .await
        .expect("Unable to spawn task");

        let attempt = run_rx.await.expect("Unable to receive data from result");

        assert!(attempt.succeeded);
        assert_eq!(attempt.output, "hello world\n");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 5)]
    async fn test_stop_execution() {
        let task: Task = serde_json::from_str(
            r#"
            {
                "class": "simple_task",
                "details": {
                    "command": [ "/bin/sleep", "60" ]
                }
            }"#,
        )
        .unwrap();

        let (tx, rx) = mpsc::channel(10);

        tokio::spawn(async move {
            start_local_executor(3, rx).await;
        });

        // Submit the task
        let (run_tx, run_rx) = oneshot::channel();
        tx.send(ExecutorMessage::ExecuteTask {
            run_id: 0,
            task_id: 0,
            task: task,
            response: run_tx,
        })
        .await
        .expect("Unable to spawn task");

        tx.send(ExecutorMessage::StopTask {
            run_id: 0,
            task_id: 0,
        })
        .await
        .expect("Unable to stop task");

        let attempt = run_rx.await.expect("Unable to receive data from result");

        assert!(attempt.killed);
        assert!(attempt.stop_time - attempt.start_time < chrono::Duration::seconds(5));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn test_max_parallel_execution() {
        let task: Task = serde_json::from_str(
            r#"
            {
                "class": "simple_task",
                "details": {
                    "command": [ "/bin/sleep", "2" ]
                }
            }"#,
        )
        .unwrap();

        let max_parallel = 5;
        let (tx, rx) = mpsc::channel(100);
        tokio::spawn(async move {
            start_local_executor(max_parallel, rx).await;
        });

        let mut chans = Vec::new();
        for i in 0..10 {
            // Submit the task
            let (run_tx, run_rx) = oneshot::channel();
            tx.send(ExecutorMessage::ExecuteTask {
                run_id: 0,
                task_id: i,
                task: task.clone(),
                response: run_tx,
            })
            .await
            .expect("Unable to spawn task");
            chans.push(run_rx);
        }

        let mut sequence = Vec::new();
        for chan in chans {
            let attempt = chan.await.expect("Unable to recv");
            sequence.push((attempt.start_time, "start"));
            sequence.push((attempt.stop_time, "stop"));
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
                "class": "simple_task",
                "details": {
                    "command": [ "/usr/bin/dd", "if=/dev/random", "count=10", "bs=1024k" ]
                }
            }"#,
        )
        .unwrap();

        let (tx, rx) = mpsc::channel(100);
        tokio::spawn(async move {
            start_local_executor(10, rx).await;
        });

        // Submit the task
        let (run_tx, run_rx) = oneshot::channel();
        tx.send(ExecutorMessage::ExecuteTask {
            run_id: 0,
            task_id: 0,
            task: task.clone(),
            response: run_tx,
        })
        .await
        .expect("Unable to spawn task");

        let attempt = run_rx.await.expect("Unable to recv");

        assert!(attempt.succeeded);
        assert!(attempt.output.len() >= 1024 * 1024 * 10);
    }
}

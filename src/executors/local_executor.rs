use super::*;
use crate::structs::*;
use crate::utilities::*;
use async_process::{Command, Stdio};
use chrono::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use tokio::task::yield_now;

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

#[derive(Clone)]
struct LocalExecutor {
    run_flags: Arc<Mutex<HashMap<(RunID, TaskID), bool>>>,
}

impl LocalExecutor {
    fn new() -> Self {
        LocalExecutor {
            run_flags: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn expand_task(&self, task: &Task, parameters: &Parameters) -> Result<Vec<Task>> {
        let mut tasks: Vec<Task> = Vec::new();
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
            Ok(vec![task.clone()])
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
                tasks.push(new_task);
            }

            Ok(tasks)
        }
    }
}

#[async_trait]
impl Executor for LocalExecutor {
    async fn validate_tasks(&self, tasks: Vec<Task>) -> Result<(), Vec<String>> {
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

    async fn expand_tasks(&self, tasks: Vec<Task>, parameters: Parameters) -> Result<Vec<Task>> {
        let mut all_tasks = Vec::new();

        for task in tasks {
            all_tasks.extend(self.expand_task(&task, &parameters)?);
        }
        Ok(all_tasks)
    }

    async fn execute_task(
        &self,
        run_id: RunID,
        task_id: TaskID,
        task: Task,
    ) -> Result<TaskAttempt> {
        let details = get_task_details(&task).unwrap();
        let mut attempt = TaskAttempt::new();
        attempt.executor.push(format!("{:?}\n", details));
        let (program, args) = details.command.split_first().unwrap();
        let mut command = Command::new(program);
        command.stdout(Stdio::piped()).stderr(Stdio::piped());
        command.args(args);
        command.envs(details.environment);

        let tid = (run_id, task_id);

        {
            let mut run_flags = self.run_flags.lock().unwrap();
            if run_flags.get(&tid).is_none() {
                println!("Inserting true for {:?}", tid);
                run_flags.insert(tid, true);
            }
        }

        attempt.start_time = Utc::now();
        match command.spawn() {
            Ok(mut child) => {
                loop {
                    match child.try_status() {
                        Ok(Some(_)) => {
                            let child = child.output().await?;
                            attempt.succeeded = child.status.success();
                            attempt.output = String::from_utf8_lossy(&child.stdout).to_string();
                            attempt.error = String::from_utf8_lossy(&child.stderr).to_string();
                            attempt.exit_code = match child.status.code() {
                                Some(code) => code,
                                None => -1i32,
                            };
                            break;
                        }
                        Ok(None) => {
                            // Job not finished
                        }
                        Err(_) => {
                            // Job doesn't exist anymore
                        }
                    }
                    if details.timeout > 0 {
                        let elapsed_seconds = (Utc::now() - attempt.start_time).num_seconds();
                        if elapsed_seconds > details.timeout {
                            match child.kill() {
                                Ok(()) => {
                                    attempt
                                        .executor
                                        .push("Task killed due to timeout".to_owned());
                                }
                                Err(_) => {}
                            }
                        }
                    }

                    let running;
                    {
                        let run_flags = self.run_flags.lock().unwrap();
                        running = *run_flags.get(&tid).unwrap_or(&false);
                    }
                    if !running {
                        if let Ok(_) = child.kill() {
                            attempt.executor.push("Task killed".to_owned());
                            attempt.killed = true;
                            break;
                        }
                    }
                    yield_now().await;
                }
            }
            Err(e) => {
                attempt.succeeded = false;
                attempt.error = e.to_string();
            }
        }

        attempt.stop_time = Utc::now();
        let mut run_flags = self.run_flags.lock().unwrap();
        run_flags.remove(&tid);
        println!("Returning attempt");
        Ok(attempt)
    }

    async fn stop_task(&self, run_id: RunID, task_id: TaskID) -> Result<()> {
        let tid = (run_id, task_id);
        let mut run_flags = self.run_flags.lock().unwrap();
        run_flags.insert(tid, false);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::task::spawn;

    #[tokio::test]
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

        let le = LocalExecutor::new();

        match le.execute_task(0, 0, task).await {
            Ok(attempt) => {
                assert!(attempt.succeeded);
                assert_eq!(attempt.output, "hello world\n");
            }
            Err(e) => {
                panic!("Unexpected error: {:?}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_stop_task() {
        let task: Task = serde_json::from_str(
            r#"
                {
                    "class": "sleepy_task",
                    "details": {
                        "command": [ "/bin/sleep", "60" ]
                    }
                }"#,
        )
        .unwrap();
        let le = LocalExecutor::new();
        let lec = LocalExecutor::new();

        let handle = spawn(async move {
            let res = le.execute_task(0, 0, task).await;
            res
        });

        println!("Spawned task");
        std::thread::sleep(std::time::Duration::from_secs(1));

        lec.stop_task(0, 0).await;
        println!("Stopped task");

        match handle.await.unwrap() {
            Ok(attempt) => {
                assert!(attempt.killed);
                assert!(attempt.stop_time - attempt.start_time < chrono::Duration::seconds(5));
            }
            Err(e) => {
                panic!("{:?}", e);
            }
        }
        ()
    }
}

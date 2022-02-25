use super::*;
use crate::structs::{Parameters, RunID, Task, TaskAttempt, TaskID};
use async_trait::async_trait;
use tokio;

#[async_trait]
pub trait Executor {
    async fn validate_tasks(&self, tasks: Vec<Task>) -> Result<(), Vec<String>>;
    async fn expand_tasks(&self, tasks: Vec<Task>, parameters: Parameters) -> Result<Vec<Task>>;
    async fn execute_task(&self, run_id: RunID, task_id: TaskID, task: Task)
        -> Result<TaskAttempt>;
    async fn stop_task(&self, run_id: RunID, task_id: TaskID) -> Result<()>;
}

pub mod local_executor;

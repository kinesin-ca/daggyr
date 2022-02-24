use super::*;
use crate::structs::{RunID, Task, TaskAttempt, TaskID};
use async_trait::async_trait;

#[async_trait]
pub trait Executor {
    async fn validate_tasks(tasks: &Vec<Task>) -> Result<()>;
    async fn expand_tasks(tasks: &Vec<Task>) -> Result<Vec<Task>>;
    async fn execute_task(run_id: RunID, task_id: TaskID, task: Task) -> Result<TaskAttempt>;
    async fn stop_task(run_id: RunID, task_id: TaskID) -> Result<()>;
}

use super::*;
use crate::structs::{Parameters, RunID, Task, TaskAttempt, TaskID};
use tokio::sync::oneshot;

#[derive(Debug)]
pub enum ExecutorMessage {
    ValidateTasks {
        tasks: Vec<Task>,
        response: oneshot::Sender<Result<(), Vec<String>>>,
    },
    ExpandTasks {
        tasks: Vec<Task>,
        parameters: Parameters,
        response: oneshot::Sender<Result<Vec<Task>>>,
    },
    ExecuteTask {
        run_id: RunID,
        task_id: TaskID,
        task: Task,
        response: oneshot::Sender<TaskAttempt>,
        // logger: Sender<LoggerMessage>,
    },
    StopTask {
        run_id: RunID,
        task_id: TaskID,
    },
    Stop {},
}

pub mod local_executor;

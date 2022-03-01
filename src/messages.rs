use crate::structs::*;
use crate::Result;
use tokio::sync::{mpsc, oneshot};
#[derive(Debug)]
pub enum LoggerMessage {
    // Creation
    CreateRun {
        tags: Tags,
        parameters: Parameters,
        response: oneshot::Sender<Result<RunID>>,
    },

    // Updates
    AddTasks {
        run_id: RunID,
        tasks: Vec<Task>,
        offset: usize,
    },
    UpdateTask {
        run_id: RunID,
        task_id: TaskID,
        task: Task,
    },
    UpdateState {
        run_id: RunID,
        state: State,
    },
    UpdateTaskState {
        run_id: RunID,
        task_id: TaskID,
        state: State,
    },
    LogTaskAttempt {
        run_id: RunID,
        task_id: TaskID,
        attempt: TaskAttempt,
    },

    // Queries
    /// Defaults to running states
    GetRuns {
        tags: Tags,
        states: HashSet<State>,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        response: oneshot::Sender<Vec<RunSummary>>,
    },

    GetRun {
        run_id: RunID,
        response: oneshot::Sender<Result<RunRecord>>,
    },

    GetState {
        run_id: RunID,
        response: oneshot::Sender<Result<StateChange>>,
    },

    GetStateUpdates {
        run_id: RunID,
        response: oneshot::Sender<Result<Vec<StateChange>>>,
    },
    GetTaskSummary {
        run_id: RunID,
        response: oneshot::Sender<Result<Vec<TaskSummary>>>,
    },
    GetTasks {
        run_id: RunID,
        response: oneshot::Sender<Result<Vec<TaskRecord>>>,
    },
    GetTask {
        run_id: RunID,
        task_id: TaskID,
        response: oneshot::Sender<Result<TaskRecord>>,
    },

    Stop {},
}

#[derive(Debug)]
pub enum RunnerMessage {
    Start {
        tags: Tags,
        tasks: Vec<Task>,
        parameters: Parameters,
        logger: mpsc::Sender<LoggerMessage>,
        executor: mpsc::Sender<ExecutorMessage>,
        response: oneshot::Sender<Result<RunID>>,
    },
    Retry {
        run_id: RunID,
        logger: mpsc::Sender<LoggerMessage>,
        executor: mpsc::Sender<ExecutorMessage>,
        response: oneshot::Sender<Result<()>>,
    },
    ExecutionReport {
        run_id: RunID,
        task_id: TaskID,
        attempt: TaskAttempt,
    },
    StopRun {
        run_id: RunID,
        response: oneshot::Sender<()>,
    },
    Stop {},
}

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
        response: mpsc::Sender<RunnerMessage>,
        // logger: Sender<LoggerMessage>,
    },
    StopTask {
        run_id: RunID,
        task_id: TaskID,
    },
    Stop {},
}

use crate::structs::*;
use crate::Result;
use tokio::sync::{mpsc, oneshot};
#[derive(Debug)]
pub enum TrackerMessage {
    // Creation
    CreateRun {
        tags: RunTags,
        parameters: Parameters,
        response: oneshot::Sender<Result<RunID>>,
    },

    // Updates
    AddTasks {
        run_id: RunID,
        tasks: TaskSet,
        response: oneshot::Sender<Result<()>>,
    },
    UpdateTask {
        task_id: TaskID,
        task: Task,
        response: oneshot::Sender<Result<()>>,
    },
    UpdateState {
        run_id: RunID,
        state: State,
        response: oneshot::Sender<Result<()>>,
    },
    UpdateTaskState {
        task_id: TaskID,
        state: State,
        response: oneshot::Sender<Result<()>>,
    },
    LogTaskAttempt {
        task_id: TaskID,
        attempt: TaskAttempt,
        response: oneshot::Sender<Result<()>>,
    },

    // Queries
    /// Defaults to running states
    GetRuns {
        tags: RunTags,
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
        response: oneshot::Sender<Result<HashMap<TaskID, TaskRecord>>>,
    },
    GetTask {
        task_id: TaskID,
        response: oneshot::Sender<Result<TaskRecord>>,
    },

    Stop {},
}

#[derive(Debug)]
pub enum RunnerMessage {
    Start {
        tags: RunTags,
        tasks: TaskSet,
        parameters: Parameters,
        tracker: mpsc::UnboundedSender<TrackerMessage>,
        executor: mpsc::UnboundedSender<ExecutorMessage>,
        response: oneshot::Sender<Result<RunID>>,
    },
    Retry {
        run_id: RunID,
        tracker: mpsc::UnboundedSender<TrackerMessage>,
        executor: mpsc::UnboundedSender<ExecutorMessage>,
        response: oneshot::Sender<Result<()>>,
    },
    ExecutionReport {
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
        tasks: TaskSet,
        parameters: Parameters,
        response: oneshot::Sender<Result<TaskSet>>,
    },
    ExecuteTask {
        task_id: TaskID,
        task: Task,
        response: mpsc::UnboundedSender<RunnerMessage>,
        tracker: mpsc::UnboundedSender<TrackerMessage>,
    },
    StopTask {
        task_id: TaskID,
        response: oneshot::Sender<()>,
    },
    Stop {},
}

/// Message used to report on the completion of a task
#[derive(Serialize, Deserialize, Clone)]
pub struct AttemptReport {
    pub task_id: TaskID,
    pub attempt: TaskAttempt,
}

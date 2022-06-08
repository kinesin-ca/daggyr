//! Contains all of the messages passed between different components.

use crate::structs::{
    DateTime, Deserialize, ExpansionValues, HashMap, HashSet, Parameters, RunID, RunRecord,
    RunSummary, RunTags, Serialize, State, StateChange, Task, TaskAttempt, TaskID, TaskRecord,
    TaskSet, TaskSummary, Utc,
};
use crate::Result;
use tokio::sync::{mpsc, oneshot};

/// `TrackerMessage`s are used to interact with a Run State Tracker actor
#[derive(Debug)]
pub enum TrackerMessage {
    /// Register a new run with the given `tags` and `parameters`.
    /// Response is sent the RunID that this run should be known as.
    CreateRun {
        tags: RunTags,
        parameters: Parameters,
        response: oneshot::Sender<Result<RunID>>,
    },

    /// Add tasks to an existing run.
    /// Errors
    ///   Will return an error if the tracker was unable to store the new tasks.
    AddTasks {
        run_id: RunID,
        tasks: TaskSet,
        response: oneshot::Sender<Result<()>>,
    },

    /// Update the definition of a task in the tracker.
    /// Errors
    ///   Will return an error if the tracker was unable to store the new definition.
    UpdateTask {
        run_id: RunID,
        task_id: TaskID,
        task: Task,
        response: oneshot::Sender<Result<()>>,
    },

    /// Record the transition of the run identified by `run_id` to the state `state`
    /// Errors
    ///   Will return an error if the tracker was unable to update the state
    UpdateState {
        run_id: RunID,
        state: State,
        response: oneshot::Sender<Result<()>>,
    },

    /// Record the transition of the task identified by `task_id` in the run identified by `run_id` to the state `state`
    /// Errors
    ///   Will return an error if the tracker was unable to update the state
    UpdateTaskState {
        run_id: RunID,
        task_id: TaskID,
        state: State,
        response: oneshot::Sender<Result<()>>,
    },

    /// Record the execution attempt for the task `task_id` in run `run_id`.
    /// Errors
    ///   Will return an error if the tracker was unable to record the attempt.
    LogTaskAttempt {
        run_id: RunID,
        task_id: TaskID,
        attempt: TaskAttempt,
        response: oneshot::Sender<Result<()>>,
    },

    // Queries

    /// Query the tracker for runs matching the given criteria.
    /// If all criteria are None, then all runs are returned.
    /// Errors
    ///   Will return an `Err` if the back-end storage can't be queried, or no
    ///   such run exists.
    GetRuns {
        tags: Option<RunTags>,
        states: Option<HashSet<State>>,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
        response: oneshot::Sender<Result<Vec<RunSummary>>>,
    },

    /// Retrieve the details of a specific run identified by `run_id` 
    /// Errors
    ///   Will return an `Err` if the back-end storage can't be queried, or no
    ///   such run exists.
    GetRun {
        run_id: RunID,
        response: oneshot::Sender<Result<RunRecord>>,
    },

    /// Get the current state of the run identified by `run_id`
    /// Errors
    ///   Will return an `Err` if the back-end storage can't be queried, or no
    ///   such run exists.
    GetState {
        run_id: RunID,
        response: oneshot::Sender<Result<StateChange>>,
    },

    /// Retrieve all of the state transition records for the run identified by `run_id`
    /// Errors
    ///   Will return an `Err` if the back-end storage can't be queried, or no
    ///   such run exists.
    GetStateUpdates {
        run_id: RunID,
        response: oneshot::Sender<Result<Vec<StateChange>>>,
    },

    /// Retrieve a summary of task states for the run identified by `run_id`
    /// Errors
    ///   Will return an `Err` if the back-end storage can't be queried, or no
    ///   such run exists.
    GetTaskSummary {
        run_id: RunID,
        response: oneshot::Sender<Result<Vec<TaskSummary>>>,
    },
    
    /// Retrieve the full details of all tasks
    /// Errors
    ///   Will return an `Err` if the back-end storage can't be queried, or no
    ///   such run exists.
    GetTasks {
        run_id: RunID,
        response: oneshot::Sender<Result<HashMap<TaskID, TaskRecord>>>,
    },

    /// Retrieve the full details of a specific task
    /// Errors
    ///   Will return an `Err` if the back-end storage can't be queried, or no
    ///   such run exists.
    GetTask {
        run_id: RunID,
        task_id: TaskID,
        response: oneshot::Sender<Result<TaskRecord>>,
    },

    /// Stop a Tracker actor
    Stop {},
}

/// Messages to interact with a Runner actor
#[derive(Debug)]
pub enum RunnerMessage {
    /// Create a run with the given parameters, returning the RunID after the
    /// run has been validated and properly registered with the tracker.
    /// Errors
    ///    Will return Err if the tasks are invalid for the given executor, the
    ///    tracker can't register the new run, or the run can't be enqueued.
    Start {
        tags: RunTags,
        tasks: TaskSet,
        parameters: Parameters,
        tracker: mpsc::UnboundedSender<TrackerMessage>,
        executor: mpsc::UnboundedSender<ExecutorMessage>,
        response: oneshot::Sender<Result<RunID>>,
    },

    /// Re-queue the Run identified by `run_id`. The run will be loaded from the tracker,
    /// tasks not in the `State::Completed` state will be reset to `State::Queued`, and
    /// the run will be re-queued up for running.
    /// 
    /// Errors
    ///    Will return an `Err` if `run_id` is already running, if the tracker doesn't know
    ///    of the `run_id`, or if the run couldn't be enqueued.
    Retry {
        run_id: RunID,
        tracker: mpsc::UnboundedSender<TrackerMessage>,
        executor: mpsc::UnboundedSender<ExecutorMessage>,
        response: oneshot::Sender<Result<()>>,
    },

    /// Send the result of an attempted task execution to the Runner
    ExecutionReport {
        run_id: RunID,
        task_id: TaskID,
        attempt: TaskAttempt,
    },

    /// Kill a run. Killing a run that isn't running is a noop.
    StopRun {
        run_id: RunID,
        response: oneshot::Sender<()>,
    },

    /// Stop the Runner actor
    Stop {},
}

/// Messages for interacting with an Executor
#[derive(Debug)]
pub enum ExecutorMessage {

    /// Validate a set of tasks.
    /// Errors
    ///    Returns the vector of task issues
    ValidateTask {
        details: serde_json::Value,
        response: oneshot::Sender<Result<()>>,
    },

    /// Expand the task details provided, using the expansion parameters `parameters`
    /// Errors
    ///    Will return `Err` if the tasks are invalid, according to the executor
    ExpandTaskDetails {
        details: serde_json::Value,
        parameters: Parameters,
        response: oneshot::Sender<Result<Vec<(serde_json::Value, ExpansionValues)>>>,
    },

    /// Execute the given task, along with enough information
    /// Errors
    ///    Will return `Err` if the tasks are invalid, according to the executor
    ExecuteTask {
        run_id: RunID,
        task_id: TaskID,
        details: serde_json::Value,
        response: mpsc::UnboundedSender<RunnerMessage>,
        tracker: mpsc::UnboundedSender<TrackerMessage>,
    },

    /// Kill the given task, if any. Only sends results when kill was successful.
    StopTask {
        run_id: RunID,
        task_id: TaskID,
        response: oneshot::Sender<()>,
    },
    Stop {},
}

/// Message used to report on the completion of a task
#[derive(Serialize, Deserialize, Clone)]
pub struct AttemptReport {
    pub run_id: RunID,
    pub task_id: TaskID,
    pub attempt: TaskAttempt,
}

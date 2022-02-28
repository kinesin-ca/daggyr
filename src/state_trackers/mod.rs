pub mod structs;

use crate::structs::*;
use structs::*;
use tokio::sync::oneshot::Sender;

#[derive(Debug)]
pub enum LoggerMessage {
    // Creation
    CreateRun {
        tags: Tags,
        parameters: Parameters,
        response: Sender<Result<RunID, String>>,
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
        response: Sender<Vec<RunSummary>>,
    },

    GetRun {
        run_id: RunID,
        response: Sender<Result<RunRecord, String>>,
    },

    GetState {
        run_id: RunID,
        response: Sender<Result<StateChange, String>>,
    },

    GetStateUpdates {
        run_id: RunID,
        response: Sender<Result<Vec<StateChange>, String>>,
    },
    GetTaskSummary {
        run_id: RunID,
        response: Sender<Result<Vec<TaskSummary>, String>>,
    },
    GetTasks {
        run_id: RunID,
        response: Sender<Result<Vec<TaskRecord>, String>>,
    },
    GetTask {
        run_id: RunID,
        task_id: TaskID,
        response: Sender<Result<TaskRecord, String>>,
    },

    Stop {},
}

/*
use super::*;
use crate::structs::*;
use async_trait::async_trait;
use chrono::prelude::*;
use std::collections::HashSet;
use structs::*;

#[async_trait]
trait StateTracker {
    // Maintaning State
    async fn create_run(tags: Tags, parameters: Parameters) -> Result<RunID>;
    async fn add_tasks(run_id: RunID, tasks: Vec<Task>, offset: usize) -> Result<(usize, usize)>;
    async fn update_run_state(run_id: RunID, state: State) -> Result<()>;
    async fn update_task_state(run_id: RunID, task_id: TaskID, state: State) -> Result<()>;
    async fn add_task_attempt(run_id: RunID, task_id: TaskID, attempt: TaskAttempt) -> Result<()>;

    // Retrieving State
    async fn get_runs(
        tags: Tags,
        states: HashSet<State>,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<RunSummary>>;
}
*/

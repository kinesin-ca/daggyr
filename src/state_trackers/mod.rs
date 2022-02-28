pub mod structs;

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

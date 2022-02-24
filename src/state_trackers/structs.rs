use crate::structs::*;
use chrono::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct StateChange {
    pub datetime: DateTime<Utc>,
    pub state: State,
}

impl StateChange {
    fn new(state: State) -> Self {
        StateChange {
            datetime: Utc::now(),
            state: state,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TaskSummary {
    pub class: String,
    pub instance: usize,
    pub task_id: TaskID,
    pub state: State,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TaskRecord {
    pub task: Task,
    pub attempts: Vec<TaskAttempt>,
    pub state_changes: Vec<StateChange>,
}

impl TaskRecord {
    fn new(task: Task) -> Self {
        TaskRecord {
            task: task,
            attempts: Vec::new(),
            state_changes: Vec::new(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RunRecord {
    pub tags: Tags,
    pub expansion_values: Parameters,
    pub tasks: Vec<TaskRecord>,
    pub state_changes: Vec<StateChange>,
}

impl RunRecord {
    pub fn new(tags: Tags, expansion_values: Parameters) -> Self {
        RunRecord {
            tags,
            expansion_values,
            tasks: Vec::new(),
            state_changes: vec![StateChange::new(State::Queued)],
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RunSummary {
    pub run_id: RunID,
    pub tags: Tags,
    pub state: State,
    pub start_time: DateTime<Utc>,
    pub last_update_time: DateTime<Utc>,
    pub task_states: HashMap<State, usize>,
}

impl RunSummary {
    fn new(run_id: RunID, tags: Tags, state: State) -> Self {
        RunSummary {
            run_id,
            tags,
            state,
            start_time: Utc::now(),
            last_update_time: Utc::now(),
            task_states: HashMap::new(),
        }
    }
}

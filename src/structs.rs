use super::Result;
pub use chrono::{DateTime, Utc};
pub use serde::{Deserialize, Serialize};
pub use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::ops::{Deref, DerefMut};

pub type RunID = usize;
pub type TaskID = String;
pub type TaskDetails = serde_json::Value;

pub type Parameters = HashMap<String, Vec<String>>;
pub type ExpansionValues = Vec<(String, String)>;

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct RunTags(HashMap<String, String>);

impl RunTags {
    #[must_use]
    pub fn new() -> Self {
        RunTags(HashMap::new())
    }

    /// Returns true if
    #[must_use]
    pub fn is_subset_of(&self, other: &RunTags) -> bool {
        for (k, v) in self.iter() {
            match other.get(k) {
                Some(val) => {
                    if val != v {
                        return false;
                    }
                }
                None => {
                    return false;
                }
            }
        }
        true
    }
}

impl Deref for RunTags {
    type Target = HashMap<String, String>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for RunTags {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct TaskResources(HashMap<String, i64>);

impl Deref for TaskResources {
    type Target = HashMap<String, i64>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for TaskResources {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl TaskResources {
    #[must_use]
    pub fn new() -> Self {
        TaskResources(HashMap::new())
    }

    #[must_use]
    pub fn can_satisfy(&self, requirements: &TaskResources) -> bool {
        requirements
            .iter()
            .all(|(k, v)| self.contains_key(k) && self[k] >= *v)
    }

    /// Subtracts resources from available resources.
    /// # Errors
    /// Returns an `Err` if the requested resources cannot be fulfilled
    /// # Panics
    /// It doesn't, keys are checked for ahead-of-time
    pub fn sub(&mut self, resources: &TaskResources) -> Result<()> {
        if self.can_satisfy(resources) {
            for (k, v) in resources.iter() {
                *self.get_mut(k).unwrap() -= v;
            }
            Ok(())
        } else {
            Err(anyhow!("Cannot satisfy requested resources"))
        }
    }

    /// # Panics
    /// It doesn't, keys are checked for ahead-of-time
    pub fn add(&mut self, resources: &TaskResources) {
        for (k, v) in resources.iter() {
            if self.contains_key(k) {
                *self.get_mut(k).unwrap() += *v;
            } else {
                self.insert(k.clone(), *v);
            }
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Copy, Debug, PartialEq, Hash, Eq)]
pub enum State {
    Queued,
    Running,
    Errored,
    Completed,
    Killed,
}

#[derive(Clone, Serialize, Deserialize, Copy, Debug, PartialEq, Hash, Eq)]
pub enum TaskType {
    Normal,
    Structural,
}

impl Default for TaskType {
    fn default() -> Self {
        TaskType::Normal
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Default)]
pub struct Task {
    #[serde(default)]
    pub run_id: RunID,

    #[serde(default)]
    pub expansion_values: Vec<(String, String)>,

    #[serde(default)]
    pub parameters: Parameters,

    #[serde(default)]
    pub task_type: TaskType,

    #[serde(default)]
    pub is_generator: bool,

    #[serde(default)]
    pub max_retries: u32,

    #[serde(default)]
    pub retries: u32,

    #[serde(default)]
    pub children: Vec<String>,

    #[serde(default)]
    pub parents: Vec<String>,

    pub details: TaskDetails,
}

impl Task {
    #[must_use]
    pub fn new() -> Self {
        Task::default()
    }
}

pub type TaskSet = HashMap<TaskID, Task>;

#[test]
fn test_task_deserialization() {
    let data = r#"
    {
        "details": {
            "cmd": "Some parameter here",
            "env": {
                "ENVVAR": 16
            }
        }
    }"#;

    // Parse the string of data into serde_json::Value.
    let task: Task = serde_json::from_str(data).unwrap();
    assert!(!task.is_generator);
    assert_eq!(task.max_retries, 0);
    assert_eq!(task.retries, 0);
    assert!(task.children.is_empty());
    assert!(task.parents.is_empty());
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TaskAttempt {
    #[serde(default = "chrono::Utc::now")]
    #[cfg_attr(
        feature = "mongo",
        serde(with = "bson::serde_helpers::chrono_datetime_as_bson_datetime")
    )]
    pub start_time: DateTime<Utc>,

    #[serde(default = "chrono::Utc::now")]
    #[cfg_attr(
        feature = "mongo",
        serde(with = "bson::serde_helpers::chrono_datetime_as_bson_datetime")
    )]
    pub stop_time: DateTime<Utc>,

    #[serde(default)]
    pub succeeded: bool,

    #[serde(default)]
    pub killed: bool,

    #[serde(default)]
    pub output: String,

    #[serde(default)]
    pub error: String,

    #[serde(default)]
    pub executor: Vec<String>,

    #[serde(default)]
    pub exit_code: i32,

    #[serde(default)]
    pub max_cpu: u32,

    #[serde(default)]
    pub max_rss: u64,
}

impl Default for TaskAttempt {
    fn default() -> Self {
        TaskAttempt {
            start_time: Utc::now(),
            stop_time: Utc::now(),
            succeeded: false,
            killed: false,
            output: "".to_owned(),
            error: "".to_owned(),
            executor: Vec::new(),
            exit_code: 0i32,
            max_cpu: 0,
            max_rss: 0,
        }
    }
}

impl TaskAttempt {
    #[must_use]
    pub fn new() -> Self {
        TaskAttempt::default()
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct StateChange {
    #[cfg_attr(
        feature = "mongo",
        serde(with = "bson::serde_helpers::chrono_datetime_as_bson_datetime")
    )]
    pub datetime: DateTime<Utc>,
    pub state: State,
}

impl Default for StateChange {
    fn default() -> Self {
        StateChange {
            datetime: Utc::now(),
            state: State::Queued,
        }
    }
}

impl StateChange {
    #[must_use]
    pub fn new(state: State) -> Self {
        StateChange {
            state,
            ..StateChange::default()
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TaskSummary {
    pub task_id: TaskID,
    pub state: State,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct TaskRecord {
    pub task: Task,
    pub attempts: Vec<TaskAttempt>,
    pub state_changes: Vec<StateChange>,
}

impl TaskRecord {
    #[must_use]
    pub fn new(task: Task) -> Self {
        TaskRecord {
            task,
            ..TaskRecord::default()
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct RunRecord {
    pub tags: RunTags,
    pub parameters: Parameters,
    pub tasks: HashMap<TaskID, TaskRecord>,
    pub state_changes: Vec<StateChange>,
}

impl RunRecord {
    #[must_use]
    pub fn new(tags: RunTags, parameters: Parameters) -> Self {
        RunRecord {
            tags,
            parameters,
            state_changes: vec![StateChange::new(State::Queued)],
            ..RunRecord::default()
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RunSummary {
    pub run_id: RunID,
    pub tags: RunTags,
    pub state: State,
    pub start_time: DateTime<Utc>,
    pub last_update_time: DateTime<Utc>,
    pub task_states: HashMap<State, usize>,
}

impl Default for RunSummary {
    fn default() -> Self {
        RunSummary {
            run_id: 0,
            tags: RunTags::new(),
            state: State::Queued,
            start_time: Utc::now(),
            last_update_time: Utc::now(),
            task_states: HashMap::new(),
        }
    }
}

impl RunSummary {
    #[must_use]
    pub fn new(run_id: RunID, tags: RunTags, state: State) -> Self {
        RunSummary {
            run_id,
            tags,
            state,
            ..RunSummary::default()
        }
    }
}

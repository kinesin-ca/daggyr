use super::Result;
pub use chrono::{DateTime, Utc};
pub use serde::{Deserialize, Serialize};
pub use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::ops::{Deref, DerefMut};

pub type RunID = usize;

pub type Parameters = HashMap<String, Vec<String>>;

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct RunTags(HashMap<String, String>);

impl RunTags {
    pub fn new() -> Self {
        RunTags(HashMap::new())
    }

    pub fn matches(&self, other: &RunTags) -> bool {
        for (k, v) in other.iter() {
            match self.get(k) {
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
        return true;
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
    pub fn new() -> Self {
        TaskResources(HashMap::new())
    }

    pub fn can_satisfy(&self, requirements: &TaskResources) -> bool {
        requirements
            .iter()
            .all(|(k, v)| self.contains_key(k) && self[k] >= *v)
    }

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

#[derive(Clone, Debug, Serialize, Deserialize, Default, Hash, PartialEq, Eq)]
pub struct TaskID {
    pub run_id: RunID,
    pub name: String,
    pub instance: usize,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Default)]
pub struct Task {
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

    pub details: serde_json::Value,
}

impl Task {
    pub fn new() -> Self {
        Task {
            is_generator: false,
            max_retries: 0,
            retries: 0,
            details: serde_json::Value::Null,
            children: Vec::new(),
            parents: Vec::new(),
        }
    }
}

pub type TaskSet = HashMap<TaskID, Task>;

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct TaskSetSpec(HashMap<String, Task>);

impl Deref for TaskSetSpec {
    type Target = HashMap<String, Task>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for TaskSetSpec {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl TaskSetSpec {
    pub fn new() -> Self {
        TaskSetSpec(HashMap::new())
    }

    pub fn to_task_set(&self) -> TaskSet {
        self.iter()
            .map(|(name, task)| {
                (
                    TaskID {
                        run_id: 0,
                        name: name.clone(),
                        instance: 0,
                    },
                    task.clone(),
                )
            })
            .collect()
    }
}

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
    assert_eq!(task.is_generator, false);
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

impl TaskAttempt {
    pub fn new() -> Self {
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

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct StateChange {
    #[cfg_attr(
        feature = "mongo",
        serde(with = "bson::serde_helpers::chrono_datetime_as_bson_datetime")
    )]
    pub datetime: DateTime<Utc>,
    pub state: State,
}

impl StateChange {
    pub fn new(state: State) -> Self {
        StateChange {
            datetime: Utc::now(),
            state: state,
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
    pub fn new(task: Task) -> Self {
        TaskRecord {
            task: task,
            attempts: Vec::new(),
            state_changes: Vec::new(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RunRecord {
    pub tags: RunTags,
    pub parameters: Parameters,
    pub tasks: HashMap<TaskID, TaskRecord>,
    pub state_changes: Vec<StateChange>,
}

impl RunRecord {
    pub fn new(tags: RunTags, parameters: Parameters) -> Self {
        RunRecord {
            tags,
            parameters,
            tasks: HashMap::new(),
            state_changes: vec![StateChange::new(State::Queued)],
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

impl RunSummary {
    pub fn new(run_id: RunID, tags: RunTags, state: State) -> Self {
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

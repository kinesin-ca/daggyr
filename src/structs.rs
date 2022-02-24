use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

pub type RunID = usize;
pub type TaskID = usize;
pub type Tags = HashSet<String>;
pub type Parameters = HashMap<String, Vec<String>>;

#[derive(Clone, Serialize, Deserialize, Copy, Debug, PartialEq, Hash, Eq)]
pub enum State {
    Queued,
    Running,
    Errored,
    Completed,
    Killed,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct Task {
    pub class: String,

    #[serde(default)]
    pub instance: usize,

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
            class: "".to_owned(),
            instance: 0,
            is_generator: false,
            max_retries: 0,
            retries: 0,
            details: serde_json::Value::Null,
            children: Vec::new(),
            parents: Vec::new(),
        }
    }
}

impl Hash for Task {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.class.hash(state);
        self.instance.hash(state);
    }
}

#[test]
fn test_task_deserialization() {
    let data = r#"
    {
        "class": "simple_task",
        "details": {
            "cmd": "Some parameter here",
            "env": {
                "ENVVAR": 16
            }
        }
    }"#;

    // Parse the string of data into serde_json::Value.
    let task: Task = serde_json::from_str(data).unwrap();
    assert_eq!(task.class, "simple_task");
    assert_eq!(task.instance, 0);
    assert_eq!(task.is_generator, false);
    assert_eq!(task.max_retries, 0);
    assert_eq!(task.retries, 0);
    assert!(task.children.is_empty());
    assert!(task.parents.is_empty());
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TaskAttempt {
    #[serde(default = "chrono::Utc::now")]
    pub start_time: DateTime<Utc>,

    #[serde(default = "chrono::Utc::now")]
    pub stop_time: DateTime<Utc>,

    #[serde(default)]
    pub succeeded: bool,

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
            output: "".to_owned(),
            error: "".to_owned(),
            executor: Vec::new(),
            exit_code: 0i32,
            max_cpu: 0,
            max_rss: 0,
        }
    }
}

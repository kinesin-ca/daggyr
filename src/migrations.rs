use super::Result;

#[derive(Clone, Debug)]
pub struct Migration<'a> {
    pub name: &'a str,
    pub up: &'a str,
    pub down: &'a str,
}

pub static MIGRATIONS: &'static [Migration] = &[
    Migration {
        name: "create runs",
        up: include_str!("../migrations/0000_create_runs/up.sql"),
        down: include_str!("../migrations/0000_create_runs/down.sql"),
    },
    Migration {
        name: "create run tags",
        up: include_str!("../migrations/0001_create_run_tags/up.sql"),
        down: include_str!("../migrations/0001_create_run_tags/down.sql"),
    },
    Migration {
        name: "create task_types",
        up: include_str!("../migrations/0002_create_task_types/up.sql"),
        down: include_str!("../migrations/0002_create_task_types/down.sql"),
    },
    Migration {
        name: "create tasks",
        up: include_str!("../migrations/0003_create_tasks/up.sql"),
        down: include_str!("../migrations/0003_create_tasks/down.sql"),
    },
    Migration {
        name: "create task attempts",
        up: include_str!("../migrations/0004_create_task_attempts/up.sql"),
        down: include_str!("../migrations/0004_create_task_attempts/down.sql"),
    },
    Migration {
        name: "create task expansion values",
        up: include_str!("../migrations/0005_create_task_expansion_values/up.sql"),
        down: include_str!("../migrations/0005_create_task_expansion_values/down.sql"),
    },
    Migration {
        name: "create parameters",
        up: include_str!("../migrations/0006_create_parameters/up.sql"),
        down: include_str!("../migrations/0006_create_parameters/down.sql"),
    },
    Migration {
        name: "create state type",
        up: include_str!("../migrations/0007_create_state_type/up.sql"),
        down: include_str!("../migrations/0007_create_state_type/down.sql"),
    },
    Migration {
        name: "create state changes",
        up: include_str!("../migrations/0008_create_state_changes/up.sql"),
        down: include_str!("../migrations/0008_create_state_changes/down.sql"),
    },
];

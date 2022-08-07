macro_rules! migration {
    ($name:literal, $dir:literal) => {{
        Migration {
            name: $name,
            up: include_str!(concat!("../migrations/", $dir, "/up.sql")),
            down: include_str!(concat!("../migrations/", $dir, "/down.sql")),
        }
    }};
}

#[derive(Clone, Debug)]
pub struct Migration<'a> {
    pub name: &'a str,
    pub up: &'a str,
    pub down: &'a str,
}

pub static MIGRATIONS: &[Migration] = &[
    // migration!("create run tags", "0001_create_run_tags"),
    // migration!( "create task expansion values", "0005_create_task_expansion_values"),
    // migration!("create parameters", "0006_create_parameters"),
    migration!("enable hstore extension", "0000_enable_hstore"),
    migration!("create runs", "0001_create_runs"),
    migration!("create task types", "0002_create_task_types"),
    migration!("create tasks", "0003_create_tasks"),
    migration!("create task attempts", "0004_create_task_attempts"),
    migration!("create state type", "0007_create_state_type"),
    migration!("create state changes", "0008_create_state_changes"),
];

-- Your SQL goes here
create table state_changes (
  id bigserial primary key,
  run_id bigint references runs(id),
  task_id bigint references tasks(id),
  datetime timestamp,
  state state
)

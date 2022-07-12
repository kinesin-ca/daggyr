-- Your SQL goes here
create table parameters (
  id bigserial primary key,
  run_id bigint references runs(id),
  task_id bigint references tasks(id),
  key varchar(255),
  value varchar(255)
)

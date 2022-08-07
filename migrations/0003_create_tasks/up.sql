-- Your SQL goes here
create table tasks (
  id bigserial primary key,
  run_id bigint references runs(id) not null,
  name varchar(255) not null,
  task_type tasktype not null default 'Normal',
  is_generator boolean not null default 'f',
  max_retries smallint not null default 3,
  details JSON not null,
  expansion_values HSTORE,
  UNIQUE (run_id, name)
)

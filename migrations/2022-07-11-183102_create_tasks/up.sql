-- Your SQL goes here
create table tasks (
  id bigserial PRIMARY KEY,
  task_id bigint,
  run_id bigint references runs(id),
  name varchar(255) not null,
  task_type tasktype not null default 'Normal',
  is_generator boolean not null default 'f',
  maxRetries smallint not null default 3,
  retries smallint not null default 0,
  UNIQUE (task_id, run_id)
)

-- Your SQL goes here
create table tasks (
  run_id bigint references runs(id),
  task_id varchar(255),
  task_type tasktype not null default 'Normal',
  is_generator boolean not null default 'f',
  max_retries smallint not null default 3,
  details TEXT,
  PRIMARY KEY (task_id, run_id)
)

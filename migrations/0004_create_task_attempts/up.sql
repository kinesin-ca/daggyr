-- Your SQL goes here
create table task_attempts (
  id bigserial primary key,
  task_id bigint references tasks(id),
  start_time timestamp,
  stop_time timestamp,
  succeeded boolean,
  killed boolean,
  output text,
  error text,
  executor text,
  exit_code int,
  max_cpu float,
  avg_cpu float,
  max_rss int,
  UNIQUE (task_id, start_time)
);

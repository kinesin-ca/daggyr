-- Your SQL goes here
create table task_expansion_values (
  id bigserial primary key,
  task_id bigint references tasks(id),
  variable varchar(255),
  value varchar(255)
)

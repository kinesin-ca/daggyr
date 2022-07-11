-- Your SQL goes here
create table run_tags (
  id bigserial primary key,
  run_id BIGINT REFERENCES runs(id),
  key varchar(255),
  value varchar(255)
)

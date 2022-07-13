-- Your SQL goes here
create table runs (
  id BIGSERIAL PRIMARY KEY,
  tags HSTORE,
  parameters HSTORE
);

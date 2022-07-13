-- Your SQL goes here
create type State AS ENUM ( 'Queued', 'Running', 'Errored', 'Completed', 'Killed')

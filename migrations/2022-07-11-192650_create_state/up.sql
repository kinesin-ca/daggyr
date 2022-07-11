-- Your SQL goes here
create type state AS ENUM ( 'Queued', 'Running', 'Errored', 'Completed', 'Killed')

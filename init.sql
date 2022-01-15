CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TYPE task_status AS ENUM ('created', 'queued', 'in_progress', 'succeeded', 'failed');

CREATE TABLE tasks
(
    id       text primary key     default gen_random_uuid(),
    created  timestamp   not null default now(),
    modified timestamp,
    status   task_status not null,
    task     text        not null,
    data     jsonb       not null
);

CREATE INDEX ON tasks(status);
CREATE INDEX ON tasks(status, task);
CREATE INDEX ON tasks(created);

CREATE OR REPLACE FUNCTION update_modified_column()
    RETURNS TRIGGER AS
$$
BEGIN
    NEW.modified = now();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_task_modtime
    BEFORE UPDATE
    ON tasks
    FOR EACH ROW
EXECUTE PROCEDURE update_modified_column();

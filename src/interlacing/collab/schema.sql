CREATE TABLE IF NOT EXISTS il_projects (
    project text PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS il_members (
    project text REFERENCES il_projects, name text NOT NULL, secret text UNIQUE NOT NULL,
    role text NOT NULL CHECK (role IN ('manager','member')), PRIMARY KEY (project,name)
);
CREATE TABLE IF NOT EXISTS il_runs (
    id uuid PRIMARY KEY, project text NOT NULL REFERENCES il_projects, graph json NOT NULL, inputs json NOT NULL,
    resources json NOT NULL, assignments json NOT NULL, state text NOT NULL DEFAULT 'queued', result json,
    generation bigint NOT NULL DEFAULT 0, worker text, expires timestamptz,
    created timestamptz NOT NULL DEFAULT clock_timestamp()
);
-- Existing runs retain their original serial scheduling on resume.
ALTER TABLE il_runs ADD COLUMN IF NOT EXISTS workers integer NOT NULL DEFAULT 0 CHECK(workers BETWEEN 0 AND 32);
ALTER TABLE il_runs ADD COLUMN IF NOT EXISTS description json;
ALTER TABLE il_runs ADD COLUMN IF NOT EXISTS origin json;
CREATE INDEX IF NOT EXISTS il_runs_project ON il_runs(project,created);
CREATE TABLE IF NOT EXISTS il_commands (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, run uuid NOT NULL REFERENCES il_runs,
    key text NOT NULL, done boolean NOT NULL DEFAULT false, UNIQUE(run,key)
);
CREATE INDEX IF NOT EXISTS il_commands_pending ON il_commands(run,id) WHERE NOT done;
CREATE TABLE IF NOT EXISTS il_events (
    run uuid REFERENCES il_runs, scope jsonb NOT NULL, seq bigint NOT NULL, record json NOT NULL,
    PRIMARY KEY(run,scope,seq)
);
CREATE TABLE IF NOT EXISTS il_checkpoints (
    run uuid REFERENCES il_runs, scope jsonb NOT NULL, seq bigint NOT NULL, state json NOT NULL,
    PRIMARY KEY(run,scope,seq)
);
CREATE TABLE IF NOT EXISTS il_tasks (
    id uuid PRIMARY KEY, run uuid NOT NULL REFERENCES il_runs, node text NOT NULL, turn bigint NOT NULL,
    request json NOT NULL, answer_schema json NOT NULL, state text NOT NULL DEFAULT 'open',
    assignee text, version bigint NOT NULL DEFAULT 1, answer json, submitted_version bigint,
    UNIQUE(run,node,turn)
);
CREATE INDEX IF NOT EXISTS il_tasks_run ON il_tasks(run);
CREATE TABLE IF NOT EXISTS il_resources (
    project text REFERENCES il_projects, namespace text NOT NULL, name text NOT NULL, version bigint NOT NULL,
    data json NOT NULL, PRIMARY KEY(project,namespace,name,version)
);
CREATE TABLE IF NOT EXISTS il_files (
    project text REFERENCES il_projects, digest text NOT NULL, size bigint NOT NULL,
    PRIMARY KEY(project,digest)
);
CREATE TABLE IF NOT EXISTS il_audit (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, project text REFERENCES il_projects,
    actor text NOT NULL, action text NOT NULL, subject text NOT NULL, details json NOT NULL,
    ts timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS il_revisions (
    run uuid REFERENCES il_runs, scope jsonb NOT NULL, delivery text NOT NULL, PRIMARY KEY(run,scope,delivery)
);
CREATE TABLE IF NOT EXISTS il_task_reads (
    task uuid REFERENCES il_tasks, run uuid REFERENCES il_runs, scope jsonb NOT NULL, delivery text NOT NULL,
    PRIMARY KEY(run,scope,delivery,task)
);

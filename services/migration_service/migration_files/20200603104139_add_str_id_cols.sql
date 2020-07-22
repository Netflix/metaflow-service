-- +goose Up
-- +goose StatementBegin
SELECT 'up SQL query';
ALTER TABLE runs_v3
ADD COLUMN run_id VARCHAR(255);

ALTER TABLE runs_v3
ADD COLUMN last_heartbeat_ts BIGINT;

ALTER TABLE runs_v3
ADD CONSTRAINT runs_v3_flow_id_run_id_key UNIQUE (flow_id, run_id);

ALTER TABLE steps_v3
ADD COLUMN run_id VARCHAR(255);

ALTER TABLE steps_v3
ADD CONSTRAINT steps_v3_flow_id_run_id_step_name_key UNIQUE (flow_id, run_id, step_name);

ALTER TABLE tasks_v3
ADD COLUMN run_id VARCHAR(255);

ALTER TABLE tasks_v3
ADD COLUMN task_name VARCHAR(255);

ALTER TABLE tasks_v3
ADD COLUMN last_heartbeat_ts BIGINT;

ALTER TABLE tasks_v3
ADD CONSTRAINT tasks_v3_flow_id_run_number_step_name_task_name_key UNIQUE (flow_id, run_number, step_name, task_name);

ALTER TABLE metadata_v3
ADD COLUMN run_id VARCHAR(255);

ALTER TABLE metadata_v3
ADD COLUMN task_name VARCHAR(255);

ALTER TABLE artifact_v3
ADD COLUMN run_id VARCHAR(255);

ALTER TABLE artifact_v3
ADD COLUMN task_name VARCHAR(255);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
SELECT 'down SQL query';
ALTER TABLE artifact_v3
DROP COLUMN task_name;

ALTER TABLE artifact_v3
DROP COLUMN run_id;

ALTER TABLE metadata_v3
DROP COLUMN run_id;

ALTER TABLE metadata_v3
DROP COLUMN task_name;

ALTER TABLE tasks_v3
DROP CONSTRAINT tasks_v3_flow_id_run_number_step_name_task_name_key;

ALTER TABLE tasks_v3
DROP COLUMN run_id;

ALTER TABLE tasks_v3
DROP COLUMN task_name;

ALTER TABLE tasks_v3
DROP COLUMN last_heartbeat_ts;

ALTER TABLE steps_v3
DROP CONSTRAINT steps_v3_flow_id_run_id_step_name_key;

ALTER TABLE steps_v3
DROP COLUMN run_id;

ALTER TABLE runs_v3
DROP CONSTRAINT runs_v3_flow_id_run_id_key;

ALTER TABLE runs_v3
DROP COLUMN last_heartbeat_ts;

ALTER TABLE runs_v3
DROP COLUMN run_id;

-- +goose StatementEnd

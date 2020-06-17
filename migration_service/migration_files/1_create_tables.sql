-- +goose Up
-- +goose StatementBegin
SELECT 'up SQL query';
CREATE TABLE flows_v3 (
  flow_id VARCHAR(255) PRIMARY KEY,
  user_name VARCHAR(255),
  ts_epoch BIGINT NOT NULL,
  tags JSONB,
  system_tags JSONB
);

CREATE TABLE runs_v3 (
  flow_id VARCHAR(255) NOT NULL,
  run_number SERIAL NOT NULL,
  user_name VARCHAR(255),
  ts_epoch BIGINT NOT NULL,
  tags JSONB,
  system_tags JSONB,
  PRIMARY KEY(flow_id, run_number),
  FOREIGN KEY(flow_id) REFERENCES flows_v3 (flow_id)
);

CREATE TABLE steps_v3 (
    flow_id VARCHAR(255) NOT NULL,
    run_number BIGINT NOT NULL,
    step_name VARCHAR(255) NOT NULL,
    user_name VARCHAR(255),
    ts_epoch BIGINT NOT NULL,
    tags JSONB,
    system_tags JSONB,
    PRIMARY KEY(flow_id, run_number, step_name),
    FOREIGN KEY(flow_id, run_number) REFERENCES runs_v3 (flow_id, run_number)
);


CREATE TABLE tasks_v3 (
    flow_id VARCHAR(255) NOT NULL,
    run_number BIGINT NOT NULL,
    step_name VARCHAR(255) NOT NULL,
    task_id BIGSERIAL PRIMARY KEY,
    user_name VARCHAR(255),
    ts_epoch BIGINT NOT NULL,
    tags JSONB,
    system_tags JSONB,
    FOREIGN KEY(flow_id, run_number, step_name) REFERENCES steps_v3 (flow_id, run_number, step_name)
);

CREATE TABLE metadata_v3 (
    flow_id VARCHAR(255),
    run_number BIGINT NOT NULL,
    step_name VARCHAR(255) NOT NULL,
    task_id BIGINT NOT NULL,
    id BIGSERIAL NOT NULL,
    field_name VARCHAR(255) NOT NULL,
    value TEXT NOT NULL,
    type VARCHAR(255) NOT NULL,
    user_name VARCHAR(255),
    ts_epoch BIGINT NOT NULL,
    tags JSONB,
    system_tags JSONB,
    PRIMARY KEY(flow_id, run_number, step_name, task_id, field_name)
);

CREATE TABLE artifact_v3 (
    flow_id VARCHAR(255) NOT NULL,
    run_number BIGINT NOT NULL,
    step_name VARCHAR(255) NOT NULL,
    task_id BIGINT NOT NULL,
    name VARCHAR(255) NOT NULL,
    location VARCHAR(255) NOT NULL,
    ds_type VARCHAR(255) NOT NULL,
    sha VARCHAR(255),
    type VARCHAR(255),
    content_type VARCHAR(255),
    user_name VARCHAR(255),
    attempt_id SMALLINT NOT NULL,
    ts_epoch BIGINT NOT NULL,
    tags JSONB,
    system_tags JSONB,
    PRIMARY KEY(flow_id, run_number, step_name, task_id, attempt_id, name)
);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
SELECT 'down SQL query';

-- +goose StatementEnd

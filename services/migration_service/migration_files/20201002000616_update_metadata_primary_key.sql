-- +goose Up
-- +goose StatementBegin
SELECT 'up SQL query';

ALTER TABLE metadata_v3
ADD CONSTRAINT metadata_v3_primary_key UNIQUE (id,flow_id, run_number, step_name, task_id, field_name);

CREATE INDEX metadata_v3_akey ON metadata_v3(flow_id, run_number, step_name, task_id, field_name);

ALTER TABLE metadata_v3
DROP CONSTRAINT metadata_v3_pkey;

ALTER TABLE metadata_v3
ADD PRIMARY KEY (id,flow_id, run_number, step_name, task_id, field_name);

ALTER TABLE metadata_v3
DROP CONSTRAINT metadata_v3_primary_key;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
SELECT 'down SQL query';

-- create index that will become the primary key
ALTER TABLE metadata_v3
ADD CONSTRAINT metadata_v3_primary_key UNIQUE (flow_id, run_number, step_name, task_id, field_name);

-- drop index created for optimized access
ALTER TABLE metadata_v3
DROP metadata_v3 metadata_v3_akey;

-- drop primary key
ALTER TABLE metadata_v3
DROP CONSTRAINT metadata_v3_pkey;

-- set index as primary key
ALTER TABLE metadata_v3
ADD PRIMARY KEY (flow_id, run_number, step_name, task_id, field_name);

-- drop index
ALTER TABLE metadata_v3
DROP CONSTRAINT metadata_v3_primary_key;
-- +goose StatementEnd

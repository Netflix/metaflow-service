-- +goose NO TRANSACTION
-- +goose Up
-- runs idx on flow_id, run_id
CREATE INDEX CONCURRENTLY IF NOT EXISTS runs_v3_idx_str_ids_primary_key ON runs_v3 (flow_id, run_id)
WHERE
  run_id IS NOT NULL;

-- steps idx on flow_id, run_id
CREATE INDEX CONCURRENTLY IF NOT EXISTS steps_v3_idx_str_ids_primary_key ON steps_v3 (flow_id, run_id, step_name)
WHERE
  run_id IS NOT NULL;

-- metadata idx on id, flow_id, run_id, step_name and task_name, field_name
CREATE INDEX CONCURRENTLY IF NOT EXISTS metadata_v3_idx_str_ids_primary_key ON metadata_v3 (
  id,
  flow_id,
  run_id,
  step_name,
  task_name,
  field_name
)
WHERE
  run_id IS NOT NULL
  AND task_name IS NOT NULL;

-- artifact idx on flow_id, run_id, step_name and task_name, attempt_id, name
CREATE INDEX CONCURRENTLY IF NOT EXISTS artifact_v3_idx_str_ids_primary_key ON artifact_v3 (
  flow_id,
  run_id,
  step_name,
  task_name,
  attempt_id,
  name
)
WHERE
  run_id IS NOT NULL
  AND task_name IS NOT NULL;

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS runs_v3_idx_str_ids_primary_key;

DROP INDEX IF EXISTS steps_v3_idx_str_ids_primary_key;

DROP INDEX IF EXISTS metadata_v3_idx_str_ids_primary_key;

DROP INDEX IF EXISTS artifact_v3_idx_str_ids_primary_key;

-- +goose StatementEnd
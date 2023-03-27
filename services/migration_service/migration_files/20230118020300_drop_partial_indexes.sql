-- +goose NO TRANSACTION
-- +goose Up

-- Drop partial str_ids indexes created with 20211202100726_add_str_id_indices.sql
-- and recreate them without the constraining WHERE clause.
-- This is being done as the psql query planner is not using these indexes many times.
-- To avoid perf downtime we first create the new indexes and then drop the old ones.
CREATE INDEX CONCURRENTLY IF NOT EXISTS runs_v3_idx_str_ids_primary_key_v2
  ON runs_v3 (flow_id, run_id);

DROP INDEX CONCURRENTLY IF EXISTS runs_v3_idx_str_ids_primary_key;

CREATE INDEX CONCURRENTLY IF NOT EXISTS steps_v3_idx_str_ids_primary_key_v2
  ON steps_v3 (flow_id, run_id, step_name);

DROP INDEX CONCURRENTLY IF EXISTS steps_v3_idx_str_ids_primary_key;

CREATE INDEX CONCURRENTLY IF NOT EXISTS tasks_v3_idx_flow_id_run_id_step_name_task_name
  ON tasks_v3(flow_id, run_id, step_name, task_name);

DROP INDEX CONCURRENTLY IF EXISTS tasks_v3_idx_flow_id_run_id_step_name_task_name;

CREATE INDEX CONCURRENTLY IF NOT EXISTS metadata_v3_idx_str_ids_a_key
  ON metadata_v3 (
    flow_id,
    run_id,
    step_name,
    task_name,
    field_name
  );

CREATE INDEX CONCURRENTLY IF NOT EXISTS metadata_v3_idx_str_ids_a_key_with_task_id
  ON metadata_v3 (
    flow_id,
    run_id,
    step_name,
    task_id,
    field_name
  );

DROP INDEX CONCURRENTLY IF EXISTS metadata_v3_idx_str_ids_primary_key;

CREATE INDEX CONCURRENTLY IF NOT EXISTS artifact_v3_idx_str_ids_primary_key_v2 ON artifact_v3 (
  flow_id,
  run_id,
  step_name,
  task_name,
  attempt_id,
  name
);

CREATE INDEX CONCURRENTLY IF NOT EXISTS artifact_v3_idx_str_ids_primary_key_with_task_id ON artifact_v3 (
  flow_id,
  run_id,
  step_name,
  task_id,
  attempt_id,
  name
);

DROP INDEX CONCURRENTLY IF EXISTS artifact_v3_idx_str_ids_primary_key;


-- +goose Down

-- copy of 20211202100726_add_str_id_indices.sql
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

DROP INDEX CONCURRENTLY IF EXISTS runs_v3_idx_str_ids_primary_key_v2;
DROP INDEX CONCURRENTLY IF EXISTS steps_v3_idx_str_ids_primary_key_v2;
DROP INDEX CONCURRENTLY IF EXISTS metadata_v3_idx_str_ids_a_key;
DROP INDEX CONCURRENTLY IF EXISTS metadata_v3_idx_str_ids_a_key_with_task_id;
DROP INDEX CONCURRENTLY IF EXISTS artifact_v3_idx_str_ids_primary_key_v2;
DROP INDEX CONCURRENTLY IF EXISTS artifact_v3_idx_str_ids_primary_key_with_task_id;

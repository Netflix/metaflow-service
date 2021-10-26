-- +goose NO TRANSACTION
-- +goose Up
-- +goose StatementBegin

-- tasks on flow_id, run_id, step_name and task_name
CREATE INDEX CONCURRENTLY IF NOT EXISTS tasks_v3_idx_flow_id_run_id_step_name_task_name ON tasks_v3 (
    flow_id, run_id, step_name, task_name) WHERE run_id IS NOT NULL AND task_name IS NOT NULL;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS tasks_v3_idx_flow_id_run_id_step_name_task_name;

-- +goose StatementEnd

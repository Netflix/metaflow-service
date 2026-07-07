-- +goose NO TRANSACTION
-- +goose Up
-- +goose StatementBegin
-- Cursor pagination on get_tasks filters by flow_id, run_number, step_name and
-- orders by ts_epoch, task_id. This index covers the filter and the ordering.
CREATE INDEX CONCURRENTLY IF NOT EXISTS tasks_v3_idx_flow_run_step_ts_task_desc ON tasks_v3 (flow_id, run_number, step_name, ts_epoch DESC, task_id DESC);
-- +goose StatementEnd
-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS tasks_v3_idx_flow_run_step_ts_task_desc;
-- +goose StatementEnd
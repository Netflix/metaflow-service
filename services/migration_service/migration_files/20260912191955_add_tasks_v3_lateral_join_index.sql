-- +goose NO TRANSACTION
-- +goose Up
-- +goose StatementBegin
-- Index for lateral join queries from steps_v3 to tasks_v3 on (flow_id, run_number, step_name).
-- This supports joins used in step queries with enable_joins=True, including step API endpoints
-- and heartbeat monitoring. The existing index uses run_id (string) instead of run_number (integer).
CREATE INDEX CONCURRENTLY IF NOT EXISTS tasks_v3_idx_flow_run_number_step ON tasks_v3 (flow_id, run_number, step_name);
-- +goose StatementEnd
-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS tasks_v3_idx_flow_run_number_step;
-- +goose StatementEnd

-- +goose NO TRANSACTION
-- +goose Up
-- +goose StatementBegin
-- Cursor pagination on get_steps filters by flow_id, run_number and orders by
-- ts_epoch, step_name. This index covers the filter and the ordering.
CREATE INDEX CONCURRENTLY IF NOT EXISTS steps_v3_idx_flow_run_ts_step_desc ON steps_v3 (flow_id, run_number, ts_epoch DESC, step_name DESC);
-- +goose StatementEnd
-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS steps_v3_idx_flow_run_ts_step_desc;
-- +goose StatementEnd

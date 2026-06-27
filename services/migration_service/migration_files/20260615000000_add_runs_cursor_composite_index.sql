-- +goose NO TRANSACTION
-- +goose Up
-- +goose StatementBegin
-- Cursor pagination on runs orders by ts_epoch and run_number within a flow.
-- This index lets those paged queries scan in order instead of sorting the whole flow.
CREATE INDEX CONCURRENTLY IF NOT EXISTS runs_v3_idx_flow_ts_runnum_desc ON runs_v3 (flow_id, ts_epoch DESC, run_number DESC);
-- +goose StatementEnd
-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS runs_v3_idx_flow_ts_runnum_desc;
-- +goose StatementEnd

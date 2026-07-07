-- +goose NO TRANSACTION
-- +goose Up
-- +goose StatementBegin
-- Cursor pagination on get_all_flows orders by ts_epoch, flow_id.
-- No filter (lists all flows), so the index only needs the cursor ordering.
CREATE INDEX CONCURRENTLY IF NOT EXISTS flows_v3_idx_ts_flow_desc ON flows_v3 (ts_epoch DESC, flow_id DESC);
-- +goose StatementEnd
-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS flows_v3_idx_ts_flow_desc;
-- +goose StatementEnd
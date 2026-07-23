-- +goose NO TRANSACTION
-- +goose Up
-- +goose StatementBegin
-- Cursor pagination on get_metadata filters by flow_id, run_number and orders by
-- ts_epoch, id. This index covers the filter and the ordering.
CREATE INDEX CONCURRENTLY IF NOT EXISTS metadata_v3_idx_flow_run_ts_id_desc ON metadata_v3 (flow_id, run_number, ts_epoch DESC, id DESC);
-- +goose StatementEnd
-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS metadata_v3_idx_flow_run_ts_id_desc;
-- +goose StatementEnd

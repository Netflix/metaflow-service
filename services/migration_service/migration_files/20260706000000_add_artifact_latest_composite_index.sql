-- +goose NO TRANSACTION
-- +goose Up
-- +goose StatementBegin
-- Composite index for artifact latest-attempt filter (DISTINCT ON) + pagination.
-- step_name omitted intentionally (breaks by-run).
CREATE INDEX CONCURRENTLY IF NOT EXISTS artifact_v3_idx_latest_attempt ON artifact_v3 (flow_id, run_number, task_id, name, ts_epoch DESC);
-- +goose StatementEnd
-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS artifact_v3_idx_latest_attempt;
-- +goose StatementEnd

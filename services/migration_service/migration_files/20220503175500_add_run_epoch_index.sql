-- +goose NO TRANSACTION
-- +goose Up
-- +goose StatementBegin

-- UI requests recent runs a lot, this index helps make those queries go faster.
-- (it seems to help it push down LIMITs even if there aren't too many runs in the db)
CREATE INDEX CONCURRENTLY IF NOT EXISTS runs_v3_idx_epoch_ts_desc ON runs_v3 (ts_epoch DESC);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

DROP INDEX IF EXISTS runs_v3_idx_epoch_ts_desc;

-- +goose StatementEnd
-- +goose Up
-- +goose StatementBegin
SELECT
  'up SQL query';

CREATE INDEX tasks_v3_idx_ts_epoch ON tasks_v3 (ts_epoch);

-- +goose StatementEnd
-- +goose Down
-- +goose StatementBegin
SELECT
  'down SQL query';

DROP INDEX tasks_v3_idx_ts_epoch;

-- +goose StatementEnd
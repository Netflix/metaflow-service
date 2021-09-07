-- +goose Up
-- +goose StatementBegin
SELECT
  'up SQL query';

CREATE INDEX IF NOT EXISTS metadata_v3_idx_run_join_end_task_attempt_ok ON metadata_v3 (flow_id, run_number, step_name, ts_epoch DESC)
WHERE
  field_name = 'attempt_ok'
  AND step_name = 'end';

-- +goose StatementEnd
-- +goose Down
-- +goose StatementBegin
SELECT
  'down SQL query';

DROP INDEX IF EXISTS metadata_v3_idx_run_join_end_task_attempt_ok;

-- +goose StatementEnd
-- +goose Up
-- +goose StatementBegin
CREATE INDEX IF NOT EXISTS metadata_v3_idx_str_ids_a_key_with_run_number_task_id
    ON metadata_v3 (flow_id, run_number, step_name, task_id, field_name)
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS metadata_v3_idx_str_ids_a_key_with_run_number_task_id;
-- +goose StatementEnd

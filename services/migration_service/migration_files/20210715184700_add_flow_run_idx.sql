-- +goose Up
-- +goose StatementBegin

CREATE INDEX IF NOT EXISTS tasks_v3_run_id_flow_id_idx ON tasks_v3 USING btree (flow_id, run_id, step_name, task_name);
CREATE INDEX IF NOT EXISTS artifact_v3_run_id_flow_id_idx ON artifact_v3 USING btree (flow_id, run_id, step_name, task_id, attempt_id, name);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

DROP INDEX IF EXISTS tasks_v3_run_id_flow_id_idx;
DROP INDEX IF EXISTS artifact_v3_run_id_flow_id_idx;

-- +goose StatementEnd

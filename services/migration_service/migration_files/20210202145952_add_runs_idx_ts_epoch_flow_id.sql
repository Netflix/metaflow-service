-- +goose Up
-- +goose StatementBegin
SELECT 'up SQL query';

-- Others

CREATE INDEX IF NOT EXISTS runs_v3_idx_ts_epoch ON runs_v3 (ts_epoch);

CREATE INDEX IF NOT EXISTS runs_v3_idx_gin_tags_combined ON runs_v3 USING gin ((tags || system_tags));

-- flow_id + ts_epoch

CREATE INDEX IF NOT EXISTS runs_v3_idx_flow_id_asc_ts_epoch_desc ON runs_v3 (flow_id ASC, ts_epoch DESC);

-- user && ts_epoch

CREATE INDEX IF NOT EXISTS runs_v3_idx_user_asc_ts_epoch_desc ON runs_v3 (
    (CASE
        WHEN system_tags ? ('user:' || user_name)
        THEN user_name
        ELSE NULL
    END) ASC, ts_epoch DESC
);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
SELECT 'down SQL query';

DROP INDEX IF EXISTS runs_v3_idx_user_asc_ts_epoch_desc;

DROP INDEX IF EXISTS runs_v3_idx_flow_id_asc_ts_epoch_desc;

DROP INDEX IF EXISTS runs_v3_idx_gin_tags_combined;

DROP INDEX IF EXISTS runs_v3_idx_ts_epoch;


-- +goose StatementEnd

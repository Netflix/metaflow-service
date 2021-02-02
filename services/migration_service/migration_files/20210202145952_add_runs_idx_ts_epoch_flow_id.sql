-- +goose Up
-- +goose StatementBegin
SELECT 'up SQL query';

-- Others

CREATE INDEX runs_v3_idx_ts_epoch ON runs_v3 (ts_epoch);

CREATE INDEX runs_v3_idx_gin_tags ON runs_v3 USING gin (tags, system_tags);
CREATE INDEX runs_v3_idx_gin_tags_combined ON runs_v3 USING gin ((tags || system_tags));

-- flow_id + ts_epoch

CREATE INDEX runs_v3_idx_flow_id_asc_ts_epoch_desc ON runs_v3 (flow_id ASC, ts_epoch DESC);
CREATE INDEX runs_v3_idx_flow_id_desc_ts_epoch_desc ON runs_v3 (flow_id DESC, ts_epoch DESC);

-- user && ts_epoch

CREATE INDEX runs_v3_idx_user_asc_ts_epoch_desc ON runs_v3 (
    (CASE
        WHEN system_tags ? ('user:' || user_name)
        THEN user_name
        ELSE NULL
    END) ASC NULLS LAST, ts_epoch DESC
);
CREATE INDEX runs_v3_idx_user_desc_ts_epoch_desc ON runs_v3 (
    (CASE
        WHEN system_tags ? ('user:' || user_name)
        THEN user_name
        ELSE NULL
    END) DESC NULLS LAST, ts_epoch DESC
);

-- run

CREATE INDEX runs_v3_idx_run_asc ON runs_v3 (COALESCE(run_id, run_number::text) ASC);
CREATE INDEX runs_v3_idx_run_desc ON runs_v3 (COALESCE(run_id, run_number::text) DESC);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
SELECT 'down SQL query';

DROP INDEX runs_v3_idx_run_desc;
DROP INDEX runs_v3_idx_run_asc;

DROP INDEX runs_v3_idx_user_desc_ts_epoch_desc;
DROP INDEX runs_v3_idx_user_asc_ts_epoch_desc;

DROP INDEX runs_v3_idx_flow_id_desc_ts_epoch_desc;
DROP INDEX runs_v3_idx_flow_id_asc_ts_epoch_desc;

DROP INDEX runs_v3_idx_gin_tags_combined;
DROP INDEX runs_v3_idx_gin_tags;

DROP INDEX runs_v3_idx_ts_epoch;


-- +goose StatementEnd

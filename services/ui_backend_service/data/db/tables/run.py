from .base import AsyncPostgresTable, HEARTBEAT_THRESHOLD, OLD_RUN_FAILURE_CUTOFF_TIME, WAIT_TIME, SCHEDULER_DELAY
from .flow import AsyncFlowTablePostgres
from ..models import RunRow
# use schema constants from the .data module to keep things consistent
from services.data.postgres_async_db import AsyncRunTablePostgres as MetadataRunTable


class AsyncRunTablePostgres(AsyncPostgresTable):
    _row_type = RunRow
    table_name = MetadataRunTable.table_name
    keys = MetadataRunTable.keys
    primary_keys = MetadataRunTable.primary_keys
    trigger_keys = MetadataRunTable.trigger_keys
    joins = [
        """
        LEFT JOIN LATERAL (
            SELECT
                ts_epoch,
                (CASE
                    WHEN {artifact_table}.step_name = 'end'
                    THEN TRUE
                    ELSE FALSE
                END) as is_end_step
            FROM {artifact_table}
            WHERE {artifact_table}.flow_id = {table_name}.flow_id
                AND {artifact_table}.run_number = {table_name}.run_number
                AND {artifact_table}.name = '_task_ok'
            ORDER BY
                "ts_epoch" DESC
            LIMIT 1
        ) as latest_artifact ON true
        """.format(
            table_name=table_name,
            artifact_table="artifact_v3"
        ),
        """
        LEFT JOIN LATERAL (
            SELECT
                {metadata_table}.value as value,
                (CASE
                 WHEN {metadata_table}.field_name = 'attempt_ok'
                 THEN TRUE
                 ELSE FALSE
                END) as is_finished,
                {metadata_table}.ts_epoch as ts_epoch
            FROM {metadata_table}
            WHERE {metadata_table}.flow_id = {table_name}.flow_id
                AND {metadata_table}.run_number = {table_name}.run_number
                AND ({metadata_table}.field_name = 'attempt_ok' OR {metadata_table}.field_name = 'attempt')
            ORDER BY
                "ts_epoch" DESC
            LIMIT 1
        ) as latest_attempt_meta ON true
        """.format(
            table_name=table_name,
            metadata_table="metadata_v3"
        )
    ]
    # User should be considered NULL when 'user:*' tag is missing
    # This is usually the case with AWS Step Functions
    select_columns = ["runs_v3.{0} AS {0}".format(k) for k in keys] \
        + ["""
            (CASE
                WHEN system_tags ? ('user:' || user_name)
                THEN user_name
                ELSE NULL
            END) AS user"""] \
        + ["""
            COALESCE({table_name}.run_id, {table_name}.run_number::text) AS run
            """.format(table_name=table_name)]
    join_columns = [
        """
        (CASE
            WHEN latest_artifact IS NOT NULL
            AND latest_artifact.is_end_step IS TRUE
            THEN latest_artifact.ts_epoch
            WHEN {table_name}.last_heartbeat_ts IS NOT NULL
            AND @(extract(epoch from now())-{table_name}.last_heartbeat_ts)>{heartbeat_threshold}
            THEN {table_name}.last_heartbeat_ts*1000
            WHEN {table_name}.last_heartbeat_ts IS NULL
            AND @(extract(epoch from now())*1000-GREATEST({table_name}.ts_epoch, latest_artifact.ts_epoch))>{cutoff}
            THEN GREATEST(latest_artifact.ts_epoch, {table_name}.ts_epoch + {cutoff})
            ELSE NULL
        END) AS finished_at
        """.format(
            table_name=table_name,
            heartbeat_threshold=HEARTBEAT_THRESHOLD,
            cutoff=OLD_RUN_FAILURE_CUTOFF_TIME
        ),
        """
        (CASE
            WHEN latest_attempt_meta.is_finished
                AND latest_attempt_meta.value::boolean IS FALSE
                AND @(extract(epoch from now())*1000 - latest_attempt_meta.ts_epoch) > {scheduler_delay}
            THEN 'failed'
            WHEN latest_artifact.is_end_step IS TRUE
                AND latest_attempt_meta.is_finished
                AND latest_attempt_meta.value::boolean IS TRUE
            THEN 'completed'
            WHEN latest_artifact.is_end_step IS TRUE
                AND latest_attempt_meta IS NULL
            THEN 'completed'
            WHEN {table_name}.last_heartbeat_ts IS NOT NULL
            AND @(extract(epoch from now())-{table_name}.last_heartbeat_ts)>{heartbeat_threshold}
            THEN 'failed'
            WHEN {table_name}.last_heartbeat_ts IS NULL
            AND @(extract(epoch from now())*1000-GREATEST({table_name}.ts_epoch, latest_artifact.ts_epoch))>{cutoff}
            THEN 'failed'
            ELSE 'running'
        END) AS status
        """.format(
            table_name=table_name,
            heartbeat_threshold=HEARTBEAT_THRESHOLD,
            cutoff=OLD_RUN_FAILURE_CUTOFF_TIME,
            scheduler_delay=SCHEDULER_DELAY
        ),
        """
        COALESCE(
            latest_artifact.ts_epoch,
            {table_name}.last_heartbeat_ts*1000,
            @(extract(epoch from now())::bigint*1000)
        ) - {table_name}.ts_epoch AS duration
        """.format(
            table_name=table_name
        )
    ]
    flow_table_name = AsyncFlowTablePostgres.table_name
    _command = MetadataRunTable._command

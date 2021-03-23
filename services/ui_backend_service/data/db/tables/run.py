import os
import time
from .base import AsyncPostgresTable, HEARTBEAT_THRESHOLD, OLD_RUN_FAILURE_CUTOFF_TIME, WAIT_TIME
from .flow import AsyncFlowTablePostgres
from ..models import RunRow
from services.data.db_utils import DBResponse, translate_run_key
# use schema constants from the .data module to keep things consistent
from services.data.postgres_async_db import (
    AsyncRunTablePostgres as MetadataRunTable,
    AsyncMetadataTablePostgres as MetaMetadataTable,
    AsyncArtifactTablePostgres as MetadataArtifactTable
)

# Prefetch runs since 2 days ago (in seconds), limit maximum of 50 runs
METAFLOW_ARTIFACT_PREFETCH_RUNS_SINCE = os.environ.get('PREFETCH_RUNS_SINCE', 86400 * 2)
METAFLOW_ARTIFACT_PREFETCH_RUNS_LIMIT = os.environ.get('PREFETCH_RUNS_LIMIT', 50)


class AsyncRunTablePostgres(AsyncPostgresTable):
    _row_type = RunRow
    table_name = MetadataRunTable.table_name
    metadata_table = MetaMetadataTable.table_name
    artifact_table = MetadataArtifactTable.table_name
    keys = MetadataRunTable.keys
    primary_keys = MetadataRunTable.primary_keys
    trigger_keys = MetadataRunTable.trigger_keys
    joins = [
        """
        LEFT JOIN (
            SELECT
                artifacts.flow_id, artifacts.run_number, artifacts.step_name,
                artifacts.task_id, artifacts.attempt_id, artifacts.ts_epoch,
                attempt_ok.value::boolean as attempt_ok
            FROM {artifact_table} as artifacts
            LEFT JOIN {metadata_table} as attempt_ok ON (
                artifacts.flow_id = attempt_ok.flow_id AND
                artifacts.run_number = attempt_ok.run_number AND
                artifacts.task_id = attempt_ok.task_id AND
                artifacts.step_name = attempt_ok.step_name AND
                attempt_ok.field_name = 'attempt_ok' AND
                attempt_ok.tags ? ('attempt_id:' || artifacts.attempt_id)
            )
            WHERE artifacts.name = '_task_ok' AND artifacts.step_name = 'end'
        ) AS artifacts ON (
            {table_name}.flow_id = artifacts.flow_id AND
            {table_name}.run_number = artifacts.run_number
        )
        """.format(
            table_name=table_name,
            metadata_table=metadata_table,
            artifact_table=artifact_table
        ),
    ]

    @property
    def select_columns(self):
        "We must use a function scope in order to be able to access the table_name variable for list comprehension."
        # User should be considered NULL when 'user:*' tag is missing
        # This is usually the case with AWS Step Functions
        return ["{table_name}.{col} AS {col}".format(table_name=self.table_name, col=k) for k in self.keys] \
            + ["""
                (CASE
                    WHEN system_tags ? ('user:' || user_name)
                    THEN user_name
                    ELSE NULL
                END) AS user"""] \
            + ["""
                COALESCE({table_name}.run_id, {table_name}.run_number::text) AS run
                """.format(table_name=self.table_name)]

    join_columns = [
        """
        (CASE
            WHEN artifacts.ts_epoch IS NOT NULL
            THEN artifacts.ts_epoch
            WHEN {table_name}.last_heartbeat_ts IS NOT NULL
            AND @(extract(epoch from now())-{table_name}.last_heartbeat_ts)>{heartbeat_threshold}
            THEN {table_name}.last_heartbeat_ts*1000
            WHEN {table_name}.last_heartbeat_ts IS NULL
            AND @(extract(epoch from now())*1000-{table_name}.ts_epoch)>{cutoff}
            THEN {table_name}.ts_epoch + {cutoff}
            ELSE NULL
        END) AS finished_at
        """.format(
            table_name=table_name,
            heartbeat_threshold=HEARTBEAT_THRESHOLD,
            cutoff=OLD_RUN_FAILURE_CUTOFF_TIME
        ),
        """
        (CASE
            WHEN artifacts.attempt_ok IS TRUE
            THEN 'completed'
            WHEN artifacts.attempt_ok IS FALSE
            THEN 'failed'
            WHEN artifacts.ts_epoch IS NOT NULL
            THEN 'completed'
            WHEN {table_name}.last_heartbeat_ts IS NOT NULL
            AND @(extract(epoch from now())-{table_name}.last_heartbeat_ts)>{heartbeat_threshold}
            THEN 'failed'
            WHEN {table_name}.last_heartbeat_ts IS NULL
            AND @(extract(epoch from now())*1000-{table_name}.ts_epoch)>{cutoff}
            THEN 'failed'
            ELSE 'running'
        END) AS status
        """.format(
            table_name=table_name,
            heartbeat_threshold=HEARTBEAT_THRESHOLD,
            cutoff=OLD_RUN_FAILURE_CUTOFF_TIME
        ),
        """
        (CASE
            WHEN artifacts.ts_epoch IS NULL AND {table_name}.last_heartbeat_ts IS NOT NULL
            THEN {table_name}.last_heartbeat_ts*1000-{table_name}.ts_epoch
            WHEN artifacts.ts_epoch IS NOT NULL
            THEN artifacts.ts_epoch - {table_name}.ts_epoch
            WHEN {table_name}.last_heartbeat_ts IS NULL
            AND @(extract(epoch from now())::bigint*1000-{table_name}.ts_epoch)>{cutoff}
            THEN {cutoff}
            ELSE @(extract(epoch from now())::bigint*1000-{table_name}.ts_epoch)
        END) AS duration
        """.format(
            table_name=table_name,
            heartbeat_threshold=HEARTBEAT_THRESHOLD,
            cutoff=OLD_RUN_FAILURE_CUTOFF_TIME
        )
    ]
    _command = MetadataRunTable._command

    async def get_recent_run_numbers(self):
        _records, *_ = await self.find_records(
            conditions=["ts_epoch >= %s"],
            values=[int(round(time.time() * 1000)) - (int(METAFLOW_ARTIFACT_PREFETCH_RUNS_SINCE) * 1000)],
            order=['ts_epoch DESC'],
            limit=METAFLOW_ARTIFACT_PREFETCH_RUNS_LIMIT,
            expanded=True
        )

        return [run['run_number'] for run in _records.body]

    async def get_expanded_run(self, run_key: str) -> DBResponse:
        "Fetch run with a given id or number from the DB"
        # Remember to enable_joins for the query, otherwise the 'status' will be missing from the run
        # and we can not broadcast an up-to-date status.
        # NOTE: task being broadcast should contain the same fields as the GET request returns so UI can easily infer changes.
        # Currently this restricts the use of expanded=True
        run_id_key, run_id_value = translate_run_key(run_key)
        result, *_ = await self.find_records(
            conditions=["{column} = %s".format(column=run_id_key)],
            values=[run_id_value],
            fetch_single=True,
            enable_joins=True,
            expanded=True
        )
        return result

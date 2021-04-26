import os
import time
from typing import List
from .base import AsyncPostgresTable, HEARTBEAT_THRESHOLD, OLD_RUN_FAILURE_CUTOFF_TIME, WAIT_TIME
from .flow import AsyncFlowTablePostgres
from ..models import RunRow
from services.data.db_utils import DBResponse, DBPagination, translate_run_key
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
            metadata_table=metadata_table,
            artifact_table=artifact_table
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

    @property
    def select_columns(self):
        # NOTE: We must use a function scope in order to be able to access the table_name variable for list comprehension.
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
            WHEN latest_attempt_meta.is_finished
                AND latest_attempt_meta.value::boolean IS FALSE
                AND @(extract(epoch from now())*1000 - latest_attempt_meta.ts_epoch) > {cutoff}
            THEN latest_artifact.ts_epoch
            WHEN latest_artifact.is_end_step IS TRUE
                AND latest_attempt_meta.is_finished
                AND latest_attempt_meta.value::boolean IS TRUE
            THEN latest_artifact.ts_epoch
            WHEN latest_artifact.is_end_step IS TRUE
                AND (
                    latest_attempt_meta IS NULL OR
                    (
                        latest_attempt_meta.is_finished IS FALSE
                        AND @(extract(epoch from now())*1000 - latest_attempt_meta.ts_epoch) > {cutoff}
                    )
                )
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
                AND @(extract(epoch from now())*1000 - latest_attempt_meta.ts_epoch) > {cutoff}
            THEN 'failed'
            WHEN latest_artifact.is_end_step IS TRUE
                AND latest_attempt_meta.is_finished
                AND latest_attempt_meta.value::boolean IS TRUE
            THEN 'completed'
            WHEN latest_artifact.is_end_step IS TRUE
                AND (
                    latest_attempt_meta IS NULL OR
                    (
                        latest_attempt_meta.is_finished IS FALSE
                        AND @(extract(epoch from now())*1000 - latest_attempt_meta.ts_epoch) > {cutoff}
                    )
                )
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
            cutoff=OLD_RUN_FAILURE_CUTOFF_TIME
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
        """
        Fetch run with a given id or number from the DB.

        Parameters
        ----------
        run_key : str
            run number or run id

        Returns
        -------
        DBResponse
            Containing a single run record, if one was found.
        """
        run_id_key, run_id_value = translate_run_key(run_key)
        result, *_ = await self.find_records(
            conditions=["{column} = %s".format(column=run_id_key)],
            values=[run_id_value],
            fetch_single=True,
            enable_joins=True,
            expanded=True
        )
        return result

    async def get_run_keys(self, conditions: List[str] = [],
                           values: List[str] = [], limit: int = 0, offset: int = 0) -> (DBResponse, DBPagination):
        """
        Get a paginated set of run keys.

        Parameters
        ----------
        conditions : List[str]
            list of conditions to pass the sql execute, with %s placeholders for values
        values : List[str]
            list of values to be passed for the sql execute.
        limit : int (optional) (default 0)
            limit for the number of results
        offset : int (optional) (default 0)
            offset for the results.

        Returns
        -------
        (DBResponse, DBPagination)
        """
        sql_template = """
            SELECT run FROM (
                SELECT DISTINCT COALESCE(run_id, run_number::text) as run, flow_id
                FROM {table_name}
            ) T
            {conditions}
            {limit}
            {offset}
            """
        select_sql = sql_template.format(
            table_name=self.table_name,
            keys=",".join(self.select_columns),
            conditions=("WHERE {}".format(" AND ".join(conditions)) if conditions else ""),
            limit="LIMIT {}".format(limit) if limit else "",
            offset="OFFSET {}".format(offset) if offset else ""
        )

        res, pag = await self.execute_sql(select_sql=select_sql, values=values, fetch_single=False,
                                          expanded=False,
                                          limit=limit, offset=offset, serialize=False)
        # process the unserialized DBResponse
        _body = [row[0] for row in res.body]

        return DBResponse(res.response_code, _body), pag

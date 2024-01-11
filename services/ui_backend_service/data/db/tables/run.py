import os
import time
from typing import List, Tuple
from .base import (
    AsyncPostgresTable,
    OLD_RUN_FAILURE_CUTOFF_TIME,
    RUN_INACTIVE_CUTOFF_TIME,
)
from ..models import RunRow
from services.data.db_utils import DBResponse, DBPagination, translate_run_key

# use schema constants from the .data module to keep things consistent
from services.data.postgres_async_db import (
    AsyncRunTablePostgres as MetadataRunTable,
    AsyncMetadataTablePostgres as MetaMetadataTable,
    AsyncArtifactTablePostgres as MetadataArtifactTable,
    AsyncTaskTablePostgres as MetadataTaskTable,
)

# Prefetch runs since 2 days ago (in seconds), limit maximum of 50 runs
METAFLOW_ARTIFACT_PREFETCH_RUNS_SINCE = os.environ.get("PREFETCH_RUNS_SINCE", 86400 * 2)
METAFLOW_ARTIFACT_PREFETCH_RUNS_LIMIT = os.environ.get("PREFETCH_RUNS_LIMIT", 50)


class AsyncRunTablePostgres(AsyncPostgresTable):
    _row_type = RunRow
    table_name = MetadataRunTable.table_name
    metadata_table = MetaMetadataTable.table_name
    artifact_table = MetadataArtifactTable.table_name
    task_table = MetadataTaskTable.table_name
    keys = MetadataRunTable.keys
    primary_keys = MetadataRunTable.primary_keys
    trigger_keys = MetadataRunTable.trigger_keys
    trigger_operations = ["INSERT"]

    # NOTE: OSS Schema has metadata value column as TEXT, but for the time being we also need to support
    # value columns of type jsonb, which is why there is additional logic when dealing with 'value'
    joins = [
        """
        LEFT JOIN LATERAL (
            SELECT
                ts_epoch,
                (CASE
                    WHEN pg_typeof(value)='jsonb'::regtype
                    THEN value::jsonb->>0
                    ELSE value::text
                END)::boolean as value
            FROM {metadata_table} as attempt_ok
            WHERE
                {table_name}.flow_id = attempt_ok.flow_id AND
                {table_name}.run_number = attempt_ok.run_number AND
                attempt_ok.step_name = 'end' AND
                attempt_ok.field_name = 'attempt_ok'
            ORDER BY ts_epoch DESC
            LIMIT 1
        ) as end_attempt_ok ON true
        """.format(
            table_name=table_name, metadata_table=metadata_table
        ),
        """
        LEFT JOIN LATERAL (
            SELECT ts_epoch
            FROM {metadata_table} as attempt
            WHERE
                {table_name}.flow_id = attempt.flow_id AND
                {table_name}.run_number = attempt.run_number AND
                attempt.step_name = 'end' AND
                attempt.field_name = 'attempt' AND
                end_attempt_ok.value IS FALSE
            ORDER BY ts_epoch DESC
            LIMIT 1
        ) as end_attempt ON true
        """.format(
            table_name=table_name, metadata_table=metadata_table
        ),
    ]

    @property
    def select_columns(self):
        # NOTE: We must use a function scope in order to be able to access the table_name variable for list comprehension.
        # User should be considered NULL when 'user:*' tag is missing
        # This is usually the case with AWS Step Functions
        return (
            [
                "{table_name}.{col} AS {col}".format(table_name=self.table_name, col=k)
                for k in self.keys
            ]
            + [
                """
                (CASE
                    WHEN system_tags ? ('user:' || user_name)
                    THEN user_name
                    ELSE NULL
                END) AS user"""
            ]
            + [
                """
                COALESCE({table_name}.run_id, {table_name}.run_number::text) AS run
                """.format(
                    table_name=self.table_name
                )
            ]
        )

    join_columns = [
        """
        (CASE
            WHEN end_attempt IS NOT NULL
                AND end_attempt_ok.ts_epoch < end_attempt.ts_epoch
            THEN NULL
            WHEN end_attempt_ok IS NOT NULL
            THEN end_attempt_ok.ts_epoch
            WHEN {table_name}.last_heartbeat_ts IS NOT NULL
                AND @(extract(epoch from now())-{table_name}.last_heartbeat_ts)<={heartbeat_cutoff}
            THEN NULL
            ELSE {table_name}.last_heartbeat_ts*1000
        END) AS finished_at
        """.format(
            table_name=table_name,
            heartbeat_cutoff=RUN_INACTIVE_CUTOFF_TIME,
        ),
        """
        (CASE
            WHEN end_attempt IS NOT NULL
                AND end_attempt_ok.ts_epoch < end_attempt.ts_epoch
            THEN 'running'
            WHEN end_attempt_ok IS NOT NULL AND end_attempt_ok.value IS TRUE
            THEN 'completed'
            WHEN end_attempt_ok IS NOT NULL AND end_attempt_ok.value IS FALSE
            THEN 'failed'
            WHEN {table_name}.last_heartbeat_ts IS NOT NULL
                AND @(extract(epoch from now())-{table_name}.last_heartbeat_ts)<={heartbeat_cutoff}
            THEN 'running'
            ELSE 'failed'
        END) AS status
        """.format(
            table_name=table_name,
            heartbeat_cutoff=RUN_INACTIVE_CUTOFF_TIME,
        ),
        """
        (CASE
            WHEN end_attempt IS NOT NULL
                AND end_attempt_ok.ts_epoch < end_attempt.ts_epoch
                AND {table_name}.last_heartbeat_ts IS NOT NULL
            THEN {table_name}.last_heartbeat_ts*1000-{table_name}.ts_epoch
            WHEN end_attempt_ok IS NOT NULL
            THEN end_attempt_ok.ts_epoch - {table_name}.ts_epoch
            WHEN {table_name}.last_heartbeat_ts IS NOT NULL
            THEN {table_name}.last_heartbeat_ts*1000-{table_name}.ts_epoch
            WHEN {table_name}.last_heartbeat_ts IS NULL
                AND @(extract(epoch from now())::bigint*1000-{table_name}.ts_epoch)>{cutoff}
            THEN NULL
            ELSE @(extract(epoch from now())::bigint*1000-{table_name}.ts_epoch)
        END) AS duration
        """.format(
            table_name=table_name,
            cutoff=OLD_RUN_FAILURE_CUTOFF_TIME,
        ),
    ]

    async def get_recent_runs(self):
        _records, *_ = await self.find_records(
            conditions=["ts_epoch >= %s"],
            values=[
                int(round(time.time() * 1000))
                - (int(METAFLOW_ARTIFACT_PREFETCH_RUNS_SINCE) * 1000)
            ],
            order=["ts_epoch DESC"],
            limit=METAFLOW_ARTIFACT_PREFETCH_RUNS_LIMIT,
            expanded=True,
        )

        return _records.body

    async def get_run(self, flow_id: str, run_key: str):
        """
        Fetch run with a given flow_id and run id or number from the DB.

        Parameters
        ----------
        flow_id : str
            flow_id
        run_key : str
            run number or run id

        Returns
        -------
        DBResponse
            Containing a single run record, if one was found.
        """
        run_id_key, run_id_value = translate_run_key(run_key)
        result, *_ = await self.find_records(
            conditions=[
                "flow_id = %s",
                "{run_id_key} = %s".format(run_id_key=run_id_key),
            ],
            values=[flow_id, run_id_value],
            fetch_single=True,
            enable_joins=True,
        )
        return result

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
            expanded=True,
        )
        return result

    async def get_run_keys(
        self,
        conditions: List[str] = [],
        values: List[str] = [],
        limit: int = 0,
        offset: int = 0,
    ) -> Tuple[DBResponse, DBPagination]:
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
            conditions=(
                "WHERE {}".format(" AND ".join(conditions)) if conditions else ""
            ),
            limit="LIMIT {}".format(limit) if limit else "",
            offset="OFFSET {}".format(offset) if offset else "",
        )

        res, pag = await self.execute_sql(
            select_sql=select_sql,
            values=values,
            fetch_single=False,
            expanded=False,
            limit=limit,
            offset=offset,
            serialize=False,
        )
        # process the unserialized DBResponse
        _body = [row[0] for row in res.body]

        return DBResponse(res.response_code, _body), pag

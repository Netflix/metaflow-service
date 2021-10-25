from .base import AsyncPostgresTable, HEARTBEAT_THRESHOLD, WAIT_TIME, OLD_RUN_FAILURE_CUTOFF_TIME
from .step import AsyncStepTablePostgres
from ..models import TaskRow
from services.data.db_utils import DBPagination, DBResponse, translate_run_key, translate_task_key
# use schema constants from the .data module to keep things consistent
from services.data.postgres_async_db import (
    AsyncTaskTablePostgres as MetadataTaskTable,
    AsyncArtifactTablePostgres as MetadataArtifactTable,
    AsyncMetadataTablePostgres as MetaMetadataTable
)
from typing import Callable


class AsyncTaskTablePostgres(AsyncPostgresTable):
    _row_type = TaskRow
    table_name = MetadataTaskTable.table_name
    artifact_table = MetadataArtifactTable.table_name
    metadata_table = MetaMetadataTable.table_name
    keys = MetadataTaskTable.keys
    primary_keys = MetadataTaskTable.primary_keys
    trigger_keys = MetadataTaskTable.trigger_keys
    # NOTE: OSS Schema has metadata value column as TEXT, but for the time being we also need to support
    # value columns of type jsonb, which is why there is additional logic when dealing with 'value'
    joins = [
        """
        LEFT JOIN LATERAL (
            SELECT
                max(started_at) as started_at,
                max(attempt_finished_at) as attempt_finished_at,
                attempt_id :: int as attempt_id,
                max(attempt_ok) :: boolean as attempt_ok,
                task_id
            FROM (
                SELECT
                    task_id,
                    ts_epoch as started_at,
                    NULL::bigint as attempt_finished_at,
                    NULL::text as attempt_ok,
                    (CASE
                        WHEN pg_typeof(value)='jsonb'::regtype
                        THEN value::jsonb->>0
                        ELSE value::text
                    END)::int as attempt_id
                FROM {metadata_table} as meta
                WHERE
                    {table_name}.flow_id = meta.flow_id AND
                    {table_name}.run_number = meta.run_number AND
                    {table_name}.step_name = meta.step_name AND
                    {table_name}.task_id = meta.task_id AND
                    meta.field_name = 'attempt'
                UNION
                SELECT
                    task_id,
                    NULL as started_at,
                    ts_epoch as attempt_finished_at,
                    NULL as attempt_ok,
                    (CASE
                        WHEN pg_typeof(value)='jsonb'::regtype
                        THEN value::json->>0
                        ELSE value::text
                    END)::int as attempt_id
                FROM {metadata_table} as meta
                WHERE
                    {table_name}.flow_id = meta.flow_id AND
                    {table_name}.run_number = meta.run_number AND
                    {table_name}.step_name = meta.step_name AND
                    {table_name}.task_id = meta.task_id AND
                    meta.field_name = 'attempt-done'
                UNION
                SELECT
                    task_id,
                    NULL as started_at,
                    ts_epoch as attempt_finished_at,
                    (CASE
                        WHEN pg_typeof(value)='jsonb'::regtype
                        THEN value::jsonb->>0
                        ELSE value::text
                    END) as attempt_ok,
                    (regexp_matches(tags::text, 'attempt_id:(\\d+)'))[1]::int as attempt_id
                FROM {metadata_table} as meta
                WHERE
                    {table_name}.flow_id = meta.flow_id AND
                    {table_name}.run_number = meta.run_number AND
                    {table_name}.step_name = meta.step_name AND
                    {table_name}.task_id = meta.task_id AND
                    meta.field_name = 'attempt_ok'
            ) a
            WHERE a.attempt_id IS NOT NULL
            GROUP BY a.task_id, a.attempt_id
        ) as attempt ON true
        LEFT JOIN LATERAL (
            SELECT ts_epoch
            FROM {metadata_table} as next_attempt_start
            WHERE
                {table_name}.flow_id = next_attempt_start.flow_id AND
                {table_name}.run_number = next_attempt_start.run_number AND
                {table_name}.step_name = next_attempt_start.step_name AND
                {table_name}.task_id = next_attempt_start.task_id AND
                next_attempt_start.field_name = 'attempt' AND
                (attempt.attempt_id + 1) = (next_attempt_start.value::jsonb->>0)::int
            LIMIT 1
        ) as next_attempt_start ON true
        """.format(
            table_name=table_name,
            metadata_table=metadata_table,
            artifact_table=artifact_table
        ),
    ]

    @property
    def select_columns(self):
        # NOTE: We must use a function scope in order to be able to access the table_name variable for list comprehension.
        return ["{table_name}.{col} AS {col}".format(table_name=self.table_name, col=k) for k in self.keys]

    join_columns = [
        "COALESCE(attempt.attempt_id, 0) as attempt_id",
        "attempt.started_at as started_at",
        """
        (CASE
        WHEN {finished_at_column} IS NULL
            AND {table_name}.last_heartbeat_ts IS NOT NULL
            AND @(extract(epoch from now())-{table_name}.last_heartbeat_ts)>{heartbeat_threshold}
        THEN {table_name}.last_heartbeat_ts*1000
        ELSE {finished_at_column}
        END) as finished_at
        """.format(
            table_name=table_name,
            heartbeat_threshold=HEARTBEAT_THRESHOLD,
            finished_at_column="COALESCE(attempt.attempt_finished_at, next_attempt_start.ts_epoch)"
        ),
        "attempt.attempt_ok as attempt_ok",
        """
        (CASE
            WHEN attempt.attempt_ok IS TRUE
            THEN 'completed'
            WHEN attempt.attempt_ok IS FALSE
            THEN 'failed'
            WHEN attempt.attempt_finished_at IS NOT NULL
                AND attempt_ok IS NULL
            THEN 'unknown'
            WHEN attempt.attempt_finished_at IS NOT NULL
            THEN 'completed'
            WHEN next_attempt_start.ts_epoch IS NOT NULL
            THEN 'failed'
            WHEN {table_name}.last_heartbeat_ts IS NOT NULL
                AND @(extract(epoch from now())-{table_name}.last_heartbeat_ts)>{heartbeat_threshold}
                AND attempt.attempt_finished_at IS NULL
            THEN 'failed'
            WHEN {table_name}.last_heartbeat_ts IS NULL
                AND @(extract(epoch from now())*1000 - COALESCE(attempt.started_at, {table_name}.ts_epoch))>{cutoff}
                AND attempt.attempt_finished_at IS NULL
            THEN 'failed'
            WHEN {table_name}.last_heartbeat_ts IS NULL
                AND attempt IS NULL
            THEN 'pending'
            ELSE 'running'
        END) AS status
        """.format(
            table_name=table_name,
            heartbeat_threshold=HEARTBEAT_THRESHOLD,
            cutoff=OLD_RUN_FAILURE_CUTOFF_TIME
        ),
        """
        (CASE
            WHEN {table_name}.last_heartbeat_ts IS NULL
                AND @(extract(epoch from now())*1000 - COALESCE(attempt.started_at, {table_name}.ts_epoch))>{cutoff}
                AND attempt.attempt_finished_at IS NULL
            THEN NULL
            WHEN {table_name}.last_heartbeat_ts IS NULL
                AND attempt IS NULL
            THEN NULL
            ELSE
                COALESCE(
                    attempt.attempt_finished_at,
                    next_attempt_start.ts_epoch,
                    {table_name}.last_heartbeat_ts*1000,
                    @(extract(epoch from now())::bigint*1000)
                ) - COALESCE(attempt.started_at, {table_name}.ts_epoch)
        END) as duration
        """.format(
            table_name=table_name,
            cutoff=OLD_RUN_FAILURE_CUTOFF_TIME
        ),
    ]
    step_table_name = AsyncStepTablePostgres.table_name
    _command = MetadataTaskTable._command

    async def get_task_attempt(self, flow_id: str, run_key: str,
                               step_name: str, task_key: str, attempt_id: int = None,
                               postprocess: Callable = None) -> DBResponse:
        """
        Fetches task attempt from DB. Specifying attempt_id will fetch the specific attempt.
        Otherwise the newest attempt is returned.

        Parameters
        ----------
        flow_id : str
            Flow id of the task
        run_key : str
            Run number or run id of the task
        step_name : str
            Step name of the task
        task_key : str
            task id or task name
        attempt_id : int (optional)
            The specific attempt of the task to be fetched. If not provided, the latest attempt is returned.
        postprocess : Callable
            A callback function for refining results.
            Receives DBResponse as an argument, and should return a DBResponse

        Returns
        -------
        DBResponse
        """
        run_id_key, run_id_value = translate_run_key(run_key)
        task_id_key, task_id_value = translate_task_key(task_key)
        conditions = [
            "flow_id = %s",
            "{run_id_column} = %s".format(run_id_column=run_id_key),
            "step_name = %s",
            "{task_id_column} = %s".format(task_id_column=task_id_key)
        ]
        values = [flow_id, run_id_value, step_name, task_id_value]
        if attempt_id:
            conditions.append("attempt_id = %s")
            values.append(attempt_id)

        result, *_ = await self.find_records(
            conditions=conditions,
            values=values,
            order=["attempt_id DESC"],
            fetch_single=True,
            enable_joins=True,
            expanded=True,
            postprocess=postprocess
        )
        return result

    async def get_tasks_for_run(self, flow_id: str, run_key: str, postprocess: Callable = None) -> DBResponse:
        """
        Fetches run tasks from DB.

        Parameters
        ----------
        flow_id : str
            Flow id of the task
        run_key : str
            Run number or run id of the task
        postprocess : Callable
            A callback function for refining results.
            Receives DBResponse as an argument, and should return a DBResponse

        Returns
        -------
        DBResponse
        """
        run_id_key, run_id_value = translate_run_key(run_key)
        conditions = [
            "flow_id = %s",
            "{run_id_column} = %s".format(run_id_column=run_id_key)
        ]
        values = [flow_id, run_id_value]

        result, *_ = await self.find_records(
            conditions=conditions,
            values=values,
            fetch_single=False,
            enable_joins=True,
            expanded=False,
            postprocess=postprocess
        )
        return result

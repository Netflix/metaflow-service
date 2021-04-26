from .base import AsyncPostgresTable, HEARTBEAT_THRESHOLD, WAIT_TIME
from .step import AsyncStepTablePostgres
from ..models import TaskRow
from services.data.db_utils import DBPagination, DBResponse, translate_run_key, translate_task_key
# use schema constants from the .data module to keep things consistent
from services.data.postgres_async_db import (
    AsyncTaskTablePostgres as MetadataTaskTable,
    AsyncArtifactTablePostgres as MetadataArtifactTable,
    AsyncMetadataTablePostgres as MetaMetadataTable
)
from typing import List, Callable
import json
import datetime


class AsyncTaskTablePostgres(AsyncPostgresTable):
    _row_type = TaskRow
    table_name = MetadataTaskTable.table_name
    artifact_table = MetadataArtifactTable.table_name
    metadata_table = MetaMetadataTable.table_name
    keys = MetadataTaskTable.keys
    primary_keys = MetadataTaskTable.primary_keys
    trigger_keys = MetadataTaskTable.trigger_keys
    # NOTE: There is a lot of unfortunate backwards compatibility for cases where task metadata, or artifacts
    # have not been stored correctly.
    # NOTE: tasks_v3 table does not have a column for 'attempt_id', instead this is added before the join
    # with a subquery in the FROM.
    # NOTE: when using these joins, we _must_ clean up the results with a WHERE that discards attempts with
    # nothing joined, otherwise we end up with ghost attempts for the task.
    joins = [
        """
        LEFT JOIN {metadata_table} as start ON (
            {table_name}.flow_id = start.flow_id AND
            {table_name}.run_number = start.run_number AND
            {table_name}.step_name = start.step_name AND
            {table_name}.task_id = start.task_id AND
            start.field_name = 'attempt' AND
            {table_name}.attempt_id = start.value::int
        )
        LEFT JOIN {metadata_table} as done ON (
            {table_name}.flow_id = done.flow_id AND
            {table_name}.run_number = done.run_number AND
            {table_name}.step_name = done.step_name AND
            {table_name}.task_id = done.task_id AND
            done.field_name = 'attempt-done' AND
            {table_name}.attempt_id = done.value::int
        )
        LEFT JOIN {metadata_table} as next_attempt_start ON (
            {table_name}.flow_id = next_attempt_start.flow_id AND
            {table_name}.run_number = next_attempt_start.run_number AND
            {table_name}.step_name = next_attempt_start.step_name AND
            {table_name}.task_id = next_attempt_start.task_id AND
            next_attempt_start.field_name = 'attempt' AND
            ({table_name}.attempt_id + 1) = next_attempt_start.value::int
        )
        LEFT JOIN {metadata_table} as attempt_ok ON (
            {table_name}.flow_id = attempt_ok.flow_id AND
            {table_name}.run_number = attempt_ok.run_number AND
            {table_name}.step_name = attempt_ok.step_name AND
            {table_name}.task_id = attempt_ok.task_id AND
            attempt_ok.field_name = 'attempt_ok' AND
            attempt_ok.tags ? ('attempt_id:' || {table_name}.attempt_id)
        )
        LEFT JOIN {artifact_table} as foreach_stack ON (
            {table_name}.flow_id = foreach_stack.flow_id AND
            {table_name}.run_number = foreach_stack.run_number AND
            {table_name}.step_name = foreach_stack.step_name AND
            {table_name}.task_id = foreach_stack.task_id AND
            foreach_stack.name = '_foreach_stack' AND
            {table_name}.attempt_id = foreach_stack.attempt_id
        )
        LEFT JOIN {artifact_table} as task_ok ON (
            {table_name}.flow_id = task_ok.flow_id AND
            {table_name}.run_number = task_ok.run_number AND
            {table_name}.step_name = task_ok.step_name AND
            {table_name}.task_id = task_ok.task_id AND
            task_ok.name = '_task_ok' AND
            {table_name}.attempt_id = task_ok.attempt_id
        )
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
        "{table_name}.attempt_id as attempt_id".format(table_name=table_name),
        "start.ts_epoch as started_at",
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
            finished_at_column="COALESCE(attempt_ok.ts_epoch, done.ts_epoch, task_ok.ts_epoch, next_attempt_start.ts_epoch)"
        ),
        "attempt_ok.value::boolean as attempt_ok",
        # If 'attempt_ok' is present, we can leave task_ok NULL since
        #   that is used to fetch the artifact value from remote location.
        # This process is performed at TaskRefiner (data_refiner.py)
        """
        (CASE
            WHEN attempt_ok.ts_epoch IS NOT NULL
            THEN NULL
            ELSE task_ok.location
        END) as task_ok
        """,
        """
        (CASE
            WHEN attempt_ok.value::boolean IS TRUE
            THEN 'completed'
            WHEN attempt_ok.value::boolean IS FALSE
            THEN 'failed'
            WHEN COALESCE(done.ts_epoch, task_ok.ts_epoch) IS NOT NULL
                AND attempt_ok IS NULL
            THEN 'unknown'
            WHEN COALESCE(attempt_ok.ts_epoch, done.ts_epoch, task_ok.ts_epoch) IS NOT NULL
            THEN 'completed'
            WHEN next_attempt_start.ts_epoch IS NOT NULL
            THEN 'failed'
            WHEN {finished_at_column} IS NULL
                AND {table_name}.last_heartbeat_ts IS NOT NULL
                AND @(extract(epoch from now())-{table_name}.last_heartbeat_ts)>{heartbeat_threshold}
            THEN 'failed'
            ELSE 'running'
        END) AS status
        """.format(
            table_name=table_name,
            heartbeat_threshold=HEARTBEAT_THRESHOLD,
            finished_at_column="COALESCE(attempt_ok.ts_epoch, done.ts_epoch, task_ok.ts_epoch)"
        ),
        """
        (CASE
            WHEN {finished_at_column} IS NULL AND {table_name}.last_heartbeat_ts IS NOT NULL
            THEN {table_name}.last_heartbeat_ts*1000-COALESCE(start.ts_epoch, {table_name}.ts_epoch)
            WHEN {finished_at_column} IS NOT NULL
            THEN {finished_at_column} - COALESCE(start.ts_epoch, {table_name}.ts_epoch)
            ELSE NULL
        END) AS duration
        """.format(
            table_name=table_name,
            finished_at_column="COALESCE(attempt_ok.ts_epoch, done.ts_epoch, task_ok.ts_epoch, next_attempt_start.ts_epoch)"
        ),
        "foreach_stack.location as foreach_stack"
    ]
    step_table_name = AsyncStepTablePostgres.table_name
    _command = MetadataTaskTable._command

    async def find_records(self, conditions: List[str] = None, values=[], fetch_single=False,
                           limit: int = 0, offset: int = 0, order: List[str] = None, groups: List[str] = None,
                           group_limit: int = 10, expanded=False, enable_joins=False,
                           postprocess: Callable[[DBResponse], DBResponse] = None,
                           benchmark: bool = False, overwrite_select_from: str = None
                           ) -> (DBResponse, DBPagination):
        if enable_joins:
            # NOTE: This is a required workaround to be able to JOIN attempt specific records to tasks.
            # the estimated max_attempts is a best guess, as tasks might have less,
            # or more (if using a custom client that allows to exceed the default max value)
            overwrite_select_from = "(SELECT *, generate_series(0,{max_attempts}) as attempt_id FROM {table_name}) as {table_name}".format(
                table_name=self.table_name,
                max_attempts=4
            )
            # NOTE: Clean up results off non-existent attempts. This is dependent heavily on the JOINs
            conditions.append("NOT (attempt_id > 0 AND started_at IS NULL AND task_ok IS NULL)")
        return await super().find_records(
            conditions, values, fetch_single,
            limit, offset, order,
            groups, group_limit, expanded,
            enable_joins, postprocess, benchmark,
            overwrite_select_from
        )

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

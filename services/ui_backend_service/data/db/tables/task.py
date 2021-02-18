from .base import AsyncPostgresTable, HEARTBEAT_THRESHOLD, WAIT_TIME
from .step import AsyncStepTablePostgres
from ..models import TaskRow
from services.data.db_utils import DBResponse, translate_run_key, translate_task_key
import json
import datetime


class AsyncTaskTablePostgres(AsyncPostgresTable):
    task_dict = {}
    step_to_task_dict = {}
    _current_count = 0
    _row_type = TaskRow
    table_name = "tasks_v3"
    keys = ["flow_id", "run_number", "run_id", "step_name", "task_id",
            "task_name", "user_name", "ts_epoch", "last_heartbeat_ts", "tags", "system_tags"]
    primary_keys = ["flow_id", "run_number", "step_name", "task_id"]
    # NOTE: There is a lot of unfortunate backwards compatibility support in the following join, due to
    # the older metadata service not recording separate metadata for task attempts. This is also the
    # reason why we must join through the artifacts table, instead of directly from metadata.
    joins = [
        """
        LEFT JOIN (
            SELECT
                task_ok.flow_id, task_ok.run_number, task_ok.step_name,
                task_ok.task_id, task_ok.attempt_id, task_ok.ts_epoch,
                task_ok.location,
                attempt.ts_epoch as started_at,
                COALESCE(attempt_ok.ts_epoch, done.ts_epoch, task_ok.ts_epoch, attempt.ts_epoch) as finished_at,
                attempt_ok.value::boolean as attempt_ok,
                foreach_stack.location as foreach_stack
            FROM {artifact_table} as task_ok
            LEFT JOIN {metadata_table} as attempt ON (
                task_ok.flow_id = attempt.flow_id AND
                task_ok.run_number = attempt.run_number AND
                task_ok.step_name = attempt.step_name AND
                task_ok.task_id = attempt.task_id AND
                attempt.field_name = 'attempt' AND
                task_ok.attempt_id = attempt.value::int
            )
            LEFT JOIN {metadata_table} as done ON (
                task_ok.flow_id = done.flow_id AND
                task_ok.run_number = done.run_number AND
                task_ok.step_name = done.step_name AND
                task_ok.task_id = done.task_id AND
                done.field_name = 'attempt-done' AND
                task_ok.attempt_id = done.value::int
            )
            LEFT JOIN {metadata_table} as attempt_ok ON (
                task_ok.flow_id = attempt_ok.flow_id AND
                task_ok.run_number = attempt_ok.run_number AND
                task_ok.step_name = attempt_ok.step_name AND
                task_ok.task_id = attempt_ok.task_id AND
                attempt_ok.field_name = 'attempt_ok' AND
                attempt_ok.tags ? ('attempt_id:' || task_ok.attempt_id)
            )
            LEFT JOIN {artifact_table} as foreach_stack ON (
                task_ok.flow_id = foreach_stack.flow_id AND
                task_ok.run_number = foreach_stack.run_number AND
                task_ok.step_name = foreach_stack.step_name AND
                task_ok.task_id = foreach_stack.task_id AND
                foreach_stack.name = '_foreach_stack' AND
                task_ok.attempt_id = foreach_stack.attempt_id
            )
            WHERE task_ok.name = '_task_ok'
        ) AS attempt ON (
            {table_name}.flow_id = attempt.flow_id AND
            {table_name}.run_number = attempt.run_number AND
            {table_name}.step_name = attempt.step_name AND
            {table_name}.task_id = attempt.task_id
        )
        """.format(
            table_name=table_name,
            metadata_table="metadata_v3",
            artifact_table="artifact_v3"
        ),
    ]
    select_columns = ["tasks_v3.{0} AS {0}".format(k) for k in keys]
    join_columns = [
        "attempt.started_at as started_at",
        """
        (CASE
        WHEN attempt.finished_at IS NULL
            AND {table_name}.last_heartbeat_ts IS NOT NULL
            AND @(extract(epoch from now())-{table_name}.last_heartbeat_ts)>{heartbeat_threshold}
        THEN {table_name}.last_heartbeat_ts*1000
        ELSE attempt.finished_at
        END) as finished_at
        """.format(
            table_name=table_name,
            heartbeat_threshold=HEARTBEAT_THRESHOLD
        ),
        "attempt.attempt_ok as attempt_ok",
        # If 'attempt_ok' is present, we can leave task_ok NULL since
        #   that is used to fetch the artifact value from remote location.
        # This process is performed at TaskRefiner (data_refiner.py)
        """
        (CASE
            WHEN attempt_ok IS NOT NULL
            THEN NULL
            ELSE attempt.location
        END) as task_ok
        """,
        """
        (CASE
            WHEN attempt_ok IS TRUE
            THEN 'completed'
            WHEN attempt_ok IS FALSE
            THEN 'failed'
            WHEN finished_at IS NOT NULL
                AND attempt_ok IS NULL
            THEN 'unknown'
            WHEN attempt.finished_at IS NOT NULL
            THEN 'completed'
            WHEN attempt.finished_at IS NULL
                AND {table_name}.last_heartbeat_ts IS NOT NULL
                AND @(extract(epoch from now())-{table_name}.last_heartbeat_ts)>{heartbeat_threshold}
            THEN 'failed'
            ELSE 'running'
        END) AS status
        """.format(
            table_name=table_name,
            heartbeat_threshold=HEARTBEAT_THRESHOLD
        ),
        """
        (CASE
            WHEN attempt.finished_at IS NULL AND {table_name}.last_heartbeat_ts IS NOT NULL
            THEN {table_name}.last_heartbeat_ts*1000-COALESCE(attempt.started_at, {table_name}.ts_epoch)
            WHEN attempt.finished_at IS NOT NULL
            THEN attempt.finished_at - COALESCE(attempt.started_at, {table_name}.ts_epoch)
            ELSE NULL
        END) AS duration
        """.format(
            table_name=table_name
        ),
        "COALESCE(attempt.attempt_id, 0) AS attempt_id",
        "attempt.foreach_stack as foreach_stack"
    ]
    step_table_name = AsyncStepTablePostgres.table_name
    _command = """
    CREATE TABLE {0} (
        flow_id VARCHAR(255) NOT NULL,
        run_number BIGINT NOT NULL,
        run_id VARCHAR(255),
        step_name VARCHAR(255) NOT NULL,
        task_id BIGSERIAL PRIMARY KEY,
        task_name VARCHAR(255),
        user_name VARCHAR(255),
        ts_epoch BIGINT NOT NULL,
        tags JSONB,
        system_tags JSONB,
        last_heartbeat_ts BIGINT,
        FOREIGN KEY(flow_id, run_number, step_name) REFERENCES {1} (flow_id, run_number, step_name),
        UNIQUE (flow_id, run_number, step_name, task_name)
    )
    """.format(
        table_name, step_table_name
    )

    async def get_tasks(self, flow_id: str, run_id: str, step_name: str):
        run_id_key, run_id_value = translate_run_key(run_id)
        filter_dict = {
            "flow_id": flow_id,
            run_id_key: run_id_value,
            "step_name": step_name,
        }
        return await self.get_records(filter_dict=filter_dict)

    async def get_task(self, flow_id: str, run_id: str, step_name: str,
                       task_id: str, expanded: bool = False):
        run_id_key, run_id_value = translate_run_key(run_id)
        task_id_key, task_id_value = translate_task_key(task_id)
        filter_dict = {
            "flow_id": flow_id,
            run_id_key: run_id_value,
            "step_name": step_name,
            task_id_key: task_id_value,
        }
        return await self.get_records(filter_dict=filter_dict,
                                      fetch_single=True, expanded=expanded)

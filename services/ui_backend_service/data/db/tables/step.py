from .base import AsyncPostgresTable
from ..models import StepRow
from services.data.db_utils import translate_run_key
# use schema constants from the .data module to keep things consistent
from services.data.postgres_async_db import (
    AsyncRunTablePostgres as MetadataRunTable,
    AsyncStepTablePostgres as MetadataStepTable,
    AsyncTaskTablePostgres as MetadataTaskTable,
    AsyncArtifactTablePostgres as MetadataArtifactTable
)
import json


class AsyncStepTablePostgres(AsyncPostgresTable):
    step_dict = {}
    run_to_step_dict = {}
    _row_type = StepRow
    table_name = MetadataStepTable.table_name
    keys = MetadataStepTable.keys
    primary_keys = MetadataStepTable.primary_keys
    trigger_keys = MetadataStepTable.trigger_keys
    select_columns = ["steps_v3.{0} AS {0}".format(k) for k in keys]
    run_table_name = MetadataRunTable.table_name
    _command = MetadataStepTable._command
    task_table_name = MetadataTaskTable.table_name
    artifact_table_name = MetadataArtifactTable.table_name
    joins = [
        """
        LEFT JOIN LATERAL (
            SELECT last_heartbeat_ts as heartbeat_ts
            FROM {task_table}
            WHERE {table_name}.flow_id={task_table}.flow_id
            AND {table_name}.run_number={task_table}.run_number
            AND {table_name}.step_name={task_table}.step_name
            ORDER BY last_heartbeat_ts DESC
            LIMIT 1
        ) AS latest_task_hb ON true
        """.format(
            table_name=table_name,
            task_table=task_table_name
        ),
        """
        LEFT JOIN LATERAL (
            SELECT ts_epoch as ts_epoch
            FROM {artifact_table}
            WHERE {table_name}.flow_id={artifact_table}.flow_id
            AND {table_name}.run_number={artifact_table}.run_number
            AND {table_name}.step_name={artifact_table}.step_name
            AND {artifact_table}.name = '_task_ok'
            ORDER BY
                ts_epoch DESC
            LIMIT 1
        ) AS latest_task_ok ON true
        """.format(
            table_name=table_name,
            artifact_table=artifact_table_name
        )
    ]
    select_columns = ["steps_v3.{0} AS {0}".format(k) for k in keys]
    join_columns = [
        """
        GREATEST(
            latest_task_ok.ts_epoch,
            latest_task_hb.heartbeat_ts*1000
        ) - {table_name}.ts_epoch as duration
        """.format(
            table_name=table_name
        )
    ]

    async def get_steps(self, flow_id: str, run_id: str):
        run_id_key, run_id_value = translate_run_key(run_id)
        filter_dict = {"flow_id": flow_id,
                       run_id_key: run_id_value}
        return await self.get_records(filter_dict=filter_dict)

    async def get_step(self, flow_id: str, run_id: str, step_name: str):
        run_id_key, run_id_value = translate_run_key(run_id)
        filter_dict = {
            "flow_id": flow_id,
            run_id_key: run_id_value,
            "step_name": step_name,
        }
        return await self.get_records(filter_dict=filter_dict, fetch_single=True)

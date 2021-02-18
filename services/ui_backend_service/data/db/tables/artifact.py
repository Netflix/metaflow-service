from .base import AsyncPostgresTable
from .task import AsyncTaskTablePostgres
from ..models import ArtifactRow
from services.data.db_utils import translate_run_key, translate_task_key
# use schema constants from the .data module to keep things consistent
from services.data.postgres_async_db import AsyncArtifactTablePostgres as MetadataArtifactTable
import json


class AsyncArtifactTablePostgres(AsyncPostgresTable):
    _row_type = ArtifactRow
    table_name = MetadataArtifactTable.table_name
    task_table_name = AsyncTaskTablePostgres.table_name
    ordering = ["attempt_id DESC"]
    keys = MetadataArtifactTable.keys
    primary_keys = MetadataArtifactTable.primary_keys
    select_columns = keys
    _command = MetadataArtifactTable._command

    async def get_artifacts_in_runs(self, flow_id: str, run_id: int):
        run_id_key, run_id_value = translate_run_key(run_id)
        filter_dict = {
            "flow_id": flow_id,
            run_id_key: run_id_value,
        }
        return await self.get_records(filter_dict=filter_dict,
                                      ordering=self.ordering)

    async def get_artifact_in_steps(self, flow_id: str, run_id: int, step_name: str):
        run_id_key, run_id_value = translate_run_key(run_id)
        filter_dict = {
            "flow_id": flow_id,
            run_id_key: run_id_value,
            "step_name": step_name,
        }
        return await self.get_records(filter_dict=filter_dict,
                                      ordering=self.ordering)

    async def get_artifact_in_task(
        self, flow_id: str, run_id: int, step_name: str, task_id: int
    ):
        run_id_key, run_id_value = translate_run_key(run_id)
        task_id_key, task_id_value = translate_task_key(task_id)
        filter_dict = {
            "flow_id": flow_id,
            run_id_key: run_id_value,
            "step_name": step_name,
            task_id_key: task_id_value,
        }
        return await self.get_records(filter_dict=filter_dict,
                                      ordering=self.ordering)

    async def get_artifact(
        self, flow_id: str, run_id: int, step_name: str, task_id: int, name: str
    ):
        run_id_key, run_id_value = translate_run_key(run_id)
        task_id_key, task_id_value = translate_task_key(task_id)
        filter_dict = {
            "flow_id": flow_id,
            run_id_key: run_id_value,
            "step_name": step_name,
            task_id_key: task_id_value,
            '"name"': name,
        }
        return await self.get_records(filter_dict=filter_dict,
                                      fetch_single=True, ordering=self.ordering)

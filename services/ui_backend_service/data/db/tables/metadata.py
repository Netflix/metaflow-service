from .base import AsyncPostgresTable
from .task import AsyncTaskTablePostgres
from ..models import MetadataRow
from services.data.db_utils import translate_run_key, translate_task_key
# use schema constants from the .data module to keep things consistent
from services.data.postgres_async_db import AsyncMetadataTablePostgres as MetaserviceMetadataTable
import json


class AsyncMetadataTablePostgres(AsyncPostgresTable):
    _row_type = MetadataRow
    table_name = MetaserviceMetadataTable.table_name
    task_table_name = AsyncTaskTablePostgres.table_name
    keys = MetaserviceMetadataTable.keys
    primary_keys = MetaserviceMetadataTable.primary_keys
    select_columns = keys
    _command = MetaserviceMetadataTable._command

    async def get_metadata_in_runs(self, flow_id: str, run_id: str):
        run_id_key, run_id_value = translate_run_key(run_id)
        filter_dict = {"flow_id": flow_id,
                       run_id_key: run_id_value}
        return await self.get_records(filter_dict=filter_dict)

    async def get_metadata(
        self, flow_id: str, run_id: int, step_name: str, task_id: str
    ):
        run_id_key, run_id_value = translate_run_key(run_id)
        task_id_key, task_id_value = translate_task_key(task_id)
        filter_dict = {
            "flow_id": flow_id,
            run_id_key: run_id_value,
            "step_name": step_name,
            task_id_key: task_id_value,
        }
        return await self.get_records(filter_dict=filter_dict)

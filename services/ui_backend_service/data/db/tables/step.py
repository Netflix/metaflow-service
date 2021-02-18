from .base import AsyncPostgresTable
from ..models import StepRow
from .run import AsyncRunTablePostgres
from services.data.db_utils import translate_run_key
# use schema constants from the .data module to keep things consistent
from services.data.postgres_async_db import AsyncStepTablePostgres as MetadataStepTable
import json


class AsyncStepTablePostgres(AsyncPostgresTable):
    _row_type = StepRow
    table_name = MetadataStepTable.table_name
    keys = MetadataStepTable.keys
    primary_keys = MetadataStepTable.primary_keys
    select_columns = keys
    run_table_name = AsyncRunTablePostgres.table_name
    _command = MetadataStepTable._command

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

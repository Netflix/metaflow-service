from .base import AsyncPostgresTable
from ..models import FlowRow
# use schema constants from the .data module to keep things consistent
from services.data.postgres_async_db import AsyncFlowTablePostgres as MetadataFlowTable
import json


class AsyncFlowTablePostgres(AsyncPostgresTable):
    flow_dict = {}
    table_name = MetadataFlowTable.table_name
    keys = ["flow_id", "user_name", "ts_epoch", "tags", "system_tags"]
    primary_keys = ["flow_id"]
    select_columns = keys
    join_columns = []
    _command = MetadataFlowTable._command
    _row_type = FlowRow

    async def get_flow(self, flow_id: str):
        filter_dict = {"flow_id": flow_id}
        return await self.get_records(filter_dict=filter_dict, fetch_single=True)

    async def get_all_flows(self):
        return await self.get_records()

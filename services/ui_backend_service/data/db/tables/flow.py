from .base import AsyncPostgresTable
from ..models import FlowRow
# use schema constants from the .data module to keep things consistent
from services.data.postgres_async_db import AsyncFlowTablePostgres as MetadataFlowTable
import json


class AsyncFlowTablePostgres(AsyncPostgresTable):
    table_name = MetadataFlowTable.table_name
    keys = MetadataFlowTable.keys
    primary_keys = MetadataFlowTable.primary_keys
    trigger_keys = MetadataFlowTable.trigger_keys
    select_columns = keys
    _command = MetadataFlowTable._command
    _row_type = FlowRow

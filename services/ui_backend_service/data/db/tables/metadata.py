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
    trigger_keys = MetaserviceMetadataTable.trigger_keys
    select_columns = keys
    _command = MetaserviceMetadataTable._command

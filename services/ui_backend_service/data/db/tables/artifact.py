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
    trigger_keys = MetadataArtifactTable.trigger_keys
    select_columns = keys
    _command = MetadataArtifactTable._command

from .base import AsyncPostgresTable
from .task import AsyncTaskTablePostgres
from ..models import MetadataRow
# use schema constants from the .data module to keep things consistent
from services.data.postgres_async_db import AsyncMetadataTablePostgres as MetaserviceMetadataTable


class AsyncMetadataTablePostgres(AsyncPostgresTable):
    _row_type = MetadataRow
    table_name = MetaserviceMetadataTable.table_name
    task_table_name = AsyncTaskTablePostgres.table_name
    keys = MetaserviceMetadataTable.keys
    primary_keys = MetaserviceMetadataTable.primary_keys
    trigger_keys = MetaserviceMetadataTable.trigger_keys
    _command = MetaserviceMetadataTable._command

    @property
    def select_columns(self):
        keys = ["{table_name}.{col} AS {col}".format(table_name=self.table_name, col=k) for k in self.keys]

        # Must use SELECT on the regexp matches in order to include non-matches as well, otherwise
        # we won't be able to fill attempt_id with NULL in case no id has been recorded
        # (f.ex. run-level metadata)
        keys.append(
            "(SELECT regexp_matches(tags::text, 'attempt_id:(\\d+)'))[1]::int as attempt_id"
        )
        return keys

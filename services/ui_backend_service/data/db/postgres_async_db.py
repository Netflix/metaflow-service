import json
import os
import time
from typing import List

import aiopg
import psycopg2
import psycopg2.extras
# baselevel classes from shared data adapter to inherit from.
from services.data.postgres_async_db import \
    _AsyncPostgresDB as BaseAsyncPostgresDB
from services.utils import DBConfiguration, logging

from .tables import (AsyncArtifactTablePostgres, AsyncFlowTablePostgres,
                     AsyncMetadataTablePostgres, AsyncRunTablePostgres,
                     AsyncStepTablePostgres, AsyncTaskTablePostgres)


class AsyncPostgresDB(BaseAsyncPostgresDB):
    """
    UI Backend specific database adapter.
    Basic functionality is inherited from the classes provided by the shared services.data.postgres_async_db module.

    Parameters
    ----------
    name : str (optional)
        name for the DB Adapter instance. Used primarily for naming the associated logger.
    """
    connection = None
    flow_table_postgres = None
    run_table_postgres = None
    step_table_postgres = None
    task_table_postgres = None
    artifact_table_postgres = None
    metadata_table_postgres = None

    pool = None
    db_conf: DBConfiguration = None

    def __init__(self, name='global'):
        self.name = name
        self.logger = logging.getLogger("AsyncPostgresDB:{name}".format(name=self.name))

        tables = []
        self.flow_table_postgres = AsyncFlowTablePostgres(self)
        self.run_table_postgres = AsyncRunTablePostgres(self)
        self.step_table_postgres = AsyncStepTablePostgres(self)
        self.task_table_postgres = AsyncTaskTablePostgres(self)
        self.artifact_table_postgres = AsyncArtifactTablePostgres(self)
        self.metadata_table_postgres = AsyncMetadataTablePostgres(self)
        tables.append(self.flow_table_postgres)
        tables.append(self.run_table_postgres)
        tables.append(self.step_table_postgres)
        tables.append(self.task_table_postgres)
        tables.append(self.artifact_table_postgres)
        tables.append(self.metadata_table_postgres)
        self.tables = tables

    async def apply_filters_to_data(self, data, conditions: List[str] = None, values=[]) -> bool:
        """
        This function is used to verify 'data' object matches the same filters as
        'AsyncPostgresTable.find_records' does. This is used with 'pg_notify' + Websocket
        events to make sure that specific subscriber receives filtered data correctly.
        """
        keys, vals, stm_vals = [], [], []
        for k, v in data.items():
            keys.append(k)
            if k == "tags" or k == "system_tags":
                # Handle JSON fields
                vals.append(json.dumps(v))
                stm_vals.append("%s::jsonb")
            else:
                vals.append(v)
                stm_vals.append("%s")

        # Prepend constructed data values before WHERE values
        values = vals + values

        select_sql = "SELECT * FROM (VALUES({values})) T({keys}) {where}".format(
            values=", ".join(stm_vals),
            keys=", ".join(map(lambda k: "\"{}\"".format(k), keys)),
            where="WHERE {}".format(" AND ".join(
                conditions)) if conditions else "",
        )

        try:
            with (
                await self.pool.cursor(
                    cursor_factory=psycopg2.extras.DictCursor
                )
            ) as cur:
                await cur.execute(select_sql, values)
                records = await cur.fetchall()
                cur.close()
                return len(records) > 0
        except:
            self.logger.exception("Exception occured")
            return False

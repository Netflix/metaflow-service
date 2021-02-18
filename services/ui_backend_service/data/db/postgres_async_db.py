import psycopg2
import psycopg2.extras
import os
import aiopg
import json
import time
from services.utils import logging
from typing import List

# baselevel classes from shared data adapter to inherit from.
from services.data.postgres_async_db import _AsyncPostgresDB as BaseAsyncPostgresDB
from .tables import (
    AsyncFlowTablePostgres, AsyncRunTablePostgres, AsyncStepTablePostgres,
    AsyncTaskTablePostgres, AsyncArtifactTablePostgres,
    AsyncMetadataTablePostgres
)

from services.utils import DBConfiguration

AIOPG_ECHO = os.environ.get("AIOPG_ECHO", 0) == "1"

WAIT_TIME = 10
# Heartbeat check interval. Add margin in case of client-server communication delays, before marking a heartbeat stale.
HEARTBEAT_THRESHOLD = int(os.environ.get("HEARTBEAT_THRESHOLD", WAIT_TIME * 6))
OLD_RUN_FAILURE_CUTOFF_TIME = int(os.environ.get("OLD_RUN_FAILURE_CUTOFF_TIME", 60 * 60 * 24 * 1000 * 14))  # default 2 weeks (in milliseconds)

# Create database triggers automatically, disabled by default
# Enable with env variable `DB_TRIGGER_CREATE=1`
DB_TRIGGER_CREATE = os.environ.get("DB_TRIGGER_CREATE", 0) == "1"


class AsyncPostgresDB(BaseAsyncPostgresDB):
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

    async def _init(self, db_conf: DBConfiguration, create_triggers=DB_TRIGGER_CREATE, create_tables=False):
        # todo make poolsize min and max configurable as well as timeout
        # todo add retry and better error message
        retries = 3
        for i in range(retries):
            try:
                self.pool = await aiopg.create_pool(
                    db_conf.dsn,
                    minsize=db_conf.pool_min,
                    maxsize=db_conf.pool_max,
                    timeout=db_conf.timeout,
                    echo=AIOPG_ECHO)

                # Clean existing trigger functions before creating new ones
                if create_triggers:
                    self.logger.info("Cleanup existing notify triggers")
                    await PostgresUtils.function_cleanup(self)

                for table in self.tables:
                    await table._init(create_tables=create_tables, create_triggers=create_triggers)

                self.logger.info(
                    "Connection established.\n"
                    "   Pool min: {pool_min} max: {pool_max}\n".format(
                        pool_min=self.pool.minsize,
                        pool_max=self.pool.maxsize))

                break  # Break the retry loop
            except Exception as e:
                self.logger.exception("Exception occured")
                if retries - i < 1:
                    raise e
                time.sleep(1)

    async def get_table_by_name(self, table_name: str):
        for table in self.tables:
            if table.table_name == table_name:
                return table
        return None

    async def get_run_ids(self, flow_id: str, run_id: str):
        run = await self.run_table_postgres.get_run(flow_id, run_id,
                                                    expanded=True)
        return run.body['run_number'], run.body['run_id']

    async def get_task_ids(self, flow_id: str, run_id: str,
                           step_name: str, task_name: str):

        task = await self.task_table_postgres.get_task(flow_id, run_id,
                                                       step_name, task_name,
                                                       expanded=True)
        return task.body['task_id'], task.body['task_name']

    # This function is used to verify 'data' object matches the same filters as
    # 'AsyncPostgresTable.find_records' does. This is used with 'pg_notify' + Websocket
    # events to make sure that specific subscriber receives filtered data correctly.
    async def apply_filters_to_data(self, data, conditions: List[str] = None, values=[]) -> bool:
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


class PostgresUtils(object):
    @staticmethod
    async def create_if_missing(db: AsyncPostgresDB, table_name, command):
        with (await db.pool.cursor()) as cur:
            try:
                await cur.execute(
                    "select * from information_schema.tables where table_name=%s",
                    (table_name,),
                )
                table_exist = bool(cur.rowcount)
                if not table_exist:
                    await cur.execute(command)
            finally:
                cur.close()
    # todo add method to check schema version

    @staticmethod
    async def function_cleanup(db: AsyncPostgresDB):
        name_prefix = "notify_ui"
        _command = """
        DO $$DECLARE r RECORD;
        BEGIN
            FOR r IN SELECT routine_schema, routine_name FROM information_schema.routines
                    WHERE routine_name LIKE '{prefix}%'
            LOOP
                EXECUTE 'DROP FUNCTION ' || quote_ident(r.routine_schema) || '.' || quote_ident(r.routine_name) || '() CASCADE';
            END LOOP;
        END$$;
        """.format(
            prefix=name_prefix
        )

        with (await db.pool.cursor()) as cur:
            await cur.execute(_command)
            cur.close()

    @staticmethod
    async def trigger_notify(db: AsyncPostgresDB, table_name, keys: List[str] = None, schema="public"):
        if not keys:
            pass

        name_prefix = "notify_ui"
        operations = ["INSERT", "UPDATE", "DELETE"]
        _commands = ["""
        CREATE OR REPLACE FUNCTION {schema}.{prefix}_{table}() RETURNS trigger
            LANGUAGE plpgsql
            AS $$
        DECLARE
            rec RECORD;
            BEGIN

            CASE TG_OP
            WHEN 'INSERT', 'UPDATE' THEN
                rec := NEW;
            WHEN 'DELETE' THEN
                rec := OLD;
            ELSE
                RAISE EXCEPTION 'Unknown TG_OP: "%"', TG_OP;
            END CASE;

            PERFORM pg_notify('notify', json_build_object(
                            'table',     TG_TABLE_NAME,
                            'schema',    TG_TABLE_SCHEMA,
                            'operation', TG_OP,
                            'data',      json_build_object({keys})
                    )::text);
            RETURN rec;
            END;
        $$;
        """.format(
            schema=schema,
            prefix=name_prefix,
            table=table_name,
            keys=", ".join(map(lambda k: "'{0}', rec.{0}".format(k), keys)),
            events=" OR ".join(operations)
        )]
        _commands += ["DROP TRIGGER IF EXISTS {prefix}_{table} ON {schema}.{table};".format(
            schema=schema,
            prefix=name_prefix,
            table=table_name
        )]

        _commands += ["""
            CREATE TRIGGER {prefix}_{table} AFTER {events} ON {schema}.{table}
                FOR EACH ROW EXECUTE PROCEDURE {schema}.{prefix}_{table}();
            """.format(
            schema=schema,
            prefix=name_prefix,
            table=table_name,
            events=" OR ".join(operations)
        )]

        with (await db.pool.cursor()) as cur:
            for _command in _commands:
                await cur.execute(_command)
            cur.close()

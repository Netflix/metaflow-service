import psycopg2
import psycopg2.extras
import os
import aiopg
import json
import math
import time
import datetime
from services.utils import logging
from typing import List, Callable
from asyncio import iscoroutinefunction

from .db_utils import DBResponse, DBPagination, aiopg_exception_handling, \
    get_db_ts_epoch_str, translate_run_key, translate_task_key
from .models import FlowRow, RunRow, StepRow, TaskRow, MetadataRow, ArtifactRow
from services.utils import DBConfiguration

AIOPG_ECHO = os.environ.get("AIOPG_ECHO", 0) == "1"

WAIT_TIME = 10
HEARTBEAT_THRESHOLD = WAIT_TIME * 6  # Add margin in case of client-server communication delays, before marking a heartbeat stale.
OLD_RUN_FAILURE_CUTOFF_TIME = 60 * 60 * 24 * 1000 * 14  # 2 weeks (in milliseconds)

# Create database triggers automatically, disabled by default
# Enable with env variable `DB_TRIGGER_CREATE=1`
DB_TRIGGER_CREATE = os.environ.get("DB_TRIGGER_CREATE", 0) == "1"


class _AsyncPostgresDB(object):
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

    async def _init(self, db_conf: DBConfiguration, create_triggers=DB_TRIGGER_CREATE):
        # todo make poolsize min and max configurable as well as timeout
        # todo add retry and better error message
        retries = 3
        for i in range(retries):
            try:
                self.pool = await aiopg.create_pool(
                    db_conf.dsn,
                    minsize=db_conf.pool_min,
                    maxsize=db_conf.pool_max,
                    echo=AIOPG_ECHO)

                # Clean existing trigger functions before creating new ones
                if create_triggers:
                    self.logger.info("Cleanup existing notify triggers")
                    await PostgresUtils.function_cleanup(self)

                for table in self.tables:
                    await table._init(create_triggers=create_triggers)

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
            keys=", ".join(keys),
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


class AsyncPostgresDB(object):
    __instance = None

    @staticmethod
    def get_instance():
        return AsyncPostgresDB()

    def __init__(self):
        if not AsyncPostgresDB.__instance:
            AsyncPostgresDB.__instance = _AsyncPostgresDB()

    def __getattribute__(self, name):
        return getattr(AsyncPostgresDB.__instance, name)


class AsyncPostgresTable(object):
    db = None
    table_name = None
    schema_version = 1
    keys: List[str] = []
    primary_keys: List[str] = None
    ordering: List[str] = None
    joins: List[str] = None
    select_columns: List[str] = keys
    join_columns: List[str] = None
    _command = None
    _insert_command = None
    _filters = None
    _base_query = "SELECT {0} from"
    _row_type = None

    def __init__(self, db: _AsyncPostgresDB = None):
        self.db = db
        if self.table_name is None or self._command is None:
            raise NotImplementedError(
                "need to specify table name and create command")

    async def _init(self, create_triggers: bool):
        await PostgresUtils.create_if_missing(self.db, self.table_name, self._command)
        if create_triggers:
            self.db.logger.info(
                "Create notify trigger for {table_name}\n   Keys: {keys}".format(
                    table_name=self.table_name, keys=self.primary_keys))
            await PostgresUtils.trigger_notify(db=self.db, table_name=self.table_name, keys=self.primary_keys)

    async def get_records(self, filter_dict={}, fetch_single=False,
                          ordering: List[str] = None, limit: int = 0, expanded=False) -> DBResponse:
        conditions = []
        values = []
        for col_name, col_val in filter_dict.items():
            conditions.append("{} = %s".format(col_name))
            values.append(col_val)

        response, _ = await self.find_records(conditions=conditions, values=values, fetch_single=fetch_single,
                                              order=ordering, limit=limit, expanded=expanded)
        return response

    async def find_records(self, conditions: List[str] = None, values=[], fetch_single=False,
                           limit: int = 0, offset: int = 0, order: List[str] = None, groups: List[str] = None,
                           group_limit: int = 10, expanded=False, enable_joins=False,
                           postprocess: Callable[[DBResponse], DBResponse] = None,
                           benchmark: bool = False) -> (DBResponse, DBPagination):
        # Alias T is important here which is used to construct ordering and conditions

        # Grouping not enabled
        if groups is None or len(groups) == 0:
            sql_template = """
            SELECT * FROM (
                SELECT
                    {keys}
                FROM {table_name}
                {joins}
            ) T
            {where}
            {order_by}
            {limit}
            {offset}
            """

            if order:
                # Order using alias T
                order = map(lambda o: "T.{}".format(o), order)

            select_sql = sql_template.format(
                keys=",".join(
                    self.select_columns + (self.join_columns if enable_joins and self.join_columns else [])),
                table_name=self.table_name,
                joins=" ".join(
                    self.joins) if enable_joins and self.joins else "",
                where="WHERE {}".format(" AND ".join(
                    conditions)) if conditions else "",
                order_by="ORDER BY {}".format(
                    ", ".join(order)) if order else "",
                limit="LIMIT {}".format(limit) if limit else "",
                offset="OFFSET {}".format(offset) if offset else ""
            ).strip()
        else:  # Grouping enabled
            sql_template = """
            SELECT * FROM (
                SELECT
                    *, ROW_NUMBER() OVER(PARTITION BY {group_by} {order_by})
                FROM (
                    SELECT
                        {keys}
                    FROM {table_name}
                    {joins}
                ) T
                {where}
            ) G
            {group_limit}
            ORDER BY {group_by} ASC NULLS LAST
            {limit}
            {offset}
            """

            select_sql = sql_template.format(
                keys=",".join(
                    self.select_columns + (self.join_columns if enable_joins and self.join_columns else [])),
                table_name=self.table_name,
                joins=" ".join(
                    self.joins) if enable_joins and self.joins is not None else "",
                where="WHERE {}".format(" AND ".join(
                    conditions)) if conditions else "",
                group_by=", ".join(groups),
                order_by="ORDER BY {}".format(
                    ", ".join(order)) if order else "",
                group_limit="WHERE row_number <= {}".format(
                    group_limit) if group_limit else "",
                limit="LIMIT {}".format(limit) if limit else "",
                offset="OFFSET {}".format(offset) if offset else ""
            ).strip()

        # Run benchmarking on query if requested
        benchmark_results = None
        if benchmark:
            benchmark_results = await self.benchmark_sql(
                select_sql=select_sql, values=values, fetch_single=fetch_single,
                expanded=expanded, limit=limit, offset=offset
            )

        result, pagination = await self.execute_sql(select_sql=select_sql, values=values, fetch_single=fetch_single,
                                                    expanded=expanded, limit=limit, offset=offset)
        # Modify the response after the fetch has been executed
        if postprocess is not None:
            if iscoroutinefunction(postprocess):
                result = await postprocess(result)
            else:
                result = postprocess(result)

        return result, pagination, benchmark_results

    async def benchmark_sql(self, select_sql: str, values=[], fetch_single=False,
                            expanded=False, limit: int = 0, offset: int = 0):
        "Benchmark and log a given SQL query with EXPLAIN ANALYZE"
        try:
            with (
                await self.db.pool.cursor(
                    cursor_factory=psycopg2.extras.DictCursor
                )
            ) as cur:
                # Run EXPLAIN ANALYZE on query and log the results.
                benchmark_sql = "EXPLAIN ANALYZE {}".format(select_sql)
                await cur.execute(benchmark_sql, values)

                records = await cur.fetchall()
                rows = []
                for record in records:
                    rows.append(record[0])
                return "\n".join(rows)
        except (Exception, psycopg2.DatabaseError):
            self.db.logger.exception("Query Benchmarking failed")
            return None

    async def execute_sql(self, select_sql: str, values=[], fetch_single=False,
                          expanded=False, limit: int = 0, offset: int = 0) -> (DBResponse, DBPagination):
        try:
            with (
                await self.db.pool.cursor(
                    cursor_factory=psycopg2.extras.DictCursor
                )
            ) as cur:
                await cur.execute(select_sql, values)

                rows = []
                records = await cur.fetchall()
                for record in records:
                    row = self._row_type(**record)
                    rows.append(row.serialize(expanded))

                count = len(rows)

                # Will raise IndexError in case fetch_single=True and there's no results
                body = rows[0] if fetch_single else rows

                pagination = DBPagination(
                    limit=limit,
                    offset=offset,
                    count=count,
                    page=math.floor(int(offset) / max(int(limit), 1)) + 1,
                )

                cur.close()
                return DBResponse(response_code=200, body=body), pagination
        except IndexError as error:
            return aiopg_exception_handling(error), None
        except (Exception, psycopg2.DatabaseError) as error:
            self.db.logger.exception("Exception occured")
            return aiopg_exception_handling(error), None

    async def get_tags(self):
        sql_template = "SELECT DISTINCT tag FROM (SELECT JSONB_ARRAY_ELEMENTS(tags||system_tags) AS tag FROM {table_name}) AS t"
        select_sql = sql_template.format(table_name=self.table_name)

        try:
            with (
                await self.db.pool.cursor(
                    cursor_factory=psycopg2.extras.DictCursor
                )
            ) as cur:
                await cur.execute(select_sql)

                tags = []
                records = await cur.fetchall()
                for record in records:
                    tags += record
                cur.close()
                return DBResponse(response_code=200, body=tags)
        except (Exception, psycopg2.DatabaseError) as error:
            self.db.logger.exception("Exception occured")
            return aiopg_exception_handling(error)

    async def create_record(self, record_dict):
        # note: need to maintain order
        cols = []
        values = []
        for col_name, col_val in record_dict.items():
            cols.append(col_name)
            values.append(col_val)

        # add create ts
        if "ts_epoch" not in cols:
            cols.append("ts_epoch")
            values.append(get_db_ts_epoch_str())

        str_format = []
        for col in cols:
            str_format.append("%s")

        seperator = ", "

        insert_sql = """
                    INSERT INTO {0}({1}) VALUES({2})
                    RETURNING *
                    """.format(
            self.table_name, seperator.join(cols), seperator.join(str_format)
        )

        try:
            response_body = {}
            with (
                await self.db.pool.cursor(
                    cursor_factory=psycopg2.extras.DictCursor
                )
            ) as cur:

                await cur.execute(insert_sql, tuple(values))
                records = await cur.fetchall()
                record = records[0]
                filtered_record = {}
                for key, value in record.items():
                    if key in self.keys:
                        filtered_record[key] = value
                response_body = self._row_type(**filtered_record).serialize()
                # todo make sure connection is closed even with error
                cur.close()
            return DBResponse(response_code=200, body=response_body)
        except (Exception, psycopg2.DatabaseError) as error:
            self.db.logger.exception("Exception occured")
            return aiopg_exception_handling(error)

    async def update_row(self, filter_dict={}, update_dict={}):
        # generate where clause
        filters = []
        for col_name, col_val in filter_dict.items():
            v = str(col_val).strip("'")
            if not v.isnumeric():
                v = "'" + v + "'"
            filters.append(col_name + "=" + str(v))

        seperator = " and "
        where_clause = ""
        if bool(filter_dict):
            where_clause = seperator.join(filters)

        sets = []
        for col_name, col_val in update_dict.items():
            sets.append(col_name + " = " + str(col_val))

        set_seperator = ", "
        set_clause = ""
        if bool(filter_dict):
            set_clause = set_seperator.join(sets)
        update_sql = """
                UPDATE {0} SET {1} WHERE {2};
        """.format(self.table_name, set_clause, where_clause)
        try:
            with (
                await self.db.pool.cursor(
                    cursor_factory=psycopg2.extras.DictCursor
                )
            ) as cur:
                await cur.execute(update_sql)
                if cur.rowcount < 1:
                    return DBResponse(response_code=404,
                                      body={"msg": "could not find row"})
                if cur.rowcount > 1:
                    return DBResponse(response_code=500,
                                      body={"msg": "duplicate rows"})
                body = {"rowcount": cur.rowcount}
                # todo make sure connection is closed even with error
                cur.close()
                return DBResponse(response_code=200, body=body)
        except (Exception, psycopg2.DatabaseError) as error:
            self.db.logger.exception("Exception occured")
            return aiopg_exception_handling(error)


class PostgresUtils(object):
    @staticmethod
    async def create_if_missing(db: _AsyncPostgresDB, table_name, command):
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
    async def function_cleanup(db: _AsyncPostgresDB):
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
    async def trigger_notify(db: _AsyncPostgresDB, table_name, keys: List[str] = None, schema="public"):
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


class AsyncFlowTablePostgres(AsyncPostgresTable):
    flow_dict = {}
    table_name = "flows_v3"
    keys = ["flow_id", "user_name", "ts_epoch", "tags", "system_tags"]
    primary_keys = ["flow_id"]
    select_columns = keys
    join_columns = []
    _command = """
    CREATE TABLE {0} (
        flow_id VARCHAR(255) PRIMARY KEY,
        user_name VARCHAR(255),
        ts_epoch BIGINT NOT NULL,
        tags JSONB,
        system_tags JSONB
    )
    """.format(
        table_name
    )
    _row_type = FlowRow

    async def add_flow(self, flow: FlowRow):
        dict = {
            "flow_id": flow.flow_id,
            "user_name": flow.user_name,
            "ts_epoch": flow.ts_epoch,
            "tags": json.dumps(flow.tags),
            "system_tags": json.dumps(flow.system_tags),
        }
        return await self.create_record(dict)

    async def get_flow(self, flow_id: str):
        filter_dict = {"flow_id": flow_id}
        return await self.get_records(filter_dict=filter_dict, fetch_single=True)

    async def get_all_flows(self):
        return await self.get_records()


class AsyncRunTablePostgres(AsyncPostgresTable):
    run_dict = {}
    run_by_flow_dict = {}
    _current_count = 0
    _row_type = RunRow
    table_name = "runs_v3"
    keys = ["flow_id", "run_number", "run_id",
            "user_name", "ts_epoch", "last_heartbeat_ts", "tags", "system_tags"]
    primary_keys = ["flow_id", "run_number"]
    joins = [
        """
        LEFT JOIN (
            SELECT
                artifacts.flow_id, artifacts.run_number, artifacts.step_name,
                artifacts.task_id, artifacts.attempt_id, artifacts.ts_epoch,
                attempt_ok.value::boolean as attempt_ok
            FROM {artifact_table} as artifacts
            LEFT JOIN {metadata_table} as attempt_ok ON (
                artifacts.flow_id = attempt_ok.flow_id AND
                artifacts.run_number = attempt_ok.run_number AND
                artifacts.task_id = attempt_ok.task_id AND
                artifacts.step_name = attempt_ok.step_name AND
                attempt_ok.field_name = 'attempt_ok' AND
                attempt_ok.tags ? CONCAT('attempt_id:', artifacts.attempt_id)
            )
            WHERE artifacts.name = '_task_ok' AND artifacts.step_name = 'end'
        ) AS artifacts ON (
            {table_name}.flow_id = artifacts.flow_id AND
            {table_name}.run_number = artifacts.run_number
        )
        """.format(
            table_name=table_name,
            metadata_table="metadata_v3",
            artifact_table="artifact_v3"
        ),
    ]
    # User should be considered NULL when 'user:*' tag is missing
    # This is usually the case with AWS Step Functions
    select_columns = ["runs_v3.{0} AS {0}".format(k) for k in keys] \
        + ["""
            (CASE
                WHEN system_tags ? CONCAT('user:', user_name)
                THEN user_name
                ELSE NULL
            END) AS user"""]
    join_columns = [
        """
        (CASE
            WHEN artifacts.ts_epoch IS NOT NULL
            THEN artifacts.ts_epoch
            WHEN {table_name}.last_heartbeat_ts IS NOT NULL
            AND @(extract(epoch from now())-{table_name}.last_heartbeat_ts)>{heartbeat_threshold}
            THEN {table_name}.last_heartbeat_ts*1000
            WHEN {table_name}.last_heartbeat_ts IS NULL
            AND @(extract(epoch from now())*1000-{table_name}.ts_epoch)>{cutoff}
            THEN {table_name}.ts_epoch + {cutoff}
            ELSE NULL
        END) AS finished_at
        """.format(
            table_name=table_name,
            heartbeat_threshold=HEARTBEAT_THRESHOLD,
            cutoff=OLD_RUN_FAILURE_CUTOFF_TIME
        ),
        """
        (CASE
            WHEN artifacts.attempt_ok IS TRUE
            THEN 'completed'
            WHEN artifacts.attempt_ok IS FALSE
            THEN 'failed'
            WHEN artifacts.ts_epoch IS NOT NULL
            THEN 'completed'
            WHEN {table_name}.last_heartbeat_ts IS NOT NULL
            AND @(extract(epoch from now())-{table_name}.last_heartbeat_ts)>{heartbeat_threshold}
            THEN 'failed'
            WHEN {table_name}.last_heartbeat_ts IS NULL
            AND @(extract(epoch from now())*1000-{table_name}.ts_epoch)>{cutoff}
            THEN 'failed'
            ELSE 'running'
        END) AS status
        """.format(
            table_name=table_name,
            heartbeat_threshold=HEARTBEAT_THRESHOLD,
            cutoff=OLD_RUN_FAILURE_CUTOFF_TIME
        ),
        """
        (CASE
            WHEN artifacts.ts_epoch IS NULL AND {table_name}.last_heartbeat_ts IS NOT NULL
            THEN {table_name}.last_heartbeat_ts*1000-{table_name}.ts_epoch
            WHEN artifacts.ts_epoch IS NOT NULL
            THEN artifacts.ts_epoch - {table_name}.ts_epoch
            ELSE NULL
        END) AS duration
        """.format(table_name=table_name)
    ]
    flow_table_name = AsyncFlowTablePostgres.table_name
    _command = """
    CREATE TABLE {0} (
        flow_id VARCHAR(255) NOT NULL,
        run_number SERIAL NOT NULL,
        run_id VARCHAR(255),
        user_name VARCHAR(255),
        ts_epoch BIGINT NOT NULL,
        tags JSONB,
        system_tags JSONB,
        last_heartbeat_ts BIGINT,
        PRIMARY KEY(flow_id, run_number),
        FOREIGN KEY(flow_id) REFERENCES {1} (flow_id),
        UNIQUE (flow_id, run_id)
    )
    """.format(
        table_name, flow_table_name
    )

    async def add_run(self, run: RunRow):
        dict = {
            "flow_id": run.flow_id,
            "user_name": run.user_name,
            "ts_epoch": run.ts_epoch,
            "tags": json.dumps(run.tags),
            "system_tags": json.dumps(run.system_tags),
            "run_id": run.run_id,
        }
        return await self.create_record(dict)

    async def get_run(self, flow_id: str, run_id: str, expanded: bool = False):
        key, value = translate_run_key(run_id)
        filter_dict = {"flow_id": flow_id, key: str(value)}
        return await self.get_records(filter_dict=filter_dict,
                                      fetch_single=True, expanded=expanded)

    async def get_all_runs(self, flow_id: str):
        filter_dict = {"flow_id": flow_id}
        return await self.get_records(filter_dict=filter_dict)

    async def update_heartbeat(self, flow_id: str, run_id: str):
        run_key, run_value = translate_run_key(run_id)
        filter_dict = {"flow_id": flow_id,
                       run_key: str(run_value)}
        set_dict = {
            "last_heartbeat_ts": int(datetime.datetime.utcnow().timestamp())
        }
        result = await self.update_row(filter_dict=filter_dict,
                                       update_dict=set_dict)
        body = {"wait_time_in_seconds": WAIT_TIME}

        return DBResponse(response_code=result.response_code,
                          body=json.dumps(body))


class AsyncStepTablePostgres(AsyncPostgresTable):
    step_dict = {}
    run_to_step_dict = {}
    _row_type = StepRow
    table_name = "steps_v3"
    keys = ["flow_id", "run_number", "run_id", "step_name",
            "user_name", "ts_epoch", "tags", "system_tags"]
    primary_keys = ["flow_id", "run_number", "step_name"]
    select_columns = keys
    run_table_name = AsyncRunTablePostgres.table_name
    _command = """
    CREATE TABLE {0} (
        flow_id VARCHAR(255) NOT NULL,
        run_number BIGINT NOT NULL,
        run_id VARCHAR(255),
        step_name VARCHAR(255) NOT NULL,
        user_name VARCHAR(255),
        ts_epoch BIGINT NOT NULL,
        tags JSONB,
        system_tags JSONB,
        PRIMARY KEY(flow_id, run_number, step_name),
        FOREIGN KEY(flow_id, run_number) REFERENCES {1} (flow_id, run_number),
        UNIQUE(flow_id, run_id, step_name)
    )
    """.format(
        table_name, run_table_name
    )

    async def add_step(self, step_object: StepRow):
        dict = {
            "flow_id": step_object.flow_id,
            "run_number": str(step_object.run_number),
            "run_id": step_object.run_id,
            "step_name": step_object.step_name,
            "user_name": step_object.user_name,
            "ts_epoch": step_object.ts_epoch,
            "tags": json.dumps(step_object.tags),
            "system_tags": json.dumps(step_object.system_tags),
        }
        return await self.create_record(dict)

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


class AsyncTaskTablePostgres(AsyncPostgresTable):
    task_dict = {}
    step_to_task_dict = {}
    _current_count = 0
    _row_type = TaskRow
    table_name = "tasks_v3"
    keys = ["flow_id", "run_number", "run_id", "step_name", "task_id",
            "task_name", "user_name", "ts_epoch", "last_heartbeat_ts", "tags", "system_tags"]
    primary_keys = ["flow_id", "run_number", "step_name", "task_id"]
    # NOTE: There is a lot of unfortunate backwards compatibility support in the following join, due to
    # the older metadata service not recording separate metadata for task attempts. This is also the
    # reason why we must join through the artifacts table, instead of directly from metadata.
    joins = [
        """
        LEFT JOIN (
            SELECT
                task_ok.flow_id, task_ok.run_number, task_ok.step_name,
                task_ok.task_id, task_ok.attempt_id, task_ok.ts_epoch,
                task_ok.location,
                attempt.ts_epoch as started_at,
                COALESCE(attempt_ok.ts_epoch, done.ts_epoch, task_ok.ts_epoch, attempt.ts_epoch) as finished_at,
                attempt_ok.value::boolean as attempt_ok,
                foreach_stack.location as foreach_stack
            FROM {artifact_table} as task_ok
            LEFT JOIN {metadata_table} as attempt ON (
                task_ok.flow_id = attempt.flow_id AND
                task_ok.run_number = attempt.run_number AND
                task_ok.step_name = attempt.step_name AND
                task_ok.task_id = attempt.task_id AND
                attempt.field_name = 'attempt' AND
                task_ok.attempt_id = attempt.value::int
            )
            LEFT JOIN {metadata_table} as done ON (
                task_ok.flow_id = done.flow_id AND
                task_ok.run_number = done.run_number AND
                task_ok.step_name = done.step_name AND
                task_ok.task_id = done.task_id AND
                done.field_name = 'attempt-done' AND
                task_ok.attempt_id = done.value::int
            )
            LEFT JOIN {metadata_table} as attempt_ok ON (
                task_ok.flow_id = attempt_ok.flow_id AND
                task_ok.run_number = attempt_ok.run_number AND
                task_ok.step_name = attempt_ok.step_name AND
                task_ok.task_id = attempt_ok.task_id AND
                attempt_ok.field_name = 'attempt_ok' AND
                attempt_ok.tags ? CONCAT('attempt_id:', task_ok.attempt_id)
            )
            LEFT JOIN {artifact_table} as foreach_stack ON (
                task_ok.flow_id = foreach_stack.flow_id AND
                task_ok.run_number = foreach_stack.run_number AND
                task_ok.step_name = foreach_stack.step_name AND
                task_ok.task_id = foreach_stack.task_id AND
                foreach_stack.name = '_foreach_stack' AND
                task_ok.attempt_id = foreach_stack.attempt_id
            )
            WHERE task_ok.name = '_task_ok'
        ) AS attempt ON (
            {table_name}.flow_id = attempt.flow_id AND
            {table_name}.run_number = attempt.run_number AND
            {table_name}.step_name = attempt.step_name AND
            {table_name}.task_id = attempt.task_id
        )
        """.format(
            table_name=table_name,
            metadata_table="metadata_v3",
            artifact_table="artifact_v3"
        ),
    ]
    select_columns = ["tasks_v3.{0} AS {0}".format(k) for k in keys]
    join_columns = [
        "attempt.started_at as started_at",
        "attempt.finished_at as finished_at",
        "attempt.attempt_ok as attempt_ok",
        # If 'attempt_ok' is present, we can leave task_ok NULL since
        #   that is used to fetch the artifact value from remote location.
        # This process is performed at TaskRefiner (data_refiner.py)
        """
        (CASE
            WHEN attempt_ok IS NOT NULL
            THEN NULL
            ELSE attempt.location
        END) as task_ok
        """,
        """
        (CASE
            WHEN attempt_ok IS TRUE
            THEN 'completed'
            WHEN attempt_ok IS FALSE
            THEN 'failed'
            WHEN finished_at IS NOT NULL
                AND attempt_ok IS NULL
            THEN 'unknown'
            WHEN attempt.finished_at IS NOT NULL
            THEN 'completed'
            WHEN attempt.finished_at IS NULL
                AND {table_name}.last_heartbeat_ts IS NOT NULL
                AND @(extract(epoch from now())-{table_name}.last_heartbeat_ts)>{heartbeat_threshold}
            THEN 'failed'
            ELSE 'running'
        END) AS status
        """.format(
            table_name=table_name,
            heartbeat_threshold=HEARTBEAT_THRESHOLD
        ),
        """
        (CASE
            WHEN attempt.finished_at IS NULL AND {table_name}.last_heartbeat_ts IS NOT NULL
            THEN {table_name}.last_heartbeat_ts*1000-COALESCE(attempt.started_at, {table_name}.ts_epoch)
            WHEN attempt.finished_at IS NOT NULL
            THEN attempt.finished_at - COALESCE(attempt.started_at, {table_name}.ts_epoch)
            ELSE NULL
        END) AS duration
        """.format(
            table_name=table_name
        ),
        "COALESCE(attempt.attempt_id, 0) AS attempt_id",
        "attempt.foreach_stack as foreach_stack"
    ]
    step_table_name = AsyncStepTablePostgres.table_name
    _command = """
    CREATE TABLE {0} (
        flow_id VARCHAR(255) NOT NULL,
        run_number BIGINT NOT NULL,
        run_id VARCHAR(255),
        step_name VARCHAR(255) NOT NULL,
        task_id BIGSERIAL PRIMARY KEY,
        task_name VARCHAR(255),
        user_name VARCHAR(255),
        ts_epoch BIGINT NOT NULL,
        tags JSONB,
        system_tags JSONB,
        last_heartbeat_ts BIGINT,
        FOREIGN KEY(flow_id, run_number, step_name) REFERENCES {1} (flow_id, run_number, step_name),
        UNIQUE (flow_id, run_number, step_name, task_name)
    )
    """.format(
        table_name, step_table_name
    )

    async def add_task(self, task: TaskRow):
        # todo backfill run_number if missing?
        dict = {
            "flow_id": task.flow_id,
            "run_number": str(task.run_number),
            "run_id": task.run_id,
            "step_name": task.step_name,
            "task_name": task.task_name,
            "user_name": task.user_name,
            "ts_epoch": task.ts_epoch,
            "tags": json.dumps(task.tags),
            "system_tags": json.dumps(task.system_tags),
        }
        return await self.create_record(dict)

    async def get_tasks(self, flow_id: str, run_id: str, step_name: str):
        run_id_key, run_id_value = translate_run_key(run_id)
        filter_dict = {
            "flow_id": flow_id,
            run_id_key: run_id_value,
            "step_name": step_name,
        }
        return await self.get_records(filter_dict=filter_dict)

    async def get_task(self, flow_id: str, run_id: str, step_name: str,
                       task_id: str, expanded: bool = False):
        run_id_key, run_id_value = translate_run_key(run_id)
        task_id_key, task_id_value = translate_task_key(task_id)
        filter_dict = {
            "flow_id": flow_id,
            run_id_key: run_id_value,
            "step_name": step_name,
            task_id_key: task_id_value,
        }
        return await self.get_records(filter_dict=filter_dict,
                                      fetch_single=True, expanded=expanded)

    async def update_heartbeat(self, flow_id: str, run_id: str, step_name: str,
                               task_id: str):
        run_key, run_value = translate_run_key(run_id)
        task_key, task_value = translate_task_key(task_id)
        filter_dict = {"flow_id": flow_id,
                       run_key: str(run_value),
                       "step_name": step_name,
                       task_key: str(task_value)}
        set_dict = {
            "last_heartbeat_ts": int(datetime.datetime.utcnow().timestamp())
        }
        result = await self.update_row(filter_dict=filter_dict,
                                       update_dict=set_dict)

        body = {"wait_time_in_seconds": WAIT_TIME}

        return DBResponse(response_code=result.response_code,
                          body=json.dumps(body))


class AsyncMetadataTablePostgres(AsyncPostgresTable):
    metadata_dict = {}
    run_to_metadata_dict = {}
    _current_count = 0
    _row_type = MetadataRow
    table_name = "metadata_v3"
    task_table_name = AsyncTaskTablePostgres.table_name
    keys = ["flow_id", "run_number", "run_id", "step_name", "task_id", "task_name", "id",
            "field_name", "value", "type", "user_name", "ts_epoch", "tags", "system_tags"]
    primary_keys = ["flow_id", "run_number",
                    "step_name", "task_id", "field_name"]
    select_columns = keys
    _command = """
    CREATE TABLE {0} (
        flow_id VARCHAR(255),
        run_number BIGINT NOT NULL,
        run_id VARCHAR(255),
        step_name VARCHAR(255) NOT NULL,
        task_name VARCHAR(255),
        task_id BIGINT NOT NULL,
        id BIGSERIAL NOT NULL,
        field_name VARCHAR(255) NOT NULL,
        value TEXT NOT NULL,
        type VARCHAR(255) NOT NULL,
        user_name VARCHAR(255),
        ts_epoch BIGINT NOT NULL,
        tags JSONB,
        system_tags JSONB,
        PRIMARY KEY(id, flow_id, run_number, step_name, task_id, field_name)
    )
    """.format(
        table_name, task_table_name
    )

    async def add_metadata(
        self,
        flow_id,
        run_number,
        run_id,
        step_name,
        task_id,
        task_name,
        field_name,
        value,
        type,
        user_name,
        tags,
        system_tags,
        ts_epoch=None,
    ):
        dict = {
            "flow_id": flow_id,
            "run_number": str(run_number),
            "run_id": run_id,
            "step_name": step_name,
            "task_id": str(task_id),
            "task_name": task_name,
            "field_name": field_name,
            "value": value,
            "type": type,
            "user_name": user_name,
            "tags": json.dumps(tags),
            "system_tags": json.dumps(system_tags),
        }
        if ts_epoch: # Catch None and 0 just in case
            dict["ts_epoch"] = ts_epoch
        return await self.create_record(dict)

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


class AsyncArtifactTablePostgres(AsyncPostgresTable):
    artifact_dict = {}
    run_to_artifact_dict = {}
    step_to_artifact_dict = {}
    task_to_artifact_dict = {}
    current_count = 0
    _row_type = ArtifactRow
    table_name = "artifact_v3"
    task_table_name = AsyncTaskTablePostgres.table_name
    ordering = ["attempt_id DESC"]
    keys = ["flow_id", "run_number", "run_id", "step_name", "task_id", "task_name", "name", "location",
            "ds_type", "sha", "type", "content_type", "user_name", "attempt_id", "ts_epoch", "tags", "system_tags"]
    primary_keys = ["flow_id", "run_number",
                    "step_name", "task_id", "attempt_id", "name"]
    select_columns = keys
    _command = """
    CREATE TABLE {0} (
        flow_id VARCHAR(255) NOT NULL,
        run_number BIGINT NOT NULL,
        run_id VARCHAR(255),
        step_name VARCHAR(255) NOT NULL,
        task_id BIGINT NOT NULL,
        task_name VARCHAR(255),
        name VARCHAR(255) NOT NULL,
        location VARCHAR(255) NOT NULL,
        ds_type VARCHAR(255) NOT NULL,
        sha VARCHAR(255),
        type VARCHAR(255),
        content_type VARCHAR(255),
        user_name VARCHAR(255),
        attempt_id SMALLINT NOT NULL,
        ts_epoch BIGINT NOT NULL,
        tags JSONB,
        system_tags JSONB,
        PRIMARY KEY(flow_id, run_number, step_name, task_id, attempt_id, name)
    )
    """.format(
        table_name, task_table_name
    )

    async def add_artifact(
        self,
        flow_id,
        run_number,
        run_id,
        step_name,
        task_id,
        task_name,
        name,
        location,
        ds_type,
        sha,
        type,
        content_type,
        user_name,
        attempt_id,
        tags,
        system_tags,
        ts_epoch=None,
    ):
        dict = {
            "flow_id": flow_id,
            "run_number": str(run_number),
            "run_id": run_id,
            "step_name": step_name,
            "task_id": str(task_id),
            "task_name": task_name,
            "name": name,
            "location": location,
            "ds_type": ds_type,
            "sha": sha,
            "type": type,
            "content_type": content_type,
            "user_name": user_name,
            "attempt_id": str(attempt_id),
            "tags": json.dumps(tags),
            "system_tags": json.dumps(system_tags),
        }
        if ts_epoch: # Catch None and 0 just in case
            dict["ts_epoch"] = ts_epoch
        return await self.create_record(dict)

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

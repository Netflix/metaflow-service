import psycopg2
import psycopg2.extras
import os
import aiopg
import json
import math
import re
import time
from services.utils import logging, DBType
from typing import List, Tuple

from .db_utils import (
    DBResponse,
    DBPagination,
    aiopg_exception_handling,
    get_db_ts_epoch_str,
    translate_run_key,
    translate_task_key,
    new_heartbeat_ts,
)
from .models import FlowRow, RunRow, StepRow, TaskRow, MetadataRow, ArtifactRow
from services.utils import DBConfiguration, USE_SEPARATE_READER_POOL

from services.data.service_configs import (
    max_connection_retires,
    connection_retry_wait_time_seconds,
)

AIOPG_ECHO = os.environ.get("AIOPG_ECHO", 0) == "1"

WAIT_TIME = 10

# Create database triggers automatically, disabled by default
# Enable with env variable `DB_TRIGGER_CREATE=1`
DB_TRIGGER_CREATE = os.environ.get("DB_TRIGGER_CREATE", 0) == "1"

# Configure DB Table names. Custom names can be supplied through environment variables,
# in case the deployment differs from the default naming scheme from the supplied migrations.
FLOW_TABLE_NAME = os.environ.get("DB_TABLE_NAME_FLOWS", "flows_v3")
RUN_TABLE_NAME = os.environ.get("DB_TABLE_NAME_RUNS", "runs_v3")
STEP_TABLE_NAME = os.environ.get("DB_TABLE_NAME_STEPS", "steps_v3")
TASK_TABLE_NAME = os.environ.get("DB_TABLE_NAME_TASKS", "tasks_v3")
METADATA_TABLE_NAME = os.environ.get("DB_TABLE_NAME_METADATA", "metadata_v3")
ARTIFACT_TABLE_NAME = os.environ.get("DB_TABLE_NAME_ARTIFACT", "artifact_v3")
DB_SCHEMA_NAME = os.environ.get("DB_SCHEMA_NAME", "public")

# Time before a run with a heartbeat is considered inactive (and thus failed).
# Default 6 minutes (in seconds). Kept in sync with the ui_backend definition so
# that status filtering here agrees with the status the UI reports.
RUN_INACTIVE_CUTOFF_TIME = int(os.environ.get("RUN_INACTIVE_CUTOFF_TIME", 60 * 6))

operator_match = re.compile("([^:]*):([=><]+)$")

# use a ddmmyyy timestamp as the version for triggers
TRIGGER_VERSION = "05092024"
TRIGGER_NAME_PREFIX = "notify_ui"


class _AsyncPostgresDB(object):
    connection = None
    flow_table_postgres = None
    run_table_postgres = None
    step_table_postgres = None
    task_table_postgres = None
    artifact_table_postgres = None
    metadata_table_postgres = None

    pool = None
    reader_pool = None
    db_conf: DBConfiguration = None

    def __init__(self, name="global"):
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
        retries = max_connection_retires
        for i in range(retries):
            try:
                self.pool = await aiopg.create_pool(
                    db_conf.get_dsn(),
                    minsize=db_conf.pool_min,
                    maxsize=db_conf.pool_max,
                    timeout=db_conf.timeout,
                    pool_recycle=10 * db_conf.timeout,
                    echo=AIOPG_ECHO,
                )

                self.reader_pool = (
                    await aiopg.create_pool(
                        db_conf.get_dsn(type=DBType.READER),
                        minsize=db_conf.pool_min,
                        maxsize=db_conf.pool_max,
                        timeout=db_conf.timeout,
                        pool_recycle=10 * db_conf.timeout,
                        echo=AIOPG_ECHO,
                    )
                    if USE_SEPARATE_READER_POOL
                    else self.pool
                )

                for table in self.tables:
                    await table._init(create_triggers=create_triggers)

                if USE_SEPARATE_READER_POOL:
                    self.logger.info(
                        "Writer Connection established.\n"
                        "   Pool min: {pool_min} max: {pool_max}\n".format(
                            pool_min=self.pool.minsize, pool_max=self.pool.maxsize
                        )
                    )

                    self.logger.info(
                        "Reader Connection established.\n"
                        "   Pool min: {pool_min} max: {pool_max}\n".format(
                            pool_min=self.reader_pool.minsize,
                            pool_max=self.reader_pool.maxsize,
                        )
                    )
                else:
                    self.logger.info(
                        "Connection established.\n"
                        "   Pool min: {pool_min} max: {pool_max}\n".format(
                            pool_min=self.pool.minsize, pool_max=self.pool.maxsize
                        )
                    )

                break  # Break the retry loop
            except Exception as e:
                self.logger.exception("Exception occurred")
                if retries - i <= 1:
                    raise e
                time.sleep(connection_retry_wait_time_seconds)

    def get_table_by_name(self, table_name: str):
        for table in self.tables:
            if table.table_name == table_name:
                return table
        return None

    async def get_run_ids(self, flow_id: str, run_id: str):
        run = await self.run_table_postgres.get_run(flow_id, run_id, expanded=True)
        return run.body["run_number"], run.body["run_id"]

    async def get_task_ids(
        self, flow_id: str, run_id: str, step_name: str, task_name: str
    ):

        task = await self.task_table_postgres.get_task(
            flow_id, run_id, step_name, task_name, expanded=True
        )
        return task.body["task_id"], task.body["task_name"]


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
    trigger_keys: List[str] = None
    trigger_operations: List[str] = ["INSERT", "UPDATE", "DELETE"]
    trigger_conditions: List[str] = None
    ordering: List[str] = None
    joins: List[str] = None
    select_columns: List[str] = keys
    join_columns: List[str] = None
    _insert_command = None
    _filters = None
    _base_query = "SELECT {0} from"
    _row_type = None

    def __init__(self, db: _AsyncPostgresDB = None):
        self.db = db
        if self.table_name is None:
            raise NotImplementedError("need to specify table name")

    async def _init(self, create_triggers: bool):
        if create_triggers:
            self.db.logger.info(
                "Setting up notify trigger for {table_name}\n   Keys: {keys}".format(
                    table_name=self.table_name, keys=self.trigger_keys
                )
            )
            await PostgresUtils.cleanup_triggers(db=self.db, table_name=self.table_name)
            if self.trigger_keys and self.trigger_operations:
                await PostgresUtils.setup_trigger_notify(
                    db=self.db,
                    table_name=self.table_name,
                    keys=self.trigger_keys,
                    operations=self.trigger_operations,
                    conditions=self.trigger_conditions,
                )

    async def get_records(
        self,
        filter_dict={},
        fetch_single=False,
        ordering: List[str] = None,
        limit: int = 0,
        expanded=False,
        cur: aiopg.Cursor = None,
    ) -> DBResponse:
        conditions = []
        values = []
        for col_name, col_val in filter_dict.items():
            conditions.append("{} = %s".format(col_name))
            values.append(col_val)

        response, _ = await self.find_records(
            conditions=conditions,
            values=values,
            fetch_single=fetch_single,
            order=ordering,
            limit=limit,
            expanded=expanded,
            cur=cur,
        )
        return response

    async def find_records(
        self,
        conditions: List[str] = None,
        values=[],
        fetch_single=False,
        limit: int = 0,
        offset: int = 0,
        order: List[str] = None,
        expanded=False,
        enable_joins=False,
        cur: aiopg.Cursor = None,
    ) -> Tuple[DBResponse, DBPagination]:
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

        select_sql = sql_template.format(
            keys=",".join(
                self.select_columns
                + (self.join_columns if enable_joins and self.join_columns else [])
            ),
            table_name=self.table_name,
            joins=(
                " ".join(self.joins) if enable_joins and self.joins is not None else ""
            ),
            where="WHERE {}".format(" AND ".join(conditions)) if conditions else "",
            order_by="ORDER BY {}".format(", ".join(order)) if order else "",
            limit="LIMIT {}".format(limit) if limit else "",
            offset="OFFSET {}".format(offset) if offset else "",
        ).strip()

        return await self.execute_sql(
            select_sql=select_sql,
            values=values,
            fetch_single=fetch_single,
            expanded=expanded,
            limit=limit,
            offset=offset,
            cur=cur,
        )

    async def execute_sql(
        self,
        select_sql: str,
        values=[],
        fetch_single=False,
        expanded=False,
        limit: int = 0,
        offset: int = 0,
        cur: aiopg.Cursor = None,
        serialize: bool = True,
    ) -> Tuple[DBResponse, DBPagination]:
        async def _execute_on_cursor(_cur):
            await _cur.execute(select_sql, values)

            rows = []
            records = await _cur.fetchall()
            if serialize:
                for record in records:
                    # pylint-initial-ignore: Lack of __init__ makes this too hard for pylint
                    # pylint: disable=not-callable
                    row = self._row_type(**record)
                    rows.append(row.serialize(expanded))
            else:
                rows = records

            count = len(rows)

            # Will raise IndexError in case fetch_single=True and there's no results
            body = rows[0] if fetch_single else rows

            pagination = DBPagination(
                limit=limit,
                offset=offset,
                count=count,
                page=math.floor(int(offset) / max(int(limit), 1)) + 1,
                # Used for cursor when has_next (records == limit + 1); ignored otherwise
                next_cursor_record=(
                    records[-2] if records is not None and len(records) > 1 else None
                ),
            )
            return body, pagination

        try:
            if cur:
                # if we are using the passed in cursor, we allow any errors to be managed by cursor owner
                body, pagination = await _execute_on_cursor(cur)
                return DBResponse(response_code=200, body=body), pagination
            else:
                db_pool = (
                    self.db.reader_pool if USE_SEPARATE_READER_POOL else self.db.pool
                )
                with await db_pool.cursor(
                    cursor_factory=psycopg2.extras.DictCursor
                ) as cur:
                    body, pagination = await _execute_on_cursor(cur)
                    cur.close()
                    return DBResponse(response_code=200, body=body), pagination
        except IndexError as error:
            return aiopg_exception_handling(error), None
        except (Exception, psycopg2.DatabaseError) as error:
            self.db.logger.exception("Exception occurred")
            return aiopg_exception_handling(error), None

    async def create_record(self, record_dict):
        # note: need to maintain order
        cols = []
        values = []
        for col_name, col_val in record_dict.items():
            cols.append(col_name)
            values.append(col_val)

        # add create ts
        cols.append("ts_epoch")
        values.append(get_db_ts_epoch_str())

        str_format = []
        for _ in cols:
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
            with await self.db.pool.cursor(
                cursor_factory=psycopg2.extras.DictCursor
            ) as cur:

                await cur.execute(insert_sql, tuple(values))
                records = await cur.fetchall()
                record = records[0]
                filtered_record = {}
                for key, value in record.items():
                    if key in self.keys:
                        filtered_record[key] = value
                response_body = self._row_type(
                    **filtered_record
                ).serialize()  # pylint: disable=not-callable
                # todo make sure connection is closed even with error
                cur.close()
            return DBResponse(response_code=200, body=response_body)
        except (Exception, psycopg2.DatabaseError) as error:
            self.db.logger.exception("Exception occurred")
            return aiopg_exception_handling(error)

    async def run_in_transaction_with_serializable_isolation_level(self, fun):
        try:
            with await self.db.pool.cursor(
                cursor_factory=psycopg2.extras.DictCursor,
            ) as cur:
                async with cur.begin():
                    await cur.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
                    res = await fun(cur)
                cur.close()  # is this really needed? TODO
                return res
        except psycopg2.errors.SerializationFailure:
            # See https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/409
            return DBResponse(
                response_code=409,
                body="Conflicting concurrent tag mutation, please retry",
            )
        except (Exception, psycopg2.DatabaseError) as error:
            self.db.logger.exception("Exception occurred")
            return aiopg_exception_handling(error)

    async def update_row(
        self, filter_dict={}, update_dict={}, cur: aiopg.Cursor = None
    ):
        # Build a single values list in the same order as SQL placeholders:
        # UPDATE ... SET col1=%s, col2=%s WHERE col3=%s AND col4=%s
        values = []

        # generate SET clause (values come first in SQL)
        sets = []
        for col_name, col_val in update_dict.items():
            sets.append("%s = %%s" % col_name)
            values.append(col_val)
        set_clause = ", ".join(sets)

        # generate WHERE clause (values come second in SQL)
        filters = []
        for col_name, col_val in filter_dict.items():
            operator = "="
            find_operator = operator_match.match(col_name)
            if find_operator:
                col_name = find_operator.group(1)
                operator = find_operator.group(2)
                filters.append(
                    "(%s IS NULL or %s %s %%s)" % (col_name, col_name, operator)
                )
            else:
                filters.append("%s %s %%s" % (col_name, operator))
            values.append(col_val)
        where_clause = " and ".join(filters)

        update_sql = """
                UPDATE {0} SET {1} WHERE {2};
        """.format(self.table_name, set_clause, where_clause)
        values = tuple(values)

        async def _execute_update_on_cursor(_cur):
            await _cur.execute(update_sql, values)
            if _cur.rowcount < 1:
                return DBResponse(response_code=404, body={"msg": "could not find row"})
            if _cur.rowcount > 1:
                return DBResponse(response_code=500, body={"msg": "duplicate rows"})
            return DBResponse(response_code=200, body={"rowcount": _cur.rowcount})

        if cur:
            return await _execute_update_on_cursor(cur)
        try:
            with await self.db.pool.cursor(
                cursor_factory=psycopg2.extras.DictCursor
            ) as cur:
                db_response = await _execute_update_on_cursor(cur)
                cur.close()
                return db_response
        except (Exception, psycopg2.DatabaseError) as error:
            self.db.logger.exception("Exception occurred")
            return aiopg_exception_handling(error)


class PostgresUtils(object):
    @staticmethod
    async def create_trigger_if_missing(
        db: _AsyncPostgresDB, table_name, trigger_name, commands=[]
    ):
        "executes the commands only if a trigger with the given name does not already exist on the table"
        with await db.pool.cursor() as cur:
            try:
                await cur.execute(
                    """
                    SELECT *
                    FROM information_schema.triggers
                    WHERE event_object_table = %s
                    AND trigger_name = %s
                    """,
                    (table_name, trigger_name),
                )
                trigger_exist = bool(cur.rowcount)
                if not trigger_exist:
                    for command in commands:
                        await cur.execute(command)
            finally:
                cur.close()

    @staticmethod
    async def cleanup_triggers(db: _AsyncPostgresDB, table_name, schema=DB_SCHEMA_NAME):
        "Cleans up old versions of table triggers"
        with await db.pool.cursor() as cur:
            try:
                await cur.execute(
                    """
                    SELECT DISTINCT trigger_name
                    FROM information_schema.triggers
                    WHERE event_object_table = %s
                    AND trigger_schema = %s
                    """,
                    [table_name, schema],
                )
                results = await cur.fetchall()

                triggers_to_cleanup = [
                    res[0]
                    for res in results
                    if res[0].startswith(TRIGGER_NAME_PREFIX)
                    and TRIGGER_VERSION not in res[0]
                ]
                if triggers_to_cleanup:
                    logging.getLogger("TriggerSetup").info(
                        "Cleaning up old triggers: %s" % triggers_to_cleanup
                    )
                    commands = []
                    for trigger_name in triggers_to_cleanup:
                        commands += [
                            (
                                f"DROP TRIGGER IF EXISTS {trigger_name} ON {schema}.{table_name}"
                            ),
                            (f"DROP FUNCTION IF EXISTS {schema}.{trigger_name}"),
                        ]

                    for command in commands:
                        await cur.execute(command)
            finally:
                cur.close()

    @staticmethod
    async def setup_trigger_notify(
        db: _AsyncPostgresDB,
        table_name,
        keys: List[str] = None,
        schema=DB_SCHEMA_NAME,
        operations: List[str] = None,
        conditions: List[str] = None,
    ):
        if not keys:
            pass

        name_prefix = "%s_%s" % (TRIGGER_NAME_PREFIX, TRIGGER_VERSION)
        operations = operations
        _commands = [
            """
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
            )
        ]

        _commands += [
            """
            CREATE TRIGGER {prefix}_{table} AFTER {events} ON {schema}.{table}
                FOR EACH ROW {conditions} EXECUTE PROCEDURE {schema}.{prefix}_{table}();
            """.format(
                schema=schema,
                prefix=name_prefix,
                table=table_name,
                events=" OR ".join(operations),
                conditions="WHEN (%s)" % " OR ".join(conditions) if conditions else "",
            )
        ]

        # This enables trigger on both replica and non-replica mode
        _commands += [
            "ALTER TABLE {schema}.{table} ENABLE ALWAYS TRIGGER {prefix}_{table};".format(
                schema=schema, prefix=name_prefix, table=table_name
            )
        ]

        # NOTE: Only try to setup triggers if they do not already exist.
        # This will require a table level lock so it should be performed during initial setup at off-peak hours.
        await PostgresUtils.create_trigger_if_missing(
            db=db,
            table_name=table_name,
            trigger_name="{}_{}".format(name_prefix, table_name),
            commands=_commands,
        )


class AsyncFlowTablePostgres(AsyncPostgresTable):
    flow_dict = {}
    table_name = FLOW_TABLE_NAME
    keys = ["flow_id", "user_name", "ts_epoch", "tags", "system_tags"]
    primary_keys = ["flow_id"]
    trigger_keys = primary_keys
    select_columns = keys
    _row_type = FlowRow

    async def add_flow(self, flow: FlowRow):
        dict = {
            "flow_id": flow.flow_id,
            "user_name": flow.user_name,
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
    table_name = RUN_TABLE_NAME
    keys = [
        "flow_id",
        "run_number",
        "run_id",
        "user_name",
        "ts_epoch",
        "last_heartbeat_ts",
        "tags",
        "system_tags",
    ]
    primary_keys = ["flow_id", "run_number"]
    trigger_keys = primary_keys + ["last_heartbeat_ts"]
    flow_table_name = AsyncFlowTablePostgres.table_name

    # Derived run status (running/completed/failed). This mirrors the definition in
    # ui_backend_service so filtering agrees with what the UI shows. The two lateral
    # joins below pull the 'end' step's attempt metadata that the status depends on.
    # Only used when joins are enabled (i.e. when filtering by status), so the plain
    # get_all_runs path stays a simple, join-free query.
    joins = [
        """
        LEFT JOIN LATERAL (
            SELECT
                ts_epoch,
                (CASE
                    WHEN pg_typeof(value)='jsonb'::regtype
                    THEN value::jsonb->>0
                    ELSE value::text
                END)::boolean as value
            FROM {metadata_table} as attempt_ok
            WHERE
                {table_name}.flow_id = attempt_ok.flow_id AND
                {table_name}.run_number = attempt_ok.run_number AND
                attempt_ok.step_name = 'end' AND
                attempt_ok.field_name = 'attempt_ok'
            ORDER BY ts_epoch DESC
            LIMIT 1
        ) as end_attempt_ok ON true
        """.format(table_name=RUN_TABLE_NAME, metadata_table=METADATA_TABLE_NAME),
        """
        LEFT JOIN LATERAL (
            SELECT ts_epoch
            FROM {metadata_table} as attempt
            WHERE
                {table_name}.flow_id = attempt.flow_id AND
                {table_name}.run_number = attempt.run_number AND
                attempt.step_name = 'end' AND
                attempt.field_name = 'attempt' AND
                end_attempt_ok.value IS FALSE
            ORDER BY ts_epoch DESC
            LIMIT 1
        ) as end_attempt ON true
        """.format(table_name=RUN_TABLE_NAME, metadata_table=METADATA_TABLE_NAME),
    ]

    join_columns = [
        """
        (CASE
            WHEN end_attempt IS NOT NULL
                AND end_attempt_ok.ts_epoch < end_attempt.ts_epoch
            THEN 'running'
            WHEN end_attempt_ok IS NOT NULL AND end_attempt_ok.value IS TRUE
            THEN 'completed'
            WHEN end_attempt_ok IS NOT NULL AND end_attempt_ok.value IS FALSE
            THEN 'failed'
            WHEN {table_name}.last_heartbeat_ts IS NOT NULL
                AND @(extract(epoch from now())-{table_name}.last_heartbeat_ts)<={cutoff}
            THEN 'running'
            ELSE 'failed'
        END) AS status
        """.format(table_name=RUN_TABLE_NAME, cutoff=RUN_INACTIVE_CUTOFF_TIME),
    ]

    @property
    def select_columns(self):
        # NOTE: a function scope is used so the comprehension can access table_name.
        # Base columns are qualified so they stay unambiguous once the status joins are in
        # play. 'user' is the verified owner: NULL unless a 'user:<name>' system tag is
        # present (e.g. AWS Step Functions runs have none). Mirrors the ui_backend run
        # table so filtering agrees with what the UI labels as the owner.
        return [
            "{table_name}.{col} AS {col}".format(table_name=self.table_name, col=k)
            for k in self.keys
        ] + [
            """
                (CASE
                    WHEN system_tags ? ('user:' || user_name)
                    THEN user_name
                    ELSE NULL
                END) AS user"""
        ]

    async def add_run(self, run: RunRow, fill_heartbeat: bool = False):
        dict = {
            "flow_id": run.flow_id,
            "user_name": run.user_name,
            "tags": json.dumps(run.tags),
            "system_tags": json.dumps(run.system_tags),
            "run_id": run.run_id,
            "last_heartbeat_ts": str(new_heartbeat_ts()) if fill_heartbeat else None,
        }
        return await self.create_record(dict)

    async def get_run(
        self,
        flow_id: str,
        run_id: str,
        expanded: bool = False,
        cur: aiopg.Cursor = None,
    ):
        key, value = translate_run_key(run_id)
        filter_dict = {"flow_id": flow_id, key: str(value)}
        return await self.get_records(
            filter_dict=filter_dict, fetch_single=True, expanded=expanded, cur=cur
        )

    async def get_all_runs(self, flow_id: str):
        filter_dict = {"flow_id": flow_id}

        return await self.get_records(filter_dict=filter_dict)

    async def get_filtered_runs(
        self,
        conditions: List[str],
        values: list,
        enable_joins: bool = False,
        limit: int = 0,
        offset: int = 0,
        order: List[str] = None,
    ):
        # conditions/values come pre-built from the shared filter grammar
        # (services/data/filter_grammar.py), with a leading flow_id predicate. The
        # caller sets enable_joins=True only when a status filter is present, since
        # status is the only derived column needing the lateral joins; user, tags and
        # ts_epoch filter base (or join-free derived) columns.
        if len(conditions) <= 1 and not enable_joins:
            # nothing beyond the flow itself: keep the original join-free query so an
            # unfiltered listing stays byte-identical to before.
            flow_id = values[0] if values else None
            return await self.get_records(
                filter_dict={"flow_id": flow_id}, ordering=order, limit=limit
            )

        response, _ = await self.find_records(
            conditions=conditions,
            values=values,
            limit=limit,
            offset=offset,
            order=order,
            enable_joins=enable_joins,
        )
        return response

    async def get_filtered_runs_paginated(
        self,
        conditions: List[str],
        values: list,
        enable_joins: bool = False,
        limit: int = 50,
        cur_ts: int = None,
        cur_run: int = None,
    ) -> Tuple[DBResponse, DBPagination]:
        # Cursor pagination over the filtered result set. Keys off
        # (ts_epoch, run_number) DESC, identical to the cursor pagination added for the
        # unfiltered endpoint (#482), so a client can page filtered runs the same way.
        # The filter conditions/values come pre-built from the grammar; we append the
        # keyset predicate so paging walks the ordered, filtered set deterministically.
        conditions = list(conditions)
        values = list(values)
        if cur_ts is not None and cur_run is not None:
            conditions.append("(ts_epoch, run_number) < (%s,%s)")
            values.extend([cur_ts, cur_run])

        order = ["ts_epoch DESC", "run_number DESC"]
        # fetch one extra row to detect whether a further page exists
        cur_limit = limit + 1

        response, pagination = await self.find_records(
            conditions=conditions,
            values=values,
            limit=cur_limit,
            order=order,
            enable_joins=enable_joins,
        )

        if len(response.body) > limit:
            response = response._replace(body=response.body[:limit])
        else:
            pagination = pagination._replace(next_cursor_record=None)

        pagination = pagination._replace(limit=str(limit))

        return response, pagination

    async def update_heartbeat(self, flow_id: str, run_id: str):
        run_key, run_value = translate_run_key(run_id)
        new_hb = new_heartbeat_ts()
        filter_dict = {
            "flow_id": flow_id,
            run_key: str(run_value),
            "last_heartbeat_ts:<=": new_hb - WAIT_TIME,
        }
        set_dict = {"last_heartbeat_ts": new_hb}
        result = await self.update_row(filter_dict=filter_dict, update_dict=set_dict)
        body = {"wait_time_in_seconds": WAIT_TIME}

        return DBResponse(response_code=result.response_code, body=json.dumps(body))

    async def update_run_tags(
        self, flow_id: str, run_id: str, run_tags: list, cur: aiopg.Cursor = None
    ):
        run_key, run_value = translate_run_key(run_id)
        filter_dict = {"flow_id": flow_id, run_key: str(run_value)}

        set_dict = {"tags": json.dumps(run_tags)}
        return await self.update_row(
            filter_dict=filter_dict, update_dict=set_dict, cur=cur
        )


class AsyncStepTablePostgres(AsyncPostgresTable):
    step_dict = {}
    run_to_step_dict = {}
    _row_type = StepRow
    table_name = STEP_TABLE_NAME
    keys = [
        "flow_id",
        "run_number",
        "run_id",
        "step_name",
        "user_name",
        "ts_epoch",
        "tags",
        "system_tags",
    ]
    primary_keys = ["flow_id", "run_number", "step_name"]
    trigger_keys = primary_keys
    select_columns = keys
    run_table_name = AsyncRunTablePostgres.table_name

    async def add_step(self, step_object: StepRow):
        dict = {
            "flow_id": step_object.flow_id,
            "run_number": str(step_object.run_number),
            "run_id": step_object.run_id,
            "step_name": step_object.step_name,
            "user_name": step_object.user_name,
            "tags": json.dumps(step_object.tags),
            "system_tags": json.dumps(step_object.system_tags),
        }
        return await self.create_record(dict)

    async def get_steps(self, flow_id: str, run_id: str):
        run_id_key, run_id_value = translate_run_key(run_id)
        filter_dict = {"flow_id": flow_id, run_id_key: run_id_value}
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
    table_name = TASK_TABLE_NAME
    keys = [
        "flow_id",
        "run_number",
        "run_id",
        "step_name",
        "task_id",
        "task_name",
        "user_name",
        "ts_epoch",
        "last_heartbeat_ts",
        "tags",
        "system_tags",
    ]
    primary_keys = ["flow_id", "run_number", "step_name", "task_id"]
    trigger_keys = primary_keys
    select_columns = keys
    step_table_name = AsyncStepTablePostgres.table_name

    async def add_task(self, task: TaskRow, fill_heartbeat=False):
        # todo backfill run_number if missing?
        dict = {
            "flow_id": task.flow_id,
            "run_number": str(task.run_number),
            "run_id": task.run_id,
            "step_name": task.step_name,
            "task_name": task.task_name,
            "user_name": task.user_name,
            "tags": json.dumps(task.tags),
            "system_tags": json.dumps(task.system_tags),
            "last_heartbeat_ts": str(new_heartbeat_ts()) if fill_heartbeat else None,
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

    async def get_task(
        self,
        flow_id: str,
        run_id: str,
        step_name: str,
        task_id: str,
        expanded: bool = False,
    ):
        run_id_key, run_id_value = translate_run_key(run_id)
        task_id_key, task_id_value = translate_task_key(task_id)
        filter_dict = {
            "flow_id": flow_id,
            run_id_key: run_id_value,
            "step_name": step_name,
            task_id_key: task_id_value,
        }
        return await self.get_records(
            filter_dict=filter_dict, fetch_single=True, expanded=expanded
        )

    async def update_heartbeat(
        self, flow_id: str, run_id: str, step_name: str, task_id: str
    ):
        run_key, run_value = translate_run_key(run_id)
        task_key, task_value = translate_task_key(task_id)
        new_hb = new_heartbeat_ts()
        filter_dict = {
            "flow_id": flow_id,
            run_key: str(run_value),
            "step_name": step_name,
            task_key: str(task_value),
            "last_heartbeat_ts:<=": new_hb - WAIT_TIME,
        }
        set_dict = {"last_heartbeat_ts": new_hb}
        result = await self.update_row(filter_dict=filter_dict, update_dict=set_dict)

        body = {"wait_time_in_seconds": WAIT_TIME}

        return DBResponse(response_code=result.response_code, body=json.dumps(body))


class AsyncMetadataTablePostgres(AsyncPostgresTable):
    metadata_dict = {}
    run_to_metadata_dict = {}
    _current_count = 0
    _row_type = MetadataRow
    table_name = METADATA_TABLE_NAME
    keys = [
        "flow_id",
        "run_number",
        "run_id",
        "step_name",
        "task_id",
        "task_name",
        "id",
        "field_name",
        "value",
        "type",
        "user_name",
        "ts_epoch",
        "tags",
        "system_tags",
    ]
    primary_keys = ["flow_id", "run_number", "step_name", "task_id", "field_name"]
    trigger_keys = [
        "flow_id",
        "run_number",
        "step_name",
        "task_id",
        "field_name",
        "value",
        "tags",
    ]
    trigger_operations = ["INSERT"]
    select_columns = keys

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
        return await self.create_record(dict)

    async def get_metadata_in_runs(self, flow_id: str, run_id: str):
        run_id_key, run_id_value = translate_run_key(run_id)
        filter_dict = {"flow_id": flow_id, run_id_key: run_id_value}
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

    async def get_filtered_task_pathspecs(
        self, flow_id: str, run_id: str, step_name: str, field_name: str, pattern: str
    ):
        """
        Returns a list of task pathspecs that match the given field_name and regexp pattern for the value
        """
        run_id_key, run_id_value = translate_run_key(run_id)
        filter_dict = {
            "flow_id": flow_id,
            run_id_key: run_id_value,
            "step_name": step_name,
        }
        conditions = [f"{k} = %s" for k, v in filter_dict.items() if v is not None]
        values = [v for k, v in filter_dict.items() if v is not None]

        if field_name:
            conditions.append("field_name = %s")
            values.append(field_name)

        if pattern:
            conditions.append("regexp_match(value, %s) IS NOT NULL")
            values.append(pattern)

        # We must return distinct task pathspecs, so we construct the select statement by hand
        sql_template = """
        SELECT DISTINCT {select_columns} FROM (
            SELECT
                {keys}
            FROM {table_name}
        ) T
        {where}
        {order_by}
        """

        select_sql = sql_template.format(
            keys=",".join(self.select_columns),
            table_name=self.table_name,
            where="WHERE {}".format(" AND ".join(conditions)),
            order_by="ORDER BY task_id",
            select_columns=",".join(
                ["flow_id, run_number, run_id, step_name, task_name, task_id"]
            ),
        ).strip()

        db_response, pagination = await self.execute_sql(
            select_sql=select_sql, values=values, serialize=False
        )

        # flatten the ids in the response
        def _format_id(row):
            flow_id, run_number, run_id, step_name, task_name, task_id = row
            # pathspec
            return (
                f"{flow_id}/{run_id or run_number}/{step_name}/{task_name or task_id}"
            )

        flattened_response = DBResponse(
            body=[_format_id(row) for row in db_response.body],
            response_code=db_response.response_code,
        )
        return flattened_response, pagination


class AsyncArtifactTablePostgres(AsyncPostgresTable):
    artifact_dict = {}
    run_to_artifact_dict = {}
    step_to_artifact_dict = {}
    task_to_artifact_dict = {}
    current_count = 0
    _row_type = ArtifactRow
    table_name = ARTIFACT_TABLE_NAME
    ordering = ["attempt_id DESC"]
    keys = [
        "flow_id",
        "run_number",
        "run_id",
        "step_name",
        "task_id",
        "task_name",
        "name",
        "location",
        "ds_type",
        "sha",
        "type",
        "content_type",
        "user_name",
        "attempt_id",
        "ts_epoch",
        "tags",
        "system_tags",
    ]
    primary_keys = [
        "flow_id",
        "run_number",
        "step_name",
        "task_id",
        "attempt_id",
        "name",
    ]
    trigger_keys = primary_keys
    trigger_operations = ["INSERT"]
    select_columns = keys

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
        return await self.create_record(dict)

    async def get_artifacts_in_runs(self, flow_id: str, run_id: int):
        run_id_key, run_id_value = translate_run_key(run_id)
        filter_dict = {
            "flow_id": flow_id,
            run_id_key: run_id_value,
        }
        return await self.get_records(filter_dict=filter_dict, ordering=self.ordering)

    async def get_artifact_in_steps(self, flow_id: str, run_id: int, step_name: str):
        run_id_key, run_id_value = translate_run_key(run_id)
        filter_dict = {
            "flow_id": flow_id,
            run_id_key: run_id_value,
            "step_name": step_name,
        }
        return await self.get_records(filter_dict=filter_dict, ordering=self.ordering)

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
        return await self.get_records(filter_dict=filter_dict, ordering=self.ordering)

    async def get_artifact(
        self, flow_id: str, run_id: int, step_name: str, task_id: int, name: str
    ):
        # Return the artifact metadata for the latest attempt of the task.
        #
        # The quirk here is that different attempts may have different sets of
        # artifacts. That is, if artifact "foo" was set in attempt N, that
        # doesn't mean it was set in attempt N+1, and vice versa.
        #
        # To get the artifact value for the "latest" attempt, we first find
        # the latest attempt_id by querying the artifacts table for the
        # artifact that always exists for every attempt (the artifact called
        # 'name', containing the flow name), then use that attempt_id to get
        # the artifact we're interested in.
        run_id_key, run_id_value = translate_run_key(run_id)
        task_id_key, task_id_value = translate_task_key(task_id)
        filter_dict = {
            "flow_id": flow_id,
            run_id_key: run_id_value,
            "step_name": step_name,
            task_id_key: task_id_value,
            '"name"': "name",
        }
        name_record = await self.get_records(
            filter_dict=filter_dict, fetch_single=True, ordering=self.ordering
        )

        return await self.get_artifact_by_attempt(
            flow_id,
            run_id,
            step_name,
            task_id,
            name,
            name_record.body.get("attempt_id", 0),
        )

    async def get_artifact_by_attempt(
        self,
        flow_id: str,
        run_id: int,
        step_name: str,
        task_id: int,
        name: str,
        attempt: int,
    ):

        run_id_key, run_id_value = translate_run_key(run_id)
        task_id_key, task_id_value = translate_task_key(task_id)
        filter_dict = {
            "flow_id": flow_id,
            run_id_key: run_id_value,
            "step_name": step_name,
            task_id_key: task_id_value,
            '"name"': name,
            '"attempt_id"': attempt,
        }
        return await self.get_records(
            filter_dict=filter_dict, fetch_single=True, ordering=self.ordering
        )

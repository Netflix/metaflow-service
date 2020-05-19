import psycopg2
import psycopg2.extras
import os
import aiopg
import json

from .db_utils import DBResponse, aiopg_exception_handling, \
    get_db_ts_epoch_str, translate_run_key, translate_task_key
from .models import FlowRow, RunRow, StepRow, TaskRow, MetadataRow, ArtifactRow


class AsyncPostgresDB(object):
    connection = None
    __instance = None
    flow_table_postgres = None
    run_table_postgres = None
    step_table_postgres = None
    task_table_postgres = None
    artifact_table_postgres = None
    metadata_table_postgres = None

    pool = None

    @staticmethod
    def get_instance():
        if AsyncPostgresDB.__instance is None:
            AsyncPostgresDB()
        return AsyncPostgresDB.__instance

    def __init__(self):
        if self.__instance is not None:
            return

        AsyncPostgresDB.__instance = self

        tables = []
        self.flow_table_postgres = AsyncFlowTablePostgres()
        self.run_table_postgres = AsyncRunTablePostgres()
        self.step_table_postgres = AsyncStepTablePostgres()
        self.task_table_postgres = AsyncTaskTablePostgres()
        self.artifact_table_postgres = AsyncArtifactTablePostgres()
        self.metadata_table_postgres = AsyncMetadataTablePostgres()
        tables.append(self.flow_table_postgres)
        tables.append(self.run_table_postgres)
        tables.append(self.step_table_postgres)
        tables.append(self.task_table_postgres)
        tables.append(self.artifact_table_postgres)
        tables.append(self.metadata_table_postgres)
        self.tables = tables

    async def _init(self):

        host = os.environ.get("MF_METADATA_DB_HOST", "localhost")
        port = os.environ.get("MF_METADATA_DB_PORT", 5432)
        user = os.environ.get("MF_METADATA_DB_USER", "postgres")
        password = os.environ.get("MF_METADATA_DB_PSWD", "postgres")
        database_name = os.environ.get("MF_METADATA_DB_NAME", "postgres")

        dsn = "dbname={0} user={1} password={2} host={3} port={4}".format(
            database_name, user, password, host, port
        )
        # todo make poolsize min and max configurable as well as timeout
        # todo add retry and better error message
        self.pool = await aiopg.create_pool(dsn)
        for table in self.tables:
            await table._init()

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


class AsyncPostgresTable(object):
    table_name = None
    schema_version = 1
    keys = None
    _command = None
    _insert_command = None
    _filters = None
    _base_query = "SELECT {0} from"
    _row_type = None

    def __init__(self):
        if self.table_name is None or self._command is None:
            raise NotImplementedError("need to specify table name and create command")

    async def _init(self):
        await PostgresUtils.create_if_missing(self.table_name, self._command)

    async def get_records(self, filter_dict={}, fetch_single=False,
                              ordering=None, limit=None, expanded=False):
        # generate where clause
        filters = []
        for col_name, col_val in filter_dict.items():
            filters.append(col_name + "=" + col_val)

        seperator = " and "
        where_clause = ""
        if bool(filter_dict):
            where_clause = "where " + seperator.join(filters)

        sql_template = "select {0} from {1} {2}"

        if ordering is not None:
            sql_template = sql_template + " {3}"

        if limit is not None:
            sql_template = sql_template + " {4}"

        select_sql = sql_template.format(
            self.keys, self.table_name, where_clause, ordering, limit).rstrip()

        try:
            with (
                await AsyncPostgresDB.get_instance().pool.cursor(
                    cursor_factory=psycopg2.extras.DictCursor
                )
            ) as cur:
                await cur.execute(select_sql)
                records = await cur.fetchall()
                rows = []
                for record in records:
                    rows.append(self._row_type(**record).serialize(expanded))

                if fetch_single:
                    body = rows[0]
                else:
                    body = rows

                cur.close()
                return DBResponse(response_code=200, body=body)
        except (Exception, psycopg2.DatabaseError) as error:
            return aiopg_exception_handling(error)

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
                await AsyncPostgresDB.get_instance().pool.cursor(
                    cursor_factory=psycopg2.extras.DictCursor
                )
            ) as cur:

                await cur.execute(insert_sql, tuple(values))
                records = await cur.fetchall()
                record = records[0]
                response_body = self._row_type(**record).serialize()
                cur.close()
            return DBResponse(response_code=200, body=response_body)
        except (Exception, psycopg2.DatabaseError) as error:
            return aiopg_exception_handling(error)


class PostgresUtils(object):
    @staticmethod
    async def create_if_missing(table_name, command):
        with (await AsyncPostgresDB.get_instance().pool.cursor()) as cur:
            await cur.execute(
                "select * from information_schema.tables where table_name=%s",
                (table_name,),
            )
            table_exist = bool(cur.rowcount)
            if not table_exist:
                await cur.execute(command)
                cur.close()
    # todo add method to check schema version

class AsyncFlowTablePostgres(AsyncPostgresTable):
    flow_dict = {}
    table_name = "flows_v3"
    keys = "flow_id, user_name, ts_epoch, tags, system_tags"
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
            "tags": json.dumps(flow.tags),
            "system_tags": json.dumps(flow.system_tags),
        }
        return await self.create_record(dict)

    async def get_flow(self, flow_id: str):
        filter_dict = {"flow_id": "'{0}'".format(flow_id)}
        return await self.get_records(filter_dict=filter_dict, fetch_single=True)

    async def get_all_flows(self):
        return await self.get_records()


class AsyncRunTablePostgres(AsyncPostgresTable):
    run_dict = {}
    run_by_flow_dict = {}
    _current_count = 0
    _row_type = RunRow
    table_name = "runs_v3"
    keys = "flow_id, run_number, run_id, user_name, ts_epoch, tags, system_tags"
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
            "tags": json.dumps(run.tags),
            "system_tags": json.dumps(run.system_tags),
            "run_id": run.run_id,
        }
        return await self.create_record(dict)

    async def get_run(self, flow_id: str, run_id: str, expanded: bool = False):
        key, value = translate_run_key(run_id)
        filter_dict = {"flow_id": "'{0}'".format(flow_id), key: str(value)}
        return await self.get_records(filter_dict=filter_dict,
                                      fetch_single=True, expanded=expanded)

    async def get_all_runs(self, flow_id: str):
        filter_dict = {"flow_id": "'{0}'".format(flow_id)}
        return await self.get_records(filter_dict=filter_dict)


class AsyncStepTablePostgres(AsyncPostgresTable):
    step_dict = {}
    run_to_step_dict = {}
    _row_type = StepRow
    table_name = "steps_v3"
    keys = "flow_id, run_number, run_id, step_name, user_name, ts_epoch, tags, system_tags"
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
            "tags": json.dumps(step_object.tags),
            "system_tags": json.dumps(step_object.system_tags),
        }
        return await self.create_record(dict)

    async def get_steps(self, flow_id: str, run_id: str):
        run_id_key, run_id_value = translate_run_key(run_id)
        filter_dict = {"flow_id": "'{0}'".format(flow_id),
                       run_id_key: run_id_value}
        return await self.get_records(filter_dict=filter_dict)

    async def get_step(self, flow_id: str, run_id: str, step_name: str):
        run_id_key, run_id_value = translate_run_key(run_id)
        filter_dict = {
            "flow_id": "'{0}'".format(flow_id),
            run_id_key: run_id_value,
            "step_name": "'{0}'".format(step_name),
        }
        return await self.get_records(filter_dict=filter_dict, fetch_single=True)


class AsyncTaskTablePostgres(AsyncPostgresTable):
    task_dict = {}
    step_to_task_dict = {}
    _current_count = 0
    _row_type = TaskRow
    table_name = "tasks_v3"
    keys = "flow_id, run_number, run_id, step_name, task_id, task_name, user_name, ts_epoch, tags, system_tags"
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
            "tags": json.dumps(task.tags),
            "system_tags": json.dumps(task.system_tags),
        }
        return await self.create_record(dict)

    async def get_tasks(self, flow_id: str, run_id: int, step_name: str):
        run_id_key, run_id_value = translate_run_key(run_id)
        filter_dict = {
            "flow_id": "'{0}'".format(flow_id),
            run_id_key: run_id_value,
            "step_name": "'{0}'".format(step_name),
        }
        return await self.get_records(filter_dict=filter_dict)

    async def get_task(self, flow_id: str, run_id: int, step_name: str,
                       task_id: int, expanded: bool = False):
        run_id_key, run_id_value = translate_run_key(run_id)
        task_id_key, task_id_value = translate_task_key(task_id)
        filter_dict = {
            "flow_id": "'{0}'".format(flow_id),
            run_id_key: run_id_value,
            "step_name": "'{0}'".format(step_name),
            task_id_key: task_id_value,
        }
        return await self.get_records(filter_dict=filter_dict,
                                      fetch_single=True, expanded=expanded)


class AsyncMetadataTablePostgres(AsyncPostgresTable):
    metadata_dict = {}
    run_to_metadata_dict = {}
    _current_count = 0
    _row_type = MetadataRow
    table_name = "metadata_v3"
    task_table_name = AsyncTaskTablePostgres.table_name
    keys = "flow_id, run_number, run_id, step_name, task_id, task_name, id, field_name, value, type, user_name, ts_epoch, tags, system_tags"
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
        PRIMARY KEY(flow_id, run_number, step_name, task_id, field_name)
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
        filter_dict = {"flow_id": "'{0}'".format(flow_id),
                       run_id_key: run_id_value}
        return await self.get_records(filter_dict=filter_dict)

    async def get_metadata(
        self, flow_id: str, run_id: int, step_name: str, task_id: str
    ):
        run_id_key, run_id_value = translate_run_key(run_id)
        task_id_key, task_id_value = translate_task_key(task_id)
        filter_dict = {
            "flow_id": "'{0}'".format(flow_id),
            run_id_key: run_id_value,
            "step_name": "'{0}'".format(step_name),
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
    ordering = "ORDER BY attempt_id DESC"
    keys = "flow_id, run_number, run_id, step_name, task_id, task_name, name, location, ds_type, sha, type, content_type, user_name, attempt_id, ts_epoch, tags, system_tags"
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
            "flow_id": "'{0}'".format(flow_id),
            run_id_key: run_id_value,
        }
        return await self.get_records(filter_dict=filter_dict,
                                      ordering=self.ordering)

    async def get_artifact_in_steps(self, flow_id: str, run_id: int, step_name: str):
        run_id_key, run_id_value = translate_run_key(run_id)
        filter_dict = {
            "flow_id": "'{0}'".format(flow_id),
            run_id_key: run_id_value,
            "step_name": "'{0}'".format(step_name),
        }
        return await self.get_records(filter_dict=filter_dict,
                                      ordering=self.ordering)

    async def get_artifact_in_task(
        self, flow_id: str, run_id: int, step_name: str, task_id: int
    ):
        run_id_key, run_id_value = translate_run_key(run_id)
        task_id_key, task_id_value = translate_task_key(task_id)
        filter_dict = {
            "flow_id": "'{0}'".format(flow_id),
            run_id_key: run_id_value,
            "step_name": "'{0}'".format(step_name),
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
            "flow_id": "'{0}'".format(flow_id),
            run_id_key: run_id_value,
            "step_name": "'{0}'".format(step_name),
            task_id_key: task_id_value,
            '"name"': "'{0}'".format(name),
        }
        return await self.get_records(filter_dict=filter_dict,
                                      fetch_single=True, ordering=self.ordering)

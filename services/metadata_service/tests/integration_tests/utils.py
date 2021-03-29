from aiohttp import web
import os
import psycopg2
import json

from services.data.postgres_async_db import AsyncPostgresDB

# Migration imports
from services.migration_service.api.admin import AdminApi as MigrationAdminApi
from services.migration_service.data.postgres_async_db import AsyncPostgresDB as MigrationAsyncPostgresDB


from services.metadata_service.api.flow import FlowApi
from services.metadata_service.api.admin import AuthApi

# Test fixture helpers begin


def init_app(loop, aiohttp_client, queue_ttl=30):
    app = web.Application()

    # Migration routes as a subapp
    migration_app = web.Application()
    MigrationAdminApi(migration_app)
    app.add_subapp("/migration/", migration_app)

    FlowApi(app)
    AuthApi(app)

    return loop.run_until_complete(aiohttp_client(app))


async def init_db(cli):
    # Make sure migration scripts are applied
    migration_db = MigrationAsyncPostgresDB.get_instance()
    await migration_db._init()

    # Apply migrations and make sure "is_up_to_date" == True
    await cli.patch("/migration/upgrade")
    # TODO: Api not responding with valid json headers so check fails.
    # status = await (await cli.get("/migration/db_schema_status")).json()
    # assert status["is_up_to_date"] is True

    db = AsyncPostgresDB.get_instance()
    await db._init()
    return db


async def clean_db(db: AsyncPostgresDB):
    # Tables to clean (order is important due to foreign keys)
    tables = [
        db.metadata_table_postgres,
        db.artifact_table_postgres,
        db.task_table_postgres,
        db.step_table_postgres,
        db.run_table_postgres,
        db.flow_table_postgres
    ]
    try:
        with (
            await db.pool.cursor(
                cursor_factory=psycopg2.extras.DictCursor
            )
        ) as cur:
            for table in tables:
                cleanup_query = "DELETE FROM {}".format(table.table_name)
                await cur.execute(cleanup_query)

            cur.close()
    except Exception as error:
        print("DB Cleanup failed after test. This might have adverse effects on further test runs", str(error))

# Test fixture helpers end

# Row helpers begin


async def add_flow(db: AsyncPostgresDB, flow_id="HelloFlow",
                   user_name="dipper", tags=["foo:bar"], system_tags=["runtime:dev"]):
    flow = {
        "flow_id": flow_id,
        "user_name": user_name,
        "tags": json.dumps(tags),
        "system_tags": json.dumps(system_tags)
    }
    return await db.flow_table_postgres.create_record(flow)


async def add_run(db: AsyncPostgresDB, flow_id="HelloFlow",
                  run_number: int = None, run_id: str = None,
                  user_name="dipper", tags=["foo:bar"], system_tags=["runtime:dev"],
                  last_heartbeat_ts: int = None):
    run = {
        "flow_id": flow_id,
        "run_id": run_id,
        "user_name": user_name,
        "tags": json.dumps(tags),
        "system_tags": json.dumps(system_tags),
        "last_heartbeat_ts": last_heartbeat_ts
    }
    return await db.run_table_postgres.create_record(run)


async def add_step(db: AsyncPostgresDB, flow_id="HelloFlow",
                   run_number: int = None, run_id: str = None, step_name="step",
                   user_name="dipper", tags=["foo:bar"], system_tags=["runtime:dev"]):
    step = {
        "flow_id": flow_id,
        "run_number": run_number,
        "run_id": run_id,
        "step_name": step_name,
        "user_name": user_name,
        "tags": json.dumps(tags),
        "system_tags": json.dumps(system_tags)
    }
    return await db.step_table_postgres.create_record(step)


async def add_task(db: AsyncPostgresDB, flow_id="HelloFlow",
                   run_number: int = None, run_id: str = None, step_name="step", task_id=None, task_name=None,
                   user_name="dipper", tags=["foo:bar"], system_tags=["runtime:dev"],
                   last_heartbeat_ts: int = None):
    task = {
        "flow_id": flow_id,
        "run_number": run_number,
        "step_name": step_name,
        "task_name": task_name,
        "user_name": user_name,
        "tags": json.dumps(tags),
        "system_tags": json.dumps(system_tags),
        "last_heartbeat_ts": last_heartbeat_ts
    }
    return await db.task_table_postgres.create_record(task)


async def add_metadata(db: AsyncPostgresDB, flow_id="HelloFlow",
                       run_number: int = None, run_id: str = None, step_name="step", task_id=None, task_name=None,
                       metadata={},
                       user_name="dipper", tags=["foo:bar"], system_tags=["runtime:dev"]):
    values = {
        "flow_id": flow_id,
        "run_number": run_number,
        "run_id": run_id,
        "step_name": step_name,
        "task_id": str(task_id),
        "task_name": task_name,
        "field_name": metadata.get("field_name", " "),
        "value": metadata.get("value", " "),
        "type": metadata.get("type", " "),
        "user_name": user_name,
        "tags": json.dumps(tags),
        "system_tags": json.dumps(system_tags)
    }
    return await db.metadata_table_postgres.create_record(values)


async def add_artifact(db: AsyncPostgresDB, flow_id="HelloFlow",
                       run_number: int = None, run_id: str = None, step_name="step", task_id=None, task_name=None,
                       artifact={},
                       user_name="dipper", tags=["foo:bar"], system_tags=["runtime:dev"]):
    values = {
        "flow_id": flow_id,
        "run_number": run_number,
        "run_id": run_id,
        "step_name": step_name,
        "task_id": str(task_id),
        "task_name": task_name,
        "name": artifact.get("name", " "),
        "location": artifact.get("location", " "),
        "ds_type": artifact.get("ds_type", " "),
        "sha": artifact.get("sha", " "),
        "type": artifact.get("type", " "),
        "content_type": artifact.get("content_type", " "),
        "attempt_id": artifact.get("attempt_id", 0),
        "user_name": user_name,
        "tags": json.dumps(tags),
        "system_tags": json.dumps(system_tags)
    }
    return await db.artifact_table_postgres.create_record(values)

# Row helpers end

# Resource helpers

async def assert_api_get_response(cli, path: str, status: int = 200, data: object = None):
    """
    Perform a GET request with the provided http cli to the provided path, assert that the status and data received are correct.
    Expectation is that the API returns text/plain format json.

    Parameters
    ----------
    cli : aiohttp cli
        aiohttp test client
    path : str
        url path to perform GET request to
    status : int (default 200)
        http status code to expect from response
    data : object
        Any json serializable data type. will undergo json.dumps before asserting with response body.
    """
    response = await cli.get(path)

    assert response.status == status
    body = await response.text()

    if data:
        assert body == json.dumps(data)

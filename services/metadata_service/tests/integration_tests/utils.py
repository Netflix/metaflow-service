import json
import os

import psycopg2
import pytest
from aiohttp import web
from services.data.postgres_async_db import AsyncPostgresDB
from services.utils import DBConfiguration
from services.metadata_service.api.admin import AuthApi
from services.metadata_service.api.flow import FlowApi
from services.metadata_service.api.run import RunApi
from services.metadata_service.api.step import StepApi
from services.metadata_service.api.task import TaskApi
from services.metadata_service.api.artifact import ArtificatsApi
from services.metadata_service.api.metadata import MetadataApi

# Migration imports
from services.migration_service.api.admin import AdminApi as MigrationAdminApi
from services.migration_service.data.postgres_async_db import \
    AsyncPostgresDB as MigrationAsyncPostgresDB

# Test fixture helpers begin


def get_test_dbconf():
    "Returns a DBConfiguration suitable for the test environment, or exits pytest completely upon failure"
    db_conf = DBConfiguration()

    if db_conf.dsn != "dbname=test user=test host=db_test port=5432 password=test":
        pytest.exit("The test suite should only be run in a test environment. \n \
            Configured database host is not suited for running tests. \n \
            expected DSN to be: dbname=test user=test host=db_test port=5432 password=test")

    return db_conf


def init_app(loop, aiohttp_client, queue_ttl=30):
    app = web.Application()

    # Migration routes as a subapp
    migration_app = web.Application()
    MigrationAdminApi(migration_app)
    app.add_subapp("/migration/", migration_app)

    FlowApi(app)
    RunApi(app)
    StepApi(app)
    TaskApi(app)
    AuthApi(app)
    ArtificatsApi(app)
    MetadataApi(app)

    return loop.run_until_complete(aiohttp_client(app))


async def init_db(cli):
    db_conf = get_test_dbconf()

    # Make sure migration scripts are applied
    migration_db = MigrationAsyncPostgresDB.get_instance()
    await migration_db._init(db_conf)

    # Apply migrations and make sure "is_up_to_date" == True
    await cli.patch("/migration/upgrade")
    status = await (await cli.get("/migration/db_schema_status")).json()
    assert status["is_up_to_date"] is True

    db = AsyncPostgresDB.get_instance()
    await db._init(db_conf)
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
    for table in tables:
        await table.execute_sql(select_sql="DELETE FROM {}".format(table.table_name))

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
        An object to assert the api response against.
    """
    response = await cli.get(path)

    assert response.status == status

    if data:
        body = json.loads(await response.text())
        assert body == data


async def assert_api_post_response(cli, path: str, payload: object = None, status: int = 200, expected_body: object = None):
    """
    Perform a POST request with the provided http cli to the provided path with the payload,
    asserts that the status and data received are correct.
    Expectation is that the API returns text/plain format json.

    Parameters
    ----------
    cli : aiohttp cli
        aiohttp test client
    path : str
        url path to perform POST request to
    payload : object (default None)
        the payload to be sent with the POST request, as json.
    status : int (default 200)
        http status code to expect from response
    expected_body : object
        An object to assert the api response against.

    Returns
    -------
    Object or None
        returns the body of the api response if no data was provided to assert against, otherwise returns None
    """
    response = await cli.post(path, json=payload)

    assert response.status == status

    body = json.loads(await response.text())
    if expected_body:
        assert body == expected_body
    else:
        return body


def compare_partial(actual, partial):
    "compare that all keys of partial exist in actual, and that the values match."
    for k, v in partial.items():
        assert k in actual
        assert v == actual[k]

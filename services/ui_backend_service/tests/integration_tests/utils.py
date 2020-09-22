from aiohttp import web
from pyee import AsyncIOEventEmitter

from services.data.postgres_async_db import AsyncPostgresDB
from services.utils import DBConfiguration

from services.ui_backend_service.api.flow import FlowApi
from services.ui_backend_service.api.run import RunApi
from services.ui_backend_service.api.step import StepApi
from services.ui_backend_service.api.task import TaskApi
from services.ui_backend_service.api.metadata import MetadataApi
from services.ui_backend_service.api.artifact import ArtificatsApi
from services.ui_backend_service.api.tag import TagApi
from services.ui_backend_service.api.ws import Websocket

from services.ui_backend_service.api.admin import AdminApi

from services.data.models import FlowRow, RunRow, StepRow, TaskRow, MetadataRow, ArtifactRow

# Migration imports

from services.migration_service.api.admin import AdminApi as MigrationAdminApi
from services.migration_service.data.postgres_async_db import AsyncPostgresDB as MigrationAsyncPostgresDB

# Constants

TIMEOUT_FUTURE = 0.1

# Test fixture helpers begin


def init_app(loop, aiohttp_client):
    app = web.Application()
    app.event_emitter = AsyncIOEventEmitter()

    # Migration routes as a subapp
    migration_app = web.Application()
    MigrationAdminApi(migration_app)
    app.add_subapp("/migration/", migration_app)

    FlowApi(app)
    RunApi(app)
    StepApi(app)
    TaskApi(app)
    MetadataApi(app)
    ArtificatsApi(app)
    TagApi(app)

    Websocket(app, app.event_emitter)

    AdminApi(app)

    return loop.run_until_complete(aiohttp_client(app))


async def init_db(cli):
    db_conf = DBConfiguration()

    # Make sure migration scripts are applied
    migration_db = MigrationAsyncPostgresDB.get_instance()
    await migration_db._init(db_conf)

    # Apply migrations and make sure "is_up_to_date" == True
    await cli.patch("/migration/upgrade")
    status = await (await cli.get("/migration/db_schema_status")).json()
    assert status["is_up_to_date"] == True

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
    flow = FlowRow(
        flow_id=flow_id,
        user_name=user_name,
        tags=tags,
        system_tags=system_tags
    )
    return await db.flow_table_postgres.add_flow(flow)


async def add_run(db: AsyncPostgresDB, flow_id="HelloFlow",
                  run_number: int = None, run_id: str = None,
                  user_name="dipper", tags=["foo:bar"], system_tags=["runtime:dev"]):
    run = RunRow(
        flow_id=flow_id,
        run_number=run_number,
        run_id=run_id,
        user_name=user_name,
        tags=tags,
        system_tags=system_tags,
    )
    return await db.run_table_postgres.add_run(run)


async def add_step(db: AsyncPostgresDB, flow_id="HelloFlow",
                   run_number: int = None, run_id: str = None, step_name="step",
                   user_name="dipper", tags=["foo:bar"], system_tags=["runtime:dev"]):
    step = StepRow(
        flow_id=flow_id,
        run_number=run_number,
        run_id=run_id,
        step_name=step_name,
        user_name=user_name,
        tags=tags,
        system_tags=system_tags
    )
    return await db.step_table_postgres.add_step(step)


async def add_task(db: AsyncPostgresDB, flow_id="HelloFlow",
                   run_number: int = None, run_id: str = None, step_name="step", task_id=None, task_name=None,
                   user_name="dipper", tags=["foo:bar"], system_tags=["runtime:dev"]):
    task = TaskRow(
        flow_id=flow_id,
        run_number=run_number,
        run_id=run_id,
        step_name=step_name,
        task_name=task_name,
        task_id=task_id,
        user_name=user_name,
        tags=tags,
        system_tags=system_tags,
    )
    return await db.task_table_postgres.add_task(task)


async def add_metadata(db: AsyncPostgresDB, flow_id="HelloFlow",
                       run_number: int = None, run_id: str = None, step_name="step", task_id=None, task_name=None,
                       metadata={},
                       user_name="dipper", tags=["foo:bar"], system_tags=["runtime:dev"]):
    values = {
        "flow_id": flow_id,
        "run_number": run_number,
        "run_id": run_id,
        "step_name": step_name,
        "task_id": task_id,
        "task_name": task_name,
        "field_name": metadata.get("field_name", " "),
        "value": metadata.get("value", " "),
        "type": metadata.get("type", " "),
        "user_name": user_name,
        "tags": tags,
        "system_tags": system_tags
    }
    return await db.metadata_table_postgres.add_metadata(**values)


async def add_artifact(db: AsyncPostgresDB, flow_id="HelloFlow",
                       run_number: int = None, run_id: str = None, step_name="step", task_id=None, task_name=None,
                       artifact={},
                       user_name="dipper", tags=["foo:bar"], system_tags=["runtime:dev"]):
    values = {
        "flow_id": flow_id,
        "run_number": run_number,
        "run_id": run_id,
        "step_name": step_name,
        "task_id": task_id,
        "task_name": task_name,
        "name": artifact.get("name", " "),
        "location": artifact.get("location", " "),
        "ds_type": artifact.get("ds_type", " "),
        "sha": artifact.get("sha", " "),
        "type": artifact.get("type", " "),
        "content_type": artifact.get("content_type", " "),
        "attempt_id": artifact.get("attempt_id", 0),
        "user_name": user_name,
        "tags": tags,
        "system_tags": system_tags
    }
    return await db.artifact_table_postgres.add_artifact(**values)

# Row helpers end

# Resource helpers begin


async def _test_list_resources(cli, db: AsyncPostgresDB, path: str, expected_status=200, expected_data=[]):
    resp = await cli.get(path)
    body = await resp.json()
    data = body.get("data")

    assert resp.status == expected_status
    assert data == expected_data


async def _test_single_resource(cli, db: AsyncPostgresDB, path: str, expected_status=200, expected_data={}):
    resp = await cli.get(path)
    body = await resp.json()
    data = body.get("data")

    assert resp.status == expected_status
    assert data == expected_data

# Resource helpers end
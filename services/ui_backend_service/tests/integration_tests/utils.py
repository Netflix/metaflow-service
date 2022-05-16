from aiohttp import web
from pyee import AsyncIOEventEmitter
import pytest
import os
import json
import datetime
import contextlib

from services.ui_backend_service.data.db import AsyncPostgresDB
from services.ui_backend_service.data.cache.store import CacheStore
from services.utils.tests import get_test_dbconf

from services.ui_backend_service.api import (
    FlowApi, RunApi, StepApi, TaskApi,
    MetadataApi, ArtificatsApi, TagApi,
    Websocket, AdminApi, FeaturesApi,
    AutoCompleteApi, PluginsApi, LogApi, CardsApi
)

from services.ui_backend_service.data.db.models import FlowRow, RunRow, StepRow, TaskRow, MetadataRow, ArtifactRow

# Migration imports

from services.migration_service.api.admin import AdminApi as MigrationAdminApi
from services.migration_service.data.postgres_async_db import AsyncPostgresDB as MigrationAsyncPostgresDB

# Constants

TIMEOUT_FUTURE = 0.2

# Test fixture helpers begin


async def init_app(aiohttp_client, queue_ttl=30):
    app = web.Application()
    app.event_emitter = AsyncIOEventEmitter()

    # Migration routes as a subapp
    migration_app = web.Application()
    MigrationAdminApi(migration_app)
    app.add_subapp("/migration/", migration_app)

    # init a db adapter explicitly to be used for the api requests.
    # Skip all creation processes, these are handled with migration service and init_db
    db_conf = get_test_dbconf()
    db = AsyncPostgresDB(name='api')
    await db._init(db_conf=db_conf, create_triggers=False)

    cache_store = CacheStore(db=db, event_emitter=app.event_emitter)

    app.AutoCompleteApi = AutoCompleteApi(app, db)
    FlowApi(app, db)
    RunApi(app, db)
    StepApi(app, db)
    TaskApi(app, db, cache_store)
    MetadataApi(app, db)
    ArtificatsApi(app, db)
    TagApi(app, db)
    FeaturesApi(app)
    PluginsApi(app)
    LogApi(app, db)
    CardsApi(app, db)

    Websocket(app, db, app.event_emitter, queue_ttl)

    AdminApi(app, cache_store)

    return await aiohttp_client(app)


async def init_db(cli):
    db_conf = get_test_dbconf()

    # Make sure migration scripts are applied
    migration_db = MigrationAsyncPostgresDB.get_instance()
    await migration_db._init(db_conf)

    # Apply migrations and make sure "is_up_to_date" == True
    await cli.patch("/migration/upgrade")
    status = await (await cli.get("/migration/db_schema_status")).json()
    assert status["is_up_to_date"] is True

    db = AsyncPostgresDB()
    await db._init(db_conf=db_conf, create_triggers=True)
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


@pytest.fixture
async def cli(aiohttp_client):
    return await init_app(aiohttp_client)


@pytest.fixture
async def db(cli):
    async_db = await init_db(cli)
    yield async_db
    await clean_db(async_db)

# Test fixture helpers end

# Environment helpers begin


@contextlib.contextmanager
def set_env(environ={}):
    old_environ = dict(os.environ)
    os.environ.clear()
    os.environ.update(environ)
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(old_environ)

# Environment helpers end

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
                  user_name="dipper", tags=["run_user_tag"], system_tags=["run_system_tag"],
                  last_heartbeat_ts=None):
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
                   user_name="dipper", tags=["step_user_tag"], system_tags=["step_system_tag"]):
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
                   user_name="dipper", tags=["task_user_tag"], system_tags=["task_system_tag"],
                   last_heartbeat_ts=None):
    task = {
        "flow_id": flow_id,
        "run_number": run_number,
        "run_id": run_id,
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
                       user_name="dipper", tags=["artifact_user_tag"], system_tags=["artifact_system_tag"]):
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

# Resource helpers begin


def _fill_missing_resource_data(_item):
    if 'run_number' in _item:
        try:
            _item['run_number'] = int(_item['run_number'])
        except:
            pass
        if 'run_id' not in _item:
            _item['run_id'] = None

    if 'task_id' in _item:
        try:
            _item['task_id'] = int(_item['task_id'])
        except:
            pass
        if 'task_name' not in _item:
            _item['task_name'] = None

    return _item


async def _test_list_resources(cli, db: AsyncPostgresDB, path: str, expected_status=200, expected_data=[], ignore_data_order=False, approx_keys=None):
    resp = await cli.get(path)
    body = await resp.json()
    data = body.get("data")

    if not expected_status:
        return resp.status, data

    assert resp.status == expected_status

    if not expected_data:
        return resp.status, data

    expected_data[:] = map(_fill_missing_resource_data, expected_data)
    assert len(data) == len(expected_data)

    data_to_be_compared = data
    expected_data_to_be_compared = expected_data
    if ignore_data_order:
        assert isinstance(data, list) and isinstance(expected_data, list)

        # If item contains fields A and B, then sort list first by item[A], then item[B]
        # Exclude approx_keys, because we don't want approx_sort.
        def _sort_key(r):
            if isinstance(r, str):
                return (r,)
            # Stringify everything for sorting purposes (avoid TypeError on "<" operator)
            return tuple(str(r[k]) for k in sorted(r.keys()) if (not approx_keys or k not in approx_keys))
        data_to_be_compared = sorted(data, key=_sort_key)
        expected_data_to_be_compared = sorted(expected_data, key=_sort_key)

    for i, d in enumerate(data_to_be_compared):
        if approx_keys:
            _test_dict_approx(d, expected_data_to_be_compared[i], approx_keys)
        else:
            assert d == expected_data_to_be_compared[i]

    # return raw data as returned from the GET - unaffected by any internal sorting
    return resp.status, data


async def _test_single_resource(cli, db: AsyncPostgresDB, path: str, expected_status=200, expected_data={}, approx_keys=None):
    resp = await cli.get(path)
    body = await resp.json()
    data = body.get("data")

    if not expected_status:
        return resp.status, data

    assert resp.status == expected_status

    if not expected_data:
        return resp.status, data

    expected_data = _fill_missing_resource_data(expected_data)
    if approx_keys:
        _test_dict_approx(data, expected_data, approx_keys)
    else:
        assert data == expected_data

    return resp.status, data


def _test_dict_approx(actual, expected, approx_keys, threshold=1000):
    "Assert that two dicts are almost equal, allowing for some leeway on specified keys"
    # NOTE: This is mainly required for testing resources that produce data during query execution. For example
    # when using extract(epoch from now()) we can not accurately expect what the timestamp returned from the api should be.
    # TODO: If possible, a less error prone solution would be to somehow mock/freeze the now() on a per-test basis.
    for k, v in actual.items():
        if k in approx_keys:
            assert v == pytest.approx(expected[k], rel=threshold)
        else:
            assert v == expected[k]


def get_heartbeat_ts(offset=5):
    "Return a heartbeat timestamp with the given offset in seconds. Default offset is 5 seconds"
    return int(datetime.datetime.utcnow().timestamp()) + offset


def update_objects_with_run_tags(obj_type_name: str, objects: list, run: object):
    # expect object's tags to be overridden by tags of their ancestral run
    for obj in objects:
        assert obj['tags'] != run['tags'], f'Expected divergent {obj_type_name} tags to ensure test efficacy'
        assert obj['system_tags'] != run['system_tags'], f'Expected divergent {obj_type_name} system_tags to ensure test efficacy'
        obj['tags'] = run['tags']
        obj['system_tags'] = run['system_tags']
# Resource helpers end

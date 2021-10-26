import pytest
from unittest import mock
from .utils import (
    add_metadata, init_app, init_db, clean_db,
    add_flow, add_run, add_step, add_task,
    _test_list_resources
)
pytestmark = [pytest.mark.integration_tests]


# Fixtures begin


@pytest.fixture
def cli(loop, aiohttp_client):
    return init_app(loop, aiohttp_client)


@pytest.fixture
async def db(cli):
    async_db = await init_db(cli)
    yield async_db
    await clean_db(async_db)

# Fixtures end


async def test_log_default_response(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="step", run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body
    _task = (await add_task(db,
                            flow_id=_step.get("flow_id"),
                            step_name=_step.get("step_name"),
                            run_number=_step.get("run_number"),
                            run_id=_step.get("run_id"))).body

    await add_metadata(db,
                       flow_id=_task.get("flow_id"),
                       run_number=_task.get("run_number"),
                       run_id=_task.get("run_id"),
                       step_name=_task.get("step_name"),
                       task_id=_task.get("task_id"),
                       metadata={
                           "field_name": "attempt",
                           "value": "0",
                           "type": "attempt"
                       }
                       )

    async def read_and_output(cache_client, task, logtype, limit=0, page=1, reverse_order=False, output_raw=False):
        # Test default pagination params
        assert limit == 1000
        assert page == 1
        assert reverse_order is False
        assert output_raw is False
        return [], 1

    with mock.patch("services.ui_backend_service.api.log.read_and_output", new=read_and_output):
        _, data = await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/logs/out".format(**_task), 200, None)
        assert data == []
        _, data = await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/logs/out?attempt_id=0".format(**_task), 200, None)
        assert data == []
        await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/logs/out?attempt_id=1".format(**_task), 404, None)


async def test_log_paginated_response(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="step", run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body
    _task = (await add_task(db,
                            flow_id=_step.get("flow_id"),
                            step_name=_step.get("step_name"),
                            run_number=_step.get("run_number"),
                            run_id=_step.get("run_id"))).body

    async def read_and_output(cache_client, task, logtype, limit=0, page=1, reverse_order=False, output_raw=False):
        assert limit == 3
        assert page == 1
        assert reverse_order is False
        assert output_raw is False
        return [], 1

    with mock.patch("services.ui_backend_service.api.log.read_and_output", new=read_and_output):
        # default log order should be oldest to newest. should obey limit.
        _, data = await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/logs/out?_limit=3".format(**_task), 200, None)
        assert data == []

    async def read_and_output(cache_client, task, logtype, limit=0, page=1, reverse_order=False, output_raw=False):
        assert limit == 3
        assert page == 4
        assert reverse_order is True
        assert output_raw is False
        return [], 1

    with mock.patch("services.ui_backend_service.api.log.read_and_output", new=read_and_output):
        # ordering by row should be possible in reverse. should obey limit.
        _, data = await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/logs/out?_order=-row&_limit=3&_page=4".format(**_task), 200, None)
        assert data == []


async def test_log_download_response(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="step", run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body
    _task = (await add_task(db,
                            flow_id=_step.get("flow_id"),
                            step_name=_step.get("step_name"),
                            run_number=_step.get("run_number"),
                            run_id=_step.get("run_id"))).body

    async def read_and_output(cache_client, task, logtype, limit=0, page=1, reverse_order=False, output_raw=False):
        assert output_raw is True
        return "some logs", 1

    with mock.patch("services.ui_backend_service.api.log.read_and_output", new=read_and_output):
        # download route should request log file in raw format from cache.
        resp = await cli.get("/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/logs/out/download".format(**_task))
        body = await resp.text()

        assert resp.status == 200
        assert body == "some logs"

import pytest
from .utils import (
    init_app, init_db, clean_db,
    add_flow, add_run, add_step, add_task, add_artifact,
    TIMEOUT_FUTURE
)
from typing import List, Dict
from asyncio import Future, wait_for

from services.ui_backend_service.api.notify import ListenNotify

pytestmark = [pytest.mark.integration_tests]

# Fixtures begin


@pytest.fixture
def cli(loop, aiohttp_client):
    return init_app(loop, aiohttp_client)


@pytest.fixture
async def db(cli):
    async_db = await init_db(cli)

    # Init after DB is ready so that connection pool is available
    app = cli.server.app
    ListenNotify(app, app.event_emitter)

    yield async_db
    await clean_db(async_db)

# Fixtures end


def _set_notify_handler(cli, loop):
    should_call = Future(loop=loop)

    async def event_handler(operation: str, resources: List[str], result: Dict):
        should_call.set_result([operation, resources, result])
    cli.server.app.event_emitter.once('notify', event_handler)

    return should_call


async def test_pg_notify_simple_flow(cli, db, loop):
    # Add new Flow
    _should_call = _set_notify_handler(cli, loop)
    _flow = (await add_flow(db, flow_id="HelloFlow")).body

    # Wait for results
    operation, resources, result = await wait_for(_should_call, TIMEOUT_FUTURE)
    assert operation == "INSERT"
    assert resources == ["/flows", "/flows/HelloFlow"]
    assert result == _flow


# Test INSERT and UPDATE pg_notify triggers
# Resource insert order is important here due to foreign keys
async def test_pg_notify_trigger_updates_on_task(cli, db, loop):
    # Add new Flow
    _should_call = _set_notify_handler(cli, loop)
    _flow = (await add_flow(db, flow_id="HelloFlow")).body

    # Wait for results
    operation, resources, result = await wait_for(_should_call, TIMEOUT_FUTURE)
    assert operation == "INSERT"
    assert resources == ["/flows", "/flows/HelloFlow"]
    assert result == _flow

    # Add new Run
    _should_call = _set_notify_handler(cli, loop)
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _run["status"] = "running"

    # Wait for results
    operation, resources, result = await wait_for(_should_call, TIMEOUT_FUTURE)
    assert operation == "INSERT"
    assert resources == ["/runs", "/flows/HelloFlow/runs",
                         "/flows/HelloFlow/runs/{run_number}".format(**_run)]
    assert result == _run

    # Add normal Step
    _should_call = _set_notify_handler(cli, loop)
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="step", run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body

    # Wait for results
    operation, resources, result = await wait_for(_should_call, TIMEOUT_FUTURE)
    assert operation == "INSERT"
    assert resources == ["/flows/{flow_id}/runs/{run_number}/steps".format(**_step),
                         "/flows/{flow_id}/runs/{run_number}/steps/{step_name}".format(**_step)]
    assert result == _step

    # Add new Task
    _should_call = _set_notify_handler(cli, loop)
    _task_step = (await add_task(db,
                                 flow_id=_step.get("flow_id"),
                                 step_name=_step.get("step_name"),
                                 run_number=_step.get("run_number"),
                                 run_id=_step.get("run_id"))).body
    _task_step['status'] = 'running'

    # Wait for results
    operation, resources, result = await wait_for(_should_call, TIMEOUT_FUTURE)
    assert operation == "INSERT"
    assert resources == ["/flows/{flow_id}/runs/{run_number}/tasks".format(**_task_step),
                         "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks".format(
                             **_task_step),
                         "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}".format(**_task_step)]
    assert result == _task_step

    # Add end Step
    _should_call = _set_notify_handler(cli, loop)
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="end", run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body

    # Wait for results
    operation, resources, result = await wait_for(_should_call, TIMEOUT_FUTURE)
    assert operation == "INSERT"
    assert resources == ["/flows/{flow_id}/runs/{run_number}/steps".format(**_step),
                         "/flows/{flow_id}/runs/{run_number}/steps/{step_name}".format(**_step)]
    assert result == _step

    # Add new Task to end Step
    _should_call = _set_notify_handler(cli, loop)
    _task_end = (await add_task(db,
                                flow_id=_run.get("flow_id"),
                                step_name="end",
                                run_number=_run.get("run_number"),
                                run_id=_run.get("run_id"))).body
    _task_end['status'] = 'running'

    # Wait for results
    operation, resources, result = await wait_for(_should_call, TIMEOUT_FUTURE)
    assert operation == "INSERT"
    assert resources == ["/flows/{flow_id}/runs/{run_number}/tasks".format(**_task_end),
                         "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks".format(
                             **_task_end),
                         "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}".format(**_task_end)]
    assert result == _task_end

    # Add artifact (Task will be done)
    _artifact_step = (await add_artifact(db,
                                         flow_id=_task_step.get("flow_id"),
                                         run_number=_task_step.get(
                                             "run_number"),
                                         run_id=_task_step.get("run_id"),
                                         step_name=_task_step.get("step_name"),
                                         task_id=_task_step.get("task_id"),
                                         task_name=_task_step.get("task_name"),
                                         artifact={"name": "_task_ok"})).body

    _should_call_artifact = Future(loop=loop)
    _should_call_task_done = Future(loop=loop)

    async def _event_handler_task_done(operation: str, resources: List[str], result: Dict):
        if operation == "INSERT":
            _should_call_artifact.set_result([operation, resources, result])
        elif operation == "UPDATE":
            _should_call_task_done.set_result([operation, resources, result])
    cli.server.app.event_emitter.on('notify', _event_handler_task_done)

    # Wait for results
    operation, resources, result = await wait_for(_should_call_artifact, TIMEOUT_FUTURE)
    assert operation == "INSERT"
    assert result == _artifact_step

    operation, resources, result = await wait_for(_should_call_task_done, TIMEOUT_FUTURE)
    assert operation == "UPDATE"
    assert result["finished_at"] > _task_step["ts_epoch"]
    assert result["duration"] > 0

    cli.server.app.event_emitter.remove_all_listeners()

    # Add artifact (Run will be done)
    _artifact_end = (await add_artifact(db,
                                        flow_id=_task_end.get("flow_id"),
                                        run_number=_task_end.get(
                                            "run_number"),
                                        run_id=_task_end.get("run_id"),
                                        step_name=_task_end.get("step_name"),
                                        task_id=_task_end.get("task_id"),
                                        task_name=_task_end.get("task_name"),
                                        artifact={"name": "_task_ok"})).body

    _should_call_artifact = Future(loop=loop)
    _should_call_task_done = Future(loop=loop)
    _should_call_run_done = Future(loop=loop)

    async def _event_handler_task_done(operation: str, resources: List[str], result: Dict):
        if operation == "INSERT":
            _should_call_artifact.set_result([operation, resources, result])
        elif operation == "UPDATE":
            if "/runs" in resources:
                _should_call_run_done.set_result(
                    [operation, resources, result])
            else:
                _should_call_task_done.set_result(
                    [operation, resources, result])
    cli.server.app.event_emitter.on('notify', _event_handler_task_done)

    # Wait for results
    operation, resources, result = await wait_for(_should_call_artifact, TIMEOUT_FUTURE)
    assert operation == "INSERT"
    assert result == _artifact_end

    operation, resources, result = await wait_for(_should_call_task_done, TIMEOUT_FUTURE)
    assert operation == "UPDATE"
    assert result["finished_at"] > _task_end["ts_epoch"]
    assert result["duration"] > 0

    operation, resources, result = await wait_for(_should_call_run_done, TIMEOUT_FUTURE)
    assert operation == "UPDATE"
    assert result["finished_at"] > _task_end["ts_epoch"]
    assert result["duration"] > 0
    assert result["status"] == "completed"

    cli.server.app.event_emitter.remove_all_listeners()


# Test INSERT and UPDATE pg_notify triggers
# Resource insert order is important here due to foreign keys
# Test artifact attempt_id and task updates related to it
# Task finished_at and attempt_id should always reflect artifact values
async def test_pg_notify_trigger_updates_on_attempt_id(cli, db, loop):
    # Add new Flow
    _should_call = _set_notify_handler(cli, loop)
    _flow = (await add_flow(db, flow_id="HelloFlow")).body

    # Wait for results
    await wait_for(_should_call, 0.1)

    # Add new Run
    _should_call = _set_notify_handler(cli, loop)
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body

    # Wait for results
    await wait_for(_should_call, 0.1)

    # Add normal Step
    _should_call = _set_notify_handler(cli, loop)
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="step", run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body

    # Wait for results
    await wait_for(_should_call, 0.1)

    # Add new Task
    _should_call = _set_notify_handler(cli, loop)
    _task_step = (await add_task(db,
                                 flow_id=_step.get("flow_id"),
                                 step_name=_step.get("step_name"),
                                 run_number=_step.get("run_number"),
                                 run_id=_step.get("run_id"))).body

    # Wait for results
    await wait_for(_should_call, 0.1)

    # Add artifact with attempt_id = 0 (Task will be done)
    _artifact_step = (await add_artifact(db,
                                         flow_id=_task_step.get("flow_id"),
                                         run_number=_task_step.get(
                                             "run_number"),
                                         run_id=_task_step.get("run_id"),
                                         step_name=_task_step.get("step_name"),
                                         task_id=_task_step.get("task_id"),
                                         task_name=_task_step.get("task_name"),
                                         artifact={"name": "_task_ok", "attempt_id": 0})).body

    _should_call_artifact = Future(loop=loop)
    _should_call_task_done = Future(loop=loop)

    async def _event_handler_task_done(operation: str, resources: List[str], result: Dict):
        if operation == "INSERT":
            _should_call_artifact.set_result([operation, resources, result])
        elif operation == "UPDATE":
            _should_call_task_done.set_result([operation, resources, result])
    cli.server.app.event_emitter.on('notify', _event_handler_task_done)

    # Wait for results
    await wait_for(_should_call_artifact, 0.1)

    operation, _, result = await wait_for(_should_call_task_done, 0.1)
    assert operation == "UPDATE"
    assert result["finished_at"] == _artifact_step["ts_epoch"]
    assert result["attempt_id"] == 0

    cli.server.app.event_emitter.remove_all_listeners()

    # Add artifact with attempt_id = 1 (Task will be done)
    _artifact_step = (await add_artifact(db,
                                         flow_id=_task_step.get("flow_id"),
                                         run_number=_task_step.get(
                                             "run_number"),
                                         run_id=_task_step.get("run_id"),
                                         step_name=_task_step.get("step_name"),
                                         task_id=_task_step.get("task_id"),
                                         task_name=_task_step.get("task_name"),
                                         artifact={"name": "_task_ok", "attempt_id": 1})).body

    _should_call_artifact = Future(loop=loop)
    _should_call_task_done = Future(loop=loop)

    async def _event_handler_task_done(operation: str, resources: List[str], result: Dict):
        if operation == "INSERT":
            _should_call_artifact.set_result([operation, resources, result])
        elif operation == "UPDATE":
            _should_call_task_done.set_result([operation, resources, result])
    cli.server.app.event_emitter.on('notify', _event_handler_task_done)

    # Wait for results
    await wait_for(_should_call_artifact, 0.1)

    operation, _, result = await wait_for(_should_call_task_done, 0.1)
    assert operation == "UPDATE"
    assert result["finished_at"] == _artifact_step["ts_epoch"]
    assert result["attempt_id"] == 1

    cli.server.app.event_emitter.remove_all_listeners()

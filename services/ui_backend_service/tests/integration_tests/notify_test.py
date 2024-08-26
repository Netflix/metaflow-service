import pytest
from .utils import (
    cli, init_db, clean_db,
    add_flow, add_run, add_step, add_task, add_artifact, add_metadata,
    TIMEOUT_FUTURE
)
from typing import List, Dict
from asyncio import Future, wait_for

from services.ui_backend_service.api.notify import ListenNotify

pytestmark = [pytest.mark.integration_tests]

# Fixtures begin


@pytest.fixture
async def db(cli):
    async_db = await init_db(cli)

    # Init after DB is ready so that connection pool is available
    app = cli.server.app
    ListenNotify(app, async_db, app.event_emitter)

    yield async_db
    await clean_db(async_db)

# Fixtures end


def _set_notify_handler(cli):
    should_call = Future()

    async def event_handler(operation: str, resources: List[str], result: Dict, table, filter_dict):
        should_call.set_result([operation, resources, result])
    cli.server.app.event_emitter.once('notify', event_handler)

    return should_call


async def test_pg_notify_simple_flow(cli, db):
    # Add new Flow
    _should_call = _set_notify_handler(cli)
    _flow = (await add_flow(db, flow_id="HelloFlow")).body

    # Wait for results
    operation, resources, result = await wait_for(_should_call, TIMEOUT_FUTURE)
    assert operation == "INSERT"
    assert resources == ["/flows", "/flows/HelloFlow"]
    assert result == assertable_flow(_flow)


# Test INSERT and UPDATE pg_notify triggers
# Resource insert order is important here due to foreign keys
async def test_pg_notify_trigger_updates_on_task(cli, db):
    # Add new Flow
    _should_call = _set_notify_handler(cli)
    _flow = (await add_flow(db, flow_id="HelloFlow")).body

    # Wait for results
    operation, resources, result = await wait_for(_should_call, TIMEOUT_FUTURE)
    assert operation == "INSERT"
    assert resources == ["/flows", "/flows/HelloFlow"]
    assert result == assertable_flow(_flow)

    # Add new Run
    _should_call = _set_notify_handler(cli)
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    # _run["status"] = "running"

    # Wait for results
    operation, resources, result = await wait_for(_should_call, TIMEOUT_FUTURE)
    assert operation == "INSERT"
    assert resources == ["/runs", "/flows/HelloFlow/runs",
                         "/flows/HelloFlow/runs/{run_number}".format(**_run)]
    assert result == assertable_run(_run)

    # Add normal Step
    _should_call = _set_notify_handler(cli)
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="step", run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body

    # Wait for results
    operation, resources, result = await wait_for(_should_call, TIMEOUT_FUTURE)
    assert operation == "INSERT"
    assert resources == ["/flows/{flow_id}/runs/{run_number}/steps".format(**_step),
                         "/flows/{flow_id}/runs/{run_number}/steps/{step_name}".format(**_step)]
    assert result == assertable_step(_step)

    # Add new Task
    _should_call = _set_notify_handler(cli)
    _task_step = (await add_task(db,
                                 flow_id=_step.get("flow_id"),
                                 step_name=_step.get("step_name"),
                                 run_number=_step.get("run_number"),
                                 run_id=_step.get("run_id"))).body

    # Wait for results
    operation, resources, result = await wait_for(_should_call, TIMEOUT_FUTURE)
    assert operation == "INSERT"
    assert resources == ["/flows/{flow_id}/runs/{run_number}/tasks".format(**_task_step),
                         "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks".format(
                             **_task_step),
                         "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}".format(**_task_step),
                         "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/attempts".format(**_task_step)
                         ]
    assert result == assertable_task(_task_step)

    # Add end Step
    _should_call = _set_notify_handler(cli)
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="end", run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body

    # Wait for results
    operation, resources, result = await wait_for(_should_call, TIMEOUT_FUTURE)
    assert operation == "INSERT"
    assert resources == ["/flows/{flow_id}/runs/{run_number}/steps".format(**_step),
                         "/flows/{flow_id}/runs/{run_number}/steps/{step_name}".format(**_step)]
    assert result == assertable_step(_step)

    # Add new Task to end Step
    _should_call = _set_notify_handler(cli)
    _task_end = (await add_task(db,
                                flow_id=_run.get("flow_id"),
                                step_name="end",
                                run_number=_run.get("run_number"),
                                run_id=_run.get("run_id"))).body

    # Wait for results
    operation, resources, result = await wait_for(_should_call, TIMEOUT_FUTURE)
    assert operation == "INSERT"
    assert resources == ["/flows/{flow_id}/runs/{run_number}/tasks".format(**_task_end),
                         "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks".format(
                             **_task_end),
                         "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}".format(**_task_end),
                         "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/attempts".format(**_task_end)]
    assert result == assertable_task(_task_end)

    cli.server.app.event_emitter.remove_all_listeners()
    # NOTE: We used to test for legacy cases here where adding a task_ok artifact 
    # would broadcast an updated status for a task and possibly run.


# NOTE: task_ok artifacts used to be able to drive task attempt updates.
# We have since gotten rid of triggers on artifacts so all task attempt updates are metadata driven.

async def test_pg_notify_dag_code_package_url(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="start", run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body
    _task = (await add_task(db,
                            flow_id=_step.get("flow_id"),
                            step_name=_step.get("step_name"),
                            run_number=_step.get("run_number"),
                            run_id=_step.get("run_id"))).body

    cli.server.app.event_emitter.remove_all_listeners()

    _should_call_dag = Future()

    async def _event_handler_dag(flow_name: str, run_number: str):
        if not _should_call_dag.done():
            _should_call_dag.set_result([flow_name, run_number])
    cli.server.app.event_emitter.on('preload-dag', _event_handler_dag)

    _metadata = (await add_metadata(db,
                                    flow_id=_task.get("flow_id"),
                                    run_number=_task.get("run_number"),
                                    run_id=_task.get("run_id"),
                                    step_name=_task.get("step_name"),
                                    task_id=_task.get("task_id"),
                                    task_name=_task.get("task_name"),
                                    metadata={
                                        "field_name": "code-package-url",
                                        "value": "s3://foobar",
                                        "type": "type"})).body

    flow_name, run_number = await wait_for(_should_call_dag, TIMEOUT_FUTURE)
    assert flow_name == "HelloFlow"
    assert str(run_number) == str(_task.get("run_number"))


async def test_pg_notify_dag_code_package(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="start", run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body
    _task = (await add_task(db,
                            flow_id=_step.get("flow_id"),
                            step_name=_step.get("step_name"),
                            run_number=_step.get("run_number"),
                            run_id=_step.get("run_id"))).body

    cli.server.app.event_emitter.remove_all_listeners()

    _should_call_dag = Future()

    async def _event_handler_dag(flow_name: str, run_number: str):
        if not _should_call_dag.done():
            _should_call_dag.set_result([flow_name, run_number])
    cli.server.app.event_emitter.on('preload-dag', _event_handler_dag)

    _metadata = (await add_metadata(db,
                                    flow_id=_task.get("flow_id"),
                                    run_number=_task.get("run_number"),
                                    run_id=_task.get("run_id"),
                                    step_name=_task.get("step_name"),
                                    task_id=_task.get("task_id"),
                                    task_name=_task.get("task_name"),
                                    metadata={
                                        "field_name": "code-package",
                                        "value": '{"location": "s3://foobar"}',
                                        "type": "type"})).body

    flow_name, run_number = await wait_for(_should_call_dag, TIMEOUT_FUTURE)
    assert flow_name == "HelloFlow"
    assert str(run_number) == str(_task.get("run_number"))

# Helpers


def assertable_flow(flow):
    return {"flow_id": flow.get("flow_id")}


def assertable_run(run):
    return {
        "flow_id": run.get("flow_id"),
        "run_number": int(run.get("run_number")),
        "last_heartbeat_ts": run.get("last_heartbeat_ts")
    }


def assertable_step(step, keys=["step_name"]):
    return {
        "flow_id": step.get("flow_id"),
        "run_number": int(step.get("run_number")),
        "step_name": step.get("step_name")
    }


def assertable_task(task):
    return {
        "flow_id": task.get("flow_id"),
        "run_number": int(task.get("run_number")),
        "step_name": task.get("step_name"),
        "task_id": int(task.get("task_id"))
    }


def assertable_artifact(artifact):
    return {
        "flow_id": artifact.get("flow_id"),
        "run_number": int(artifact.get("run_number")),
        "step_name": artifact.get("step_name"),
        "task_id": int(artifact.get("task_id")),
        "attempt_id": int(artifact.get("attempt_id")),
        "name": artifact.get("name"),
    }

# Helpers end

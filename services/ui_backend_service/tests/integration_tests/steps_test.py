import pytest
from .utils import (
    init_app, init_db, clean_db,
    add_flow, add_run, add_step, add_task,
    add_artifact, get_heartbeat_ts,
    _test_list_resources, _test_single_resource
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


async def test_list_steps(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body

    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps".format(**_run), 200, [])

    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="step", run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body

    _, data = await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps".format(**_step), 200, None)

    assert len(data) == 1
    assert data[0]['run_number'] == int(_run.get('run_number'))
    assert data[0]['step_name'] == 'step'


async def test_single_step(cli, db):
    await _test_single_resource(cli, db, "/flows/HelloFlow/runs/404/steps/none", 404, {})

    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="step", run_number=_run.get("run_number"))).body

    _, data = await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}".format(**_step), 200, None)

    assert data['run_number'] == int(_run.get('run_number'))
    assert data['step_name'] == 'step'


async def test_step_duration(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="step", run_number=_run.get("run_number"))).body
    _step['run_id'] = _run['run_number']
    _step['duration'] = None

    # No tasks exist so step should have no duration.
    await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}".format(**_step), 200, _step)

    # existing task should have an effect on step duration
    _task = (await add_task(
        db,
        flow_id=_flow.get("flow_id"),
        run_number=_run.get("run_number"),
        step_name=_step.get("step_name"),
        last_heartbeat_ts=get_heartbeat_ts(offset=10)
    )).body

    # if only task heartbeat exists, this should be used for the step duration
    _step['duration'] = _task['last_heartbeat_ts'] * 1000 - _step['ts_epoch']

    await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}".format(**_step), 200, _step)

    # more recent _task_ok artifact timestamp should be used in favor of last_heartbeat if exists.

    _task_ok = (await add_artifact(
        db,
        flow_id=_flow.get("flow_id"),
        run_number=_run.get("run_number"),
        step_name=_step.get("step_name"),
        task_id=_task.get("task_id"),
        artifact={
            "name": "_task_ok",
            "location": "location",
            "ds_type": "ds_type",
            "sha": "sha",
            "type": "type",
            "content_type": "content_type",
            "attempt_id": 0
        }
    )).body

    # update ts_epoch to be newer than the task heartbeat.
    _new_ts = _task['last_heartbeat_ts'] * 1000 + 10
    await db.artifact_table_postgres.update_row(
        filter_dict={
            "flow_id": _task_ok.get("flow_id"),
            "run_number": _task_ok.get("run_number"),
            "step_name": _task_ok.get("step_name"),
            "task_id": _task_ok.get("task_id")
        },
        update_dict={
            "ts_epoch": _new_ts
        }
    )

    # _task_ok should be used in favor of heartbeat_ts for step duration.
    _step['duration'] = _new_ts - _step['ts_epoch']

    await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}".format(**_step), 200, _step)

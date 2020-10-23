import pytest
from .utils import (
    init_app, init_db, clean_db,
    add_flow, add_run, add_artifact,
    _test_list_resources, _test_single_resource, get_heartbeat_ts
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


async def test_list_runs(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body

    await _test_list_resources(cli, db, "/runs", 200, [])
    await _test_list_resources(cli, db, "/flows/{flow_id}/runs".format(**_flow), 200, [])

    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _run["status"] = "running"

    await _test_list_resources(cli, db, "/runs", 200, [_run])
    await _test_list_resources(cli, db, "/flows/{flow_id}/runs".format(**_flow), 200, [_run])


async def test_list_runs_non_numerical(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body

    await _test_list_resources(cli, db, "/runs", 200, [])
    await _test_list_resources(cli, db, "/flows/{flow_id}/runs".format(**_flow), 200, [])

    _run = (await add_run(db, flow_id=_flow.get("flow_id"), run_id="hello")).body
    _run["status"] = "running"

    await _test_list_resources(cli, db, "/runs", 200, [_run])
    await _test_list_resources(cli, db, "/flows/{flow_id}/runs".format(**_flow), 200, [_run])


async def test_single_run(cli, db):
    await _test_single_resource(cli, db, "/flows/HelloFlow/runs/404", 404, {})

    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _run["status"] = "running"

    await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}".format(**_run), 200, _run)


async def test_single_run_non_numerical(cli, db):
    await _test_single_resource(cli, db, "/flows/HelloFlow/runs/hello", 404, {})

    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"), run_id="hello")).body
    _run["status"] = "running"

    await _test_single_resource(cli, db, "/flows/{flow_id}/runs/hello".format(**_run), 200, _run)

async def test_run_status_with_heartbeat(cli, db):
    await _test_single_resource(cli, db, "/flows/HelloFlow/runs/hello", 404, {})

    _flow = (await add_flow(db, flow_id="HelloFlow")).body

    # A run with no end task and an expired heartbeat should count as failed.
    _run_failed = (await add_run(db, flow_id=_flow.get("flow_id"), last_heartbeat_ts=1)).body
    _run_failed["status"] = "failed"
    _run_failed["last_heartbeat_ts"] = 1
    # NOTE: heartbeat_ts and ts_epoch have different units.
    _run_failed["duration"] = _run_failed["last_heartbeat_ts"]*1000 - _run_failed["ts_epoch"]

    await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}".format(**_run_failed), 200, _run_failed)
    
    # A run with recent heartbeat and no end task should count as running.
    _beat = get_heartbeat_ts()
    _run_running = (await add_run(db, flow_id=_flow.get("flow_id"), last_heartbeat_ts=_beat)).body
    _run_running["status"] = "running"
    _run_running["last_heartbeat_ts"] = _beat
    # NOTE: heartbeat_ts and ts_epoch have different units.
    _run_running["duration"] = _run_running["last_heartbeat_ts"]*1000 - _run_running["ts_epoch"]

    await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}".format(**_run_running), 200, _run_running)

    # A run with an end task _task_ok artifact should count as completed.
    _beat = get_heartbeat_ts()
    _run_complete = (await add_run(db, flow_id=_flow.get("flow_id"), last_heartbeat_ts=_beat)).body

    _artifact = (await add_artifact(
                        db,
                        flow_id=_run_complete.get("flow_id"),
                        run_number=_run_complete.get("run_number"),
                        step_name="end",
                        task_id=1,
                        artifact={
                            "name": "_task_ok",
                            "location": "location",
                            "ds_type": "ds_type",
                            "sha": "sha",
                            "type": "type",
                            "content_type": "content_type",
                            "attempt_id": 0
                        })).body
    
    _run_complete["status"] = "completed"
    _run_complete["finished_at"] = _artifact["ts_epoch"]
    _run_complete["duration"] = _run_complete["finished_at"] - _run_complete["ts_epoch"]

    await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}".format(**_run_complete), 200, _run_complete)

import pytest
import time
from .utils import (
    init_app, init_db, clean_db,
    add_flow, add_run, add_artifact,
    add_step, add_task, add_metadata,
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
    _run["user"] = None
    _run["run"] = _run["run_number"]
    _run["duration"] = int(round(time.time() * 1000)) - _run["ts_epoch"]

    await _test_list_resources(cli, db, "/runs", 200, [_run], approx_keys=["duration"])
    await _test_list_resources(cli, db, "/flows/{flow_id}/runs".format(**_flow), 200, [_run], approx_keys=["duration"])


async def test_list_runs_real_user(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body

    _run = (await add_run(db, flow_id=_flow.get("flow_id"), user_name="hello", system_tags=["user:hello"])).body
    _run["status"] = "running"
    _run["user"] = "hello"
    _run["duration"] = int(round(time.time() * 1000)) - _run["ts_epoch"]
    _run["run"] = _run["run_number"]

    await _test_list_resources(cli, db, "/runs", 200, [_run], approx_keys=["duration"])


async def test_list_runs_real_user_filter(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body

    _run = (await add_run(db, flow_id=_flow.get("flow_id"), user_name="hello", system_tags=["user:hello"])).body
    _run["status"] = "running"
    _run["user"] = "hello"
    _run["duration"] = int(round(time.time() * 1000)) - _run["ts_epoch"]
    _run["run"] = _run["run_number"]

    await _test_list_resources(cli, db, "/runs?user=hello", 200, [_run], approx_keys=["duration"])


async def test_list_runs_real_user_filter_null(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    (await add_run(db, flow_id=_flow.get("flow_id"), user_name="foo", system_tags=["user:bar"])).body
    await _test_list_resources(cli, db, "/runs?user=foo", 200, [])


async def test_list_runs_real_user_none(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body

    async def _test_run_with_user(expected_user: str = None, user_name: str = None, tag: str = None):
        _run = (await add_run(db, flow_id=_flow.get("flow_id"), user_name=user_name, system_tags=["user:" + tag] if tag else [])).body
        _run["status"] = "running"
        _run["user"] = expected_user
        _run["duration"] = int(round(time.time() * 1000)) - _run["ts_epoch"]
        _run["run"] = _run["run_number"]

        await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}".format(**_run), 200, _run, approx_keys=["duration"])

    await _test_run_with_user(expected_user="foo", user_name="foo", tag="foo")

    await _test_run_with_user(expected_user=None, user_name="foo")
    await _test_run_with_user(expected_user=None, tag="foo")
    await _test_run_with_user(expected_user=None, user_name="foo", tag="bar")


async def test_list_runs_non_numerical(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body

    await _test_list_resources(cli, db, "/runs", 200, [])
    await _test_list_resources(cli, db, "/flows/{flow_id}/runs".format(**_flow), 200, [])

    _run = (await add_run(db, flow_id=_flow.get("flow_id"), run_id="hello")).body
    _run["status"] = "running"

    _, data = await _test_list_resources(cli, db, "/runs", 200, None)

    assert len(data) == 1
    assert data[0]['run_id'] == 'hello'
    assert data[0]['run_number'] != 'hello'

    _, data = await _test_list_resources(cli, db, "/flows/{flow_id}/runs".format(**_flow), 200, None)

    assert len(data) == 1
    assert data[0]['run_id'] == 'hello'
    assert data[0]['run_number'] != 'hello'


async def test_single_run(cli, db):
    await _test_single_resource(cli, db, "/flows/HelloFlow/runs/404", 404, {})

    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _run["status"] = "running"
    _run["user"] = None
    _run["duration"] = int(round(time.time() * 1000)) - _run["ts_epoch"]
    _run["run"] = _run["run_number"]

    await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}".format(**_run), 200, _run, approx_keys=["duration"])


async def test_single_run_non_numerical(cli, db):
    await _test_single_resource(cli, db, "/flows/HelloFlow/runs/hello", 404, {})

    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"), run_id="hello")).body
    _run["status"] = "running"

    _, data = await _test_single_resource(cli, db, "/flows/{flow_id}/runs/hello".format(**_run), 200, None)

    assert data['run_id'] == 'hello'
    assert data['run_number'] != 'hello'


async def test_single_run_run_column_id(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"), run_id="hello")).body

    _, data = await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}".format(**_run), 200, None)

    assert data['run'] == 'hello'


async def test_single_run_run_column_number(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body

    _, data = await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}".format(**_run), 200, None)

    assert data['run'] == str(_run['run_number'])


async def test_run_status_with_heartbeat(cli, db):
    await _test_single_resource(cli, db, "/flows/HelloFlow/runs/hello", 404, {})

    _flow = (await add_flow(db, flow_id="HelloFlow")).body

    # A run with no end task and an expired heartbeat should count as failed.
    _run_failed = (await add_run(db, flow_id=_flow.get("flow_id"), last_heartbeat_ts=1)).body
    # even when a run has a heartbeat, it still requires a task that has failed via attempt_ok=false OR by an expired heartbeat.
    _step = (await add_step(db, flow_id=_run_failed.get("flow_id"), step_name="end", run_number=_run_failed.get("run_number"), run_id=_run_failed.get("run_id"))).body
    _task = (await add_task(db,
                            flow_id=_step.get("flow_id"),
                            step_name=_step.get("step_name"),
                            run_number=_step.get("run_number"),
                            run_id=_step.get("run_id"),
                            last_heartbeat_ts=1)).body
    _run_failed["status"] = "failed"
    _run_failed["user"] = None
    _run_failed["run"] = _run_failed["run_number"]
    _run_failed["last_heartbeat_ts"] = 1
    # NOTE: heartbeat_ts and ts_epoch have different units.
    _run_failed["duration"] = _run_failed["last_heartbeat_ts"] * 1000 - _run_failed["ts_epoch"]
    _run_failed["finished_at"] = _run_failed["last_heartbeat_ts"] * 1000

    await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}".format(**_run_failed), 200, _run_failed)

    # A run with recent heartbeat and no end task should count as running.
    _beat = get_heartbeat_ts()
    _run_running = (await add_run(db, flow_id=_flow.get("flow_id"), last_heartbeat_ts=_beat)).body
    _run_running["status"] = "running"
    _run_running["user"] = None
    _run_running["run"] = _run_running["run_number"]
    _run_running["last_heartbeat_ts"] = _beat
    # NOTE: heartbeat_ts and ts_epoch have different units.
    _run_running["duration"] = _run_running["last_heartbeat_ts"] * 1000 - _run_running["ts_epoch"]

    await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}".format(**_run_running), 200, _run_running)

    # A run with an end task attempt_ok metadata should count as completed.
    _beat = get_heartbeat_ts()
    _run_complete = (await add_run(db, flow_id=_flow.get("flow_id"), last_heartbeat_ts=_beat)).body
    _step = (await add_step(db, flow_id=_run_complete.get("flow_id"), step_name="end", run_number=_run_complete.get("run_number"), run_id=_run_complete.get("run_id"))).body
    _task = (await add_task(db,
                            flow_id=_step.get("flow_id"),
                            step_name=_step.get("step_name"),
                            run_number=_step.get("run_number"),
                            run_id=_step.get("run_id"))).body
    # _artifact = (await add_artifact(
    #     db,
    #     flow_id=_run_complete.get("flow_id"),
    #     run_number=_run_complete.get("run_number"),
    #     step_name="end",
    #     task_id=1,
    #     artifact={
    #         "name": "_task_ok",
    #         "location": "location",
    #         "ds_type": "ds_type",
    #         "sha": "sha",
    #         "type": "type",
    #         "content_type": "content_type",
    #         "attempt_id": 0
    #     })).body
    _metadata = (await add_metadata(db,
                                    flow_id=_task.get("flow_id"),
                                    run_number=_task.get("run_number"),
                                    run_id=_task.get("run_id"),
                                    step_name=_task.get("step_name"),
                                    task_id=_task.get("task_id"),
                                    task_name=_task.get("task_name"),
                                    tags=["attempt_id:0"],
                                    metadata={
                                        "field_name": "attempt_ok",
                                        "value": "True",  # run status = 'completed'
                                        "type": "internal_attempt_status"})).body

    _run_complete["status"] = "completed"
    _run_complete["user"] = None
    _run_complete["run"] = _run_complete["run_number"]
    _run_complete["finished_at"] = _metadata["ts_epoch"]
    _run_complete["duration"] = _run_complete["finished_at"] - _run_complete["ts_epoch"]

    await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}".format(**_run_complete), 200, _run_complete)


async def Xtest_old_run_status_without_heartbeat(cli, db):
    # Test does not make sense with current changes.
    # Run is only complete if it records attempt_ok True metadata. _task_ok artifact is not part of the check anymore.
    await _test_single_resource(cli, db, "/flows/HelloFlow/runs/hello", 404, {})

    _flow = (await add_flow(db, flow_id="HelloFlow")).body

    # A run with epoch and no end step _task_ok should count as running.
    _run_running = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _run_running["status"] = "running"
    _run_running["user"] = None
    _run_running["run"] = _run_running["run_number"]
    _run_running["duration"] = int(round(time.time() * 1000)) - _run_running["ts_epoch"]

    await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}".format(**_run_running), 200, _run_running, approx_keys=["duration"])

    # A run with an end task _task_ok artifact should count as completed.
    _run_complete = (await add_run(db, flow_id=_flow.get("flow_id"))).body

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
    _run_complete["user"] = None
    _run_complete["run"] = _run_complete["run_number"]
    _run_complete["finished_at"] = _artifact["ts_epoch"]
    _run_complete["duration"] = _run_complete["finished_at"] - _run_complete["ts_epoch"]

    await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}".format(**_run_complete), 200, _run_complete)

    # A run with no end task and a timestamp older than two weeks should count as failed.
    _run_failed = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _old_ts = _run_failed["ts_epoch"] - (60 * 60 * 24 * 14 * 1000 + 3600)
    # TODO: consider mocking get_db_ts_epoch_str() in the database adapter to be able to insert custom epochs.
    await db.run_table_postgres.update_row(
        filter_dict={
            "flow_id": _run_failed.get("flow_id"),
            "run_number": _run_failed.get("run_number")
        },
        update_dict={
            "ts_epoch": _old_ts
        }
    )
    _run_failed["ts_epoch"] = _old_ts
    # finished at should be none, as we can not determine a clear one.
    _run_failed["finished_at"] = None
    _run_failed["duration"] = None  # duration should also be none as it is indeterminate.
    _run_failed["status"] = "failed"
    _run_failed["user"] = None
    _run_failed["run"] = _run_failed["run_number"]

    await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}".format(**_run_failed), 200, _run_failed)


async def test_single_run_attempt_ok_completed(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="end", run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body
    _task = (await add_task(db,
                            flow_id=_step.get("flow_id"),
                            step_name=_step.get("step_name"),
                            run_number=_step.get("run_number"),
                            run_id=_step.get("run_id"))).body

    _metadata = (await add_metadata(db,
                                    flow_id=_task.get("flow_id"),
                                    run_number=_task.get("run_number"),
                                    run_id=_task.get("run_id"),
                                    step_name=_task.get("step_name"),
                                    task_id=_task.get("task_id"),
                                    task_name=_task.get("task_name"),
                                    tags=["attempt_id:0"],
                                    metadata={
                                        "field_name": "attempt_ok",
                                        "value": "True",  # run status = 'completed'
                                        "type": "internal_attempt_status"})).body

    # We are expecting run status 'completed'
    _run["status"] = "completed"
    _run["user"] = None
    _run["run"] = _run["run_number"]
    _run["finished_at"] = _metadata["ts_epoch"]
    _run["duration"] = _run["finished_at"] - _run["ts_epoch"]

    await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}".format(**_run), 200, _run)


async def test_single_run_attempt_ok_failed(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="end", run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body
    _task = (await add_task(db,
                            flow_id=_step.get("flow_id"),
                            step_name=_step.get("step_name"),
                            run_number=_step.get("run_number"),
                            run_id=_step.get("run_id"))).body

    (await add_metadata(db,
                        flow_id=_task.get("flow_id"),
                        run_number=_task.get("run_number"),
                        run_id=_task.get("run_id"),
                        step_name=_task.get("step_name"),
                        task_id=_task.get("task_id"),
                        task_name=_task.get("task_name"),
                        metadata={
                            "field_name": "attempt",
                            "value": "0",  # run status = 'running'
                            "type": "attempt"})).body

    _metadata = (await add_metadata(db,
                                    flow_id=_task.get("flow_id"),
                                    run_number=_task.get("run_number"),
                                    run_id=_task.get("run_id"),
                                    step_name=_task.get("step_name"),
                                    task_id=_task.get("task_id"),
                                    task_name=_task.get("task_name"),
                                    tags=["attempt_id:0"],
                                    metadata={
                                        "field_name": "attempt_ok",
                                        "value": "False",  # run status = 'failed'
                                        "type": "internal_attempt_status"})).body

    # We are expecting run status 'failed'
    _run["status"] = "failed"
    _run["user"] = None
    _run["run"] = _run["run_number"]
    _run["finished_at"] = _metadata["ts_epoch"]
    _run["duration"] = _run["finished_at"] - _run["ts_epoch"]

    await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}".format(**_run), 200, _run)

    _second_attempt = (await add_metadata(db,
                                          flow_id=_task.get("flow_id"),
                                          run_number=_task.get("run_number"),
                                          run_id=_task.get("run_id"),
                                          step_name=_task.get("step_name"),
                                          task_id=_task.get("task_id"),
                                          task_name=_task.get("task_name"),
                                          metadata={
                                              "field_name": "attempt",
                                              "value": "1",  # run status = 'running'
                                              "type": "attempt"})).body

    # run should count as running again, as a newer 'end' attempt is still going on.
    _run["status"] = "running"
    _run["finished_at"] = None
    _run["duration"] = get_heartbeat_ts() * 1000 - _run["ts_epoch"]

    await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}".format(**_run), 200, _run, approx_keys=["duration"])

    _second_metadata = (await add_metadata(db,
                                           flow_id=_task.get("flow_id"),
                                           run_number=_task.get("run_number"),
                                           run_id=_task.get("run_id"),
                                           step_name=_task.get("step_name"),
                                           task_id=_task.get("task_id"),
                                           task_name=_task.get("task_name"),
                                           tags=["attempt_id:1"],
                                           metadata={
                                               "field_name": "attempt_ok",
                                               "value": "True",  # run status = 'failed'
                                               "type": "internal_attempt_status"})).body

    # run should count as running again, as a newer 'end' attempt is still going on.
    _run["status"] = "completed"
    _run["finished_at"] = _second_metadata["ts_epoch"]
    _run["duration"] = _run["finished_at"] - _run["ts_epoch"]

    await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}".format(**_run), 200, _run)

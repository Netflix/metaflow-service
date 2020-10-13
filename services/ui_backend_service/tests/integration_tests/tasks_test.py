import pytest
from .utils import (
    init_app, init_db, clean_db,
    add_flow, add_run, add_step, add_task, add_artifact,
    _test_list_resources, _test_single_resource, add_metadata
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


async def test_list_tasks(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="step", run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body

    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/tasks".format(**_step), 200, [])
    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks".format(**_step), 200, [])

    _task = await create_task(db, step=_step)

    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/tasks".format(**_task), 200, [_task])
    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks".format(**_task), 200, [_task])


async def test_list_tasks_non_numerical(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="step", run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body

    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/tasks".format(**_step), 200, [])
    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks".format(**_step), 200, [])

    _task = await create_task(db, step=_step, task_name="bar")

    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/tasks".format(**_task), 200, [_task])
    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks".format(**_task), 200, [_task])


async def test_single_task(cli, db):
    await _test_single_resource(cli, db, "/flows/HelloFlow/runs/404/steps/none/tasks/5", 404, {})

    _task = await create_task(db)

    await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}".format(**_task), 200, _task)


async def test_single_task_non_numerical(cli, db):
    _task = await create_task(db, task_name="bar")

    await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}".format(**_task), 200, _task)
    await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/bar".format(**_task), 200, _task)


async def test_list_old_metadata_task_attempts(cli, db):
    # Test tasks with old (missing attempt) metadata
    _task = await create_task(db)

    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/attempts".format(**_task), 200, [_task])

    _artifact_first = await create_ok_artifact_for_task(db, _task)
    _artifact_second = await create_ok_artifact_for_task(db, _task, attempt=1)

    _task['status'] = 'completed'

    _task_first_attempt = dict(_task)
    _task_second_attempt = dict(_task)

    _task_first_attempt['attempt_id'] = 0
    # as no other timestamps are available, the task ts_epoch will be used for started_at
    _task_first_attempt['started_at'] = _task_first_attempt['ts_epoch']
    _task_first_attempt['finished_at'] = _artifact_first['ts_epoch']
    _task_first_attempt['duration'] = _artifact_first['ts_epoch'] - \
        _task_first_attempt['ts_epoch']

    _task_second_attempt['attempt_id'] = 1
    # as no other timestamps are available, the task ts_epoch will be used for started_at
    _task_second_attempt['started_at'] = _task_second_attempt['ts_epoch']
    _task_second_attempt['finished_at'] = _artifact_second['ts_epoch']
    _task_second_attempt['duration'] = _artifact_second['ts_epoch'] - \
        _task_second_attempt['ts_epoch']

    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks?task_id={task_id}".format(**_task), 200, [_task_second_attempt, _task_first_attempt])

    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/attempts".format(**_task), 200, [_task_second_attempt, _task_first_attempt])


async def test_old_metadata_task_with_multiple_attempts(cli, db):
    # Test tasks with old (missing attempt) metadata
    _task = await create_task(db)

    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/attempts".format(**_task), 200, [_task])

    _artifact_first = await create_ok_artifact_for_task(db, _task)

    _artifact_second = await create_ok_artifact_for_task(db, _task, attempt=1)

    _task['status'] = 'completed'

    _task['attempt_id'] = 1
    # as no other timestamps are available, the task ts_epoch will be used for started_at
    _task['started_at'] = _task['ts_epoch']
    _task['finished_at'] = _artifact_second['ts_epoch']
    _task['duration'] = _artifact_second['ts_epoch'] - \
        _task['ts_epoch']

    await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}".format(**_task), 200, _task)

async def test_task_with_attempt_metadata(cli, db):
    _task = await create_task(db)

    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/attempts".format(**_task), 200, [_task])

    _artifact_first = await create_ok_artifact_for_task(db, _task)
    _attempt_first = await create_task_attempt_metadata(db, _task)
    _task['started_at'] = _attempt_first['ts_epoch']
    await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}".format(**_task), 200, _task)

    _attempt_done_first = await create_task_attempt_done_metadata(db, _task)
    _task['finished_at'] = _attempt_done_first['ts_epoch']
    _task['status'] = 'completed'
    _task['duration'] = _task['finished_at']-_task['started_at']
    
    await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}".format(**_task), 200, _task)

async def test_list_task_attempts(cli, db):
    _task = await create_task(db)

    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/attempts".format(**_task), 200, [_task])

    _artifact_first = await create_ok_artifact_for_task(db, _task)
    _attempt_first = await create_task_attempt_metadata(db, _task)
    _attempt_done_first = await create_task_attempt_done_metadata(db, _task)

    # Second task will remain 'running' without attempt-done metadata
    _artifact_second = await create_ok_artifact_for_task(db, _task, attempt = 1)
    _attempt_second = await create_task_attempt_metadata(db, _task, attempt = 1)

    _task_first_attempt = dict(_task)
    _task_second_attempt = dict(_task)

    _task_first_attempt['attempt_id'] = 0
    _task_first_attempt['status'] = 'completed'
    _task_first_attempt['started_at'] = _attempt_first['ts_epoch']
    _task_first_attempt['finished_at'] = _attempt_done_first['ts_epoch']
    _task_first_attempt['duration'] = _task_first_attempt['finished_at'] \
                                    - _task_first_attempt['started_at']

    _task_second_attempt['attempt_id'] = 1
    _task_second_attempt['started_at'] = _attempt_second['ts_epoch']

    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks?task_id={task_id}".format(**_task), 200, [_task_second_attempt, _task_first_attempt])

    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/attempts".format(**_task), 200, [_task_second_attempt, _task_first_attempt])

# Resource Helpers / factories

async def create_ok_artifact_for_task(db, task, attempt=0):
    "Creates and returns a _task_ok artifact for a task"
    _task = (await add_artifact(
                db,
                flow_id=task.get("flow_id"),
                run_number=task.get("run_number"),
                run_id=task.get("run_id"),
                step_name=task.get("step_name"),
                task_id=task.get("task_id"),
                task_name=task.get("task_name"),
                artifact={
                    "name": "_task_ok",
                    "location": "location",
                    "ds_type": "ds_type",
                    "sha": "sha",
                    "type": "type",
                    "content_type": "content_type",
                    "attempt_id": attempt
                })
            ).body
    return _task

async def create_task(db, step = None, status = "running", task_name=None):
    "Creates and returns a task with specific status. Optionally creates the task for a specific step if provided."
    if not step:
        _flow = (await add_flow(db, flow_id="HelloFlow")).body
        _run  = (await add_run(db, flow_id=_flow.get("flow_id"))).body
        step = (await add_step(
                    db,
                    flow_id=_run.get("flow_id"),
                    run_number=_run.get("run_number"),
                    step_name="step")
                ).body
    _task = (await add_task(
                db,
                flow_id=step.get("flow_id"),
                run_number=step.get("run_number"),
                step_name=step.get("step_name"),
                task_name=task_name)
            ).body
    _task['status'] = status

    return _task

async def create_metadata_for_task(db, task, metadata = {}):
    "Creates a metadata record for a task"
    _meta = (await add_metadata(db,
                                flow_id=task.get("flow_id"),
                                run_number=task.get("run_number"),
                                run_id=task.get("run_id"),
                                step_name=task.get("step_name"),
                                task_id=task.get("task_id"),
                                task_name=task.get("task_name"),
                                metadata=metadata)
            ).body
    return _meta

async def create_task_attempt_metadata(db, task, attempt = 0):
    "Create 'attempt' metadata for a task"
    return await create_metadata_for_task(
        db,
        task,
        metadata={
            "type": "attempt",
            "field_name": "attempt",
            "value": str(attempt)
        }
    )

async def create_task_attempt_done_metadata(db, task, attempt = 0):
    "Create 'attempt-done' metadata for a task"
    return await create_metadata_for_task(
        db,
        task,
        metadata={
            "type": "attempt-done",
            "field_name": "attempt-done",
            "value": str(attempt)
        }
    )

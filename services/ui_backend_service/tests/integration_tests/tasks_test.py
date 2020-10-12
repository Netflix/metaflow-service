import pytest
from .utils import (
    init_app, init_db, clean_db,
    add_flow, add_run, add_step, add_task, add_artifact,
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


async def test_list_tasks(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="step", run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body

    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/tasks".format(**_step), 200, [])
    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks".format(**_step), 200, [])

    _task = (await add_task(db,
                            flow_id=_step.get("flow_id"),
                            step_name=_step.get("step_name"),
                            run_number=_step.get("run_number"),
                            run_id=_step.get("run_id"))).body
    _task['status'] = 'running'

    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/tasks".format(**_task), 200, [_task])
    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks".format(**_task), 200, [_task])


async def test_list_tasks_non_numerical(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="step", run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body

    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/tasks".format(**_step), 200, [])
    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks".format(**_step), 200, [])

    _task = (await add_task(db,
                            flow_id=_step.get("flow_id"),
                            step_name=_step.get("step_name"),
                            run_number=_step.get("run_number"),
                            run_id=_step.get("run_id"),
                            task_name="bar")).body
    _task['status'] = 'running'

    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/tasks".format(**_task), 200, [_task])
    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks".format(**_task), 200, [_task])


async def test_single_task(cli, db):
    await _test_single_resource(cli, db, "/flows/HelloFlow/runs/404/steps/none/tasks/5", 404, {})

    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="step", run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body
    _task = (await add_task(db,
                            flow_id=_step.get("flow_id"),
                            step_name=_step.get("step_name"),
                            run_number=_step.get("run_number"),
                            run_id=_step.get("run_id"))).body
    _task['status'] = 'running'

    await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}".format(**_task), 200, _task)


async def test_single_task_non_numerical(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _step = (await add_step(db, flow_id=_run.get("flow_id"), run_number=_run.get("run_number"), step_name="step")).body
    _task = (await add_task(db,
                            flow_id=_step.get("flow_id"),
                            run_number=_run.get("run_number"),
                            step_name=_step.get("step_name"),
                            task_name="bar")).body
    _task['status'] = 'running'

    await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}".format(**_task), 200, _task)
    await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/bar".format(**_task), 200, _task)


async def test_list_task_attempts(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="step", run_number=_run.get("run_number"))).body

    _task = (await add_task(db,
                            flow_id=_step.get("flow_id"),
                            step_name=_step.get("step_name"),
                            run_number=_step.get("run_number"),
                            run_id=_step.get("run_id"))).body
    _task['status'] = 'running'

    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/attempts".format(**_task), 200, [_task])

    _artifact_first = (await add_artifact(db,
                                          flow_id=_task.get("flow_id"),
                                          run_number=_task.get("run_number"),
                                          run_id=_task.get("run_id"),
                                          step_name=_task.get("step_name"),
                                          task_id=_task.get("task_id"),
                                          task_name=_task.get("task_name"),
                                          artifact={
                                              "name": "_task_ok",
                                              "location": "location",
                                              "ds_type": "ds_type",
                                              "sha": "sha",
                                              "type": "type",
                                              "content_type": "content_type",
                                              "attempt_id": 0})).body

    _artifact_second = (await add_artifact(db,
                                           flow_id=_task.get("flow_id"),
                                           run_number=_task.get("run_number"),
                                           run_id=_task.get("run_id"),
                                           step_name=_task.get("step_name"),
                                           task_id=_task.get("task_id"),
                                           task_name=_task.get("task_name"),
                                           artifact={
                                               "name": "_task_ok",
                                               "location": "location",
                                               "ds_type": "ds_type",
                                               "sha": "sha",
                                               "type": "type",
                                               "content_type": "content_type",
                                               "attempt_id": 1})).body

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


async def test_task_with_multiple_attempts(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="step", run_number=_run.get("run_number"))).body

    _task = (await add_task(db,
                            flow_id=_step.get("flow_id"),
                            step_name=_step.get("step_name"),
                            run_number=_step.get("run_number"),
                            run_id=_step.get("run_id"))).body
    _task['status'] = 'running'

    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/attempts".format(**_task), 200, [_task])

    _artifact_first = (await add_artifact(db,
                                          flow_id=_task.get("flow_id"),
                                          run_number=_task.get("run_number"),
                                          run_id=_task.get("run_id"),
                                          step_name=_task.get("step_name"),
                                          task_id=_task.get("task_id"),
                                          task_name=_task.get("task_name"),
                                          artifact={
                                              "name": "_task_ok",
                                              "location": "location",
                                              "ds_type": "ds_type",
                                              "sha": "sha",
                                              "type": "type",
                                              "content_type": "content_type",
                                              "attempt_id": 0})).body

    _artifact_second = (await add_artifact(db,
                                           flow_id=_task.get("flow_id"),
                                           run_number=_task.get("run_number"),
                                           run_id=_task.get("run_id"),
                                           step_name=_task.get("step_name"),
                                           task_id=_task.get("task_id"),
                                           task_name=_task.get("task_name"),
                                           artifact={
                                               "name": "_task_ok",
                                               "location": "location",
                                               "ds_type": "ds_type",
                                               "sha": "sha",
                                               "type": "type",
                                               "content_type": "content_type",
                                               "attempt_id": 1})).body

    _task['status'] = 'completed'

    _task['attempt_id'] = 1
    # as no other timestamps are available, the task ts_epoch will be used for started_at
    _task['started_at'] = _task['ts_epoch']
    _task['finished_at'] = _artifact_second['ts_epoch']
    _task['duration'] = _artifact_second['ts_epoch'] - \
        _task['ts_epoch']

    await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}".format(**_task), 200, _task)

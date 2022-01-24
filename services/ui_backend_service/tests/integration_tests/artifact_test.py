import pytest
from .utils import (
    cli, db,
    add_flow, add_run, add_step, add_task, add_artifact,
    _test_list_resources
)
pytestmark = [pytest.mark.integration_tests]


async def test_list_artifact(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="step", run_number=_run.get("run_number"))).body
    _task = (await add_task(db,
                            flow_id=_step.get("flow_id"),
                            step_name=_step.get("step_name"),
                            run_number=_step.get("run_number"),
                            run_id=_step.get("run_id"))).body

    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/artifacts".format(**_task), 200, [])
    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/artifacts".format(**_task), 200, [])
    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/artifacts".format(**_task), 200, [])

    _first = (await add_artifact(db,
                                 flow_id=_task.get("flow_id"),
                                 run_number=_task.get("run_number"),
                                 run_id=_task.get("run_id"),
                                 step_name=_task.get("step_name"),
                                 task_id=_task.get("task_id"),
                                 task_name=_task.get("task_name"),
                                 artifact={
                                     "name": "name",
                                     "location": "location",
                                     "ds_type": "ds_type",
                                     "sha": "sha",
                                     "type": "type",
                                     "content_type": "content_type",
                                     "attempt_id": 0})).body

    _second = (await add_artifact(db,
                                  flow_id=_task.get("flow_id"),
                                  run_number=_task.get("run_number"),
                                  run_id=_task.get("run_id"),
                                  step_name=_task.get("step_name"),
                                  task_id=_task.get("task_id"),
                                  task_name=_task.get("task_name"),
                                  artifact={
                                      "name": "name",
                                      "location": "location",
                                      "ds_type": "ds_type",
                                      "sha": "sha",
                                      "type": "type",
                                      "content_type": "content_type",
                                      "attempt_id": 1})).body

    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/artifacts".format(**_task), 200, [_first, _second])
    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/artifacts".format(**_task), 200, [_first, _second])
    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/artifacts".format(**_task), 200, [_first, _second])


async def test_list_artifact_attempt_ids(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body

    steps = []
    for step_name in ["first", "second"]:
        _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
        steps.append((await add_step(db, flow_id=_run.get("flow_id"), step_name=step_name,
                                     run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body)

    artifacts_visible_for_first_step = []
    for _ in range(1):
        _task = (await add_task(db, flow_id=steps[0].get("flow_id"), step_name=steps[0].get("step_name"),
                                run_number=steps[0].get("run_number"), run_id=steps[0].get("run_id"))).body

        for attempt_id in [0, 1]:
            _artifact = (await add_artifact(db, flow_id=_task.get("flow_id"), step_name=_task.get("step_name"),
                                            run_number=_task.get("run_number"), run_id=_task.get("run_id"),
                                            task_id=_task.get("task_id"), task_name=_task.get("task_name"),
                                            artifact={"attempt_id": attempt_id})).body

            artifacts_visible_for_first_step.append(_artifact)

    artifacts_visible_for_second_step = []
    for _ in range(2):
        _task = (await add_task(db, flow_id=steps[1].get("flow_id"), step_name=steps[1].get("step_name"),
                                run_number=steps[1].get("run_number"), run_id=steps[1].get("run_id"))).body

        for attempt_id in [0]:
            _artifact = (await add_artifact(db, flow_id=_task.get("flow_id"), step_name=_task.get("step_name"),
                                            run_number=_task.get("run_number"), run_id=_task.get("run_id"),
                                            task_id=_task.get("task_id"), task_name=_task.get("task_name"),
                                            artifact={"attempt_id": attempt_id})).body

            artifacts_visible_for_second_step.append(_artifact)

    await _test_list_resources(cli, db,
                               "/flows/{flow_id}/runs/{run_number}/artifacts".format(
                                   **steps[0]),
                               200, artifacts_visible_for_first_step)
    await _test_list_resources(cli, db,
                               "/flows/{flow_id}/runs/{run_number}/artifacts".format(
                                   **steps[1]),
                               200, artifacts_visible_for_second_step)

    await _test_list_resources(cli, db,
                               "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/artifacts".format(
                                   **steps[0]),
                               200, artifacts_visible_for_first_step)
    await _test_list_resources(cli, db,
                               "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/artifacts".format(
                                   **steps[1]),
                               200, artifacts_visible_for_second_step)

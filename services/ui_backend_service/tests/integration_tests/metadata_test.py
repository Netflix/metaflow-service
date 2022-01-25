import pytest
from .utils import (
    cli, db,
    add_flow, add_run, add_step, add_task, add_metadata,
    _test_list_resources
)
pytestmark = [pytest.mark.integration_tests]


async def test_list_metadata(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="step", run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body
    _task = (await add_task(db,
                            flow_id=_step.get("flow_id"),
                            step_name=_step.get("step_name"),
                            run_number=_step.get("run_number"),
                            run_id=_step.get("run_id"))).body

    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/metadata".format(**_task), 200, [])
    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/metadata".format(**_task), 200, [])

    _metadata = (await add_metadata(db,
                                    flow_id=_task.get("flow_id"),
                                    run_number=_task.get("run_number"),
                                    run_id=_task.get("run_id"),
                                    step_name=_task.get("step_name"),
                                    task_id=_task.get("task_id"),
                                    task_name=_task.get("task_name"),
                                    metadata={
                                        "field_name": "field_name",
                                        "value": "value",
                                        "type": "type"})).body

    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/metadata".format(**_task), 200, [_metadata])
    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/metadata".format(**_metadata), 200, [_metadata])


async def test_list_metadata_field_names(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="step", run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body
    _task = (await add_task(db,
                            flow_id=_step.get("flow_id"),
                            step_name=_step.get("step_name"),
                            run_number=_step.get("run_number"),
                            run_id=_step.get("run_id"))).body

    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/metadata".format(**_task), 200, [])
    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/metadata".format(**_task), 200, [])

    _metadata_first = (await add_metadata(db,
                                          flow_id=_task.get("flow_id"),
                                          run_number=_task.get("run_number"),
                                          run_id=_task.get("run_id"),
                                          step_name=_task.get("step_name"),
                                          task_id=_task.get("task_id"),
                                          task_name=_task.get("task_name"),
                                          metadata={
                                              "field_name": "log_location_stdout",
                                              "value": '{"ds_type": "s3", "location": "s3://bucket/Flow/1/end/2/0.stdout.log", "attempt": 0}',
                                              "type": "log_path"})).body

    _metadata_second = (await add_metadata(db,
                                           flow_id=_task.get("flow_id"),
                                           run_number=_task.get("run_number"),
                                           run_id=_task.get("run_id"),
                                           step_name=_task.get("step_name"),
                                           task_id=_task.get("task_id"),
                                           task_name=_task.get("task_name"),
                                           metadata={
                                               "field_name": "log_location_stdout",
                                               "value": '{"ds_type": "s3", "location": "s3://bucket/Flow/1/end/2/1.stdout.log", "attempt": 1}',
                                               "type": "log_path"})).body

    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/metadata".format(**_task), 200, [_metadata_second, _metadata_first])


async def test_list_metadata_attempt_id_filter(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="step", run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body
    _task = (await add_task(db,
                            flow_id=_step.get("flow_id"),
                            step_name=_step.get("step_name"),
                            run_number=_step.get("run_number"),
                            run_id=_step.get("run_id"))).body

    _second_task = (await add_task(db,
                                   flow_id=_step.get("flow_id"),
                                   step_name=_step.get("step_name"),
                                   run_number=_step.get("run_number"),
                                   run_id=_step.get("run_id"))).body

    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/metadata".format(**_task), 200, [])
    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/metadata".format(**_task), 200, [])

    _metadata_global = (await add_metadata(db,
                                           flow_id=_second_task.get("flow_id"),
                                           run_number=_second_task.get("run_number"),
                                           run_id=_second_task.get("run_id"),
                                           step_name=_second_task.get("step_name"),
                                           task_id=_second_task.get("task_id"),
                                           task_name=_second_task.get("task_name"),
                                           metadata={
                                               "field_name": "code-package",
                                               "value": 'location',
                                               "type": "code-package"}
                                           )).body

    _metadata_first = (await add_metadata(db,
                                          flow_id=_task.get("flow_id"),
                                          run_number=_task.get("run_number"),
                                          run_id=_task.get("run_id"),
                                          step_name=_task.get("step_name"),
                                          task_id=_task.get("task_id"),
                                          task_name=_task.get("task_name"),
                                          metadata={
                                              "field_name": "attempt",
                                              "value": '0',
                                              "type": "attempt"},
                                          tags=["attempt_id:0"]
                                          )).body
    _metadata_first["attempt_id"] = 0

    _metadata_second = (await add_metadata(db,
                                           flow_id=_task.get("flow_id"),
                                           run_number=_task.get("run_number"),
                                           run_id=_task.get("run_id"),
                                           step_name=_task.get("step_name"),
                                           task_id=_task.get("task_id"),
                                           task_name=_task.get("task_name"),
                                           metadata={
                                               "field_name": "attempt",
                                               "value": '1',
                                               "type": "attempt"},
                                           tags=["attempt_id:1"])).body
    _metadata_second["attempt_id"] = 1

    # no filter
    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/metadata?order=ts_epoch".format(**_task), 200, [_metadata_second, _metadata_first, _metadata_global])
    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/metadata?_order=ts_epoch".format(**_task), 200, [_metadata_second, _metadata_first])

    # attempt_id=0
    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/metadata?order=ts_epoch&attempt_id=0".format(**_task), 200, [_metadata_first])
    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/metadata?_order=ts_epoch&attempt_id=0".format(**_task), 200, [_metadata_first])

    # attempt_id=0,1
    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/metadata?order=ts_epoch&attempt_id=0,1".format(**_task), 200, [_metadata_second, _metadata_first])
    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/metadata?_order=ts_epoch&attempt_id=0,1".format(**_task), 200, [_metadata_second, _metadata_first])

    # attempt_id IS NULL
    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/metadata?order=ts_epoch&attempt_id:is=null".format(**_task), 200, [_metadata_global])
    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/metadata?_order=ts_epoch&attempt_id:is=null".format(**_task), 200, [])
    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/metadata?_order=ts_epoch&attempt_id:is=null".format(**_second_task), 200, [_metadata_global])

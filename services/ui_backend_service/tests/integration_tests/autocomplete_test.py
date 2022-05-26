import pytest
from .utils import (
    cli, db, add_flow, _test_list_resources,
    add_run, add_step, add_task, add_artifact
)
pytestmark = [pytest.mark.integration_tests]


async def test_flows_autocomplete(cli, db):
    await _test_list_resources(cli, db, '/flows/autocomplete', 200, [])
    await add_flow(db, flow_id="HelloFlow")
    await add_flow(db, flow_id="HelloFlow2")
    await add_flow(db, flow_id="HelloFlow3")

    await _test_list_resources(cli, db, '/flows/autocomplete', 200, ['HelloFlow', 'HelloFlow2', 'HelloFlow3'])

    # Partial matches
    await _test_list_resources(cli, db, '/flows/autocomplete?flow_id:co=low2', 200, ['HelloFlow2'])

    # no-match
    await _test_list_resources(cli, db, '/flows/autocomplete?flow_id:co=test', 200, [])


async def test_runs_autocomplete(cli, db):
    await _test_list_resources(cli, db, '/flows/HelloFlow/runs/autocomplete', 200, [])
    await add_flow(db, flow_id="HelloFlow")
    await add_run(db, flow_id="HelloFlow", run_id="HelloRun")
    await add_run(db, flow_id="HelloFlow", run_id="HelloRun2")
    await add_run(db, flow_id="HelloFlow", run_id="HelloRun3")

    await _test_list_resources(cli, db, '/flows/HelloFlow/runs/autocomplete', 200, ['HelloRun', 'HelloRun2', 'HelloRun3'])

    # Partial matches
    await _test_list_resources(cli, db, '/flows/HelloFlow/runs/autocomplete?run:co=Run2', 200, ['HelloRun2'])

    # no-match
    await _test_list_resources(cli, db, '/flows/HelloFlow/runs/autocomplete?run:co=test', 200, [])


async def test_steps_autocomplete(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="regular_step", run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body
    await add_step(db, flow_id=_run.get("flow_id"), step_name="another_step", run_number=_run.get("run_number"), run_id=_run.get("run_id"))
    await add_step(db, flow_id=_run.get("flow_id"), step_name="step3", run_number=_run.get("run_number"), run_id=_run.get("run_id"))

    await _test_list_resources(cli, db, '/flows/{flowid}/runs/{runid}/steps/autocomplete'.format(flowid=_step.get('flow_id'), runid=_step.get('run_number')), 200, ["another_step", "regular_step", "step3"])

    # Partial match
    await _test_list_resources(cli, db, '/flows/{flowid}/runs/{runid}/steps/autocomplete?step_name:co=lar'.format(flowid=_step.get('flow_id'), runid=_step.get('run_number')), 200, ["regular_step"])

    # no-match
    await _test_list_resources(cli, db, '/flows/{flowid}/runs/{runid}/steps/autocomplete?step_name:co=test'.format(flowid=_step.get('flow_id'), runid=_step.get('run_number')), 200, [])


async def test_tags_autocomplete(cli, db):
    await _test_list_resources(cli, db, '/tags/autocomplete', 200, [])
    await add_flow(db, flow_id="HelloFlow")
    await add_run(db, flow_id="HelloFlow", run_id="HelloRun", tags=["tag:something"])

    # manually refresh the cached tags
    await cli.server.app.AutoCompleteApi.update_cached_tags()
    # Note that runtime:dev tags gets assigned automatically
    await _test_list_resources(cli, db, '/tags/autocomplete', 200, ['run_system_tag', 'tag:something'])

    # Partial match
    await _test_list_resources(cli, db, '/tags/autocomplete?tag:co=thing', 200, ['tag:something'])

    # no-match
    await _test_list_resources(cli, db, '/tags/autocomplete?tag:co=nothing', 200, [])

    # Custom match 'tag:.*thing'
    await _test_list_resources(cli, db, '/tags/autocomplete?tag:re=tag:.*thing', 200, ['tag:something'])


async def test_artifacts_autocomplete(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="regular_step", run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body
    _task = (await add_task(db, flow_id=_step.get("flow_id"), step_name=_step.get("step_name"), run_number=_step.get("run_number"), run_id=_step.get("run_id"))).body

    async def _create_artifact(name, task):
        await add_artifact(
            db,
            flow_id=task.get("flow_id"),
            run_number=task.get("run_number"),
            step_name=task.get("step_name"),
            task_id=task.get("task_id"),
            artifact={"name": name}
        )

    await _create_artifact("first", _task)
    await _create_artifact("second", _task)
    await _create_artifact("first3", _task)

    await _test_list_resources(cli, db, '/flows/{flowid}/runs/{runid}/artifacts/autocomplete'.format(flowid=_task.get('flow_id'), runid=_task.get('run_number')), 200, ["first", "first3", "second"])

    # Partial match
    await _test_list_resources(cli, db, '/flows/{flowid}/runs/{runid}/artifacts/autocomplete?name:co=rst'.format(flowid=_task.get('flow_id'), runid=_task.get('run_number')), 200, ["first", "first3"])

    # no-match
    await _test_list_resources(cli, db, '/flows/{flowid}/runs/{runid}/artifacts/autocomplete?name:co=test'.format(flowid=_task.get('flow_id'), runid=_task.get('run_number')), 200, [])

import json
from base64 import b64encode

import pytest

from .utils import cli, db, add_flow, add_run, add_step, add_task

pytestmark = [pytest.mark.integration_tests]

# A cursor carrying every endpoint's cursor keys, with a null ts_epoch.
NULL_TS_CURSOR = {
    "ts_epoch": None,
    "flow_id": "HelloFlow",
    "run_number": 1,
    "step_name": "step",
    "task_id": 1,
    "name": "artifact",
    "id": 1,
}

PAGINATED_PATHS = [
    "/flows",
    "/flows/{flow_id}/runs",
    "/flows/{flow_id}/runs/{run_number}/steps",
    "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks",
    "/flows/{flow_id}/runs/{run_number}/metadata",
    "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/metadata",
    "/flows/{flow_id}/runs/{run_number}/artifacts",
    "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/artifacts",
    "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/artifacts",
]


@pytest.fixture
async def paths(db):
    await add_flow(db, flow_id="HelloFlow")
    _run = (await add_run(db, flow_id="HelloFlow")).body
    await add_step(db, flow_id="HelloFlow", run_number=_run["run_number"])
    _task = (
        await add_task(db, flow_id="HelloFlow", run_number=_run["run_number"])
    ).body
    return [
        path.format(
            flow_id="HelloFlow",
            run_number=_run["run_number"],
            step_name="step",
            task_id=_task["task_id"],
        )
        for path in PAGINATED_PATHS
    ]


@pytest.mark.parametrize("limit", ["0", "-1", "abc", "1.5"])
async def test_invalid_limit_returns_400(cli, paths, limit):
    for path in paths:
        resp = await cli.get(path, params={"_limit": limit})
        assert resp.status == 400, path
        assert json.loads(await resp.text()) == "Invalid limit"


@pytest.mark.parametrize(
    "cursor",
    [
        b64encode(b"123").decode(),
        b64encode(json.dumps(NULL_TS_CURSOR).encode()).decode(),
    ],
    ids=["json-number", "null-value"],
)
async def test_malformed_cursor_returns_400(cli, paths, cursor):
    for path in paths:
        resp = await cli.get(path, params={"_cursor": cursor})
        assert resp.status == 400, path
        assert json.loads(await resp.text()) == "Invalid cursor"

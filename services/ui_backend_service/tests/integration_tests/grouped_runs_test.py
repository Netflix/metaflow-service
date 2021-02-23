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


async def test_list_runs_group_by_flow_id(cli, db):
    await _test_list_resources(cli, db, "/runs", 200, [])
    await _test_list_resources(cli, db, "/runs?_group=flow_id", 200, [])

    first_runs = await create_n_runs(db, 11, "A-FirstFlow")
    second_runs = await create_n_runs(db, 11, "B-SecondFlow")

    # default per-group limit should be 10
    await _test_list_resources(cli, db, "/runs?_group=flow_id", 200, [*first_runs[:10], *second_runs[:10]], approx_keys=["duration"])

    # _group_limit should limit number of records returned per group
    await _test_list_resources(cli, db, "/runs?_group=flow_id&_group_limit=1", 200, [first_runs[0], second_runs[0]], approx_keys=["duration"])

    # _limit should limit number of groups, not number of rows.
    await _test_list_resources(cli, db, "/runs?_group=flow_id&_order=%2Bts_epoch&_group_limit=2&_limit=1", 200, first_runs[:2], approx_keys=["duration"])

    # _order should order within groups.
    await _test_list_resources(cli, db, "/runs?_group=flow_id&_order=run_number", 200, [*first_runs[::-1][:10], *second_runs[::-1][:10]], approx_keys=["duration"])


async def test_list_runs_group_by_user(cli, db):
    await _test_list_resources(cli, db, "/runs", 200, [])
    await _test_list_resources(cli, db, "/runs?_group=user", 200, [])

    first_runs = await create_n_runs(db, 11, "A-Flow", "B-user")
    second_runs = await create_n_runs(db, 11, "B-Flow", "A-user")

    # default per-group should be 10. ordering by run_number ASC within group to test sorting,
    # and to retain order of test runs list.
    await _test_list_resources(cli, db, "/runs?_group=user&_order=%2Brun", 200, [*second_runs[:10], *first_runs[:10]], approx_keys=["duration"])

    # _group_limit should limit number of records returned per group
    await _test_list_resources(cli, db, "/runs?_group=user&&_order=%2Brun&_group_limit=1", 200, [second_runs[0], first_runs[0]], approx_keys=["duration"])

    # _limit should limit number of groups, not number of rows.
    await _test_list_resources(cli, db, "/runs?_group=user&&_order=%2Brun&_group_limit=2&_limit=1", 200, second_runs[:2], approx_keys=["duration"])


async def create_n_runs(db, n=1, flow_id="TestFlow", user="TestUser"):
    await add_flow(db, flow_id=flow_id)
    created_runs = []
    for _ in range(n):
        _run = (await add_run(db, flow_id=flow_id, user_name=user, system_tags=["runtime:dev", "user:{}".format(user)])).body
        _run["run"] = _run["run_number"]
        _run["status"] = "running"
        _run["duration"] = int(round(time.time() * 1000)) - _run["ts_epoch"]
        _run["user"] = user
        created_runs.append(_run)
    return created_runs

import pytest
from .utils import (
    cli, db,
    add_flow,
    _test_list_resources, _test_single_resource
)
pytestmark = [pytest.mark.integration_tests]


async def test_list_flows(cli, db):
    await _test_list_resources(cli, db, "/flows", 200, [])

    _flow = (await add_flow(db, flow_id="HelloFlow")).body

    await _test_list_resources(cli, db, "/flows", 200, [_flow])


async def test_single_flow(cli, db):
    await _test_single_resource(cli, db, "/flows/HelloFlow", 404, {})

    _flow = (await add_flow(db, flow_id="HelloFlow")).body

    await _test_single_resource(cli, db, "/flows/{flow_id}".format(**_flow), 200, _flow)

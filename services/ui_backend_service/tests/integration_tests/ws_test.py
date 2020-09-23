import pytest
from .utils import (
    init_app, init_db, clean_db,
    add_flow,
    TIMEOUT_FUTURE
)
from services.ui_backend_service.api.notify import ListenNotify

pytestmark = [pytest.mark.integration_tests]

# Fixtures begin


@pytest.fixture
def cli(loop, aiohttp_client):
    return init_app(loop, aiohttp_client)


@pytest.fixture
async def db(cli):
    async_db = await init_db(cli)

    # Init after DB is ready so that connection pool is available
    app = cli.server.app
    ListenNotify(app, app.event_emitter)

    yield async_db
    await clean_db(async_db)

# Fixtures end


async def _subscribe(ws, resource, uuid="123"):
    return await ws.send_json({
        "type": "SUBSCRIBE",
        "uuid": uuid,
        "resource": resource})


async def _unsubscribe(ws, uuid="123"):
    return await ws.send_json({
        "type": "UNSUBSCRIBE",
        "uuid": uuid})


async def test_subscription(cli, db, loop):
    ws = await cli.ws_connect("/ws")

    await _subscribe(ws, "/flows")

    _flow = (await add_flow(db, flow_id="HelloFlow")).body

    msg = await ws.receive_json(timeout=TIMEOUT_FUTURE)
    assert msg["type"] == "INSERT"
    assert msg["data"] == _flow

    await ws.close()


async def test_no_subscription(cli, db, loop):
    ws = await cli.ws_connect("/ws")

    (await add_flow(db, flow_id="HelloFlow")).body

    try:
        await ws.receive_json(timeout=TIMEOUT_FUTURE)
        assert False
    except:
        # This is expected since there's no subscriptions
        pass

    await ws.close()


async def test_unubscribe(cli, db, loop):
    ws = await cli.ws_connect("/ws")

    await _subscribe(ws, "/flows")

    _flow = (await add_flow(db, flow_id="HelloFlow")).body

    msg = await ws.receive_json(timeout=TIMEOUT_FUTURE)
    assert msg["type"] == "INSERT"
    assert msg["data"] == _flow

    await _unsubscribe(ws)

    await add_flow(db, flow_id="AnotherFlow")

    try:
        await ws.receive_json(timeout=TIMEOUT_FUTURE)
        assert False
    except:
        # This is expected since there's no subscriptions any longer
        pass

    await ws.close()


async def test_subscription_filters(cli, db, loop):
    ws = await cli.ws_connect("/ws")

    await _subscribe(ws, "/flows?_tags=custom:tag")

    _flow = (await add_flow(db, flow_id="HelloFlow", tags=["custom:tag"])).body

    msg = await ws.receive_json(timeout=TIMEOUT_FUTURE)
    assert msg["type"] == "INSERT"
    assert msg["data"] == _flow

    await _unsubscribe(ws)

    await add_flow(db, flow_id="AnotherFlow", tags=["unknown:tag"])

    try:
        await ws.receive_json(timeout=TIMEOUT_FUTURE)
        assert False
    except:
        # This is expected since there's no subscriptions any longer
        pass

    await ws.close()

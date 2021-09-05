import pytest
import time
import asyncio
import math
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
def cli_short_ttl(loop, aiohttp_client):
    # Use 0.5 second TTL for Websocket queue
    return init_app(loop, aiohttp_client, queue_ttl=0.5)


@pytest.fixture
async def db(cli):
    async_db = await init_db(cli)

    # Init after DB is ready so that connection pool is available
    app = cli.server.app
    ListenNotify(app, async_db, app.event_emitter)

    yield async_db
    await clean_db(async_db)

# Fixtures end


async def _subscribe(ws, resource, uuid="123", since: int = None):
    subscription = {
        "type": "SUBSCRIBE",
        "uuid": uuid,
        "resource": resource}
    if since is not None:
        subscription['since'] = since
    return await ws.send_json(subscription)


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


async def test_subscription_queue(cli, db, loop):
    ws = await cli.ws_connect("/ws")

    now = int(math.floor(time.time()))

    # Subscribe to resource.
    await _subscribe(ws=ws, resource="/flows")
    # Disconnect from ws.
    await ws.close()

    # create a new flow while disconnected subscription is still less than TTL old.
    _flow = (await add_flow(db, flow_id="HelloFlow")).body

    # Reconnect to websocket.
    ws = await cli.ws_connect("/ws")

    # Subscribe and request events that happened in the past
    await _subscribe(ws=ws, resource="/flows", since=now - 10)

    msg = await ws.receive_json(timeout=TIMEOUT_FUTURE)
    assert msg["type"] == "INSERT"
    assert msg["data"] == _flow

    await ws.close()


async def test_subscription_queue_without_since(cli, db, loop):
    ws = await cli.ws_connect("/ws")

    # At this point we have not subscribed to any resources
    _flow = (await add_flow(db, flow_id="HelloFlow")).body

    try:
        # Make sure nothing is received via websocket
        await ws.receive_json(timeout=TIMEOUT_FUTURE)
        assert False
    except asyncio.TimeoutError:
        pass

    # Above flow should not be returned without `since` parameter
    await _subscribe(ws=ws, resource="/flows")

    try:
        # Make sure still nothing is received via websocket
        await ws.receive_json(timeout=TIMEOUT_FUTURE)
        assert False
    except asyncio.TimeoutError:
        # This is expected since queue data has expired
        pass

    await ws.close()


async def test_subscription_queue_ttl_expired(cli_short_ttl, db, loop):
    ws = await cli_short_ttl.ws_connect("/ws")

    now = int(math.floor(time.time()))

    # At this point we have not subscribed to any resources
    _flow = (await add_flow(db, flow_id="HelloFlow")).body

    try:
        # Make sure nothing is received via websocket
        await ws.receive_json(timeout=TIMEOUT_FUTURE)
        assert False
    except asyncio.TimeoutError:
        pass

    # Sleep 1 second to make sure we are past queue TTL (0.5s in this case)
    await asyncio.sleep(1)

    # Subscribe and request events that happened in the past
    # Nothing should be returned since queue TTL is 0.5 seconds and we have waited 1 second
    await _subscribe(ws=ws, resource="/flows", since=now)

    try:
        await ws.receive_json(timeout=TIMEOUT_FUTURE)
        assert False
    except asyncio.TimeoutError:
        # This is expected since queue data has expired
        pass

    await ws.close()


async def test_no_subscription(cli, db, loop):
    ws = await cli.ws_connect("/ws")

    (await add_flow(db, flow_id="HelloFlow")).body

    try:
        await ws.receive_json(timeout=TIMEOUT_FUTURE)
        assert False
    except asyncio.TimeoutError:
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
    except asyncio.TimeoutError:
        # This is expected since there's no subscriptions any longer
        pass

    await ws.close()


async def test_subscription_filters(cli, db, loop):
    ws = await cli.ws_connect("/ws")

    await _subscribe(ws, "/flows?_tags=custom:tag")

    # Should receive resource as it matches subscription conditions
    _flow = (await add_flow(db, flow_id="HelloFlow", tags=["custom:tag"])).body

    msg = await ws.receive_json(timeout=TIMEOUT_FUTURE)
    assert msg["type"] == "INSERT"
    assert msg["data"] == _flow

    # Does not match subscription so this resource should never be received.
    await add_flow(db, flow_id="SecondFlow", tags=["different:tag"])

    try:
        await ws.receive_json(timeout=TIMEOUT_FUTURE)
        assert False  # We should not have received anything from the web socket!
    except asyncio.TimeoutError:
        # This is expected since the subscription does not match the resource
        pass

    # After unsubscribing, no updates should be received.

    await _unsubscribe(ws)

    await add_flow(db, flow_id="AnotherFlow", tags=["custom:tag"])

    try:
        await ws.receive_json(timeout=TIMEOUT_FUTURE)
        assert False
    except asyncio.TimeoutError:
        # This is expected since there's no subscriptions any longer
        pass

    await ws.close()

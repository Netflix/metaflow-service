import asyncio
import os

from aiohttp import web
from aiohttp_swagger import *

from .api.admin import AdminApi

from .api.flow import FlowApi
from .api.run import RunApi
from .api.step import StepApi
from .api.task import TaskApi
from .api.metadata import MetadataApi
from .api.artifact import ArtificatsApi
from .api.tag import TagApi
from .api.artifactsearch import ArtifactSearchApi
from .api.log import LogApi
from .api.dag import DagApi

from .api.ws import Websocket
from .api.notify import ListenNotify
from .api.heartbeat_monitor import RunHeartbeatMonitor, TaskHeartbeatMonitor
from .cache.store import CacheStore
from .frontend import Frontend

from services.data.postgres_async_db import AsyncPostgresDB
from services.utils import DBConfiguration

from pyee import AsyncIOEventEmitter, ExecutorEventEmitter

from .doc import swagger_definitions, swagger_description


def app(loop=None, db_conf: DBConfiguration = None):

    loop = loop or asyncio.get_event_loop()
    app = web.Application(loop=loop)
    async_db = AsyncPostgresDB()
    loop.run_until_complete(async_db._init(db_conf))

    event_emitter = ExecutorEventEmitter()
    cache_store = CacheStore(event_emitter=event_emitter)
    app.on_startup.append(cache_store.start_caches)
    app.on_cleanup.append(cache_store.stop_caches)
    ListenNotify(app, event_emitter)
    RunHeartbeatMonitor(event_emitter)
    TaskHeartbeatMonitor(event_emitter)
    Websocket(app, event_emitter)

    FlowApi(app)
    RunApi(app)
    StepApi(app)
    TaskApi(app)
    MetadataApi(app)
    ArtificatsApi(app)
    TagApi(app)
    ArtifactSearchApi(app)
    DagApi(app)

    LogApi(app)
    AdminApi(app)

    setup_swagger(app,
                  description=swagger_description,
                  definitions=swagger_definitions)

    if os.environ.get("UI_ENABLED", 0) == "1":
        # Serve UI bundle only if enabled
        # This has to be placed last due to catch-all route
        Frontend(app)

    return app


def main():
    loop = asyncio.get_event_loop()
    the_app = app(loop, DBConfiguration())
    handler = web.AppRunner(the_app)
    loop.run_until_complete(handler.setup())
    port = os.environ.get("MF_UI_METADATA_PORT", 8083)
    host = str(os.environ.get("MF_UI_METADATA_HOST", "0.0.0.0"))
    f = loop.create_server(handler.server, host, port)

    srv = loop.run_until_complete(f)
    print("serving on", srv.sockets[0].getsockname())
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

import asyncio
import os
import signal

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

from services.data.postgres_async_db import _AsyncPostgresDB
from services.utils import DBConfiguration, logging

from pyee import AsyncIOEventEmitter

from .doc import swagger_definitions, swagger_description

from .features import FEATURE_DB_LISTEN_ENABLE, FEATURE_WS_ENABLE, FEATURE_HEARTBEAT_ENABLE

PATH_PREFIX = os.environ.get("PATH_PREFIX", "")


def app(loop=None, db_conf: DBConfiguration = None):

    loop = loop or asyncio.get_event_loop()
    _app = web.Application(loop=loop)
    app = web.Application(loop=loop) if len(PATH_PREFIX) > 0 else _app

    async_db = _AsyncPostgresDB('ui')
    loop.run_until_complete(async_db._init(db_conf))

    event_emitter = AsyncIOEventEmitter()

    async_db_cache = _AsyncPostgresDB('ui:cache')
    loop.run_until_complete(async_db_cache._init(db_conf))
    cache_store = CacheStore(event_emitter=event_emitter, db=async_db_cache)
    app.on_startup.append(cache_store.start_caches)
    # app.on_cleanup.append(cache_store.stop_caches)

    if FEATURE_DB_LISTEN_ENABLE:
        async_db_notify = _AsyncPostgresDB('ui:notify')
        loop.run_until_complete(async_db_notify._init(db_conf))
        ListenNotify(app, event_emitter, db=async_db_notify)

    if FEATURE_HEARTBEAT_ENABLE:
        async_db_heartbeat = _AsyncPostgresDB('ui:heartbeat')
        loop.run_until_complete(async_db_heartbeat._init(db_conf))
        RunHeartbeatMonitor(event_emitter, db=async_db_heartbeat)
        TaskHeartbeatMonitor(event_emitter, db=async_db_heartbeat)

    if FEATURE_WS_ENABLE:
        async_db_ws = _AsyncPostgresDB('ui:websocket')
        loop.run_until_complete(async_db_ws._init(db_conf))
        Websocket(app, event_emitter, db=async_db_ws)

    FlowApi(app, async_db)
    RunApi(app, async_db)
    StepApi(app, async_db)
    TaskApi(app, async_db)
    MetadataApi(app, async_db)
    ArtificatsApi(app, async_db)
    TagApi(app, async_db)
    ArtifactSearchApi(app, async_db)
    DagApi(app, async_db)

    LogApi(app, async_db)
    AdminApi(app)

    setup_swagger(app,
                  description=swagger_description,
                  definitions=swagger_definitions)

    if os.environ.get("UI_ENABLED", 0) == "1":
        # Serve UI bundle only if enabled
        # This has to be placed last due to catch-all route
        Frontend(app)

    if len(PATH_PREFIX) > 0:
        _app.add_subapp(PATH_PREFIX, app)

    return _app


def main():
    loop = asyncio.get_event_loop()
    # Set exception and signal handlers for async loop. Mainly for logging purposes.
    loop.set_exception_handler(async_loop_error_handler)
    for sig in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT):
        loop.add_signal_handler(sig, lambda sig=sig: async_loop_signal_handler(sig))

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


def async_loop_error_handler(loop, context):
    msg = context.get("exception", context["message"])
    logging.error("Encountered an exception: {}\n{}".format(msg, context))


def async_loop_signal_handler(signal):
    logging.info("Received signal: {}".format(signal))


if __name__ == "__main__":
    main()

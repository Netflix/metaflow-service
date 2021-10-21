import asyncio
import os
import signal
import concurrent

from aiohttp import web
from aiohttp_swagger import *
from pyee import AsyncIOEventEmitter
from services.utils import DBConfiguration, logging

from services.metadata_service.server import app as metadata_service_app

# service processes and routes
from .api import (AdminApi, ArtifactSearchApi, ArtificatsApi, AutoCompleteApi, ConfigApi,
                  DagApi, FeaturesApi, FlowApi, ListenNotify, LogApi,
                  MetadataApi, RunApi, RunHeartbeatMonitor, StepApi, TagApi,
                  TaskApi, TaskHeartbeatMonitor, Websocket, PluginsApi)

from .data.cache import CacheStore
from .data.db import AsyncPostgresDB
from .doc import swagger_definitions, swagger_description
from .features import (FEATURE_DB_LISTEN_ENABLE, FEATURE_HEARTBEAT_ENABLE,
                       FEATURE_WS_ENABLE)
from .frontend import Frontend

from .plugins import init_plugins

PATH_PREFIX = os.environ.get("PATH_PREFIX", "")

DEFAULT_SERVICE_HOST = str(os.environ.get('MF_UI_METADATA_HOST', '0.0.0.0'))
DEFAULT_SERVICE_PORT = os.environ.get('MF_UI_METADATA_PORT', 8083)
DEFAULT_METADATA_SERVICE_URL = "http://{}:{}/metadata".format(DEFAULT_SERVICE_HOST, DEFAULT_SERVICE_PORT)

# Provide defaults for Metaflow Client
os.environ['METAFLOW_SERVICE_URL'] = os.environ.get('METAFLOW_SERVICE_URL', DEFAULT_METADATA_SERVICE_URL)
os.environ['USERNAME'] = os.environ.get('USERNAME', 'none')
os.environ['METAFLOW_S3_RETRY_COUNT'] = os.environ.get('METAFLOW_S3_RETRY_COUNT', '0')
os.environ['METAFLOW_DEFAULT_METADATA'] = os.environ.get('METAFLOW_DEFAULT_METADATA', 'service')

# Create database triggers automatically, enabled by default
# Disable with env variable `DB_TRIGGER_CREATE=0`
DB_TRIGGER_CREATE = os.environ.get("DB_TRIGGER_CREATE", "1") == "1"


def app(loop=None, db_conf: DBConfiguration = None):

    loop = loop or asyncio.get_event_loop()
    _app = web.Application(loop=loop)
    app = web.Application(loop=loop) if len(PATH_PREFIX) > 0 else _app

    async_db = AsyncPostgresDB('ui')
    loop.run_until_complete(async_db._init(db_conf=db_conf, create_triggers=DB_TRIGGER_CREATE))

    event_emitter = AsyncIOEventEmitter()

    async_db_cache = AsyncPostgresDB('ui:cache')
    loop.run_until_complete(async_db_cache._init(db_conf))
    cache_store = CacheStore(db=async_db_cache, event_emitter=event_emitter)
    app.on_startup.append(cache_store.start_caches)
    app.on_cleanup.append(cache_store.stop_caches)

    if FEATURE_DB_LISTEN_ENABLE:
        async_db_notify = AsyncPostgresDB('ui:notify')
        loop.run_until_complete(async_db_notify._init(db_conf))
        ListenNotify(app, db=async_db_notify, event_emitter=event_emitter)

    if FEATURE_HEARTBEAT_ENABLE:
        async_db_heartbeat = AsyncPostgresDB('ui:heartbeat')
        loop.run_until_complete(async_db_heartbeat._init(db_conf))
        RunHeartbeatMonitor(event_emitter, db=async_db_heartbeat)
        TaskHeartbeatMonitor(event_emitter, db=async_db_heartbeat, cache=cache_store)

    if FEATURE_WS_ENABLE:
        async_db_ws = AsyncPostgresDB('ui:websocket')
        loop.run_until_complete(async_db_ws._init(db_conf))
        Websocket(app, db=async_db_ws, event_emitter=event_emitter, cache=cache_store)

    AutoCompleteApi(app, async_db)
    FlowApi(app, async_db)
    RunApi(app, async_db, cache_store)
    StepApi(app, async_db)
    TaskApi(app, async_db, cache_store)
    MetadataApi(app, async_db)
    ArtificatsApi(app, async_db, cache_store)
    TagApi(app, async_db)
    ArtifactSearchApi(app, async_db, cache_store)
    DagApi(app, async_db, cache_store)
    FeaturesApi(app)
    ConfigApi(app)
    PluginsApi(app)

    LogApi(app, async_db, cache_store)
    AdminApi(app)

    # Add Metadata Service as a sub application so that Metaflow Client
    # can use it as a service backend in case none provided via METAFLOW_SERVICE_URL
    app.add_subapp("/metadata", metadata_service_app(loop=loop, db_conf=db_conf))

    setup_swagger(app,
                  description=swagger_description,
                  definitions=swagger_definitions)

    if os.environ.get("UI_ENABLED", 0) == "1":
        # Serve UI bundle only if enabled
        # This has to be placed last due to catch-all route
        Frontend(app)

    if len(PATH_PREFIX) > 0:
        _app.add_subapp(PATH_PREFIX, app)

    async def _init_plugins():
        with concurrent.futures.ThreadPoolExecutor() as pool:
            await loop.run_in_executor(pool, init_plugins)
    asyncio.run_coroutine_threadsafe(_init_plugins(), loop)

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
    f = loop.create_server(handler.server, DEFAULT_SERVICE_HOST, DEFAULT_SERVICE_PORT)

    srv = loop.run_until_complete(f)
    print("serving on", srv.sockets[0].getsockname())
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass


def async_loop_error_handler(loop, context):
    msg = context.get("exception", context["message"])
    logging.error("Encountered an exception: {}".format(msg))


def async_loop_signal_handler(signal):
    logging.info("Received signal: {}".format(signal))


if __name__ == "__main__":
    main()

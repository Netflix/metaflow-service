import asyncio
import os

from aiohttp import web

from .api.run import RunApi
from .api.flow import FlowApi

from .api.step import StepApi
from .api.task import TaskApi
from .api.artifact import ArtificatsApi
from .api.admin import AuthApi

from .api.metadata import MetadataApi
from services.data.postgres_async_db import AsyncPostgresDB
from services.utils import DBConfiguration

PATH_PREFIX = os.environ.get("PATH_PREFIX", "")


def app(loop=None, db_conf: DBConfiguration = None, middlewares=None, path_prefix=""):

    loop = loop or asyncio.get_event_loop()

    _app = web.Application(loop=loop)
    app = web.Application(loop=loop) if path_prefix else _app
    async_db = AsyncPostgresDB()
    loop.run_until_complete(async_db._init(db_conf))
    FlowApi(app)
    RunApi(app)
    StepApi(app)
    TaskApi(app)
    MetadataApi(app)
    ArtificatsApi(app)
    AuthApi(app)

    if path_prefix:
        _app.add_subapp(path_prefix, app)
    if middlewares:
        _app.middlewares.extend(middlewares)
    return _app


def main():
    loop = asyncio.get_event_loop()
    the_app = app(loop, DBConfiguration(), path_prefix=PATH_PREFIX)
    handler = web.AppRunner(the_app)
    loop.run_until_complete(handler.setup())

    port = os.environ.get("MF_METADATA_PORT", 8080)
    host = str(os.environ.get("MF_METADATA_HOST", "0.0.0.0"))
    f = loop.create_server(handler.server, host, port)

    srv = loop.run_until_complete(f)
    print("serving on", srv.sockets[0].getsockname())
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

import asyncio
import os

from aiohttp import web
from aiohttp_swagger import *

from subprocess import Popen, PIPE

from .api.admin import AdminApi

from .data.postgres_async_db import AsyncPostgresDB


def app(loop=None):

    loop = loop or asyncio.get_event_loop()
    app = web.Application(loop=loop)
    async_db = AsyncPostgresDB()
    loop.run_until_complete(async_db._init())
    AdminApi(app)
    setup_swagger(app)
    return app


if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        the_app = app(loop)
        handler = the_app.make_handler()
        port = os.environ.get("MF_MIGRATION_PORT", 8082)
        host = str(os.environ.get("MF_METADATA_HOST", "0.0.0.0"))
        f = loop.create_server(handler, host, port)

        srv = loop.run_until_complete(f)

        print("serving on", srv.sockets[0].getsockname())
    except Exception:
        pass
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

import asyncio
import os

from aiohttp import web
from aiohttp_swagger import setup_swagger

from .api.admin import AdminApi

from .data.postgres_async_db import AsyncPostgresDB
from services.utils import DBConfiguration
from .migration_config import db_conf


def app(loop=None, db_conf: DBConfiguration = None):

    loop = loop or asyncio.get_event_loop()
    app = web.Application(loop=loop)
    async_db = AsyncPostgresDB()
    loop.run_until_complete(async_db._init(db_conf))
    AdminApi(app)
    setup_swagger(app)
    return app


def main():
    loop = asyncio.get_event_loop()
    the_app = app(loop, db_conf)
    handler = web.AppRunner(the_app)
    loop.run_until_complete(handler.setup())

    port = os.environ.get("MF_MIGRATION_PORT", 8082)
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

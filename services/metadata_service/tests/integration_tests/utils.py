from aiohttp import web
import os
import psycopg2
import json

from services.data.postgres_async_db import AsyncPostgresDB

# Migration imports
from services.migration_service.api.admin import AdminApi as MigrationAdminApi
from services.migration_service.data.postgres_async_db import AsyncPostgresDB as MigrationAsyncPostgresDB


from services.metadata_service.api.flow import FlowApi
from services.metadata_service.api.admin import AuthApi

# Test fixture helpers begin


def init_app(loop, aiohttp_client, queue_ttl=30):
    app = web.Application()

    # Migration routes as a subapp
    migration_app = web.Application()
    MigrationAdminApi(migration_app)
    app.add_subapp("/migration/", migration_app)

    FlowApi(app)
    AuthApi(app)

    return loop.run_until_complete(aiohttp_client(app))


async def init_db(cli):
    # Make sure migration scripts are applied
    migration_db = MigrationAsyncPostgresDB.get_instance()
    await migration_db._init()

    # Apply migrations and make sure "is_up_to_date" == True
    await cli.patch("/migration/upgrade")
    # TODO: Api not responding with valid json headers so check fails.
    # status = await (await cli.get("/migration/db_schema_status")).json()
    # assert status["is_up_to_date"] is True

    db = AsyncPostgresDB.get_instance()
    await db._init()
    return db


async def clean_db(db: AsyncPostgresDB):
    # Tables to clean (order is important due to foreign keys)
    tables = [
        db.metadata_table_postgres,
        db.artifact_table_postgres,
        db.task_table_postgres,
        db.step_table_postgres,
        db.run_table_postgres,
        db.flow_table_postgres
    ]
    try:
        with (
            await db.pool.cursor(
                cursor_factory=psycopg2.extras.DictCursor
            )
        ) as cur:
            for table in tables:
                cleanup_query = "DELETE FROM {}".format(table.table_name)
                await cur.execute(cleanup_query)

            cur.close()
    except Exception as error:
        print("DB Cleanup failed after test. This might have adverse effects on further test runs", str(error))

# Test fixture helpers end

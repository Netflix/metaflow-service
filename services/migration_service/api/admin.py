import os
import json
from aiohttp import web
from subprocess import Popen
from multidict import MultiDict
from .utils import ApiUtils
from . import make_goose_migration_template
from services.migration_service.migration_config import db_conf


class AdminApi(object):
    def __init__(self, app):
        app.router.add_route("GET", "/version", self.version)
        app.router.add_route("GET", "/ping", self.ping)
        app.router.add_route("GET", "/db_schema_status", self.db_schema_status)

        endpoints_enabled = int(os.environ.get("MF_MIGRATION_ENDPOINTS_ENABLED",
                                               1))
        if endpoints_enabled:
            app.router.add_route("PATCH", "/upgrade", self.upgrade)

    async def ping(self, request):
        """
        ---
        description: This end-point allow to test that service is up.
        tags:
        - Admin
        produces:
        - 'text/plain'
        responses:
            "202":
                description: successful operation. Return "pong" text
            "405":
                description: invalid HTTP Method
        """
        return web.Response(text="pong")

    async def version(self, request):
        """
        ---
        description: This end-point returns the latest compatible version of the
            metadata service
        tags:
        - Admin
        produces:
        - 'application/json'
        responses:
            "200":
                description: successful operation. Return version text
            "405":
                description: invalid HTTP Method
        """
        version = await ApiUtils.get_latest_compatible_version()
        return web.Response(text=version)

    async def upgrade(self, request):
        """
        ---
        description: This end-point upgrades to the latest available version of
            of the schema
        tags:
        - Admin
        produces:
        - 'text/plain'
        responses:
            "200":
                description: successful operation. Return  text
            "500":
                description: could not upgrade
        """
        goose_version_cmd = make_goose_migration_template(
            db_conf.connection_string_url,
            "up"
        )
        p = Popen(goose_version_cmd, shell=True,
                  close_fds=True)
        p.wait()
        if p.returncode == 0:
            return web.Response(text="upgrade success")
        else:
            return web.Response(text="upgrade failed", status=500)

    async def db_schema_status(self, request):
        """
        ---
        description: This end-point returns varius stats around
        tags:
        - Admin
        produces:
        - 'application/json'
        responses:
            "200":
                description: successful operation. returns status of db schema and migrations
            "500":
                description: could not upgrade
        """
        try:
            version = await ApiUtils.get_goose_version()
            migration_in_progress = await ApiUtils.is_migration_in_progress()
            unapplied_migrations = ApiUtils.get_unapplied_migrations(version)
            body = {
                "is_up_to_date": len(unapplied_migrations) == 0,
                "current_version": version,
                "migration_in_progress": migration_in_progress,
                "db_schema_versions": ApiUtils.list_migrations(),
                "unapplied_migrations": unapplied_migrations
            }
            return web.Response(body=json.dumps(body),
                                headers=MultiDict({"Content-Type": "application/json"}))

        except Exception as e:
            body = {
                "detail": repr(e)
            }
            return web.Response(status=500, body=json.dumps(body),
                                headers=MultiDict({"Content-Type": "application/json"}))

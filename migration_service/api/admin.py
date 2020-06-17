import os
from aiohttp import web
from subprocess import Popen
from .utils import ApiUtils
from . import goose_migration_template, host, port, user, password, \
    database_name


class AdminApi(object):
    def __init__(self, app):
        app.router.add_route("GET", "/version", self.version)
        app.router.add_route("GET", "/ping", self.ping)

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
        - 'text/plain'
        responses:
            "202":
                description: successful operation. Return version text
            "405":
                description: invalid HTTP Method
        """
        version = await ApiUtils.get_goose_version()
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
            "202":
                description: successful operation. Return  text
            "500":
                description: could not upgrade
        """
        goose_version_cmd = goose_migration_template.format(
             database_name, user, password, host, port,
            "up"
        )
        p = Popen(goose_version_cmd, shell=True,
                  close_fds=True)
        p.wait()
        if p.returncode == 0:
            return web.Response(text="upgrade success")
        else:
            return web.Response(text="upgrade failed", status=500)

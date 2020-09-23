from aiohttp import web
from multidict import MultiDict
from services.utils import METADATA_SERVICE_VERSION, METADATA_SERVICE_HEADER


class AdminApi(object):
    def __init__(self, app):
        app.router.add_route("GET", "/ping", self.ping)
        app.router.add_route("GET", "/version", self.version)

    async def version(self, request):
        """
        ---
        description: Returns the version of the metadata service
        tags:
        - Admin
        produces:
        - 'text/plain'
        responses:
            "200":
                description: successful operation. Return the version number
            "405":
                description: invalid HTTP Method
        """
        return web.Response(text=str(METADATA_SERVICE_VERSION))

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
        return web.Response(text="pong", headers=MultiDict(
            {METADATA_SERVICE_HEADER: METADATA_SERVICE_VERSION}))

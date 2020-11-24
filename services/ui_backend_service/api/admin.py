from aiohttp import web
import json
import os
from multidict import MultiDict
from services.utils import METADATA_SERVICE_VERSION, METADATA_SERVICE_HEADER, SERVICE_COMMIT_HASH, SERVICE_BUILD_TIMESTAMP

UI_SERVICE_VERSION = "{metadata_v}-{timestamp}-{commit}".format(
    metadata_v=METADATA_SERVICE_VERSION,
    timestamp=SERVICE_BUILD_TIMESTAMP or "",
    commit=SERVICE_COMMIT_HASH or ""
)

class AdminApi(object):
    def __init__(self, app):
        app.router.add_route("GET", "/ping", self.ping)
        app.router.add_route("GET", "/version", self.version)
        app.router.add_route("GET", "/links", self.links)

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
        return web.Response(text=str(UI_SERVICE_VERSION))

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

    async def links(self, request):
        """
        ---
        description: Provides custom navigation links for UI.
        tags:
        - Admin
        produces:
        - 'application/json'
        responses:
            "200":
                description: Returns the custom navigation links for UI
                schema:
                    $ref: '#/definitions/ResponsesLinkList'
            "405":
                description: invalid HTTP Method
        """
        defaults = [
            {"href": 'https://docs.metaflow.org/', "label": 'Metaflow documentation'},
            {"href": 'https://github.com/Netflix/metaflow', "label": 'Github'}
        ]
        custom_links = _load_links_file()

        links = custom_links if custom_links else defaults
        return web.Response(status=200, body=json.dumps(links))


def _load_links_file():
    try:
        linkfilepath = os.path.join(os.path.dirname(__file__), "..", "links.json")
        with open(linkfilepath) as f:
            return json.load(f)
    except Exception:
        return None

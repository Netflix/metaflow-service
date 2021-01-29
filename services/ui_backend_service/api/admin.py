from aiohttp import web
import json
import os
from multidict import MultiDict
from services.utils import web_response, METADATA_SERVICE_VERSION, METADATA_SERVICE_HEADER, SERVICE_COMMIT_HASH, SERVICE_BUILD_TIMESTAMP

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

        defaults = [
            {"href": 'https://docs.metaflow.org/', "label": 'Documentation'},
            {"href": 'https://gitter.im/metaflow_org/community?source=orgpage', "label": 'Help'}
        ]

        self.navigation_links = _get_links_from_env() or defaults

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

        return web_response(status=200, body=self.navigation_links)


def _get_links_from_env():
    try:
        envlinks_jsons = os.environ.get("CUSTOM_QUICKLINKS")
        return json.loads(envlinks_jsons)
    except Exception:
        return None

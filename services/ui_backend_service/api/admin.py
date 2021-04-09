import os
import hashlib
import json
import time
from multidict import MultiDict
from services.utils import web_response, METADATA_SERVICE_VERSION, METADATA_SERVICE_HEADER, SERVICE_COMMIT_HASH, SERVICE_BUILD_TIMESTAMP
from aiohttp import web

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
        app.router.add_route("GET", "/announcements", self.announcements)

        defaults = [
            {"href": 'https://docs.metaflow.org/', "label": 'Documentation'},
            {"href": 'https://gitter.im/metaflow_org/community?source=orgpage', "label": 'Help'}
        ]

        self.announcements = _get_announcements_from_env() or []
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

    async def announcements(self, request):
        """
        ---
        description: Provides announcements for the UI
        tags:
        - Admin
        produces:
        - 'application/json'
        responses:
            "200":
                description: Returns list of active announcements
                schema:
                    $ref: '#/definitions/ResponsesAnnouncementList'
            "405":
                description: invalid HTTP Method
        """

        now = time.time()
        processed_announcements = []
        for announcement in self.announcements:
            try:
                if "message" not in announcement:
                    continue
                if "start" in announcement and announcement["start"] < now:
                    continue
                if "end" in announcement and announcement["end"] > now:
                    continue

                processed_announcements.append({
                    "id": announcement.get("id", hashlib.sha1(
                        str(announcement).encode('utf-8')).hexdigest()),
                    "type": announcement.get("type", "info"),
                    "message": announcement.get("message", ""),
                    "start": announcement.get("start", None),
                    "end": announcement.get("end", None)
                })
            except:
                pass

        return web_response(status=200, body=processed_announcements)


def _get_json_from_env(variable_name: str):
    try:
        return json.loads(os.environ.get(variable_name))
    except Exception:
        return None


def _get_links_from_env():
    return _get_json_from_env("CUSTOM_QUICKLINKS")


def _get_announcements_from_env():
    return _get_json_from_env("ANNOUNCEMENTS")

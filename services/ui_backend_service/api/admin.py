import hashlib
import json
import time

from aiohttp import web
from multidict import MultiDict
from services.utils import (METADATA_SERVICE_HEADER, METADATA_SERVICE_VERSION,
                            SERVICE_BUILD_TIMESTAMP, SERVICE_COMMIT_HASH,
                            web_response)

from .utils import get_json_from_env

UI_SERVICE_VERSION = "{metadata_v}-{timestamp}-{commit}".format(
    metadata_v=METADATA_SERVICE_VERSION,
    timestamp=SERVICE_BUILD_TIMESTAMP or "",
    commit=SERVICE_COMMIT_HASH or ""
)


class AdminApi(object):
    """
    Provides administrative routes for the UI Service,
    such as health checks, version info and custom navigation links.
    """

    def __init__(self, app):
        app.router.add_route("GET", "/ping", self.ping)
        app.router.add_route("GET", "/version", self.version)
        app.router.add_route("GET", "/links", self.links)
        app.router.add_route("GET", "/notifications", self.get_notifications)

        defaults = [
            {"href": 'https://docs.metaflow.org/', "label": 'Documentation'},
            {"href": 'https://gitter.im/metaflow_org/community?source=orgpage', "label": 'Help'}
        ]

        self.notifications = _get_notifications_from_env() or []
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

    async def get_notifications(self, request):
        """
        ---
        description: Provides System Notifications for the UI
        tags:
        - Admin
        produces:
        - 'application/json'
        responses:
            "200":
                description: Returns list of active system notification
                schema:
                    $ref: '#/definitions/ResponsesNotificationList'
            "405":
                description: invalid HTTP Method
        """
        processed_notifications = []
        for notification in self.notifications:
            try:
                if "message" not in notification:
                    continue

                # Created at is required and "start" is used by default if not value provided
                # Notification will be ignored if both "created" and "start" are missing
                created = notification.get("created", notification.get("start", None))
                if not created:
                    continue

                processed_notifications.append({
                    "id": notification.get("id", hashlib.sha1(
                        str(notification).encode('utf-8')).hexdigest()),
                    "type": notification.get("type", "info"),
                    "contentType": notification.get("contentType", "text"),
                    "message": notification.get("message", ""),
                    "url": notification.get("url", None),
                    "urlText": notification.get("urlText", None),
                    "created": created,
                    "start": notification.get("start", None),
                    "end": notification.get("end", None)
                })
            except:
                pass

        # Filter notifications based on query parameters
        # Supports eq,ne.lt,le,gt,ge operators for all the fields
        def filter_notifications(notification):
            comp_operators = {
                "eq": lambda a, b: a == b,
                "ne": lambda a, b: a != b,
                "lt": lambda a, b: a < b,
                "le": lambda a, b: a <= b,
                "gt": lambda a, b: a > b,
                "ge": lambda a, b: a >= b,
            }

            try:
                for q in request.query.keys():
                    if ":" in q:
                        field, op = q.split(":", 1)
                    else:
                        field, op = q, "eq"

                    # Make sure compare operator is supported, otherwise ignore
                    # Compare value is typecasted to match field type
                    if op in comp_operators:
                        field_val = notification.get(field, None)
                        if not field_val:
                            continue
                        comp_val = type(field_val)(request.query.get(q, None))
                        if not comp_val:
                            continue
                        if not comp_operators[op](field_val, comp_val):
                            return False
            except:
                pass

            return True

        return web_response(status=200, body=list(
            filter(filter_notifications, processed_notifications)))


def _get_links_from_env():
    return get_json_from_env("CUSTOM_QUICKLINKS")


def _get_notifications_from_env():
    return get_json_from_env("NOTIFICATIONS")

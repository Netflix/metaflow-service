import os
import hashlib
import asyncio

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

    def __init__(self, app, cache_store):
        self.cache_store = cache_store

        app.router.add_route("GET", "/ping", self.ping)
        app.router.add_route("GET", "/version", self.version)
        app.router.add_route("GET", "/links", self.links)
        app.router.add_route("GET", "/notifications", self.get_notifications)
        app.router.add_route("GET", "/status", self.status)

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

    async def status(self, request):
        """
        ---
        description: Display system status information, such as cache
        tags:
        - Admin
        produces:
        - 'application/json'
        responses:
            "200":
                description: Return system status information, such as cache
            "405":
                description: invalid HTTP Method
        """

        cache_status = {}
        for store in [self.cache_store.artifact_cache, self.cache_store.dag_cache, self.cache_store.log_cache]:
            try:
                # Use client ping to verify communcation, True = ok
                await store.cache.ping()
                ping = True
            except Exception as ex:
                ping = str(ex)

            try:
                # Use Check -action to verify Cache communication, True = ok
                await store.cache.request_and_return([store.cache.check()], None)
                check = True
            except Exception as ex:
                check = str(ex)

            # Extract list of worker subprocesses
            worker_list = []
            cache_server_pid = store.cache._proc.pid if store.cache._proc else None
            if cache_server_pid:
                try:
                    proc = await asyncio.create_subprocess_shell(
                        "pgrep -P {}".format(cache_server_pid),
                        stdout=asyncio.subprocess.PIPE)
                    stdout, _ = await proc.communicate()
                    if stdout:
                        pids = stdout.decode().splitlines()
                        proc = await asyncio.create_subprocess_shell(
                            "ps -p {} -o pid,%cpu,%mem,stime,time,command".format(",".join(pids)),
                            stdout=asyncio.subprocess.PIPE)
                        stdout, _ = await proc.communicate()

                        worker_list = stdout.decode().splitlines()
                except Exception as ex:
                    worker_list = str(ex)
            else:
                worker_list = "Unable to get cache server pid"

            # Extract current cache data usage in bytes
            current_size = 0
            try:
                cache_data_path = os.path.abspath(store.cache._root)
                proc = await asyncio.create_subprocess_shell(
                    "du -s {} | cut -f1".format(cache_data_path),
                    stdout=asyncio.subprocess.PIPE)
                stdout, _ = await proc.communicate()
                if stdout:
                    current_size = int(stdout.decode())
            except Exception as ex:
                current_size = str(ex)

            cache_status[store.__class__.__name__] = {
                "restart_requested": store.cache._restart_requested,
                "is_alive": store.cache._is_alive,
                "pending_requests": list(store.cache.pending_requests),
                "root": store.cache._root,
                "prev_is_alive": store.cache._prev_is_alive,
                "action_classes": list(map(lambda cls: cls.__name__, store.cache._action_classes)),
                "max_actions": store.cache._max_actions,
                "max_size": store.cache._max_size,
                "current_size": current_size,
                "ping": ping,
                "check_action": check,
                "proc": {
                    "pid": store.cache._proc.pid,
                    "returncode": store.cache._proc.returncode,
                } if store.cache._proc else None,
                "workers": worker_list
            }

        return web_response(status=200, body={
            "cache": cache_status
        })


def _get_links_from_env():
    return get_json_from_env("CUSTOM_QUICKLINKS")


def _get_notifications_from_env():
    return get_json_from_env("NOTIFICATIONS")

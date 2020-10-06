from aiohttp import web, WSMsgType
from typing import List, Dict

import json
import asyncio
import collections

from .utils import resource_conditions
from services.data.postgres_async_db import AsyncPostgresDB
from pyee import AsyncIOEventEmitter

SUBSCRIBE = 'SUBSCRIBE'
UNSUBSCRIBE = 'UNSUBSCRIBE'

WSSubscription = collections.namedtuple(
    "WSSubscription", "ws fullpath resource query uuid conditions values")

class Websocket(object):
    '''
    Adds a '/ws' endpoint and support for broadcasting realtime resource events to subscribed frontend clients.
    
    Subscribe to runs created by user dipper:
    /runs?_tags=user:dipper
    'uuid' can be used to identify specific subscription.
    
    Subscribe:
    {"type":"SUBSCRIBE", "uuid": "myst3rySh4ck", "resource": "/runs"}
    
    Unsubscribe:
    {"type": "UNSUBSCRIBE", "uuid": "myst3rySh4ck"}

    Example event:
    {"type": "UPDATE", "uuid": "myst3rySh4ck", "resource": "/runs", "data": {"foo": "bar"}}
    '''
    subscriptions: List[WSSubscription] = []

    def __init__(self, app, event_emitter=None):
        self.app = app
        self.event_emitter = event_emitter or AsyncIOEventEmitter()
        self.db = AsyncPostgresDB.get_instance()

        event_emitter.on('notify', self.event_handler)
        app.router.add_route('GET', '/ws', self.websocket_handler)

    async def event_handler(self, operation: str, resources: List[str], data: Dict):
        for sub in self.subscriptions:
            for resource in resources:
                if sub.resource == resource:
                    # Check if possible filters match this event
                    # only if the subscription actually provided conditions.
                    if sub.conditions:
                        filters_match_request = await self.db.apply_filters_to_data(
                            data=data, conditions=sub.conditions, values=sub.values)
                    else:
                        filters_match_request = True
                    if filters_match_request:
                        payload = {'type': operation, 'uuid': sub.uuid,
                                   'resource': resource, 'data': data}
                        await sub.ws.send_str(json.dumps(payload))

    async def subscribe_to(self, ws, uuid: str, resource: str):
        # Always unsubscribe existing duplicate identifiers
        await self.unsubscribe_from(ws, uuid)

        _resource, query, conditions, values = resource_conditions(resource)
        self.subscriptions.append(WSSubscription(
            ws=ws, fullpath=resource, resource=_resource, query=query, uuid=uuid,
            conditions=conditions, values=values))

    async def unsubscribe_from(self, ws, uuid: str = None):
        if uuid:
            self.subscriptions = list(
                filter(lambda s: uuid !=
                       s.uuid or ws != s.ws, self.subscriptions))
        else:
            self.subscriptions = list(
                filter(lambda s: ws != s.ws, self.subscriptions))

    async def websocket_handler(self, request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)

        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                try:
                    payload = json.loads(msg.data)
                    op_type = payload.get("type")
                    resource = payload.get("resource")
                    uuid = payload.get("uuid")

                    if op_type == SUBSCRIBE and uuid and resource:
                        await self.subscribe_to(ws, uuid, resource)
                    elif op_type == UNSUBSCRIBE and uuid:
                        await self.unsubscribe_from(ws, uuid)

                except Exception as err:
                    print(err, flush=True)

        # Always remove clients from listeners
        await self.unsubscribe_from(ws)
        return ws

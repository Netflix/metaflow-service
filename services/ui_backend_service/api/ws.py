from aiohttp import web, WSMsgType
from typing import List, Dict

import json
import asyncio
import collections

from .utils import resource_conditions, TTLQueue
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

    Subscribe to future events:
    {"type": "SUBSCRIBE", "uuid": "myst3rySh4ck", "resource": "/runs"}

    Subscribing to future events and return past data since unix time (seconds):
    {"type": "SUBSCRIBE", "uuid": "myst3rySh4ck", "resource": "/runs", "since": 1602752197}

    Unsubscribe:
    {"type": "UNSUBSCRIBE", "uuid": "myst3rySh4ck"}

    Example event:
    {"type": "UPDATE", "uuid": "myst3rySh4ck", "resource": "/runs", "data": {"foo": "bar"}}
    '''
    subscriptions: List[WSSubscription] = []
    queue = TTLQueue(60 * 5)  # 5 minute queue

    def __init__(self, app, event_emitter=None):
        self.app = app
        self.event_emitter = event_emitter or AsyncIOEventEmitter()
        self.db = AsyncPostgresDB.get_instance()

        event_emitter.on('notify', self.event_handler)
        app.router.add_route('GET', '/ws', self.websocket_handler)

    async def event_handler(self, operation: str, resources: List[str], data: Dict):
        # Append to QUEUE so that we can send events to users in case of
        self.queue.append({
            'operation': operation,
            'resources': resources,
            'data': data
        })

        for subscription in self.subscriptions:
            await self._event_subscription(subscription, operation, resources, data)

    async def _event_subscription(self, subscription: WSSubscription, operation: str, resources: List[str], data: Dict):
        for resource in resources:
            if subscription.resource == resource:
                # Check if possible filters match this event
                # only if the subscription actually provided conditions.
                if subscription.conditions:
                    filters_match_request = await self.db.apply_filters_to_data(
                        data=data, conditions=subscription.conditions, values=subscription.values)
                else:
                    filters_match_request = True
                if filters_match_request:
                    payload = {'type': operation, 'uuid': subscription.uuid,
                               'resource': resource, 'data': data}
                    await subscription.ws.send_str(json.dumps(payload))

    async def subscribe_to(self, ws, uuid: str, resource: str, since: int):
        # Always unsubscribe existing duplicate identifiers
        await self.unsubscribe_from(ws, uuid)

        # Create new subscription
        _resource, query, conditions, values = resource_conditions(resource)
        subscription = WSSubscription(
            ws=ws, fullpath=resource, resource=_resource, query=query, uuid=uuid,
            conditions=conditions, values=values)
        self.subscriptions.append(subscription)

        # Send previous events that client might have missed due to disconnection
        if since:
            event_queue = self.queue.values_since(since)
            for _, event in event_queue:
                await self._event_subscription(subscription, event['operation'], event['resources'], event['data'])

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
                    since = payload.get("since")
                    if since is not None and str(since).isnumeric():
                        since = int(since)
                    else:
                        since = None

                    if op_type == SUBSCRIBE and uuid and resource:
                        await self.subscribe_to(ws, uuid, resource, since)
                    elif op_type == UNSUBSCRIBE and uuid:
                        await self.unsubscribe_from(ws, uuid)

                except Exception as err:
                    print(err, flush=True)

        # Always remove clients from listeners
        await self.unsubscribe_from(ws)
        return ws

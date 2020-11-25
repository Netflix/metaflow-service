import os
import json
import time
import asyncio
import collections

from aiohttp import web, WSMsgType
from typing import List, Dict, Any, Callable

from .utils import resource_conditions, TTLQueue
from services.data.postgres_async_db import AsyncPostgresDB
from pyee import AsyncIOEventEmitter
from .data_refiner import TaskRefiner

from throttler import throttle_simultaneous

WS_QUEUE_TTL_SECONDS = os.environ.get("WS_QUEUE_TTL_SECONDS", 60 * 5)  # 5 minute TTL by default
WS_POSTPROCESS_CONCURRENCY_LIMIT = int(os.environ.get("WS_POSTPROCESS_CONCURRENCY_LIMIT", 8))

SUBSCRIBE = 'SUBSCRIBE'
UNSUBSCRIBE = 'UNSUBSCRIBE'

WSSubscription = collections.namedtuple(
    "WSSubscription", "ws disconnected_ts fullpath resource query uuid conditions values")


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

    def __init__(self, app, event_emitter=None, queue_ttl: int = WS_QUEUE_TTL_SECONDS, db=AsyncPostgresDB.get_instance()):
        self.app = app
        self.event_emitter = event_emitter or AsyncIOEventEmitter()
        self.db = db
        self.queue = TTLQueue(queue_ttl)
        self.task_refiner = TaskRefiner()

        event_emitter.on('notify', self.event_handler)
        app.router.add_route('GET', '/ws', self.websocket_handler)
        self.loop = asyncio.get_event_loop()

    async def event_handler(self, operation: str, resources: List[str], data: Dict, table=None, filter_dict: Dict = {}):
        """Either receives raw data from table triggers listener and either performs a database load
        before broadcasting from the provided table, or receives predefined data and broadcasts it as-is.
        """
        # Check if event needs to be broadcast (if anyone is subscribed to the resource)
        if any(subscription.resource in resources for subscription in self.subscriptions):
            # load the data and postprocessor for broadcasting if table
            # is provided (otherwise data has already been loaded in advance)
            if table:
                _postprocess = await self.get_table_postprocessor(table.table_name)
                _data = await load_data_from_db(table, data, filter_dict, postprocess=_postprocess)
            else:
                _data = data
            # Append event to the queue so that we can later dispatch them in case of disconnections
            #
            # NOTE: server instance specific ws queue will not work when scaling across multiple instances.
            # but on the other hand loading data and pushing everything into the queue for every server instance is also
            # a suboptimal solution.
            await self.queue.append({
                'operation': operation,
                'resources': resources,
                'data': _data
            })
            for subscription in self.subscriptions:
                if not subscription.disconnected_ts:
                    await self._event_subscription(subscription, operation, resources, _data)
                elif subscription.disconnected_ts and time.time() - subscription.disconnected_ts > WS_QUEUE_TTL_SECONDS:
                    await self.unsubscribe_from(subscription.ws, subscription.uuid)

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
            conditions=conditions, values=values, disconnected_ts=None)
        self.subscriptions.append(subscription)

        # Send previous events that client might have missed due to disconnection
        if since:
            # Subtract 1 second to make sure all events are included
            event_queue = await self.queue.values_since(since)
            for _, event in event_queue:
                await self._event_subscription(subscription, event['operation'], event['resources'], event['data'])

    async def unsubscribe_from(self, ws, uuid: str = None):
        if uuid:
            self.subscriptions = list(
                filter(lambda s: uuid != s.uuid or ws != s.ws, self.subscriptions))
        else:
            self.subscriptions = list(
                filter(lambda s: ws != s.ws, self.subscriptions))

    async def handle_disconnect(self, ws):
        """Sets disconnected timestamp on websocket subscription without removing it from the list.
        Removing is handled by event_handler that checks for expired subscriptions before emitting
        """
        self.subscriptions = list(
            map(
                lambda sub: sub._replace(disconnected_ts=time.time()) if sub.ws == ws else sub,
                self.subscriptions)
        )

    async def websocket_handler(self, request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)

        while not ws.closed:
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
        await self.handle_disconnect(ws)
        return ws

    @throttle_simultaneous(count=8)
    async def get_table_postprocessor(self, table_name):
        if table_name == self.db.task_table_postgres.table_name:
            return self.task_refiner.postprocess
        else:
            return None


async def load_data_from_db(table, data: Dict[str, Any],
                            filter_dict: Dict = {},
                            postprocess: Callable = None):
    # filter the data for loading based on available primary keys
    conditions_dict = {
        key: data[key] for key in table.primary_keys
        if key in data
    }
    filter_dict = {**conditions_dict, **filter_dict}

    conditions, values = [], []
    for k, v in filter_dict.items():
        conditions.append("{} = %s".format(k))
        values.append(v)

    results, _ = await table.find_records(
        conditions=conditions, values=values, fetch_single=True,
        enable_joins=True, postprocess=postprocess
    )
    return results.body

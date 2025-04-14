import os
import json
import time
import asyncio
import collections

from aiohttp import web, WSMsgType
from typing import List, Dict, Any, Callable

from .utils import resource_conditions, TTLQueue, postprocess_chain
from services.utils import logging
from pyee import AsyncIOEventEmitter
from ..data.refiner import TaskRefiner, ArtifactRefiner

from throttler import throttle_simultaneous

from services.data.db_utils import DBResponse
from services.data.tagging_utils import apply_run_tags_to_db_response

WS_QUEUE_TTL_SECONDS = os.environ.get("WS_QUEUE_TTL_SECONDS", 60 * 5)  # 5 minute TTL by default
WS_POSTPROCESS_CONCURRENCY_LIMIT = int(os.environ.get("WS_POSTPROCESS_CONCURRENCY_LIMIT", 8))

SUBSCRIBE = 'SUBSCRIBE'
UNSUBSCRIBE = 'UNSUBSCRIBE'

WSSubscription = collections.namedtuple(
    "WSSubscription", "ws disconnected_ts fullpath resource query uuid filter")


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
    _subscriptions: Dict[web.WebSocketResponse, List[WSSubscription]] = collections.defaultdict(list)

    def __init__(self, app, db, event_emitter=None, queue_ttl: int = WS_QUEUE_TTL_SECONDS, cache=None):
        self.event_emitter = event_emitter or AsyncIOEventEmitter()
        self.db = db
        self.queue = TTLQueue(queue_ttl)
        self.task_refiner = TaskRefiner(cache=cache.artifact_cache) if cache else None
        self.artifact_refiner = ArtifactRefiner(cache=cache.artifact_cache) if cache else None
        self.logger = logging.getLogger("Websocket")

        event_emitter.on('notify', self.event_handler)
        app.router.add_route('GET', '/ws', self.websocket_handler)
        self.loop = asyncio.get_event_loop()

    async def event_handler(self, operation: str, resources: List[str], data: Dict, table_name: str = None, filter_dict: Dict = {}):
        """
        Event handler for websocket events on 'notify'.
        Either receives raw data from table triggers listener and either performs a database load
        before broadcasting from the provided table, or receives predefined data and broadcasts it as-is.

        Parameters
        ----------
        operation : str
            name of the operation related to the DB event, either 'INSERT' or 'UPDATE'
        resources : List[str]
            List of resource paths that this event is related to. Used strictly for broadcasting to
            websocket subscriptions
        data : Dict
            The data of the record to be broadcast. Can either be complete, or partial.
            In case of partial data (and a provided table name) this is only used for the DB query.
        table_name : str (optional)
            name of the table that the complete data should be queried from.
        filter_dict : Dict (optional)
            a dictionary of filters used in the query when fetching complete data.
        """
        # Check if event needs to be broadcast (if anyone is subscribed to the resource)
        if any(subscription.resource in resources for subscription in self.subscriptions()):
            # load the data and postprocessor for broadcasting if table
            # is provided (otherwise data has already been loaded in advance)
            if table_name:
                table = self.db.get_table_by_name(table_name)
                _postprocess = await self.get_table_postprocessor(table_name)
                _data = await load_data_from_db(table, data, filter_dict, postprocess=_postprocess)
            else:
                _data = data

            if not _data:
                # Skip sending this event to subscriptions in case data is None or empty.
                # This could be caused by insufficient/broken data and can break the UI.
                return

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
            for subscription in self.subscriptions():
                try:
                    if subscription.disconnected_ts and time.time() - subscription.disconnected_ts > WS_QUEUE_TTL_SECONDS:
                        # We can assume that all websockets (not just this UUID) are disconnected, don't filter by UUID as well.
                        await self.unsubscribe_from(subscription.ws)
                    else:
                        await self._event_subscription(subscription, operation, resources, _data)
                except ConnectionResetError:
                    self.logger.debug("Trying to broadcast to a stale subscription. Unsubscribing")
                    await self.unsubscribe_from(subscription.ws)
                except Exception:
                    self.logger.exception("Broadcasting to subscription failed")

    def subscriptions(self):
        # Grab all of the keys upfront and use that to iterate so that callers can
        # safely modify the subscriptions dict while we are iterating through it.
        # This is primarily useful when calling `unsubscribe_from` during the cleanup
        # loop in the event handler.
        for k in list(self._subscriptions.keys()):
            for sub in self._subscriptions[k]:
                yield sub

    async def _event_subscription(self, subscription: WSSubscription, operation: str, resources: List[str], data: Dict):
        for resource in resources:
            if subscription.resource == resource:
                # Check if possible filters match this event
                # only if the subscription actually provided conditions.
                if subscription.filter:
                    filters_match_request = subscription.filter(data)
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
        _resource, query, filter_fn = resource_conditions(resource)
        subscription = WSSubscription(
            ws=ws, fullpath=resource, resource=_resource, query=query, uuid=uuid,
            filter=filter_fn, disconnected_ts=None)
        self._subscriptions[ws].append(subscription)

        # Send previous events that client might have missed due to disconnection
        if since:
            # Subtract 1 second to make sure all events are included
            event_queue = await self.queue.values_since(since)
            for _, event in event_queue:
                self.loop.create_task(
                    self._event_subscription(subscription, event['operation'], event['resources'], event['data'])
                )

    async def unsubscribe_from(self, ws, uuid: str = None):
        if ws not in self._subscriptions:
            return
        if uuid:
            self._subscriptions[ws] = list(
                filter(lambda s: uuid != s.uuid or ws != s.ws, self._subscriptions[ws]))
            if len(self._subscriptions[ws]) == 0:
                del self._subscriptions[ws]
        else:
            del self._subscriptions[ws]

    async def handle_disconnect(self, ws):
        """
        Sets disconnected timestamp on websocket subscription without removing it from the list.
        Removing is handled by event_handler that checks for expired subscriptions before emitting
        """
        self._subscriptions[ws] = list(
            map(
                lambda sub: sub._replace(disconnected_ts=time.time()),
                self._subscriptions[ws])
        )

    async def websocket_handler(self, request):
        "Handler for received messages from the open Web Socket connection."
        # TODO: Consider using options autoping=True and heartbeat=20 if supported by clients.
        ws = web.WebSocketResponse()
        await ws.prepare(request)

        while not ws.closed:
            async for msg in ws:
                if msg.type == WSMsgType.TEXT:
                    try:
                        # Custom ping message handling.
                        # If someone is pinging, lets answer with pong rightaway.
                        if msg.data == "__ping__":
                            await ws.send_str("__pong__")
                        else:
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
                    except Exception:
                        self.logger.exception("Exception occurred.")

        # Always remove clients from listeners
        await self.handle_disconnect(ws)
        return ws

    @throttle_simultaneous(count=8)
    async def get_table_postprocessor(self, table_name):
        refiner_postprocess = None
        table = None
        if table_name == self.db.task_table_postgres.table_name:
            table = self.db.run_table_postgres
            refiner_postprocess = self.task_refiner.postprocess
        elif table_name == self.db.artifact_table_postgres.table_name:
            table = self.db.run_table_postgres
            refiner_postprocess = self.artifact_refiner.postprocess

        if table:
            async def _tags_postprocess(db_response: DBResponse, invalidate_cache=False):
                flow_id = db_response.body.get('flow_id')
                run_id = db_response.body.get('run_id') or db_response.body.get('run_number')
                if not flow_id or not run_id:
                    self.logger.warning("Missing flow_id or run_id (or run_number) for a record from table {}".format(table.table_name))
                    return db_response
                return await apply_run_tags_to_db_response(flow_id, run_id, table, db_response)
            return postprocess_chain([_tags_postprocess, refiner_postprocess])
        return refiner_postprocess


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

    results, *_ = await table.find_records(
        conditions=conditions, values=values, fetch_single=True,
        enable_joins=True,
        expanded=True,
        postprocess=postprocess
    )
    return results.body

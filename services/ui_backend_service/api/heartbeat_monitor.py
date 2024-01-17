import asyncio
import datetime
from typing import Dict, Callable, Optional

from pyee import AsyncIOEventEmitter

from .utils import postprocess_chain, apply_run_tags_postprocess
from ..data.db.tables.base import HEARTBEAT_THRESHOLD
from ..data.refiner import TaskRefiner
from .notify import resource_list

# interval for how often to check heartbeats. Use the heartbeat_threshold from the database queries with a margin (10sec),
# not to check heartbeats too often and miss failures as a result.
HEARTBEAT_INTERVAL = HEARTBEAT_THRESHOLD + 10


class HeartbeatMonitor(object):
    """
    Generic class for starting an async watcher task that periodically checks timestamps for a list of objects,
    and performs load_and_broadcast() for expired ones.

    Listens on event_emitter for 'event_name' and calls heartbeat_handler() for processing events.

    Parameters
    ----------
    event_name : str
        string name of an event to bind to on the event_emitter
    db : PostgresDB
        initialized database adapter instance to be used with load_and_broadcast()
    event_emitter : AsyncIOEventEmitter
        Any event emitter instance that implements .on('event_name', callback)
    cache : AsyncCacheClient
        cache client instance that implements the required actions.
    """

    def __init__(self, event_name: str, db, event_emitter=None, cache=None):
        self.watched = {}
        # Handle HB Events
        self.event_emitter = event_emitter or AsyncIOEventEmitter()
        event_emitter.on(event_name, self.heartbeat_handler)
        self.db = db
        self.cache = cache

        # Start heartbeat watcher
        self.loop = asyncio.get_event_loop()
        self.loop.create_task(self.check_heartbeats())

    async def heartbeat_handler(self):
        "handle the event_emitter events"
        raise NotImplementedError

    async def add_to_watch(self):
        "Adds object to heartbeat monitoring"
        raise NotImplementedError

    def remove_from_watch(self, key):
        "Removes object from heartbeat monitoring"
        self.watched.pop(key, None)

    async def load_and_broadcast(self, key):
        """
        Triggered when a heartbeat for a key has expired.
        Loads object based on key from watchlist, and broadcasts content to listeners.
        """
        raise NotImplementedError

    async def check_heartbeats(self):
        """
        Async Task that is responsible for checking the heartbeats of all monitored runs,
        and triggering handlers in case the heartbeat is too old.
        """
        while True:
            time_now = heartbeat_time_now()
            for key, hb in list(self.watched.items()):
                if time_now - hb > HEARTBEAT_INTERVAL:
                    self.loop.create_task(self.load_and_broadcast(key))
                    self.remove_from_watch(key)

            await asyncio.sleep(HEARTBEAT_INTERVAL)


class RunHeartbeatMonitor(HeartbeatMonitor):
    """
    Service class for adding objects with heartbeat timestamps to be monitored and acted upon
    when heartbeat becomes too old.

    Starts an async watcher loop that periodically checks the heartbeat of the subscribed runs for expired timestamps

    Usage
    -----
    Responds to event_emitter emissions with messages:
      "run-heartbeat", "update", data -> updates heartbeat timestamp that is passed as part of data.
      "run-heartbeat", "complete" data -> removes run from heartbeat checks
    """

    def __init__(self, event_emitter=None, db=None):
        # Init the abstract class
        super().__init__(
            event_name="run-heartbeat",
            db=db,
            event_emitter=event_emitter
        )
        # Table for data fetching for load_and_broadcast and add_to_watch
        self._run_table = self.db.run_table_postgres

    async def heartbeat_handler(self, action: str, data: Dict):
        """
        Event handler for heartbeat events on 'run-heartbeat'

        Parameters
        ----------
        action : str
            'update' or 'complete', depending on if we want to update(or add) an existing heartbeat,
            or remove one from monitoring after completion.
        run_number : int
            the run number to update the heartbeat for.
        """
        if action == "update":
            await self.add_to_watch(data)
        elif action == "complete":
            self.remove_from_watch(data.get("run_number", None))

    async def add_to_watch(self, run: Dict):
        if "last_heartbeat_ts" in run and "run_number" in run:
            run_number = run["run_number"]
            heartbeat_ts = run["last_heartbeat_ts"] or heartbeat_time_now()
            self.watched[run_number] = heartbeat_ts

    async def get_run(self, run_key: str) -> Optional[Dict]:
        "Fetch run with a given id or number from the DB"
        # NOTE: task being broadcast should contain the same fields as the GET request returns so UI can easily infer changes.
        result = await self._run_table.get_expanded_run(run_key)
        return result.body if result.response_code == 200 else None

    async def load_and_broadcast(self, key):
        run = await self.get_run(key)
        resources = resource_list(self._run_table.table_name, run) if run else None
        if not resources:
            return
        if run['status'] == "running":
            await self.add_to_watch(run)
        if run['status'] == "failed":
            # The purpose of the monitor is to emit otherwise unnoticed failed attempts.
            # Do not unnecessarily broadcast other statuses that already get propagated by Notify.
            self.event_emitter.emit('notify', 'UPDATE', resources, run)


class TaskHeartbeatMonitor(HeartbeatMonitor):
    """
    Service class for adding objects with heartbeat timestamps to be monitored and acted upon
    when heartbeat becomes too old.

    Starts an async watcher loop that periodically checks the heartbeat of the subscribed tasks for expired timestamps

    Usage
    -----
    Responds to event_emitter emissions with messages:
      "task-heartbeat", "update", data -> updates heartbeat timestamp that is found in database
      "task-heartbeat", "complete" data -> removes task from heartbeat checks
    """

    def __init__(self, event_emitter=None, db=None, cache=None):
        # Init the abstract class
        super().__init__(
            event_name="task-heartbeat",
            db=db,
            event_emitter=event_emitter,
            cache=cache
        )
        # Table for data fetching for load_and_broadcast and add_to_watch
        self._task_table = self.db.task_table_postgres
        self._run_table = self.db.run_table_postgres
        self.refiner = TaskRefiner(cache=self.cache.artifact_cache) if cache else None

    async def heartbeat_handler(self, action: str, data: Dict):
        """
        Event handler for task heartbeat events on 'task-heartbeat'

        Parameters
        ----------
        action : str
            'update' or 'complete' depending on if we want to update(or add) an existing heartbeat,
            or remove one from monitoring after completion.
        data : Dict
            The task object that the heartbeat event relates to.
        """
        if action == "update":
            await self.add_to_watch(data)
        elif action == "complete":
            key = self.generate_dict_key(data)
            self.remove_from_watch(key)

    async def add_to_watch(self, data):
        # TODO: Optimize db trigger so we do not have to fetch a record in order to add it to the
        # heartbeat monitor
        task = await self._get_task(
            data["flow_id"],
            data["run_number"],
            data["step_name"],
            data["task_id"]
        )

        key = self.generate_dict_key(task)
        if key and "last_heartbeat_ts" in task:
            heartbeat_ts = task["last_heartbeat_ts"] or heartbeat_time_now()
            self.watched[key] = heartbeat_ts

    async def _get_task(self, flow_id: str, run_key: str, step_name: str, task_key: str, attempt_id: int = None, postprocess=None) -> Optional[Dict]:
        """
        Fetches task from DB. Specifying attempt_id will fetch the specific attempt.
        Otherwise the newest attempt is returned.

        Returns
        -------
        Dict or None
            either the task Dict object is returned, or if no task is found, None is returned.
        """
        # NOTE: task being broadcast should contain the same fields as the GET request returns so UI can easily infer changes.
        result = await self._task_table.get_task_attempt(flow_id, run_key, step_name, task_key,
                                                         attempt_id=attempt_id,
                                                         postprocess=postprocess_chain(
                                                             [apply_run_tags_postprocess(flow_id, run_key, self._run_table), postprocess]))

        return result.body if result.response_code == 200 else None

    async def load_and_broadcast(self, key):
        flow_id, run_number, step_name, task_id, attempt_id = self.decode_key_ids(key)
        task = await self._get_task(flow_id, run_number, step_name, task_id, attempt_id, postprocess=self.refiner.postprocess)
        resources = resource_list(self._task_table.table_name, task) if task else None
        if not resources:
            return
        
        if task['status'] == "running":
            await self.add_to_watch(task)
        if task['status'] == "failed":
            # The purpose of the monitor is to emit otherwise unnoticed failed attempts.
            # Do not unnecessarily broadcast other statuses that already get propagated by Notify.
            self.event_emitter.emit('notify', 'UPDATE', resources, task)

    def generate_dict_key(self, data):
        "Creates an unique key for the 'watched' dictionary for storing the heartbeat of a specific task"
        try:
            return "{flow_id}/{run_number}/{step_name}/{task_id}/{attempt_id}".format(**data)
        except:
            return None

    def decode_key_ids(self, key):
        flow, run, step, task, attempt = key.split("/")
        return flow, run, step, task, attempt

def heartbeat_time_now():
    # same format as the metadata heartbeat uses
    return int(datetime.datetime.utcnow().timestamp())
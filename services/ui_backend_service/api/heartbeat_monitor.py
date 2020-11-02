import asyncio
import datetime
from typing import Dict
from pyee import ExecutorEventEmitter
from services.data.postgres_async_db import AsyncPostgresDB
from .notify import resource_list
from .data_refiner import TaskRefiner

HEARTBEAT_INTERVAL = 10 # interval of heartbeats, in seconds

class HeartbeatMonitor(object):
  def __init__(self, event_name, event_emitter=None):
    self.watched = {}
    # Handle HB Events
    self.event_emitter = event_emitter or ExecutorEventEmitter()
    event_emitter.on(event_name, self._heartbeat_handler)

    # Start heartbeat watcher
    self.loop = asyncio.get_event_loop()
    self.loop.create_task(self.check_heartbeats())

  def _heartbeat_handler(self, *args, **kwargs):
    """Wrapper to run coroutine event handler code threadsafe in a loop,
    as the ExecutorEventEmitter does not accept coroutines as handlers.
    """
    asyncio.run_coroutine_threadsafe(self.heartbeat_handler(*args, **kwargs), self.loop)

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
    '''Triggered when a heartbeat for a key has expired.
    Loads object based on key from watchlist, and broadcasts content to listeners.'''
    raise NotImplementedError

  async def check_heartbeats(self):
    '''
    Async Task that is responsible for checking the heartbeats of all monitored runs,
    and triggering handlers in case the heartbeat is too old.
    '''
    while True:
      time_now = int(datetime.datetime.utcnow().timestamp()) # same format as the metadata heartbeat uses
      for key, hb in list(self.watched.items()):
        if time_now - hb > HEARTBEAT_INTERVAL * 2:
          await self.load_and_broadcast(key)
          self.remove_from_watch(key)
          
      await asyncio.sleep(HEARTBEAT_INTERVAL)

class RunHeartbeatMonitor(HeartbeatMonitor):
  '''
  Service class for adding objects with heartbeat timestamps to be monitored and acted upon
  when heartbeat becomes too old.

  Starts an async watcher loop that periodically checks the heartbeat of the subscribed runs for expired timestamps

  Usage
  -----
  Responds to event_emitter emissions with messages:
    "run-heartbeat", "update", run_id -> updates heartbeat timestamp that is found in database
    "run-heartbeat", "complete" run_id -> removes run from heartbeat checks
  '''
  def __init__(self, event_emitter=None):
    # Init the abstract class
    super().__init__(
      event_name="run-heartbeat",
      event_emitter=event_emitter
    )
    # Table for data fetching for load_and_broadcast and add_to_watch
    self._run_table = AsyncPostgresDB.get_instance().run_table_postgres

  async def heartbeat_handler(self, action, run_id):
    if action == "update":
      await self.add_to_watch(run_id)
    elif action == "complete":
      self.remove_from_watch(run_id)
    
  async def add_to_watch(self, run_id):
    # TODO: Optimize db trigger so we do not have to fetch a record in order to add it to the
    # heartbeat monitor
    run = await self.get_run(run_id)

    if "last_heartbeat_ts" in run and "run_number" in run:
      run_number = run["run_number"]
      heartbeat_ts = run["last_heartbeat_ts"]
      if heartbeat_ts is not None: # only start monitoring on runs that have a heartbeat
        self.watched[run_number] = heartbeat_ts

  async def get_run(self, run_id):
    # Remember to enable_joins for the query, otherwise the 'status' will be missing from the run
    # and we can not broadcast an up-to-date status.
    result, _ = await self._run_table.find_records(
                            conditions=["run_number = %s"],
                            values=[run_id],
                            fetch_single=True,
                            enable_joins=True
                          )
    return result.body if result.response_code==200 else None
  
  async def load_and_broadcast(self, key):
    run = await self.get_run(key)
    resources = resource_list(self._run_table.table_name, run)
    self.event_emitter.emit('notify', 'UPDATE', self._run_table, resources, run)

class TaskHeartbeatMonitor(HeartbeatMonitor):
  '''
  Service class for adding objects with heartbeat timestamps to be monitored and acted upon
  when heartbeat becomes too old.

  Starts an async watcher loop that periodically checks the heartbeat of the subscribed tasks for expired timestamps

  Usage
  -----
  Responds to event_emitter emissions with messages:
    "task-heartbeat", "update", data -> updates heartbeat timestamp that is found in database
    "task-heartbeat", "complete" data -> removes task from heartbeat checks
  '''
  def __init__(self, event_emitter=None):
    # Init the abstract class
    super().__init__(
      event_name="task-heartbeat",
      event_emitter=event_emitter
    )
    # Table for data fetching for load_and_broadcast and add_to_watch
    self._task_table = AsyncPostgresDB.get_instance().task_table_postgres
    self.refiner = TaskRefiner()

  async def heartbeat_handler(self, action, data):
    if action == "update":
      await self.add_to_watch(data)
    elif action == "complete":
      key = self.generate_dict_key(data)
      self.remove_from_watch(key)
    
  async def add_to_watch(self, data):
    # TODO: Optimize db trigger so we do not have to fetch a record in order to add it to the
    # heartbeat monitor
    task = await self.get_task(
      data["flow_id"],
      data["run_number"],
      data["step_name"],
      data["task_id"]
    )

    key = self.generate_dict_key(task)
    if key and "last_heartbeat_ts" in task:
      heartbeat_ts = task["last_heartbeat_ts"]
      if heartbeat_ts is not None: # only start monitoring on runs that have a heartbeat
        self.watched[key] = heartbeat_ts

  async def get_task(self, flow_id, run_number, step_name, task_id, attempt_id = None):
    "Fetches task from DB. Specifying attempt_id will fetch the specific attempt. Otherwise the newest attempt is returned."
    # Remember to enable_joins for the query, otherwise the 'status' will be missing from the task
    # and we can not broadcast an up-to-date status.
    conditions = ["flow_id = %s", "run_number = %s", "step_name = %s", "task_id = %s"]
    values = [flow_id, run_number, step_name, task_id]
    if attempt_id:
      conditions.append("attempt_id = %s")
      values.append(attempt_id)

    result, _ = await self._task_table.find_records(
                            conditions=conditions,
                            values=values,
                            order=["attempt_id DESC"],
                            fetch_single=True,
                            enable_joins=True,
                            postprocess=self.refiner.postprocess
                          )
    return result.body if result.response_code==200 else None
  
  async def load_and_broadcast(self, key):
    flow_id, run_number, step_name, task_id, attempt_id = self.decode_key_ids(key)
    task = await self.get_task(flow_id, run_number, step_name, task_id, attempt_id)
    resources = resource_list(self._task_table.table_name, task)
    if resources and task['status'] == "failed":
      # The purpose of the monitor is to emit otherwise unnoticed failed attempts.
      # Do not unnecessarily broadcast other statuses that already get propagated by Notify.
      self.event_emitter.emit('notify', 'UPDATE', self._task_table, resources, task)
  
  def generate_dict_key(self, data):
    "Creates an unique key for the 'watched' dictionary for storing the heartbeat of a specific task"
    try:
      return "{flow_id}/{run_number}/{step_name}/{task_id}/{attempt_id}".format(**data)
    except:
      return None
  
  def decode_key_ids(self, key):
    flow, run, step, task, attempt = key.split("/")
    return flow, run, step, task, attempt

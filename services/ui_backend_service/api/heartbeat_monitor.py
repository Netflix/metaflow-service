import asyncio
import datetime
from typing import Dict
from pyee import AsyncIOEventEmitter
from services.data.postgres_async_db import AsyncPostgresDB
from .notify import resource_list

HEARTBEAT_INTERVAL = 10 # interval of heartbeats, in seconds

class HeartbeatMonitor(object):
  def __init__(self, event_name, event_emitter=None):
    self.watched = {}
    # Handle DB Events
    self.event_emitter = event_emitter or AsyncIOEventEmitter()
    event_emitter.on(event_name, self.heartbeat_handler)

    # Start heartbeat watcher
    loop = asyncio.get_event_loop()
    loop.create_task(self.check_heartbeats())

  async def heartbeat_handler(self):
    "handle the event_emitter events"
    raise NotImplementedError

  async def add_to_watch(self):
    "Adds object to heartbeat monitoring"
    raise NotImplementedError

  async def remove_from_watch(self, key):
    "Removes object from heartbeat monitoring"
    raise NotImplementedError

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
      # print(f"checking heartbeats of {len(self.watched)} runs, timestamp: {time_now}", flush=True)
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
    run = await self.get_run(run_id)

    if "last_heartbeat_ts" in run and "run_number" in run:
      run_number = run["run_number"]
      heartbeat_ts = run["last_heartbeat_ts"]
      if heartbeat_ts is not None: # only start monitoring on runs that have a heartbeat
        self.watched[run_number] = heartbeat_ts

  def remove_from_watch(self, run_id):
    self.watched.pop(run_id, None)

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
  
  async def load_and_broadcast(self, run_id):
    run = await self.get_run(run_id)
    resources = resource_list(self._run_table.table_name, run)
    self.event_emitter.emit('notify', 'UPDATE', resources, run)


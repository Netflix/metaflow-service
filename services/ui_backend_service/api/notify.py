import json
import asyncio
from typing import Dict, List
from services.data.postgres_async_db import AsyncPostgresDB
from pyee import ExecutorEventEmitter


class ListenNotify(object):
    def __init__(self, app, event_emitter=None):
        self.app = app
        self.event_emitter = event_emitter or ExecutorEventEmitter()
        self.db = AsyncPostgresDB.get_instance()

        self.loop = asyncio.get_event_loop()
        self.loop.create_task(self._init())

    async def _init(self):
        pool = self.db.pool

        async with pool.acquire() as conn1:
            listener = self.listen(conn1)
            await asyncio.gather(listener)

    async def listen(self, conn):
        async with conn.cursor() as cur:
            await cur.execute("LISTEN notify")
            while True:
                msg = await conn.notifies.get()
                await self.handle_trigger_msg(msg)

    async def handle_trigger_msg(self, msg: str):
        try:
            payload = json.loads(msg.payload)

            table_name = payload.get("table")
            operation = payload.get("operation")
            data = payload.get("data")

            table = await self.db.get_table_by_name(table_name)
            if table != None:
                resources = resource_list(table.table_name, data)

                # Broadcast this event to `api/ws.py` (Websocket.event_handler)
                # and notify each Websocket connection about this event
                if resources != None and len(resources) > 0:
                    await _broadcast(self.event_emitter, operation, table, data)

                # Heartbeat watcher for Runs.
                if table.table_name == self.db.run_table_postgres.table_name:
                    self.event_emitter.emit('run-heartbeat', 'update', data['run_number'])

                # Heartbeat watcher for Tasks.
                # if table.table_name == self.db.task_table_postgres.table_name:
                #     self.event_emitter.emit('task-heartbeat', 'update', data)

                # Notify when Run parameters are ready.
                if operation == "INSERT" and \
                        table.table_name == self.db.step_table_postgres.table_name and \
                        data["step_name"] == "start":
                    self.event_emitter.emit("run-parameters", data['flow_id'], data['run_number'])

                # Notify related resources once new `_task_ok` artifact has been created
                if operation == "INSERT" and \
                        table.table_name == self.db.artifact_table_postgres.table_name and \
                        data["name"] == "_task_ok":

                    # remove heartbeat watcher for completed task
                    # self.event_emitter.emit("task-heartbeat", "complete", data)

                    # Always mark task finished if '_task_ok' artifact is created
                    # Include 'attempt_id' so we can identify which attempt this artifact related to
                    _attempt_id = data.get("attempt_id", 0)
                    # First attempt has already been inserted by task table trigger.
                    # Later attempts must count as inserts to register properly for the UI
                    _op = "UPDATE" if _attempt_id == 0 else "INSERT"
                    await _broadcast(
                        event_emitter=self.event_emitter,
                        operation=_op,
                        table=self.db.task_table_postgres,
                        data=data,
                        filter_dict={"attempt_id": _attempt_id}
                    )

                    # Last step is always called 'end' and only one '_task_ok' should be present
                    # Run is considered finished once 'end' step has '_task_ok' artifact
                    if data["step_name"] == "end":
                        await _broadcast(
                            self.event_emitter, "UPDATE", self.db.run_table_postgres,
                            data)
                        # Also trigger preload of artifacts after a run finishes.
                        self.event_emitter.emit("preload-artifacts", data['run_number'])
                        # And remove possible heartbeat watchers for completed runs
                        self.event_emitter.emit("run-heartbeat", "complete", data['run_number'])

        except Exception as err:
            print(err, flush=True)

def resource_list(table_name: str, data: Dict):
    resource_paths = {
        "flows_v3": [
            "/flows",
            "/flows/{flow_id}"
        ],
        "runs_v3": [
            "/runs",
            "/flows/{flow_id}/runs",
            "/flows/{flow_id}/runs/{run_number}"
        ],
        "steps_v3": [
            "/flows/{flow_id}/runs/{run_number}/steps",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}"
        ],
        "tasks_v3": [
            "/flows/{flow_id}/runs/{run_number}/tasks",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/attempts"
        ]
    }
    if table_name in resource_paths:
        return [path.format(**data) for path in resource_paths[table_name]]
    return []


async def _broadcast(event_emitter, operation: str, table, data: Dict, filter_dict={}):
    _resources = resource_list(table.table_name, data)
    event_emitter.emit('notify', operation, _resources, data, table, filter_dict)

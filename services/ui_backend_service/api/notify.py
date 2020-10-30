import json
import asyncio
from typing import Dict, List, Any, Callable
from services.data.postgres_async_db import AsyncPostgresDB
from pyee import AsyncIOEventEmitter
from .data_refiner import TaskRefiner


class ListenNotify(object):
    def __init__(self, app, event_emitter=None):
        self.app = app
        self.event_emitter = event_emitter or AsyncIOEventEmitter()
        self.db = AsyncPostgresDB.get_instance()
        self.task_refiner = TaskRefiner()

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
                postprocess = self.get_table_postprocessor(table.table_name)
                # Broadcast this event to `api/ws.py` (Websocket.event_handler)
                # and notify each Websocket connection about this event
                if resources != None and len(resources) > 0:
                    await load_and_broadcast(self.event_emitter, operation, table,
                                                data, table.primary_keys, postprocess=postprocess)

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
                    await load_and_broadcast(
                        event_emitter=self.event_emitter,
                        operation=_op,
                        table=self.db.task_table_postgres,
                        data=data,
                        keys=self.db.task_table_postgres.primary_keys,
                        custom_filter_dict={"attempt_id": _attempt_id},
                        postprocess=self.task_refiner.postprocess
                    )

                    # Last step is always called 'end' and only one '_task_ok' should be present
                    # Run is considered finished once 'end' step has '_task_ok' artifact
                    if data["step_name"] == "end":
                        await load_and_broadcast(
                            self.event_emitter, "UPDATE", self.db.run_table_postgres,
                            data, self.db.run_table_postgres.primary_keys)
                        # Also trigger preload of artifacts after a run finishes.
                        self.event_emitter.emit("preload-artifacts", data['run_number'])
                        # And remove possible heartbeat watchers for completed runs
                        self.event_emitter.emit("run-heartbeat", "complete", data['run_number'])

        except Exception as err:
            print(err, flush=True)

    def get_table_postprocessor(self, table_name):
        if table_name == self.db.task_table_postgres.table_name:
            return self.task_refiner.postprocess
        else:
            return None


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
        ],
        # "metadata_v3": [
        #     "/flows/{flow_id}/runs/{run_number}/metadata",
        #     "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/metadata"
        # ],
        # "artifact_v3": [
        #     "/flows/{flow_id}/runs/{run_number}/artifacts",
        #     "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/artifacts",
        #     "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/artifacts"
        # ],
    }
    if table_name in resource_paths:
        return [path.format(**data) for path in resource_paths[table_name]]
    return []


async def load_data_from_db(table, filter_dict: Dict[str, Any],
                            postprocess: Callable = None):
    conditions, values = [], []
    for k, v in filter_dict.items():
        conditions.append("{} = %s".format(k))
        values.append(v)

    results, _ = await table.find_records(
        conditions=conditions, values=values, fetch_single=True,
        enable_joins=True, postprocess=postprocess
    )
    resources = resource_list(table.table_name, results.body)
    return results.body, resources


async def load_and_broadcast(event_emitter, operation: str, table,
                             data: Dict, keys: List[str], custom_filter_dict={},
                             postprocess: Callable = None):
    filter_dict = {}
    for k in keys:
        filter_dict[k] = data[k]

    _data, _resources = await load_data_from_db(table, {**filter_dict, **custom_filter_dict}, postprocess)
    event_emitter.emit('notify', operation, _resources, _data)

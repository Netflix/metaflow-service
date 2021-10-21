import json
import asyncio
from typing import Dict
from services.utils import logging
from services.data.postgres_async_db import (
    FLOW_TABLE_NAME, RUN_TABLE_NAME,
    STEP_TABLE_NAME, TASK_TABLE_NAME,
    METADATA_TABLE_NAME, ARTIFACT_TABLE_NAME
)
from pyee import AsyncIOEventEmitter


class ListenNotify(object):
    """
    Class for starting an async listener task that listens on a DB connection for notifications,
    and processes these as events before broadcasting them on the provided event_emitter.

    Consumes messages from DB connection with 'LISTEN notify' and processes the contents before passing
    to event_emitter.emit('notify', *args)

    Parameters
    ----------
    db : AsyncPostgresDB
        initialized instance of a postgresDB adapter
    event_emitter : AsyncIOEventEmitter
        Any event emitter class that implements .emit('notify', *args)
    """

    def __init__(self, app, db, event_emitter=None):
        self.event_emitter = event_emitter or AsyncIOEventEmitter()
        self.db = db
        self.logger = logging.getLogger("ListenNotify")

        self.loop = asyncio.get_event_loop()
        self.loop.create_task(self._init(self.db.pool))

    async def _init(self, pool):
        while True:
            try:
                async with pool.acquire() as conn:
                    self.logger.info("Connection acquired")
                    await asyncio.gather(
                        self.listen(conn),
                        self.ping(conn)
                    )
            except Exception as ex:
                self.logger.warn(str(ex))
            finally:
                await asyncio.sleep(1)

    async def listen(self, conn):
        async with conn.cursor() as cur:
            await cur.execute("LISTEN notify")
            while not cur.closed:
                try:
                    msg = conn.notifies.get_nowait()
                    self.loop.create_task(self.handle_trigger_msg(msg))
                except asyncio.QueueEmpty:
                    await asyncio.sleep(0.1)
                except Exception:
                    self.logger.exception("Exception when listening to notify.")

    async def ping(self, conn):
        async with conn.cursor() as cur:
            while not cur.closed:
                try:
                    await cur.execute("NOTIFY listen")
                except Exception:
                    self.logger.debug("Exception NOTIFY ping.")
                finally:
                    await asyncio.sleep(1)

    async def handle_trigger_msg(self, msg: str):
        "Handler for the messages received from 'LISTEN notify'"
        try:
            payload = json.loads(msg.payload)

            table_name = payload.get("table")
            operation = payload.get("operation")
            data = payload.get("data")

            table = self.db.get_table_by_name(table_name)
            if table is not None:
                resources = resource_list(table.table_name, data)

                # Broadcast this event to `api/ws.py` (Websocket.event_handler)
                # and notify each Websocket connection about this event
                if resources is not None and len(resources) > 0:
                    await _broadcast(self.event_emitter, operation, table, data)

                # Heartbeat watcher for Runs.
                if table.table_name == self.db.run_table_postgres.table_name:
                    self.event_emitter.emit('run-heartbeat', 'update', data)

                # Heartbeat watcher for Tasks.
                if table.table_name == self.db.task_table_postgres.table_name:
                    self.event_emitter.emit('task-heartbeat', 'update', data)

                # Notify when Run parameters are ready.
                if operation == "INSERT" and \
                        table.table_name == self.db.step_table_postgres.table_name and \
                        data["step_name"] == "start":
                    self.event_emitter.emit("preload-run-parameters", data['flow_id'], data['run_number'])

                # Notify task resources of a new attempt if 'attempt' metadata is inserted.
                if operation == "INSERT" and \
                        table.table_name == self.db.metadata_table_postgres.table_name and \
                        data["field_name"] == "attempt":

                    # Extract the attempt number from metadata attempt value, so we know which task attempt to broadcast.
                    _attempt_id = int(data.get("value", 0))
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

                # Notify related resources once new `_task_ok` artifact has been created
                if operation == "INSERT" and \
                        table.table_name == self.db.artifact_table_postgres.table_name and \
                        data["name"] == "_task_ok":

                    # remove heartbeat watcher for completed task
                    self.event_emitter.emit("task-heartbeat", "complete", data)

                    # Always mark task finished if '_task_ok' artifact is created
                    # Include 'attempt_id' so we can identify which attempt this artifact related to
                    _attempt_id = data.get("attempt_id", 0)
                    await _broadcast(
                        event_emitter=self.event_emitter,
                        operation="UPDATE",
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
                        self.event_emitter.emit("preload-task-statuses", data['flow_id'], data['run_number'])
                        # And remove possible heartbeat watchers for completed runs
                        self.event_emitter.emit("run-heartbeat", "complete", data)

                # Notify DAG cache store to preload artifact
                if operation == "INSERT" and \
                        table.table_name == self.db.metadata_table_postgres.table_name and \
                        data["step_name"] == "start" and \
                        data["field_name"] in ["code-package-url", "code-package"]:
                    self.event_emitter.emit("preload-dag", data['flow_id'], data['run_number'])

        except Exception:
            self.logger.exception("Exception occurred")


def resource_list(table_name: str, data: Dict):
    """
    List of RESTful resources that the provided table and data are included in.

    Used for determining which Web Socket subscriptions this resource relates to.

    Parameters
    ----------
    table_name : str
        table name that the Data belongs to
    data : Dict
        Dictionary of the data for a record of the table.

    Returns
    -------
    List
        example:
        [
            "/runs",
            "/flows/ExampleFlow/runs",
            "/flows/ExampleFlow/runs/1234"
        ]
    """
    resource_paths = {
        FLOW_TABLE_NAME: [
            "/flows",
            "/flows/{flow_id}"
        ],
        RUN_TABLE_NAME: [
            "/runs",
            "/flows/{flow_id}/runs",
            "/flows/{flow_id}/runs/{run_number}"
        ],
        STEP_TABLE_NAME: [
            "/flows/{flow_id}/runs/{run_number}/steps",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}"
        ],
        TASK_TABLE_NAME: [
            "/flows/{flow_id}/runs/{run_number}/tasks",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/attempts"
        ],
        ARTIFACT_TABLE_NAME: [
            "/flows/{flow_id}/runs/{run_number}/artifacts",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/artifacts",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/artifacts",
        ],
        METADATA_TABLE_NAME: [
            "/flows/{flow_id}/runs/{run_number}/metadata",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/metadata"
        ]
    }
    if table_name in resource_paths:
        return [path.format(**data) for path in resource_paths[table_name]]
    return []


async def _broadcast(event_emitter, operation: str, table, data: Dict, filter_dict={}):
    _resources = resource_list(table.table_name, data)
    event_emitter.emit('notify', operation, _resources, data, table.table_name, filter_dict)

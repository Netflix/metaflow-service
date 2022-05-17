from services.data import TaskRow
from services.data.postgres_async_db import AsyncPostgresDB
from services.data.tagging_utils import apply_run_tags_to_db_response
from services.utils import has_heartbeat_capable_version_tag, read_body
from services.metadata_service.api.utils import format_response, \
    handle_exceptions
import json
from aiohttp import web
import asyncio


class TaskApi(object):
    _task_table = None
    lock = asyncio.Lock()

    def __init__(self, app):
        app.router.add_route(
            "GET",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks",
            self.get_tasks,
        )
        app.router.add_route(
            "GET",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}",
            self.get_task,
        )
        app.router.add_route(
            "POST",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/task",
            self.create_task,
        )
        app.router.add_route("POST",
                             "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/heartbeat",
                             self.tasks_heartbeat)
        self._async_table = AsyncPostgresDB.get_instance().task_table_postgres
        self._async_run_table = AsyncPostgresDB.get_instance().run_table_postgres
        self._db = AsyncPostgresDB.get_instance()

    @format_response
    @handle_exceptions
    async def get_tasks(self, request):
        """
        ---
        description: get all tasks associated with the specified step.
        tags:
        - Tasks
        parameters:
        - name: "flow_id"
          in: "path"
          description: "flow_id"
          required: true
          type: "string"
        - name: "run_number"
          in: "path"
          description: "run_number"
          required: true
          type: "string"
        - name: "step_name"
          in: "path"
          description: "step_name"
          required: true
          type: "string"
        produces:
        - text/plain
        responses:
            "200":
                description: successful operation. Return tasks
            "405":
                description: invalid HTTP Method
        """
        flow_id = request.match_info.get("flow_id")
        run_number = request.match_info.get("run_number")
        step_name = request.match_info.get("step_name")

        db_response = await self._async_table.get_tasks(flow_id, run_number, step_name)
        db_response = await apply_run_tags_to_db_response(flow_id, run_number, self._async_run_table, db_response)
        return db_response

    @format_response
    @handle_exceptions
    async def get_task(self, request):
        """
        ---
        description: get all artifacts associated with the specified task.
        tags:
        - Tasks
        parameters:
        - name: "flow_id"
          in: "path"
          description: "flow_id"
          required: true
          type: "string"
        - name: "run_number"
          in: "path"
          description: "run_number"
          required: true
          type: "string"
        - name: "step_name"
          in: "path"
          description: "step_name"
          required: true
          type: "string"
        - name: "task_id"
          in: "path"
          description: "task_id"
          required: true
          type: "integer"
        produces:
        - text/plain
        responses:
            "200":
                description: successful operation. Return task
            "405":
                description: invalid HTTP Method
        """
        flow_id = request.match_info.get("flow_id")
        run_number = request.match_info.get("run_number")
        step_name = request.match_info.get("step_name")
        task_id = request.match_info.get("task_id")
        db_response = await self._async_table.get_task(
            flow_id, run_number, step_name, task_id
        )
        db_response = await apply_run_tags_to_db_response(flow_id, run_number, self._async_run_table, db_response)
        return db_response

    @format_response
    @handle_exceptions
    async def create_task(self, request):
        """
        ---
        description: This end-point allow to test that service is up.
                    "tags" and "system_tags" values will be persisted to DB, but will not be
                    returned by read endpoints - the related run's "tags" and "system_tags" will
                    be returned instead.
        tags:
        - Tasks
        parameters:
        - name: "flow_id"
          in: "path"
          description: "flow_id"
          required: true
          type: "string"
        - name: "run_number"
          in: "path"
          description: "run_number"
          required: true
          type: "string"
        - name: "step_name"
          in: "path"
          description: "step_name"
          required: true
          type: "string"
        - name: "body"
          in: "body"
          description: "body"
          required: true
          schema:
            type: object
            properties:
                user_name:
                    type: string
                tags:
                    type: object
                system_tags:
                    type: object
                task_id:
                    type: string
        produces:
        - 'text/plain'
        responses:
            "202":
                description: successful operation. Return newly registered task
            "400":
                description: invalid HTTP Request
            "405":
                description: invalid HTTP Method
        """
        flow_id = request.match_info.get("flow_id")
        run_number = request.match_info.get("run_number")
        step_name = request.match_info.get("step_name")
        body = await read_body(request.content)

        user = body.get("user_name")
        tags = body.get("tags")
        system_tags = body.get("system_tags")
        task_name = body.get("task_id")

        client_supports_heartbeats = has_heartbeat_capable_version_tag(system_tags)

        if task_name and task_name.isnumeric():
            return web.Response(status=400, body=json.dumps(
                {"message": "provided task_name may not be a numeric"}))

        run_number, run_id = await self._db.get_run_ids(flow_id, run_number)

        task = TaskRow(
            flow_id=flow_id,
            run_number=run_number,
            run_id=run_id,
            step_name=step_name,
            task_name=task_name,
            user_name=user,
            tags=tags,
            system_tags=system_tags,
        )
        db_response = await self._async_table.add_task(task, fill_heartbeat=client_supports_heartbeats)
        db_response = await apply_run_tags_to_db_response(flow_id, run_number, self._async_run_table, db_response)
        if client_supports_heartbeats and db_response.response_code == 200:
            await self._async_run_table.update_heartbeat(flow_id, run_number)
        return db_response

    @format_response
    @handle_exceptions
    async def tasks_heartbeat(self, request):
        """
        ---
        description: update hb
        tags:
        - Tasks
        parameters:
        - name: "flow_id"
          in: "path"
          description: "flow_id"
          required: true
          type: "string"
        - name: "run_number"
          in: "path"
          description: "run_number"
          required: true
          type: "string"
        - name: "step_name"
          in: "path"
          description: "step_name"
          required: true
          type: "string"
        - name: "task_id"
          in: "path"
          description: "task_id"
          required: true
          type: "string"
        - name: "body"
          in: "body"
          description: "body"
          required: true
          schema:
            type: object
        produces:
        - 'text/plain'
        responses:
            "200":
                description: successful operation. Return newly registered run
            "400":
                description: invalid HTTP Request
            "405":
                description: invalid HTTP Method
        """
        flow_name = request.match_info.get("flow_id")
        run_number = request.match_info.get("run_number")
        step_name = request.match_info.get("step_name")
        task_id = request.match_info.get("task_id")
        await self._async_run_table.update_heartbeat(flow_name, run_number)
        return await self._async_table.update_heartbeat(flow_name,
                                                        run_number, step_name,
                                                        task_id)

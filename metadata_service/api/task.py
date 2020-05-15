from aiohttp import web
from ..data.models import TaskRow
from ..data.postgres_async_db import AsyncPostgresDB
import json
from .utils import read_body
import asyncio


class TaskApi(object):
    _task_table = None
    lock = asyncio.Lock()

    def __init__(self, app):
        app.router.add_route(
            "GET",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/" "tasks",
            self.get_tasks,
        )
        app.router.add_route(
            "GET",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/" "tasks/{task_id}",
            self.get_task,
        )
        app.router.add_route(
            "POST",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/" "task",
            self.create_task,
        )
        self._async_table = AsyncPostgresDB.get_instance().task_table_postgres
        self._db = AsyncPostgresDB.get_instance()

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
        flow_name = request.match_info.get("flow_id")
        run_number = request.match_info.get("run_number")
        step_name = request.match_info.get("step_name")

        tasks = await self._async_table.get_tasks(flow_name, run_number, step_name)
        return web.Response(status=tasks.response_code, body=json.dumps(tasks.body))

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
        flow_name = request.match_info.get("flow_id")
        run_number = request.match_info.get("run_number")
        step_name = request.match_info.get("step_name")
        task_id = request.match_info.get("task_id")

        task = await self._async_table.get_task(
            flow_name, run_number, step_name, task_id
        )
        return web.Response(status=task.response_code, body=json.dumps(task.body))

    async def create_task(self, request):
        """
        ---
        description: This end-point allow to test that service is up.
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
                task_name:
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
        task_name = body.get("task_name")

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
        task_response = await self._async_table.add_task(task)
        return web.Response(
            status=task_response.response_code, body=json.dumps(task_response.body)
        )

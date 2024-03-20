from aiohttp import web
from services.data.postgres_async_db import AsyncPostgresDB
from services.data.db_utils import (
    filter_artifacts_for_latest_attempt,
    filter_artifacts_by_attempt_id_for_tasks,
)
from services.data.tagging_utils import apply_run_tags_to_db_response
from services.utils import read_body
from services.metadata_service.api.utils import (
    format_response,
    handle_exceptions,
    http_500,
)
import json


class ArtificatsApi(object):

    _artifact_table = None

    def __init__(self, app):
        app.router.add_route(
            "GET",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/"
            "tasks/{task_id}/artifacts/{artifact_name}",
            self.get_artifact,
        )
        app.router.add_route(
            "GET",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/"
            "tasks/{task_id}/artifacts/{artifact_name}/attempt/{attempt_id}",
            self.get_artifact_with_attempt,
        )
        app.router.add_route(
            "GET",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/"
            "tasks/{task_id}/artifacts",
            self.get_artifacts_by_task,
        )
        app.router.add_route(
            "GET",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/"
            "tasks/{task_id}/attempt/{attempt_id}/artifacts",
            self.get_artifacts_by_task_attempt,
        )
        app.router.add_route(
            "GET",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/artifacts",
            self.get_artifacts_by_step,
        )
        app.router.add_route(
            "GET",
            "/flows/{flow_id}/runs/{run_number}/artifacts",
            self.get_artifacts_by_run,
        )
        app.router.add_route(
            "POST",
            "/flows/{flow_id}/runs/{run_number}/steps/"
            "{step_name}/tasks/{task_id}/artifact",
            self.create_artifacts,
        )
        self._async_table = AsyncPostgresDB.get_instance().artifact_table_postgres
        self._async_run_table = AsyncPostgresDB.get_instance().run_table_postgres
        self._db = AsyncPostgresDB.get_instance()

    @format_response
    @handle_exceptions
    async def get_artifact(self, request):
        """
        ---
        description: get all artifacts associated with the specified task.
        tags:
        - Artifacts
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
        - name: "artifact_name"
          in: "path"
          description: "artifact_name"
          required: true
          type: "string"
        produces:
        - text/plain
        responses:
            "200":
                description: successful operation
            "405":
                description: invalid HTTP Method
        """
        flow_id = request.match_info.get("flow_id")
        run_number = request.match_info.get("run_number")
        step_name = request.match_info.get("step_name")
        task_id = request.match_info.get("task_id")
        artifact_name = request.match_info.get("artifact_name")

        db_response = await self._async_table.get_artifact(
            flow_id, run_number, step_name, task_id, artifact_name
        )
        db_response = await apply_run_tags_to_db_response(flow_id, run_number, self._async_run_table, db_response)
        return db_response

    @format_response
    @handle_exceptions
    async def get_artifact_with_attempt(self, request):
        """
        ---
        description: get all artifacts associated with the specified task.
        tags:
        - Artifacts
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
        - name: "artifact_name"
          in: "path"
          description: "artifact_name"
          required: true
          type: "string"
        - name: "attempt_id"
          in: "path"
          description: "attempt_id"
          required: true
          type: "integer"
        produces:
        - text/plain
        responses:
            "200":
                description: successful operation
            "405":
                description: invalid HTTP Method
        """
        flow_id = request.match_info.get("flow_id")
        run_number = request.match_info.get("run_number")
        step_name = request.match_info.get("step_name")
        task_id = request.match_info.get("task_id")
        artifact_name = request.match_info.get("artifact_name")
        attempt_id = request.match_info.get("attempt_id")

        db_response = await self._async_table.get_artifact_by_attempt(
            flow_id, run_number, step_name, task_id, artifact_name, attempt_id
        )
        db_response = await apply_run_tags_to_db_response(flow_id, run_number, self._async_run_table, db_response)
        return db_response

    async def get_artifacts_by_task(self, request):
        """
        ---
        description: get all artifacts associated with the specified task.
        tags:
        - Artifacts
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
        produces:
        - text/plain
        responses:
            "200":
                description: successful operation
            "405":
                description: invalid HTTP Method
        """
        flow_id = request.match_info.get("flow_id")
        run_number = request.match_info.get("run_number")
        step_name = request.match_info.get("step_name")
        task_id = request.match_info.get("task_id")

        db_response = await self._async_table.get_artifact_in_task(
            flow_id, run_number, step_name, task_id
        )
        if db_response.response_code == 200:
            db_response = await apply_run_tags_to_db_response(flow_id, run_number, self._async_run_table, db_response)
            filtered_body = filter_artifacts_for_latest_attempt(db_response.body)
            return web.Response(
                status=db_response.response_code, body=json.dumps(filtered_body)
            )
        else:
            return web.Response(
                status=db_response.response_code,
                body=json.dumps(http_500(db_response.body)),
            )

    async def get_artifacts_by_task_attempt(self, request):
        """
        ---
        description: get all artifacts associated with the specified task.
        tags:
        - Artifacts
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
        - name: "attempt_id"
          in: "path"
          description: "attempt_id"
          required: true
          type: "integer"
        produces:
        - text/plain
        responses:
            "200":
                description: successful operation
            "405":
                description: invalid HTTP Method
        """
        flow_id = request.match_info.get("flow_id")
        run_number = request.match_info.get("run_number")
        step_name = request.match_info.get("step_name")
        task_id = request.match_info.get("task_id")
        attempt_id = request.match_info.get("attempt_id")

        db_response = await self._async_table.get_artifact_in_task(
            flow_id, run_number, step_name, task_id
        )
        if db_response.response_code == 200:
            db_response = await apply_run_tags_to_db_response(flow_id, run_number, self._async_run_table, db_response)
            if db_response.body:
                attempt_for_task = {db_response.body[0]["task_id"]: int(attempt_id)}
            else:
                # Doesn't matter
                attempt_for_task = {}
            filtered_body = filter_artifacts_by_attempt_id_for_tasks(
                db_response.body, attempt_for_task
            )
            return web.Response(
                status=db_response.response_code, body=json.dumps(filtered_body)
            )
        else:
            return web.Response(
                status=db_response.response_code,
                body=json.dumps(http_500(db_response.body)),
            )

    async def get_artifacts_by_step(self, request):
        """
        ---
        description: get all artifacts associated with the specified task.
        tags:
        - Artifacts
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
                description: successful operation
            "405":
                description: invalid HTTP Method
        """
        flow_id = request.match_info.get("flow_id")
        run_number = request.match_info.get("run_number")
        step_name = request.match_info.get("step_name")

        db_response = await self._async_table.get_artifact_in_steps(
            flow_id, run_number, step_name
        )
        if db_response.response_code == 200:
            db_response = await apply_run_tags_to_db_response(flow_id, run_number, self._async_run_table, db_response)
            filtered_body = filter_artifacts_for_latest_attempt(db_response.body)
            return web.Response(
                status=db_response.response_code, body=json.dumps(filtered_body)
            )
        else:
            return web.Response(
                status=db_response.response_code,
                body=json.dumps(http_500(db_response.body)),
            )

    async def get_artifacts_by_run(self, request):
        """
        ---
        description: get all artifacts associated with the specified task.
        tags:
        - Artifacts
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
        produces:
        - text/plain
        responses:
            "200":
                description: successful operation
            "405":
                description: invalid HTTP Method
        """
        flow_id = request.match_info.get("flow_id")
        run_number = request.match_info.get("run_number")

        db_response = await self._async_table.get_artifacts_in_runs(flow_id, run_number)
        if db_response.response_code == 200:
            db_response = await apply_run_tags_to_db_response(flow_id, run_number, self._async_run_table, db_response)
            filtered_body = filter_artifacts_for_latest_attempt(db_response.body)
            return web.Response(
                status=db_response.response_code, body=json.dumps(filtered_body)
            )
        else:
            return web.Response(
                status=db_response.response_code,
                body=json.dumps(http_500(db_response.body)),
            )

    async def create_artifacts(self, request):
        """
        ---
        description: This end-point allow to test that service is up.
                    "tags" and "system_tags" values will be persisted to DB, but will not be
                    returned by read endpoints - the related run's "tags" and "system_tags" will
                    be returned instead.
        tags:
        - Artifacts
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
            type: array
            items:
                type: object
                properties:
                    name:
                        type: string
                    location:
                        type: string
                    ds_type:
                        type: string
                    content_type:
                        type: string
                    attempt_id:
                        type: integer
                    user_name:
                        type: string
                    tags:
                        type: object
                    system_tags:
                        type: object
        produces:
        - 'text/plain'
        responses:
            "202":
                description: successful operation.
            "405":
                description: invalid HTTP Method
        """

        # {
        # 	"name": "test",
        # 	"location": "test",
        # 	"ds_type": "content",
        # 	"sha": "test",
        # 	"type": "content",
        # 	"content_type": "content",
        # 	"attempt_id": 0,
        # 	"user_name": "fhamad",
        # 	"tags": {
        # 		"user": "fhamad"
        # 	},
        # 	"system_tags": {
        # 		"user": "fhamad"
        # 	}
        # }

        flow_name = request.match_info.get("flow_id")
        run_number = request.match_info.get("run_number")
        step_name = request.match_info.get("step_name")
        task_id = request.match_info.get("task_id")
        body = await read_body(request.content)
        count = 0

        try:
            run_number, run_id = await self._db.get_run_ids(flow_name, run_number)
            task_id, task_name = await self._db.get_task_ids(
                flow_name, run_number, step_name, task_id
            )
        except Exception:
            return web.Response(
                status=400,
                body=json.dumps(
                    {"message": "need to register run_id and task_id first"}
                ),
            )

        # todo change to bulk insert
        for artifact in body:
            values = {
                "flow_id": flow_name,
                "run_number": run_number,
                "run_id": run_id,
                "step_name": step_name,
                "task_id": task_id,
                "task_name": task_name,
                "name": artifact.get("name", " "),
                "location": artifact.get("location", " "),
                "ds_type": artifact.get("ds_type", " "),
                "sha": artifact.get("sha", " "),
                "type": artifact.get("type", " "),
                "content_type": artifact.get("content_type", " "),
                "attempt_id": artifact.get("attempt_id", 0),
                "user_name": artifact.get("user_name", " "),
                "tags": artifact.get("tags"),
                "system_tags": artifact.get("system_tags"),
            }
            artifact_response = await self._async_table.add_artifact(**values)
            if artifact_response.response_code == 200:
                count = count + 1

        result = {"artifacts_created": count}

        return web.Response(body=json.dumps(result))

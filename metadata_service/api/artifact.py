from aiohttp import web
from ..data.postgres_async_db import AsyncPostgresDB
from .utils import read_body, format_response, handle_exceptions
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
            "tasks/{task_id}/artifacts",
            self.get_artifacts_by_task,
        )
        app.router.add_route(
            "GET",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/" "artifacts",
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
        flow_name = request.match_info.get("flow_id")
        run_number = request.match_info.get("run_number")
        step_name = request.match_info.get("step_name")
        task_id = request.match_info.get("task_id")
        artifact_name = request.match_info.get("artifact_name")

        return await self._async_table.get_artifact(
            flow_name, run_number, step_name, task_id, artifact_name
        )

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
        flow_name = request.match_info.get("flow_id")
        run_number = request.match_info.get("run_number")
        step_name = request.match_info.get("step_name")
        task_id = request.match_info.get("task_id")

        artifacts = await self._async_table.get_artifact_in_task(
            flow_name, run_number, step_name, task_id
        )

        filtered_body = ArtificatsApi._filter_artifacts_by_attempt_id(
            artifacts.body)
        return web.Response(
            status=artifacts.response_code, body=json.dumps(filtered_body)
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
        flow_name = request.match_info.get("flow_id")
        run_number = request.match_info.get("run_number")
        step_name = request.match_info.get("step_name")

        artifacts = await self._async_table.get_artifact_in_steps(
            flow_name, run_number, step_name
        )

        filtered_body = ArtificatsApi._filter_artifacts_by_attempt_id(artifacts.body)
        return web.Response(
            status=artifacts.response_code, body=json.dumps(filtered_body)
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
        flow_name = request.match_info.get("flow_id")
        run_number = request.match_info.get("run_number")

        artifacts = await self._async_table.get_artifacts_in_runs(flow_name, run_number)
        filtered_body = ArtificatsApi._filter_artifacts_by_attempt_id(artifacts.body)
        return web.Response(
            status=artifacts.response_code, body=json.dumps(filtered_body)
        )

    async def create_artifacts(self, request):
        """
        ---
        description: This end-point allow to test that service is up.
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

        run_number, run_id = await self._db.get_run_ids(flow_name, run_number)
        task_id, task_name = await self._db.get_task_ids(flow_name, run_number,
                                                         step_name, task_id)
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

    @staticmethod
    def _get_latest_attempt_id(artifacts):
        attempt_id = 0
        for artifact in artifacts:
            if artifact['attempt_id'] > attempt_id:
                attempt_id = artifact['attempt_id']
        return attempt_id

    @staticmethod
    def _filter_artifacts_by_attempt_id(artifacts):
        attempt_id = ArtificatsApi._get_latest_attempt_id(artifacts)
        result = []
        for artifact in artifacts:
            if artifact['attempt_id'] == attempt_id:
                result.append(artifact)

        return result


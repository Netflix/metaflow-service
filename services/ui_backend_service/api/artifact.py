import json
from aiohttp import web
from services.data.postgres_async_db import AsyncPostgresDB
from services.data.db_utils import DBResponse, _filter_artifacts_by_attempt_id_for_tasks, translate_run_key, translate_task_key
from services.utils import handle_exceptions
from .utils import find_records


class ArtificatsApi(object):
    def __init__(self, app):
        app.router.add_route(
            "GET",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/artifacts",
            self.get_artifacts_by_task,
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
        self._async_table = AsyncPostgresDB.get_instance().artifact_table_postgres

    @handle_exceptions
    async def get_artifacts_by_task(self, request):
        """
        ---
        description: Get all artifacts of specified task
        tags:
        - Artifact
        parameters:
          - $ref: '#/definitions/Params/Path/flow_id'
          - $ref: '#/definitions/Params/Path/run_number'
          - $ref: '#/definitions/Params/Path/step_name'
          - $ref: '#/definitions/Params/Path/task_id'
          - $ref: '#/definitions/Params/Builtin/_page'
          - $ref: '#/definitions/Params/Builtin/_limit'
          - $ref: '#/definitions/Params/Builtin/_order'
          - $ref: '#/definitions/Params/Builtin/_tags'
          - $ref: '#/definitions/Params/Builtin/_group'
          - $ref: '#/definitions/Params/Custom/flow_id'
          - $ref: '#/definitions/Params/Custom/run_number'
          - $ref: '#/definitions/Params/Custom/step_name'
          - $ref: '#/definitions/Params/Custom/task_id'
          - $ref: '#/definitions/Params/Custom/name'
          - $ref: '#/definitions/Params/Custom/type'
          - $ref: '#/definitions/Params/Custom/ds_type'
          - $ref: '#/definitions/Params/Custom/attempt_id'
          - $ref: '#/definitions/Params/Custom/user_name'
          - $ref: '#/definitions/Params/Custom/ts_epoch'
        produces:
        - application/json
        responses:
            "200":
                description: Returns all artifacts of specified task
                schema:
                  $ref: '#/definitions/ResponsesArtifactList'
            "405":
                description: invalid HTTP Method
                schema:
                  $ref: '#/definitions/ResponsesError405'
        """

        flow_name = request.match_info.get("flow_id")
        run_id_key, run_id_value = translate_run_key(
            request.match_info.get("run_number"))
        step_name = request.match_info.get("step_name")
        task_id_key, task_id_value = translate_task_key(
            request.match_info.get("task_id"))

        return await find_records(request,
                                  self._async_table,
                                  initial_conditions=[
                                      "flow_id = %s",
                                      "{run_id_key} = %s".format(
                                          run_id_key=run_id_key),
                                      "step_name = %s",
                                      "{task_id_key} = %s".format(
                                          task_id_key=task_id_key)],
                                  initial_values=[
                                      flow_name, run_id_value, step_name, task_id_value],
                                  allowed_order=self._async_table.keys,
                                  allowed_group=self._async_table.keys,
                                  allowed_filters=self._async_table.keys,
                                  postprocess=ArtificatsApi._postprocess
                                  )

    @handle_exceptions
    async def get_artifacts_by_step(self, request):
        """
        ---
        description: Get all artifacts of specified step
        tags:
        - Artifact
        parameters:
          - $ref: '#/definitions/Params/Path/flow_id'
          - $ref: '#/definitions/Params/Path/run_number'
          - $ref: '#/definitions/Params/Path/step_name'
          - $ref: '#/definitions/Params/Builtin/_page'
          - $ref: '#/definitions/Params/Builtin/_limit'
          - $ref: '#/definitions/Params/Builtin/_order'
          - $ref: '#/definitions/Params/Builtin/_tags'
          - $ref: '#/definitions/Params/Builtin/_group'
          - $ref: '#/definitions/Params/Custom/flow_id'
          - $ref: '#/definitions/Params/Custom/run_number'
          - $ref: '#/definitions/Params/Custom/step_name'
          - $ref: '#/definitions/Params/Custom/task_id'
          - $ref: '#/definitions/Params/Custom/name'
          - $ref: '#/definitions/Params/Custom/type'
          - $ref: '#/definitions/Params/Custom/ds_type'
          - $ref: '#/definitions/Params/Custom/attempt_id'
          - $ref: '#/definitions/Params/Custom/user_name'
          - $ref: '#/definitions/Params/Custom/ts_epoch'
        produces:
        - application/json
        responses:
            "200":
                description: Returns all artifacts of specified step
                schema:
                  $ref: '#/definitions/ResponsesArtifactList'
            "405":
                description: invalid HTTP Method
                schema:
                  $ref: '#/definitions/ResponsesError405'
        """

        flow_name = request.match_info.get("flow_id")
        run_id_key, run_id_value = translate_run_key(
            request.match_info.get("run_number"))
        step_name = request.match_info.get("step_name")

        return await find_records(request,
                                  self._async_table,
                                  initial_conditions=[
                                      "flow_id = %s",
                                      "{run_id_key} = %s".format(
                                          run_id_key=run_id_key),
                                      "step_name = %s"],
                                  initial_values=[
                                      flow_name, run_id_value, step_name],
                                  allowed_order=self._async_table.keys,
                                  allowed_group=self._async_table.keys,
                                  allowed_filters=self._async_table.keys,
                                  postprocess=ArtificatsApi._postprocess
                                  )

    @handle_exceptions
    async def get_artifacts_by_run(self, request):
        """
        ---
        description: Get all artifacts of specified run
        tags:
        - Artifact
        parameters:
          - $ref: '#/definitions/Params/Path/flow_id'
          - $ref: '#/definitions/Params/Path/run_number'
          - $ref: '#/definitions/Params/Builtin/_page'
          - $ref: '#/definitions/Params/Builtin/_limit'
          - $ref: '#/definitions/Params/Builtin/_order'
          - $ref: '#/definitions/Params/Builtin/_tags'
          - $ref: '#/definitions/Params/Builtin/_group'
          - $ref: '#/definitions/Params/Custom/flow_id'
          - $ref: '#/definitions/Params/Custom/run_number'
          - $ref: '#/definitions/Params/Custom/step_name'
          - $ref: '#/definitions/Params/Custom/task_id'
          - $ref: '#/definitions/Params/Custom/name'
          - $ref: '#/definitions/Params/Custom/type'
          - $ref: '#/definitions/Params/Custom/ds_type'
          - $ref: '#/definitions/Params/Custom/attempt_id'
          - $ref: '#/definitions/Params/Custom/user_name'
          - $ref: '#/definitions/Params/Custom/ts_epoch'
        produces:
        - application/json
        responses:
            "200":
                description: Returns all artifacts of specified run
                schema:
                  $ref: '#/definitions/ResponsesArtifactList'
            "405":
                description: invalid HTTP Method
                schema:
                  $ref: '#/definitions/ResponsesError405'
        """

        flow_name = request.match_info.get("flow_id")
        run_id_key, run_id_value = translate_run_key(
            request.match_info.get("run_number"))

        return await find_records(request,
                                  self._async_table,
                                  initial_conditions=[
                                      "flow_id = %s",
                                      "{run_id_key} = %s".format(
                                          run_id_key=run_id_key)],
                                  initial_values=[
                                      flow_name, run_id_value],
                                  allowed_order=self._async_table.keys,
                                  allowed_group=self._async_table.keys,
                                  allowed_filters=self._async_table.keys,
                                  postprocess=ArtificatsApi._postprocess
                                  )

    @staticmethod
    def _postprocess(response: DBResponse):
        if response.response_code != 200 or not response.body:
            return response
        return DBResponse(response_code=response.response_code,
                          body=_filter_artifacts_by_attempt_id_for_tasks(response.body))

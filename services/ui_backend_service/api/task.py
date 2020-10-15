from services.data.postgres_async_db import AsyncPostgresDB
from services.data.db_utils import DBResponse, translate_run_key, translate_task_key
from services.utils import handle_exceptions
from .utils import find_records
from .data_refiner import Refinery


class TaskApi(object):
    def __init__(self, app):
        app.router.add_route(
            "GET",
            "/flows/{flow_id}/runs/{run_number}/tasks",
            self.get_run_tasks,
        )
        app.router.add_route(
            "GET",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks",
            self.get_step_tasks,
        )
        app.router.add_route(
            "GET",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}",
            self.get_task,
        )
        app.router.add_route(
            "GET",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/attempts",
            self.get_task_attempts,
        )
        self._async_table = AsyncPostgresDB.get_instance().task_table_postgres
        self.refiner = Refinery(field_names=["task_ok"])

    @handle_exceptions
    async def get_run_tasks(self, request):
        """
        ---
        description: Get all tasks of specified run
        tags:
        - Task
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
          - $ref: '#/definitions/Params/Custom/user_name'
          - $ref: '#/definitions/Params/Custom/ts_epoch'
          - $ref: '#/definitions/Params/Custom/finished_at'
          - $ref: '#/definitions/Params/Custom/duration'
        produces:
        - application/json
        responses:
            "200":
                description: Returns all tasks of specified run
                schema:
                  $ref: '#/definitions/ResponsesTaskList'
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
                                  initial_order=["attempt_id DESC"],
                                  allowed_order=self._async_table.keys +
                                  ["finished_at", "duration", "attempt_id"],
                                  allowed_group=self._async_table.keys,
                                  allowed_filters=self._async_table.keys +
                                  ["finished_at", "duration", "attempt_id"],
                                  enable_joins=True,
                                  postprocess=self._postprocess
                                  )

    @handle_exceptions
    async def get_step_tasks(self, request):
        """
        ---
        description: Get all tasks of specified step
        tags:
        - Task
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
          - $ref: '#/definitions/Params/Custom/user_name'
          - $ref: '#/definitions/Params/Custom/ts_epoch'
          - $ref: '#/definitions/Params/Custom/finished_at'
          - $ref: '#/definitions/Params/Custom/duration'
        produces:
        - application/json
        responses:
            "200":
                description: Returns all tasks of specified step
                schema:
                  $ref: '#/definitions/ResponsesTaskList'
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
                                  initial_order=["attempt_id DESC"],
                                  allowed_order=self._async_table.keys +
                                  ["finished_at", "duration", "attempt_id"],
                                  allowed_group=self._async_table.keys,
                                  allowed_filters=self._async_table.keys +
                                  ["finished_at", "duration", "attempt_id"],
                                  enable_joins=True,
                                  postprocess=self._postprocess
                                  )

    @handle_exceptions
    async def get_task(self, request):
        """
        ---
        description: Get one task
        tags:
        - Task
        parameters:
          - $ref: '#/definitions/Params/Path/flow_id'
          - $ref: '#/definitions/Params/Path/run_number'
          - $ref: '#/definitions/Params/Path/step_name'
          - $ref: '#/definitions/Params/Path/task_id'
        produces:
        - application/json
        responses:
            "200":
                description: Returns one task
                schema:
                  $ref: '#/definitions/ResponsesTask'
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
                                  fetch_single=True,
                                  initial_conditions=[
                                      "flow_id = %s",
                                      "{run_id_key} = %s".format(
                                          run_id_key=run_id_key),
                                      "step_name = %s",
                                      "{task_id_key} = %s".format(
                                          task_id_key=task_id_key)],
                                  initial_values=[
                                      flow_name, run_id_value, step_name, task_id_value],
                                  initial_order=["attempt_id DESC"],
                                  enable_joins=True,
                                  postprocess=self._postprocess)

    @handle_exceptions
    async def get_task_attempts(self, request):
        """
        ---
        description: Get all task attempts of specified step
        tags:
        - Task
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
          - $ref: '#/definitions/Params/Custom/user_name'
          - $ref: '#/definitions/Params/Custom/ts_epoch'
          - $ref: '#/definitions/Params/Custom/finished_at'
          - $ref: '#/definitions/Params/Custom/duration'
        produces:
        - application/json
        responses:
            "200":
                description: Returns all task attempts of specified step
                schema:
                  $ref: '#/definitions/ResponsesTaskList'
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
                                  initial_order=["attempt_id DESC"],
                                  allowed_order=self._async_table.keys +
                                  ["finished_at", "duration", "attempt_id"],
                                  allowed_group=self._async_table.keys,
                                  allowed_filters=self._async_table.keys +
                                  ["finished_at", "duration", "attempt_id"],
                                  enable_joins=True,
                                  postprocess=self._postprocess
                                  )

    async def _postprocess(self, response: DBResponse):
        """Calls the refiner postprocessing to fetch S3 values for content.
        Cleans up returned fields, for example by combining 'task_ok' boolean into the 'status'
        """
        refined_response = await self.refiner.postprocess(response)
        if response.response_code != 200 or not response.body:
            return response

        def _cleanup(item):
            # TODO: test if 'running' statuses go through correctly.
            item['status'] = 'failed' if item['status'] == 'completed' and item['task_ok'] is False else item['status']
            item.pop('task_ok')
            return item

        if isinstance(refined_response.body, list):
            body = [_cleanup(task) for task in refined_response.body ]
        else:
            body = _cleanup(refined_response.body)
        
        return DBResponse(response_code=refined_response.response_code, body=body)
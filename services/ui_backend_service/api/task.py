from services.data.db_utils import translate_run_key, translate_task_key
from services.utils import handle_exceptions
from .utils import find_records
from ..data.refiner import TaskRefiner


class TaskApi(object):
    def __init__(self, app, db, cache=None):
        self.db = db
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
        self._async_table = self.db.task_table_postgres
        self.refiner = TaskRefiner(cache=cache.artifact_cache) if cache else None

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
          - $ref: '#/definitions/Params/Custom/postprocess'
          - $ref: '#/definitions/Params/Custom/invalidate'
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
                                  allowed_order=self._async_table.keys + ["finished_at", "duration", "attempt_id"],
                                  allowed_group=self._async_table.keys,
                                  allowed_filters=self._async_table.keys + ["finished_at", "duration", "attempt_id"],
                                  enable_joins=True,
                                  postprocess=self.get_postprocessor(request)
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
          - $ref: '#/definitions/Params/Custom/postprocess'
          - $ref: '#/definitions/Params/Custom/invalidate'
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
                                  allowed_order=self._async_table.keys + ["finished_at", "duration", "attempt_id"],
                                  allowed_group=self._async_table.keys,
                                  allowed_filters=self._async_table.keys + ["finished_at", "duration", "attempt_id"],
                                  enable_joins=True,
                                  postprocess=self.get_postprocessor(request)
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
          - $ref: '#/definitions/Params/Custom/postprocess'
          - $ref: '#/definitions/Params/Custom/invalidate'
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
                                  postprocess=self.get_postprocessor(request)
                                  )

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
          - $ref: '#/definitions/Params/Custom/postprocess'
          - $ref: '#/definitions/Params/Custom/invalidate'
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
                                  allowed_order=self._async_table.keys + ["finished_at", "duration", "attempt_id"],
                                  allowed_group=self._async_table.keys,
                                  allowed_filters=self._async_table.keys + ["finished_at", "duration", "attempt_id"],
                                  enable_joins=True,
                                  postprocess=self.get_postprocessor(request)
                                  )

    def get_postprocessor(self, request):
        "pass query param &postprocess=true to enable postprocessing of S3 content. Otherwise returns None as postprocessor"
        if request.query.get("postprocess", False) in ["true", "True", "1"]:
            return self.refiner.postprocess
        else:
            return None

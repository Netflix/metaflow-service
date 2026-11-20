from services.data.db_utils import translate_run_key, translate_task_key
from services.utils import handle_exceptions
from .utils import find_records, postprocess_chain, apply_run_tags_postprocess
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
        self._async_run_table = self.db.run_table_postgres
        self.refiner = TaskRefiner(cache=cache.artifact_cache) if cache else None

    @handle_exceptions
    async def get_run_tasks(self, request):
        """
        ---
        description: Get all tasks of specified run
        tags:
        - Task
        parameters:
          - $ref: '#/parameters/path_flow_id'
          - $ref: '#/parameters/path_run_number'
          - $ref: '#/parameters/builtin__page'
          - $ref: '#/parameters/builtin__limit'
          - $ref: '#/parameters/builtin__order'
          - $ref: '#/parameters/builtin__tags'
          - $ref: '#/parameters/builtin__group'
          - $ref: '#/parameters/custom_flow_id'
          - $ref: '#/parameters/custom_run_number'
          - $ref: '#/parameters/custom_step_name'
          - $ref: '#/parameters/custom_task_id'
          - $ref: '#/parameters/custom_user_name'
          - $ref: '#/parameters/custom_ts_epoch'
          - $ref: '#/parameters/custom_finished_at'
          - $ref: '#/parameters/custom_duration'
          - $ref: '#/parameters/custom_postprocess'
          - $ref: '#/parameters/custom_invalidate'
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
        run_number = request.match_info.get("run_number")
        run_id_key, run_id_value = translate_run_key(run_number)

        return await find_records(
            request,
            self._async_table,
            initial_conditions=[
                "flow_id = %s",
                "{run_id_key} = %s".format(run_id_key=run_id_key),
            ],
            initial_values=[flow_name, run_id_value],
            allowed_order=self._async_table.keys
            + ["finished_at", "duration", "attempt_id"],
            allowed_group=self._async_table.keys,
            allowed_filters=self._async_table.keys
            + ["finished_at", "duration", "attempt_id"],
            enable_joins=True,
            postprocess=postprocess_chain(
                [
                    apply_run_tags_postprocess(
                        flow_name, run_number, self._async_run_table
                    ),
                    self.get_postprocessor(request),
                ]
            ),
        )

    @handle_exceptions
    async def get_step_tasks(self, request):
        """
        ---
        description: Get all tasks of specified step
        tags:
        - Task
        parameters:
          - $ref: '#/parameters/path_flow_id'
          - $ref: '#/parameters/path_run_number'
          - $ref: '#/parameters/path_step_name'
          - $ref: '#/parameters/builtin__page'
          - $ref: '#/parameters/builtin__limit'
          - $ref: '#/parameters/builtin__order'
          - $ref: '#/parameters/builtin__tags'
          - $ref: '#/parameters/builtin__group'
          - $ref: '#/parameters/custom_flow_id'
          - $ref: '#/parameters/custom_run_number'
          - $ref: '#/parameters/custom_step_name'
          - $ref: '#/parameters/custom_task_id'
          - $ref: '#/parameters/custom_user_name'
          - $ref: '#/parameters/custom_ts_epoch'
          - $ref: '#/parameters/custom_finished_at'
          - $ref: '#/parameters/custom_duration'
          - $ref: '#/parameters/custom_postprocess'
          - $ref: '#/parameters/custom_invalidate'
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
        run_number = request.match_info.get("run_number")
        run_id_key, run_id_value = translate_run_key(run_number)
        step_name = request.match_info.get("step_name")

        return await find_records(
            request,
            self._async_table,
            initial_conditions=[
                "flow_id = %s",
                "{run_id_key} = %s".format(run_id_key=run_id_key),
                "step_name = %s",
            ],
            initial_values=[flow_name, run_id_value, step_name],
            initial_order=["attempt_id DESC"],
            allowed_order=self._async_table.keys
            + ["finished_at", "duration", "attempt_id"],
            allowed_group=self._async_table.keys,
            allowed_filters=self._async_table.keys
            + ["finished_at", "duration", "attempt_id"],
            enable_joins=True,
            postprocess=postprocess_chain(
                [
                    apply_run_tags_postprocess(
                        flow_name, run_number, self._async_run_table
                    ),
                    self.get_postprocessor(request),
                ]
            ),
        )

    @handle_exceptions
    async def get_task(self, request):
        """
        ---
        description: Get one task
        tags:
        - Task
        parameters:
          - $ref: '#/parameters/path_flow_id'
          - $ref: '#/parameters/path_run_number'
          - $ref: '#/parameters/path_step_name'
          - $ref: '#/parameters/path_task_id'
          - $ref: '#/parameters/custom_postprocess'
          - $ref: '#/parameters/custom_invalidate'
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
        run_number = request.match_info.get("run_number")
        run_id_key, run_id_value = translate_run_key(run_number)
        step_name = request.match_info.get("step_name")
        task_id_key, task_id_value = translate_task_key(
            request.match_info.get("task_id")
        )

        return await find_records(
            request,
            self._async_table,
            fetch_single=True,
            initial_conditions=[
                "flow_id = %s",
                "{run_id_key} = %s".format(run_id_key=run_id_key),
                "step_name = %s",
                "{task_id_key} = %s".format(task_id_key=task_id_key),
            ],
            initial_values=[flow_name, run_id_value, step_name, task_id_value],
            initial_order=["attempt_id DESC"],
            enable_joins=True,
            postprocess=postprocess_chain(
                [
                    apply_run_tags_postprocess(
                        flow_name, run_number, self._async_run_table
                    ),
                    self.get_postprocessor(request),
                ]
            ),
        )

    @handle_exceptions
    async def get_task_attempts(self, request):
        """
        ---
        description: Get all task attempts of specified step
        tags:
        - Task
        parameters:
          - $ref: '#/parameters/path_flow_id'
          - $ref: '#/parameters/path_run_number'
          - $ref: '#/parameters/path_step_name'
          - $ref: '#/parameters/path_task_id'
          - $ref: '#/parameters/builtin__page'
          - $ref: '#/parameters/builtin__limit'
          - $ref: '#/parameters/builtin__order'
          - $ref: '#/parameters/builtin__tags'
          - $ref: '#/parameters/builtin__group'
          - $ref: '#/parameters/custom_flow_id'
          - $ref: '#/parameters/custom_run_number'
          - $ref: '#/parameters/custom_step_name'
          - $ref: '#/parameters/custom_task_id'
          - $ref: '#/parameters/custom_user_name'
          - $ref: '#/parameters/custom_ts_epoch'
          - $ref: '#/parameters/custom_finished_at'
          - $ref: '#/parameters/custom_duration'
          - $ref: '#/parameters/custom_postprocess'
          - $ref: '#/parameters/custom_invalidate'
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
        run_number = request.match_info.get("run_number")
        run_id_key, run_id_value = translate_run_key(run_number)
        step_name = request.match_info.get("step_name")
        task_id_key, task_id_value = translate_task_key(
            request.match_info.get("task_id")
        )

        return await find_records(
            request,
            self._async_table,
            initial_conditions=[
                "flow_id = %s",
                "{run_id_key} = %s".format(run_id_key=run_id_key),
                "step_name = %s",
                "{task_id_key} = %s".format(task_id_key=task_id_key),
            ],
            initial_values=[flow_name, run_id_value, step_name, task_id_value],
            initial_order=["attempt_id DESC"],
            allowed_order=self._async_table.keys
            + ["finished_at", "duration", "attempt_id"],
            allowed_group=self._async_table.keys,
            allowed_filters=self._async_table.keys
            + ["finished_at", "duration", "attempt_id"],
            enable_joins=True,
            postprocess=postprocess_chain(
                [
                    apply_run_tags_postprocess(
                        flow_name, run_number, self._async_run_table
                    ),
                    self.get_postprocessor(request),
                ]
            ),
        )

    def get_postprocessor(self, request):
        "pass query param &postprocess=true to enable postprocessing of S3 content. Otherwise returns None as postprocessor"
        if request.query.get("postprocess", False) in ["true", "True", "1"]:
            return self.refiner.postprocess
        else:
            return None

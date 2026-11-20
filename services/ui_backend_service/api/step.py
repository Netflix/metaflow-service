from services.data.db_utils import translate_run_key
from services.utils import handle_exceptions
from .utils import find_records, apply_run_tags_postprocess


class StepApi(object):
    def __init__(self, app, db):
        self.db = db
        app.router.add_route(
            "GET", "/flows/{flow_id}/runs/{run_number}/steps", self.get_steps
        )
        app.router.add_route(
            "GET", "/flows/{flow_id}/runs/{run_number}/steps/{step_name}", self.get_step
        )
        self._async_table = self.db.step_table_postgres
        self._async_run_table = self.db.run_table_postgres

    @handle_exceptions
    async def get_steps(self, request):
        """
        ---
        description: Get all steps of specified run
        tags:
        - Step
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
          - $ref: '#/parameters/custom_user_name'
          - $ref: '#/parameters/custom_ts_epoch'
        produces:
        - application/json
        responses:
            "200":
                description: Returns all steps of specified run
                schema:
                  $ref: '#/definitions/ResponsesStepList'
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
            allowed_order=self._async_table.keys,
            allowed_group=self._async_table.keys,
            allowed_filters=self._async_table.keys,
            enable_joins=True,
            postprocess=apply_run_tags_postprocess(
                flow_name, run_number, self._async_run_table
            ),
        )

    @handle_exceptions
    async def get_step(self, request):
        """
        ---
        description: Get one step
        tags:
        - Step
        parameters:
          - $ref: '#/parameters/path_flow_id'
          - $ref: '#/parameters/path_run_number'
          - $ref: '#/parameters/path_step_name'
        produces:
        - application/json
        responses:
            "200":
                description: Returns one step
                schema:
                  $ref: '#/definitions/ResponsesStep'
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
            fetch_single=True,
            initial_conditions=[
                "flow_id = %s",
                "{run_id_key} = %s".format(run_id_key=run_id_key),
                "step_name = %s",
            ],
            initial_values=[flow_name, run_id_value, step_name],
            enable_joins=True,
            postprocess=apply_run_tags_postprocess(
                flow_name, run_number, self._async_run_table
            ),
        )

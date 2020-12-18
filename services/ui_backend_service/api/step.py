from services.data.postgres_async_db import AsyncPostgresDB
from services.data.db_utils import translate_run_key
from services.utils import handle_exceptions
from .utils import find_records


class StepApi(object):
    def __init__(self, app, db=AsyncPostgresDB.get_instance()):
        self.db = db
        app.router.add_route(
            "GET", "/flows/{flow_id}/runs/{run_number}/steps", self.get_steps
        )
        app.router.add_route(
            "GET", "/flows/{flow_id}/runs/{run_number}/steps/{step_name}", self.get_step
        )
        self._async_table = self.db.step_table_postgres

    @handle_exceptions
    async def get_steps(self, request):
        """
        ---
        description: Get all steps of specified run
        tags:
        - Step
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
          - $ref: '#/definitions/Params/Custom/user_name'
          - $ref: '#/definitions/Params/Custom/ts_epoch'
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
        run_id_key, run_id_value = translate_run_key(
            request.match_info.get("run_number"))

        return await find_records(request,
                                  self._async_table,
                                  initial_conditions=[
                                      "flow_id = %s",
                                      "{run_id_key} = %s".format(run_id_key=run_id_key)],
                                  initial_values=[flow_name, run_id_value],
                                  allowed_order=self._async_table.keys,
                                  allowed_group=self._async_table.keys,
                                  allowed_filters=self._async_table.keys
                                  )

    @handle_exceptions
    async def get_step(self, request):
        """
        ---
        description: Get one step
        tags:
        - Step
        parameters:
          - $ref: '#/definitions/Params/Path/flow_id'
          - $ref: '#/definitions/Params/Path/run_number'
          - $ref: '#/definitions/Params/Path/step_name'
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
        run_id_key, run_id_value = translate_run_key(
            request.match_info.get("run_number"))
        step_name = request.match_info.get("step_name")

        return await find_records(request,
                                  self._async_table,
                                  fetch_single=True,
                                  initial_conditions=[
                                      "flow_id = %s",
                                      "{run_id_key} = %s".format(
                                          run_id_key=run_id_key),
                                      "step_name = %s"],
                                  initial_values=[
                                      flow_name, run_id_value, step_name],
                                  )

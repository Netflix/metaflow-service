from ..data.refiner.parameter_refiner import GetParametersFailed
from services.data.db_utils import DBResponse, translate_run_key
from services.utils import handle_exceptions
from .utils import find_records, web_response, format_response,\
    builtin_conditions_query, pagination_query, query_param_enabled


class RunApi(object):
    def __init__(self, app, db, cache=None):
        self.db = db
        app.router.add_route(
            "GET", "/runs", self.get_all_runs)
        app.router.add_route(
            "GET", "/flows/{flow_id}/runs", self.get_flow_runs)
        app.router.add_route(
            "GET", "/flows/{flow_id}/runs/{run_number}", self.get_run)
        app.router.add_route(
            "GET", "/flows/{flow_id}/runs/{run_number}/parameters", self.get_run_parameters)

        self._async_table = self.db.run_table_postgres
        self._artifact_table = self.db.artifact_table_postgres
        self._artifact_store = getattr(cache, "artifact_cache", None)

    @handle_exceptions
    async def get_run(self, request):
        """
        ---
        description: Get one run
        tags:
        - Run
        parameters:
          - $ref: '#/definitions/Params/Path/flow_id'
          - $ref: '#/definitions/Params/Path/run_number'
        produces:
        - application/json
        responses:
            "200":
                description: Returns one run
                schema:
                  $ref: '#/definitions/ResponsesRun'
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
                                  fetch_single=True,
                                  initial_conditions=[
                                      "flow_id = %s",
                                      "{run_id_key} = %s".format(
                                          run_id_key=run_id_key),
                                  ],
                                  initial_values=[flow_name, run_id_value],
                                  enable_joins=True)

    @handle_exceptions
    async def get_all_runs(self, request):
        """
        ---
        description: Get all runs
        tags:
        - Run
        parameters:
          - $ref: '#/definitions/Params/Builtin/_page'
          - $ref: '#/definitions/Params/Builtin/_limit'
          - $ref: '#/definitions/Params/Builtin/_order'
          - $ref: '#/definitions/Params/Builtin/_tags'
          - $ref: '#/definitions/Params/Builtin/_group'
          - $ref: '#/definitions/Params/Custom/run_number'
          - $ref: '#/definitions/Params/Custom/run_id'
          - $ref: '#/definitions/Params/Custom/user_name'
          - $ref: '#/definitions/Params/Custom/ts_epoch'
          - $ref: '#/definitions/Params/Custom/finished_at'
          - $ref: '#/definitions/Params/Custom/duration'
          - $ref: '#/definitions/Params/Custom/status'
        produces:
        - application/json
        responses:
            "200":
                description: Returns all runs
                schema:
                  $ref: '#/definitions/ResponsesRunList'
            "405":
                description: invalid HTTP Method
                schema:
                  $ref: '#/definitions/ResponsesError405'
        """

        allowed_order = self._async_table.keys + ["user", "run", "finished_at", "duration", "status"]
        allowed_group = self._async_table.keys + ["user"]
        allowed_filters = self._async_table.keys + ["user", "run", "finished_at", "duration", "status"]

        # JSONB tag filters combined with `ORDER BY` causes performance impact
        # due to lack of pg statistics on JSONB fields. To battle this, first execute
        # subquery of ordered list from runs_v3 table in and filter by tags on outer query.
        # This needs more research in the future to further improve performance.
        builtin_conditions, _ = builtin_conditions_query(request)
        has_tag_filter = len([s for s in builtin_conditions if 'tags||system_tags' in s]) > 0
        if has_tag_filter:
            allowed_optimized_order = ['flow_id', 'ts_epoch']
            allowed_unoptimized_order = [o for o in allowed_group if o not in allowed_optimized_order]

            _, _, _, optimized_order, _, _ = pagination_query(request, allowed_order=allowed_optimized_order)
            _, _, _, unoptimized_order, _, _ = pagination_query(request, allowed_order=allowed_unoptimized_order)

            # Allow optimized order only when sorting by real columns only
            if optimized_order and not unoptimized_order:
                overwrite_select_from = "(SELECT * FROM {table_name} {order_by}) AS {table_name}".format(
                    order_by="ORDER BY {}".format(", ".join(optimized_order)),
                    table_name=self._async_table.table_name
                )

                return await find_records(request, self._async_table,
                                          allowed_group=allowed_group,
                                          allowed_filters=allowed_filters,
                                          enable_joins=True,
                                          overwrite_select_from=overwrite_select_from
                                          )

        return await find_records(request, self._async_table,
                                  allowed_order=allowed_order,
                                  allowed_group=allowed_group,
                                  allowed_filters=allowed_filters,
                                  enable_joins=True
                                  )

    @handle_exceptions
    async def get_flow_runs(self, request):
        """
        ---
        description: Get all runs of specified flow
        tags:
        - Run
        parameters:
          - $ref: '#/definitions/Params/Path/flow_id'
          - $ref: '#/definitions/Params/Builtin/_page'
          - $ref: '#/definitions/Params/Builtin/_limit'
          - $ref: '#/definitions/Params/Builtin/_order'
          - $ref: '#/definitions/Params/Builtin/_tags'
          - $ref: '#/definitions/Params/Builtin/_group'
          - $ref: '#/definitions/Params/Custom/run_number'
          - $ref: '#/definitions/Params/Custom/run_id'
          - $ref: '#/definitions/Params/Custom/user_name'
          - $ref: '#/definitions/Params/Custom/ts_epoch'
          - $ref: '#/definitions/Params/Custom/finished_at'
          - $ref: '#/definitions/Params/Custom/duration'
          - $ref: '#/definitions/Params/Custom/status'
        produces:
        - application/json
        responses:
            "200":
                description: Returns all runs of specified flow
                schema:
                  $ref: '#/definitions/ResponsesRunList'
            "405":
                description: invalid HTTP Method
                schema:
                  $ref: '#/definitions/ResponsesError405'
        """

        flow_name = request.match_info.get("flow_id")
        return await find_records(request,
                                  self._async_table,
                                  initial_conditions=["flow_id = %s"],
                                  initial_values=[flow_name],
                                  allowed_order=self._async_table.keys + ["user", "run", "finished_at", "duration", "status"],
                                  allowed_group=self._async_table.keys + ["user"],
                                  allowed_filters=self._async_table.keys + ["user", "run", "finished_at", "duration", "status"],
                                  enable_joins=True
                                  )

    @handle_exceptions
    async def get_run_parameters(self, request):
        """
         ---
          description: Get parameters of a run
          tags:
          - Run
          parameters:
            - $ref: '#/definitions/Params/Path/flow_id'
            - $ref: '#/definitions/Params/Path/run_number'
            - $ref: '#/definitions/Params/Custom/invalidate'
          produces:
          - application/json
          responses:
              "200":
                  description: Returns parameters of a run
                  schema:
                    $ref: '#/definitions/ResponsesRunParameters'
              "405":
                  description: invalid HTTP Method
                  schema:
                    $ref: '#/definitions/ResponsesError405'
              "500":
                  description: Internal Server Error (with error id)
                  schema:
                    $ref: '#/definitions/ResponsesRunParametersError500'
        """
        flow_name = request.match_info['flow_id']
        run_number = request.match_info.get("run_number")

        invalidate_cache = query_param_enabled(request, "invalidate")

        # _artifact_store.get_run_parameters will translate run_number/run_id properly
        combined_results = await self._artifact_store.get_run_parameters(
            flow_name, run_number, invalidate_cache=invalidate_cache)

        postprocess_error = combined_results.get("postprocess_error", None)
        if postprocess_error:
            raise GetParametersFailed(
                postprocess_error["detail"], postprocess_error["id"], postprocess_error["traceback"])
        else:
            response = DBResponse(200, combined_results)

        status, body = format_response(request, response)

        return web_response(status, body)

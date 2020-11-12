from services.data.postgres_async_db import AsyncPostgresDB
from services.data.db_utils import DBResponse, translate_run_key
from services.utils import handle_exceptions
from .utils import find_records, web_response, format_response

from ..cache.store import CacheStore
import json


class RunApi(object):
    def __init__(self, app):
        app.router.add_route(
            "GET", "/runs", self.get_all_runs)
        app.router.add_route(
            "GET", "/flows/{flow_id}/runs", self.get_flow_runs)
        app.router.add_route(
            "GET", "/flows/{flow_id}/runs/{run_number}", self.get_run)
        app.router.add_route(
            "GET", "/flows/{flow_id}/runs/{run_number}/parameters", self.get_run_parameters)

        self._async_table = AsyncPostgresDB.get_instance().run_table_postgres
        self._artifact_table = AsyncPostgresDB.get_instance().artifact_table_postgres
        self._artifact_store = CacheStore().artifact_cache

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

        return await find_records(request,
                                  self._async_table,
                                  initial_conditions=[],
                                  initial_values=[],
                                  allowed_order=self._async_table.keys + ["finished_at", "duration", "status"],
                                  allowed_group=self._async_table.keys,
                                  allowed_filters=self._async_table.keys + ["finished_at", "duration", "status"],
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
                                  allowed_order=self._async_table.keys + ["finished_at", "duration", "status"],
                                  allowed_group=self._async_table.keys,
                                  allowed_filters=self._async_table.keys + ["finished_at", "duration", "status"],
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

        # _artifact_store.get_run_parameters will translate run_number/run_id properly
        combined_results = await self._artifact_store.get_run_parameters(flow_name, run_number)

        response = DBResponse(200, combined_results)
        status, body = format_response(request, response)

        return web_response(status, body)

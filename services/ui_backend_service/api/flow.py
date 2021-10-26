from services.utils import handle_exceptions
from .utils import find_records


class FlowApi(object):
    def __init__(self, app, db):
        self.db = db
        app.router.add_route("GET", "/flows", self.get_all_flows)
        app.router.add_route("GET", "/flows/{flow_id}", self.get_flow)
        self._async_table = self.db.flow_table_postgres

    @handle_exceptions
    async def get_flow(self, request):
        """
        ---
        description: Get one flow
        tags:
        - Flow
        parameters:
          - $ref: '#/definitions/Params/Path/flow_id'
        produces:
        - application/json
        responses:
            "200":
                description: Returns one flow
                schema:
                  $ref: '#/definitions/ResponsesFlow'
            "405":
                description: invalid HTTP Method
                schema:
                  $ref: '#/definitions/ResponsesError405'
        """

        flow_name = request.match_info.get("flow_id")

        return await find_records(request,
                                  self._async_table,
                                  fetch_single=True,
                                  initial_conditions=["flow_id = %s"],
                                  initial_values=[flow_name])

    @handle_exceptions
    async def get_all_flows(self, request):
        """
        ---
        description: Get all flows
        tags:
        - Flow
        parameters:
          - $ref: '#/definitions/Params/Builtin/_page'
          - $ref: '#/definitions/Params/Builtin/_limit'
          - $ref: '#/definitions/Params/Builtin/_order'
          - $ref: '#/definitions/Params/Builtin/_tags'
          - $ref: '#/definitions/Params/Builtin/_group'
          - $ref: '#/definitions/Params/Custom/flow_id'
          - $ref: '#/definitions/Params/Custom/user_name'
          - $ref: '#/definitions/Params/Custom/ts_epoch'
        produces:
        - application/json
        responses:
            "200":
                description: Returns all flows
                schema:
                  $ref: '#/definitions/ResponsesFlowList'
            "405":
                description: invalid HTTP Method
                schema:
                  $ref: '#/definitions/ResponsesError405'
        """

        return await find_records(request,
                                  self._async_table,
                                  initial_conditions=[],
                                  initial_values=[],
                                  allowed_order=self._async_table.keys,
                                  allowed_group=self._async_table.keys,
                                  allowed_filters=self._async_table.keys
                                  )

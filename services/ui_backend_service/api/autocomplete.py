from services.utils import handle_exceptions
from services.data.db_utils import translate_run_key
from .utils import format_response, format_response_list, web_response, custom_conditions_query, pagination_query
import asyncio


# Interval for refetching tags from database. Tags data is cached since requesting all of them might take a while.
TAGS_FILL_INTERVAL = 300


class AutoCompleteApi(object):
    # Cache tags so we don't have to request DB everytime
    tags = None

    def __init__(self, app, db):
        self.db = db
        # Cached resources
        app.router.add_route("GET", "/tags/autocomplete", self.get_tags)
        # Non-cached resources
        app.router.add_route("GET", "/flows/autocomplete", self.get_flows)
        app.router.add_route("GET", "/flows/{flow_id}/runs/autocomplete", self.get_runs_for_flow)
        app.router.add_route("GET", "/flows/{flow_id}/runs/{run_id}/steps/autocomplete", self.get_steps_for_run)
        self._flow_table = self.db.flow_table_postgres
        self._run_table = self.db.run_table_postgres
        self._step_table = self.db.step_table_postgres

    @handle_exceptions
    async def get_tags(self, request):
        """
        ---
        description: Get all available tags, with pagination.
        tags:
        - Autocomplete
        produces:
        - application/json
        responses:
            "200":
                description: Returns string list of all tags
                schema:
                    $ref: '#/definitions/ResponsesAutocompleteTagList'
        """
        # pagination setup
        page, limit, offset, _, _, _ = pagination_query(request)

        # custom query conditions
        custom_conditions, custom_vals = custom_conditions_query(request, allowed_keys=["tag"])

        conditions = custom_conditions
        values = custom_vals
        db_response, pagination = await self._run_table.get_tags(conditions=conditions, values=values, limit=limit, offset=offset)
        status, body = format_response_list(request, db_response, pagination, page)

        return web_response(status, body)

    @handle_exceptions
    async def get_flows(self, request):
        """
        ---
        description: Get all flow id's as a list, with pagination.
        tags:
        - Autocomplete
        - Flow
        produces:
        - application/json
        responses:
            "200":
                description: Returns string list of all flow id's
                schema:
                    $ref: '#/definitions/ResponsesAutocompleteFlowList'
        """
        # pagination setup
        page, limit, offset, _, _, _ = pagination_query(request)

        # custom query conditions
        custom_conditions, custom_vals = custom_conditions_query(request, allowed_keys=["flow_id"])

        conditions = custom_conditions
        values = custom_vals
        db_response, pagination = await self._flow_table.get_flow_ids(conditions=conditions, values=values, limit=limit, offset=offset)
        status, body = format_response_list(request, db_response, pagination, page)
        return web_response(status, body)

    @handle_exceptions
    async def get_runs_for_flow(self, request):
        """
        ---
        description: Get all run id's for single flow
        tags:
        - Autocomplete
        - Run
        produces:
        - application/json
        responses:
            "200":
                description: Returns string list of run ids
                schema:
                    $ref: '#/definitions/ResponsesAutocompleteRunList'
        """
        flow_id = request.match_info.get("flow_id")

        # pagination setup
        page, limit, offset, _, _, _ = pagination_query(request)

        # custom query conditions
        custom_conditions, custom_vals = custom_conditions_query(request, allowed_keys=["run"])

        conditions = ["flow_id=%s"] + custom_conditions
        values = [flow_id] + custom_vals

        results, pagination = await self._run_table.get_run_keys(conditions=conditions, values=values, limit=limit, offset=offset)
        status, body = format_response_list(request, results, pagination, page)
        return web_response(status, body)

    @handle_exceptions
    async def get_steps_for_run(self, request):
        """
        ---
        description: Get all step names for single run
        tags:
        - Autocomplete
        - Step
        produces:
        - application/json
        responses:
            "200":
                description: Returns string list of step names
                schema:
                    $ref: '#/definitions/ResponsesAutocompleteStepList'
        """
        flow_id = request.match_info.get("flow_id")
        run_id = request.match_info.get("run_id")

        run_key, run_value = translate_run_key(run_id)

        # pagination setup
        page, limit, offset, _, _, _ = pagination_query(request)

        # custom query conditions
        custom_conditions, custom_vals = custom_conditions_query(request, allowed_keys=["step_name"])

        sql_conditions = ["flow_id=%s", "{}=%s".format(run_key)] + custom_conditions
        values = [flow_id, run_value] + custom_vals

        results, pagination = await self._step_table.get_step_names(conditions=sql_conditions, values=values, limit=limit, offset=offset)

        status, body = format_response_list(request, results, pagination, page)
        return web_response(status, body)

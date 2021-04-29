from services.utils import handle_exceptions
from services.data.db_utils import translate_run_key
from .utils import format_response, format_response_list, web_response, custom_conditions_query, pagination_query


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
        app.router.add_route("GET", "/flows/{flow_id}/runs/{run_id}/artifacts/autocomplete", self.get_artifacts_for_run)

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
        return await resource_response(request, self.db.run_table_postgres.get_tags, allowed_keys=["tag"])

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

        return await resource_response(request, self.db.flow_table_postgres.get_flow_ids, allowed_keys=["flow_id"])

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

        return await resource_response(
            request,
            self.db.run_table_postgres.get_run_keys,
            initial_conditions=["flow_id=%s"],
            initial_values=[flow_id],
            allowed_keys=["run"]
        )

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

        return await resource_response(
            request,
            self.db.step_table_postgres.get_step_names,
            initial_conditions=["flow_id=%s", "{}=%s".format(run_key)],
            initial_values=[flow_id, run_value],
            allowed_keys=["step_name"]
        )

    @handle_exceptions
    async def get_artifacts_for_run(self, request):
        """
        ---
        description: Get all artifact names for single run
        tags:
        - Autocomplete
        - Artifact
        produces:
        - application/json
        responses:
            "200":
                description: Returns string list of step names
                schema:
                    $ref: '#/definitions/ResponsesAutocompleteArtifactList'
        """
        flow_id = request.match_info.get("flow_id")
        run_id = request.match_info.get("run_id")

        run_key, run_value = translate_run_key(run_id)

        return await resource_response(
            request,
            self.db.artifact_table_postgres.get_artifact_names,
            initial_conditions=["flow_id=%s", "{}=%s".format(run_key)],
            initial_values=[flow_id, run_value],
            allowed_keys=["name"]
        )


async def resource_response(request, get_record_fun, initial_conditions=[], initial_values=[], allowed_keys=[]):
    """
    Abstract resource fetch helper that processes query and pagination parameters from the request,
    and performs a db query with the generated conditions, with a provided db getter.

    Parameters
    ----------
    request : WebRequest
        aiohttp web request with .query key available.
    get_record_fun : Callable
        DB getter that should accept the following variables:
        (conditions, values, limit, offset)
    initial_conditions : List (optional)
        optional list of initial conditions to pass the db getter,
        along with values extracted from request parameters.
    initial_values : List (optional)
        optional list of initial values, for the initial conditions.
    allowed_keys : List (optional)
        optional list of allowed keys.
        Used to determine which keys are extracted from request parameters and which should be omitted.

    Returns
    -------
    WebResponse
        A formatted web response with the default API response body.
    """
    # pagination setup
    page, limit, offset, _, _, _ = pagination_query(request)

    # custom query conditions
    custom_conditions, custom_vals = custom_conditions_query(request, allowed_keys=allowed_keys)

    conditions = initial_conditions + custom_conditions
    values = initial_values + custom_vals

    db_response, pagination = await get_record_fun(conditions=conditions, values=values, limit=limit, offset=offset)

    status, body = format_response_list(request, db_response, pagination, page)

    return web_response(status, body)

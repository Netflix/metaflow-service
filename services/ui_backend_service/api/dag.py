from services.data.db_utils import DBResponse, translate_run_key
from services.utils import handle_exceptions
from .utils import format_response, web_response, query_param_enabled


class DagApi(object):
    def __init__(self, app, db, cache=None):
        self.db = db
        app.router.add_route(
            "GET", "/flows/{flow_id}/runs/{run_number}/dag", self.get_run_dag
        )
        self._metadata_table = self.db.metadata_table_postgres
        self._dag_store = getattr(cache, "dag_cache", None)

    @handle_exceptions
    async def get_run_dag(self, request):
        """
        ---
        description: Get DAG structure for a run.
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
                description: Return DAG structure for a specific run
                schema:
                  $ref: '#/definitions/ResponsesDag'
            "405":
                description: invalid HTTP Method
                schema:
                  $ref: '#/definitions/ResponsesError405'
            "404":
                description: necessary data for DAG generation Not Found
                schema:
                  $ref: '#/definitions/ResponsesError404'
            "500":
                description: Internal Server Error (with error id)
                schema:
                    $ref: '#/definitions/ResponsesDagError500'
        """
        # Before running the cache action, we make sure that the run has
        # the necessary data to generate a DAG.
        db_response = await self.get_run_dag_data(request)

        if not db_response.response_code == 200:
            # DAG data was not found, return with the corresponding status.
            status, body = format_response(request, db_response)
            return web_response(status, body)

        # Prefer run_id over run_number
        flow_name = db_response.body['flow_id']
        run_id = db_response.body.get('run_id') or db_response.body['run_number']
        invalidate_cache = query_param_enabled(request, "invalidate")

        dag = await self._dag_store.cache.GenerateDag(
            flow_name, run_id, invalidate_cache=invalidate_cache)

        if dag.has_pending_request():
            async for event in dag.stream():
                if event["type"] == "error":
                    # raise error, there was an exception during processing.
                    raise GenerateDAGFailed(event["message"], event["id"], event["traceback"])
            await dag.wait()  # wait until results are ready
        dag = dag.get()
        response = DBResponse(200, dag)
        status, body = format_response(request, response)

        return web_response(status, body)

    async def get_run_dag_data(self, request) -> DBResponse:
        """
        Fetches either a _graph_info artifact, or a code-package metadata entry if the artifact is missing.
        Used to determine whether a run can display a DAG.
        """
        flow_name = request.match_info['flow_id']
        run_number = request.match_info.get("run_number")
        db_response = await self.get_run_graph_info_artifact(flow_name, run_number)
        if not db_response.response_code == 200:
            # Try to look for codepackage if graph artifact is missing
            db_response = await self.get_run_codepackage_metadata(flow_name, run_number)

        return db_response

    async def get_run_codepackage_metadata(self, flow_name: str, run_id: str) -> DBResponse:
        """
        Tries to locate 'code-package' or 'code-package-url' in run metadata.
        """
        run_id_key, run_id_value = translate_run_key(run_id)
        # 'code-package' value contains json with dstype, sha1 hash and location
        # 'code-package-url' value contains only location as a string
        db_response, *_ = await self.db.metadata_table_postgres.find_records(
            conditions=[
                "flow_id = %s",
                "{run_id_key} = %s".format(
                    run_id_key=run_id_key),
                "(field_name = %s OR field_name = %s)"
            ],
            values=[
                flow_name, run_id_value,
                "code-package", "code-package-url"
            ],
            fetch_single=True, expanded=True
        )

        return db_response

    async def get_run_graph_info_artifact(self, flow_name: str, run_id: str) -> DBResponse:
        """
        Tries to locate '_graph_info' in run artifacts
        """
        run_id_key, run_id_value = translate_run_key(run_id)

        db_response, *_ = await self.db.artifact_table_postgres.find_records(
            conditions=[
                "flow_id = %s",
                "{run_id_key} = %s".format(
                    run_id_key=run_id_key),
                "step_name = %s",
                "name = %s"
            ],
            values=[
                flow_name, run_id_value, "_parameters",
                "_graph_info",
            ],
            fetch_single=True, expanded=True
        )

        return db_response


class GenerateDAGFailed(Exception):
    def __init__(self, msg="Failed to process DAG", id="failed-to-process-dag", traceback_str=None):
        self.message = msg
        self.id = id
        self.traceback_str = traceback_str

    def __str__(self):
        return self.message

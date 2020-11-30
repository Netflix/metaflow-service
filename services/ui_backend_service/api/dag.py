from services.data.postgres_async_db import AsyncPostgresDB
from services.data.db_utils import DBResponse, translate_run_key
from services.utils import handle_exceptions
from .utils import format_response, web_response

from ..cache.store import CacheStore
from aiohttp import web
import json

from ..features import FEATURE_MODEL_EXPAND


class DagApi(object):
    def __init__(self, app, db=AsyncPostgresDB.get_instance()):
        self.db = db
        app.router.add_route(
            "GET", "/flows/{flow_id}/runs/{run_number}/dag", self.get_run_dag
        )
        self._metadata_table = self.db.metadata_table_postgres
        self._dag_store = CacheStore().dag_cache

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
                description: codepackage for DAG generation Not Found
                schema:
                  $ref: '#/definitions/ResponsesError404'
            "500":
                description: Internal Server Error (with error id)
                schema:
                    $ref: '#/definitions/ResponsesDagError500'
        """
        flow_name = request.match_info['flow_id']
        run_id_key, run_id_value = translate_run_key(
            request.match_info.get("run_number"))
        # 'code-package' value contains json with dstype, sha1 hash and location
        db_response, _ = await self._metadata_table.find_records(
            conditions=["flow_id = %s", "{run_id_key} = %s".format(
                run_id_key=run_id_key), "field_name = %s"],
            values=[flow_name, run_id_value, "code-package"],
            fetch_single=True, expanded=FEATURE_MODEL_EXPAND
        )
        if not db_response.response_code == 200:
            status, body = format_response(request, db_response)
            return web_response(status, body)

        # parse codepackage location.
        codepackage_loc = json.loads(db_response.body['value'])['location']
        flow_name = db_response.body['flow_id']

        # Fetch or Generate the DAG from the codepackage.
        dag = await self._dag_store.generate_dag(flow_name, codepackage_loc)
        # if not dag.is_ready():
        #     async for event in dag.stream():
        #         if event["type"] == "error":
        #             # raise error, there was an exception during processing.
        #             raise GenerateDAGFailed(event["message"], event["id"], event["traceback"])
        #     await dag.wait()  # wait until results are ready
        response = DBResponse(200, dag)
        status, body = format_response(request, response)

        return web_response(status, body)


class GenerateDAGFailed(Exception):
    def __init__(self, msg="Failed to process DAG", id="failed-to-process-dag", traceback_str=None):
        self.message = msg
        self.id = id
        self.traceback_str = traceback_str

    def __str__(self):
        return self.message

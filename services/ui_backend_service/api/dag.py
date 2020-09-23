from services.data.postgres_async_db import AsyncPostgresDB
from services.data.db_utils import DBResponse, translate_run_key
from services.utils import handle_exceptions
from .utils import format_response, web_response

from ..cache.store import CacheStore
from aiohttp import web
import json


class DagApi(object):
    def __init__(self, app):
        app.router.add_route(
            "GET", "/flows/{flow_id}/runs/{run_number}/dag", self.get_run_dag
        )
        self._metadata_table = AsyncPostgresDB.get_instance().metadata_table_postgres
        self._dag_store = CacheStore().dag_cache

    @handle_exceptions
    async def get_run_dag(self, request):
        flow_name = request.match_info['flow_id']
        run_id_key, run_id_value = translate_run_key(
            request.match_info.get("run_number"))
        # 'code-package' value contains json with dstype, sha1 hash and location
        db_response, _ = await self._metadata_table.find_records(
            conditions=["flow_id = %s", "{run_id_key} = %s".format(
                run_id_key=run_id_key), "field_name = %s"],
            values=[flow_name, run_id_value, "code-package"],
            fetch_single=True
        )
        if not db_response.response_code == 200:
            status, body = format_response(request, db_response)
            return web_response(status, body)

        # parse codepackage location.
        codepackage_loc = json.loads(db_response.body['value'])['location']

        # Fetch or Generate the DAG from the codepackage.
        dag = await self._dag_store.cache.GenerateDag(codepackage_loc)
        await dag.wait()  # wait for results to be ready
        success, dag = dag.get()
        response = DBResponse(200 if success else 404, dag)
        status, body = format_response(request, response)

        return web_response(status, body)

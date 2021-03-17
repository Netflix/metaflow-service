from services.utils import handle_exceptions, web_response
import asyncio
import threading


class AutoCompleteApi(object):
    # Cache tags so we don't have to request DB everytime
    tags = []

    def __init__(self, app, db):
        self.db = db
        # Cached resources
        app.router.add_route("GET", "/autocomplete/tags", self.get_all_tags)
        app.router.add_route("GET", "/autocomplete/flows", self.get_all_flows)
        # Non-cached resources
        app.router.add_route("GET", "/autocomplete/flows/{flow_id}/runs", self.get_runs_for_flow)
        app.router.add_route("GET", "/autocomplete/flows/{flow_id}/runs/{run_id}/steps", self.get_steps_for_run)
        self._async_table = self.db.run_table_postgres
        self.loop = asyncio.get_event_loop()
        self.loop.create_task(self.setup_tags())

    @handle_exceptions
    async def setup_tags(self):
        db_response = await self._async_table.get_tags()
        
        if db_response.response_code == 200:
            self.tags = db_response.body

        self.loop.call_later(300, lambda x: self.loop.create_task(self.setup_tags()), self)

    @handle_exceptions
    async def get_all_tags(self, request):
        return web_response(200, self.tags)

    @handle_exceptions
    async def get_all_flows(self, request):
        db_response = await self._async_table.get_field_from(field="flow_id", table="flows_v3")
        return web_response(db_response.response_code, db_response.body)

    @handle_exceptions
    async def get_runs_for_flow(self, request):
        flowid = request.match_info.get("flow_id")
        sql_conditions = ["flow_id=%s"]
        db_response = await self._async_table.get_field_from(field="COALESCE(run_id, run_number::text)", table="runs_v3", conditions=sql_conditions, values=[flowid])
        return web_response(db_response.response_code, db_response.body)

    @handle_exceptions
    async def get_steps_for_run(self, request):
        flowid = request.match_info.get("flow_id")
        runid = request.match_info.get("run_id")
        sql_conditions = ["flow_id=%s", "(run_number=%s OR run_id=%s)"]

        db_response = await self._async_table.get_field_from(field="step_name", table="steps_v3", conditions=sql_conditions, values=[flowid, runid, runid])
        return web_response(db_response.response_code, db_response.body)

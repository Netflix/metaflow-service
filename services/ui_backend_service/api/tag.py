from services.utils import handle_exceptions, web_response


class TagApi(object):
    def __init__(self, app, db):
        self.db = db
        app.router.add_route("GET", "/tags", self.get_all_tags)
        self._async_table = self.db.run_table_postgres

    @handle_exceptions
    async def get_all_tags(self, request):
        db_response, _ = await self._async_table.get_tags()
        return web_response(db_response.response_code, db_response.body)

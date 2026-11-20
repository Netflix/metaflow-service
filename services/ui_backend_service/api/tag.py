from services.utils import handle_exceptions, web_response


class TagApi(object):
    def __init__(self, app, db):
        self.db = db
        app.router.add_route("GET", "/tags", self.get_all_tags)
        self._async_table = self.db.run_table_postgres

    @handle_exceptions
    async def get_all_tags(self, request):
        """
        ---
        description: Get all distinct tags and system tags of all runs.
            Unlike /tags/autocomplete this queries the database on every request
            and returns a plain list, without pagination.
        tags:
        - Run
        produces:
        - application/json
        responses:
            "200":
                description: Returns string list of all tags
                schema:
                    $ref: '#/definitions/ResponsesTagList'
            "405":
                description: invalid HTTP Method
                schema:
                    $ref: '#/definitions/ResponsesError405'
            "500":
                description: Internal Server Error (with error id)
                schema:
                    $ref: '#/definitions/ResponsesError500'
        """
        db_response, _ = await self._async_table.get_tags()
        return web_response(db_response.response_code, db_response.body)

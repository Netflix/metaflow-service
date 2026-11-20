# API documentation

A thorough documentation of the RESTful API routes, responses and types is available as a Swagger 2.0 spec generated from the handler docstrings.
The spec is not served by the backend; generate it locally and open `api-docs/ui.json` in any Swagger viewer, e.g. editor.swagger.io. This needs neither a database nor the service dependencies, only `requirements.docs.txt` (pyyaml, openapi-spec-validator):

```sh
pip install -r requirements.docs.txt
python -m services.openapi --service ui --out ./api-docs --strict
```

Use `--service all` to also generate `metadata.json` and `migration.json`. The UI spec covers the UI API only: the `/metadata` sub-app (see `metadata.json`), the websocket `/ws` and the frontend routes are not included, and paths are documented without `PATH_PREFIX`.

## Documenting an endpoint

Every route registered in a service's `register_routes()` must be documented; `--strict` and the unit tests in `services/openapi/tests` fail otherwise. Undocumented routes get a stub operation and a warning when generating without `--strict`.

The generator does not import the services: it reads `register_routes()`, the API classes it instantiates and their `app.router.add_route(...)` calls from the source. Keep registrations in that form: instantiate each API class directly in `register_routes()`, and in its `__init__` call `app.router.add_route("METHOD", "/literal/path", self.handler)` (or `add_get`/`add_post`/...) with a handler defined in the same class. The generator fails with the file and line on anything it cannot follow (loops, computed paths, helpers that receive `app`, inherited handlers), and a unit test checks that it finds exactly the routes aiohttp registers.

Handlers are documented with a docstring: optional free text, a line that is exactly `---`, then a Swagger 2.0 [operation object](https://swagger.io/specification/v2/#operation-object) in YAML. The free text becomes the `summary` if no `description` is given. `operationId` defaults to `ClassName.method_name`.

```python
@handle_exceptions
async def get_run(self, request):
    """
    ---
    description: Get one run
    tags:
    - Run
    parameters:
      - $ref: '#/parameters/path_flow_id'
      - $ref: '#/parameters/path_run_number'
    produces:
    - application/json
    responses:
        "200":
            description: Returns one run
            schema:
              $ref: '#/definitions/ResponsesRun'
    """
```

Components live in plain Python dicts:

- `services/utils/openapi_components.py`: parameters and models shared by all services (`path_flow_id`, `path_run_number`, `path_step_name`, `path_task_id`, `query_limit`, ..., the base `Flow`/`Run`/`Step`/`Task`/`Metadata`/`Artifact` models), plus helpers to build properties, models and the UI `{data, status, links}` response wrappers.
- `services/ui_backend_service/doc.py` and `services/metadata_service/doc.py`: service specific `parameters` and `definitions`, built with those helpers. Keys must not clash with the shared ones. These modules (and `openapi_components.py`) are loaded by the generator on their own, so they may only import the standard library and `services.utils.openapi_components`.

Guidelines:

- Prefer the shared `#/parameters/path_*` parameters over inline path parameters. Every `{placeholder}` in the route must be declared.
- Only use Swagger 2.0: no `anyOf`/`oneOf`/`nullable`/`style`, no keys next to a `$ref`, sample values in `example` (not `default`).
- Model properties must match the `serialize()` output of the matching row class (`services/data/models.py` for the metadata service, `services/ui_backend_service/data/db/models/*_row.py` for the UI). When you change a serializer, change the model in the same commit; the unit tests compare the two.
- Every component must be referenced by at least one operation; unused components fail the tests.

## Examples

```
/flows/HelloFlow/runs?_page=4                               List page 4
/flows/HelloFlow/runs?_page=2&_limit=10                     List page 4, each page contains 10 items

/flows/HelloFlow/runs?_order=run_number                     Order by `run_number` in descending order
/flows/HelloFlow/runs?_order=+run_number                    Order by `run_number` in ascending order
/flows/HelloFlow/runs?_order=-run_number                    Order by `run_number` in descending order
/flows/HelloFlow/runs?_order=run_number,ts_epoch            Order by `run_number` and `ts_epoch` in descending order

/runs?_tags=user:dipper                                     Filter by one tag
/runs?_tags=user:dipper,runtime:dev                         Filter by multiple tags (AND)
/runs?_tags:all=user:dipper,runtime:dev                     Filter by multiple tags (AND)
/runs?_tags:any=user:dipper,runtime:dev                     Filter by multiple tags (OR)
/runs?_tags:likeall=user:dip,untime:de                      Filter by multiple tags that contains string (AND)
/runs?_tags:likeany=user:,untime:de                         Filter by multiple tags that contains string (OR)

/runs?_group=flow_id                                        Group by `flow_id`
/runs?_group=flow_id,user_name                              Group by `flow_id` and `user_name`
/runs?_group=user_name&_limit=2                             Group by `user_name` and limit each group to `2` runs
/runs?_group=flow_id&_order=flow_id,run_number              Group by `flow_id` and order by `flow_id & run_number`
/runs?_group=flow_id&user_name=dipper                       List runs by `dipper` and group by `flow_id`
/runs?user=null                                             `user` is NULL

/flows/HelloFlow/runs?run_number=40                         `run_number` equals `40`
/flows/HelloFlow/runs?run_number:eq=40                      `run_number` equals `40`
/flows/HelloFlow/runs?run_number:ne=40                      `run_number` not equals `40`
/flows/HelloFlow/runs?run_number:lt=40                      `run_number` less than `40`
/flows/HelloFlow/runs?run_number:le=40                      `run_number` less than or equals `40`
/flows/HelloFlow/runs?run_number:gt=40                      `run_number` greater than `40`
/flows/HelloFlow/runs?run_number:ge=40                      `run_number` greater than equals `40`

/flows/HelloFlow/runs?user_name:co=atia                     `user_name` contains `atia`
/flows/HelloFlow/runs?user_name:sw=mati                     `user_name` starts with `mati`
/flows/HelloFlow/runs?user_name:ew=tias                     `user_name` ends with `tias`

/flows?user_name=dipper,mabel                               `user_name` is either `dipper` OR `mabel`

/flows/HelloFlow/runs?run_number:lt=60&run_number:gt=40     `run_number` less than 60 and greater than 40
```

## Available operators

| URL operator | Description             | SQL operator |
|--------------|-------------------------|--------------|
| `eq`         | equals                  | `=`          |
| `ne`         | not equals              | `!=`         |
| `lt`         | less than               | `<`          |
| `le`         | less than equals        | `<=`         |
| `gt`         | greater than            | `>`          |
| `ge`         | greater than equals     | `>=`         |
| `co`         | contains                | `*string*`   |
| `sw`         | starts with             | `^string*`   |
| `ew`         | ends with               | `*string$`   |
| `is`         | is                      | `IS`         |
| `li`         | is like (use with %val) | `ILIKE`      |

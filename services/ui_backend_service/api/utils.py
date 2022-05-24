import json
import os
import re
import time
from collections import deque
from typing import Callable, Dict, List, Tuple, Optional
from urllib.parse import parse_qsl, urlsplit

from asyncio import iscoroutinefunction
from aiohttp import web
from multidict import MultiDict
from services.data.db_utils import DBPagination, DBResponse
from services.data.tagging_utils import apply_run_tags_to_db_response
from services.utils import format_baseurl, format_qs, web_response
from functools import reduce
from services.utils import logging

logger = logging.getLogger("Utils")


# only look for config.json files in ui_backend_service root
JSON_CONFIG_ROOT = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..")
)


def get_json_config(variable_name: str):
    """
    Attempts to read a JSON configuration from an environment variable with
    the given variable_name (in upper case). Failing to find an environment variable, it will
    fallback to looking for a config.variable_name.json file in the ui_backend_service root
    Example
    -------
    get_json_config("plugins")
        Looks for a 'PLUGINS' environment variable. If none is found,
        looks for a  'config.plugins.json' file in ui_backend_service root.
    """
    env_name = variable_name.upper()

    filepath = os.path.join(JSON_CONFIG_ROOT, f"config.{variable_name.lower()}.json")

    return get_json_from_env(env_name) or \
        get_json_from_file(filepath)


def get_json_from_env(variable_name: str):
    env_json = os.environ.get(variable_name)
    if env_json:
        try:
            return json.loads(env_json)
        except Exception as e:
            logger.warning(f"Error parsing JSON: {e}, from {variable_name}: {env_json}")
    return None


def get_json_from_file(filepath: str):
    try:
        with open(filepath) as f:
            return json.load(f)
    except FileNotFoundError:
        # not an issue, as users might not want to configure certain components.
        return None
    except Exception as ex:
        logger.warning(
            f"Error parsing JSON from file: {filepath}\n Error: {str(ex)}"
        )
        return None


def format_response(request: web.BaseRequest, db_response: DBResponse) -> Tuple[int, Dict]:
    query = {}
    for key in request.query:
        query[key] = request.query.get(key)

    baseurl = format_baseurl(request)
    response_object = {
        "data": db_response.body,
        "status": db_response.response_code,
        "links": {
            "self": "{}{}".format(baseurl, format_qs(query))
        },
        "query": query,
    }
    return db_response.response_code, response_object


def format_response_list(request: web.BaseRequest, db_response: DBResponse, pagination: DBPagination, page: int, page_count: int = None) -> Tuple[int, Dict]:
    query = {}
    for key in request.query:
        query[key] = request.query.get(key)

    if not pagination:
        nextPage = None
    else:
        nextPage = page + 1 if (pagination.count or 0) >= pagination.limit else None
    prevPage = max(page - 1, 1)

    baseurl = format_baseurl(request)
    response_object = {
        "data": db_response.body,
        "status": db_response.response_code,
        "links": {
            "self": "{}{}".format(baseurl, format_qs(query)),
            "first": "{}{}".format(baseurl, format_qs(query, {"_page": 1})),
            "prev": "{}{}".format(baseurl, format_qs(query, {"_page": prevPage})),
            "next": "{}{}".format(baseurl, format_qs(query, {"_page": nextPage})) if nextPage else None,
            "last": "{}{}".format(baseurl, format_qs(query, {"_page": page_count})) if page_count else None
        },
        "pages": {
            "self": page,
            "first": 1,
            "prev": prevPage,
            "next": nextPage,
            "last": page_count
        },
        "query": query,
    }
    return db_response.response_code, response_object


def pagination_query(request: web.BaseRequest, allowed_order: List[str] = [], allowed_group: List[str] = []):
    # Page
    try:
        page = max(int(request.query.get("_page", 1)), 1)
    except:
        page = 1

    # Limit
    try:
        # Default limit is 10, maximum is 1000
        limit = min(int(request.query.get("_limit", 10)), 1000)
    except:
        limit = 10

    # Group limit
    try:
        # default rows per group 10. Cap at 1000
        group_limit = min(int(request.query.get("_group_limit", 10)), 1000)
    except:
        group_limit = 10

    # Offset
    offset = limit * (page - 1)

    # Order by
    try:
        _order = request.query.get("_order")
        if _order is not None:
            _orders = []
            for order in _order.split(","):
                if order.startswith("+"):
                    column = order[1:]
                    direction = "ASC"
                elif order.startswith("-"):
                    column = order[1:]
                    direction = "DESC"
                else:
                    column = order
                    direction = "DESC"

                if column in allowed_order:
                    _orders.append("\"{}\" {}".format(column, direction))

            order = _orders
        else:
            order = None

    except:
        order = None

    # Grouping (partitioning)
    # Allows single or multiple grouping rules (nested grouping)
    # Limits etc. will be applied to each group
    _group = request.query.get("_group")
    if _group is not None:
        groups = []
        for g in _group.split(","):
            if g in allowed_group:
                groups.append("\"{}\"".format(g))
    else:
        groups = None

    return page, limit, offset, \
        order if order else None, \
        groups if groups else None, \
        group_limit


# Built-in conditions (always prefixed with _)
def builtin_conditions_query(request: web.BaseRequest):
    return builtin_conditions_query_dict(request.query)


def builtin_conditions_query_dict(query: MultiDict):
    conditions = []
    values = []

    for key, val in query.items():
        if not key.startswith("_"):
            continue

        deconstruct = key.split(":", 1)
        if len(deconstruct) > 1:
            field = deconstruct[0]
            operator = deconstruct[1]
        else:
            field = key
            operator = None

        # Tags
        if field == "_tags":
            tags = val.split(",")
            if operator == "likeany" or operator == "likeall":
                # `?_tags:likeany` => LIKE ANY (OR)
                # `?_tags:likeall` => LIKE ALL (AND)
                # Raw SQL: SELECT * FROM runs_v3 WHERE tags||system_tags::text LIKE ANY(array['{%runtime:dev%','%user:m%']');
                # Psycopg SQL: SELECT * FROM runs_v3 WHERE tags||system_tags::text LIKE ANY(array[%s,%s]);
                # Values for Psycopg: ['%runtime:dev%','%user:m%']
                compare = "ANY" if operator == "likeany" else "ALL"

                conditions.append(
                    "tags||system_tags::text LIKE {}(array[{}])"
                    .format(compare, ",".join(["%s"] * len(tags))))
                values += map(lambda t: "%{}%".format(t), tags)

            else:
                # `?_tags:any` => ?| (OR)
                # `?_tags:all` => ?& (AND) (default)
                compare = "?|" if operator == "any" else "?&"

                conditions.append("tags||system_tags {} array[{}]".format(
                    compare, ",".join(["%s"] * len(tags))))
                values += tags

    return conditions, values


operators_to_sql = {
    "eq": "\"{}\" = %s",          # equals
    "ne": "\"{}\" != %s",         # not equals
    "lt": "\"{}\" < %s",          # less than
    "le": "\"{}\" <= %s",         # less than or equals
    "gt": "\"{}\" > %s",          # greater than
    "ge": "\"{}\" >= %s",         # greater than or equals
    "co": "\"{}\" ILIKE %s",      # contains
    "sw": "\"{}\" ILIKE %s",      # starts with
    "ew": "\"{}\" ILIKE %s",      # ends with
    "li": "\"{}\" ILIKE %s",      # ILIKE (used with % placeholders supplied in the request params)
    "is": "\"{}\" IS %s",         # IS
}

operators_to_sql_values = {
    "eq": "{}",
    "ne": "{}",
    "lt": "{}",
    "le": "{}",
    "gt": "{}",
    "ge": "{}",
    "co": "%{}%",
    "sw": "{}%",
    "ew": "%{}",
    "li": "{}",
    "is": "{}",
}


def bound_filter(op, term, key):
    "returns function that binds the key, and the term that should be compared to, on an item"
    _filter = operators_to_filters[op]

    def _fn(item):
        try:
            return _filter(item[key], term) if key in item else False
        except Exception:
            return False
    return _fn


# NOTE: keep these as simple comparisons,
# any kind of value decoding should be done outside the lambdas instead
# to promote reusability.
operators_to_filters = {
    "eq": (lambda item, term: str(item) == term),
    "ne": (lambda item, term: str(item) != term),
    "lt": (lambda item, term: int(item) < int(term)),
    "le": (lambda item, term: int(item) <= int(term)),
    "gt": (lambda item, term: int(item) > int(term)),
    "ge": (lambda item, term: int(item) >= int(term)),
    "co": (lambda item, term: str(term) in str(item)),
    "sw": (lambda item, term: str(item).startswith(str(term))),
    "ew": (lambda item, term: str(item).endswith(str(term))),
    "li": (lambda item, term: True),  # Not implemented yet
    "is": (lambda item, term: str(item) is str(term)),
    're': (lambda item, pattern: re.compile(pattern).match(str(item))),
}


def filter_and(filter_a, filter_b):
    return lambda item: filter_a(item) and filter_b(item)


def filter_or(filter_a, filter_b):
    return lambda item: filter_a(item) or filter_b(item)


def filter_from_conditions_query(request: web.BaseRequest, allowed_keys: List[str] = []):
    return filter_from_conditions_query_dict(request.query, allowed_keys)


def filter_from_conditions_query_dict(query: MultiDict, allowed_keys: List[str] = []):
    """
    Gathers all custom conditions from request query and returns a filter function
    """
    filters = []

    def _no_op(item):
        return True

    for key, val in query.items():
        if key.startswith("_") and not key.startswith('_tags'):
            continue  # skip internal conditions except _tags

        deconstruct = key.split(":", 1)
        if len(deconstruct) > 1:
            field = deconstruct[0]
            operator = deconstruct[1]
        else:
            field = key
            operator = "eq"

        if allowed_keys is not None and field not in allowed_keys:
            continue  # skip conditions on non-allowed fields

        if operator not in operators_to_filters and field != '_tags':
            continue  # skip conditions with no known operators

        # Tags
        if field == "_tags":
            tags = val.split(",")
            _fils = []
            # support likeany, likeall, any, all. default to all
            if operator == "likeany":
                joiner_fn = filter_or
                op = "re"
            elif operator == "likeall":
                joiner_fn = filter_and
                op = "re"
            elif operator == "any":
                joiner_fn = filter_or
                op = "co"
            else:
                joiner_fn = filter_and
                op = "co"

            def bound(op, term):
                _filter = operators_to_filters[op]
                return lambda item: _filter(item['tags'] + item['system_tags'], term) if 'tags' in item and 'system_tags' in item else False

            for tag in tags:
                # Necessary to wrap value inside quotes as we are
                # checking for containment on a list that has been cast to a string
                _pattern = ".*{}.*".format(tag) if op == "re" else "'{}'"
                _val = _pattern.format(tag)
                _fils.append(bound(op, _val))

            if len(_fils) == 0:
                _fil = _no_op
            elif len(_fils) == 1:
                _fil = _fils[0]
            else:
                _fil = reduce(joiner_fn, _fils)
            filters.append(_fil)

        # Default case
        else:
            vals = val.split(",")

            _val_filters = []
            for val in vals:
                _val_filters.append(bound_filter(operator, val, field))

            # OR with a no_op filter would break, so handle the case of no values separately.
            if len(_val_filters) == 0:
                _fil = _no_op
            elif len(_val_filters) == 1:
                _fil = _val_filters[0]
            else:
                # if multiple values, join filters with filter_or()
                _fil = reduce(filter_or, _val_filters)
            filters.append(_fil)

    _final_filter = reduce(filter_and, filters, _no_op)

    return _final_filter  # return filters reduced with filter_and()


# Custom conditions parser (table columns, never prefixed with _)
def custom_conditions_query(request: web.BaseRequest, allowed_keys: List[str] = []):
    return custom_conditions_query_dict(request.query, allowed_keys)


def custom_conditions_query_dict(query: MultiDict, allowed_keys: List[str] = []):
    conditions = []
    values = []

    for key, val in query.items():
        if key.startswith("_"):
            continue

        deconstruct = key.split(":", 1)
        if len(deconstruct) > 1:
            field = deconstruct[0]
            operator = deconstruct[1]
        else:
            field = key
            operator = "eq"

        if allowed_keys is not None and field not in allowed_keys:
            continue

        if operator not in operators_to_sql:
            continue

        vals = val.split(",")

        conditions.append(
            "({})".format(" OR ".join(
                map(lambda v: operators_to_sql["is" if v == "null" else operator].format(field), vals)
            ))
        )
        values += map(
            lambda v: None if v == "null" else operators_to_sql_values[operator].format(v), vals)

    return conditions, values


# Parse path, query params, SQL conditions and values from URL
#
# Example:
#   /runs?flow_id=HelloFlow&status=running
#
#   -> Path: /runs
#   -> Query: MultiDict('flow_id': 'HelloFlow', 'status': 'completed')
#   -> Conditions: ["(flow_id = %s)", "(status = %s)"]
#   -> Values: ["HelloFlow", "Completed"]
def resource_conditions(fullpath: str = None) -> Tuple[str, MultiDict, List[str], List]:
    parsedurl = urlsplit(fullpath)
    query = MultiDict(parse_qsl(parsedurl.query))

    filter_fn = filter_from_conditions_query_dict(query, allowed_keys=None)

    return parsedurl.path, query, filter_fn


async def find_records(request: web.BaseRequest, async_table=None, initial_conditions: List[str] = [], initial_values=[],
                       initial_order: List[str] = [], allowed_order: List[str] = [], allowed_group: List[str] = [],
                       allowed_filters: List[str] = [], postprocess: Callable[[DBResponse], DBResponse] = None,
                       fetch_single=False, enable_joins=False, overwrite_select_from: str = None):
    page, limit, offset, order, groups, group_limit = pagination_query(
        request,
        allowed_order=allowed_order,
        allowed_group=allowed_group)

    builtin_conditions, builtin_vals = builtin_conditions_query(request)
    custom_conditions, custom_vals = custom_conditions_query(
        request,
        allowed_keys=allowed_filters)

    conditions = initial_conditions + builtin_conditions + custom_conditions
    values = initial_values + builtin_vals + custom_vals
    ordering = (initial_order or []) + (order or [])
    benchmark = query_param_enabled(request, "benchmark")
    invalidate_cache = query_param_enabled(request, "invalidate")

    results, pagination, benchmark_result = await async_table.find_records(
        conditions=conditions, values=values, limit=limit, offset=offset,
        order=ordering if len(ordering) > 0 else None, groups=groups, group_limit=group_limit,
        fetch_single=fetch_single, enable_joins=enable_joins,
        expanded=True,
        postprocess=postprocess,
        invalidate_cache=invalidate_cache,
        benchmark=benchmark,
        overwrite_select_from=overwrite_select_from
    )

    if fetch_single:
        status, res = format_response(request, results)
    else:
        status, res = format_response_list(request, results, pagination, page)

    if benchmark_result:
        res["benchmark_result"] = benchmark_result

    return web_response(status, res)


def query_param_enabled(request: web.BaseRequest, name: str) -> bool:
    """Parse boolean query parameter and return enabled status"""
    return request.query.get(name, False) in ['True', 'true', '1', "t"]


class TTLQueue:
    def __init__(self, ttl_in_seconds: int):
        self._ttl: int = ttl_in_seconds
        self._queue = deque()

    async def append(self, value: any):
        self._queue.append((time.time(), value))
        await self.discard_expired_values()

    async def discard_expired_values(self):
        cutoff_time = time.time() - self._ttl
        try:
            while self._queue[0][0] < cutoff_time:
                self._queue.popleft()
        except IndexError:
            pass

    async def values(self):
        await self.discard_expired_values()
        return self._queue

    async def values_since(self, since_epoch: int):
        return [value for value in await self.values() if value[0] >= since_epoch]


def get_pathspec_from_request(request: MultiDict) -> Tuple[str, str, str, str, Optional[str]]:
    """extract relevant resource id's from the request

    Returns
    -------
    flow_id, run_number, step_name, task_id, attempt_id
    """
    flow_id = request.match_info.get("flow_id")
    run_number = request.match_info.get("run_number")
    step_name = request.match_info.get("step_name")
    task_id = request.match_info.get("task_id")
    attempt_id = request.query.get("attempt_id", None)

    return flow_id, run_number, step_name, task_id, attempt_id


# Postprocess functions also accept a keyword argument "invalidate_cache"
Postprocess = Callable[[DBResponse], DBResponse]


def postprocess_chain(postprocess_list: List[Optional[Postprocess]]) -> Optional[Postprocess]:
    if not postprocess_list:
        return None

    async def _chained(input_db_response: DBResponse, invalidate_cache: bool = False) -> DBResponse:
        result = input_db_response
        for _postprocess in postprocess_list:
            if _postprocess is None:
                continue
            if iscoroutinefunction(_postprocess):
                result = await _postprocess(result, invalidate_cache=invalidate_cache)
            else:
                result = _postprocess(result, invalidate_cache=invalidate_cache)
        return result
    return _chained


def apply_run_tags_postprocess(flow_id, run_number, run_table_postgres):
    async def _postprocess(db_response: DBResponse, invalidate_cache=False):
        return await apply_run_tags_to_db_response(flow_id, run_number, run_table_postgres, db_response)
    return _postprocess


@web.middleware
async def allow_get_requests_only(request, handler):
    """
    Only allow GET request, otherwise raise 405 Method Not Allowed.
    """
    if request.method != 'GET':
        raise web.HTTPMethodNotAllowed(method=request.method, allowed_methods=['GET'])
    return await handler(request)

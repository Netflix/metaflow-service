import math
import os
from asyncio import iscoroutinefunction
from typing import Callable, List, Tuple

import psycopg2
import psycopg2.extras
from services.data.db_utils import (DBPagination, DBResponse,
                                    aiopg_exception_handling)
from services.data.postgres_async_db import WAIT_TIME
from services.data.postgres_async_db import \
    AsyncPostgresTable as MetadataAsyncPostgresTable

# Heartbeat check interval. Add margin in case of client-server communication delays, before marking a heartbeat stale.
HEARTBEAT_THRESHOLD = int(os.environ.get("HEARTBEAT_THRESHOLD", WAIT_TIME * 6))
# Time before a run without heartbeat will be marked as failed, if it is decisively not running, or completed.
# Default 6 days (in milliseconds)
OLD_RUN_FAILURE_CUTOFF_TIME = int(os.environ.get("OLD_RUN_FAILURE_CUTOFF_TIME", 60 * 60 * 24 * 1000 * 6))
# Time before a run with a heartbeat will be considered inactive (and thus failed).
# Default to 6 days (in seconds)
RUN_INACTIVE_CUTOFF_TIME = int(os.environ.get("RUN_INACTIVE_CUTOFF_TIME", 60 * 60 * 24 * 6))


class AsyncPostgresTable(MetadataAsyncPostgresTable):
    """
    Base Table class that inherits common behavior from services.data.postgres_async_db module, including
        - table creation and schema configuration
        - table trigger setup
        - common query functions

    UI Service specific features
    ----------------------------
        - find_records() that supports grouping by column, and postprocessing of results with a callable
        - query benchmarking
        - constants for query thresholds related to heartbeats.
    """
    db = None
    table_name = None
    schema_version = MetadataAsyncPostgresTable.schema_version
    keys: List[str] = []
    primary_keys: List[str] = None
    trigger_keys: List[str] = None
    ordering: List[str] = None
    joins: List[str] = None
    select_columns: List[str] = keys
    join_columns: List[str] = None
    _filters = None
    _row_type = None

    async def get_records(self, filter_dict={}, fetch_single=False,
                          ordering: List[str] = None, limit: int = 0, expanded=False) -> DBResponse:
        conditions = []
        values = []
        for col_name, col_val in filter_dict.items():
            conditions.append("{} = %s".format(col_name))
            values.append(col_val)

        response, *_ = await self.find_records(conditions=conditions, values=values, fetch_single=fetch_single,
                                               order=ordering, limit=limit, expanded=expanded)
        return response

    async def find_records(self, conditions: List[str] = None, values=[], fetch_single=False,
                           limit: int = 0, offset: int = 0, order: List[str] = None, groups: List[str] = None,
                           group_limit: int = 10, expanded=False, enable_joins=False,
                           postprocess: Callable[[DBResponse], DBResponse] = None,
                           invalidate_cache=False, benchmark: bool = False,
                           overwrite_select_from: str = None
                           ) -> Tuple[DBResponse, DBPagination]:
        # Grouping not enabled
        if groups is None or len(groups) == 0:
            sql_template = """
            SELECT * FROM (
                SELECT
                    {keys}
                FROM {table_name}
                {joins}
            ) T
            {where}
            {order_by}
            {limit}
            {offset}
            """

            select_sql = sql_template.format(
                keys=",".join(
                    self.select_columns + (self.join_columns if enable_joins and self.join_columns else [])),
                table_name=overwrite_select_from if overwrite_select_from else self.table_name,
                joins=" ".join(
                    self.joins) if enable_joins and self.joins else "",
                where="WHERE {}".format(" AND ".join(
                    conditions)) if conditions else "",
                order_by="ORDER BY {}".format(
                    ", ".join(order)) if order else "",
                limit="LIMIT {}".format(limit) if limit else "",
                offset="OFFSET {}".format(offset) if offset else ""
            ).strip()
        else:  # Grouping enabled
            # NOTE: we are performing a DISTINCT select on the group labels before the actual window function, to limit the set
            # being queried. Without this restriction the query planner kept hitting the whole table contents, resulting in very slow queries.

            # Query for groups matching filters.
            groups_sql_template = """
            SELECT DISTINCT ON({group_by}) * FROM (
                SELECT
                    {keys}
                FROM {table_name}
                {joins}
            ) T
            {where}
            ORDER BY {group_by} ASC NULLS LAST
            {limit}
            {offset}
            """

            groups_sql = groups_sql_template.format(
                keys=",".join(
                    self.select_columns + (self.join_columns if enable_joins and self.join_columns else [])),
                table_name=self.table_name,
                joins=" ".join(
                    self.joins) if enable_joins and self.joins is not None else "",
                where="WHERE {}".format(" AND ".join(
                    conditions)) if conditions else "",
                group_by=", ".join(groups),
                limit="LIMIT {}".format(limit) if limit else "",
                offset="OFFSET {}".format(offset) if offset else ""
            ).strip()

            group_results, _ = await self.execute_sql(select_sql=groups_sql, values=values, fetch_single=fetch_single,
                                                      expanded=expanded, limit=limit, offset=offset)
            if len(group_results.body) == 0:
                # Return early if no groups match the query.
                return group_results, None, None

            # construct the group_where clause.
            group_label_selects = []
            for group in groups:
                _group_values = [row[group.strip("\"")] for row in group_results.body]
                if len(_group_values) > 0:
                    _clause = "{group} = ANY(%s)".format(group=group)
                    group_label_selects.append(_clause)
                    values.append(_group_values)

            # Query for group content. Restricted by groups received from previous query.
            sql_template = """
            SELECT * FROM (
                SELECT
                    *, ROW_NUMBER() OVER(PARTITION BY {group_by} {order_by})
                FROM (
                    SELECT
                        {keys}
                    FROM {table_name}
                    {joins}
                ) T
                {where}
            ) G
            {group_where}
            """

            select_sql = sql_template.format(
                keys=",".join(
                    self.select_columns + (self.join_columns if enable_joins and self.join_columns else [])),
                table_name=overwrite_select_from if overwrite_select_from else self.table_name,
                joins=" ".join(
                    self.joins) if enable_joins and self.joins is not None else "",
                where="WHERE {}".format(" AND ".join(
                    conditions)) if conditions else "",
                group_by=", ".join(groups),
                order_by="ORDER BY {}".format(
                    ", ".join(order)) if order else "",
                group_where="""
                    WHERE {group_limit} {group_selects}
                """.format(
                    group_limit="row_number <= {} AND ".format(group_limit) if group_limit else "",
                    group_selects=" AND ".join(group_label_selects)
                )
            ).strip()

        # Run benchmarking on query if requested
        benchmark_results = None
        if benchmark:
            benchmark_results = await self.benchmark_sql(
                select_sql=select_sql, values=values, fetch_single=fetch_single,
                expanded=expanded, limit=limit, offset=offset
            )

        result, pagination = await self.execute_sql(select_sql=select_sql, values=values, fetch_single=fetch_single,
                                                    expanded=expanded, limit=limit, offset=offset)
        # Modify the response after the fetch has been executed
        if postprocess is not None:
            if iscoroutinefunction(postprocess):
                result = await postprocess(result, invalidate_cache=invalidate_cache)
            else:
                result = postprocess(result, invalidate_cache=invalidate_cache)

        return result, pagination, benchmark_results

    async def benchmark_sql(self, select_sql: str, values=[], fetch_single=False,
                            expanded=False, limit: int = 0, offset: int = 0):
        "Benchmark and log a given SQL query with EXPLAIN ANALYZE"
        try:
            with (
                await self.db.pool.cursor(
                    cursor_factory=psycopg2.extras.DictCursor
                )
            ) as cur:
                # Run EXPLAIN ANALYZE on query and log the results.
                benchmark_sql = "EXPLAIN ANALYZE {}".format(select_sql)
                await cur.execute(benchmark_sql, values)

                records = await cur.fetchall()
                rows = []
                for record in records:
                    rows.append(record[0])
                return "\n".join(rows)
        except (Exception, psycopg2.DatabaseError):
            self.db.logger.exception("Query Benchmarking failed")
            return None

    async def execute_sql(self, select_sql: str, values=[], fetch_single=False,
                          expanded=False, limit: int = 0, offset: int = 0, serialize: bool = True) -> Tuple[DBResponse, DBPagination]:
        try:
            with (
                await self.db.pool.cursor(
                    cursor_factory=psycopg2.extras.DictCursor
                )
            ) as cur:
                await cur.execute(select_sql, values)

                rows = []
                records = await cur.fetchall()
                if serialize:
                    for record in records:
                        # pylint-initial-ignore: Lack of __init__ makes this too hard for pylint
                        # pylint: disable=not-callable
                        row = self._row_type(**record)
                        rows.append(row.serialize(expanded))
                else:
                    rows = records

                count = len(rows)

                # Will raise IndexError in case fetch_single=True and there's no results
                body = rows[0] if fetch_single else rows

                pagination = DBPagination(
                    limit=limit,
                    offset=offset,
                    count=count,
                    page=math.floor(int(offset) / max(int(limit), 1)) + 1,
                )

                cur.close()
                return DBResponse(response_code=200, body=body), pagination
        except IndexError as error:
            return aiopg_exception_handling(error), None
        except (Exception, psycopg2.DatabaseError) as error:
            self.db.logger.exception("Exception occured")
            return aiopg_exception_handling(error), None

    async def get_tags(self, conditions: List[str] = None, values=[], limit: int = 0, offset: int = 0):
        sql_template = """
        SELECT DISTINCT tag
        FROM (
            SELECT JSONB_ARRAY_ELEMENTS_TEXT(tags||system_tags) AS tag
            FROM {table_name}
        ) AS t
        {conditions}
        {limit}
        {offset}
        """
        select_sql = sql_template.format(
            table_name=self.table_name,
            conditions="WHERE {}".format(" AND ".join(conditions)) if conditions else "",
            limit="LIMIT {}".format(limit) if limit else "",
            offset="OFFSET {}".format(offset) if offset else "",
        )

        res, pagination = await self.execute_sql(select_sql=select_sql, values=values, serialize=False)

        # process the unserialized DBResponse
        _body = [row[0] for row in res.body]

        return DBResponse(res.response_code, _body), pagination

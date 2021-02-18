import psycopg2
import psycopg2.extras
import os
import math
from services.data.postgres_async_db import (
    PostgresUtils,
    AsyncPostgresTable as MetadataAsyncPostgresTable
)
from services.data.db_utils import DBResponse, DBPagination, aiopg_exception_handling, \
    get_db_ts_epoch_str, translate_run_key, translate_task_key
from services.utils import DBConfiguration
from typing import List, Callable
from asyncio import iscoroutinefunction

WAIT_TIME = 10
# Heartbeat check interval. Add margin in case of client-server communication delays, before marking a heartbeat stale.
HEARTBEAT_THRESHOLD = int(os.environ.get("HEARTBEAT_THRESHOLD", WAIT_TIME * 6))
OLD_RUN_FAILURE_CUTOFF_TIME = int(os.environ.get("OLD_RUN_FAILURE_CUTOFF_TIME", 60 * 60 * 24 * 1000 * 14))  # default 2 weeks (in milliseconds)


class AsyncPostgresTable(object):
    db = None
    table_name = None
    schema_version = MetadataAsyncPostgresTable.schema_version
    keys: List[str] = []
    primary_keys: List[str] = None
    ordering: List[str] = None
    joins: List[str] = None
    select_columns: List[str] = keys
    join_columns: List[str] = None
    _command = None
    _filters = None
    _row_type = None

    def __init__(self, db):
        self.db = db
        if self.table_name is None or self._command is None:
            raise NotImplementedError(
                "need to specify table name and create command")

    async def _init(self, create_tables: bool, create_triggers: bool):
        if create_tables:
            await PostgresUtils.create_if_missing(self.db, self.table_name, self._command)
        if create_triggers:
            self.db.logger.info(
                "Create notify trigger for {table_name}\n   Keys: {keys}".format(
                    table_name=self.table_name, keys=self.primary_keys))
            await PostgresUtils.trigger_notify(db=self.db, table_name=self.table_name, keys=self.primary_keys)

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
                           benchmark: bool = False, overwrite_select_from: str = None
                           ) -> (DBResponse, DBPagination):
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
            {group_limit}
            ORDER BY {group_by} ASC NULLS LAST
            {limit}
            {offset}
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
                group_limit="WHERE row_number <= {}".format(
                    group_limit) if group_limit else "",
                limit="LIMIT {}".format(limit) if limit else "",
                offset="OFFSET {}".format(offset) if offset else ""
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
                result = await postprocess(result)
            else:
                result = postprocess(result)

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
                          expanded=False, limit: int = 0, offset: int = 0) -> (DBResponse, DBPagination):
        try:
            with (
                await self.db.pool.cursor(
                    cursor_factory=psycopg2.extras.DictCursor
                )
            ) as cur:
                await cur.execute(select_sql, values)

                rows = []
                records = await cur.fetchall()
                for record in records:
                    row = self._row_type(**record)
                    rows.append(row.serialize(expanded))

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

    async def get_tags(self):
        sql_template = "SELECT DISTINCT tag FROM (SELECT JSONB_ARRAY_ELEMENTS(tags||system_tags) AS tag FROM {table_name}) AS t"
        select_sql = sql_template.format(table_name=self.table_name)

        try:
            with (
                await self.db.pool.cursor(
                    cursor_factory=psycopg2.extras.DictCursor
                )
            ) as cur:
                await cur.execute(select_sql)

                tags = []
                records = await cur.fetchall()
                for record in records:
                    tags += record
                cur.close()
                return DBResponse(response_code=200, body=tags)
        except (Exception, psycopg2.DatabaseError) as error:
            self.db.logger.exception("Exception occured")
            return aiopg_exception_handling(error)

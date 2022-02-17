from typing import List
from .base import AsyncPostgresTable
from ..models import FlowRow
from services.data.db_utils import DBResponse, DBPagination
# use schema constants from the .data module to keep things consistent
from services.data.postgres_async_db import AsyncFlowTablePostgres as MetadataFlowTable


class AsyncFlowTablePostgres(AsyncPostgresTable):
    table_name = MetadataFlowTable.table_name
    keys = MetadataFlowTable.keys
    primary_keys = MetadataFlowTable.primary_keys
    trigger_keys = MetadataFlowTable.trigger_keys
    select_columns = keys
    _row_type = FlowRow

    async def get_flow_ids(self, conditions: List[str] = [],
                           values: List[str] = [], limit: int = 0, offset: int = 0) -> (DBResponse, DBPagination):
        """
        Get a paginated set of flow ids.

        Parameters
        ----------
        conditions : List[str]
            list of conditions to pass the sql execute, with %s placeholders for values
        values : List[str]
            list of values to be passed for the sql execute.
        limit : int (optional) (default 0)
            limit for the number of results
        offset : int (optional) (default 0)
            offset for the results.

        Returns
        -------
        (DBResponse, DBPagination)
        """
        sql_template = """
            SELECT DISTINCT flow_id
            FROM {table_name}
            {conditions}
            {limit}
            {offset}
            """
        select_sql = sql_template.format(
            table_name=self.table_name,
            keys=",".join(self.select_columns),
            conditions=("WHERE {}".format(" AND ".join(conditions)) if conditions else ""),
            limit="LIMIT {}".format(limit) if limit else "",
            offset="OFFSET {}".format(offset) if offset else ""
        )

        res, pag = await self.execute_sql(select_sql=select_sql, values=values, fetch_single=False,
                                          expanded=False,
                                          limit=limit, offset=offset, serialize=False)
        # process the unserialized DBResponse
        _body = [row[0] for row in res.body]

        return DBResponse(res.response_code, _body), pag

from typing import List
from .base import AsyncPostgresTable
from .task import AsyncTaskTablePostgres
from ..models import ArtifactRow
# use schema constants from the .data module to keep things consistent
from services.data.postgres_async_db import AsyncArtifactTablePostgres as MetadataArtifactTable
from services.data.db_utils import translate_run_key, DBResponse, DBPagination


class AsyncArtifactTablePostgres(AsyncPostgresTable):
    _row_type = ArtifactRow
    table_name = MetadataArtifactTable.table_name
    task_table_name = AsyncTaskTablePostgres.table_name
    ordering = ["attempt_id DESC"]
    keys = MetadataArtifactTable.keys
    primary_keys = MetadataArtifactTable.primary_keys
    trigger_keys = MetadataArtifactTable.trigger_keys
    select_columns = keys
    _command = MetadataArtifactTable._command

    async def get_run_parameter_artifacts(self, flow_name, run_number, postprocess=None, invalidate_cache=False):
        run_id_key, run_id_value = translate_run_key(run_number)

        # '_parameters' step has all the parameters as artifacts. only pick the
        # public parameters (no underscore prefix)
        return await self.find_records(
            conditions=[
                "flow_id = %s",
                "{run_id_key} = %s".format(run_id_key=run_id_key),
                "step_name = %s",
                "name NOT LIKE %s",
                "name <> %s",
                "name <> %s"
            ],
            values=[
                flow_name,
                run_id_value,
                "_parameters",
                r"\_%",
                "name",  # exclude the 'name' parameter as this always exists, and contains the FlowName
                "script_name"  # exclude the internally used 'script_name' parameter.
            ],
            fetch_single=False,
            expanded=True,
            postprocess=postprocess,
            invalidate_cache=invalidate_cache
        )

    async def get_artifact_names(self, conditions: List[str] = [],
                                 values: List[str] = [], limit: int = 0, offset: int = 0) -> (DBResponse, DBPagination):
        """
        Get a paginated set of artifact names.

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
            SELECT name FROM (
                SELECT DISTINCT name, flow_id, run_number, run_id
                FROM {table_name}
            ) T
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

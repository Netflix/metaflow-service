from typing import List
from .base import AsyncPostgresTable
from ..models import StepRow
from services.data.db_utils import DBResponse, DBPagination
# use schema constants from the .data module to keep things consistent
from services.data.postgres_async_db import (
    AsyncRunTablePostgres as MetadataRunTable,
    AsyncStepTablePostgres as MetadataStepTable,
    AsyncTaskTablePostgres as MetadataTaskTable,
    AsyncArtifactTablePostgres as MetadataArtifactTable
)


class AsyncStepTablePostgres(AsyncPostgresTable):
    step_dict = {}
    run_to_step_dict = {}
    _row_type = StepRow
    table_name = MetadataStepTable.table_name
    keys = MetadataStepTable.keys
    primary_keys = MetadataStepTable.primary_keys
    trigger_keys = MetadataStepTable.trigger_keys
    run_table_name = MetadataRunTable.table_name
    _command = MetadataStepTable._command
    task_table_name = MetadataTaskTable.table_name
    artifact_table_name = MetadataArtifactTable.table_name
    joins = [
        """
        LEFT JOIN LATERAL (
            SELECT last_heartbeat_ts as heartbeat_ts
            FROM {task_table}
            WHERE {table_name}.flow_id={task_table}.flow_id
            AND {table_name}.run_number={task_table}.run_number
            AND {table_name}.step_name={task_table}.step_name
            ORDER BY last_heartbeat_ts DESC
            LIMIT 1
        ) AS latest_task_hb ON true
        """.format(
            table_name=table_name,
            task_table=task_table_name
        ),
        """
        LEFT JOIN LATERAL (
            SELECT ts_epoch as ts_epoch
            FROM {artifact_table}
            WHERE {table_name}.flow_id={artifact_table}.flow_id
            AND {table_name}.run_number={artifact_table}.run_number
            AND {table_name}.step_name={artifact_table}.step_name
            AND {artifact_table}.name = '_task_ok'
            ORDER BY
                ts_epoch DESC
            LIMIT 1
        ) AS latest_task_ok ON true
        """.format(
            table_name=table_name,
            artifact_table=artifact_table_name
        )
    ]

    @property
    def select_columns(self):
        # NOTE: We must use a function scope in order to be able to access the table_name variable for list comprehension.
        return ["{table_name}.{col} AS {col}".format(table_name=self.table_name, col=k) for k in self.keys]

    join_columns = [
        """
        GREATEST(
            latest_task_ok.ts_epoch,
            latest_task_hb.heartbeat_ts*1000
        ) - {table_name}.ts_epoch as duration
        """.format(
            table_name=table_name
        )
    ]

    async def get_step_names(self, conditions: List[str] = [],
                             values: List[str] = [], limit: int = 0, offset: int = 0) -> (DBResponse, DBPagination):
        """
        Get a paginated set of step names.

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
            SELECT step_name FROM (
                SELECT DISTINCT step_name, flow_id, run_number, run_id
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

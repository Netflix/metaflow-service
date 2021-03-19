from .base import AsyncPostgresTable
from .task import AsyncTaskTablePostgres
from ..models import ArtifactRow
# use schema constants from the .data module to keep things consistent
from services.data.postgres_async_db import AsyncArtifactTablePostgres as MetadataArtifactTable
from services.data.db_utils import translate_run_key


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

    async def get_artifact_locations_for_run_numbers(self, run_numbers=[]):
        # do not touch DB if no run_numbers were given.
        if len(run_numbers) == 0:
            return []
        # run_numbers are bigint, so cast the conditions array to the correct type.
        run_id_cond = "run_number = ANY (array[%s]::bigint[])"

        artifact_loc_cond = "ds_type = %s"
        artifact_loc = "s3"
        _records, *_ = await self.find_records(
            conditions=[run_id_cond, artifact_loc_cond],
            values=[run_numbers, artifact_loc],
            expanded=True
        )

        # be sure to return a list of unique locations
        return list(frozenset(artifact['location'] for artifact in _records.body if 'location' in artifact))

    async def get_run_parameter_artifacts(self, flow_name, run_number, postprocess=None):
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
            postprocess=postprocess
        )

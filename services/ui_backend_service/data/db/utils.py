
from services.data.db_utils import DBResponse
from services.ui_backend_service.data.db.postgres_async_db import AsyncPostgresDB


async def get_run_dag_data(db: AsyncPostgresDB, flow_name: str, run_number: str) -> DBResponse:
    """
    Fetches either a _graph_info artifact, or a code-package metadata entry if the artifact is missing.
    Used to determine whether a run can display a DAG.
    """
    db_response = await db.artifact_table_postgres.get_run_graph_info_artifact(flow_name, run_number)
    if not db_response.response_code == 200:
        # Try to look for codepackage if graph artifact is missing
        db_response = await db.metadata_table_postgres.get_run_codepackage_metadata(flow_name, run_number)

    return db_response

"""Unit tests for the run-tags join optimization.

The step/task/artifact read API returns the ancestral run's tags, not the row's
own stored tags. Handler-facing reads opt in via with_run_tags=True, embedding
a LEFT JOIN to runs_v3 in the query -- replacing the per-request get_run +
deepcopy in apply_run_tags_to_db_response. Default DB-layer reads are unchanged.

with_run_tags travels as a call argument, never as instance state: table objects
are shared across concurrent requests.
"""

import asyncio
from unittest import mock

from services.data.db_utils import DBResponse, DBPagination
from services.data.postgres_async_db import (
    AsyncPostgresTable,
    AsyncStepTablePostgres,
    AsyncTaskTablePostgres,
    AsyncArtifactTablePostgres,
    _run_tags_join,
    _run_tag_qualified_columns,
    RUN_TABLE_NAME,
    STEP_TABLE_NAME,
    TASK_TABLE_NAME,
)

_EMPTY_PAGE = (
    DBResponse(response_code=200, body=[]),
    DBPagination(limit=0, offset=0, count=0, page=1, next_cursor_record=None),
)


def test_join_references_runs_table():
    join = _run_tags_join(STEP_TABLE_NAME)
    assert RUN_TABLE_NAME in join
    assert "run_for_tags" in join


def test_join_qualifies_both_tables():
    join = _run_tags_join(TASK_TABLE_NAME)
    assert "tasks_v3.flow_id = run_for_tags.flow_id" in join
    assert "tasks_v3.run_number = run_for_tags.run_number" in join


def test_qualified_columns_pull_tags_from_join():
    keys = ["flow_id", "run_number", "tags", "system_tags"]
    col_sql = ", ".join(_run_tag_qualified_columns(STEP_TABLE_NAME, keys))
    assert "steps_v3.flow_id AS flow_id" in col_sql
    assert "steps_v3.run_number AS run_number" in col_sql
    assert "run_for_tags.tags AS tags" in col_sql
    assert "run_for_tags.system_tags AS system_tags" in col_sql


def test_default_select_columns_are_the_plain_keys():
    # Default DB-layer reads must stay byte-identical to the pre-optimization
    # behavior: unqualified keys, no run-tags redirection.
    for table_cls in (
        AsyncStepTablePostgres,
        AsyncTaskTablePostgres,
        AsyncArtifactTablePostgres,
    ):
        table = table_cls.__new__(table_cls)
        columns = table._resolve_select_columns(with_run_tags=False)
        assert list(columns) == list(table.keys)
        assert columns, "default reads must still select something"


def test_opted_in_select_columns_redirect_tags():
    table = AsyncStepTablePostgres.__new__(AsyncStepTablePostgres)
    col_sql = ", ".join(table._resolve_select_columns(with_run_tags=True))
    assert "run_for_tags.tags AS tags" in col_sql
    assert "run_for_tags.system_tags AS system_tags" in col_sql
    assert "steps_v3.step_name AS step_name" in col_sql


def test_column_choice_is_a_pure_function_of_the_argument():
    # No instance state: the same table object must answer both ways, in any
    # order, without one call influencing the next.
    table = AsyncStepTablePostgres.__new__(AsyncStepTablePostgres)
    for flag in (True, False, True, False, False, True):
        columns = table._resolve_select_columns(with_run_tags=flag)
        assert ("run_for_tags" in ", ".join(columns)) is flag
    assert not hasattr(table, "_with_run_tags")


async def test_get_records_passes_the_flag_down():
    table = AsyncTaskTablePostgres.__new__(AsyncTaskTablePostgres)
    with mock.patch.object(
        AsyncPostgresTable, "find_records", new_callable=mock.AsyncMock
    ) as mock_super:
        mock_super.return_value = _EMPTY_PAGE
        await table.get_records(filter_dict={"flow_id": "f"}, with_run_tags=True)
        assert mock_super.call_args.kwargs["with_run_tags"] is True


async def test_get_records_default_does_not_join():
    table = AsyncStepTablePostgres.__new__(AsyncStepTablePostgres)
    with mock.patch.object(
        AsyncPostgresTable, "find_records", new_callable=mock.AsyncMock
    ) as mock_super:
        mock_super.return_value = _EMPTY_PAGE
        await table.get_records(filter_dict={"flow_id": "f"})
        assert mock_super.call_args.kwargs["with_run_tags"] is False


async def test_get_filtered_tasks_paginated_honors_the_flag():
    table = AsyncTaskTablePostgres.__new__(AsyncTaskTablePostgres)
    with mock.patch.object(
        AsyncPostgresTable, "find_records", new_callable=mock.AsyncMock
    ) as mock_super:
        mock_super.return_value = _EMPTY_PAGE
        await table.get_filtered_tasks_paginated(
            conditions=["flow_id = %s"], values=["f"], limit=50, with_run_tags=True
        )
        assert mock_super.call_args.kwargs["with_run_tags"] is True

        await table.get_filtered_tasks_paginated(
            conditions=["flow_id = %s"], values=["f"], limit=50
        )
        assert mock_super.call_args.kwargs["with_run_tags"] is False


async def test_concurrent_reads_on_one_table_do_not_affect_each_other():
    """One table object serves every request, so an opted-in read and a default
    read must not be able to see each other's setting.

    The interleaving is forced rather than hoped for: the opted-in read parks at
    the DB round-trip -- the one real suspension point in a read -- while a
    default read runs start to finish, and only then resumes. Each call's SQL is
    captured where it is handed to the driver, so what is asserted is the query
    that would actually have been sent.
    """
    table = AsyncStepTablePostgres.__new__(AsyncStepTablePostgres)
    captured = {}
    opted_in_parked = asyncio.Event()
    default_finished = asyncio.Event()

    async def read(name, flag):
        async def fake_execute_sql(**kwargs):
            captured[name] = kwargs["select_sql"]
            if name == "opted_in":
                opted_in_parked.set()
                await default_finished.wait()
            return _EMPTY_PAGE

        table.execute_sql = fake_execute_sql
        return await table.get_records(filter_dict={"flow_id": "f"}, with_run_tags=flag)

    opted_in = asyncio.create_task(read("opted_in", True))
    await opted_in_parked.wait()
    await read("default", False)
    # The default read has finished while the opted-in read is still mid-flight;
    # without this the test would also pass if the two never actually overlapped.
    assert not opted_in.done()
    default_finished.set()
    await opted_in

    assert "run_for_tags.tags AS tags" in captured["opted_in"]
    assert "run_for_tags" not in captured["default"]
    # and the shared instance carries nothing over from either call
    assert not hasattr(table, "_with_run_tags")


async def test_artifact_paginated_sql_embeds_join_only_when_opted_in():
    """The custom paginated SQL templates bypass find_records, so the join must
    be embedded in the template itself -- and only when with_run_tags is set."""
    table = AsyncArtifactTablePostgres.__new__(AsyncArtifactTablePostgres)
    with mock.patch.object(
        AsyncArtifactTablePostgres, "execute_sql", new_callable=mock.AsyncMock
    ) as mock_exec:
        mock_exec.return_value = _EMPTY_PAGE

        await table.get_artifacts_in_runs_paginated(flow_id="f", run_id="1", limit=50)
        default_sql = mock_exec.call_args.kwargs["select_sql"]
        assert "run_for_tags" not in default_sql

        await table.get_artifacts_in_runs_paginated(
            flow_id="f", run_id="1", limit=50, with_run_tags=True
        )
        join_sql = mock_exec.call_args.kwargs["select_sql"]
        assert "run_for_tags.tags AS tags" in join_sql
        assert "artifact_v3.flow_id" in join_sql

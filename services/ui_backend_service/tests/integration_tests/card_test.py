import pytest
from unittest import mock
from .utils import (
    cli, db,
    add_flow, add_run, add_step, add_task,
    _test_list_resources
)
pytestmark = [pytest.mark.integration_tests]


async def test_task_not_found_response(cli, db):
    _, data = await _test_list_resources(cli, db, "/flows/DummyFlow/runs/123/steps/start/tasks/456/cards", 404, None)
    assert data == []

    resp = await cli.get("/flows/DummyFlow/runs/123/steps/start/tasks/456/cards/abcd123")
    body = await resp.text()

    assert resp.status == 404
    assert "Task" in body and "not found" in body


async def test_card_not_returned(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="step", run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body
    _task = (await add_task(db,
                            flow_id=_step.get("flow_id"),
                            step_name=_step.get("step_name"),
                            run_number=_step.get("run_number"),
                            run_id=_step.get("run_id"))).body

    async def get_cards_for_task(cache_client, task, invalidate_cache=False):
        return None

    with mock.patch("services.ui_backend_service.api.card.get_cards_for_task", new=get_cards_for_task):
        _, data = await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/cards".format(**_task), 200, None)
        assert data == []

        resp = await cli.get("/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/cards/abcd1234".format(**_task))
        body = await resp.text()
        assert resp.status == 404
        assert "Card" in body and "not found" in body


async def test_cards_list_paginated_response(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="step", run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body
    _task = (await add_task(db,
                            flow_id=_step.get("flow_id"),
                            step_name=_step.get("step_name"),
                            run_number=_step.get("run_number"),
                            run_id=_step.get("run_id"))).body

    async def get_cards_for_task(cache_client, task, invalidate_cache=False):
        return {
            "abc": {"id": 1, "type": "default", "html": "content"},
            "def": {"id": 2, "type": "default", "html": "content2"}
        }

    with mock.patch("services.ui_backend_service.api.card.get_cards_for_task", new=get_cards_for_task):
        _, data = await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/cards".format(**_task), 200, None)
        assert data == [
            {"id": 1, "type": "default", "hash": "abc"},
            {"id": 2, "type": "default", "hash": "def"}
        ]

        _, data = await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/cards?_limit=1".format(**_task), 200, None)
        assert data == [{"id": 1, "type": "default", "hash": "abc"}]

        _, data = await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/cards?_limit=1&_page=2".format(**_task), 200, None)
        assert data == [{"id": 2, "type": "default", "hash": "def"}]


async def test_card_content_for_task(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="step", run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body
    _task = (await add_task(db,
                            flow_id=_step.get("flow_id"),
                            step_name=_step.get("step_name"),
                            run_number=_step.get("run_number"),
                            run_id=_step.get("run_id"))).body

    async def get_cards_for_task(cache_client, task, invalidate_cache=False):
        return {
            "abc": {"id": 1, "type": "default", "html": "content"},
            "def": {"id": 2, "type": "default", "html": "content2"}
        }

    with mock.patch("services.ui_backend_service.api.card.get_cards_for_task", new=get_cards_for_task):
        resp = await cli.get("/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/cards/hij123".format(**_task))
        assert resp.status == 404

        resp = await cli.get("/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/cards/abc".format(**_task))
        body = await resp.text()
        assert resp.status == 200
        assert body == "content"

        resp = await cli.get("/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/cards/def".format(**_task))
        body = await resp.text()
        assert resp.status == 200
        assert body == "content2"

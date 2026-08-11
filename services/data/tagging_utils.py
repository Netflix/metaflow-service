from services.data.db_utils import DBResponse
import copy


async def apply_run_tags_to_db_response(
    flow_id, run_number, run_table_postgres, db_response: DBResponse
) -> DBResponse:
    """
    We want read APIs to return steps, tasks and artifact objects with tags
    and system_tags set to their ancestral Run.

    This is a prerequisite for supporting Run-based tag mutation.

    Kept rather than removed, now that metadata_service reads get run tags from
    the runs_v3 join instead (see _RunTagsJoinMixin in postgres_async_db.py).
    Two callers still need the post-query form:

    1. Write paths -- step.py add_step and task.py add_task. The response there
       is built from the INSERT, not from a SELECT, so there is no query to hang
       the join off. Getting run tags any other way would mean re-fetching the
       row after writing it, which is strictly more work than this helper.
    2. ui_backend_service -- ws.py, api/utils.py and api/search.py all still
       call this on their own read paths. Those handlers do not go through the
       metadata_service read tables, so moving them to the join is a separate
       change and is deliberately out of scope here.

    The join is the faster path and should be preferred wherever a read already
    issues a SELECT; this helper covers the cases where one does not.
    """
    # we will return a modified copy of db_response
    new_db_response = copy.deepcopy(db_response)
    # Only replace tags if response code is legit
    # Object creation ought to return 201 (let's prepare for that)
    if new_db_response.response_code not in (200, 201):
        return new_db_response
    if isinstance(new_db_response.body, list):
        items_to_modify = new_db_response.body
    else:
        items_to_modify = [new_db_response.body]
    if not items_to_modify:
        return new_db_response
    # items_to_modify now references all the items we want to modify

    # The ancestral run must be successfully read from DB
    db_response_for_run = await run_table_postgres.get_run(flow_id, run_number)
    if db_response_for_run.response_code != 200:
        return DBResponse(response_code=500, body=db_response_for_run.body)
    run = db_response_for_run.body
    for item_as_dict in items_to_modify:
        item_as_dict["tags"] = run["tags"]
        item_as_dict["system_tags"] = run["system_tags"]
    return new_db_response

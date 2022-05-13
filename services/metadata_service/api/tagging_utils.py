from services.data.db_utils import DBResponse
import copy


async def apply_run_tags_to_db_response(flow_id, run_number, run_table_postgres, db_response: DBResponse) -> DBResponse:
    """
    We want read APIs to return steps, tasks and artifact objects with tags
    and system_tags set to their ancestral Run.

    This is a prerequisite for supporting Run-based tag mutation.
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
        item_as_dict['tags'] = run['tags']
        item_as_dict['system_tags'] = run['system_tags']
    return new_db_response

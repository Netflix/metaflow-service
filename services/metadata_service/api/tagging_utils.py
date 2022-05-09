from services.data.db_utils import DBResponse


async def replace_with_run_tags_in_db_response(flow_id, run_number, run_table_postgres, db_response: DBResponse) -> DBResponse:
    # Only replace tags if response code is legit
    # Object creation ought to return 201 (let's prepare for that)
    if db_response.response_code not in (200, 201):
        return db_response
    if type(db_response.body) != list:
        items_to_modify = [db_response.body]
    else:
        items_to_modify = db_response.body
    if not items_to_modify:
        return db_response
    # items_to_modify now references all the items we want to modify (in-place)
    db_response_for_run = await run_table_postgres.get_run(flow_id, run_number)
    # The ancestral run must check out
    if db_response_for_run.response_code != 200:
        return DBResponse(response_code=500, body=db_response_for_run.body)
    run = db_response_for_run.body
    for item_as_dict in items_to_modify:
        item_as_dict['tags'] = run['tags']
        item_as_dict['system_tags'] = run['system_tags']
    # db_response has been updated in-place
    return db_response

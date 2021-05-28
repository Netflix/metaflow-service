from services.data import TaskRow
from services.data.db_utils import DBResponse
from services.data.postgres_async_db import AsyncPostgresDB
from services.metadata_service.api.utils import format_response, \
    handle_exceptions
import json

import asyncio


class TagApi(object):
    lock = asyncio.Lock()

    def __init__(self, app):
        app.router.add_route(
            "POST",
            "/tags",
            self.update_tags,
        )
        self._db = AsyncPostgresDB.get_instance()

    def _get_table(self, type):
        if type == 'flow':
            return self._db.flow_table_postgres
        elif type == 'run':
            return self._db.run_table_postgres
        elif type == 'step':
            return self._db.step_table_postgres
        elif type == 'task':
            return self._db.task_table_postgres
        elif type == 'artifact':
            return self._db.artifact_table_postgres
        else:
            raise ValueError("cannot find table for type %s" % type)

    @handle_exceptions
    @format_response
    async def update_tags(self, request):
        """
        ---
        description: Update user-tags for objects
        tags:
        - Tags
        parameters:
        - name: "body"
          in: "body"
          description: "body"
          required: true
          schema:
            type: array
            items:
              type: object
              required:
                - object_type
                - id
                - tag
                - operation
              properties:
                object_type:
                  type: string
                  enum: [flow, run, step, task, artifact]
                id:
                  type: string
                operation:
                  type: string
                  enum: [add, remove]
                tag:
                  type: string
                user:
                  type: string
        produces:
        - application/json
        responses:
            "202":
                description: successful operation. Return newly registered task
            "404":
                description: not found
            "500":
                description: internal server error
        """
        body = await request.json()
        results = []
        for o in body:
            try:
                table = self._get_table(o['object_type'])
                pathspec = o['id'].split('/')
                # Do some basic verification
                if o['object_type'] == 'flow' and len(pathspec) != 1:
                    raise ValueError("invalid flow specification: %s" % o['id'])
                elif o['object_type'] == 'run' and len(pathspec) != 2:
                    raise ValueError("invalid run specification: %s" % o['id'])
                elif o['object_type'] == 'step' and len(pathspec) != 3:
                    raise ValueError("invalid step specification: %s" % o['id'])
                elif o['object_type'] == 'task' and len(pathspec) != 4:
                    raise ValueError("invalid task specification: %s" % o['id'])
                elif o['object_type'] == 'artifact' and len(pathspec) != 5:
                    raise ValueError("invalid artifact specification: %s" % o['id'])
                obj_filter = table.get_filter_dict(*pathspec)
            except ValueError as e:
                return DBResponse(response_code=400, body=json.dumps(
                    {"message": "invalid input: %s" % str(e)}))

            # Now we can get the object
            obj = await table.get_records(
                filter_dict=obj_filter, fetch_single=True, expanded=True)
            if obj.response_code != 200:
                return DBResponse(response_code=obj.response_code, body=json.dumps(
                    {"message": "could not get object %s: %s" % (o['id'], obj.body)}))

            # At this point do some checks and update the tags
            obj = obj.body
            modified = False
            if o['operation'] == 'add':
                # This is the only error we fail hard on; adding a tag that is
                # in system tag
                if o['tag'] in obj['system_tags']:
                    return DBResponse(response_code=405, body=json.dumps(
                        {"message": "tag %s is already a system tag and can't be added to %s"
                        % (o['tag'], o['id'])}))
                if o['tag'] not in obj['tags']:
                    modified = True
                    obj['tags'].append(o['tag'])
            elif o['operation'] == 'remove':
                if o['tag'] in obj['tags']:
                    modified = True
                    obj['tags'].remove(o['tag'])
            else:
                return DBResponse(response_code=400, body=json.dumps(
                    {"message": "invalid tag operation %s" % o['operation']}))
            if modified:
                # We save the value back
                result = await table.update_row(filter_dict=obj_filter, update_dict={
                    'tags': "'%s'" % json.dumps(obj['tags'])})
                if result.response_code != 200:
                    return DBResponse(response_code=result.response_code, body=json.dumps(
                        {"message": "error updating tags for %s: %s" % (o['id'], result.body)}))
            results.append(obj)

        return DBResponse(response_code=200, body=json.dumps(results))

from ..cache.store import CacheStore
from services.data.db_utils import DBResponse
import json


class Refinery(object):
    """Used to refine objects with data only available from S3.

    Parameters:
    -----------
    field_names: list of field names that contain S3 locations to be replaced with content.
    """

    def __init__(self, field_names):
        self.artifact_store = CacheStore().artifact_cache
        self.field_names = field_names

    async def refine_record(self, record):
        locations = [record[field] for field in self.field_names if field in record]
        data = await self.fetch_data(locations)
        _rec = record
        for k, v in _rec.items():
            if k in self.field_names:
                _rec[k] = data[v] if v in data else None
        return _rec

    async def refine_records(self, records):
        locations = [record[field] for field in self.field_names for record in records if field in record]
        data = await self.fetch_data(locations)

        _recs = []
        for rec in records:
            for k, v in rec.items():
                if k in self.field_names:
                    rec[k] = data[v] if v in data else None
            _recs.append(rec)
        return _recs

    async def fetch_data(self, locations):
        try:
            _res = await self.artifact_store.cache.GetArtifacts(locations)
            if not _res.is_ready():
                await _res.wait()
            return _res.get()
        except:
            return {}

    async def _postprocess(self, response: DBResponse):
        """
        Async post processing callback that can be used as the find_records helpers
        postprocessing parameter.

        Passed in DBResponse will be refined on the configured field_names, replacing the S3 locations
        with their contents.
        """
        if response.response_code != 200 or not response.body:
            return response
        if isinstance(response.body, list):
            body = await self.refine_records(response.body)
        else:
            body = await self.refine_record(response.body)

        return DBResponse(response_code=response.response_code,
                          body=body)


class TaskRefiner(Refinery):
    """
    Refiner class for postprocessing Task rows.

    Fetches specified content from S3 and cleans up unnecessary fields from response.
    """

    def __init__(self):
        super().__init__(field_names = ["task_ok", "foreach_stack"])

    async def postprocess(self, response: DBResponse):
        """Calls the refiner postprocessing to fetch S3 values for content.
        Cleans up returned fields, for example by combining 'task_ok' boolean into the 'status'
        """
        refined_response = await self._postprocess(response)
        if response.response_code != 200 or not response.body:
            return response

        def _cleanup(item):
            # TODO: test if 'running' statuses go through correctly.
            item['status'] = 'failed' if item['status'] == 'completed' and item['task_ok'] is False else item['status']
            item.pop('task_ok')
            return item

        if isinstance(refined_response.body, list):
            body = [_cleanup(task) for task in refined_response.body]
        else:
            body = _cleanup(refined_response.body)

        return DBResponse(response_code=refined_response.response_code, body=body)

from ..cache.store import CacheStore
from services.data.db_utils import DBResponse
import json

from ..features import FEATURE_REFINE_DISABLE
from services.utils import logging


class Refinery(object):
    """Used to refine objects with data only available from S3.

    Parameters:
    -----------
    field_names: list of field names that contain S3 locations to be replaced with content.
    """

    def __init__(self, field_names):
        self.artifact_store = CacheStore().artifact_cache
        self.field_names = field_names
        self.logger = logging.getLogger("DataRefiner")

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
            self.logger.exception("Exception when fetching artifact data from cache")
            return {}

    async def _postprocess(self, response: DBResponse):
        if FEATURE_REFINE_DISABLE:
            return response

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
        super().__init__(field_names=["task_ok", "foreach_stack"])

    async def postprocess(self, response: DBResponse):
        """Calls the refiner postprocessing to fetch S3 values for content.
        Cleans up returned fields, for example by combining 'task_ok' boolean into the 'status'
        """
        refined_response = await self._postprocess(response)
        if response.response_code != 200 or not response.body:
            return response

        def _process(item):
            if item['status'] == 'unknown':
                # cover boolean cases explicitly, as S3 refinement might fail,
                # in which case we want the 'unknown' status to remain.
                if item['task_ok'] is False:
                    item['status'] = 'failed'
                elif item['task_ok'] is True:
                    item['status'] = 'completed'

            item.pop('task_ok', None)

            if item['foreach_stack'] and len(item['foreach_stack']) > 0 and len(item['foreach_stack'][0]) >= 4:
                _, _name, _, _index = item['foreach_stack'][0]
                item['foreach_label'] = "{}[{}]".format(item['task_id'], _index)
            item.pop('foreach_stack', None)
            return item

        if isinstance(refined_response.body, list):
            body = [_process(task) for task in refined_response.body]
        else:
            body = _process(refined_response.body)

        return DBResponse(response_code=refined_response.response_code, body=body)

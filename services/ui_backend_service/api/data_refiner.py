from ..cache.store import CacheStore
from services.data.db_utils import DBResponse
import json

class Refinery(object):
  "Used to refine objects with data only available from S3"

  def __init__(self, field_names = []):
    self.artifact_store = CacheStore().artifact_cache
    self.field_names = field_names

  async def refine_record(self, record):
    locations = [ record[field] for field in self.field_names if field in record ]
    data = await self.fetch_data(locations)
    _rec = record
    for k, v in _rec.items():
      if k in self.field_names and v in data:
        _rec[k] = data[v]
    return _rec

  async def refine_records(self, records):
    locations = [ record[field] for field in self.field_names for record in records if field in record ]
    data = await self.fetch_data(locations)

    _recs = []
    for rec in records:
      for k, v in rec.items():
        if k in self.field_names and v in data:
          rec[k] = data[v]
      _recs.append(rec)
    return _recs
  
  async def fetch_data(self, locations):
    _res = await self.artifact_store.cache.GetArtifacts(locations)
    if not _res.is_ready():
      await _res.wait()
    return _res.get()

  async def postprocess(self, response: DBResponse):
    if response.response_code != 200 or not response.body:
        return response
    if isinstance(response.body, list):
      body = await self.refine_records(response.body)
    else:
      body = await self.refine_record(response.body)
    
    return DBResponse(response_code=response.response_code,
                      body=body)

from .refinery import Refinery


class ArtifactRefiner(Refinery):
    """
    Refiner class for postprocessing Artifact rows.

    Uses Metaflow Client API to refine Artifact's actual content from Metaflow Service and Datastore.

    Parameters
    -----------
    cache : AsyncCacheClient
        An instance of a cache that implements the GetArtifacts action.
    """

    def __init__(self, cache):
        super().__init__(cache=cache)

    def _action(self):
        return self.cache_store.cache.GetArtifacts

    def _record_to_action_input(self, record):
        # Prefer run_id over run_number
        # Prefer task_name over task_id
        return "{flow_id}/{run_id}/{step_name}/{task_name}/{name}/{attempt_id}".format(
            flow_id=record['flow_id'],
            run_id=record.get('run_id') or record['run_number'],
            step_name=record['step_name'],
            task_name=record.get('task_name') or record['task_id'],
            name=record['name'],
            attempt_id=record['attempt_id'])

    async def refine_record(self, record, values):
        record['content'] = str(values)
        return record

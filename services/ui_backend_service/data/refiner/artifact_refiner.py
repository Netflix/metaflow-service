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
        return "{flow_id}/{run_number}/{step_name}/{task_id}/{name}/{attempt_id}".format(**record)

    async def refine_record(self, record, values):
        record['content'] = str(values)
        return record
